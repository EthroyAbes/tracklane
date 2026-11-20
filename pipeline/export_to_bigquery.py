"""
Tracklane -> BigQuery nightly export (v1.1).

Reads priceintel.db READ-ONLY and replaces each exported table in BigQuery with a
fresh full copy. BigQuery is a copy for analysis; priceintel.db stays the master.

By default it exports EVERY table in the database (so it keeps working while the
schema is being cleaned up). Restrict with --tables, or add custom SQL views to
EXTRA_EXPORTS (v2 will add the validation view here).

Config (in .env next to this script, or exported in the shell):
    PI_DB_PATH        /home/<user>/priceintel/priceintel.db
    PI_GCP_KEY_PATH   /home/<user>/priceintel/secrets/bq-exporter.json   (never commit)
    PI_BQ_PROJECT     tracklane-analytics
    PI_BQ_DATASET     tracklane
    PI_BQ_LOCATION    EU

Usage:
    python3.X export_to_bigquery.py --list                 # show tables + row counts, nothing else
    python3.X export_to_bigquery.py --dry-run              # no Google calls; shows schemas
    python3.X export_to_bigquery.py                        # real upload, all tables
    python3.X export_to_bigquery.py --tables snapshots     # real upload, named tables only
"""
import argparse, datetime as dt, json, os, re, sqlite3, sys, tempfile
from pathlib import Path

# Optional custom exports: BigQuery table name -> SQL (v2: validation view goes here)
EXTRA_EXPORTS = {
    # "validation": open(Path(__file__).with_name("sql") / "validation_view.sql").read(),
}

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
BQ_NAME_RE = re.compile(r"[^A-Za-z0-9_]")


def load_env(path):
    p = Path(path)
    if p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def list_tables(con):
    return [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]


def merge_type(current, value):
    """Widen the column type as values are seen: None < BOOL < INT64 < FLOAT64 < STRING; DATE is its own lane."""
    if value is None or value == "":
        return current
    if isinstance(value, bool):
        t = "BOOL"
    elif isinstance(value, int):
        t = "INT64"
    elif isinstance(value, float):
        t = "FLOAT64"
    elif isinstance(value, str) and DATE_RE.match(value):
        t = "DATE"
    else:
        t = "STRING"
    if current is None or current == t:
        return t
    numeric = {"BOOL", "INT64", "FLOAT64"}
    if current in numeric and t in numeric:
        return "FLOAT64" if "FLOAT64" in (current, t) else "INT64"
    return "STRING"


def coerce(value, bq_type):
    if value is None or (value == "" and bq_type != "STRING"):
        return None
    if isinstance(value, bytes):
        return value.hex()
    if bq_type == "FLOAT64":
        return float(value)
    if bq_type == "INT64":
        return int(value)
    if bq_type == "STRING" and not isinstance(value, str):
        return str(value)
    return value


def infer_schema(con, sql):
    """Pass 1 (streaming): column names + widest type seen across ALL rows."""
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    types = [None] * len(cols)
    n = 0
    for row in cur:
        n += 1
        for i, v in enumerate(row):
            types[i] = merge_type(types[i], v)
    return cols, [t or "STRING" for t in types], n


def write_jsonl(con, sql, cols, types, path, exported_at):
    """Pass 2 (streaming): write rows as newline-delimited JSON."""
    safe = [BQ_NAME_RE.sub("_", c) for c in cols]
    with open(path, "w", encoding="utf-8") as f:
        for row in con.execute(sql):
            rec = {c: coerce(v, t) for c, t, v in zip(safe, types, row)}
            rec["exported_at"] = exported_at
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return safe


def stage_exports(con, exports, directory, exported_at):
    """Freeze every raw table, then release SQLite before network uploads."""
    staged = []
    try:
        for index, (table, sql) in enumerate(exports.items()):
            cols, types, count = infer_schema(con, sql)
            path = str(Path(directory) / f'{index}.jsonl')
            safe = write_jsonl(con, sql, cols, types, path, exported_at)
            staged.append((table, safe, types, count, path))
            memory_checkpoint("staged:"+table)
        return staged
    finally:
        con.close()


def memory_checkpoint(label):
    try:
        import resource
    except ImportError:  # Windows development; production measurements are Linux RSS.
        return
    print("MEMORY", label, "peak_rss_kib", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, flush=True)


class DiskRows:
    """Repeatable validated rows without retaining all export tables in RAM."""
    def __init__(self, rows):
        self.file = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        self.count = 0
        try:
            for row in rows:
                self.file.write(json.dumps(row, ensure_ascii=False) + chr(10))
                self.count += 1
            self.file.flush()
        except BaseException:
            self.file.close()
            raise

    def __len__(self):
        return self.count

    def __iter__(self):
        self.file.seek(0)
        for line in self.file:
            yield json.loads(line)

    def close(self):
        self.file.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--export-worker", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--tables", nargs="*", help="only export these tables")
    ap.add_argument('--retailer-cycle', action='store_true', help='Build governed decisions and refresh retailer tables only')
    ap.add_argument('--reuse-snapshot', action='store_true', help='Use a verified current-day frozen batch without recalculating')
    ap.add_argument('--governed-config', default='/home/Ethanabraham/governed-export.json')
    args = ap.parse_args()

    load_env(Path(__file__).with_name(".env"))
    db = os.environ.get("PI_DB_PATH", "priceintel.db")
    project = os.environ.get("PI_BQ_PROJECT")
    dataset = os.environ.get("PI_BQ_DATASET", "tracklane")
    location = os.environ.get("PI_BQ_LOCATION", "EU")
    key = os.environ.get("PI_GCP_KEY_PATH")

    governed_config = Path(args.governed_config)
    governed = json.loads(governed_config.read_text()) if governed_config.exists() else None
    export_lock = None
    if governed:
        import fcntl
        export_lock = governed_config.with_suffix('.lock').open('a')
        try:
            fcntl.flock(export_lock,fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            sys.exit('Another export is in progress; no overlapping load started')
    if args.retailer_cycle:
        if not governed or args.tables or args.list:
            sys.exit('Retailer cycle requires governed configuration and no table/list override')
        today=dt.datetime.now(dt.timezone.utc).date().isoformat()
        if not governed['early_start'] <= today <= governed['early_end']:
            print('Outside approved early export window');return
        import subprocess
        if not args.reuse_snapshot:
            subprocess.run([governed['python'], governed['builder'], '--config', str(governed_config)],check=True)
        sys.path.insert(0,governed['code_root'])
        from tools.governed_bigquery import RETAILER_TABLES
        args.tables=RETAILER_TABLES

    if not Path(db).exists():
        sys.exit(f"DB not found: {db}")
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)   # read-only: cannot modify the master
    con.execute('BEGIN')

    tables = list_tables(con)
    if args.list:
        for t in tables:
            n = con.execute('SELECT COUNT(*) FROM "%s"' % t).fetchone()[0]
            print(f"{t}: {n:,} rows")
        return

    selected = args.tables if args.tables else tables
    missing = [t for t in selected if t not in tables]
    if missing:
        sys.exit(f"Unknown table(s): {missing}. Available: {tables}")
    exports = {t: f'SELECT * FROM "{t}"' for t in selected}
    if not args.tables:
        exports.update(EXTRA_EXPORTS)

    exported_at = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    governed_rows = None
    governed_error = None
    if governed and (args.retailer_cycle or not args.tables):
        sys.path.insert(0,governed['code_root'])
        from tools.governed_bigquery import prepare, upload
        try:
            governed_rows=DiskRows(prepare(con,governed['archive'],exported_at[:10]))
        except ValueError as exc:
            governed_error=str(exc)
            print('Governed export held: '+governed_error)
    historical_tables = None
    if governed_rows is not None and governed.get('historical'):
        from tools.historical_bigquery import prepare as prepare_history
        historical_tables = {k:DiskRows(v) for k,v in prepare_history(con,governed['historical']).items()}
    measure_rows = None
    measure_error = None
    if governed_rows is not None and governed.get('measures'):
        from tools.retailer_measure_bigquery import prepare as prepare_measures
        try:
            measure_config = governed['measures']
            if measure_config.get('daily'):
                from tools.retailer_measure_daily_export import resolve
                measure_config = resolve(measure_config, exported_at[:10])
            measure_rows = prepare_measures(con,measure_config)
        except Exception as exc:
            measure_error = str(exc)
            print('Measures held: '+measure_error)
    memory_checkpoint("prepared-derived")
    staging = None
    try:
        staging = tempfile.TemporaryDirectory(prefix='tracklane-export-')
        staged = stage_exports(con, exports, staging.name, exported_at)
        client = None
        if not args.dry_run:
            if not (project and key):
                sys.exit("Set PI_BQ_PROJECT and PI_GCP_KEY_PATH in .env (or use --dry-run).")
            from google.cloud import bigquery
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(key)
            client = bigquery.Client(project=project, credentials=creds, location=location)
            client.create_dataset(f"{project}.{dataset}", exists_ok=True)

        failures = 0
        for table, safe_cols, types, n, tmp_path in staged:
            bq_table = BQ_NAME_RE.sub("_", table)
            print(f"[{table}] {n:,} rows staged")
            try:
                if args.dry_run:
                    print(f"  dry run OK ({os.path.getsize(tmp_path)/1e6:.1f} MB of JSON would be uploaded)")
                    continue
                schema = [bigquery.SchemaField(c, t) for c, t in zip(safe_cols, types)]
                schema.append(bigquery.SchemaField("exported_at", "TIMESTAMP"))
                job_cfg = bigquery.LoadJobConfig(
                    schema=schema,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # full replace each run
                )
                with open(tmp_path, "rb") as f:
                    client.load_table_from_file(f, f"{project}.{dataset}.{bq_table}", job_config=job_cfg).result()
                bq_n = client.get_table(f"{project}.{dataset}.{bq_table}").num_rows
                ok = bq_n == n
                failures += not ok
                print(f"  -> {project}.{dataset}.{bq_table}: source {n:,} vs BigQuery {bq_n:,} [{'OK' if ok else 'MISMATCH'}]")
            except Exception as e:
                failures += 1
                print(f"  FAILED: {e}")
            finally:
                os.remove(tmp_path)

        if governed_rows is not None and not failures:
            if args.dry_run:
                print(f'governed dry run: {len(governed_rows)} verified offer rows')
            else:
                print(json.dumps(upload(client,governed_rows,project,dataset,exported_at,bigquery)))
        if historical_tables is not None and not failures:
            if args.dry_run:
                print('historical dry run: '+json.dumps({k:len(v) for k,v in historical_tables.items()}))
            else:
                from tools.historical_bigquery import upload as upload_history
                print(json.dumps(upload_history(client,historical_tables,project,dataset,exported_at,bigquery)))
        if measure_rows is not None and not failures:
            if args.dry_run:
                print('measure dry run: '+str(len(measure_rows))+' rows')
            else:
                from tools.retailer_measure_bigquery import upload as upload_measures
                try:
                    print(json.dumps(upload_measures(client,measure_rows,project,dataset,exported_at,bigquery)))
                except Exception as exc:
                    measure_error = str(exc)
                    print('Measures held: '+measure_error)
    finally:
        if measure_rows is not None:
            measure_rows.close()
        if governed_rows is not None:
            governed_rows.close()
        if historical_tables is not None:
            for rows in historical_tables.values():
                rows.close()
        if export_lock is not None:
            export_lock.close()
        if staging is not None:
            staging.cleanup()
    if measure_error:
        failures += 1
    if governed_error:
        failures += 1
    if failures:
        sys.exit(f"{failures} table(s) failed")
    memory_checkpoint("completed")
    print("done", exported_at)


if __name__ == "__main__":
    if "--export-worker" in sys.argv:
        main()
    else:
        import subprocess
        # The small parent survives a worker RAM kill and removes only this
        # run's generated upload copies, including files left by SIGKILL.
        with tempfile.TemporaryDirectory(prefix="tracklane-export-run-") as scratch:
            result = subprocess.run(
                [sys.executable, "-u", __file__, "--export-worker", *sys.argv[1:]],
                env=dict(os.environ, TMPDIR=scratch, TEMP=scratch, TMP=scratch),
            )
        if result.returncode:
            print("EXPORT FAILED: worker return code", result.returncode, flush=True)
        sys.exit(result.returncode if result.returncode >= 0 else 128 - result.returncode)
