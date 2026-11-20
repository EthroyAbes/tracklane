"""
PriceIntel — run_ledger.py

A run-outcome ledger for ingest runs, and the single definition of which runs a
published figure is allowed to read.

Why this exists
---------------
Before this, a scraper that failed part-way through pagination wrote a partial
set of rows and exited 0. Downstream, those rows were indistinguishable from a
complete but quiet day. Row-count checks cannot tell the two apart: a floor
breach means "fewer rows than usual", which is exactly what a genuine range
reduction also looks like. The distinction between "the source was enumerated
in full" and "collection aborted" is only knowable at write time, so it has to
be recorded at write time.

Contract
--------
Every scraper wraps its work in start_run() / complete_run() / fail_run():

    run_id = start_run(conn_path, source, run_date)
    try:
        ... fetch, parse, write snapshot rows ...
        complete_run(conn_path, run_id, rows_written=n, units_fetched=m)
    except Exception as e:
        fail_run(conn_path, run_id, stage="fetch", detail=str(e))
        raise

A run left in status='running' means the process died without recording an
outcome — that is itself a finding, not a pass.

Readers call canonical_run_ids() rather than filtering on run_date alone. It
applies two filters, in this order:

  1. Only runs recorded 'complete' are eligible. A failed or still-running run
     is excluded from every published figure.
  2. Among eligible runs for the same (source, run_date), exactly one wins —
     the most recently started. This is the existing duplicate-run defence
     (the 2026-08-06 incident) expressed against the ledger rather than
     re-derived from max(snapshots.id) at read time.

Historical exemption — bounded by an explicit date
--------------------------------------------------
The ledger starts empty. Every run_id already in snapshots predates it, and
treating those as ineligible would blank the entire history. So unrecorded runs
are exempt — but ONLY those dated before LEDGER_ADOPTION_DATE.

The bound matters. An open-ended "no ledger row means fine" rule is a permanent
backdoor: any future run that dies before recording itself, or any scraper added
later and never instrumented, would inherit the exemption and publish unchecked
forever. The exemption is a statement about a finite past, not a fallback.

On or after LEDGER_ADOPTION_DATE, a run present in snapshots with no ledger row
is a defect: either a scraper is uninstrumented or a process died before
start_run() committed. It is excluded from reporting and reported as a blocking
finding (validation.check_unrecorded_runs).

LEDGER_ADOPTION_DATE must be set to the first date on which every scraper writes
ledger rows — i.e. the day after deployment, not the day the code was written.
See DEPLOYMENT.md.

Run statuses
------------
'running'   started, no outcome recorded. The process died. Excluded; blocking.
'complete'  the source was enumerated in full. Publishable.
'partial'   some units failed but real rows were written. The ROWS ARE KEPT in
            storage, but the run is EXCLUDED from canonical published metrics.
'failed'    the run aborted, or what it wrote cannot be trusted. Excluded.

Why partial is still a distinct status if it is excluded like failed
--------------------------------------------------------------------
The distinction is about the data, not about publication. Altra raises before
writing anything, so an incomplete Altra fetch leaves no rows at all: 'failed'.
scraper.py (Google Shopping) and hoka_scraper.py both continue past a failed
unit and write what they got, so their incomplete runs leave real observations
in the database: 'partial'. Those observations are worth keeping — they are true
prices that were really on sale that day, and a later partial-coverage product
may use them, separately disclosed.

What they are not is a basis for a published panel figure. An earlier version of
this file let partial runs through into reporting when at least 90% of requests
had succeeded. That threshold was invented here, not agreed, and it silently
decided that a day missing a tenth of its coverage was a publishable day — which
is a publication policy, not an implementation detail. Withheld for this release
(DECISIONS.md D-2026-09-10-17): a run whose completeness is anything other than
'complete' does not feed a canonical metric, and unknown completeness never
passes as a warning.

Where a source has BOTH a partial and a complete run for the same day — a
re-run after a bad first attempt, which is the normal remedy — the complete run
is used. Exclusion happens before the canonical-run pick, so preferring the
complete run falls out of that ordering rather than needing its own rule.
"""

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

STATUS_RUNNING = "running"
STATUS_COMPLETE = "complete"
STATUS_PARTIAL = "partial"
STATUS_FAILED = "failed"

# Runs dated before this are exempt from requiring a ledger row. Runs on or
# after it must have one. Set this to the first date on which ALL scrapers ran
# with ledger instrumentation deployed — see DEPLOYMENT.md step 5.
LEDGER_ADOPTION_DATE = "2026-09-11"

# Statuses whose rows must never reach a canonical published figure. 'partial' is
# here deliberately: the rows stay in storage, but incomplete coverage is not a
# publishable basis. See the module docstring and DECISIONS.md D-2026-09-10-17.
EXCLUDED_STATUSES = (STATUS_RUNNING, STATUS_FAILED, STATUS_PARTIAL)

# Statuses that make a run usable for publication. Exactly one, by design: any
# state other than "we know this run enumerated its source in full" is unknown
# completeness, and unknown completeness does not publish.
PUBLISHABLE_STATUSES = (STATUS_COMPLETE,)


class IncompleteFetchError(RuntimeError):
    """Raised when a source could not be enumerated in full.

    Distinct from "the source returned nothing": that is an empty catalogue and
    a legitimate (if alarming) observation. This means we do not know what the
    catalogue contains, which is not an observation at all.
    """


def _now():
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path):
    return sqlite3.connect(str(db_path))


def ledger_available(db_path) -> bool:
    """True when migration 007 has been applied. Lets a scraper degrade to its
    old behaviour on a database that has not been migrated yet, rather than
    crashing on a missing table."""
    if not Path(db_path).exists():
        return False
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ingest_run'"
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def start_run(db_path, source, run_date, run_id=None) -> str:
    run_id = run_id or str(uuid.uuid4())
    if not ledger_available(db_path):
        return run_id
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO ingest_run (run_id, source, run_date, status, started_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (run_id, source, run_date, STATUS_RUNNING, _now()),
        )
        conn.commit()
    finally:
        conn.close()
    return run_id


def complete_run(db_path, run_id, rows_written=None, units_fetched=None,
                 units_expected=None):
    _finish(db_path, run_id, STATUS_COMPLETE, rows_written=rows_written,
            units_fetched=units_fetched, units_expected=units_expected)


def fail_run(db_path, run_id, stage, detail, rows_written=None, units_fetched=None,
             units_expected=None):
    """Record an aborted run. Rows already written by this run are intentionally
    left in place: they are evidence of what went wrong, and excluded_run_ids()
    keeps them out of every published figure by run_id."""
    _finish(db_path, run_id, STATUS_FAILED, stage, detail, rows_written,
            units_fetched, units_expected)


def partial_run(db_path, run_id, stage, detail, rows_written=None,
                units_fetched=None, units_expected=None):
    """Record a run that wrote real rows but did not enumerate its source in full.

    Distinct from fail_run: these rows ARE used. A Google Shopping run where 2 of
    36 query/page requests failed still observed 34 pages' worth of real market
    prices, and discarding them would lose good data and put a hole in the series.
    What must not happen is the day being read as complete.
    """
    _finish(db_path, run_id, STATUS_PARTIAL, stage, detail, rows_written,
            units_fetched, units_expected)


def _finish(db_path, run_id, status, stage=None, detail=None, rows_written=None,
            units_fetched=None, units_expected=None):
    if not ledger_available(db_path):
        return
    conn = _connect(db_path)
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(ingest_run)")}
        sets = ["status=?", "finished_at=?", "rows_written=?", "units_fetched=?",
                "failure_stage=?", "failure_detail=?"]
        vals = [status, _now(), rows_written, units_fetched, stage,
                (detail or "")[:2000]]
        if "units_expected" in cols:
            sets.append("units_expected=?")
            vals.append(units_expected)
        vals.append(run_id)
        conn.execute(f"UPDATE ingest_run SET {', '.join(sets)} WHERE run_id=?", vals)
        conn.commit()
    finally:
        conn.close()


def _table_exists(conn, name):
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _ledger_exists(conn):
    return _table_exists(conn, "ingest_run")


def excluded_run_ids(conn, adoption_date=None) -> set:
    """run_ids that must not reach a published figure.

    Two populations:
      1. Runs the ledger recorded as 'running' or 'failed'. 'partial' is NOT
         excluded — see the module docstring.
      2. Runs present in snapshots on or after the adoption date with NO ledger
         row at all. Before the adoption date these are historical and exempt.

    Returns an empty set when the ledger table does not exist, so callers on an
    unmigrated database behave exactly as they did before.
    """
    if not _ledger_exists(conn):
        return set()
    cutoff = adoption_date or LEDGER_ADOPTION_DATE
    excluded = {
        r[0] for r in conn.execute(
            f"SELECT run_id FROM ingest_run WHERE status IN "
            f"({','.join('?' * len(EXCLUDED_STATUSES))})", EXCLUDED_STATUSES
        )
    }
    excluded |= {r[0] for r in unrecorded_run_ids(conn, cutoff)}
    return excluded


def unrecorded_run_ids(conn, adoption_date=None):
    """(run_id, source, run_date) for runs in snapshots dated on or after the
    adoption date that have no ledger row.

    On or after adoption this means either an uninstrumented scraper or a process
    that died before start_run() committed. Both are defects, and both would
    otherwise sail through on the historical exemption.
    """
    # A ledger with no snapshots table is degenerate rather than impossible
    # (a fresh or partially built database). Returning nothing beats raising
    # from inside the report generator's data layer.
    if not _ledger_exists(conn) or not _table_exists(conn, "snapshots"):
        return []
    cutoff = adoption_date or LEDGER_ADOPTION_DATE
    return [tuple(r) for r in conn.execute(
        """
        SELECT DISTINCT s.run_id, s.source, s.run_date
          FROM snapshots s
         WHERE s.run_date >= ?
           AND NOT EXISTS (SELECT 1 FROM ingest_run i WHERE i.run_id = s.run_id)
         ORDER BY s.run_date, s.source
        """, (cutoff,))]


def has_publishable_run(conn, source, run_date):
    """True when this source has at least one run recorded 'complete' for the day.

    Used to decide whether a partial run is merely superseded (a complete re-run
    exists and is what gets used) or whether excluding it leaves the source with
    no usable data at all, which is a blocking condition.
    """
    if not _ledger_exists(conn):
        return False
    row = conn.execute(
        "SELECT 1 FROM ingest_run WHERE source=? AND run_date=? AND status=? LIMIT 1",
        (source, run_date, STATUS_COMPLETE),
    ).fetchone()
    return row is not None


def sources_with_rows(conn, run_date):
    if not _table_exists(conn, "snapshots"):
        return []
    return [r[0] for r in conn.execute(
        "SELECT DISTINCT source FROM snapshots WHERE run_date=?", (run_date,))]


def incomplete_runs(conn, run_date=None, statuses=EXCLUDED_STATUSES):
    """Ledger rows for runs in the given statuses. Defaults to the excluded ones
    ('running', 'failed'); pass statuses=(STATUS_PARTIAL,) for partials."""
    if not _ledger_exists(conn):
        return []
    placeholders = ",".join("?" * len(statuses))
    sql = ("SELECT run_id, source, run_date, status, failure_stage, failure_detail, "
           "       rows_written, units_fetched "
           f"FROM ingest_run WHERE status IN ({placeholders})")
    params = list(statuses)
    if run_date:
        sql += " AND run_date = ?"
        params.append(run_date)
    keys = ("run_id", "source", "run_date", "status", "failure_stage",
            "failure_detail", "rows_written", "units_fetched")
    return [dict(zip(keys, r)) for r in conn.execute(sql, params)]
