#!/usr/bin/env python3
"""
build_site.py: rebuild the tracklane.co.uk homepage from BigQuery and (optionally) deploy it to Netlify.

What it does
  1. Reads daily counts from tracklane-analytics.tracklane.retailer_offer_measure (read-only).
  2. Picks the retailers that count towards the headline "markdown rate" (the panel):
     verified on the latest day AND on at least PANEL_MIN of the last PANEL_WIN days.
  3. Pulls brand counts and medians for the latest day and the comparison day (a week earlier).
  4. Runs sanity checks. If any fail, it writes nothing and exits with code 1,
     so the live site keeps the last good version.
  5. Writes <out>/index.html from site_template.html.
  6. With --deploy, zips <out> and uploads it to Netlify (needs NETLIFY_AUTH_TOKEN and NETLIFY_SITE_ID).

Usage
  # normal (PythonAnywhere): needs google-cloud-bigquery and a service-account key
  GOOGLE_APPLICATION_CREDENTIALS=~/priceintel/secrets/bq-exporter.json \
  python3 build_site.py --out ./site [--deploy]

  # offline (no BigQuery client): feed it query results saved as JSON
  python3 build_site.py --print-sql daily
  python3 build_site.py --offline ./data --print-sql followups   # after saving daily.json
  python3 build_site.py --offline ./data --out ./site             # after saving brands.json, pcts.json

Metric rules (match the accepted dashboard query, 8 Nov 2026):
  - one population per source/day: frozen_live where it exists, else accepted_historical
  - denominator = metric_ready; reduced = metric_ready AND reduced
  - retailer_first_seen -> markdown rate; retailer_advertised -> share showing a "was" price
  - medians / p90 via PERCENTILE_CONT over reduced rows
  - a source-day is "verified" when source_day_usable and both measures have ready rows
"""
import argparse, datetime as dt, html, io, json, os, re, sys, urllib.request, zipfile
from pathlib import Path
from publication_guard import check_update

TABLE = "tracklane-analytics.tracklane.retailer_offer_measure"
PROJECT = "tracklane-analytics"
HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "site_template.html"

RETAILER_NAMES = {
    "prodirect_direct": "Pro:Direct",
    "startfitness_direct": "Start Fitness",
    "runnersneed_direct": "Runners Need",
    "sportsshoes_direct": "SportsShoes",
}
PANEL_WIN = 14          # look-back window for panel membership (days)
PANEL_MIN = 12          # verified days needed in that window to join the headline rate
COMPARE_DAYS = 7        # "this week" comparison
COMPARE_SLACK = 3       # if no clean comparison day, step back up to this many extra days
BRAND_MIN = 50          # shoes needed for a brand to get a bar
EVENT_PTS = 15          # retailer/brand rise (pts in a week) that earns the red-dot marker
ANNOT_PTS = 10          # single-day rise in a panel retailer's own rate that gets a chart label
MAX_AGE_DAYS = 2        # refuse to build if the newest data is older than this (vs today, UTC)
MIN_PANEL_SHOES = 1000  # refuse to build if the headline denominator is smaller than this

BRAND_CASE = """CASE brand WHEN 'Hoka' THEN 'HOKA' WHEN 'adidas' THEN 'Adidas' WHEN 'Inov8' THEN 'Inov-8'
  WHEN 'On Running' THEN 'On' WHEN 'Vivobarefoot' THEN 'Vivo Barefoot' ELSE brand END"""

BASE_CTE = f"""base AS (
  SELECT t.* FROM `{TABLE}` t
  QUALIFY population_origin = MAX(IF(population_origin = 'frozen_live', 'frozen_live', 'accepted_historical'))
          OVER (PARTITION BY source, observed_date)
)"""

SQL_DAILY = f"""WITH {BASE_CTE}
SELECT CAST(observed_date AS STRING) AS d, source,
  COUNTIF(measure = 'retailer_advertised' AND metric_ready) AS adv_ready,
  COUNTIF(measure = 'retailer_advertised' AND metric_ready AND reduced) AS adv_red,
  COUNTIF(measure = 'retailer_first_seen' AND metric_ready) AS fs_ready,
  COUNTIF(measure = 'retailer_first_seen' AND metric_ready AND reduced) AS fs_red,
  LOGICAL_AND(source_day_usable) AS usable
FROM base GROUP BY 1, 2 ORDER BY 1, 2"""

SQL_BRANDS = f"""WITH {BASE_CTE}
SELECT CAST(observed_date AS STRING) AS d, {BRAND_CASE} AS brand, source,
  COUNTIF(measure = 'retailer_first_seen' AND metric_ready) AS fs_ready,
  COUNTIF(measure = 'retailer_first_seen' AND metric_ready AND reduced) AS fs_red
FROM base
WHERE CAST(observed_date AS STRING) IN UNNEST(@dates) AND source IN UNNEST(@sources)
GROUP BY 1, 2, 3"""

SQL_PCTS = f"""WITH {BASE_CTE}
SELECT DISTINCT CAST(observed_date AS STRING) AS d, measure,
  PERCENTILE_CONT(reduction_pct, 0.5) OVER (PARTITION BY observed_date, measure) AS med,
  PERCENTILE_CONT(reduction_pct, 0.9) OVER (PARTITION BY observed_date, measure) AS p90
FROM base
WHERE metric_ready AND reduced
  AND CAST(observed_date AS STRING) IN UNNEST(@dates) AND source IN UNNEST(@sources)"""


class BuildError(Exception):
    pass


# ---------------------------------------------------------------- data access
def literal_sql(sql, params):
    """Inline @params for printing / pasting into the BigQuery console."""
    for k, v in params.items():
        sql = sql.replace("@" + k, "[" + ", ".join("'%s'" % x for x in v) + "]")
    return sql


def run_bq(sql, params=None):
    from google.cloud import bigquery  # imported lazily so --offline needs no dependency
    client = bigquery.Client(project=PROJECT)
    qp = [bigquery.ArrayQueryParameter(k, "STRING", v) for k, v in (params or {}).items()]
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp))
    return [dict(r.items()) for r in job.result()]


def load_json(path):
    rows = json.loads(Path(path).read_text())
    # Accept either a list of dicts, or the raw BigQuery REST shape {"schema":..., "rows":[{"f":[{"v":..}]}]}
    if isinstance(rows, dict) and "rows" in rows:
        names = [f["name"] for f in rows["schema"]["fields"]]
        rows = [{n: c["v"] for n, c in zip(names, r["f"])} for r in rows["rows"]]
    return rows


# ---------------------------------------------------------------- model
def to_int(v):
    return int(v) if v not in (None, "") else 0


def to_bool(v):
    return v is True or str(v).lower() == "true"


def pct(n, d):
    return 100.0 * n / d if d else None


def parse_daily(rows):
    days = {}
    for r in rows:
        rec = dict(adv_ready=to_int(r["adv_ready"]), adv_red=to_int(r["adv_red"]),
                   fs_ready=to_int(r["fs_ready"]), fs_red=to_int(r["fs_red"]))
        rec["verified"] = to_bool(r["usable"]) and rec["fs_ready"] > 0 and rec["adv_ready"] > 0
        days.setdefault(str(r["d"]), {})[r["source"]] = rec
    return days


def verified(days, d, src):
    return days.get(d, {}).get(src, {}).get("verified", False)


def date_range(start, end):
    s, e = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    return [(s + dt.timedelta(i)).isoformat() for i in range((e - s).days + 1)]


def choose(days):
    dates = sorted(days)
    latest = max(d for d in dates if any(v["verified"] for v in days[d].values()))
    window = date_range((dt.date.fromisoformat(latest) - dt.timedelta(PANEL_WIN - 1)).isoformat(), latest)
    status = {}
    for src in sorted({s for d in days.values() for s in d}):
        n_ok = sum(verified(days, d, src) for d in window)
        last_ok = max((d for d in dates if verified(days, d, src)), default=None)
        status[src] = dict(window_ok=n_ok, last_ok=last_ok, today=verified(days, latest, src))
    panel = [s for s, st in status.items() if st["today"] and st["window_ok"] >= PANEL_MIN]
    if not panel:
        raise BuildError(f"no retailer qualifies for the rate on {latest} "
                         f"(needs verified today and {PANEL_MIN}/{PANEL_WIN} days)")
    comp = None
    for back in range(COMPARE_DAYS, COMPARE_DAYS + COMPARE_SLACK + 1):
        cand = (dt.date.fromisoformat(latest) - dt.timedelta(back)).isoformat()
        if all(verified(days, cand, s) for s in panel):
            comp = cand
            break
    if comp is None:
        raise BuildError(f"no clean comparison day {COMPARE_DAYS}-{COMPARE_DAYS + COMPARE_SLACK} days before {latest}")
    return latest, comp, panel, status


def pooled(days, d, panel):
    if not all(verified(days, d, s) for s in panel):
        return None
    tot = {k: sum(days[d][s][k] for s in panel) for k in ("adv_ready", "adv_red", "fs_ready", "fs_red")}
    return tot


def build_model(days, brand_rows, pct_rows, today=None):
    latest, comp, panel, status = choose(days)
    first = min(days)
    errors = []

    # staleness
    today = today or dt.datetime.now(dt.timezone.utc).date()
    age = (today - dt.date.fromisoformat(latest)).days
    if age > MAX_AGE_DAYS:
        errors.append(f"newest verified data is {latest}, {age} days old (limit {MAX_AGE_DAYS})")

    # daily pooled series over the panel
    series = []
    for d in date_range(first, latest):
        t = pooled(days, d, panel)
        series.append([d] + ([None] * 4 if t is None else [t["adv_ready"], t["adv_red"], t["fs_ready"], t["fs_red"]]))
    now, prev = pooled(days, latest, panel), pooled(days, comp, panel)
    rate_now, rate_prev = pct(now["fs_red"], now["fs_ready"]), pct(prev["fs_red"], prev["fs_ready"])
    adv_now = pct(now["adv_red"], now["adv_ready"])
    if now["fs_ready"] < MIN_PANEL_SHOES:
        errors.append(f"only {now['fs_ready']} shoes in the rate on {latest} (minimum {MIN_PANEL_SHOES})")

    # percentiles
    P = {(r["d"], r["measure"]): (float(r["med"]), float(r["p90"])) for r in pct_rows}
    need = [(latest, "retailer_first_seen"), (comp, "retailer_first_seen"), (latest, "retailer_advertised")]
    missing = [k for k in need if k not in P]
    if missing:
        errors.append(f"missing percentile rows: {missing}")
        P.update({k: (float("nan"), float("nan")) for k in missing})

    # brands (panel only, latest vs comparison day)
    agg = {}
    for r in brand_rows:
        b = agg.setdefault(r["brand"], {"now": [0, 0], "prev": [0, 0], "src": set()})
        key = "now" if r["d"] == latest else "prev" if r["d"] == comp else None
        if key is None:
            continue
        b[key][0] += to_int(r["fs_red"]); b[key][1] += to_int(r["fs_ready"])
        if key == "now" and to_int(r["fs_ready"]) > 0:
            b["src"].add(RETAILER_NAMES.get(r["source"], r["source"]))
    brands = []
    for name, b in agg.items():
        if b["now"][1] < BRAND_MIN or not name:
            continue
        src = sorted(b["src"])
        src_txt = " + ".join(src) if len(src) > 1 else f"{src[0]} only"
        prev_red, prev_ready = (b["prev"] if b["prev"][1] >= BRAND_MIN // 2 else (None, None))
        brands.append([name, b["now"][0], b["now"][1], prev_red, prev_ready, src_txt])
    if len(brands) < 3:
        errors.append(f"only {len(brands)} brands with {BRAND_MIN}+ shoes")

    # retailer cards: every retailer verified on the latest day
    retailers = []
    for src, st in status.items():
        if not st["today"]:
            continue
        r = days[latest][src]
        fs, adv = pct(r["fs_red"], r["fs_ready"]), pct(r["adv_red"], r["adv_ready"])
        fs_prev = pct(days[comp][src]["fs_red"], days[comp][src]["fs_ready"]) if verified(days, comp, src) else None
        # biggest single-day rise in this retailer's own rate within the last week
        jump = None
        for d in date_range((dt.date.fromisoformat(latest) - dt.timedelta(COMPARE_DAYS - 1)).isoformat(), latest):
            p = (dt.date.fromisoformat(d) - dt.timedelta(1)).isoformat()
            if verified(days, d, src) and verified(days, p, src):
                rise = pct(days[d][src]["fs_red"], days[d][src]["fs_ready"]) - pct(days[p][src]["fs_red"], days[p][src]["fs_ready"])
                if rise >= ANNOT_PTS and (jump is None or rise > jump[1]):
                    jump = (d, rise, pct(days[p][src]["fs_red"], days[p][src]["fs_ready"]), pct(days[d][src]["fs_red"], days[d][src]["fs_ready"]))
        retailers.append(dict(src=src, name=RETAILER_NAMES.get(src, src), fs=fs, adv=adv, fs_prev=fs_prev,
                              in_rate=src in panel, window_ok=st["window_ok"], jump=jump,
                              event=fs_prev is not None and fs - fs_prev >= EVENT_PTS))
    retailers.sort(key=lambda r: (not r["in_rate"], -r["fs"]))
    absent = [(RETAILER_NAMES.get(s, s), st["last_ok"]) for s, st in status.items() if not st["today"]]

    # chart annotation: most recent single-day rise >= ANNOT_PTS in a panel retailer's own rate
    annotation = None
    for d in reversed(date_range(first, latest)):
        p = (dt.date.fromisoformat(d) - dt.timedelta(1)).isoformat()
        best = None
        for s in panel:
            if verified(days, d, s) and verified(days, p, s):
                rise = pct(days[d][s]["fs_red"], days[d][s]["fs_ready"]) - pct(days[p][s]["fs_red"], days[p][s]["fs_ready"])
                if rise >= ANNOT_PTS and (best is None or rise > best[1]):
                    best = (s, rise)
        if best:
            annotation = {"date": d, "label": f"{RETAILER_NAMES.get(best[0], best[0])} sale starts"}
            break

    # sanity on every rate
    for v in [rate_now, rate_prev, adv_now] + [r["fs"] for r in retailers]:
        if v is None or not (0 <= v <= 100):
            errors.append(f"rate out of range: {v}")

    coverage = sum(days[latest][s]["fs_ready"] for s in status if status[s]["today"])
    return dict(latest=latest, comp=comp, first=first, panel=panel, series=series, now=now, prev=prev,
                rate_now=rate_now, rate_prev=rate_prev, adv_now=adv_now, P=P, brands=brands,
                retailers=retailers, absent=absent, annotation=annotation, coverage=coverage,
                n_verified=sum(1 for s in status if status[s]["today"]), errors=errors)


# ---------------------------------------------------------------- render
def long_date(d):
    x = dt.date.fromisoformat(d)
    return f"{x.day} {x.strftime('%B %Y')}"


def short_date(d):
    x = dt.date.fromisoformat(d)
    return f"{x.day} {x.strftime('%b')}"


def join_names(names):
    names = list(names)
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


def retailer_html(r, m, span):
    esc = html.escape
    badges = ""
    if r["event"]:
        badges += ' <span class="ev"><i class="mark" aria-hidden="true"></i>Sale event</span>'
    if not r["in_rate"]:
        badges += ' <span class="tag">Not yet in the rate</span>'
    prev = f"<br>({r['fs_prev']:.1f}% a week earlier)" if r["fs_prev"] is not None else ""
    if r["event"]:
        note = f"Markdowns rose from <b>{r['fs_prev']:.1f}% to {r['fs']:.1f}%</b> since {short_date(m['comp'])}"
        if r["jump"]:
            j = r["jump"]
            note += f", including a jump from {j[2]:.1f}% to {j[3]:.1f}% on {short_date(j[0])}"
        note += "."
    elif not r["in_rate"]:
        note = (f"Back after a gap in our verified data; joins the rate once it has {PANEL_MIN} verified days "
                f"out of the last {PANEL_WIN} ({r['window_ok']} so far).")
    else:
        note = (f"{r['adv']:.0f}% of shoes carry a sale label, but <b>{r['fs']:.0f}%</b> have actually dropped "
                f"since we first saw them.")
    muted = "" if r["in_rate"] else " muted-card"
    return f"""    <article class="card retailer-card span-{span}{muted}">
      <h3>{esc(r['name'])}{badges}</h3>
      <div class="pair">
        <div><div class="v">{r['fs']:.1f}%</div><div class="d">below first-seen price{prev}</div></div>
        <div><div class="v" style="color:var(--muted)">{r['adv']:.1f}%</div><div class="d">showing a &ldquo;was&rdquo; price</div></div>
      </div>
      <p class="note">{note}</p>
    </article>"""


def retailer_row_html(r, m):
    """15 Nov layout: one stacked row per retailer in the rate, inside the 'Retailers this week' card."""
    esc = html.escape
    badge = ' <span class="ev"><i class="mark" aria-hidden="true"></i>Sale event</span>' if r["event"] else ""
    prev = f"<br>({r['fs_prev']:.1f}% a week earlier)" if r["fs_prev"] is not None else ""
    if r["event"]:
        note = f"Markdowns rose from <b>{r['fs_prev']:.1f}% to {r['fs']:.1f}%</b> since {short_date(m['comp'])}"
        if r["jump"]:
            j = r["jump"]
            note += f", including a jump from {j[2]:.1f}% to {j[3]:.1f}% on {short_date(j[0])}"
        note += "."
    else:
        note = (f"{r['adv']:.0f}% of shoes carry a sale label, but <b>{r['fs']:.0f}%</b> have actually dropped "
                f"since we first saw them.")
    return f"""        <div class="retailer">
          <h3>{esc(r['name'])}{badge}</h3>
          <div class="pair">
            <div><div class="v">{r['fs']:.1f}%</div><div class="d">below first-seen price{prev}</div></div>
            <div><div class="v" style="color:var(--muted)">{r['adv']:.1f}%</div><div class="d">showing a &ldquo;was&rdquo; price</div></div>
          </div>
          <p class="note">{note}</p>
        </div>"""


def retailer_caption(m):
    """Plain-text note for retailers outside the rate (verified-but-waiting, or not verified on the latest day)."""
    waiting = [r for r in m["retailers"] if not r["in_rate"]]
    parts = []
    if waiting:
        names = join_names(f"{r['name']} ({r['window_ok']} of the last {PANEL_WIN} days verified)" for r in waiting)
        parts.append(f"{names} {'join' if len(waiting) > 1 else 'joins'} the rate after {PANEL_MIN} verified days out of {PANEL_WIN}.")
    if m["absent"]:
        parts.append("Not verified on " + short_date(m["latest"]) + ": " +
                     "; ".join(f"{n} (last verified {short_date(d)})" if d else n for n, d in m["absent"]) + ".")
    if not parts:
        parts.append(f"Retailers join the rate after {PANEL_MIN} verified days out of the last {PANEL_WIN}.")
    return " ".join(html.escape(p) for p in parts)


def render(m, template_text):
    chg = m["rate_now"] - m["rate_prev"]
    panel_names = join_names(RETAILER_NAMES.get(s, s) for s in sorted(m["panel"], key=lambda s: RETAILER_NAMES.get(s, s)))
    foot_parts = []
    if m["absent"]:
        foot_parts.append("Not verified on " + short_date(m["latest"]) + ": " +
                          "; ".join(f"{n} (last verified {short_date(d)})" if d else n for n, d in m["absent"]) + ".")
    foot_parts.append(f"Retailers join the rate after {PANEL_MIN} verified days out of the last {PANEL_WIN}.")
    vals = {
        "BUILD_STAMP": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "LATEST_LONG": long_date(m["latest"]),
        "LATEST_SHORT": short_date(m["latest"]),
        "FIRST_LONG": long_date(m["first"]),
        "COMP_SHORT": short_date(m["comp"]),
        "HERO_NUM": f"{m['rate_now']:.1f}",
        "HERO_ARROW": "&#9650;" if chg >= 0 else "&#9660;",
        "HERO_CHG": f"{abs(chg):.1f}",
        "HERO_FOOT": f"{m['now']['fs_red']:,} of {m['now']['fs_ready']:,} shoes &middot; {html.escape(panel_names)} &middot; {len(m['brands'])} brands",
        "DEPTH_NOW": f"{m['P'][(m['latest'], 'retailer_first_seen')][0]:.1f}",
        "DEPTH_PREV": f"{m['P'][(m['comp'], 'retailer_first_seen')][0]:.1f}",
        "ADV_NOW": f"{m['adv_now']:.1f}",
        "ADV_MED": f"{m['P'][(m['latest'], 'retailer_advertised')][0]:.0f}",
        "P90_NOW": f"{m['P'][(m['latest'], 'retailer_first_seen')][1]:.0f}",
        "P90_PREV": f"{m['P'][(m['comp'], 'retailer_first_seen')][1]:.0f}",
        "COVERAGE": f"{m['coverage']:,}",
        "COVERAGE_FOOT": f"across {m['n_verified']} retailers &middot; not part of the rate",
        "EVENT_PTS": str(EVENT_PTS),
        "PANEL_MIN": str(PANEL_MIN),
        "PANEL_WIN": str(PANEL_WIN),
        "RETAILERS_HTML": "\n".join(retailer_html(r, m, {1: 12, 2: 6, 3: 4}.get(len(m["retailers"]), 3)) for r in m["retailers"]),
        "RETAILER_FOOT": " ".join(html.escape(p) for p in foot_parts),
        "RETAILER_ROWS_HTML": "\n".join(retailer_row_html(r, m) for r in m["retailers"] if r["in_rate"]),
        "RETAILER_CAPTION": retailer_caption(m),
        "DATA_JSON": json.dumps({"daily": m["series"], "brands": m["brands"], "annotation": m["annotation"],
                                 "event_pts": EVENT_PTS}, separators=(",", ":")),
    }
    out = template_text
    for k, v in vals.items():
        out = out.replace("{{" + k + "}}", v)
    left = re.findall(r"\{\{[A-Z_]+\}\}", out)
    if left:
        raise BuildError(f"unfilled placeholders: {sorted(set(left))}")
    return out


# ---------------------------------------------------------------- deploy
def deploy_netlify(site_dir):
    candidate = Path(site_dir) / "index.html"
    with urllib.request.urlopen("https://tracklane.co.uk/", timeout=30) as response:
        current = response.read().decode("utf-8")
    check_update(current, candidate.read_text(encoding="utf-8"), dt.datetime.now(dt.timezone.utc).date())
    return upload_homepage(candidate)


def upload_homepage(candidate):
    token, site = os.environ.get("NETLIFY_AUTH_TOKEN"), os.environ.get("NETLIFY_SITE_ID")
    if not token or not site:
        raise BuildError("NETLIFY_AUTH_TOKEN and NETLIFY_SITE_ID must be set to deploy")
    # Resolve the configured project before posting a production deploy.
    from uuid import UUID
    site = str(UUID(site))
    lookup = urllib.request.Request(f"https://api.netlify.com/api/v1/sites/{site}",
                                    headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(lookup, timeout=30) as response:
        if json.load(response).get("custom_domain") != "tracklane.co.uk":
            raise BuildError("Netlify project does not serve tracklane.co.uk")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(candidate, "index.html")
        z.writestr("_headers", "/*\n  Content-Type: text/html; charset=utf-8\n")
    req = urllib.request.Request(f"https://api.netlify.com/api/v1/sites/{site}/deploys", data=buf.getvalue(),
                                 method="POST", headers={"Content-Type": "application/zip",
                                                         "Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        info = json.loads(resp.read())
    return info.get("id"), info.get("state"), info.get("deploy_ssl_url") or info.get("ssl_url")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=str(HERE / "site"), help="folder to write index.html into (the Netlify folder)")
    ap.add_argument("--offline", help="folder with daily.json, brands.json, pcts.json instead of querying BigQuery")
    ap.add_argument("--expected-panel", help="hold before rendering if the published retailer list changed")
    ap.add_argument("--print-sql", choices=["daily", "followups"], help="print the SQL needed for --offline and exit")
    ap.add_argument("--deploy", action="store_true", help="upload --out to Netlify after a successful build")
    ap.add_argument("--today", help="override today's date (YYYY-MM-DD) for the staleness check")
    a = ap.parse_args()

    if a.print_sql == "daily":
        print(SQL_DAILY); return 0

    if a.offline:
        days = parse_daily(load_json(Path(a.offline) / "daily.json"))
    else:
        days = parse_daily(run_bq(SQL_DAILY))
    latest, comp, panel, _ = choose(days)
    if a.expected_panel:
        if latest != dt.datetime.now(dt.timezone.utc).date().isoformat():
            raise BuildError("The latest chart observation is not today's date")
        if join_names(sorted(RETAILER_NAMES[s] for s in panel)) != a.expected_panel:
            raise BuildError("The headline retailer list changed")
    params = {"dates": [latest, comp], "sources": panel}

    if a.print_sql == "followups":
        print("-- brands.json\n" + literal_sql(SQL_BRANDS, params) + ";\n\n-- pcts.json\n" + literal_sql(SQL_PCTS, params) + ";")
        return 0

    if a.offline:
        brand_rows = load_json(Path(a.offline) / "brands.json")
        pct_rows = load_json(Path(a.offline) / "pcts.json")
    else:
        brand_rows, pct_rows = run_bq(SQL_BRANDS, params), run_bq(SQL_PCTS, params)

    today = dt.date.fromisoformat(a.today) if a.today else None
    m = build_model(days, brand_rows, pct_rows, today=today)
    if m["errors"]:
        print("BUILD REFUSED (live site left unchanged):", *m["errors"], sep="\n  - ", file=sys.stderr)
        return 1
    page = render(m, TEMPLATE.read_text(encoding="utf-8"))

    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    tmp = out / "index.html.tmp"
    tmp.write_text(page, encoding="utf-8")
    tmp.replace(out / "index.html")
    print(f"Built {out / 'index.html'}: data to {m['latest']} vs {m['comp']}, rate {m['rate_now']:.1f}% "
          f"({m['rate_now'] - m['rate_prev']:+.1f} pts), panel {', '.join(RETAILER_NAMES.get(s, s) for s in m['panel'])}, "
          f"{len(m['brands'])} brands, {len(m['retailers'])} retailer cards")

    if a.deploy:
        dep_id, state, url = deploy_netlify(out)
        print(f"Deployed to Netlify: id={dep_id} state={state} url={url}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BuildError as e:
        print(f"BUILD REFUSED (live site left unchanged): {e}", file=sys.stderr)
        sys.exit(1)
