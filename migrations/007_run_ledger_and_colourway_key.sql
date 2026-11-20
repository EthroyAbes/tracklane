-- Migration 007 — run ledger, colourway_key on snapshots, size-system label.
--
-- Wholly additive. Nothing existing is dropped, altered or rewritten: every
-- statement below either creates a new table/index or adds a new nullable
-- column. Safe to run against the live PythonAnywhere database as-is, and
-- safe to run before the matching code is deployed (the new columns simply
-- stay NULL until a scraper that writes them runs).
--
-- Three unrelated-looking changes ship together because all three are
-- prerequisites for the same thing — being able to say, of any published
-- figure, which run produced it and whether that run was complete.
--
-- 1. snapshots.colourway_key
--    on_scraper.py and altra_scraper.py both already compute a stable
--    colourway key ("on:<objectID>", "altra:<product_id>:<colour>") and
--    write it to size_observation, but the snapshot rows they write
--    alongside do not carry it. The join between the price fact and the
--    availability fact has therefore been on (model, colourway, run_date)
--    by coincidence rather than on an identifier. This adds the column so
--    the join is explicit. It is NOT an identity table and NOT a foreign
--    key — Panel Spec §5's colourway entity model is deliberately deferred
--    (see DECISIONS.md 2026-09-10). This is the smaller first step.
--
-- 2. ingest_run
--    A run-outcome ledger. Until now a scraper that failed part-way could
--    write a partial set of rows that looked exactly like a small but
--    successful day: nothing recorded that the run aborted. Row-count
--    checks cannot distinguish "the retailer delisted half its range" from
--    "page 2 of 4 timed out". This table records the distinction at write
--    time, which is the only time it is knowable.
--
--    status values:
--      'running'   — started, not yet finished (a row left in this state is
--                    itself a finding: the process died without recording an
--                    outcome)
--      'complete'  — the source was enumerated in full and rows were written
--      'partial'   — some units failed but real rows were written. These rows
--                    ARE used: a Google Shopping run that lost 2 of 36 query
--                    pages still observed 34 pages of real prices, and throwing
--                    them away would lose good data and hole the series. What
--                    must not happen is the day reading as complete.
--      'failed'    — the fetch or write aborted, or what it wrote cannot be
--                    trusted; rows from this run must not enter a published
--                    figure
--    Readers exclude 'running' and 'failed'. A failed run's rows (if any were
--    written before the abort) stay in the database as diagnostic evidence
--    rather than being deleted — deleting them would destroy the record of
--    what went wrong.
--
--    units_expected records how many units the run intended to fetch (Google:
--    brands x pages; HOKA: category URLs), so a partial run's magnitude is
--    visible without re-deriving it from the failure text.
--
-- 3. size_observation.size_system
--    on_scraper.py reads Algolia's productsAvailableSizes, which is keyed by
--    size scale (the key is the scale, the value is that scale's size list),
--    and flattens it with .values() — discarding the scale label. If a size
--    string such as "10" appears under two scales it is written twice, as two
--    indistinguishable rows. This column lets the label be retained. Existing
--    rows are left NULL: they were written without the label and it cannot be
--    reconstructed after the fact. NULL therefore means "scale not recorded",
--    which is honest, and is distinguishable from any real scale value.
--    No backfill is attempted. See DECISIONS.md 2026-09-10.

ALTER TABLE snapshots ADD COLUMN colourway_key TEXT;
CREATE INDEX IF NOT EXISTS idx_snapshots_colourway_key
    ON snapshots(colourway_key, run_date);

ALTER TABLE size_observation ADD COLUMN size_system TEXT;

CREATE TABLE IF NOT EXISTS ingest_run (
    run_id          TEXT PRIMARY KEY,
    source          TEXT NOT NULL,
    run_date        TEXT NOT NULL,
    status          TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    rows_written    INTEGER,
    units_fetched   INTEGER,
    units_expected  INTEGER,
    failure_stage   TEXT,
    failure_detail  TEXT
);
CREATE INDEX IF NOT EXISTS idx_ingest_run_source_date
    ON ingest_run(source, run_date, status);
