# CLAUDE.md


## Session notes and task list live in a separate repo

Session notes and the task list for this project are NOT in this repo. They live in the
private `work-notes` repo, cloned as a sibling, at `work-notes/consumer-goods-price-pipeline/`:

    C:\Users\zande\PycharmProjects\work-notes\consumer-goods-price-pipeline\

When Zander asks to update session notes or the task list, edit the files there, not here.
This repo keeps only durable documentation (this file, `docs/`), so it can be public
without a visitor scrolling through a working log. See `work-notes/CLAUDE.md` for the
convention.

## What this is

A consumer-goods price data pipeline mirroring
`financial-data-pipeline`'s architecture: flat `*_pipeline.py` scripts at
repo root that fetch from free public sources and write Hive-partitioned
Parquet to `storage/raw/<table>/year=/month=/`, which `curated.py` dedups
into one file per table under `storage/curated/`, exposed as DuckDB views by
`query.py`'s CATALOG. `run_all.py` orchestrates stages; `validate.py` checks
schema/null/range health; tests live in `tests/`.

## Commands

```
C:\ProgramData\anaconda3\python.exe -m pytest tests -v   # full test suite
C:\ProgramData\anaconda3\python.exe run_all.py --dry-run # what a run would do
C:\ProgramData\anaconda3\python.exe run_all.py           # incremental run
C:\ProgramData\anaconda3\python.exe validate.py          # data health check
C:\ProgramData\anaconda3\python.exe curated.py           # rebuild curated
C:\ProgramData\anaconda3\python.exe query.py             # store summary
```

Always use the full path to Anaconda python. Bare `python` on this machine is
a broken MS Store stub. Do not edit `.env` with secret values — keys stay in
`storage/logs`-adjacent local `.env` and are gitignored; commit only to
`.env.example`.

## New pipeline wiring checklist

Adding a pipeline means touching FIVE places or tests fail (the guard tests
enforce this):

1. Write `mypipeline_pipeline.py` (fetch, normalize, `write_partitioned`).
2. Add the table + glob path to `query.py` CATALOG (paths under
   `storage/raw/<table>/`, end in `.parquet`).
3. Add a schema + checks to `validate.py` SCHEMAS (and a range/row-count
   check where sensible).
4. Add a natural key to `curated.py` KEYS so re-fetches dedup instead of
   stacking rows.
5. Register a `PipelineSpec` in `run_all.py` PIPELINES (name, file, stage,
   tables, requires_env — list the exact env var(s), empty list if keyless).
6. Update `tests/test_catalog.py` EXPECTED_TABLES.
7. Update `docs/PIPELINE_CATALOG.md` and `docs/SOURCES.md`.

## Conventions

- No comments in code unless the "why" is non-obvious (this CLAUDE.md is the
  place for the "why").
- Console output ASCII-only — no unicode box-drawing or em-dashes. This
  bit us in `query.py`'s docstring (an embedded `"""` example terminated the
  module docstring early and a non-ASCII char rendered as U+FFFD in a
  comment). Keep file encoding UTF-8, but avoid fancy punctuation in code.
- Pipelines are idempotent: re-running appends new partitions and `curated.py`
  dedups by natural key. No deletes, no in-place edits of raw files.
- `fetched_at` column: ISO-8601 UTC string, set by each pipeline at fetch
  time; `write_partitioned` partitions by the MAX fetched_at (month) unless
  the frame carries a `date` column (then it partitions by MAX date).
- Keep the docstring of every pipeline up to date with: what it fetches,
  where it writes, which env keys it needs, known gaps.
- `requires_env` is the source of truth for keyless-vs-keyed; `run_all.py`
  SKIPs pipelines whose env vars are missing so a run never half-fails.

## Test layout

- `tests/test_catalog.py` — CATALOG/glob/path guards + discovery helpers.
- `tests/test_pipelines.py` — run_all registry consistency + per-pipeline
  smoke (file exists, imports, has `main`).
- `tests/test_storage.py` — Hive partitioning + read-back.
- `tests/test_curated.py` — natural-key dedup semantics.
- `tests/test_validation.py` — check severity + schema coverage.
- `tests/test_logging.py` — logger + failure-log helpers.

## Gotchas

- `year` and `month` are reserved column names — `write_partitioned` creates
  a Hive `year=YYYY/month=MM/` partition, and `query.py` reads tables back
  with `hive_partitioning=True`, so a domain column literally named `year`
  or `month` gets silently overwritten by the partition value on read-back
  (bit us in `cms_drug_pricing_pipeline.py` on 2026-08-04 — a real "spending year"
  2020-2024 column read back as a constant 2026 for every row, which also
  silently wrecked curated.py's dedup). Name domain year/month columns
  something else (e.g. `spending_year`, `fiscal_year`).

- `query.py`'s module docstring must not contain a literal `"""` — it
  terminates the docstring early (broke `query.py` on 2026-08-03; fixed by
  rewriting the SQL example with string concatenation).
- `validate.py` defaults `value_ranges` to a WARNING so new tables don't fail
  the run while you're still calibrating ranges — set `value_ranges_error: True`
  in the table's SCHEMAS entry only when the range is proven over the historical
  spread (fews_net_food_prices does this: full 1995+ backfill max is 168M, bound
  (0, 500M)).
- USDA AMS (`usda_ams_pipeline.py`): base URL was updated to MARS v1.2
  (`https://marsapi.ams.usda.gov/services/v1.2`, HTTP Basic auth, 180-day
  request windows) after live verification on 2026-08-03. The FVWV wholesale
  terminal report slug is still pending a live check with a real key.
- Keep `storage/**` gitignored (data files, logs); only the empty directory
  skeleton is committed.
