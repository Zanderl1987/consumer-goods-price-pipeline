# Architecture

Mirrors `financial-data-pipeline` (see that repo's `docs/ARCHITECTURE.md`
for the original rationale; this is the consumer-goods adaptation).

## Data flow

```
*_pipeline.py ──► storage/raw/<table>/year=YYYY/month=MM/*.parquet
                        │  (write_partitioned: append-only, Hive-partitioned)
                        ▼
                 curated.py ──► storage/curated/<table>/<table>.parquet
                        │  (natural-key dedup → one latest-value file per table)
                        ▼
                 query.py (DuckDB CATALOG: 26 views over curated, falling
                        back to raw globs when no curated snapshot exists)
                        ▼
                 validate.py + run_all.py (health checks, orchestration)
```

## Layers

### 1. Pipelines (`*_pipeline.py`, repo root)

Each is a standalone script with a `main()` and a `--backfill` flag. A
pipeline's job: authenticate (if keyed), fetch from the source API/feed,
normalize to the table's schema, and call `storage_utils.write_partitioned`.
Pipelines never read the store and never modify raw files.

- **Keyless first**: BLS falls back to keyless v1 (lower request limits),
  Open Food Facts is keyless, EIA/USDA/FRED require a free key. Missing keys
  are handled by `run_all.py` SKIP, not a crash.
- **Incremental by default**: fetch recent window; `--backfill` for full
  history where the source supports it.

### 2. Raw store (`storage/raw/`)

Append-only Parquet, Hive-partitioned by `year=`/`month=` from the frame's
MAX `fetched_at` (or MAX `date` if present). Partitioning by MAX is a
simplification for small tables: a fetch spanning two months lands in the
later month's directory. Fine at this scale; the tradeoff is documented in
the financial repo if it ever matters.

### 3. Curated (`storage/curated/`)

`curated.dedup` applies each table's natural key from `curated.KEYS` (e.g.
`bls_cpi` → `[series_id, date]`), keeps the newest `fetched_at` per key, and
writes one Parquet per table. Unknown tables get full-row dedup. `run_all.py`
compacts curated after every run.

### 4. Query layer (`query.py`)

`CATALOG` maps table name → glob under `storage/raw/`; `_register_views()`
creates DuckDB views. When a curated snapshot exists, the view reads it;
otherwise it reads the raw glob. Public API: `load`, `sql`, `schema`,
`tables`, `date_range`, `reload`.

### 5. Validation (`validate.py`) and orchestration (`run_all.py`)

`validate.py` holds the canonical `SCHEMAS` dict (column type/required/null
specs) plus checks: not-empty, required columns, null rates, future dates,
value ranges (WARNING by default), row counts, fetched-at recency. `validate_all`
runs the store health check. `run_all.py` has the `PIPELINES` registry
(`PipelineSpec(name, file, stage, tables, requires_env)`) and stages:
stage 1 = government/keyless, stage 2 = retail/e-commerce, stage 3 =
international.

## Point-in-time discipline

The store keeps `fetched_at` on every row. Series like CPI and AMS wholesale
are revised/restated by their sources; a re-fetch with `--backfill` will
overwrite history in the raw store (append of corrected partitions) and
`curated.py` keeps the newest fetch per natural key. Any future analytics
that joins across tables should join on **publication/effective date**, not
fetch date. See the financial repo's PIT section before building analytics.

## Status

- Stage 1 seed pipelines written and unit-tested (no live keys configured
  yet): `bls_cpi`, `bls_avg_prices`, `usda_ams` (MARS v1.2 API verified live),
  `usda_nass_prices`, `eia_energy`, `fred_consumer`.
- Stage 2: `openfoodfacts` (keyless) written.
- Stage 3 and retail tier (Kroger/Walmart/eBay) planned; see
  `docs/PIPELINE_CATALOG.md` and `docs/SOURCES.md`.
