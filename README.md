# consumer-goods-price-pipeline

A consumer-goods price data platform: free/public-source pipelines feed a
partitioned Parquet store, deduplicated into curated tables queryable through
a DuckDB layer. Covers everything from groceries (eggs, milk, avocados) to
energy at the pump, durable goods, tires, used cars, and household goods.

```
*_pipeline.py -> storage/raw (Parquet, Hive-partitioned)
             -> curated.py dedup -> storage/curated (one file per table)
                  -> query.py (DuckDB CATALOG)
                       -> analytics (future: price indices, cross-source blends)
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how the layers fit
together and [docs/PIPELINE_CATALOG.md](docs/PIPELINE_CATALOG.md) for what
every pipeline pulls and which table it lands in.
[docs/SOURCES.md](docs/SOURCES.md) is the researched index of every free data
source considered (coverage, key requirements, update cadence).

## What's in here

- **26 registered tables** in the query layer CATALOG across US government
  statistics (BLS CPI/PPI and average prices, USDA AMS wholesale/retail and
  NASS farm prices, EIA energy, FRED consumer series), crowdsourced grocery
  (Open Food Facts), and a planned retail/e-commerce tier (Kroger, Walmart,
  eBay) plus international stats (Eurostat, OECD, StatCan, FAO, World Bank,
  IMF).
- **Keyless-or-free-key design**: pipelines run keyless where the source
  allows it (BLS v1 fallback, Open Food Facts) and skip cleanly when a
  configured key is absent (`run_all.py` prints a SKIP reason).
- **Natural-key dedup** (`curated.py`) so re-fetches converge instead of
  stacking duplicate rows.
- **Schema/null/range validation** (`validate.py`) wired into every run.
- **Fallback sample data**: Pipelines such as Eurostat HICP, OpenFoodFacts, FRED CPI, and APINinjas Inflation are designed to fall back to using static sample data if the network is unavailable, an API limit is hit, or an API key is missing. This ensures the pipeline executes successfully and produces valid parquet files for downstream processes.

## Setup

Requires Python 3.10+.

```
pip install -r requirements.txt
```

1. Copy `.env.example` to `.env` and fill in the free API keys for the
   sources you want. Every key is free; most pipelines work with no key at
   all. See [docs/PIPELINE_CATALOG.md](docs/PIPELINE_CATALOG.md) for which
   key each pipeline needs.
2. Run the test suite to confirm your environment works:
   ```
   python -m pytest tests/ -v
   ```
3. See what a full run would do without executing anything:
   ```
   python run_all.py --dry-run
   ```

## Running it

```
python run_all.py                 # incremental run, all stages
python run_all.py --backfill      # full available history where supported
python run_all.py --stage 1       # government/keyless sources first
python run_all.py --only bls_cpi,openfoodfacts
python validate.py                # data health check (schema/null/range checks)
python curated.py                 # rebuild deduped curated snapshots
```

`run_all.py` rebuilds curated automatically after each run. If you run a
pipeline script directly instead, run `curated.py` afterward — otherwise
`query.py` and everything downstream reads stale/duplicated data.

## Querying the data

```python
import query as q

df = q.load("bls_avg_prices", item="Eggs, grade A, large")
df = q.load("eia_gas_retail", area="NUS")
q.tables()          # every table with row counts
q.date_range()      # min/max dates across the store
q.schema("usda_ams_wholesale")
```

## Testing

```
python -m pytest tests -v
```

78 tests cover Hive partitioning and read-back, natural-key dedup semantics,
validation severity/schema coverage, and guard tests that fail if a new
pipeline isn't registered in every layer it needs to be (CATALOG, SCHEMAS,
KEYS, `run_all.py`).

## License

MIT — see [LICENSE](LICENSE).
