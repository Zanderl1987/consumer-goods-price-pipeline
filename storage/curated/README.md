---
language:
  - en
tags:
  - economics
  - consumer-prices
  - retail
  - inflation
  - alternative-data
  - pipeline
task_categories:
  - other
size_categories:
  - 10MB<n<100MB
---

# Consumer Goods Price Pipeline — Full Curated Snapshot

A dataset of consumer-facing retail prices, government price indexes, and commodity
prices covering **29 tables** and **1,688,630 rows**.

## Data Sources

| Category | Tables | Key Sources |
|---|---|---|
| Retail Prices | 7 | Kroger, Open Food Facts, USDA AMS, Statistics Canada, CMS drug pricing |
| Government Price Indexes | 7 | BLS CPI/PPI/Average Price, FRED, Eurostat HICP, OECD CPI |
| Commodity Prices | 11 | FAO, World Bank Pink Sheet, IMF, WFP, USDA prices paid/received |
| Energy Prices | 4 | EIA electricity/gas/natural gas |

## Usage

```python
from datasets import load_dataset

# Load entire dataset
ds = load_dataset("ZanderL1337/consumer-goods-price-pipeline", trust_remote_code=True)

# Load specific table
df = ds["bls_avg_prices"].to_pandas()
```

Or load individual parquet files directly:

```python
import pandas as pd

df = pd.read_parquet("path/to/parquet/file.parquet")
```

## Schema

Each table is stored as a separate parquet file. Key columns:

- `date`: Temporal column (monthly or as published by the source)
- `item` / `series_id`: Commodity or price-series identifier
- `price` / `value`: The observed price or index value
- `fetched_at`: UTC timestamp when data was fetched

## Engineering & data quality

- **Schema/null-rate/range validation** (`validate.py`) runs as an operational health
  check against every table after each pipeline run.
- **Raw vs. curated separation**: pipelines write Hive-partitioned raw Parquet, which
  can contain overlapping re-fetches; a dedup step (`curated.py`) produces the
  deduplicated tables published here.
- **BLS Average Price series verified against BLS's authoritative item catalog**
  (download.bls.gov/pub/time.series/ap/ap.item) as of 2026-08-24, after discovering
  the original series-ID list was systematically mismatched against its labels.

Full source, tests, and architecture docs: https://github.com/Zanderl1987/consumer-goods-price-pipeline

## Build Info

- **Generated**: 2026-08-26
- **Pipeline**: consumer-goods-price-pipeline (https://github.com/Zanderl1987/consumer-goods-price-pipeline)
- **Tables**: 29
- **Total Rows**: 1,688,630
- **Total Size**: 18.8 MB

## License

CC BY 4.0 — data sourced from public APIs and government databases.
