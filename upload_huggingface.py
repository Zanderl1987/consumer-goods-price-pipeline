#!/usr/bin/env python3
"""
Upload the full curated consumer-goods price dataset to HuggingFace.

Usage:
    python upload_huggingface.py [--repo-name consumer-goods-price-pipeline] [--private]

Requires HUGGINGFACE_TOKEN or HF_TOKEN env variable.

Ported 2026-08-24 from financial-data-pipeline/upload_huggingface.py, including
its per-table row-count regression guard: `upload_folder` is last-writer-wins
per file with no pruning, so a stale local snapshot can silently overwrite
fresher remote data while the aggregate row count still looks fine (see that
repo's feedback_hf_publish_hazards memory for the incident that motivated it).
"""

import os
import argparse
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

from huggingface_hub import HfApi, HfFileSystem, login

STORAGE_ROOT = Path(__file__).parent / "storage" / "curated"
README_TEMPLATE = """---
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
prices covering **{n_tables} tables** and **{n_rows:,} rows**.

## Data Sources

| Category | Tables | Key Sources |
|---|---|---|
| Retail Prices | {n_retail} | Kroger, Open Food Facts, USDA AMS, Statistics Canada, CMS drug pricing |
| Government Price Indexes | {n_index} | BLS CPI/PPI/Average Price, FRED, Eurostat HICP, OECD CPI |
| Commodity Prices | {n_commodity} | FAO, World Bank Pink Sheet, IMF, WFP, USDA prices paid/received |
| Energy Prices | {n_energy} | EIA electricity/gas/natural gas |

## Usage

```python
from datasets import load_dataset

# Load entire dataset
ds = load_dataset("{repo_id}", trust_remote_code=True)

# Load specific table
df = ds["{first_table}"].to_pandas()
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

- **Generated**: {generated_date}
- **Pipeline**: consumer-goods-price-pipeline (https://github.com/Zanderl1987/consumer-goods-price-pipeline)
- **Tables**: {n_tables}
- **Total Rows**: {n_rows:,}
- **Total Size**: {total_size_mb:.1f} MB

## License

CC BY 4.0 — data sourced from public APIs and government databases.
"""


def count_rows(parquet_path: Path) -> int:
    """Count rows in a parquet file without loading full DataFrame."""
    import pyarrow.parquet as pq
    return pq.read_metadata(str(parquet_path)).num_rows


def remote_row_counts(repo_id: str, token: str) -> dict:
    """
    Row counts for every table currently published on HF, read from each
    parquet file's footer over HTTP range requests (no full download).
    Returns {} if the repo has no parquet files yet (first publish).
    """
    import pyarrow.parquet as pq

    fs = HfFileSystem(token=token)
    counts = {}
    try:
        remote_files = fs.glob(f"datasets/{repo_id}/**/*.parquet")
    except Exception:
        return counts
    for rf in remote_files:
        name = Path(rf).stem
        try:
            with fs.open(rf, "rb") as f:
                counts[name] = pq.read_metadata(f).num_rows
        except Exception as e:
            print(f"  WARN: could not read remote row count for {name}: {e}")
    return counts


def main(repo_name: str = "consumer-goods-price-pipeline", private: bool = False, force: bool = False):
    token = os.environ.get("HUGGINGFACE_TOKEN") or os.environ.get("HF_TOKEN")
    if not token:
        print("ERROR: Set HUGGINGFACE_TOKEN or HF_TOKEN env variable.")
        return

    parquet_files = sorted(STORAGE_ROOT.glob("**/*.parquet"))
    print(f"Found {len(parquet_files)} parquet files")

    if not parquet_files:
        print(f"ERROR: No parquet files found under {STORAGE_ROOT} -- "
              f"refusing to publish an empty snapshot.")
        return

    login(token=token)
    api = HfApi()

    repo_id = f"ZanderL1337/{repo_name}"
    print(f"Creating/updating repo: {repo_id} (private={private})")

    api.create_repo(repo_id, repo_type="dataset", private=private, exist_ok=True)

    # create_repo's `private` only applies when it actually creates the repo --
    # with exist_ok=True it silently no-ops on an existing one. Enforce the
    # requested visibility BEFORE any data is uploaded.
    current = api.dataset_info(repo_id).private
    if current != private:
        print(f"  repo already existed with private={current}; setting private={private}")
        api.update_repo_settings(repo_id=repo_id, repo_type="dataset", private=private)

    total_rows = 0
    table_stats = []
    categories = {"retail": 0, "index": 0, "commodity": 0, "energy": 0}

    retail_prefixes = ("kroger_", "openfoodfacts_", "usda_ams_", "statcan_", "cms_drug_")
    index_prefixes = ("bls_cpi", "bls_ppi", "bls_avg_prices", "fred_cpi", "fred_consumer_prices",
                       "eurostat_hicp", "oecd_cpi", "apininja_")
    commodity_prefixes = ("fao_", "worldbank_", "imf_", "wfp_", "usda_prices_", "fred_used_cars",
                           "noaa_seafood_")
    energy_prefixes = ("eia_",)

    for pf in parquet_files:
        name = pf.stem
        rows = count_rows(pf)
        total_rows += rows
        table_stats.append((name, rows, pf.stat().st_size))

        if any(name.startswith(p) for p in retail_prefixes):
            categories["retail"] += 1
        elif any(name.startswith(p) for p in index_prefixes):
            categories["index"] += 1
        elif any(name.startswith(p) for p in commodity_prefixes):
            categories["commodity"] += 1
        elif any(name.startswith(p) for p in energy_prefixes):
            categories["energy"] += 1
        else:
            categories["commodity"] += 1  # default bucket

    total_size_mb = sum(s[2] for s in table_stats) / 1024 / 1024

    print(f"\n{len(parquet_files)} tables, {total_rows:,} rows, {total_size_mb:.1f} MB")
    print(f"  Retail: {categories['retail']}, Index: {categories['index']}, "
          f"Commodity: {categories['commodity']}, Energy: {categories['energy']}")

    # Per-table regression guard -- see remote_row_counts() docstring.
    print("\nChecking remote row counts for regressions...")
    remote_counts = remote_row_counts(repo_id, token)
    if not remote_counts:
        print("  (first publish or repo has no parquet files yet -- nothing to compare)")
    else:
        local_counts = {name: rows for name, rows, _ in table_stats}
        regressions = []
        for name, remote_n in remote_counts.items():
            local_n = local_counts.get(name)
            if local_n is None:
                regressions.append((name, remote_n, 0))
            elif local_n < remote_n:
                regressions.append((name, remote_n, local_n))
        if regressions:
            print(f"  REGRESSION: {len(regressions)} table(s) would lose rows:")
            for name, remote_n, local_n in regressions:
                pct = (local_n / remote_n - 1) * 100 if remote_n else 0
                print(f"    {name}: {remote_n:,} -> {local_n:,} ({pct:+.1f}%)")
            if not force:
                print("\nABORTING publish -- pass --force to publish anyway "
                      "(only if this regression is expected, e.g. a source "
                      "removed data upstream).")
                return None
            print("  --force set: publishing despite regression(s).")
        else:
            print(f"  OK: no table regressed vs. the current remote revision "
                  f"({len(remote_counts)} tables compared).")

    readme = README_TEMPLATE.format(
        repo_id=repo_id,
        n_tables=len(parquet_files),
        n_rows=total_rows,
        total_size_mb=total_size_mb,
        generated_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        first_table=table_stats[0][0] if table_stats else "bls_avg_prices",
        n_retail=categories["retail"],
        n_index=categories["index"],
        n_commodity=categories["commodity"],
        n_energy=categories["energy"],
    )

    readme_path = STORAGE_ROOT / "README.md"
    readme_path.write_text(readme, encoding="utf-8")

    print(f"\nUploading to {repo_id}...")
    api.upload_folder(
        folder_path=str(STORAGE_ROOT),
        repo_id=repo_id,
        repo_type="dataset",
        allow_patterns=["*.parquet", "README.md"],
        commit_message=f"Update curated snapshot ({len(parquet_files)} tables, {total_rows:,} rows)",
    )

    print(f"\nDone! Dataset: https://huggingface.co/datasets/{repo_id}")
    print(f"  Load with: ds = load_dataset('{repo_id}')")

    return {
        "repo_id": repo_id,
        "tables": len(parquet_files),
        "rows": total_rows,
        "size_mb": total_size_mb,
        "files": [
            str(pf.relative_to(STORAGE_ROOT)).replace(os.sep, "/")
            for pf in parquet_files
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload curated data to HuggingFace")
    parser.add_argument("--repo-name", default="consumer-goods-price-pipeline", help="HF repo name")
    parser.add_argument("--private", action="store_true", help="Make dataset private")
    parser.add_argument("--force", action="store_true",
                         help="Publish even if a table's row count would regress vs. the remote revision")
    args = parser.parse_args()
    main(repo_name=args.repo_name, private=args.private, force=args.force)
