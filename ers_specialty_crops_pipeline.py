#!/usr/bin/env python3
"""
USDA ERS Specialty Crops Pipeline — Fruit & Tree Nuts + Vegetables & Pulses.

Downloads monthly CSVs from USDA Economic Research Service covering:
  - CPI/PPI indexes and average retail prices for specialty crops
  - Imports/exports by partner country (opt-in via --with-trade)

Data is organized into 4 tables:
  ers_fruit_nut_prices   — CPI/PPI + retail prices for fruit & tree nuts
  ers_veg_prices         — CPI/PPI + retail prices for vegetables & pulses
  ers_fruit_nut_trade    — imports/exports by partner country (--with-trade)
  ers_veg_trade          — imports/exports by partner country (--with-trade)

Source URLs (monthly CSVs, updated ~18th of each month):
  Fruit price index:  https://www.ers.usda.gov/media/6476/price-index-data.csv
  Veg price index:    https://www.ers.usda.gov/media/5629/price-index-data.csv
  Fruit trade:        https://www.ers.usda.gov/media/6475/fruit-and-tree-nuts-trade-data.csv
  Veg trade:          https://www.ers.usda.gov/media/5628/vegetables-and-dry-pulses-trade-data.csv

NOTE: media/6476 and media/5629 served BYTE-IDENTICAL price-index files on
2026-08-24 (same SHA256, both listed as 2.71 MB). The file itself covers BOTH
fruit and vegetable series. Both tables will land with identical content until
ERS splits them again. Documented in docs/SOURCES.md.

No API key required.

CLI:
  python ers_specialty_crops_pipeline.py                 # prices only
  python ers_specialty_crops_pipeline.py --with-trade    # prices + trade tables
  python ers_specialty_crops_pipeline.py --backfill      # full history
"""

import argparse
import datetime
import os
import time

import pandas as pd
import requests
from storage_utils import write_partitioned

FRUIT_PRICE_URL = "https://www.ers.usda.gov/media/6476/price-index-data.csv"
VEG_PRICE_URL = "https://www.ers.usda.gov/media/5629/price-index-data.csv"
FRUIT_TRADE_URL = "https://www.ers.usda.gov/media/6475/fruit-and-tree-nuts-trade-data.csv"
VEG_TRADE_URL = "https://www.ers.usda.gov/media/5628/vegetables-and-dry-pulses-trade-data.csv"

MAX_RETRIES = 3
BACKOFF_SECONDS = 30
REQUEST_TIMEOUT = 120
HEADERS = {"User-Agent": "Mozilla/5.0 consumer-goods-price-pipeline/1.0"}


def _get_csv(url: str) -> pd.DataFrame:
    """Download a CSV from ERS with retry/backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and r.content:
                from io import StringIO
                return pd.read_csv(StringIO(r.text), encoding="utf-8", low_memory=False)
            if r.status_code == 429:
                time.sleep(BACKOFF_SECONDS * attempt)
            else:
                print(f"    HTTP {r.status_code} for {url}")
                return pd.DataFrame()
        except requests.RequestException as e:
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS)
            else:
                print(f"    Request error: {e}")
    return pd.DataFrame()


def _parse_price_index(url: str, source_label: str) -> pd.DataFrame:
    """Parse an ERS price-index CSV into a tidy long-format DataFrame."""
    raw = _get_csv(url)
    if raw.empty:
        return pd.DataFrame()

    # ERS price-index CSVs have a complex structure:
    # First column is "Category" (series type), subsequent columns are
    # date-labeled (e.g. "Jan 2020", "Feb 2020"). Melt to long format.
    if "Category" not in raw.columns:
        # Try to find the actual first column name
        first_col = raw.columns[0]
        if "category" in str(first_col).lower():
            raw = raw.rename(columns={first_col: "Category"})
        else:
            print(f"    Unexpected columns: {list(raw.columns[:5])}")
            return pd.DataFrame()

    # All columns after "Category" are date columns
    date_cols = [c for c in raw.columns if c != "Category"]

    if not date_cols:
        print(f"    No date columns found in {source_label}")
        return pd.DataFrame()

    # Melt: Category stays as identifier, date columns become rows
    melted = raw.melt(
        id_vars=["Category"],
        value_vars=date_cols,
        var_name="date_str",
        value_name="value",
    )

    # Parse dates
    melted["date"] = pd.to_datetime(melted["date_str"], format="%b %Y", errors="coerce")
    melted = melted.dropna(subset=["date", "value"])

    # Numeric value
    melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
    melted = melted.dropna(subset=["value"])

    # Build series_id from Category
    melted["series_id"] = melted["Category"].str.strip().str.lower().str.replace(
        r"[^a-z0-9]+", "_", regex=True
    ).str.strip("_")

    result = melted[["series_id", "Category", "date", "value"]].copy()
    result = result.rename(columns={"Category": "series_name"})
    result["source"] = source_label
    result["fetched_at"] = datetime.datetime.utcnow().isoformat()

    return result.drop_duplicates(subset=["series_id", "date"]).reset_index(drop=True)


def _parse_trade(url: str, source_label: str) -> pd.DataFrame:
    """Parse an ERS trade CSV. These are simpler tabular CSVs."""
    raw = _get_csv(url)
    if raw.empty:
        return pd.DataFrame()

    # Normalize column names
    raw.columns = [c.strip().lower().replace(" ", "_").replace("/", "_per_") for c in raw.columns]

    # Find the date column (various names across datasets)
    date_col = None
    for candidate in ["date", "month", "period", "time"]:
        matches = [c for c in raw.columns if candidate in c]
        if matches:
            date_col = matches[0]
            break

    if date_col:
        raw["date"] = pd.to_datetime(raw[date_col], format="%b %Y", errors="coerce")
        raw = raw.dropna(subset=["date"])

    raw["source"] = source_label
    raw["fetched_at"] = datetime.datetime.utcnow().isoformat()

    return raw.drop_duplicates().reset_index(drop=True)


def main(backfill: bool = False, with_trade: bool = False) -> None:
    now = datetime.datetime.utcnow()
    today_str = now.strftime("%Y%m%d")
    mode = "backfill" if backfill else "incremental"
    print(f"ERS Specialty Crops Pipeline  mode={mode}  with_trade={with_trade}\n")

    base_dir = os.path.join("storage", "raw", "ers")
    os.makedirs(base_dir, exist_ok=True)

    frames: dict[str, list[pd.DataFrame]] = {
        "ers_fruit_nut_prices": [],
        "ers_veg_prices": [],
        "ers_fruit_nut_trade": [],
        "ers_veg_trade": [],
    }

    # Price index tables
    for label, url in [("Fruit & Tree Nuts", FRUIT_PRICE_URL),
                       ("Vegetables & Pulses", VEG_PRICE_URL)]:
        table = "ers_fruit_nut_prices" if "Fruit" in label else "ers_veg_prices"
        print(f"[{label}] fetching price index data...")
        df = _parse_price_index(url, label)
        if not df.empty:
            print(f"  {len(df):,} rows")
            frames[table].append(df)
        else:
            print(f"  no data returned")
        time.sleep(1)

    # Trade tables (opt-in)
    if with_trade:
        for label, url in [("Fruit & Tree Nuts", FRUIT_TRADE_URL),
                           ("Vegetables & Pulses", VEG_TRADE_URL)]:
            table = "ers_fruit_nut_trade" if "Fruit" in label else "ers_veg_trade"
            print(f"[{label}] fetching trade data...")
            df = _parse_trade(url, label)
            if not df.empty:
                print(f"  {len(df):,} rows")
                frames[table].append(df)
            else:
                print(f"  no data returned")
            time.sleep(1)

    # Write each table
    for table, dfs in frames.items():
        if not dfs:
            print(f"\n[{table}] no data to write")
            continue
        combined = (
            pd.concat(dfs, ignore_index=True)
            .drop_duplicates()
            .sort_values(["series_id", "date"] if "date" in dfs[0].columns else dfs[0].columns.tolist())
            .reset_index(drop=True)
        )
        combined["fetched_at"] = now.isoformat()

        out_dir = os.path.join(base_dir, table)
        path = write_partitioned(
            combined, out_dir,
            f"{table}_{mode}_{today_str}.parquet",
        )
        print(f"\n-> {path}")
        print(f"   {len(combined):,} rows")

    print("\n--- ERS SPECIALTY CROPS PIPELINE COMPLETE ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="USDA ERS specialty crops: fruit/tree nuts + vegetables/pulses prices and trade"
    )
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch full history (vs. recent window)")
    parser.add_argument("--with-trade", action="store_true",
                        help="Also fetch import/export trade tables (~100MB each)")
    args = parser.parse_args()
    main(backfill=args.backfill, with_trade=args.with_trade)
