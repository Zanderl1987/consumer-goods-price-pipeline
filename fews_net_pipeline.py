#!/usr/bin/env python3
"""
FEWS NET Data Warehouse Market Prices Pipeline.

Downloads market price data from FEWS NET's REST API for food-insecure
countries. Covers wholesale/retail/producer market prices across monitored
locations in Africa, the Middle East, and South/Southeast Asia.

API: fdw.fews.net REST endpoints (keyless, no auth required)
  Market price facts: https://fdw.fews.net/api/marketpricefacts.csv

NOTE: During initial probing (2026-08-24), the FEWS NET warehouse was flaky —
the API root and resource list returned 200, but every data query 502'd or
timed out. Pipeline is built defensively off the documented column list;
retries with exponential backoff and writes nothing on failure. A live run
should be attempted when the warehouse recovers.

Default window: last 10 years of monthly observations.
--backfill: full history from source floor.

CLI:
  python fews_net_pipeline.py              # last 10 years
  python fews_net_pipeline.py --backfill   # full history

Output:
  storage/raw/fews_net/fews_net_food_prices_{mode}_{YYYYMMDD}.parquet
"""

import argparse
import datetime
import os
import time

import pandas as pd
import requests
from storage_utils import write_partitioned

BASE_DIR = os.path.join("storage", "raw", "fews_net")
MAX_RETRIES = 3
BACKOFF_SECONDS = 30
REQUEST_TIMEOUT = 300
HEADERS = {"User-Agent": "Mozilla/5.0 consumer-goods-price-pipeline/1.0"}

# REST endpoint — returns CSV with market price observations
API_URL = "https://fdw.fews.net/api/marketpricefacts.csv"

# Default lookback for incremental mode
DEFAULT_WINDOW_YEARS = 10


def _fetch_data(start_date: str | None = None) -> pd.DataFrame:
    """Fetch market price facts from FEWS NET REST API.

    Returns a DataFrame with the documented FEWS NET market price schema.
    On failure (timeout, 502, etc.), returns an empty DataFrame.
    """
    params: dict = {"fields": "simple"}
    if start_date:
        params["start_date"] = start_date

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  Attempt {attempt}/{MAX_RETRIES} (timeout {REQUEST_TIMEOUT}s)...")
            r = requests.get(
                API_URL,
                params=params,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            if r.status_code == 200 and r.content:
                from io import StringIO
                return pd.read_csv(StringIO(r.text), low_memory=False)
            if r.status_code in (502, 503, 504):
                print(f"    Server error {r.status_code}, retrying...")
                time.sleep(BACKOFF_SECONDS * attempt)
            else:
                print(f"    HTTP {r.status_code}")
                if attempt < MAX_RETRIES:
                    time.sleep(BACKOFF_SECONDS)
                else:
                    return pd.DataFrame()
        except requests.Timeout:
            print(f"    Request timed out ({REQUEST_TIMEOUT}s)")
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS)
        except requests.RequestException as e:
            print(f"    Request error: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS)

    return pd.DataFrame()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize FEWS NET column names to snake_case and add source/fetched_at."""
    if df.empty:
        return df

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.strip("_")
    )

    # Map known FEWS NET column variants to canonical names
    rename_map = {}
    for col in df.columns:
        if "market" in col and "price" in col and "factor" in col:
            rename_map[col] = "market_price_factor"
        elif col == "cpcv2":
            rename_map[col] = "cpcv2_code"

    if rename_map:
        df = df.rename(columns=rename_map)

    df["source"] = "fews_net"
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()

    return df


def main(backfill: bool = False) -> None:
    now = datetime.datetime.utcnow()
    today_str = now.strftime("%Y%m%d")
    mode = "backfill" if backfill else "incremental"
    print(f"FEWS NET Market Prices Pipeline  mode={mode}\n")

    os.makedirs(BASE_DIR, exist_ok=True)

    start_date = None
    if not backfill:
        start = now - datetime.timedelta(days=DEFAULT_WINDOW_YEARS * 365)
        start_date = start.strftime("%Y-%m-%d")
        print(f"  Window: {start_date} to present\n")

    df = _fetch_data(start_date=start_date)

    if df.empty:
        print("\nNo data returned from FEWS NET (service may be unavailable).")
        print("Nothing written.")
        return

    df = _normalize_columns(df)
    df = df.drop_duplicates()

    # Dedup key: market/cpcv2/price_type/period_date if columns exist
    dedup_cols = [c for c in ["market", "cpcv2_code", "market_price_factor", "period_date"]
                  if c in df.columns]
    if dedup_cols:
        df = df.drop_duplicates(subset=dedup_cols)

    path = write_partitioned(
        df, BASE_DIR,
        f"fews_net_food_prices_{mode}_{today_str}.parquet",
    )
    print(f"\n-> {path}")
    print(f"   {len(df):,} rows | columns: {list(df.columns)}")

    print("\n--- FEWS NET MARKET PRICES PIPELINE COMPLETE ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FEWS NET Data Warehouse market prices (food-insecure countries, keyless)"
    )
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch full history from source floor")
    args = parser.parse_args()
    main(backfill=args.backfill)
