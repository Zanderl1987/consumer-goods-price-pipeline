#!/usr/bin/env python3
"""
FRED Headline CPI Pipeline — the broad, index-level Consumer Price Index
series from the St. Louis Fed FRED API (free key at
https://fred.stlouisfed.org/docs/api/api_key.html).

Complements fred_consumer_prices_pipeline.py, which covers narrow retail
sub-indices (used cars, gasoline, food-at-home, ...) — this pipeline covers
the headline/core aggregate series those sub-indices roll up into.

CLI:
  python fred_cpi_pipeline.py             # incremental (last 5 years)
  python fred_cpi_pipeline.py --backfill  # full history

Outputs:
  storage/raw/fred/cpi/fred_cpi_{mode}_{YYYYMMDD}.parquet  (CATALOG: fred_cpi)
"""

import argparse
import datetime
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

from storage_utils import write_partitioned

load_dotenv()

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

OUTPUT_DIR = os.path.join("storage", "raw", "fred", "cpi")
REQUEST_INTERVAL = 0.35
MAX_RETRIES = 3
BACKOFF_SECONDS = 30
INCREMENTAL_YEARS = 5

CPI_SERIES = {
    "CPIAUCSL": "CPI-U All Items (Seasonally Adjusted)",
    "CPIAUCNS": "CPI-U All Items (Not Seasonally Adjusted)",
    "CPILFESL": "CPI-U Core (All Items Less Food & Energy)",
}


def fetch_series(series_id, start_date=None):
    params = {"api_key": FRED_API_KEY, "file_type": "json", "series_id": series_id}
    if start_date:
        params["observation_start"] = start_date
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(FRED_BASE, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                return data.get("observations", [])
            if r.status_code == 429:
                wait = BACKOFF_SECONDS * attempt
                print(f"  429 -- backing off {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                print(f"  HTTP {r.status_code}: {r.text[:150]}")
                return []
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt}): {e}")
            time.sleep(BACKOFF_SECONDS)
    return []


def parse_observations(series_id, obs_list):
    rows = []
    for obs in obs_list:
        value = obs.get("value", ".")
        if value == "." or value == "":
            continue
        try:
            value = float(value)
        except (ValueError, TypeError):
            continue
        date = pd.to_datetime(obs.get("date"))
        if pd.isna(date):
            continue
        rows.append({
            "series_id": series_id,
            "date":      date.strftime("%Y-%m-%d"),
            "value":     value,
        })
    return rows


def main():
    parser = argparse.ArgumentParser(description="FRED headline CPI pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch full history")
    args = parser.parse_args()

    if not FRED_API_KEY:
        print("ERROR: No FRED_API_KEY found. Register free at https://fred.stlouisfed.org/docs/api/api_key.html")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    today_str = datetime.datetime.utcnow().strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    start = None if args.backfill else \
        (datetime.datetime.utcnow() - datetime.timedelta(days=365 * INCREMENTAL_YEARS)).strftime("%Y-%m-%d")
    print(f"FRED Headline CPI  mode={mode}")

    all_rows = []
    for sid, name in CPI_SERIES.items():
        print(f"  {name}...", end=" ", flush=True)
        obs = fetch_series(sid, start)
        if obs:
            all_rows.extend(parse_observations(sid, obs))
            print(f"{len(obs)} obs")
        else:
            print("no data")
        time.sleep(REQUEST_INTERVAL)

    if not all_rows:
        print("\nNo data returned.")
        return

    df = pd.DataFrame(all_rows)
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()
    df = df.drop_duplicates(subset=["series_id", "date"]).sort_values(["series_id", "date"])
    path = write_partitioned(df, OUTPUT_DIR, f"fred_cpi_{mode}_{today_str}.parquet")
    print(f"\n[+] {path}  ({len(df):,} rows, {df['series_id'].nunique()} series)")
    print("\n--- FRED HEADLINE CPI COMPLETE ---")


if __name__ == "__main__":
    main()
