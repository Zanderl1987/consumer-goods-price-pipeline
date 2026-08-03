#!/usr/bin/env python3
"""
FRED Consumer Price Pipeline — consumer-facing price series from the St. Louis
Fed FRED API (free key at https://fred.stlouisfed.org/docs/api/api_key.html).

Covers retail-ish consumer series that a "tires to avocados" tracker needs:
used/new vehicles, tires, gasoline, food-at-home, apparel, household goods,
medical commodities — plus level series like average gas price by state where
FRED mirrors the EIA data.

CLI:
  python fred_consumer_prices_pipeline.py             # incremental (last 5 years)
  python fred_consumer_prices_pipeline.py --backfill  # full history

Outputs:
  storage/raw/fred/consumer_prices/fred_consumer_prices_{mode}_{YYYYMMDD}.parquet  (CATALOG: fred_consumer_prices)
  storage/raw/fred/used_cars/fred_used_cars_{mode}_{YYYYMMDD}.parquet              (CATALOG: fred_used_cars)
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

OUTPUT_DIR = os.path.join("storage", "raw", "fred")
REQUEST_INTERVAL = 0.35
MAX_RETRIES = 3
BACKOFF_SECONDS = 30
INCREMENTAL_YEARS = 5

# series_id -> (name, category/table)
CONSUMER_SERIES = {
    # Used & new vehicles
    "CUSR0000SETA02": ("CPI Used Cars & Trucks (Urban)",             "consumer_prices"),
    "CUSR0000SETA01": ("CPI New Vehicles (Urban)",                   "consumer_prices"),
    "CUUR0000SETB01": ("CPI Gasoline All Types",                     "consumer_prices"),
    # Tires and parts — verify series IDs on first run
    "CUUR0000SS62031": ("CPI Tires (Urban)",                         "consumer_prices"),
    "CUUR0000SS62021": ("CPI Motor Vehicle Parts & Equipment",       "consumer_prices"),
    # Food
    "CUSR0000SAF11":  ("CPI Food At Home",                           "consumer_prices"),
    "CUSR0000SAF113": ("CPI Fruits & Vegetables",                    "consumer_prices"),
    "CUSR0000SAF114": ("CPI Meats, Poultry, Fish & Eggs",            "consumer_prices"),
    "CUSR0000SAF116": ("CPI Alcoholic Beverages",                    "consumer_prices"),
    "CUSR0000SEFV":   ("CPI Food Away From Home",                    "consumer_prices"),
    # Other consumer goods
    "CUSR0000SAA":    ("CPI Apparel",                                "consumer_prices"),
    "CUSR0000SEMC":   ("CPI Medical Care Commodities",               "consumer_prices"),
    "CUSR0000SAE1":   ("CPI Energy",                                 "consumer_prices"),
    "CUSR0000SAF1":   ("CPI Food",                                   "consumer_prices"),
    "CUSR0000SAM":    ("CPI Medical Care",                           "consumer_prices"),
    "CUSR0000SACE":   ("CPI New & Used Motor Vehicles",              "consumer_prices"),
    # Energy level prices (dollars, not indexes) — mirror of EIA retail
    "GASREGW":        ("U.S. Regular Gas Price (dollars/gal)",       "consumer_prices"),
    "GASREOW":        ("U.S. Regular Conventional Gas Price",        "consumer_prices"),
    "GASMIDW":        ("U.S. Midgrade Gas Price",                    "consumer_prices"),
    "GASPRM":         ("U.S. Premium Gas Price",                     "consumer_prices"),
    "WPSFD4131":      ("PPI Dairy Products",                         "consumer_prices"),
}

USED_CAR_SERIES = {
    "CUSR0000SETA02": ("CPI Used Cars & Trucks (Urban)",             "used_cars"),
    "WPSU11102":      ("PPI Used Motor Vehicles",                    "used_cars"),
    "CUSR0000SETA01": ("CPI New Vehicles (Urban)",                   "used_cars"),
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


def parse_observations(series_id, name, obs_list):
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
            "name":      name,
            "date":      date.strftime("%Y-%m-%d"),
            "value":     value,
        })
    return rows


def run_table(series_map, subdir, table_name, backfill, today_str):
    os.makedirs(os.path.join(OUTPUT_DIR, subdir), exist_ok=True)
    mode = "backfill" if backfill else "incremental"
    start = None if backfill else \
        (datetime.datetime.utcnow() - datetime.timedelta(days=365 * INCREMENTAL_YEARS)).strftime("%Y-%m-%d")

    all_rows = []
    for sid, (name, _) in series_map.items():
        print(f"  {name}...", end=" ", flush=True)
        obs = fetch_series(sid, start)
        if obs:
            all_rows.extend(parse_observations(sid, name, obs))
            print(f"{len(obs)} obs")
        else:
            print("no data")
        time.sleep(REQUEST_INTERVAL)

    if not all_rows:
        print(f"[!] {table_name}: no data returned.")
        return
    df = pd.DataFrame(all_rows)
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()
    df = df.drop_duplicates(subset=["series_id", "date"]).sort_values(["series_id", "date"])
    path = write_partitioned(df, os.path.join(OUTPUT_DIR, subdir),
                             f"{table_name}_{mode}_{today_str}.parquet")
    print(f"[+] {path}  ({len(df):,} rows, {df['series_id'].nunique()} series)")


def main():
    parser = argparse.ArgumentParser(description="FRED consumer price series pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch full history")
    args = parser.parse_args()

    if not FRED_API_KEY:
        print("ERROR: No FRED_API_KEY found. Register free at https://fred.stlouisfed.org/docs/api/api_key.html")
        return

    today_str = datetime.datetime.utcnow().strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"FRED Consumer Prices  mode={mode}")

    print("\n--- consumer_prices ---")
    run_table(CONSUMER_SERIES, "consumer_prices", "fred_consumer_prices", args.backfill, today_str)
    print("\n--- used_cars ---")
    run_table(USED_CAR_SERIES, "used_cars", "fred_used_cars", args.backfill, today_str)

    print("\n--- FRED CONSUMER PRICES COMPLETE ---")


if __name__ == "__main__":
    main()
