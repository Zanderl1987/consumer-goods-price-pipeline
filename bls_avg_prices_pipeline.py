#!/usr/bin/env python3
"""
BLS Average Price Data Pipeline — actual retail dollar prices, not indexes.

The BLS "Average Price Data" series give real per-unit retail prices (USD) for
~80 commonly purchased items — milk, eggs, bread, bananas, avocados, ground
beef, gasoline, electricity, etc. This is the closest free government source
to a true "price" (vs an index) for everyday consumer goods.

Uses API v2 if BLS_API_KEY is in .env, else v1 (no key, free).
Register free at https://data.bls.gov/registrationEngine/ to get a v2 key.

CLI:
  python bls_avg_prices_pipeline.py             # incremental (last 2 years)
  python bls_avg_prices_pipeline.py --backfill  # full history from 1980

Outputs:
  storage/raw/bls/avg_prices/bls_avg_prices_{mode}_{YYYYMMDD}.parquet  (CATALOG: bls_avg_prices)

NOTE: series IDs are the standard APU* average-price codes. Verify a handful
against https://data.bls.gov/cgi-bin/srgate on first run.
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

BLS_API_KEY = os.environ.get("BLS_API_KEY", "")
BLS_V1 = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
BLS_V2 = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_URL = BLS_V2 if BLS_API_KEY else BLS_V1

OUTPUT_DIR = os.path.join("storage", "raw", "bls", "avg_prices")
REQUEST_INTERVAL = 1.5
MAX_RETRIES = 3
BATCH_SIZE = 50 if BLS_API_KEY else 25
BACKFILL_START_YEAR = 1980
INCREMENTAL_YEARS = 2

# (series_id, item, unit) — APU average-price codes. Prices are USD per unit.
AVG_PRICE_SERIES = [
    # ── Dairy & eggs ───────────────────────────────────────────────────────
    ("APU0000709112", "Milk, fresh, whole, per gallon",            "USD/gallon"),
    ("APU0000709111", "Milk, fresh, whole, per half gallon",       "USD/half-gallon"),
    ("APU0000709121", "Milk, fresh, lowfat, per gallon",           "USD/gallon"),
    ("APU0000710311", "Eggs, grade A, large, per dozen",           "USD/dozen"),
    ("APU0000702111", "Cheddar cheese, natural, per pound",        "USD/lb"),
    ("APU0000701111", "Butter, salted, per pound",                 "USD/lb"),
    ("APU0000712311", "Yogurt, plain, per 8 oz",                   "USD/8oz"),
    ("APU0000712211", "Ice cream, prepackaged, bulk, per half gallon", "USD/half-gallon"),
    # ── Bakery & grains ────────────────────────────────────────────────────
    ("APU0000703111", "White bread, per pound",                    "USD/lb"),
    ("APU0000703212", "Bread, wheat, per pound",                   "USD/lb"),
    ("APU0000701111", "Flour, white, all purpose, per lb",         "USD/lb"),
    ("APU0000703311", "Rice, white, long grain, uncooked, per lb", "USD/lb"),
    ("APU0000704111", "Spaghetti and macaroni, per lb",            "USD/lb"),
    ("APU0000705111", "Peanut butter, creamy, per 18 oz",          "USD/18oz"),
    # ── Produce ────────────────────────────────────────────────────────────
    ("APU0000711411", "Avocados, all types, per lb",               "USD/lb"),
    ("APU0000710111", "Potatoes, white, per lb",                   "USD/lb"),
    ("APU0000710211", "Lettuce, iceberg, per lb",                  "USD/lb"),
    ("APU0000711111", "Tomatoes, field grown, per lb",             "USD/lb"),
    ("APU0000711211", "Tomatoes, grape, per pint",                 "USD/pint"),
    ("APU0000712111", "Broccoli, per lb",                          "USD/lb"),
    ("APU0000712311", "Cauliflower, per head",                     "USD/head"),
    ("APU0000712411", "Celery, per lb",                            "USD/lb"),
    ("APU0000713311", "Green beans, per lb",                       "USD/lb"),
    ("APU0000714111", "Peppers, sweet, per lb",                    "USD/lb"),
    ("APU0000715211", "Apples, Red Delicious, per lb",             "USD/lb"),
    ("APU0000716111", "Bananas, per lb",                           "USD/lb"),
    ("APU0000717111", "Grapes, per lb",                            "USD/lb"),
    ("APU0000717311", "Lemons, per lb",                            "USD/lb"),
    ("APU0000718111", "Oranges, navel, per lb",                    "USD/lb"),
    ("APU0000719211", "Strawberries, dry pint",                    "USD/pint"),
    ("APU0000719411", "Watermelon, per lb",                        "USD/lb"),
    # ── Meat, poultry, fish ────────────────────────────────────────────────
    ("APU0000720311", "Ground beef, 100% beef, per lb",            "USD/lb"),
    ("APU0000723111", "Bacon, sliced, per lb",                     "USD/lb"),
    ("APU0000724111", "Ham, rump or shank half, bone-in, per lb",  "USD/lb"),
    ("APU0000725111", "Chicken breast, boneless, per lb",          "USD/lb"),
    ("APU0000726111", "Chicken legs, bone-in, per lb",             "USD/lb"),
    ("APU0000727111", "Turkey, frozen, whole, per lb",             "USD/lb"),
    ("APU0000721211", "Beef chuck roast, per lb",                  "USD/lb"),
    ("APU0000721311", "Beef steak, round, per lb",                 "USD/lb"),
    # ── Beverages & condiments ─────────────────────────────────────────────
    ("APU0000732111", "Coffee, 100% ground roast, per lb",         "USD/lb"),
    ("APU0000733111", "Coffee, instant, plain, per 3 oz",          "USD/3oz"),
    ("APU0000734111", "Tea, black, per 16 bags",                   "USD/16-bag"),
    ("APU0000735111", "Cola, nondiet, per 2-liter",                "USD/2L"),
    ("APU0000736211", "Soda, other, per 2-liter",                  "USD/2L"),
    ("APU0000737111", "Milk, nonfat, per gallon",                  "USD/gallon"),
    ("APU0000738111", "Sugar, white, per lb",                      "USD/lb"),
    ("APU0000741211", "Salt, per 26 oz",                           "USD/26oz"),
    # ── Energy (consumer-facing) ───────────────────────────────────────────
    ("APU0000726101", "Gasoline, all types, per gallon",           "USD/gallon"),
    ("APU0000726201", "Gasoline, regular unleaded, per gallon",    "USD/gallon"),
    ("APU0000726301", "Gasoline, midgrade, per gallon",            "USD/gallon"),
    ("APU0000726401", "Gasoline, premium, per gallon",             "USD/gallon"),
    ("APU0000727161", "Diesel fuel, per gallon",                   "USD/gallon"),
    ("APU0000745121", "Electricity, per KWH",                      "USD/KWH"),
    ("APU0000745141", "Utility (piped) gas, per therm",            "USD/therm"),
    ("APU0000747131", "Fuel oil #2, per gallon",                   "USD/gallon"),
]


def fetch_batch(series_ids, start_year, end_year):
    """POST a batch of BLS series IDs; return raw series list from API."""
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if BLS_API_KEY:
        payload["registrationkey"] = BLS_API_KEY
        payload["annualaverage"] = "false"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(BLS_URL, json=payload, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") != "REQUEST_SUCCEEDED":
                    msgs = data.get("message", [])
                    print(f"  BLS API non-success: {msgs}")
                    return []
                return data.get("Results", {}).get("series", [])
            if resp.status_code == 429:
                wait = 60 * attempt
                print(f"  429 rate limit. Backing off {wait}s (attempt {attempt}/{MAX_RETRIES}).")
                time.sleep(wait)
            else:
                print(f"  HTTP {resp.status_code}: {resp.text[:120]}")
                return []
        except requests.RequestException as exc:
            print(f"  Request error (attempt {attempt}): {exc}")
            time.sleep(30 * attempt)
    return []


def parse_series(raw_series):
    """Convert BLS API response to a long-format DataFrame of retail prices."""
    meta = {sid: (item, unit) for sid, item, unit in AVG_PRICE_SERIES}
    rows = []
    for s in raw_series:
        sid = s.get("seriesID", "")
        if sid not in meta:
            continue
        item, unit = meta[sid]
        for obs in s.get("data", []):
            period = obs.get("period", "")
            year_str = obs.get("year", "")
            value_str = obs.get("value", "")
            try:
                price = float(value_str)
                year = int(year_str)
            except (ValueError, TypeError):
                continue
            if not period.startswith("M"):
                continue
            month = int(period[1:])
            if month > 12:
                continue
            rows.append({
                "series_id": sid,
                "item":      item,
                "unit":      unit,
                "date":      f"{year}-{month:02d}-01",
                "price":     price,
            })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="BLS average retail price data pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help=f"Fetch full history from {BACKFILL_START_YEAR}")
    args = parser.parse_args()

    now = datetime.datetime.utcnow()
    today_str = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    start_year = BACKFILL_START_YEAR if args.backfill else now.year - INCREMENTAL_YEARS

    print(f"BLS Average Price Data  mode={mode}  start={start_year}")
    print(f"API: {'v2 (key present)' if BLS_API_KEY else 'v1 (no key -- add BLS_API_KEY to .env for higher limits)'}\n")

    year_chunks = []
    SPAN = 20
    y = start_year
    while y <= now.year:
        year_chunks.append((y, min(y + SPAN - 1, now.year)))
        y += SPAN

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_frames = []
    for y_start, y_end in year_chunks:
        for batch_start in range(0, len(AVG_PRICE_SERIES), BATCH_SIZE):
            batch = [sid for sid, _, _ in AVG_PRICE_SERIES[batch_start:batch_start + BATCH_SIZE]]
            raw = fetch_batch(batch, y_start, y_end)
            if raw:
                df = parse_series(raw)
                if not df.empty:
                    all_frames.append(df)
            time.sleep(REQUEST_INTERVAL)

    if not all_frames:
        print("  No data returned. Check BLS_API_KEY / series IDs.")
        return

    combined = (
        pd.concat(all_frames, ignore_index=True)
        .drop_duplicates(subset=["series_id", "date"])
        .sort_values(["series_id", "date"])
    )
    combined["fetched_at"] = now.isoformat()

    path = write_partitioned(
        combined, OUTPUT_DIR,
        f"bls_avg_prices_{mode}_{today_str}.parquet",
    )
    print(f"  -> {path}  ({len(combined):,} rows, {combined['item'].nunique()} items)")

    print("\n--- BLS AVERAGE PRICES PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
