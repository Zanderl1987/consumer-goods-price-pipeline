#!/usr/bin/env python3
"""
USDA NASS Prices Pipeline — prices received (what farmers are paid) and prices
paid (input costs that flow into consumer prices).

Uses the USDA NASS QuickStats API. Register free at
https://quickstats.nass.usda.gov/api to get your key. Add USDA_NASS_API_KEY to
.env.

CLI:
  python usda_nass_prices_pipeline.py             # incremental (last 5 years)
  python usda_nass_prices_pipeline.py --backfill  # full history from 2000

Outputs:
  storage/raw/usda/prices_received/usda_prices_received_{mode}_{YYYYMMDD}.parquet  (CATALOG: usda_prices_received)
  storage/raw/usda/prices_paid/usda_prices_paid_{mode}_{YYYYMMDD}.parquet          (CATALOG: usda_prices_paid)
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

NASS_API_KEY   = os.environ.get("USDA_NASS_API_KEY", "")
NASS_API_KEY_2 = os.environ.get("USDA_NASS_API_KEY_2", "")
NASS_BASE = "https://quickstats.nass.usda.gov/api/api_GET/"

OUTPUT_DIR = os.path.join("storage", "raw", "usda")
REQUEST_INTERVAL = 0.5
MAX_RETRIES = 3
BACKOFF_SECONDS = 30
BACKFILL_START_YEAR = 2000
INCREMENTAL_YEARS = 5

# Prices received (farm-gate) — livestock, crops, and products consumers buy.
# Filtered live to statisticcat_desc="PRICE RECEIVED" (see fetch_commodities).
# NOT sector/group-filtered: this list spans livestock (ANIMALS & PRODUCTS/
# LIVESTOCK, DAIRY, POULTRY groups), field crops (CROPS/FIELD CROPS), and
# fruit/veg (CROPS/FRUIT & TREE NUTS, VEGETABLES) -- a single sector_desc+
# group_desc filter can only ever match one of those groups, so it silently
# 400s -- "bad request - invalid query" -- for every commodity outside
# whichever group was picked (found live 2026-08-04, first real run of this
# pipeline since a key was configured: every fruit/veg/dairy/poultry item
# failed both LIVESTOCK and FIELD CROPS filters). commodity_desc alone is
# specific enough; NASS resolves sector/group from it internally.
PRICES_RECEIVED = [
    "CATTLE",
    "HOGS",
    # "BROILERS" is a class_desc under commodity_desc="CHICKENS", not its
    # own commodity -- CHICKENS below already returns broiler-class rows
    # (found live 2026-08-04: "BROILERS" 400s, CHICKENS's own results
    # include class_desc="BROILERS" entries).
    "CHICKENS",
    "TURKEYS",
    "MILK",
    "EGGS",
    "CORN",
    "SOYBEANS",
    "WHEAT",
    "COTTON",
    "RICE",
    "APPLES",
    "GRAPES",
    "STRAWBERRIES",
    "POTATOES",
    "LETTUCE",
    "TOMATOES",
    "AVOCADOS",
    "ORANGES",
]

# Prices paid (input costs) that eventually show up in consumer goods.
# Filtered live to statisticcat_desc="INDEX FOR PRICE PAID, 2011" (the
# modern-base index; NASS also publishes a legacy 1910-1914=100 base for
# the same commodities, and a "RELATIVE WEIGHT" series that isn't a price
# at all -- both excluded by the statisticcat filter). These are all
# INDEX values (unit_desc="INDEX"), not absolute dollars -- "FEED"/
# "FERTILIZER TOTALS"/etc. are aggregate baskets, not single priced goods,
# same as this repo's other CPI-style index tables.
# Two of the originally-listed commodity_desc values ("ANIMAL DRUGS",
# "BABY CHICKS") don't exist anywhere in NASS's PRICES PAID taxonomy --
# also found live 2026-08-04 (400 on every request, even with the correct
# statisticcat) -- swapped for real commodity_desc values covering similar
# ground (POULTRY TOTALS, ANIMAL SECTOR).
PRICES_PAID = [
    "FEED",
    "FERTILIZER TOTALS",
    "FUELS",
    "SEEDS & PLANTS TOTALS",
    "POULTRY TOTALS",
    "ANIMAL SECTOR",
]

PRICES_RECEIVED_STATISTICCAT = "PRICE RECEIVED"
PRICES_PAID_STATISTICCAT = "INDEX FOR PRICE PAID, 2011"


def _get_with_backoff(params: dict) -> dict | None:
    keys = [k for k in [NASS_API_KEY, NASS_API_KEY_2] if k]
    if not keys:
        return None
    key_idx = 0
    params["key"] = keys[key_idx]
    attempt = 0
    while attempt < MAX_RETRIES:
        attempt += 1
        try:
            r = requests.get(NASS_BASE, params=params, timeout=60)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception as e:
                    print(f"  JSON parse error: {e}")
                    return None
            if r.status_code == 401 and key_idx + 1 < len(keys):
                key_idx += 1
                params["key"] = keys[key_idx]
                print("  401 unauthorized -- switching to backup API key")
                attempt -= 1
                continue
            if r.status_code == 429:
                wait = BACKOFF_SECONDS * attempt
                print(f"  429 rate limit -- backing off {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                print(f"  HTTP {r.status_code}: {r.text[:200]}")
                return None
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt}): {e}")
            time.sleep(BACKOFF_SECONDS)
    print(f"  Giving up after {MAX_RETRIES} attempts.")
    return None


def fetch_commodities(commodities: list[str], statisticcat: str,
                      start_year: int, end_year: int) -> pd.DataFrame:
    """Fetch one statisticcat_desc's price statistics for a list of commodities."""
    frames = []
    for comm in commodities:
        params = {
            "source_desc": "SURVEY",
            "commodity_desc": comm,
            "statisticcat_desc": statisticcat,
            "agg_level_desc": "NATIONAL",
            "year__GE": str(start_year),
            "year__LE": str(end_year),
            "format": "JSON",
        }
        print(f"  {comm}...", end=" ", flush=True)
        data = _get_with_backoff(params)
        if data is None:
            print("request failed")
        elif "error" in data:
            print(f"API error: {data['error']}")
        elif "data" in data and data["data"]:
            frames.append(pd.DataFrame(data["data"]))
            print(f"{len(data['data'])} records")
        else:
            print("no data")
        time.sleep(REQUEST_INTERVAL)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def clean(df: pd.DataFrame, sector_tag: str) -> pd.DataFrame:
    df.columns = [c.lower().strip() for c in df.columns]
    rename = {
        "commodity_desc": "commodity",
        "statisticcat_desc": "stat_category",
        "short_desc": "description",
        "unit_desc": "unit",
        "agg_level_desc": "agg_level",
        "value": "value_raw",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    if "value_raw" in df.columns:
        df["value"] = pd.to_numeric(
            df["value_raw"].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )
    if "year" in df.columns:
        # begin_code is the reference month ("01".."12") for MONTHLY series,
        # "00" for ANNUAL series -- collapsing straight to year-01-01 for
        # every row (the original approach) silently merged up to 12
        # distinct monthly observations per commodity/year into one row
        # once curated.py deduped on (commodity, date) -- found live
        # 2026-08-04 as a 99%+ row-count collapse on this pipeline's
        # first-ever real run.
        month = pd.to_numeric(df.get("begin_code"), errors="coerce").fillna(1).clip(lower=1, upper=12).astype(int)
        year = pd.to_numeric(df["year"], errors="coerce")
        df["date"] = pd.to_datetime(
            year.astype("Int64").astype(str) + "-" + month.astype(str).str.zfill(2) + "-01",
            errors="coerce",
        )
    # "year" dropped: Hive partitioning treats it as a reserved virtual column
    keep = ["commodity", "stat_category", "description", "unit", "date", "agg_level", "value"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].dropna(subset=["value"])
    df["price_type"] = sector_tag
    df["source"] = "USDA NASS QuickStats"
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()
    return df.sort_values(["commodity", "date"]).reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="USDA NASS prices received/paid pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help=f"Fetch full history from {BACKFILL_START_YEAR}")
    args = parser.parse_args()

    if not NASS_API_KEY and not NASS_API_KEY_2:
        print("ERROR: No USDA NASS API key found (USDA_NASS_API_KEY).")
        print("  Register free at https://quickstats.nass.usda.gov/api")
        return

    os.makedirs(os.path.join(OUTPUT_DIR, "prices_received"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "prices_paid"), exist_ok=True)

    now = datetime.datetime.utcnow()
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    start_year = BACKFILL_START_YEAR if args.backfill else now.year - INCREMENTAL_YEARS
    print(f"Mode: {'BACKFILL' if args.backfill else 'INCREMENTAL'} ({start_year}-{now.year})")

    print(f"\n--- Prices Received ({', '.join(PRICES_RECEIVED[:5])}...) ---")
    df = fetch_commodities(PRICES_RECEIVED, PRICES_RECEIVED_STATISTICCAT, start_year, now.year)
    clean_recv = clean(df, "prices_received")
    if not clean_recv.empty:
        path = write_partitioned(clean_recv, os.path.join(OUTPUT_DIR, "prices_received"),
                                 f"usda_prices_received_{mode}_{today}.parquet")
        print(f"\n[+] {path}  ({len(clean_recv):,} rows)")

    print(f"\n--- Prices Paid ({', '.join(PRICES_PAID)}) ---")
    df_paid = fetch_commodities(PRICES_PAID, PRICES_PAID_STATISTICCAT, start_year, now.year)
    clean_paid = clean(df_paid, "prices_paid")
    if not clean_paid.empty:
        path = write_partitioned(clean_paid, os.path.join(OUTPUT_DIR, "prices_paid"),
                                 f"usda_prices_paid_{mode}_{today}.parquet")
        print(f"\n[+] {path}  ({len(clean_paid):,} rows)")

    print("\n--- COMPLETE ---")


if __name__ == "__main__":
    main()
