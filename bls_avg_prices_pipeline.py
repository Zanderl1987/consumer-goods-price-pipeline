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

# Bulk flat-file mirror of the "Average Price" (AP) survey -- keyless, no
# per-day request quota (unlike the timeseries API above, whose shared v1
# quota gets exhausted repo-wide, and even machine-wide when
# financial-data-pipeline runs a BLS backfill the same day). Full history,
# updated monthly. BLS splits the survey across three files by topic; each
# series below is tagged with which one it lives in.
AP_FLATFILE_URLS = {
    "food":            "https://download.bls.gov/pub/time.series/ap/ap.data.3.Food",
    "household_fuels": "https://download.bls.gov/pub/time.series/ap/ap.data.1.HouseholdFuels",
    "gasoline":        "https://download.bls.gov/pub/time.series/ap/ap.data.2.Gasoline",
}
AP_FLATFILE_HEADERS = {"User-Agent": "consumer-goods-price-pipeline research (contact: zander.s.luke@gmail.com)"}

OUTPUT_DIR = os.path.join("storage", "raw", "bls", "avg_prices")
REQUEST_INTERVAL = 1.5
MAX_RETRIES = 3
BATCH_SIZE = 50 if BLS_API_KEY else 25
BACKFILL_START_YEAR = 1980
INCREMENTAL_YEARS = 2

# (series_id, item, unit, flatfile_key) — APU average-price codes verified
# 2026-08-24 against BLS's authoritative item catalog
# (download.bls.gov/pub/time.series/ap/ap.item) and confirmed present in
# their respective flat files. The previous catalog here had every series_id
# mismatched against its label (an off-by-rows error from hand-guessing
# codes rather than looking them up) -- e.g. "APU0000702111" was labeled
# "Cheddar cheese" but is actually "Bread, white, pan"; ~half the codes
# (energy, tea, salt, avocados, watermelon, cauliflower) didn't exist in the
# BLS catalog at all and always returned nothing. Rebuilt from scratch.
AVG_PRICE_SERIES = [
    # ── Dairy & eggs ───────────────────────────────────────────────────────
    ("APU0000709112", "Milk, fresh, whole, per gallon",              "USD/gallon",      "food"),
    ("APU0000709111", "Milk, fresh, whole, per half gallon",         "USD/half-gallon", "food"),
    ("APU0000709212", "Milk, fresh, low fat, per half gallon",       "USD/half-gallon", "food"),
    ("APU0000709213", "Milk, fresh, low fat, per gallon",            "USD/gallon",      "food"),
    ("APU0000708111", "Eggs, grade A, large, per dozen",             "USD/dozen",       "food"),
    ("APU0000708112", "Eggs, grade AA, large, per dozen",            "USD/dozen",       "food"),
    ("APU0000710211", "American processed cheese, per lb",           "USD/lb",          "food"),
    ("APU0000710212", "Cheddar cheese, natural, per lb",              "USD/lb",          "food"),
    ("APU0000710111", "Butter, salted, grade AA, stick, per lb",     "USD/lb",          "food"),
    ("APU0000710122", "Yogurt, natural, fruit flavored, per 8 oz",   "USD/8oz",         "food"),
    ("APU0000710411", "Ice cream, prepackaged, bulk, per half gallon", "USD/half-gallon", "food"),
    # ── Bakery & grains ────────────────────────────────────────────────────
    ("APU0000702111", "Bread, white, pan, per lb",                   "USD/lb",          "food"),
    ("APU0000702212", "Bread, whole wheat, pan, per lb",             "USD/lb",          "food"),
    ("APU0000701111", "Flour, white, all purpose, per lb",           "USD/lb",          "food"),
    ("APU0000701312", "Rice, white, long grain, uncooked, per lb",   "USD/lb",          "food"),
    ("APU0000701322", "Spaghetti and macaroni, per lb",              "USD/lb",          "food"),
    ("APU0000716141", "Peanut butter, creamy, per lb",               "USD/lb",          "food"),
    # ── Produce ────────────────────────────────────────────────────────────
    ("APU0000711111", "Apples, Red Delicious, per lb",               "USD/lb",          "food"),
    ("APU0000711211", "Bananas, per lb",                             "USD/lb",          "food"),
    ("APU0000711311", "Oranges, Navel, per lb",                      "USD/lb",          "food"),
    ("APU0000711411", "Grapefruit, per lb",                          "USD/lb",          "food"),
    ("APU0000711412", "Lemons, per lb",                              "USD/lb",          "food"),
    ("APU0000711415", "Strawberries, dry pint, per 12 oz",           "USD/12oz",        "food"),
    ("APU0000712112", "Potatoes, white, per lb",                     "USD/lb",          "food"),
    ("APU0000712211", "Lettuce, iceberg, per lb",                    "USD/lb",          "food"),
    ("APU0000FL2101", "Lettuce, romaine, per lb",                    "USD/lb",          "food"),
    ("APU0000712311", "Tomatoes, field grown, per lb",               "USD/lb",          "food"),
    ("APU0000712402", "Celery, per lb",                              "USD/lb",          "food"),
    ("APU0000712403", "Carrots, short trimmed and topped, per lb",   "USD/lb",          "food"),
    ("APU0000712404", "Onions, dry yellow, per lb",                  "USD/lb",          "food"),
    ("APU0000712406", "Peppers, sweet, per lb",                      "USD/lb",          "food"),
    ("APU0000712409", "Cucumbers, per lb",                           "USD/lb",          "food"),
    ("APU0000712412", "Broccoli, per lb",                            "USD/lb",          "food"),
    # ── Meat, poultry, fish ────────────────────────────────────────────────
    ("APU0000703112", "Ground beef, 100% beef, per lb",              "USD/lb",          "food"),
    ("APU0000703211", "Chuck roast, USDA Choice, bone-in, per lb",   "USD/lb",          "food"),
    ("APU0000703511", "Steak, round, USDA Choice, boneless, per lb", "USD/lb",          "food"),
    ("APU0000704111", "Bacon, sliced, per lb",                       "USD/lb",          "food"),
    ("APU0000704311", "Ham, rump or shank half, bone-in, smoked, per lb", "USD/lb",     "food"),
    ("APU0000706111", "Chicken, fresh, whole, per lb",               "USD/lb",          "food"),
    ("APU0000706211", "Chicken breast, bone-in, per lb",             "USD/lb",          "food"),
    ("APU0000FF1101", "Chicken breast, boneless, per lb",            "USD/lb",          "food"),
    ("APU0000706212", "Chicken legs, bone-in, per lb",               "USD/lb",          "food"),
    ("APU0000706311", "Turkey, frozen, whole, per lb",               "USD/lb",          "food"),
    ("APU0000707111", "Tuna, light, chunk, per lb",                  "USD/lb",          "food"),
    # ── Beverages & condiments ─────────────────────────────────────────────
    ("APU0000717311", "Coffee, 100% ground roast, all sizes, per lb", "USD/lb",         "food"),
    ("APU0000717324", "Coffee, instant, plain, regular, per 16 oz", "USD/16oz",         "food"),
    ("APU0000717114", "Cola, nondiet, per 2-liter",                  "USD/2L",          "food"),
    ("APU0000715211", "Sugar, white, all sizes, per lb",             "USD/lb",          "food"),
    # ── Energy (consumer-facing) ───────────────────────────────────────────
    ("APU000074714",  "Gasoline, unleaded regular, per gallon",      "USD/gallon",      "gasoline"),
    ("APU000074715",  "Gasoline, unleaded midgrade, per gallon",     "USD/gallon",      "gasoline"),
    ("APU000074716",  "Gasoline, unleaded premium, per gallon",      "USD/gallon",      "gasoline"),
    ("APU00007471A",  "Gasoline, all types, per gallon",             "USD/gallon",      "gasoline"),
    ("APU000074717",  "Automotive diesel fuel, per gallon",          "USD/gallon",      "gasoline"),
    ("APU000072610",  "Electricity, per KWH",                        "USD/KWH",         "household_fuels"),
    ("APU000072620",  "Utility (piped) gas, per therm",              "USD/therm",       "household_fuels"),
    ("APU000072511",  "Fuel oil #2, per gallon",                     "USD/gallon",      "household_fuels"),
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
    meta = {sid: (item, unit) for sid, item, unit, _ in AVG_PRICE_SERIES}
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


def fetch_avg_prices_flatfile():
    """Pull all of AVG_PRICE_SERIES from BLS's keyless bulk flat files instead
    of the quota-limited timeseries API. Downloads each distinct flat file
    (food / gasoline / household_fuels) once. Returns a DataFrame matching
    parse_series()'s schema, or an empty DataFrame on total failure."""
    meta = {sid: (item, unit, ffkey) for sid, item, unit, ffkey in AVG_PRICE_SERIES}
    wanted_by_file = {}
    for sid, (item, unit, ffkey) in meta.items():
        wanted_by_file.setdefault(ffkey, set()).add(sid)

    rows = []
    for ffkey, wanted in wanted_by_file.items():
        url = AP_FLATFILE_URLS[ffkey]
        try:
            resp = requests.get(url, headers=AP_FLATFILE_HEADERS, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"  Flat-file fetch failed ({ffkey}): {exc}")
            continue

        for line in resp.text.splitlines()[1:]:
            parts = line.split("\t")
            if len(parts) < 4:
                continue
            sid = parts[0].strip()
            if sid not in wanted:
                continue
            item, unit, _ = meta[sid]
            year_str = parts[1].strip()
            period = parts[2].strip()
            value_str = parts[3].strip()
            if not period.startswith("M"):
                continue
            month = int(period[1:])
            if month > 12:
                continue  # M13 = annual average -- skip
            try:
                price = float(value_str)
                year = int(year_str)
            except ValueError:
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

    flat_df = fetch_avg_prices_flatfile()
    if not flat_df.empty:
        all_frames.append(flat_df)
    else:
        print("  Flat files returned nothing -- falling back to timeseries API.")
        for y_start, y_end in year_chunks:
            for batch_start in range(0, len(AVG_PRICE_SERIES), BATCH_SIZE):
                batch = [sid for sid, _, _, _ in AVG_PRICE_SERIES[batch_start:batch_start + BATCH_SIZE]]
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
