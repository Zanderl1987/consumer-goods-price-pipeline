#!/usr/bin/env python3
"""
USDA AMS Market News Pipeline - wholesale terminal and retail prices for fresh
produce (including avocados) and other specialty crops.

The USDA Agricultural Marketing Service publishes daily/weekly market news
reports. Access is via the MARS API (https://marsapi.ams.usda.gov), which
requires a free registration key shown in "My Profile" after creating a
MyMarketNews account at https://mymarketnews.ams.usda.gov/.

Verified against the live API (2026-08-03, see docs/SOURCES.md):
  Base URL  : https://marsapi.ams.usda.gov/services/v1.2   (v3.0 = corrections)
  Auth      : HTTP Basic, username = API key, empty password
  Limits    : 100,000 records per request; 180-day date window per request
              (backfill must paginate in windows); AMS blocks high-frequency
              polling, so keep REQUEST_INTERVAL sane and send a real User-Agent.

Reports of interest (report slugs on the MARS API):
  FVWRETAIL  - "National Retail Report - Specialty Crops" (weekly retail
               produce prices from 400+ retailers, incl. avocados) - VERIFIED
  FVWV       - "National Fresh Fruit and Vegetable Terminal Market" -
               terminal-market wholesale quotes per city (slug to confirm live)

CLI:
  python usda_ams_pipeline.py             # incremental (last 60 days)
  python usda_ams_pipeline.py --backfill  # full available history

Outputs:
  storage/raw/usda/ams_wholesale/...  (CATALOG: usda_ams_wholesale)
  storage/raw/usda/ams_retail/...     (CATALOG: usda_ams_retail)
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

AMS_API_KEY = os.environ.get("USDA_AMS_API_KEY", "")
AMS_BASE = "https://marsapi.ams.usda.gov/services/v1.2"

OUTPUT_DIR = os.path.join("storage", "raw", "usda")
REQUEST_INTERVAL = 1.0
MAX_RETRIES = 3
BACKOFF_SECONDS = 60
MAX_DAYS_PER_REQUEST = 180

# Report slugs on the MARS API. FVWRETAIL is verified; others confirm live.
RETAIL_REPORT_SLUGS = [
    "FVWRETAIL",   # National Retail Report - Specialty Crops (incl. avocados)
]

WHOLESALE_REPORT_SLUGS = [
    "FVWV",        # National Fresh Fruit and Vegetable Terminal Market
]


def _window_ranges(date_start: str, date_end: str) -> list[tuple[str, str]]:
    """Split a date span into <=180-day windows (AMS per-request cap)."""
    start = datetime.datetime.strptime(date_start, "%Y-%m-%d")
    end = datetime.datetime.strptime(date_end, "%Y-%m-%d")
    windows = []
    cur = start
    while cur <= end:
        w_end = min(cur + datetime.timedelta(days=MAX_DAYS_PER_REQUEST - 1), end)
        windows.append((cur.strftime("%Y-%m-%d"), w_end.strftime("%Y-%m-%d")))
        cur = w_end + datetime.timedelta(days=1)
    return windows


def get_report(slug: str, date_start: str | None = None,
               date_end: str | None = None) -> list[dict]:
    """Fetch a single AMS MARS report slug as a list of raw records."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "consumer-goods-price-pipeline/0.1 (personal research)",
    }
    windows = [(date_start, date_end)] if date_start else [(None, None)]
    if date_start and date_end:
        windows = _window_ranges(date_start, date_end)
    records: list[dict] = []
    for w_start, w_end in windows:
        params = {}
        if w_start:
            params["date_start"] = w_start
            params["date_end"] = w_end
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(f"{AMS_BASE}/{slug}", params=params, headers=headers,
                                 auth=(AMS_API_KEY, ""), timeout=60)
                if r.status_code == 200:
                    payload = r.json()
                    records.extend(payload.get("results", payload.get("data", [])))
                    break
                if r.status_code == 429:
                    wait = BACKOFF_SECONDS * attempt
                    print(f"  429 -- backing off {wait}s (attempt {attempt}/{MAX_RETRIES})")
                    time.sleep(wait)
                elif r.status_code in (401, 403):
                    print(f"  Auth error {r.status_code}: {r.text[:150]} -- check USDA_AMS_API_KEY")
                    return []
                else:
                    print(f"  HTTP {r.status_code}: {r.text[:150]}")
                    return []
            except requests.RequestException as e:
                print(f"  Request error (attempt {attempt}): {e}")
                time.sleep(BACKOFF_SECONDS)
        time.sleep(REQUEST_INTERVAL)
    return records


def parse_reports(records: list[dict]) -> pd.DataFrame:
    """Flatten report records into long-format price observations."""
    rows = []
    for rec in records:
        # MARS records carry commodity / variety / grade / size / price and a
        # report slug plus published date at the record level.
        slug = rec.get("slug") or rec.get("report") or ""
        published = rec.get("published_date") or rec.get("report_date") or ""
        commodity = rec.get("commodity") or rec.get("item")
        price = rec.get("price") or rec.get("low_price")
        rows.append({
            "report":       slug,
            "commodity":    commodity,
            "variety":      rec.get("variety"),
            "grade":        rec.get("grade"),
            "size":         rec.get("size"),
            "unit":         rec.get("unit"),
            "location":     rec.get("location") or rec.get("market") or rec.get("city"),
            "date":         pd.to_datetime(rec.get("date") or published, errors="coerce"),
            "price":        pd.to_numeric(price, errors="coerce"),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["fetched_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return df.dropna(subset=["commodity"])


def main():
    parser = argparse.ArgumentParser(description="USDA AMS market news price pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch full available history")
    args = parser.parse_args()

    if not AMS_API_KEY:
        print("ERROR: No USDA_AMS_API_KEY found. Register free at https://mymarketnews.ams.usda.gov/")
        return

    os.makedirs(os.path.join(OUTPUT_DIR, "ams_wholesale"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "ams_retail"), exist_ok=True)

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    date_start = None if args.backfill else \
        (now - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
    date_end = now.strftime("%Y-%m-%d")
    print(f"Mode: {'BACKFILL' if args.backfill else f'INCREMENTAL (from {date_start})'}")

    for label, slug_list, subdir, prefix in [
        ("retail produce", RETAIL_REPORT_SLUGS, "ams_retail", "usda_ams_retail"),
        ("wholesale terminal", WHOLESALE_REPORT_SLUGS, "ams_wholesale", "usda_ams_wholesale"),
    ]:
        print(f"\n--- {label} ---")
        frames = []
        for slug in slug_list:
            print(f"  {slug}...")
            records = get_report(slug, date_start, date_end)
            if records:
                df = parse_reports(records)
                if not df.empty:
                    frames.append(df)
                    print(f"    {len(df):,} rows")
            time.sleep(REQUEST_INTERVAL)

        if not frames:
            print("[!] No data returned.")
            continue
        combined = pd.concat(frames, ignore_index=True).drop_duplicates()
        path = write_partitioned(
            combined, os.path.join(OUTPUT_DIR, subdir),
            f"{prefix}_{mode}_{today}.parquet",
        )
        print(f"[+] {path}  ({len(combined):,} rows)")

    print("\n--- COMPLETE ---")


if __name__ == "__main__":
    main()
