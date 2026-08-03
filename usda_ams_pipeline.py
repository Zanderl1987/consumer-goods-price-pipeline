#!/usr/bin/env python3
"""
USDA AMS Market News Pipeline — wholesale terminal and retail prices for fresh
produce (including avocados) and other specialty crops.

The USDA Agricultural Marketing Service publishes daily/weekly market news
reports. The public API requires a free registration key at:
  https://mymarketnews.ams.usda.gov/  (USDA_AMS_API_KEY)

Reports of interest for a consumer-goods tracker:
  - "National Retail Report - Specialty Crops"  (weekly retail produce prices)
  - "National Fruit and Vegetable Retail Report"
  - Terminal market wholesale prices (e.g. Hass avocados, Los Angeles/New York)

STATUS: scaffold. The AMS API base URL / report IDs below follow the published
API contract (api/v1/data/reports) but MUST be verified live on first run —
see docs/SOURCES.md for the endpoints confirmed during research.

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
AMS_BASE = "https://mymarketnews.ams.usda.gov/api/v1/data/reports"

OUTPUT_DIR = os.path.join("storage", "raw", "usda")
REQUEST_INTERVAL = 0.5
MAX_RETRIES = 3
BACKOFF_SECONDS = 30

# Report slugs / IDs from the AMS Market News API catalog. Verify live.
# Retail specialty-crops report and key wholesale reports:
RETAIL_REPORTS = [
    "National Retail Report - Specialty Crops",          # weekly retail produce
    "National Fruit and Vegetable Retail Report",        # national retail F/V
]

WHOLESALE_REPORTS = [
    "Hass Avocado Price Report",                         # terminal avocado prices
    "National Fresh Fruit and Vegetable Terminal Market",# terminal wholesale
]


def get_report(report_name: str, date_start: str | None = None,
               date_end: str | None = None) -> list[dict]:
    """Fetch a single AMS Market News report as a list of raw records."""
    params = {"api_key": AMS_API_KEY}
    if date_start:
        params["date_start"] = date_start
    if date_end:
        params["date_end"] = date_end
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(AMS_BASE, params=params, headers={"Accept": "application/json"},
                             timeout=60)
            if r.status_code == 200:
                payload = r.json()
                reports = payload.get("data", [])
                # Filter client-side by report slug if server doesn't
                return [rep for rep in reports if report_name.lower() in
                        rep.get("slug", "").lower()]
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
    return []


def parse_reports(reports: list[dict]) -> pd.DataFrame:
    """Flatten report records into long-format price observations."""
    rows = []
    for rep in reports:
        slug = rep.get("slug", "")
        published = rep.get("published_date", "")
        for rec in rep.get("data", []):
            # AMS records carry commodity / variety / grade / size / price
            commodity = rec.get("commodity") or rec.get("item")
            price = rec.get("price") or rec.get("low_price")
            rows.append({
                "report":       slug,
                "commodity":    commodity,
                "variety":      rec.get("variety"),
                "grade":        rec.get("grade"),
                "size":         rec.get("size"),
                "unit":         rec.get("unit"),
                "location":     rec.get("location") or rec.get("market"),
                "date":         pd.to_datetime(rec.get("date") or published, errors="coerce"),
                "price":        pd.to_numeric(price, errors="coerce"),
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()
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

    today = datetime.datetime.utcnow().strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    date_start = None if args.backfill else \
        (datetime.datetime.utcnow() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
    print(f"Mode: {'BACKFILL' if args.backfill else f'INCREMENTAL (from {date_start})'}")

    for label, report_list, subdir, prefix in [
        ("retail produce", RETAIL_REPORTS, "ams_retail", "usda_ams_retail"),
        ("wholesale terminal", WHOLESALE_REPORTS, "ams_wholesale", "usda_ams_wholesale"),
    ]:
        print(f"\n--- {label} ---")
        frames = []
        for report in report_list:
            print(f"  {report}...")
            reps = get_report(report, date_start)
            if reps:
                df = parse_reports(reps)
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
