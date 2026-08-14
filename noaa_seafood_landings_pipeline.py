#!/usr/bin/env python3
"""
NOAA Fisheries Commercial Landings Pipeline — ex-vessel (dockside) seafood
prices, the closest free source to a producer/farmgate price for seafood
(mirrors usda_prices_received's role for crops/livestock; this repo had no
seafood category before).

Source: NOAA Fisheries One Stop Shop (FOSS), Commercial Landings table, via
its keyless public REST API (Oracle ORDS) at apps-st.fisheries.noaa.gov.
Verified live 2026-08-14:
  Endpoint : GET https://apps-st.fisheries.noaa.gov/ods/foss/landings/
  Filter   : ?q={"collection":"Commercial"}  (recreational MRIP rows have
             dollars=null and are excluded)
  Paginate : limit/offset, up to 10,000 rows/page confirmed working
  Shape    : ~159k rows, annual, per species x state x region x source,
             1950-present (2024 is the latest complete year as of this run)
  No API key, no documented rate limit hit during a full 16-page pull.

NOTE: the old NEFSC "Boston/NY Market News" page (nefsc.noaa.gov/read/
socialsci/marketNews.php) is dead -- 301s to a generic region page and its
own InPort metadata flags internal-network access constraints. The FOSS ODS
REST API (found via InPort item 10574's "ords/foss/metadata-catalog" link,
which itself redirects to the current apps-st.fisheries.noaa.gov host) is
the real, live, public replacement.

price_per_lb = dollars / pounds is derived client-side; NOAA does not
publish a per-unit price field directly.

CLI:
  python noaa_seafood_landings_pipeline.py             # incremental (last 5 years)
  python noaa_seafood_landings_pipeline.py --backfill  # full history from 1950

Outputs:
  storage/raw/noaa/seafood_landings/noaa_seafood_landings_{mode}_{YYYYMMDD}.parquet
  (CATALOG: noaa_seafood_landings)
"""

import argparse
import datetime
import os
import time

import pandas as pd
import requests

from storage_utils import write_partitioned

FOSS_BASE = "https://apps-st.fisheries.noaa.gov/ods/foss/landings/"
OUTPUT_DIR = os.path.join("storage", "raw", "noaa", "seafood_landings")
PAGE_SIZE = 10000
MAX_RETRIES = 3
BACKOFF_SECONDS = 30
REQUEST_INTERVAL = 0.5
BACKFILL_START_YEAR = 1950
INCREMENTAL_YEARS = 5


def fetch_commercial_landings() -> pd.DataFrame:
    """Paginate the full FOSS Commercial-collection landings table."""
    frames = []
    params = {"q": '{"collection":"Commercial"}', "limit": PAGE_SIZE, "offset": 0}
    page = 0
    while True:
        page += 1
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(FOSS_BASE, params=params, timeout=60)
                if r.status_code == 200:
                    break
                print(f"  HTTP {r.status_code} on page {page} (attempt {attempt})")
                time.sleep(BACKOFF_SECONDS * attempt)
            except requests.RequestException as exc:
                print(f"  Request error page {page} (attempt {attempt}): {exc}")
                time.sleep(BACKOFF_SECONDS * attempt)
        else:
            print(f"  Giving up on page {page} after {MAX_RETRIES} attempts.")
            break

        data = r.json()
        items = data.get("items", [])
        if items:
            frames.append(pd.DataFrame(items))
        print(f"  page {page}: {len(items)} rows (offset {params['offset']})")
        if not data.get("hasMore") or not items:
            break
        params["offset"] += PAGE_SIZE
        time.sleep(REQUEST_INTERVAL)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "tsn":                "species_tsn",
        "ts_afs_name":        "species",
        "ts_scientific_name": "scientific_name",
        "region_name":        "region",
        "state_name":         "state",
        # "year" is a reserved Hive partition name in this repo (write_partitioned
        # / query.py's hive_partitioning=True silently overwrites a same-named
        # data column on read-back) -- renamed, same fix as cms_drug_pricing.
        "year":               "landing_year",
    })
    df["pounds"] = pd.to_numeric(df["pounds"], errors="coerce")
    df["dollars"] = pd.to_numeric(df["dollars"], errors="coerce")
    df["price_per_lb"] = df["dollars"] / df["pounds"].where(df["pounds"] > 0)
    df["date"] = pd.to_datetime(
        df["landing_year"].astype("Int64").astype(str) + "-01-01", errors="coerce"
    )
    keep = [
        "species_tsn", "species", "scientific_name", "region", "state",
        "landing_year", "date", "pounds", "dollars", "price_per_lb",
        "tot_count", "source", "collection",
    ]
    df = df[[c for c in keep if c in df.columns]]
    df = df.dropna(subset=["species", "date"])
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()
    return df.sort_values(["species", "state", "landing_year"]).reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="NOAA FOSS commercial seafood landings pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help=f"Keep full history from {BACKFILL_START_YEAR}")
    args = parser.parse_args()

    now = datetime.datetime.utcnow()
    today_str = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"

    print(f"NOAA FOSS Commercial Landings  mode={mode}")
    raw = fetch_commercial_landings()
    if raw.empty:
        print("  No data returned.")
        return

    df = clean(raw)
    if not args.backfill:
        cutoff = now.year - INCREMENTAL_YEARS
        df = df[df["landing_year"] >= cutoff]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = write_partitioned(
        df, OUTPUT_DIR,
        f"noaa_seafood_landings_{mode}_{today_str}.parquet",
    )
    print(f"  -> {path}  ({len(df):,} rows, {df['species'].nunique()} species, "
          f"{df['landing_year'].min()}-{df['landing_year'].max()})")
    print("\n--- NOAA SEAFOOD LANDINGS PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
