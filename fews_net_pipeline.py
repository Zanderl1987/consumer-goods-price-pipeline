#!/usr/bin/env python3
"""
FEWS NET Market Prices Pipeline.

Downloads market price observations from the FEWS NET Data Warehouse website
export renderer (keyless, no auth). Covers retail/wholesale food prices
across monitored markets in Africa, the Middle East, South/Southeast Asia,
and Latin America.

WHY THIS DESIGN (learned 2026-08-24 and 2026-09-05):
  The warehouse's raw API renderers (/api/marketpricefacts.csv?fields=...)
  hang indefinitely for every observation resource, but the SAME endpoint
  used behind the website's "Download File" buttons works reliably when
  scoped to ONE country:
    /api/marketpricefacts/?dataset=FEWS_NET_Staple_Food_Price_Data
        &country=<ISO>&format=csv&fields=body
  The unscoped whole-dataset request (~267MB, all countries) is flaky (500s
  on nearly every attempt), so the pipeline ALWAYS fetches per country.
  Country codes are discovered at run time from /api/market.json, the one
  metadata renderer that consistently works; countries with no observations
  return an empty body and are skipped. No hardcoded country list to drift.

  fields=body (equivalent to omitting fields) returns the full 63-column
  schema: value, unit, currency, period_date, market lat/lon, cpcv2
  commodity code, exchange_rate, common-currency price, period-over-period
  pct changes, and is_staple_food. fields=website restricts to the site's
  small default table and must NOT be used.

ToS/attribution: FEWS NET's data use and attribution policy explicitly
permits API access; redistribution requires attribution (docs/SOURCES.md).

CLI:
  python fews_net_pipeline.py              # incremental (last 10 years per country)
  python fews_net_pipeline.py --backfill   # full history per country

Output:
  storage/raw/fews_net/fews_net_food_prices_{mode}_{YYYYMMDD}.parquet
"""

import argparse
import datetime
import json
import os
import time
from io import StringIO

import pandas as pd
import requests

from storage_utils import write_partitioned

BASE_DIR = os.path.join("storage", "raw", "fews_net")
DATASET = "FEWS_NET_Staple_Food_Price_Data"
API_FACTS_URL = "https://fdw.fews.net/api/marketpricefacts/"
API_MARKETS_URL = "https://fdw.fews.net/api/market.json"

MAX_RETRIES = 3
BACKOFF_SECONDS = 30
REQUEST_TIMEOUT = 300
HEADERS = {"User-Agent": "Mozilla/5.0 consumer-goods-price-pipeline/1.0"}

DEFAULT_WINDOW_YEARS = 10

# Matches curated.py KEYS for fews_net_food_prices.
DEDUP_COLS = ["country", "market", "cpcv2", "price_type", "period_date"]


def _country_codes() -> list[str]:
    """Discover candidate ISO country codes from the working market metadata renderer."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(API_MARKETS_URL, headers=HEADERS, timeout=60)
            if r.status_code == 200:
                d = r.json()
                markets = d if isinstance(d, list) else d.get("value", [])
                return sorted({m.get("country_code") for m in markets if m.get("country_code")})
            time.sleep(BACKOFF_SECONDS * attempt)
        except requests.RequestException:
            time.sleep(BACKOFF_SECONDS * attempt)
    return []


def _fetch_country(code: str, start_date: str | None) -> pd.DataFrame:
    """Fetch one country's observations; returns empty DataFrame on failure."""
    params = {"dataset": DATASET, "country": code, "format": "csv", "fields": "body"}
    if start_date:
        params["start_date"] = start_date
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(API_FACTS_URL, params=params, headers=HEADERS,
                             timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and r.content:
                try:
                    return pd.read_csv(StringIO(r.text), low_memory=False, thousands=",")
                except Exception:
                    return pd.DataFrame()
            if r.status_code in (500, 502, 503, 504):
                print(f"server {r.status_code}, retrying...", end=" ")
                time.sleep(BACKOFF_SECONDS * attempt)
                continue
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS)
            else:
                return pd.DataFrame()
        except requests.Timeout:
            print(f"timeout, retrying...", end=" ")
            time.sleep(BACKOFF_SECONDS)
        except requests.RequestException as e:
            print(f"error {e}, retrying...", end=" ")
            time.sleep(BACKOFF_SECONDS * attempt)
    return pd.DataFrame()


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names, types, and add source/fetched_at."""
    if len(df) == 0 or df.columns.size == 0:
        return pd.DataFrame()
    df = df.copy()
    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.strip("_")
    )
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    if "period_date" in df.columns:
        df["period_date"] = pd.to_datetime(df["period_date"], errors="coerce")
    df["source"] = "fews_net"
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()
    drop = [c for c in DEDUP_COLS if c in df.columns]
    df = df.dropna(subset=drop)
    return df


def main(backfill: bool = False) -> None:
    now = datetime.datetime.utcnow()
    today_str = now.strftime("%Y%m%d")
    mode = "backfill" if backfill else "incremental"
    start_date = None
    if not backfill:
        start_date = (now - datetime.timedelta(days=DEFAULT_WINDOW_YEARS * 365)).strftime("%Y-%m-%d")
    print(f"FEWS NET Market Prices Pipeline  mode={mode}" +
          (f"  start_date={start_date}" if start_date else ""))

    os.makedirs(BASE_DIR, exist_ok=True)

    codes = _country_codes()
    if not codes:
        print("Could not read the country list from market.json - nothing written.")
        return
    print(f"  {len(codes)} candidate countries from market.json")

    frames: list[pd.DataFrame] = []
    empty = 0
    for i, code in enumerate(codes, 1):
        df = _fetch_country(code, start_date)
        if df.empty:
            empty += 1
            continue
        df = _normalize(df)
        df = df.drop_duplicates()
        df = df.drop_duplicates(subset=[c for c in DEDUP_COLS if c in df.columns])
        frames.append(df)
        print(f"  [{i}/{len(codes)}] {code}: {len(df):,} rows")
        time.sleep(0.5)

    if not frames:
        print("\nNo data returned from FEWS NET (service may be unavailable).")
        print("Nothing written.")
        return

    combined = pd.concat(frames, ignore_index=True).reset_index(drop=True)
    print(f"\n  total: {len(combined):,} rows | {len(frames)} countries with data | {empty} empty")

    path = write_partitioned(
        combined, BASE_DIR,
        f"fews_net_food_prices_{mode}_{today_str}.parquet",
    )
    print(f"\n-> {path}")
    print(f"   {len(combined):,} rows | columns: {list(combined.columns)}")

    print("\n--- FEWS NET MARKET PRICES PIPELINE COMPLETE ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FEWS NET Data Warehouse market prices (food-insecure countries, keyless)")
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch full history per country (default: last 10 years)")
    args = parser.parse_args()
    main(backfill=args.backfill)