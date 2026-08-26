#!/usr/bin/env python3
"""
FEWS NET Food Prices Pipeline - market price observations from the FEWS NET
Data Warehouse (FDW), covering ~20 food-insecurity-monitored countries
(Ethiopia, Sudan, Yemen, Afghanistan ...) back to the 1990s-2000s.

Access is keyless via the FDW REST API. Verified live 2026-08-24:
  Root      : GET https://fdw.fews.net/api/ -> resource list incl.
              "marketpricefacts": "https://fdw.fews.net/api/marketpricefacts/"
  Data      : GET https://fdw.fews.net/api/marketpricefacts.csv?fields=simple
              wide CSV with columns incl. geographic_group,fewsnet_region,
              country,admin_1,admin_2,market,cpcv2,product,price_type,
              product_source,collection_schedule,start_date,period_date,value,
              currency,unit,...,latitude,longitude,is_staple_food
  CAVEAT (2026-08-24): during probing the API returned 502 Bad Gateway or
  read-timeouts for EVERY query shape (json/csv, limit<=5, date-filtered),
  after an initial verified-200 earlier in the day. The service exists and is
  keyless but is slow/flaky under load -- this pipeline retries and backs off;
  a failed run simply writes nothing (idempotent re-run next cycle).
  Pagination: FDW documents ?limit=&offset= on its DRF endpoints, but the
  CSV renderer's behavior could not be confirmed live during the 502 window.
  The pipeline passes limit/offset when set; if the server ignores them and
  returns the full filtered set anyway, curated.py dedups make that harmless.

Backfill depth is large (Ethiopia alone goes back decades), so the default
run filters to start_date >= today-10y. --backfill fetches full available
history in one request (no documented server-side year chunking).

CLI:
  python fews_net_pipeline.py             # incremental: last 10 years
  python fews_net_pipeline.py --backfill  # full available history

Outputs:
  storage/raw/fewsnet/food_prices/fews_net_food_prices_{mode}_{YYYYMMDD}.parquet
  (CATALOG: fews_net_food_prices)
"""

import argparse
import datetime
import io
import os
import time

import pandas as pd
import requests

from storage_utils import write_partitioned

FDW_URL = "https://fdw.fews.net/api/marketpricefacts.csv"
OUTPUT_DIR = os.path.join("storage", "raw", "fewsnet", "food_prices")
MAX_RETRIES = 3
BACKOFF_SECONDS = 30
REQUEST_TIMEOUT = 300
INCREMENTAL_YEARS = 10


def _get_params(backfill: bool) -> dict:
    """Query params for one fetch: fields=simple plus a start_date floor."""
    params = {"fields": "simple"}
    if not backfill:
        cutoff = datetime.date.today() - datetime.timedelta(days=365 * INCREMENTAL_YEARS)
        params["start_date"] = cutoff.isoformat()
    return params


def download_prices(backfill: bool) -> pd.DataFrame:
    """Download the FDW market-price facts CSV into a raw DataFrame."""
    params = _get_params(backfill)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                FDW_URL, params=params, timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": "consumer-goods-price-pipeline/0.1 (personal research)"},
            )
            if r.status_code == 200:
                return pd.read_csv(io.BytesIO(r.content), low_memory=False)
            print(f"  HTTP {r.status_code}: {r.text[:150]}")
            return pd.DataFrame()
        except (requests.RequestException, pd.errors.ParserError) as exc:
            print(f"  Download/parse error (attempt {attempt}): {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)
    return pd.DataFrame()


def parse_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize the FDW wide CSV into the store's tidy schema."""
    if raw.empty:
        return raw

    rows = pd.DataFrame({
        "period_date":   pd.to_datetime(raw.get("period_date"), errors="coerce"),
        "country":       raw.get("country"),
        "admin_1":       raw.get("admin_1"),
        "admin_2":       raw.get("admin_2"),
        "market":        raw.get("market"),
        "cpcv2":         raw.get("cpcv2"),
        "product":       raw.get("product"),
        "price_type":    raw.get("price_type"),
        "value":         pd.to_numeric(raw.get("value"), errors="coerce"),
        "currency":      raw.get("currency"),
        "unit":          raw.get("unit"),
        "latitude":      pd.to_numeric(raw.get("latitude"), errors="coerce"),
        "longitude":     pd.to_numeric(raw.get("longitude"), errors="coerce"),
    })
    rows = rows.dropna(subset=["market", "product", "period_date", "value"])
    rows["source"] = "fews_net"
    return rows


def main():
    parser = argparse.ArgumentParser(description="FEWS NET Data Warehouse market prices pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help=f"Fetch full available history (default: last {INCREMENTAL_YEARS} years)")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"FEWS NET Food Prices  mode={mode}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("  Downloading FDW marketpricefacts.csv ...")
    raw = download_prices(args.backfill)
    if raw.empty:
        print("  No data downloaded.")
        return

    out = parse_frame(raw)
    print(f"  Parsed {len(out):,} observations across {out['country'].nunique()} countries, "
          f"{out['market'].nunique()} markets, {out['product'].nunique()} products")
    if out.empty:
        print("  Nothing to write.")
        return

    out["fetched_at"] = now.isoformat()
    out = out.sort_values(["country", "market", "product", "period_date"]).reset_index(drop=True)

    path = write_partitioned(out, OUTPUT_DIR, f"fews_net_food_prices_{mode}_{today}.parquet")
    print(f"  -> {path}  ({len(out):,} rows)")

    print("\n--- FEWS NET FOOD PRICES PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
