#!/usr/bin/env python3
"""
WFP Global Food Prices Pipeline — retail/wholesale food prices, 98 countries.

World Food Programme price observations via the Humanitarian Data Exchange
(HDX), dataset `global-wfp-food-prices`: maize, rice, beans, fish, sugar,
bread and more, per market, since 1990. Real per-observation `date` column
(not just month/year) plus both local-currency `price` and normalized
`usdprice`.

IMPORTANT: the dataset literally named `wfp-food-prices` on HDX is a DEAD,
no-longer-updated snapshot frozen at 2021-08 (verified live 2026-08-04) — its
own metadata says it "has been replaced by" `global-wfp-food-prices`, which is
what this pipeline reads. Don't switch back to the old slug.

Access is keyless via HDX's CKAN API. Verified live 2026-08-04:
  Metadata : GET https://data.humdata.org/api/3/action/package_show?id=global-wfp-food-prices
  Data     : one CSV resource per calendar year (e.g. "Global WFP food prices
             2026"), named f"Global WFP food prices {year}"; a stray "1900"
             resource holds rows with unparseable source dates and is skipped.

Because the whole dataset is split into per-year files with no incremental
delta endpoint, every run re-downloads the relevant year(s) and curated.py
dedups. --backfill fetches every year 1990->present (~150-250MB total);
incremental fetches only the current + prior calendar year (revisions land
in the most recent data, same overlap logic as statcan_retail_prices).

CLI:
  python wfp_food_prices_pipeline.py             # incremental (current + prior year)
  python wfp_food_prices_pipeline.py --backfill  # full history 1990->present

Outputs:
  storage/raw/wfp/food_prices/wfp_food_prices_{mode}_{YYYYMMDD}.parquet
  (CATALOG: wfp_food_prices)
"""

import argparse
import datetime
import io
import os
import re
import time

import pandas as pd
import requests

from storage_utils import write_partitioned

CKAN_PACKAGE_URL = "https://data.humdata.org/api/3/action/package_show"
DATASET_ID = "global-wfp-food-prices"
EARLIEST_YEAR = 1990

OUTPUT_DIR = os.path.join("storage", "raw", "wfp", "food_prices")
MAX_RETRIES = 3
BACKOFF_SECONDS = 30

_YEAR_RESOURCE_RE = re.compile(r"^Global WFP food prices (\d{4})$")


def get_year_resources() -> dict[int, str]:
    """Resolve {year: download_url} for every real per-year CSV resource."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(CKAN_PACKAGE_URL, params={"id": DATASET_ID}, timeout=60)
            if r.status_code != 200:
                print(f"  HTTP {r.status_code}: {r.text[:150]}")
                return {}
            resources = r.json()["result"]["resources"]
            out = {}
            for res in resources:
                m = _YEAR_RESOURCE_RE.match(res.get("name", ""))
                if not m:
                    continue
                out[int(m.group(1))] = res["url"]
            return out
        except requests.RequestException as exc:
            print(f"  Request error (attempt {attempt}): {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)
        except (KeyError, ValueError) as exc:
            print(f"  Unexpected response shape: {exc}")
            return {}
    return {}


def download_year(url: str) -> pd.DataFrame:
    """Download one year's CSV into a DataFrame."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                return pd.read_csv(io.BytesIO(r.content), low_memory=False)
            print(f"  HTTP {r.status_code}: {r.text[:150]}")
            return pd.DataFrame()
        except (requests.RequestException, pd.errors.ParserError) as exc:
            print(f"  Download/parse error (attempt {attempt}): {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)
    return pd.DataFrame()


def parse_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize the WFP CSV columns into the store's schema."""
    if raw.empty:
        return raw

    rows = pd.DataFrame({
        "countryiso3":   raw.get("countryiso3"),
        "admin1":        raw.get("admin1"),
        "admin2":        raw.get("admin2"),
        "market":        raw.get("market"),
        "market_id":     raw.get("market_id"),
        "latitude":      pd.to_numeric(raw.get("latitude"), errors="coerce"),
        "longitude":     pd.to_numeric(raw.get("longitude"), errors="coerce"),
        "category":      raw.get("category"),
        "commodity":     raw.get("commodity"),
        "commodity_id":  raw.get("commodity_id"),
        "unit":          raw.get("unit"),
        "priceflag":     raw.get("priceflag"),
        "pricetype":     raw.get("pricetype"),
        "currency":      raw.get("currency"),
        "date":          pd.to_datetime(raw.get("date"), errors="coerce"),
        "price":         pd.to_numeric(raw.get("price"), errors="coerce"),
        "usdprice":      pd.to_numeric(raw.get("usdprice"), errors="coerce"),
    })
    return rows.dropna(subset=["countryiso3", "commodity", "date", "price"])


def main():
    parser = argparse.ArgumentParser(description="WFP global food prices pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch full 1990->present history (default: current + prior year)")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"WFP Global Food Prices  mode={mode}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("  Resolving year resources...")
    year_urls = get_year_resources()
    if not year_urls:
        print("  Could not resolve dataset resources. See messages above.")
        return

    if args.backfill:
        years = sorted(y for y in year_urls if EARLIEST_YEAR <= y <= now.year)
    else:
        years = sorted(y for y in year_urls if y in (now.year, now.year - 1))

    if not years:
        print("  No matching year resources found.")
        return

    frames = []
    for year in years:
        print(f"  {year}...", end=" ", flush=True)
        df = download_year(year_urls[year])
        print(f"{len(df):,} rows")
        if not df.empty:
            frames.append(df)

    if not frames:
        print("  No data downloaded.")
        return

    combined = pd.concat(frames, ignore_index=True)
    out = parse_frame(combined)
    print(f"  Parsed {len(out):,} observations across {len(years)} year file(s)")

    if out.empty:
        print("  Nothing to write.")
        return

    out["fetched_at"] = now.isoformat()
    out = out.sort_values(["countryiso3", "market_id", "commodity_id", "date"]).reset_index(drop=True)

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"wfp_food_prices_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['countryiso3'].nunique()} countries, "
          f"{out['commodity'].nunique()} commodities, {out['market'].nunique()} markets)")

    print("\n--- WFP GLOBAL FOOD PRICES PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
