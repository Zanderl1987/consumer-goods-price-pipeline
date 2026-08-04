#!/usr/bin/env python3
"""
Open Prices Pipeline — real barcode/category-level price observations
(keyless), formerly "Open Food Facts pipeline".

Re-pointed 2026-08-04. The old version scraped world.openfoodfacts.org's
product search (price fields are thin/mostly empty on the main product DB)
and prices.openfoodfacts.org's live paginated API. Neither is the right
source: the live API has no bulk endpoint (checked every documented route
under prices.openfoodfacts.org/api/v1/ — only CRUD-style resources, no
dump/export), and the "3 gzipped JSONL dumps" a prior research pass expected
don't exist. What DOES exist and is what this pipeline reads: Open Food
Facts publishes a full, actively-updated snapshot of Open Prices as a single
Parquet file on Hugging Face (verified live 2026-08-04, last modified the
day before this pipeline was written).

  https://huggingface.co/datasets/openfoodfacts/open-prices  (prices.parquet)

This is real crowdsourced price data: receipt/price-tag photo proofs,
barcode-level (`type=PRODUCT`) AND loose/no-barcode items like fresh produce
(`type=CATEGORY`, ~3% of rows — this is where avocados/bananas/carrots show
up), with location (OSM shop, city, country, lat/lon), currency, and date.
~285k rows, ODbL licensed.

The dump is always the full current snapshot (no incremental delta exists),
so every run downloads the whole ~30MB file; curated.py's `id` natural key
(the source's own price-observation primary key) makes re-fetches cheap to
dedup. --backfill and incremental fetch identically — the flag only changes
the output filename's mode label, for consistency with the other pipelines'
CLI.

CLI:
  python openfoodfacts_pipeline.py             # fetch current snapshot
  python openfoodfacts_pipeline.py --backfill  # same fetch (dump has no history to extend)

Outputs:
  storage/raw/openfoodfacts/prices/openfoodfacts_prices_{mode}_{YYYYMMDD}.parquet
  (CATALOG: openfoodfacts_prices)
"""

import argparse
import datetime
import io
import os
import time

import pandas as pd
import requests

from storage_utils import write_partitioned

DUMP_URL = "https://huggingface.co/datasets/openfoodfacts/open-prices/resolve/main/prices.parquet"
OUTPUT_DIR = os.path.join("storage", "raw", "openfoodfacts", "prices")
MAX_RETRIES = 3
BACKOFF_SECONDS = 30
HEADERS = {"User-Agent": "consumer-goods-price-pipeline/0.2 (research)"}


def download_dump() -> pd.DataFrame:
    """Download the full Open Prices Parquet snapshot from Hugging Face."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(DUMP_URL, headers=HEADERS, timeout=120)
            if r.status_code == 200:
                return pd.read_parquet(io.BytesIO(r.content))
            print(f"  HTTP {r.status_code}: {r.text[:150]}")
            return pd.DataFrame()
        except (requests.RequestException, OSError) as exc:
            print(f"  Download/parse error (attempt {attempt}): {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)
    return pd.DataFrame()


def parse_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize the Open Prices columns into the store's schema."""
    if raw.empty:
        return raw

    rows = pd.DataFrame({
        "id":                     raw.get("id"),
        "type":                   raw.get("type"),
        "product_code":           raw.get("product_code"),
        "product_name":           raw.get("product_name"),
        "category":               raw.get("category_tag"),
        "price":                  pd.to_numeric(raw.get("price"), errors="coerce"),
        "price_is_discounted":    raw.get("price_is_discounted"),
        "currency":               raw.get("currency"),
        "price_per":              raw.get("price_per"),
        "date":                   pd.to_datetime(raw.get("date"), errors="coerce"),
        "proof_type":             raw.get("proof_type"),
        "location_country":       raw.get("location_osm_address_country"),
        "location_country_code":  raw.get("location_osm_address_country_code"),
        "location_city":          raw.get("location_osm_address_city"),
        "location_shop_type":     raw.get("location_osm_tag_value"),
        "location_lat":           pd.to_numeric(raw.get("location_osm_lat"), errors="coerce"),
        "location_lon":           pd.to_numeric(raw.get("location_osm_lon"), errors="coerce"),
        "owner":                  raw.get("owner"),
        "source":                 raw.get("source"),
    })
    return rows.dropna(subset=["id", "price", "currency", "date"])


def main():
    parser = argparse.ArgumentParser(description="Open Prices (Open Food Facts) pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Same fetch as incremental — the dump has no separate history to extend")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"Open Prices  mode={mode}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("  Downloading prices.parquet from Hugging Face...")
    raw = download_dump()
    if raw.empty:
        print("  No data downloaded.")
        return

    out = parse_frame(raw)
    print(f"  Parsed {len(out):,} observations")

    if out.empty:
        print("  Nothing to write.")
        return

    out["fetched_at"] = now.isoformat()
    out = out.sort_values(["id"]).reset_index(drop=True)

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"openfoodfacts_prices_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['currency'].nunique()} currencies, "
          f"{out['location_country'].nunique()} countries)")

    print("\n--- OPEN PRICES PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
