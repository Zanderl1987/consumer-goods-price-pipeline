#!/usr/bin/env python3
"""
Best Buy Products Pipeline — electronics/appliance prices incl. sale/clearance.

Free instant key (register at https://developer.bestbuy.com/apis). Verified
live 2026-08-04 against the official docs (bestbuyapis.github.io/api-
documentation) — no live key available in this environment yet, so this
SKIPs cleanly via requires_env until BESTBUY_API_KEY is configured, same as
the other keyed pipelines here.

  Base URL : https://api.bestbuy.com/v1/products
  Auth     : apiKey query param
  Query    : parenthesized filter expression, e.g. (search=laptop)
  Format   : ?format=json (default is XML)
  Paging   : page / pageSize (max 100)

Keyword search (`search=term`) is used instead of categoryId filters — the
numeric Best Buy category IDs aren't independently verifiable without a live
key, whereas the search syntax is documented plainly and self-describing.
SEARCH_TERMS below covers the everyday-electronics/appliance spirit of this
repo (TVs, laptops, phones, appliances, small kitchen/home goods) rather
than the full 1M+ SKU catalog.

CLI:
  python bestbuy_products_pipeline.py             # incremental (re-fetch current prices)
  python bestbuy_products_pipeline.py --backfill  # same; API has no price history endpoint

Outputs:
  storage/raw/bestbuy/products/bestbuy_products_{mode}_{YYYYMMDD}.parquet
  (CATALOG: bestbuy_products)
"""

import argparse
import datetime
import os
import time

import pandas as pd
import requests

from storage_utils import write_partitioned

BASE_URL = "https://api.bestbuy.com/v1/products"
OUTPUT_DIR = os.path.join("storage", "raw", "bestbuy", "products")
MAX_RETRIES = 3
BACKOFF_SECONDS = 15
PAGE_SIZE = 100
MAX_PAGES_PER_TERM = 3   # cap at 300 products/term to keep runtime reasonable

SEARCH_TERMS = [
    "television", "laptop", "smartphone", "tablet", "monitor",
    "headphones", "refrigerator", "microwave", "vacuum", "washing machine",
    "dishwasher", "air fryer", "coffee maker", "smartwatch", "gaming console",
]

SHOW_FIELDS = "sku,name,manufacturer,class,department,regularPrice,salePrice,onSale,currency"


def fetch_term(api_key: str, term: str) -> list:
    rows = []
    for page in range(1, MAX_PAGES_PER_TERM + 1):
        params = {
            "apiKey": api_key,
            "format": "json",
            "show": SHOW_FIELDS,
            "pageSize": PAGE_SIZE,
            "page": page,
        }
        url = f"{BASE_URL}(search={term})"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    payload = r.json()
                    products = payload.get("products", [])
                    rows.extend(products)
                    total_pages = payload.get("totalPages", 1)
                    break
                if r.status_code == 429:
                    print(f"    429 rate limited on '{term}' page {page}; backing off")
                    time.sleep(BACKOFF_SECONDS * attempt)
                    continue
                print(f"    HTTP {r.status_code} on '{term}' page {page}: {r.text[:150]}")
                return rows
            except requests.RequestException as exc:
                print(f"    Request error (attempt {attempt}) on '{term}' page {page}: {exc}")
                time.sleep(BACKOFF_SECONDS * attempt)
        else:
            break
        if page >= total_pages:
            break
        time.sleep(0.3)
    return rows


def parse_products(raw_products: list, fetched_at: str) -> pd.DataFrame:
    rows = []
    for p in raw_products:
        rows.append({
            "sku": p.get("sku"),
            "product_name": p.get("name"),
            "manufacturer": p.get("manufacturer"),
            "category": p.get("class"),
            "department": p.get("department"),
            "regular_price": p.get("regularPrice"),
            "price": p.get("salePrice") if p.get("onSale") else p.get("regularPrice"),
            "on_sale": p.get("onSale"),
            "currency": p.get("currency") or "USD",
            "fetched_at": fetched_at,
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Best Buy products pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="No-op vs incremental — the API only exposes current prices")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"Best Buy Products  mode={mode}")

    api_key = os.environ.get("BESTBUY_API_KEY", "")
    if not api_key:
        print("  Could not find BESTBUY_API_KEY.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    frames = []
    for term in SEARCH_TERMS:
        print(f"  Searching '{term}'...")
        products = fetch_term(api_key, term)
        if products:
            frames.append(parse_products(products, now.isoformat()))
            print(f"    {len(products)} products")
        time.sleep(0.3)

    if not frames:
        print("  No products fetched.")
        return

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["sku", "product_name", "price"])
    out = out.drop_duplicates(subset=["sku"]).reset_index(drop=True)
    print(f"  Parsed {len(out):,} unique products")

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"bestbuy_products_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['category'].nunique()} categories)")

    print("\n--- BEST BUY PRODUCTS PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
