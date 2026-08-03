#!/usr/bin/env python3
"""
Open Food Facts Pipeline — crowdsourced grocery product prices (keyless).

Open Food Facts is the free, open, crowdsourced food-product database
(5M+ products). Its product records include contributor-entered prices, store
location, and category tags. The prices sub-project
(prices.openfoodfacts.org) collects standalone price observations with
currency, date, and location.

This is a keyless source — no API key required, but be polite:
~10 req/sec max; keep page sizes small. It is a supplementary/crowdsourced
source, not a canonical one.

CLI:
  python openfoodfacts_pipeline.py

Outputs:
  storage/raw/openfoodfacts/prices/openfoodfacts_prices_{YYYYMMDD}.parquet  (CATALOG: openfoodfacts_prices)
"""

import argparse
import datetime
import json
import os
import time

import pandas as pd
import requests

from storage_utils import write_partitioned

OFF_BASE = "https://world.openfoodfacts.org"
OFF_SEARCH = OFF_BASE + "/cgi/search.pl"
PRICES_API = "https://prices.openfoodfacts.org/api/v1/prices"

OUTPUT_DIR = os.path.join("storage", "raw", "openfoodfacts", "prices")
REQUEST_INTERVAL = 0.3
MAX_RETRIES = 3
BACKOFF_SECONDS = 30
MAX_PAGES = 3  # keep it light for a crowdsourced source

# Categories that map to a "tires to avocados" grocery tracker
CATEGORIES = [
    "en:avocados",
    "en:milk",
    "en:eggs",
    "en:bread",
    "en:bananas",
    "en:ground-beef",
    "en:butter",
    "en:coffee",
    "en:chicken-breasts",
]


def get_with_backoff(url, params=None, headers=None):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers or {}, timeout=60)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                wait = BACKOFF_SECONDS * attempt
                print(f"  429 -- backing off {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                print(f"  HTTP {r.status_code}: {r.text[:120]}")
                return None
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt}): {e}")
            time.sleep(BACKOFF_SECONDS)
    return None


def search_category(category: str) -> list[dict]:
    """Return product docs for a category from the search API."""
    products = []
    for page in range(1, MAX_PAGES + 1):
        params = {
            "action": "process",
            "tagtype_0": "categories",
            "tag_contains_0": "contains",
            "tag_0": category,
            "fields": "code,product_name,brands,categories_tags,countries_tags,"
                      "stores,price,price_per_kg,quantity,origins_tags",
            "page": page,
            "page_size": "100",
            "json": "1",
        }
        r = get_with_backoff(OFF_SEARCH, params=params,
                             headers={"User-Agent": "consumer-goods-price-pipeline/0.1 (research)"})
        if not r:
            break
        data = r.json()
        batch = data.get("products", [])
        products.extend(batch)
        if not batch or page >= data.get("page_count", 1):
            break
        time.sleep(REQUEST_INTERVAL)
    return products


def fetch_recent_prices() -> list[dict]:
    """Try the Open Food Facts prices sub-API for recent price observations."""
    r = get_with_backoff(PRICES_API + "?limit=100",
                         headers={"User-Agent": "consumer-goods-price-pipeline/0.1 (research)"})
    if not r:
        return []
    try:
        return r.json().get("items", r.json().get("data", []))
    except Exception:
        return []


def products_to_df(products: list[dict]) -> pd.DataFrame:
    rows = []
    for p in products:
        price = p.get("price")
        if price is None:
            continue
        rows.append({
            "code":          p.get("code"),
            "product_name":  p.get("product_name"),
            "brands":        p.get("brands"),
            "category":      p.get("categories_tags", []),
            "countries":     p.get("countries_tags", []),
            "stores":        p.get("stores"),
            "price":         price,
            "price_per_kg":  p.get("price_per_kg"),
            "quantity":      p.get("quantity"),
            "source":        "openfoodfacts_search",
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Open Food Facts crowdsourced grocery price pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch all available pages (currently capped)")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    today = datetime.datetime.utcnow().strftime("%Y%m%d")
    now = datetime.datetime.utcnow()

    all_products = []
    for cat in CATEGORIES:
        print(f"  {cat}...", end=" ", flush=True)
        batch = search_category(cat)
        if batch:
            all_products.extend(batch)
            print(f"{len(batch)} products")
        else:
            print("none")
        time.sleep(REQUEST_INTERVAL)

    frames = []
    df = products_to_df(all_products)
    if not df.empty:
        frames.append(df)

    print("  prices sub-api...", end=" ", flush=True)
    recent = fetch_recent_prices()
    if recent:
        pr = pd.DataFrame(recent)
        pr["source"] = "openfoodfacts_prices"
        frames.append(pr)
        print(f"{len(recent)} observations")
    else:
        print("none (endpoint may have moved — see docs/SOURCES.md)")

    if not frames:
        print("[!] No price data returned.")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined["fetched_at"] = now.isoformat()

    path = write_partitioned(combined, OUTPUT_DIR,
                             f"openfoodfacts_prices_{today}.parquet")
    print(f"\n[+] {path}  ({len(combined):,} rows)")


if __name__ == "__main__":
    main()
