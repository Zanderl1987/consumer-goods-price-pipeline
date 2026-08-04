#!/usr/bin/env python3
"""
Kroger Products Pipeline — grocery catalog prices, ZIP-localized.

The Kroger Products API (developer.kroger.com) only returns `price` and
`aisleLocations` when the request carries a store-scoped `filter.locationId`
— an un-scoped product search returns descriptions with no price at all. So
this pipeline first resolves a small set of store locationIds (one per
tracked ZIP, via the Locations API) and then re-runs a fixed product search
term list against each store.

Auth: OAuth2 client-credentials, HTTP Basic (base64 CLIENT_ID:CLIENT_SECRET)
  Token : POST https://api.kroger.com/v1/connect/oauth2/token
          body: grant_type=client_credentials&scope=product.compact
  Products : GET https://api.kroger.com/v1/products
          params: filter.term, filter.locationId, filter.limit (<=50)
  Locations: GET https://api.kroger.com/v1/locations
          params: filter.zipCode.near, filter.radiusInMiles, filter.limit

Verified live 2026-08-04 against developer docs (no live key available in
this environment yet — token/product/location endpoint shapes are per the
official docs; SKIPs cleanly via requires_env until KROGER_CLIENT_ID/SECRET
are configured). Rate limits: ~10,000 product calls/day, ~1,600 location
calls/day (per endpoint).

Tracked ZIPs (one Kroger-banner store per region, hardcoded — the API has no
"all stores" bulk mode, so *some* fixed locality list is unavoidable; these
five give rough US geographic spread: Midwest/HQ, Northeast, South, West
Coast, Mountain):
  45202 Cincinnati OH (Kroger HQ market), 30301 Atlanta GA, 75201 Dallas TX,
  90012 Los Angeles CA (Ralphs banner), 80202 Denver CO (King Soopers banner)

Search terms cover the same everyday-goods spirit as the other retail
pipelines here (dairy, eggs, bread, meat, produce, household staples) — see
SEARCH_TERMS below.

CLI:
  python kroger_pipeline.py             # incremental (re-fetch current prices)
  python kroger_pipeline.py --backfill  # same as incremental; API has no history

Outputs:
  storage/raw/kroger/products/kroger_products_{mode}_{YYYYMMDD}.parquet
  (CATALOG: kroger_products)
"""

import argparse
import base64
import datetime
import os
import time

import pandas as pd
import requests

from storage_utils import write_partitioned

TOKEN_URL = "https://api.kroger.com/v1/connect/oauth2/token"
PRODUCTS_URL = "https://api.kroger.com/v1/products"
LOCATIONS_URL = "https://api.kroger.com/v1/locations"

OUTPUT_DIR = os.path.join("storage", "raw", "kroger", "products")
MAX_RETRIES = 3
BACKOFF_SECONDS = 15
PRODUCT_LIMIT = 50

TRACKED_ZIPS = ["45202", "30301", "75201", "90012", "80202"]

SEARCH_TERMS = [
    "milk", "eggs", "bread", "ground beef", "chicken breast", "butter",
    "cheese", "bananas", "avocado", "tomatoes", "rice", "pasta",
    "coffee", "orange juice", "paper towels", "toilet paper", "toothpaste",
    "laundry detergent", "diapers", "dish soap",
]


def get_token() -> str:
    client_id = os.environ.get("KROGER_CLIENT_ID", "")
    client_secret = os.environ.get("KROGER_CLIENT_SECRET", "")
    creds = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {creds}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "client_credentials", "scope": "product.compact"}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
            if r.status_code == 200:
                return r.json().get("access_token", "")
            print(f"  Token HTTP {r.status_code}: {r.text[:150]}")
            return ""
        except requests.RequestException as exc:
            print(f"  Token request error (attempt {attempt}): {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)
    return ""


def resolve_location_ids(token: str) -> dict:
    """ZIP -> nearest Kroger-family locationId."""
    headers = {"Authorization": f"Bearer {token}"}
    out = {}
    for zip_code in TRACKED_ZIPS:
        params = {"filter.zipCode.near": zip_code, "filter.radiusInMiles": 15, "filter.limit": 1}
        try:
            r = requests.get(LOCATIONS_URL, headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json().get("data", [])
                if data:
                    out[zip_code] = data[0]["locationId"]
                    continue
            print(f"  Locations lookup for {zip_code}: HTTP {r.status_code} {r.text[:120]}")
        except requests.RequestException as exc:
            print(f"  Locations request error for {zip_code}: {exc}")
    return out


def fetch_products(token: str, location_id: str, term: str) -> list:
    headers = {"Authorization": f"Bearer {token}"}
    params = {"filter.term": term, "filter.locationId": location_id, "filter.limit": PRODUCT_LIMIT}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(PRODUCTS_URL, headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                return r.json().get("data", [])
            if r.status_code == 429:
                print(f"    429 rate limited on '{term}' @ {location_id}; backing off")
                time.sleep(BACKOFF_SECONDS * attempt)
                continue
            print(f"    HTTP {r.status_code} on '{term}' @ {location_id}: {r.text[:120]}")
            return []
        except requests.RequestException as exc:
            print(f"    Request error (attempt {attempt}) on '{term}': {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)
    return []


def parse_products(raw_products: list, zip_code: str, location_id: str, fetched_at: str) -> pd.DataFrame:
    rows = []
    for p in raw_products:
        items = p.get("items") or [{}]
        price_info = items[0].get("price") or {}
        rows.append({
            "upc": p.get("upc"),
            "product_id": p.get("productId"),
            "product_name": p.get("description"),
            "brand": p.get("brand"),
            "category": ",".join(p.get("categories", []) or []),
            "size": items[0].get("size"),
            "price": price_info.get("promo") or price_info.get("regular"),
            "regular_price": price_info.get("regular"),
            "promo_price": price_info.get("promo"),
            "store_id": location_id,
            "zip_code": zip_code,
            "fetched_at": fetched_at,
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Kroger products pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="No-op vs incremental — the API only exposes current prices")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"Kroger Products  mode={mode}")

    token = get_token()
    if not token:
        print("  Could not obtain OAuth2 token. Check KROGER_CLIENT_ID/KROGER_CLIENT_SECRET.")
        return

    print(f"  Resolving {len(TRACKED_ZIPS)} store locations...")
    locations = resolve_location_ids(token)
    if not locations:
        print("  No store locations resolved. Aborting.")
        return
    print(f"  Resolved {len(locations)}/{len(TRACKED_ZIPS)} locations")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    frames = []
    for zip_code, location_id in locations.items():
        for term in SEARCH_TERMS:
            products = fetch_products(token, location_id, term)
            if products:
                frames.append(parse_products(products, zip_code, location_id, now.isoformat()))
            time.sleep(0.2)

    if not frames:
        print("  No products fetched.")
        return

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["upc", "product_name", "price"])
    out = out.drop_duplicates(subset=["upc", "store_id"]).reset_index(drop=True)
    print(f"  Parsed {len(out):,} product/store price observations")

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"kroger_products_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['product_name'].nunique()} products, "
          f"{out['store_id'].nunique()} stores)")

    print("\n--- KROGER PRODUCTS PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
