#!/usr/bin/env python3
"""
Kroger Catalog Pipeline - broad paginated sweep per store, channel flags.

Complements kroger_pipeline.py (fixed 20 search terms, one page each) with a
much wider per-store pull: for each tracked store, a list of ~40 broad
one-word terms is walked with full pagination (filter.start advances by
filter.limit until the reported total is exhausted), so every product the
search index returns for those terms is captured, not just the first 50.

Known API facts (verified live 2026-08-25 against api-ce.kroger.com with a
Certification-environment app):
  - No-filter / term-less listing is rejected (PRODUCT-2016: "Field 'term'
    or 'productId' must be used"), so a true whole-catalog walk is impossible
    on this environment; coverage is bounded by the term list.
  - filter.fulfillment returns zero rows on Certification and the items[]
    fulfillment object carries only availability booleans, never per-channel
    prices -> a fulfillment-price-spread table is NOT buildable here. The
    booleans are kept as columns instead.
  - Pagination meta: {"pagination": {"start": N, "limit": L, "total": T}}.
  - Some Certification locationIds have no catalog behind them: they either
    404 on product search or 503 PRODUCT-4109-500. Stores are probed at run
    time and dead ones skipped (Denver 80202 has failed since 2026-08-04).
    Locations named like test fixtures ("Test for Fuel Pay...") also fail.

Auth + rate limits: OAuth2 client-credentials (product.compact), token valid
~30 min; Products endpoint ~10,000 calls/day. A full run is roughly
3 stores x 40 terms x ~8 pages = ~1,000 calls.

CLI:
  python kroger_catalog_pipeline.py                # full sweep
  python kroger_catalog_pipeline.py --max-pages 3 # cap pages per term

Outputs:
  storage/raw/kroger/catalog/kroger_catalog_{mode}_{YYYYMMDD}.parquet
  (CATALOG: kroger_catalog)
"""

import argparse
import base64
import datetime
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

from storage_utils import write_partitioned

load_dotenv()

BASE_URL = ("https://api.kroger.com" if os.environ.get("KROGER_ENV") == "production"
            else "https://api-ce.kroger.com")
TOKEN_URL = f"{BASE_URL}/v1/connect/oauth2/token"
PRODUCTS_URL = f"{BASE_URL}/v1/products"
LOCATIONS_URL = f"{BASE_URL}/v1/locations"

OUTPUT_DIR = os.path.join("storage", "raw", "kroger", "catalog")
MAX_RETRIES = 3
BACKOFF_SECONDS = 15
PAGE_SIZE = 50
REQUEST_PAUSE = 0.2

TRACKED_ZIPS = ["45202", "30301", "75201", "90012", "80202"]

# Broad single-concept terms chosen to tile the grocery catalog rather than
# target specific staples; each term typically matches 300-600 SKUs per store.
SWEEP_TERMS = [
    "milk", "eggs", "bread", "cheese", "butter", "yogurt", "cream",
    "chicken", "beef", "pork", "fish", "shrimp", "turkey", "bacon",
    "rice", "pasta", "cereal", "oats", "flour", "sugar", "beans",
    "soup", "sauce", "salsa", "snacks", "chips", "cookies", "candy",
    "chocolate", "coffee", "tea", "juice", "soda", "water",
    "apple", "banana", "orange", "salad", "tomato", "potato",
    "frozen", "pizza", "soap", "detergent", "paper", "diapers",
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


def resolve_stores(token: str) -> dict:
    """ZIP -> (locationId, store_name) for the first nearby store whose
    product catalog actually answers (Certification env has dead locations)."""
    headers = {"Authorization": f"Bearer {token}"}
    out = {}
    for zip_code in TRACKED_ZIPS:
        try:
            r = requests.get(LOCATIONS_URL, headers=headers, params={
                "filter.zipCode.near": zip_code,
                "filter.radiusInMiles": 15,
                "filter.limit": 5,
            }, timeout=30)
            if r.status_code != 200:
                print(f"  Locations lookup {zip_code}: HTTP {r.status_code}")
                continue
            candidates = [
                loc for loc in r.json().get("data", [])
                if not _is_test_store(loc.get("name", ""))
            ]
            for loc in candidates[:5]:
                probe = requests.get(PRODUCTS_URL, headers=headers, params={
                    "filter.locationId": loc["locationId"],
                    "filter.term": "milk",
                    "filter.limit": 1,
                }, timeout=30)
                if probe.status_code == 200 and probe.json().get("data"):
                    out[zip_code] = (loc["locationId"], loc.get("name", ""))
                    break
                time.sleep(REQUEST_PAUSE)
            else:
                print(f"  {zip_code}: no candidate store serves products")
        except requests.RequestException as exc:
            print(f"  Locations request error for {zip_code}: {exc}")
    return out


def _is_test_store(name: str) -> bool:
    lowered = name.lower()
    return "test" in lowered or "forecast" in lowered or "spoke" in lowered


def fetch_term_pages(token: str, location_id: str, term: str, max_pages: int | None) -> tuple[list, int]:
    """All products for `term` at one store, walking filter.start. Returns
    (products, total_reported); stops when start >= total, a short page comes
    back, or max_pages is hit."""
    headers = {"Authorization": f"Bearer {token}"}
    collected: list = []
    start = 0
    total = None
    pages = 0
    while True:
        params = {
            "filter.locationId": location_id,
            "filter.term": term,
            "filter.limit": PAGE_SIZE,
            "filter.start": start,
        }
        rows = []
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(PRODUCTS_URL, headers=headers, params=params, timeout=30)
                if r.status_code == 200:
                    body = r.json()
                    rows = body.get("data", [])
                    pagination = (body.get("meta") or {}).get("pagination") or {}
                    total = pagination.get("total")
                    break
                if r.status_code == 429:
                    print(f"    429 on '{term}' @ start={start}; backing off")
                    time.sleep(BACKOFF_SECONDS * attempt)
                    continue
                print(f"    HTTP {r.status_code} on '{term}' @ start={start}: {r.text[:100]}")
                return collected, total or len(collected)
            except requests.RequestException as exc:
                print(f"    Request error (attempt {attempt}) on '{term}': {exc}")
                time.sleep(BACKOFF_SECONDS * attempt)
        if not rows:
            break
        collected.extend(rows)
        pages += 1
        if max_pages is not None and pages >= max_pages:
            break
        start += PAGE_SIZE
        if total is not None and start >= total:
            break
        if len(rows) < PAGE_SIZE:
            break
        time.sleep(REQUEST_PAUSE)
    return collected, total or len(collected)


def parse_products(raw_products: list, zip_code: str, location_id: str,
                   store_name: str, term: str, fetched_at: str) -> pd.DataFrame:
    rows = []
    seen_upcs: set = set()
    for p in raw_products:
        upc = p.get("upc")
        if not upc or upc in seen_upcs:
            continue
        seen_upcs.add(upc)
        items = p.get("items") or [{}]
        item = items[0] if items else {}
        price_info = item.get("price") or {}
        fulfillment = item.get("fulfillment") or {}
        rows.append({
            "upc": upc,
            "product_id": p.get("productId"),
            "product_name": p.get("description"),
            "brand": p.get("brand"),
            "category": ",".join(p.get("categories", []) or []),
            "size": item.get("size"),
            "price": price_info.get("promo") or price_info.get("regular"),
            "regular_price": price_info.get("regular"),
            "promo_price": price_info.get("promo"),
            "in_store": bool(fulfillment.get("inStore")),
            "curbside": bool(fulfillment.get("curbside")),
            "delivery": bool(fulfillment.get("delivery")),
            "ship_to_home": bool(fulfillment.get("shipToHome")),
            "store_id": location_id,
            "store_name": store_name,
            "zip_code": zip_code,
            "match_term": term,
            "fetched_at": fetched_at,
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Kroger catalog sweep pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="No-op vs incremental; the API exposes current prices only")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Cap pages fetched per (store, term)")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"Kroger Catalog Sweep  mode={mode}")

    token = get_token()
    if not token:
        print("  Could not obtain OAuth2 token. Check KROGER_CLIENT_ID/KROGER_CLIENT_SECRET.")
        return

    stores = resolve_stores(token)
    if not stores:
        print("  No working stores resolved. Aborting.")
        return
    print(f"  Working stores: "
          f"{ {z: lid for z, (lid, _) in stores.items()} }")

    frames = []
    call_budget_note = f"(max {args.max_pages} pages/term)" if args.max_pages else "(full walk)"
    for zip_code, (location_id, store_name) in stores.items():
        print(f"[{store_name}] {len(SWEEP_TERMS)} terms {call_budget_note}")
        for term in SWEEP_TERMS:
            products, total = fetch_term_pages(token, location_id, term, args.max_pages)
            if products:
                frame = parse_products(products, zip_code, location_id, store_name,
                                       term, now.isoformat())
                frames.append(frame)
                print(f"  {term}: {len(frame)} unique UPCs (index reports {total})")
            time.sleep(REQUEST_PAUSE)

    if not frames:
        print("  Nothing fetched.")
        return

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["upc", "product_name"])
    out["price"] = out["price"].astype(float)
    out = out.dropna(subset=["price"])
    out = out.drop_duplicates(subset=["upc", "store_id"]).reset_index(drop=True)
    print(f"  Parsed {len(out):,} store-level catalog observations")

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"kroger_catalog_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['upc'].nunique():,} UPCs, "
          f"{out['store_id'].nunique()} stores)")

    print("\n--- KROGER CATALOG PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
