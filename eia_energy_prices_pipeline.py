#!/usr/bin/env python3
"""
EIA Consumer Energy Price Pipeline — retail gasoline/diesel, electricity, and
natural gas prices that households actually pay.

Free EIA API key at https://www.eia.gov/opendata/register.php

CLI:
  python eia_energy_prices_pipeline.py             # incremental (last 90 days)
  python eia_energy_prices_pipeline.py --backfill  # full available history

Outputs:
  storage/raw/eia/gas_retail/...     (CATALOG: eia_gas_retail)
  storage/raw/eia/gas_spot/...       (CATALOG: eia_gas_spot)
  storage/raw/eia/electricity_price/...  (CATALOG: eia_electricity_price)
  storage/raw/eia/natgas_price/...       (CATALOG: eia_natgas_price)
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

EIA_API_KEY = os.environ.get("EIA_API_KEY", "")
EIA_BASE = "https://api.eia.gov/v2"

OUTPUT_DIR = os.path.join("storage", "raw", "eia")

REQUEST_INTERVAL = 0.25
MAX_RETRIES = 3
BACKOFF_SECONDS = 60
PAGE_SIZE = 5000

# Weekly retail pump prices by grade and region (petroleum/pri/gnd)
RETAIL_PRODUCTS = ["EPMR", "EPMM", "EPMP", "EPD2D"]
RETAIL_DUOAREAS = ["NUS", "R10", "R1X", "R1Y", "R1Z", "R20", "R30", "R40", "R50"]
PRODUCT_NAMES = {
    "EPMR":  "Regular Gasoline",
    "EPMM":  "Midgrade Gasoline",
    "EPMP":  "Premium Gasoline",
    "EPD2D": "No. 2 Diesel (On-Highway)",
}

# Daily spot prices at trading hubs (petroleum/pri/spt)
SPOT_SERIES = {
    "EER_EPMRU_PF4_Y35NY_DPG":    "Gasoline Regular Conv., NY Harbor",
    "EER_EPMRU_PF4_RGC_DPG":      "Gasoline Regular Conv., Gulf Coast",
    "EER_EPMRR_PF4_Y05LA_DPG":    "Gasoline Regular RBOB, Los Angeles",
    "EER_EPD2DXL0_PF4_Y35NY_DPG": "ULS No. 2 Diesel, NY Harbor",
    "EER_EPD2DXL0_PF4_RGC_DPG":   "ULS No. 2 Diesel, Gulf Coast",
}


def get_with_backoff(url, params):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                wait = BACKOFF_SECONDS * attempt
                print(f"  429 from EIA -- backing off {wait}s (attempt {attempt}/{MAX_RETRIES}).")
                time.sleep(wait)
            else:
                print(f"  HTTP {r.status_code}: {r.text[:200]}")
                return None
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt}): {e}")
            time.sleep(BACKOFF_SECONDS)
    print(f"  Giving up after {MAX_RETRIES} attempts.")
    return None


def fetch_paginated(route, base_params, list_params=None, start_date=None, description=""):
    """Fetch all pages from an EIA v2 data route."""
    url = f"{EIA_BASE}/{route}"
    all_rows = []
    offset = 0
    while True:
        params = list(base_params.items())
        if list_params:
            for key, values in list_params.items():
                for v in values:
                    params.append((key, v))
        if start_date:
            params.append(("start", start_date))
        params.append(("offset", str(offset)))
        params.append(("length", str(PAGE_SIZE)))

        r = get_with_backoff(url, params)
        if not r:
            break
        payload = r.json()
        resp = payload.get("response", {})
        data = resp.get("data", [])
        total = int(resp.get("total", 0))
        all_rows.extend(data)
        fetched = offset + len(data)
        if description:
            print(f"  {description}: {fetched}/{total} rows...", end="\r")
        if len(data) < PAGE_SIZE or fetched >= total:
            break
        offset += PAGE_SIZE
        time.sleep(REQUEST_INTERVAL)
    if description:
        print(f"  {description}: {len(all_rows)} rows fetched.   ")
    return all_rows


def fetch_gas_retail(start_date=None):
    """Weekly retail pump prices by grade x region."""
    base = {
        "api_key": EIA_API_KEY, "data[]": "value", "frequency": "weekly",
        "sort[0][column]": "period", "sort[0][direction]": "asc",
    }
    list_params = {
        "facets[product][]": RETAIL_PRODUCTS,
        "facets[duoarea][]": RETAIL_DUOAREAS,
    }
    rows = fetch_paginated("petroleum/pri/gnd/data/", base,
                           list_params=list_params, start_date=start_date,
                           description="retail gas")
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df = df.rename(columns={"period": "date", "value": "price_usd_gallon"})
    df["date"] = pd.to_datetime(df["date"])
    df["price_usd_gallon"] = pd.to_numeric(df["price_usd_gallon"], errors="coerce")
    df["product_name"] = df["product"].map(PRODUCT_NAMES)
    df["price_type"] = "retail"
    df["frequency"] = "weekly"
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()
    wanted = ["date", "duoarea", "product", "product_name", "price_usd_gallon",
              "price_type", "frequency", "units", "fetched_at"]
    cols = [c for c in wanted if c in df.columns]
    return df[cols].dropna(subset=["price_usd_gallon"]).sort_values(["product", "duoarea", "date"])


def fetch_gas_spot(start_date=None):
    """Daily wholesale spot prices at major trading hubs."""
    base = {
        "api_key": EIA_API_KEY, "data[]": "value", "frequency": "daily",
        "sort[0][column]": "period", "sort[0][direction]": "asc",
    }
    list_params = {"facets[series][]": list(SPOT_SERIES.keys())}
    rows = fetch_paginated("petroleum/pri/spt/data/", base,
                           list_params=list_params, start_date=start_date,
                           description="spot gas")
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df = df.rename(columns={"period": "date", "value": "price_usd_gallon"})
    df["date"] = pd.to_datetime(df["date"])
    df["price_usd_gallon"] = pd.to_numeric(df["price_usd_gallon"], errors="coerce")
    df["series_name"] = df["series"].map(SPOT_SERIES)
    df["price_type"] = "spot"
    df["frequency"] = "daily"
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()
    wanted = ["date", "series", "series_name", "price_usd_gallon", "price_type",
              "frequency", "units", "fetched_at"]
    cols = [c for c in wanted if c in df.columns]
    return df[cols].dropna(subset=["price_usd_gallon"]).sort_values(["series", "date"])


def fetch_electricity(start_date=None):
    """Average retail electricity price by state / sector (monthly, cents/kWh).

    The retail-sales route's `data` dict only accepts "revenue", "sales",
    "price", or "customers" -- "value" 400s ("Invalid data 'value'
    provided", found live 2026-08-04, first real run of this pipeline
    since a key was configured). Requesting data[]=price also renames the
    observation column itself to "price" (not "value") and the units
    column to "price-units" (not "units").
    """
    base = {
        "api_key": EIA_API_KEY, "data[]": "price", "frequency": "monthly",
        "sort[0][column]": "period", "sort[0][direction]": "asc",
    }
    rows = fetch_paginated("electricity/retail-sales/data/", base,
                           start_date=start_date, description="electricity")
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df = df.rename(columns={"period": "date", "price": "cents_per_kwh", "price-units": "units"})
    df["date"] = pd.to_datetime(df["date"])
    df["cents_per_kwh"] = pd.to_numeric(df["cents_per_kwh"], errors="coerce")
    df["series_id"] = df["stateid"] + "-" + df["sectorid"].astype(str)
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()
    wanted = ["date", "stateid", "sectorid", "series_id", "cents_per_kwh", "units", "fetched_at"]
    cols = [c for c in wanted if c in df.columns]
    return df[cols].dropna(subset=["cents_per_kwh"]).sort_values(["series_id", "date"])


def fetch_natgas_price(start_date=None):
    """Citygate/wellhead/residential natural gas prices by area (monthly).

    This route's facets are duoarea/product/process/series -- there is no
    "stateid" facet or column (found live 2026-08-04, first real run of
    this pipeline since a key was configured; the code had assumed the
    same "stateid" shape as the electricity route). `series` is already a
    unique per-area/process code, so it's used directly as series_id
    instead of hand-building one from a nonexistent column.
    """
    base = {
        "api_key": EIA_API_KEY, "data[]": "value", "frequency": "monthly",
        "sort[0][column]": "period", "sort[0][direction]": "asc",
    }
    rows = fetch_paginated("natural-gas/pri/sum/data/", base,
                           start_date=start_date, description="natgas")
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df = df.rename(columns={"period": "date", "value": "price", "series": "series_id"})
    df["date"] = pd.to_datetime(df["date"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["fetched_at"] = datetime.datetime.utcnow().isoformat()
    wanted = ["date", "duoarea", "process", "series_id", "series-description",
              "price", "units", "fetched_at"]
    cols = [c for c in wanted if c in df.columns]
    return df[cols].dropna(subset=["price"]).sort_values(["series_id", "date"])


def main():
    parser = argparse.ArgumentParser(description="EIA consumer energy price pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch full available history")
    args = parser.parse_args()

    if not EIA_API_KEY:
        print("ERROR: No EIA_API_KEY found. Register free at https://www.eia.gov/opendata/register.php")
        return

    os.makedirs(os.path.join(OUTPUT_DIR, "gas_retail"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "gas_spot"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "electricity_price"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "natgas_price"), exist_ok=True)

    today = datetime.datetime.utcnow().strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    start_date = None if args.backfill else \
        (datetime.datetime.utcnow() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    print(f"Mode: {'BACKFILL (full history)' if args.backfill else f'INCREMENTAL (from {start_date})'}")

    for label, fn, subdir, prefix in [
        ("retail gas", fetch_gas_retail, "gas_retail", "eia_gas_retail_weekly"),
        ("spot gas",   fetch_gas_spot,   "gas_spot",   "eia_gas_spot_daily"),
        ("electricity", fetch_electricity, "electricity_price", "eia_electricity_price_monthly"),
        ("natgas",     fetch_natgas_price, "natgas_price", "eia_natgas_price_monthly"),
    ]:
        print(f"\n--- {label} ---")
        df = fn(start_date)
        if df is not None and not df.empty:
            path = write_partitioned(
                df, os.path.join(OUTPUT_DIR, subdir),
                f"{prefix}_{mode}_{today}.parquet",
            )
            print(f"[+] {path}")
            print(f"    {len(df):,} rows")
        else:
            print("[!] No data returned.")

    print("\n--- COMPLETE ---")


if __name__ == "__main__":
    main()
