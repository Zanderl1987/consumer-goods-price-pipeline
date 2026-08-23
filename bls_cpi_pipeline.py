#!/usr/bin/env python3
"""
BLS CPI/PPI Pipeline — detailed consumer price indexes by item and area.

Fetches from the BLS Public Data API:
  - CPI-U detailed item indexes (food & beverages, apparel, vehicles, tires,
    appliances, medical care commodities, energy, etc.)
  - PPI commodity groups that feed into retail prices

Uses API v2 if BLS_API_KEY is in .env (higher limits), else v1 (no key, free).
Register free at https://data.bls.gov/registrationEngine/ to get a v2 key.

CLI:
  python bls_cpi_pipeline.py             # incremental (last 2 calendar years)
  python bls_cpi_pipeline.py --backfill  # full history back to 2000

Outputs:
  storage/raw/bls/cpi/bls_cpi_{mode}_{YYYYMMDD}.parquet      (CATALOG: bls_cpi)
  storage/raw/bls/ppi/bls_ppi_{mode}_{YYYYMMDD}.parquet      (CATALOG: bls_ppi)
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

BLS_API_KEY = os.environ.get("BLS_API_KEY", "")
BLS_V1 = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
BLS_V2 = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_URL = BLS_V2 if BLS_API_KEY else BLS_V1

BASE_DIR = os.path.join("storage", "raw", "bls")
REQUEST_INTERVAL = 1.5
MAX_RETRIES = 3
BATCH_SIZE = 50 if BLS_API_KEY else 25  # v2 supports 50 series/request, v1 supports 25

# ---------------------------------------------------------------------------
# Series catalogs — (human name, unit)
# ---------------------------------------------------------------------------

CPI_SERIES = {
    # Aggregate
    "CUUR0000SA0":    ("CPI-U All Items NSA",                  "Index 1982-84=100"),
    "CUSR0000SAF1":   ("CPI Food",                             "Index 1982-84=100"),
    "CUSR0000SAF11":  ("CPI Food At Home",                     "Index 1982-84=100"),
    "CUSR0000SAF113": ("CPI Fruits & Vegetables",              "Index 1982-84=100"),
    "CUSR0000SAF114": ("CPI Meats, Poultry, Fish, Eggs",       "Index 1982-84=100"),
    "CUSR0000SAF115": ("CPI Dairy & Related Products",         "Index 1982-84=100"),
    "CUSR0000SAF116": ("CPI Beverages & Beverage Materials",   "Index 1982-84=100"),
    "CUSR0000SAF117": ("CPI Other Food At Home",               "Index 1982-84=100"),
    "CUSR0000SEFV":   ("CPI Food Away From Home",              "Index 1982-84=100"),
    "CUSR0000SAA":    ("CPI Apparel",                          "Index 1982-84=100"),
    "CUSR0000SACE":   ("CPI New & Used Motor Vehicles",        "Index 1982-84=100"),
    "CUSR0000SETA01": ("CPI New Vehicles",                     "Index 1982-84=100"),
    "CUSR0000SETA02": ("CPI Used Cars & Trucks",               "Index 1982-84=100"),
    "CUSR0000SAT":    ("CPI Transportation",                   "Index 1982-84=100"),
    "CUSR0000SETB01": ("CPI Gasoline All Types",               "Index 1982-84=100"),
    "CUSR0000SEHC01": ("CPI Motor Oil, Coolant & Fluids",      "Index 1982-84=100"),
    "CUSR0000SEMC":   ("CPI Medical Care Commodities",         "Index 1982-84=100"),
    "CUSR0000SEMD":   ("CPI Medical Care Services",            "Index 1982-84=100"),
    "CUSR0000SAE21":  ("CPI Fuel Oil & Other Fuels",           "Index 1982-84=100"),
    "CUSR0000SEHF01": ("CPI Electricity",                      "Index 1982-84=100"),
    "CUSR0000SEHF02": ("CPI Utility (Piped) Gas",              "Index 1982-84=100"),
    "CUSR0000SAG":    ("CPI Other Goods & Services",           "Index 1982-84=100"),
    "CUSR0000SAS":    ("CPI Services",                         "Index 1982-84=100"),
    # Detailed item series that map to "tires to avocados" coverage
    "CUUR0000SS62031": ("CPI Tires",                          "Index 1982-84=100"),
    "CUUR0000SEHF":    ("CPI Energy",                          "Index 1982-84=100"),
    "CUUR0000SAF112":  ("CPI Cereals & Bakery Products",       "Index 1982-84=100"),
    "CUUR0000SS62021": ("CPI Motor Vehicle Parts & Equipment", "Index 1982-84=100"),
}


# Correction (verified live 2026-08-14, first live run of the bls_ppi table):
# the original PCU3363/PCU336111336111/PCU335221335221 IDs 400 with "Series
# does not exist". BLS PCU industry series key off the exact 6-digit NAICS
# industry code doubled (e.g. automobile mfg is NAICS 336110, not 336111;
# household appliance mfg is 335220, not 335221) -- a plausible-looking
# adjacent code silently fails. Motor Vehicle Parts Mfg (NAICS 3363, a
# 4-digit subsector with no single reporting industry) has no working
# PCU total-industry series found live -- dropped rather than guessed.
PPI_SERIES = {
    "WPU00000000":   ("PPI All Commodities",                    "Index 1982=100"),
    "WPSFD41":       ("PPI Finished Consumer Goods",            "Index 1982=100"),
    "WPU102":        ("PPI Processed Foods & Feeds",            "Index 1982=100"),
    "WPU101":        ("PPI Farm Products",                      "Index 1982=100"),
    "WPU0561":       ("PPI Gasoline",                           "Index 1982=100"),
    "WPU15":         ("PPI Rubber & Plastic Products",          "Index 1982=100"),
    "PCU336110336110": ("PPI Automobile Manufacturing",         "Index 2012=100"),
    "PCU335220335220": ("PPI Household Appliance Manufacturing","Index 2012=100"),
    "PCU334310334310": ("PPI Audio & Video Equipment Mfg",      "Index 2012=100"),
    "PCU334118334118": ("PPI Computer Terminal Mfg",            "Index 2012=100"),
    "PCU325414325414": ("PPI Biological Product Mfg",           "Index 2012=100"),
    "PCU325412325412": ("PPI Pharmaceutical Preparation Mfg",   "Index 2012=100"),
}

TABLE_CONFIGS = {
    "bls_cpi": (CPI_SERIES, os.path.join(BASE_DIR, "cpi")),
    "bls_ppi": (PPI_SERIES, os.path.join(BASE_DIR, "ppi")),
}


def fetch_batch(series_ids, start_year, end_year):
    """POST a batch of BLS series IDs; return raw series list from API."""
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if BLS_API_KEY:
        payload["registrationkey"] = BLS_API_KEY
        payload["annualaverage"] = "false"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(BLS_URL, json=payload, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") != "REQUEST_SUCCEEDED":
                    msgs = data.get("message", [])
                    print(f"  BLS API non-success: {msgs}")
                    return []
                return data.get("Results", {}).get("series", [])
            if resp.status_code == 429:
                wait = 60 * attempt
                print(f"  429 rate limit. Backing off {wait}s (attempt {attempt}/{MAX_RETRIES}).")
                time.sleep(wait)
            else:
                print(f"  HTTP {resp.status_code}: {resp.text[:120]}")
                return []
        except requests.RequestException as exc:
            print(f"  Request error (attempt {attempt}): {exc}")
            time.sleep(30 * attempt)
    return []


def parse_series(raw_series, catalog):
    """Convert BLS API response list to a DataFrame."""
    rows = []
    for s in raw_series:
        sid = s.get("seriesID", "")
        meta = catalog.get(sid)
        if not meta:
            continue
        name, unit = meta
        for obs in s.get("data", []):
            period = obs.get("period", "")
            year_str = obs.get("year", "")
            value_str = obs.get("value", "")
            try:
                value = float(value_str)
                year = int(year_str)
            except (ValueError, TypeError):
                continue
            if period.startswith("M"):
                month = int(period[1:])
                if month > 12:
                    continue  # M13 = annual average — skip
                date_str = f"{year}-{month:02d}-01"
            elif period.startswith("Q"):
                month = (int(period[1:]) - 1) * 3 + 1
                date_str = f"{year}-{month:02d}-01"
            elif period == "A01":
                date_str = f"{year}-01-01"
            else:
                continue
            rows.append({
                "series_id": sid,
                "name":      name,
                "unit":      unit,
                "date":      date_str,
                "period":    period,
                "value":     value,
            })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="BLS CPI/PPI consumer-goods price index pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch full history back to 2000")
    args = parser.parse_args()

    now = datetime.datetime.utcnow()
    current_year = now.year
    today_str = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    start_year = 2000 if args.backfill else current_year - 2

    print(f"BLS CPI/PPI Pipeline  mode={mode}  start={start_year}")
    print(f"API: {'v2 (key present)' if BLS_API_KEY else 'v1 (no key -- add BLS_API_KEY to .env for higher limits)'}\n")

    # BLS API accepts max 20-year spans per request; chunk for backfill
    year_chunks = []
    SPAN = 20
    y = start_year
    while y <= current_year:
        year_chunks.append((y, min(y + SPAN - 1, current_year)))
        y += SPAN

    for table_name, (catalog, output_dir) in TABLE_CONFIGS.items():
        os.makedirs(output_dir, exist_ok=True)
        series_list = list(catalog.keys())
        print(f"[{table_name}]  {len(series_list)} series, {len(year_chunks)} year chunk(s)...")

        all_frames = []
        for y_start, y_end in year_chunks:
            for batch_start in range(0, len(series_list), BATCH_SIZE):
                batch = series_list[batch_start:batch_start + BATCH_SIZE]
                raw = fetch_batch(batch, y_start, y_end)
                if raw:
                    df = parse_series(raw, catalog)
                    if not df.empty:
                        all_frames.append(df)
                time.sleep(REQUEST_INTERVAL)

        if not all_frames:
            print("  No data returned.\n")
            continue

        combined = (
            pd.concat(all_frames, ignore_index=True)
            .drop_duplicates(subset=["series_id", "date"])
            .sort_values(["series_id", "date"])
        )
        combined["fetched_at"] = now.isoformat()

        path = write_partitioned(
            combined, output_dir,
            f"{table_name}_{mode}_{today_str}.parquet",
        )
        print(f"  -> {path}  ({len(combined):,} rows, {combined['series_id'].nunique()} series)\n")

    print("--- BLS CPI/PPI PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
