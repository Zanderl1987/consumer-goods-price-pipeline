#!/usr/bin/env python3
"""
OECD CPI Pipeline — consumer price indices, ~38 member + G20 economies.

Keyless SDMX 3.0 REST API on the new sdmx.oecd.org host (OECD migrated off
stats.oecd.org around 2024 — the old host is dead). Verified live 2026-08-04:
  Dataflow : OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,1.0
  Data URL : https://sdmx.oecd.org/public/rest/data/{dataflow}/{key}?format=csv&startPeriod=...

Dimension order in the key (8 dot-separated positions, TIME_PERIOD is
separate): REF_AREA.FREQ.METHODOLOGY.MEASURE.UNIT_MEASURE.EXPENDITURE.ADJUSTMENT.TRANSFORMATION

Fixed to the raw index level (not growth rates) so this is directly
comparable to the other CPI-index tables in this repo:
  FREQ=M, METHODOLOGY=N (national CPI methodology), MEASURE=CPI,
  UNIT_MEASURE=IX (index level), ADJUSTMENT=N (not seasonally adjusted),
  TRANSFORMATION=_Z (no transformation applied)
  EXPENDITURE: _T (all items) and CP01 (food, COICOP 1999) fetched.

REF_AREA left wildcarded (empty key segment) to pull every country the
dataflow publishes in one request. CSV format keeps the response small
(~a few hundred KB per expenditure group) versus the much larger SDMX-JSON.

CLI:
  python oecd_cpi_pipeline.py             # incremental (last 24 months)
  python oecd_cpi_pipeline.py --backfill  # full available history

Outputs:
  storage/raw/oecd/cpi/oecd_cpi_{mode}_{YYYYMMDD}.parquet
  (CATALOG: oecd_cpi)
"""

import argparse
import datetime
import io
import os
import time

import pandas as pd
import requests

from storage_utils import write_partitioned

DATAFLOW = "OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,1.0"
BASE_URL = f"https://sdmx.oecd.org/public/rest/data/{DATAFLOW}"
EXPENDITURE_GROUPS = ["_T", "CP01"]   # all-items, food

OUTPUT_DIR = os.path.join("storage", "raw", "oecd", "cpi")
MAX_RETRIES = 3
BACKOFF_SECONDS = 20
INCREMENTAL_MONTHS = 24


def fetch_expenditure_group(expenditure: str, start_period: str) -> pd.DataFrame:
    key = f".M.N.CPI.IX.{expenditure}.N._Z"
    params = {"format": "csv", "startPeriod": start_period}
    headers = {"Accept": "text/csv"}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(f"{BASE_URL}/{key}", params=params, headers=headers, timeout=90)
            if r.status_code == 200 and r.text.strip():
                return pd.read_csv(io.StringIO(r.text))
            print(f"  HTTP {r.status_code} for expenditure={expenditure}: {r.text[:150]}")
            return pd.DataFrame()
        except requests.RequestException as exc:
            print(f"  Request error (attempt {attempt}) for expenditure={expenditure}: {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)
    return pd.DataFrame()


def parse_frame(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw
    out = pd.DataFrame({
        "geo": raw.get("REF_AREA"),
        "expenditure": raw.get("EXPENDITURE"),
        "date": pd.to_datetime(raw.get("TIME_PERIOD"), errors="coerce"),
        "value": pd.to_numeric(raw.get("OBS_VALUE"), errors="coerce"),
        "obs_status": raw.get("OBS_STATUS"),
    })
    out["series_id"] = out["geo"].astype(str) + ".CPI.IX." + out["expenditure"].astype(str)
    return out.dropna(subset=["geo", "date", "value"])


def main():
    parser = argparse.ArgumentParser(description="OECD CPI pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch full available history (default keeps last 24 months)")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"OECD CPI  mode={mode}")

    start_period = "1990-01" if args.backfill else \
        (now - datetime.timedelta(days=30 * INCREMENTAL_MONTHS)).strftime("%Y-%m")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    frames = []
    for expenditure in EXPENDITURE_GROUPS:
        print(f"  Fetching expenditure={expenditure} from {start_period}...")
        raw = fetch_expenditure_group(expenditure, start_period)
        parsed = parse_frame(raw)
        if not parsed.empty:
            frames.append(parsed)
            print(f"    {len(parsed):,} observations")
        time.sleep(1)

    if not frames:
        print("  No data downloaded.")
        return

    out = pd.concat(frames, ignore_index=True)
    out["fetched_at"] = now.isoformat()
    out = out.sort_values(["geo", "expenditure", "date"]).reset_index(drop=True)
    print(f"  Parsed {len(out):,} total observations")

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"oecd_cpi_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['geo'].nunique()} countries, "
          f"{out['expenditure'].nunique()} expenditure groups)")

    print("\n--- OECD CPI PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
