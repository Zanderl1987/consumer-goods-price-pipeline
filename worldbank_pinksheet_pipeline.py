#!/usr/bin/env python3
"""
World Bank Pink Sheet Pipeline — ~70 global commodity benchmark prices.

Keyless, but the monthly-updated Excel file lives at a URL with a rotating
per-release hash (`.../doc/{hash}-{YYYYMMYYYY-ish}/related/CMO-Historical-
Data-Monthly.xlsx`), so this pipeline scrapes the current URL out of the
commodity-markets landing page HTML on every run rather than hardcoding it —
same "resolve, don't hardcode" pattern used for the WFP and StatCan
pipelines. Verified live 2026-08-04:
  Landing page : https://www.worldbank.org/en/research/commodity-markets
  Resolved xlsx: https://thedocs.worldbank.org/en/doc/<hash>/related/CMO-Historical-Data-Monthly.xlsx
  Sheet        : "Monthly Prices" — nominal USD, 1960-present, ~70 commodity
                 columns (2 header rows: commodity name, unit), date rows
                 formatted "1960M01"

Wholesale/benchmark prices, not retail — energy, metals, agriculture. This
is the single best free source of absolute (not indexed) global commodity
price levels in the repo.

CLI:
  python worldbank_pinksheet_pipeline.py             # incremental (last 24 months)
  python worldbank_pinksheet_pipeline.py --backfill  # full history 1960->present

Outputs:
  storage/raw/worldbank/pinksheet/worldbank_pinksheet_{mode}_{YYYYMMDD}.parquet
  (CATALOG: worldbank_pinksheet)
"""

import argparse
import datetime
import io
import os
import re

import openpyxl
import pandas as pd
import requests

from storage_utils import write_partitioned

LANDING_PAGE_URL = "https://www.worldbank.org/en/research/commodity-markets"
XLSX_LINK_RE = re.compile(r"https://thedocs\.worldbank\.org/[^\"'\s]*CMO-Historical-Data-Monthly\.xlsx")

OUTPUT_DIR = os.path.join("storage", "raw", "worldbank", "pinksheet")
INCREMENTAL_MONTHS = 24


def resolve_xlsx_url() -> str:
    r = requests.get(LANDING_PAGE_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    m = XLSX_LINK_RE.search(r.text)
    return m.group(0) if m else ""


def download_and_parse(xlsx_url: str) -> pd.DataFrame:
    r = requests.get(xlsx_url, timeout=90)
    r.raise_for_status()
    wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
    ws = wb["Monthly Prices"]

    rows = list(ws.iter_rows(values_only=True))
    commodity_row = rows[4]
    unit_row = rows[5]
    commodities = commodity_row[1:]
    units = unit_row[1:]

    records = []
    for row in rows[6:]:
        date_str = row[0]
        if not isinstance(date_str, str) or "M" not in date_str:
            continue
        year, month = date_str.split("M")
        for commodity, unit, value in zip(commodities, units, row[1:]):
            if commodity is None or value is None or not isinstance(value, (int, float)):
                continue
            records.append({
                "commodity": commodity,
                "unit": unit,
                "date": f"{year}-{int(month):02d}-01",
                "value": float(value),
            })
    return pd.DataFrame(records)


def main():
    parser = argparse.ArgumentParser(description="World Bank Pink Sheet pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Keep the full 1960->present history (default keeps last 24 months)")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"World Bank Pink Sheet  mode={mode}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("  Resolving current xlsx URL from landing page...")
    xlsx_url = resolve_xlsx_url()
    if not xlsx_url:
        print("  Could not find the Pink Sheet download link on the landing page.")
        return
    print(f"  -> {xlsx_url}")

    print("  Downloading and parsing Monthly Prices sheet...")
    out = download_and_parse(xlsx_url)
    if out.empty:
        print("  No data parsed.")
        return

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["series_id"] = out["commodity"].astype(str) + "." + out["unit"].astype(str)
    out = out.dropna(subset=["commodity", "date", "value"])
    print(f"  Parsed {len(out):,} observations")

    if not args.backfill:
        cutoff = (now - datetime.timedelta(days=30 * INCREMENTAL_MONTHS)).date()
        out = out[out["date"].dt.date >= cutoff]
        print(f"  Incremental: kept {len(out):,} rows since {cutoff}")

    if out.empty:
        print("  Nothing to write.")
        return

    out["fetched_at"] = now.isoformat()
    out = out.sort_values(["commodity", "date"]).reset_index(drop=True)

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"worldbank_pinksheet_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['commodity'].nunique()} commodities)")

    print("\n--- WORLD BANK PINK SHEET PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
