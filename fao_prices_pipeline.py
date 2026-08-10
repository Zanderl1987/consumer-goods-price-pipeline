#!/usr/bin/env python3
"""
FAO Prices Pipeline — national food CPI + meat/livestock producer prices.

Keyless bulk CSV ZIPs from FAOSTAT (bulks-faostat.fao.org). Verified live
2026-08-04. Two separate FAOSTAT domains, feeding two tables:

  fao_food_prices  <- "Consumer Price Indices" (CP) domain
    ZIP: https://bulks-faostat.fao.org/production/ConsumerPriceIndices_E_All_Data.zip
    Per-country monthly national CPI, item = "Consumer Prices, Food Indices
    (2015 = 100)" or "...General Indices...", wide format (Y2000..Y2026
    columns x Months rows), melted to long. This is the actual national-
    CPI-food series, not the FAO Food Price Index (FFPI) headline number —
    FFPI itself is a single global monthly figure with no API (HTML/Excel
    export only on fao.org/worldfoodsituation); the FAOSTAT CP domain gives
    per-country granularity that fits this repo's schema far better.

  fao_meat_prices  <- "Producer Prices" (PP) domain, filtered to meat items
    ZIP: https://bulks-faostat.fao.org/production/Prices_E_All_Data.zip
    Per-country ANNUAL producer prices for every "Meat of ..." / "Meat,
    Total" item, Element = "Producer Price (USD/tonne)" (the one
    cross-country-comparable unit; LCU/tonne and SLC/tonne are also in the
    file but omitted here), wide format (Y1991..Y2025) melted to long.

License: CC BY-NC-SA 3.0 IGO (non-commercial, share-alike) — see
docs/SOURCES.md.

CLI:
  python fao_prices_pipeline.py             # incremental (last 3 years)
  python fao_prices_pipeline.py --backfill  # full history

Outputs:
  storage/raw/fao/food_prices/fao_food_prices_{mode}_{YYYYMMDD}.parquet
  storage/raw/fao/meat_prices/fao_meat_prices_{mode}_{YYYYMMDD}.parquet
  (CATALOG: fao_food_prices, fao_meat_prices)
"""

import argparse
import datetime
import io
import os
import zipfile

import pandas as pd
import requests

from storage_utils import write_partitioned

CP_ZIP_URL = "https://bulks-faostat.fao.org/production/ConsumerPriceIndices_E_All_Data.zip"
CP_CSV_NAME = "ConsumerPriceIndices_E_All_Data_NOFLAG.csv"
PP_ZIP_URL = "https://bulks-faostat.fao.org/production/Prices_E_All_Data.zip"
PP_CSV_NAME = "Prices_E_All_Data_NOFLAG.csv"

FOOD_OUTPUT_DIR = os.path.join("storage", "raw", "fao", "food_prices")
MEAT_OUTPUT_DIR = os.path.join("storage", "raw", "fao", "meat_prices")

MONTH_NUM = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11,
    "December": 12,
}

INCREMENTAL_YEARS = 3


def download_csv(zip_url: str, csv_name: str) -> pd.DataFrame:
    r = requests.get(zip_url, timeout=180)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    return pd.read_csv(zf.open(csv_name), encoding="latin1", low_memory=False)


def parse_food_prices(raw: pd.DataFrame, min_year: int) -> pd.DataFrame:
    """National monthly CPI food/general indices -> long format."""
    year_cols = [c for c in raw.columns if c.startswith("Y") and c[1:].isdigit() and int(c[1:]) >= min_year]
    id_cols = ["Area", "Item", "Element", "Months", "Unit"]
    long = raw.melt(id_vars=id_cols, value_vars=year_cols, var_name="year_col", value_name="value")
    long["year"] = long["year_col"].str[1:].astype(int)
    long["month"] = long["Months"].map(MONTH_NUM)
    long = long.dropna(subset=["value", "month"])
    long["date"] = pd.to_datetime(dict(year=long["year"], month=long["month"], day=1), errors="coerce")
    out = pd.DataFrame({
        "area": long["Area"],
        "item": long["Item"],
        "element": long["Element"],
        "unit": long["Unit"],
        "date": long["date"],
        "value": pd.to_numeric(long["value"], errors="coerce"),
    })
    return out.dropna(subset=["area", "item", "date", "value"])


def parse_meat_prices(raw: pd.DataFrame, min_year: int) -> pd.DataFrame:
    """Annual producer prices (USD/tonne) for meat/livestock items -> long format."""
    meat = raw[raw["Item"].str.contains("Meat", na=False) & (raw["Element"] == "Producer Price (USD/tonne)")]
    if meat.empty:
        return pd.DataFrame()
    year_cols = [c for c in meat.columns if c.startswith("Y") and c[1:].isdigit() and int(c[1:]) >= min_year]
    id_cols = ["Area", "Item", "Element", "Unit"]
    long = meat.melt(id_vars=id_cols, value_vars=year_cols, var_name="year_col", value_name="value")
    long["year"] = long["year_col"].str[1:].astype(int)
    long = long.dropna(subset=["value"])
    long["date"] = pd.to_datetime(dict(year=long["year"], month=1, day=1), errors="coerce")
    out = pd.DataFrame({
        "area": long["Area"],
        "item": long["Item"],
        "unit": long["Unit"],
        "date": long["date"],
        "value": pd.to_numeric(long["value"], errors="coerce"),
    })
    return out.dropna(subset=["area", "item", "date", "value"])


def main():
    parser = argparse.ArgumentParser(description="FAO food + meat prices pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Keep full history (default keeps last 3 years)")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"FAO Prices  mode={mode}")

    min_year = 1900 if args.backfill else now.year - INCREMENTAL_YEARS

    os.makedirs(FOOD_OUTPUT_DIR, exist_ok=True)
    os.makedirs(MEAT_OUTPUT_DIR, exist_ok=True)

    print("  Downloading Consumer Price Indices (CP) bulk data...")
    cp_raw = download_csv(CP_ZIP_URL, CP_CSV_NAME)
    food = parse_food_prices(cp_raw, min_year)
    print(f"    {len(food):,} food-CPI observations")

    print("  Downloading Producer Prices (PP) bulk data...")
    pp_raw = download_csv(PP_ZIP_URL, PP_CSV_NAME)
    meat = parse_meat_prices(pp_raw, min_year)
    print(f"    {len(meat):,} meat producer-price observations")

    fetched_at = now.isoformat()

    if not food.empty:
        food["fetched_at"] = fetched_at
        food = food.sort_values(["area", "item", "date"]).reset_index(drop=True)
        path = write_partitioned(food, FOOD_OUTPUT_DIR, f"fao_food_prices_{mode}_{today}.parquet")
        print(f"  -> {path}  ({len(food):,} rows, {food['area'].nunique()} areas)")
    else:
        print("  No food-CPI data to write.")

    if not meat.empty:
        meat["fetched_at"] = fetched_at
        meat = meat.sort_values(["area", "item", "date"]).reset_index(drop=True)
        path = write_partitioned(meat, MEAT_OUTPUT_DIR, f"fao_meat_prices_{mode}_{today}.parquet")
        print(f"  -> {path}  ({len(meat):,} rows, {meat['area'].nunique()} areas, "
              f"{meat['item'].nunique()} meat items)")
    else:
        print("  No meat producer-price data to write.")

    print("\n--- FAO PRICES PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
