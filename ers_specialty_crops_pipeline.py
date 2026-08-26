#!/usr/bin/env python3
"""
USDA ERS Specialty Crops Pipeline - retail prices + CPI/PPI price indexes for
fruit/tree nuts and vegetables/dry pulses, plus optional trade data.

The USDA Economic Research Service publishes monthly CSVs under its "Fruit and
Tree Nuts Data" and "Vegetables and Pulses Data" products (public domain).
Each price-index CSV is a long-format monthly time series per commodity:
BLS Consumer Price Indexes, Producer Price Indexes, and average retail prices
for selected commodities (grapes, oranges, avocados, potatoes ...), 2000 to
present (~2.7 MB each). Access is keyless; the server appends a rotating
?v=NNNNN cache-buster on its download links -- request WITHOUT the query
string and follow redirects.

Verified live 2026-08-24:
  Fruit & Tree Nuts : GET https://www.ers.usda.gov/media/6476/price-index-data.csv  (200)
  Vegetables/Pulses : GET https://www.ers.usda.gov/media/5629/price-index-data.csv  (200)
  Columns           : seriesType,seriesID,seriesName,year,monthNumber,unit,value
                      seriesType in {Average retail price, Consumer Price Index,
                      Producer Price Index}; monthNumber 1-12.
  ANOMALY: both media IDs served byte-identical content on 2026-08-24 (same
  SHA256) even though the ERS pages list them as separate downloads. The file
  itself covers BOTH fruit and vegetable series, so both tables still land;
  re-check whether ERS merges/splits these files before relying on the two
  tables being distinct.

Optional trade CSVs (--with-trade): US imports/exports by partner country,
monthly value (thousand dollars) and volume (thousand pounds). These are
95-127 MB each, so they are OFF by default:
  Fruit trade : https://www.ers.usda.gov/media/6475/fruit-and-tree-nuts-trade-data.csv
  Veg trade   : https://www.ers.usda.gov/media/5628/vegetables-and-dry-pulses-trade-data.csv

Because each file is the full history, every run re-downloads it; curated.py
dedups by natural key. Incremental keeps the last 25 months (recent months get
restated); --backfill keeps everything.

CLI:
  python ers_specialty_crops_pipeline.py                # incremental, prices only
  python ers_specialty_crops_pipeline.py --backfill     # full history
  python ers_specialty_crops_pipeline.py --with-trade   # also fetch 95-127MB trade CSVs

Outputs:
  storage/raw/ers/fruit_nut_prices/...  (CATALOG: ers_fruit_nut_prices)
  storage/raw/ers/veg_prices/...        (CATALOG: ers_veg_prices)
  --with-trade adds:
  storage/raw/ers/fruit_nut_trade/...   (CATALOG: ers_fruit_nut_trade)
  storage/raw/ers/veg_trade/...         (CATALOG: ers_veg_trade)
"""

import argparse
import datetime
import io
import os
import time

import pandas as pd
import requests

from storage_utils import write_partitioned

FRUIT_PRICES_URL = "https://www.ers.usda.gov/media/6476/price-index-data.csv"
VEG_PRICES_URL = "https://www.ers.usda.gov/media/5629/price-index-data.csv"
FRUIT_TRADE_URL = "https://www.ers.usda.gov/media/6475/fruit-and-tree-nuts-trade-data.csv"
VEG_TRADE_URL = "https://www.ers.usda.gov/media/5628/vegetables-and-dry-pulses-trade-data.csv"

OUTPUT_DIR = os.path.join("storage", "raw", "ers")
MAX_RETRIES = 3
BACKOFF_SECONDS = 30
INCREMENTAL_MONTHS = 25


def download_csv(url: str, timeout: int = 180) -> pd.DataFrame:
    """Download one ERS CSV into a DataFrame (no cache-buster query string)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=timeout,
                             headers={"User-Agent": "consumer-goods-price-pipeline/0.1 (personal research)"})
            if r.status_code == 200:
                return pd.read_csv(io.BytesIO(r.content), low_memory=False)
            print(f"  HTTP {r.status_code}: {r.text[:150]}")
            return pd.DataFrame()
        except (requests.RequestException, pd.errors.ParserError) as exc:
            print(f"  Download/parse error (attempt {attempt}): {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)
    return pd.DataFrame()


def parse_prices(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize the ERS long-format price-index CSV into the store's schema."""
    if raw.empty:
        return raw

    date = pd.to_datetime(
        raw["year"].astype(str) + "-" + raw["monthNumber"].astype(str).str.zfill(2) + "-01",
        errors="coerce",
    )
    rows = pd.DataFrame({
        "date":        date,
        "commodity":   raw.get("seriesName"),
        "series_id":   raw.get("seriesID"),
        "series_type": raw.get("seriesType"),
        "unit":        raw.get("unit"),
        "value":       pd.to_numeric(raw.get("value"), errors="coerce"),
    })
    return rows.dropna(subset=["commodity", "series_id", "date", "value"])


def parse_trade(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize the ERS trade CSV (imports/exports by partner country)."""
    if raw.empty:
        return raw

    date = pd.to_datetime(
        raw["Year"].astype(str) + "-" + raw["MonthNumber"].astype(str).str.zfill(2) + "-01",
        errors="coerce",
    )
    rows = pd.DataFrame({
        "date":             date,
        "trade_flow":       raw.get("Trade"),
        "partner_country":  raw.get("GeographicDesc"),
        "market_year":      raw.get("MarketYear"),
        "group":            raw.get("Group"),
        "subgroup":         raw.get("Subgroup"),
        "market_segment":   raw.get("MarketSegment"),
        "commodity":        raw.get("CommodityName"),
        "commodity_detail": raw.get("CommodityDetail"),
        "unit_type":        raw.get("UnitType"),
        "unit_desc":        raw.get("UnitDesc"),
        "amount":           pd.to_numeric(raw.get("Amount"), errors="coerce"),
    })
    return rows.dropna(subset=["commodity", "date", "amount"])


def _filter_incremental(df: pd.DataFrame, now: datetime.datetime, months: int) -> pd.DataFrame:
    cutoff = (now - datetime.timedelta(days=30 * months)).date()
    kept = df[df["date"].dt.date >= cutoff]
    print(f"  Incremental: kept {len(kept):,} rows since {cutoff}")
    return kept


def main():
    parser = argparse.ArgumentParser(description="USDA ERS specialty crop retail prices + indexes")
    parser.add_argument("--backfill", action="store_true",
                        help="Keep the full history (default keeps last 25 months)")
    parser.add_argument("--with-trade", action="store_true",
                        help="Also fetch the 95-127MB fruit/vegetable trade CSVs (slow)")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"USDA ERS Specialty Crops  mode={mode}")

    os.makedirs(os.path.join(OUTPUT_DIR, "fruit_nut_prices"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "veg_prices"), exist_ok=True)

    for label, url, subdir, prefix, parse_fn in [
        ("fruit & tree nut prices", FRUIT_PRICES_URL, "fruit_nut_prices", "ers_fruit_nut_prices", parse_prices),
        ("vegetable & pulse prices", VEG_PRICES_URL, "veg_prices", "ers_veg_prices", parse_prices),
    ]:
        print(f"\n--- {label} ---")
        raw = download_csv(url)
        if raw.empty:
            print("  No data downloaded.")
            continue
        out = parse_fn(raw)
        print(f"  Parsed {len(out):,} observations "
              f"({out['commodity'].nunique()} commodities, {out['series_type'].nunique()} series types)")
        if not args.backfill:
            out = _filter_incremental(out, now, INCREMENTAL_MONTHS)
        if out.empty:
            print("  Nothing to write.")
            continue
        out["fetched_at"] = now.isoformat()
        out = out.sort_values(["series_id", "date"]).reset_index(drop=True)
        path = write_partitioned(out, os.path.join(OUTPUT_DIR, subdir),
                                 f"{prefix}_{mode}_{today}.parquet")
        print(f"  -> {path}  ({len(out):,} rows)")

    if args.with_trade:
        for label, url, subdir, prefix in [
            ("fruit & tree nut trade", FRUIT_TRADE_URL, "fruit_nut_trade", "ers_fruit_nut_trade"),
            ("vegetable & pulse trade", VEG_TRADE_URL, "veg_trade", "ers_veg_trade"),
        ]:
            print(f"\n--- {label} ---")
            raw = download_csv(url, timeout=600)
            if raw.empty:
                print("  No data downloaded.")
                continue
            out = parse_trade(raw)
            print(f"  Parsed {len(out):,} observations ({out['commodity'].nunique()} commodities)")
            if not args.backfill:
                out = _filter_incremental(out, now, INCREMENTAL_MONTHS)
            if out.empty:
                print("  Nothing to write.")
                continue
            out["fetched_at"] = now.isoformat()
            out = out.sort_values(["trade_flow", "partner_country", "commodity", "date"]).reset_index(drop=True)
            path = write_partitioned(out, os.path.join(OUTPUT_DIR, subdir),
                                     f"{prefix}_{mode}_{today}.parquet")
            print(f"  -> {path}  ({len(out):,} rows)")

    print("\n--- ERS SPECIALTY CROPS PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
