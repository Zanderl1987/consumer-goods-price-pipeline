#!/usr/bin/env python3
"""
Statistics Canada Retail Prices Pipeline — absolute retail price levels in CAD.

Statistics Canada table 18-10-0245-01 ("Monthly average retail prices for
selected products") gives real dollar prices for 110 everyday consumer goods —
milk, eggs, bread, ground beef, butter, produce, and household goods like
deodorant, toothpaste and shampoo — for Canada, each province and two
territories. Scanner-data quality since Jan 2024. This is the single best free
source of absolute grocery price levels (vs indexes) in the repo.

Access is keyless via the WDS (Web Data Service) REST API. Verified live
2026-08-03:
  PID (8-digit)    : 18100245   (table 18-10-0245-01 simple view)
  Metadata         : POST https://www150.statcan.gc.ca/t1/wds/rest/getCubeMetadata
  Full CSV ZIP     : GET  https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/18100245/en
                     -> https://www150.statcan.gc.ca/n1/tbl/csv/18100245-eng.zip
  Shape            : 110 products x 13 geographies x monthly, 2017-01 to present
                     (~137k observations, ~1.6 MB zipped)
  Limits           : 25 req/s/IP (50/s server-wide); tables locked 00:00-08:30
                     ET (HTTP 409) while updating.

Because the whole table is a single small CSV, every run re-downloads the ZIP
and curates it. --backfill keeps the full history; incremental keeps the last
13 months (StatCan restates recent months, so the overlap keeps curated rows
fresh without bloating raw storage).

CLI:
  python statcan_retail_prices_pipeline.py             # incremental (last 13 months)
  python statcan_retail_prices_pipeline.py --backfill  # full history 2017->present

Outputs:
  storage/raw/statcan/retail_prices/statcan_retail_prices_{mode}_{YYYYMMDD}.parquet
  (CATALOG: statcan_retail_prices)
"""

import argparse
import datetime
import io
import os
import zipfile

import pandas as pd
import requests

from storage_utils import write_partitioned

WDS_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
STATCAN_PID = "18100245"
FULL_TABLE_URL = f"{WDS_BASE}/getFullTableDownloadCSV/{STATCAN_PID}/en"

OUTPUT_DIR = os.path.join("storage", "raw", "statcan", "retail_prices")
MAX_RETRIES = 3
BACKOFF_SECONDS = 30
INCREMENTAL_MONTHS = 13

# StatCan metadata says the WDS is unavailable 00:00-08:30 ET while tables are
# being updated. Detected by a 409 on the download call; we surface a clear
# message instead of a confusing error.
LOCKED_WINDOW_START = (0, 0)
LOCKED_WINDOW_END = (8, 30)


def _in_locked_window() -> bool:
    """True when the current local time is inside StatCan's update window."""
    now = datetime.datetime.now()
    t = (now.hour, now.minute)
    return LOCKED_WINDOW_START <= t < LOCKED_WINDOW_END


def get_full_table_csv_url() -> str:
    """Resolve the current full-table CSV ZIP URL via the WDS endpoint."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(FULL_TABLE_URL, timeout=60)
            if r.status_code == 200:
                payload = r.json()
                url = payload.get("object")
                if isinstance(url, str) and url:
                    return url
                print(f"  Unexpected response: {str(payload)[:200]}")
                return ""
            if r.status_code == 409:
                print("  StatCan WDS locked (00:00-08:30 ET update window); retry later.")
                return ""
            print(f"  HTTP {r.status_code}: {r.text[:150]}")
            return ""
        except requests.RequestException as exc:
            print(f"  Request error (attempt {attempt}): {exc}")
            time_sleep = BACKOFF_SECONDS * attempt
            print(f"  backing off {time_sleep}s")
            import time
            time.sleep(time_sleep)
    return ""


def download_full_table(url: str) -> pd.DataFrame:
    """Download the CSV ZIP and read the main table into a DataFrame."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                zf = zipfile.ZipFile(io.BytesIO(r.content))
                csv_name = [n for n in zf.namelist() if n.endswith(".csv") and "MetaData" not in n]
                if not csv_name:
                    print(f"  No main CSV in ZIP: {zf.namelist()}")
                    return pd.DataFrame()
                df = pd.read_csv(zf.open(csv_name[0]), encoding="utf-8-sig",
                                 low_memory=False)
                return df
            print(f"  HTTP {r.status_code}: {r.text[:150]}")
            return pd.DataFrame()
        except (requests.RequestException, zipfile.BadZipFile, pd.errors.ParserError) as exc:
            print(f"  Download/parse error (attempt {attempt}): {exc}")
            import time
            time.sleep(BACKOFF_SECONDS * attempt)
    return pd.DataFrame()


def parse_table(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize StatCan long-format CSV rows into the store's schema."""
    if raw.empty:
        return raw

    price = pd.to_numeric(raw.get("VALUE"), errors="coerce")
    date = pd.to_datetime(raw["REF_DATE"] + "-01", errors="coerce")
    rows = pd.DataFrame({
        "geo":        raw.get("GEO"),
        "item":       raw.get("Products"),
        "uom":        raw.get("UOM"),
        "date":       date,
        "price":      price,
        "vector":     raw.get("VECTOR"),
        "coordinate": raw.get("COORDINATE"),
        "status":     raw.get("STATUS"),
        "symbol":     raw.get("SYMBOL"),
        "decimals":   raw.get("DECIMALS"),
    })
    return rows.dropna(subset=["geo", "item", "date", "price"])


def main():
    parser = argparse.ArgumentParser(description="Statistics Canada retail price pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Keep the full 2017->present history (default keeps last 13 months)")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"Statistics Canada Retail Prices  mode={mode}")

    if _in_locked_window():
        print("  Skipping: StatCan WDS is in its 00:00-08:30 ET update window.")
        print("  Re-run after 8:30 AM local time.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("  Resolving full-table download URL...")
    csv_url = get_full_table_csv_url()
    if not csv_url:
        print("  Could not resolve download URL. See messages above.")
        return

    print(f"  Downloading {csv_url}")
    df = download_full_table(csv_url)
    if df.empty:
        print("  No data downloaded.")
        return

    out = parse_table(df)
    print(f"  Parsed {len(out):,} observations")

    if not args.backfill:
        cutoff = (now - datetime.timedelta(days=30 * INCREMENTAL_MONTHS)).date()
        out = out[out["date"].dt.date >= cutoff]
        print(f"  Incremental: kept {len(out):,} rows since {cutoff}")

    if out.empty:
        print("  Nothing to write.")
        return

    out["fetched_at"] = now.isoformat()
    out = out.sort_values(["geo", "item", "date"]).reset_index(drop=True)

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"statcan_retail_prices_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['item'].nunique()} items, "
          f"{out['geo'].nunique()} geographies)")

    print("\n--- STATCAN RETAIL PRICES PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
