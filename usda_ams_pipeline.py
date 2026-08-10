#!/usr/bin/env python3
"""
USDA AMS Market News Pipeline - wholesale terminal and retail prices for fresh
produce (including avocados) and other specialty crops.

The USDA Agricultural Marketing Service publishes daily/weekly market news
reports. Access is via the MARS API (https://marsapi.ams.usda.gov), which
requires a free registration key shown in "My Profile" after creating a
MyMarketNews account at https://mymarketnews.ams.usda.gov/.

Verified against the live API 2026-08-03 AND 2026-08-04 (this pipeline's
first-ever live run, once a real key existed -- the 2026-08-03 pass had
verified auth/base-URL but never actually fetched priced data). See
docs/SOURCES.md for the full correction history. Current understanding:
  Base URL  : https://marsapi.ams.usda.gov/services/v3.1
  Auth      : HTTP Basic, username = API key, empty password
  Endpoint  : GET {base}/reports/{slug}/Report Details?lastDays=N
              NOT {base}/{slug} (404s) and NOT {base}/reports/{slug} alone
              (that returns the "Report Header" section -- weekly narrative
              text, no per-commodity prices at all -- by default; the
              "Report Details" section has to be requested explicitly via
              the path).
  Date filtering gotcha (the big one, found live 2026-08-04): date_start/
  date_end are silently IGNORED on Report Details, on both v1.2 and v3.1
  -- confirmed by requesting a 2020-01-01..2020-01-07 window and getting
  2026 dates back, with an identical stats.totalRows regardless of what
  range was asked for. Requesting Report Details with no filter at all
  also hangs/times out server-side (tried up to 90s). The parameter that
  actually works is lastDays (documented at {base}/help, v3.1 only) --
  confirmed live: lastDays=14 on FVWRETAIL returns exactly 2 report weeks
  and a stats.totalRows that changes correctly with the window, in ~1s.
  The old date_start/date_end approach against v1.2 took 90-120s per
  terminal-market report (fetching ~100k basically-unfiltered rows every
  time regardless of window) and this pipeline's very first live run
  timed out at 900s because of it.
  Limits    : 100,000 records per request, no offset/pagination param in
              the API's own /services/help listing. With lastDays scoped
              correctly this is a non-issue for INCREMENTAL_DAYS-sized
              windows (a few thousand rows, not 100k) -- BACKFILL uses a
              much larger lastDays value and WILL still truncate at 100k
              for dense reports; that's a real, currently-unmitigated
              limit of the API itself, not a bug to "fix" without a real
              pagination mechanism to fix it with.
              AMS also blocks high-frequency polling, so keep
              REQUEST_INTERVAL sane and send a real User-Agent.

Reports of interest (report slugs on the MARS API):
  FVWRETAIL  - "Weekly Grocery Store Specialty Crops Feature Activity"
               (weekly retail produce prices from 400+ retailers, incl.
               avocados) -- VERIFIED live 2026-08-04, real field shape:
               commodity/variety/region/size/wtd_avg_price/organic/...
  {CITY}_FV010 / {CITY}_FV020 - per-city terminal-market fruit/vegetable
               price reports. There is NO single national terminal-market
               report -- "FVWV" (the originally-assumed slug) does not
               exist anywhere in the live report catalog (confirmed
               2026-08-04 against the full /reports listing, 1049 reports).
               ~30 cities publish _FV010/_FV020 pairs, but roughly half are
               marked "(Discontinued)" in their own report_title (Dallas,
               San Francisco, Montreal, several European/Asian cities) --
               WHOLESALE_REPORT_SLUGS below is a hand-picked 6-city
               geographic spread of the ACTIVE US ones only, same kind of
               judgment call as kroger_pipeline.py's 5 tracked ZIPs. Real
               field shape differs from retail: commodity/variety/package/
               item_size/low_price/high_price/market_location_name/...

CLI:
  python usda_ams_pipeline.py             # incremental (last 14 days)
  python usda_ams_pipeline.py --backfill  # last ~10 years (may truncate
                                           # per-report at the 100k-row
                                           # cap for dense ones -- see
                                           # Limits above)

Outputs:
  storage/raw/usda/ams_wholesale/...  (CATALOG: usda_ams_wholesale)
  storage/raw/usda/ams_retail/...     (CATALOG: usda_ams_retail)
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

AMS_API_KEY = os.environ.get("USDA_AMS_API_KEY", "")
AMS_BASE = "https://marsapi.ams.usda.gov/services/v3.1"

OUTPUT_DIR = os.path.join("storage", "raw", "usda")
REQUEST_INTERVAL = 1.0
MAX_RETRIES = 3
BACKOFF_SECONDS = 60
INCREMENTAL_DAYS = 14
BACKFILL_DAYS = 3650   # ~10 years; longer just means more truncation risk
                        # at the 100k-row cap, not more real coverage

RETAIL_REPORT_SLUGS = [
    "FVWRETAIL",   # Weekly Grocery Store Specialty Crops Feature Activity (incl. avocados)
]

# Active (non-"Discontinued") US terminal markets: Atlanta, Boston, Chicago,
# Los Angeles, New York, Miami -- South/Northeast/Midwest/West/Southeast
# spread. Each city publishes separate fruit (FV010) and vegetable (FV020)
# reports. lastDays-based requests take ~5s each (vs. 90-120s for the old
# broken date_start/date_end approach), so all 6 cities are practical.
WHOLESALE_REPORT_SLUGS = [
    "AJ_FV010", "AJ_FV020",   # Atlanta
    "BH_FV010", "BH_FV020",   # Boston
    "HX_FV010", "HX_FV020",   # Chicago
    "HC_FV010", "HC_FV020",   # Los Angeles
    "NX_FV010", "NX_FV020",   # New York
    "MH_FV010", "MH_FV020",   # Miami
]


def get_report(slug: str, last_days: int) -> list[dict]:
    """
    Fetch a single AMS MARS report slug's "Report Details" section (the
    per-commodity price line items -- NOT the default "Report Header"
    section, which is just weekly narrative text) as a list of raw records.

    Uses lastDays, not date_start/date_end -- see module docstring.
    """
    headers = {
        "Accept": "application/json",
        "User-Agent": "consumer-goods-price-pipeline/0.1 (personal research)",
    }
    params = {"lastDays": str(last_days)}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(f"{AMS_BASE}/reports/{slug}/Report Details", params=params,
                             headers=headers, auth=(AMS_API_KEY, ""), timeout=90)
            if r.status_code == 200:
                payload = r.json()
                batch = payload.get("results", payload.get("data", []))
                stats = payload.get("stats", {})
                if stats.get("returnedRows") == stats.get("userAllowedRows") and stats.get("totalRows", 0) > stats.get("returnedRows", 0):
                    print(f"    [!] {slug}: hit the {stats.get('userAllowedRows'):,}-row cap "
                          f"({stats.get('totalRows'):,} rows matched) -- truncated")
                return batch
            if r.status_code == 429:
                wait = BACKOFF_SECONDS * attempt
                print(f"  429 -- backing off {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
            elif r.status_code in (401, 403):
                print(f"  Auth error {r.status_code}: {r.text[:150]} -- check USDA_AMS_API_KEY")
                return []
            else:
                print(f"  HTTP {r.status_code}: {r.text[:150]}")
                return []
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt}): {e}")
            time.sleep(BACKOFF_SECONDS)
    return []


def parse_reports(records: list[dict]) -> pd.DataFrame:
    """
    Flatten "Report Details" records into long-format price observations.

    Retail (FVWRETAIL) and terminal (*_FV010/_FV020) reports are different
    schemas -- verified live 2026-08-04, none of the field names this
    function originally guessed ("price", "location"/"market"/"city",
    "slug"/"report", "date") exist in either real shape:
      Retail   : wtd_avg_price (single weighted-average price), region,
                 size, report_begin_date.
      Terminal : low_price/high_price (a range, no single point price --
                 averaged here), market_location_name, item_size + package,
                 report_date.
    """
    rows = []
    for rec in records:
        if "wtd_avg_price" in rec:
            price = rec.get("wtd_avg_price")
            unit_size = rec.get("size")
            location = rec.get("region")
        else:
            low = pd.to_numeric(rec.get("low_price"), errors="coerce")
            high = pd.to_numeric(rec.get("high_price"), errors="coerce")
            price = (low + high) / 2 if pd.notna(low) and pd.notna(high) else (low if pd.notna(low) else high)
            unit_size = rec.get("item_size")
            location = rec.get("market_location_name")
        date = rec.get("report_begin_date") or rec.get("report_date")
        rows.append({
            "report":       rec.get("slug_name"),
            "commodity":    rec.get("commodity"),
            "variety":      rec.get("variety"),
            "grade":        rec.get("grade"),
            "size":         unit_size,
            "unit":         rec.get("package"),
            "organic":      rec.get("organic"),
            "origin":       rec.get("origin"),
            "location":     location,
            "date":         pd.to_datetime(date, format="%m/%d/%Y", errors="coerce"),
            "price":        pd.to_numeric(price, errors="coerce"),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["fetched_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return df.dropna(subset=["commodity", "date", "price"])


def main():
    parser = argparse.ArgumentParser(description="USDA AMS market news price pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help=f"Fetch the last {BACKFILL_DAYS} days (may truncate for dense reports)")
    args = parser.parse_args()

    if not AMS_API_KEY:
        print("ERROR: No USDA_AMS_API_KEY found. Register free at https://mymarketnews.ams.usda.gov/")
        return

    os.makedirs(os.path.join(OUTPUT_DIR, "ams_wholesale"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "ams_retail"), exist_ok=True)

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    last_days = BACKFILL_DAYS if args.backfill else INCREMENTAL_DAYS
    print(f"Mode: {'BACKFILL' if args.backfill else 'INCREMENTAL'} (lastDays={last_days})")

    for label, slug_list, subdir, prefix in [
        ("retail produce", RETAIL_REPORT_SLUGS, "ams_retail", "usda_ams_retail"),
        ("wholesale terminal", WHOLESALE_REPORT_SLUGS, "ams_wholesale", "usda_ams_wholesale"),
    ]:
        print(f"\n--- {label} ---")
        frames = []
        for slug in slug_list:
            print(f"  {slug}...")
            records = get_report(slug, last_days)
            if records:
                df = parse_reports(records)
                if not df.empty:
                    frames.append(df)
                    print(f"    {len(df):,} rows")
            time.sleep(REQUEST_INTERVAL)

        if not frames:
            print("[!] No data returned.")
            continue
        combined = pd.concat(frames, ignore_index=True).drop_duplicates()
        path = write_partitioned(
            combined, os.path.join(OUTPUT_DIR, subdir),
            f"{prefix}_{mode}_{today}.parquet",
        )
        print(f"[+] {path}  ({len(combined):,} rows)")

    print("\n--- COMPLETE ---")


if __name__ == "__main__":
    main()
