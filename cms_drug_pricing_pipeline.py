#!/usr/bin/env python3
"""
CMS Drug Pricing Pipeline — Medicare Part D spending by drug.

Keyless data.cms.gov Data API. Verified live 2026-08-04:
  Catalog     : https://data.cms.gov/data.json
  Dataset     : "Medicare Part D Spending by Drug" (identifier
                7e0b4365-fd63-4a29-8f5e-e0ac9f66a81b), annual, updated 2026-06-25
  Data URL    : https://data.cms.gov/data-api/v1/dataset/{uuid}/data?size=5000&offset=N
                (5000 rows/page hard cap; paginate with offset until a page
                returns < 5000 rows)

Gotcha found live: this dataset has NO NDC column — it's aggregated to
brand-name x generic-name x manufacturer, not NDC-level (the reserved
SCHEMAS/KEYS entries for this table originally assumed an NDC natural key;
corrected here to (brand_name, generic_name, manufacturer, year), which is
the real grain of this data).

Program/payer spending, not retail cash prices: `avg_spnd_per_dsg_unt_wghtd`
(the closest thing to a "price") is Medicare's average reimbursement per
dosage unit, net of rebates excluded and small cells suppressed — useful as
a drug-cost trend proxy, not a consumer sticker price. Source is wide format
(one row per drug, columns repeat per year: Tot_Spndng_{year},
Avg_Spnd_Per_Dsg_Unt_Wghtd_{year}, ...); melted here into one row per
drug/year.

CLI:
  python cms_drug_pricing_pipeline.py             # incremental == backfill (annual dataset, always re-pulled whole)
  python cms_drug_pricing_pipeline.py --backfill  # same; flag kept for run_all.py symmetry

Outputs:
  storage/raw/cms/drug_pricing/cms_drug_pricing_{mode}_{YYYYMMDD}.parquet
  (CATALOG: cms_drug_pricing)
"""

import argparse
import datetime
import os

import pandas as pd
import requests

from storage_utils import write_partitioned

DATASET_UUID = "7e0b4365-fd63-4a29-8f5e-e0ac9f66a81b"
DATA_URL = f"https://data.cms.gov/data-api/v1/dataset/{DATASET_UUID}/data"
PAGE_SIZE = 5000

OUTPUT_DIR = os.path.join("storage", "raw", "cms", "drug_pricing")
MAX_RETRIES = 3
BACKOFF_SECONDS = 15


def fetch_all_pages() -> pd.DataFrame:
    frames = []
    offset = 0
    while True:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(DATA_URL, params={"size": PAGE_SIZE, "offset": offset}, timeout=90)
                if r.status_code == 200:
                    page = r.json()
                    break
                print(f"  HTTP {r.status_code} at offset={offset}: {r.text[:150]}")
                return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            except requests.RequestException as exc:
                print(f"  Request error (attempt {attempt}) at offset={offset}: {exc}")
                import time
                time.sleep(BACKOFF_SECONDS * attempt)
        else:
            break

        if not page:
            break
        frames.append(pd.DataFrame(page))
        print(f"    fetched {len(page)} rows (offset={offset})")
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def parse_frame(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw

    year_cols = sorted({c.split("_")[-1] for c in raw.columns if c.startswith("Tot_Spndng_")})
    rows = []
    for _, r in raw.iterrows():
        for year in year_cols:
            spend_col = f"Tot_Spndng_{year}"
            avg_unit_col = f"Avg_Spnd_Per_Dsg_Unt_Wghtd_{year}"
            avg_clm_col = f"Avg_Spnd_Per_Clm_{year}"
            claims_col = f"Tot_Clms_{year}"
            benes_col = f"Tot_Benes_{year}"
            if spend_col not in raw.columns:
                continue
            value = pd.to_numeric(r.get(avg_unit_col), errors="coerce")
            total_spend = pd.to_numeric(r.get(spend_col), errors="coerce")
            if pd.isna(value) and pd.isna(total_spend):
                continue
            rows.append({
                "brand_name": r.get("Brnd_Name"),
                "generic_name": r.get("Gnrc_Name"),
                "manufacturer": r.get("Mftr_Name"),
                "drug_name": r.get("Brnd_Name") or r.get("Gnrc_Name"),
                # Not "year": write_partitioned's Hive year=YYYY/ partition column
                # (write date) would collide with and silently overwrite a
                # same-named domain column when DuckDB reads it back.
                "spending_year": int(year),
                "total_spending": total_spend,
                "avg_spend_per_dosage_unit": value,
                "avg_spend_per_claim": pd.to_numeric(r.get(avg_clm_col), errors="coerce"),
                "total_claims": pd.to_numeric(r.get(claims_col), errors="coerce"),
                "total_beneficiaries": pd.to_numeric(r.get(benes_col), errors="coerce"),
            })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="CMS Medicare Part D drug pricing pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="No-op vs incremental — the dataset is small enough to always re-pull whole")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"CMS Drug Pricing (Medicare Part D)  mode={mode}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("  Fetching pages...")
    raw = fetch_all_pages()
    if raw.empty:
        print("  No data downloaded.")
        return
    print(f"  Downloaded {len(raw):,} drug rows")

    out = parse_frame(raw)
    out = out.dropna(subset=["drug_name", "spending_year"])
    print(f"  Parsed {len(out):,} drug/year observations")

    if out.empty:
        print("  Nothing to write.")
        return

    out["fetched_at"] = now.isoformat()
    out = out.sort_values(["brand_name", "generic_name", "spending_year"]).reset_index(drop=True)

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"cms_drug_pricing_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['drug_name'].nunique()} drugs, "
          f"{out['spending_year'].nunique()} years)")

    print("\n--- CMS DRUG PRICING PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
