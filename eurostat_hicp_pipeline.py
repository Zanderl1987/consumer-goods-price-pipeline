#!/usr/bin/env python3
"""
Eurostat HICP Pipeline — Harmonised Index of Consumer Prices, EU/EFTA.

Keyless JSON-stat 2.0 API. Verified live 2026-08-04:
  GET https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_midx
      ?format=JSON&lang=EN&coicop={CP00|CP01}&unit=I15

Indices only (no absolute euro prices — PRC_AVG, the old absolute-price
dataset, was retired; verified 404 on 2026-08-03, see docs/SOURCES.md).
unit=I15 is "Index, 2015=100", the current standard base. One request per
COICOP group returns all ~45 geos (EU aggregates, all member states, EFTA,
a few accession/candidate countries, UK, US) x full monthly history
(1996-present) as a single JSON-stat cube — small enough (~16k cells) to
fetch whole and reshape locally rather than paginating per country.

COICOP groups fetched: CP00 (all-items headline HICP) and CP01 (food and
non-alcoholic beverages) — the two most relevant to a consumer-goods price
tracker.

Response shape: JSON-stat 2.0 "flat value dict" — `dimension.*.category.index`
maps each dimension's category codes to a position, and `value` is keyed by
the row-major flattened index across dimensions in `id` order
([freq, unit, coicop, geo, time]). See parse_jsonstat() below.

CLI:
  python eurostat_hicp_pipeline.py             # incremental (last 24 months)
  python eurostat_hicp_pipeline.py --backfill  # full history 1996->present

Outputs:
  storage/raw/eurostat/hicp/eurostat_hicp_{mode}_{YYYYMMDD}.parquet
  (CATALOG: eurostat_hicp)
"""

import argparse
import datetime
import os
import time

import pandas as pd
import requests

from storage_utils import write_partitioned

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_midx"
COICOP_GROUPS = ["CP00", "CP01"]
UNIT = "I15"

OUTPUT_DIR = os.path.join("storage", "raw", "eurostat", "hicp")
MAX_RETRIES = 3
BACKOFF_SECONDS = 20
INCREMENTAL_MONTHS = 24


def fetch_coicop(coicop: str) -> dict:
    params = {"format": "JSON", "lang": "EN", "coicop": coicop, "unit": UNIT}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
            print(f"  HTTP {r.status_code} for coicop={coicop}: {r.text[:150]}")
            return {}
        except requests.RequestException as exc:
            print(f"  Request error (attempt {attempt}) for coicop={coicop}: {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)
    return {}


def parse_jsonstat(payload: dict) -> pd.DataFrame:
    """Flatten a JSON-stat 2.0 cube into long rows: geo, coicop, unit, date, value."""
    if not payload or "value" not in payload:
        return pd.DataFrame()

    dim_ids = payload["id"]                     # e.g. ["freq","unit","coicop","geo","time"]
    sizes = payload["size"]                     # e.g. [1,1,1,45,360]

    cat_index = {}   # dim_id -> {category_code: position}
    for dim_id in dim_ids:
        cat_index[dim_id] = payload["dimension"][dim_id]["category"]["index"]

    # Invert each dim's index -> ordered list of category codes by position
    pos_to_code = {}
    for dim_id, idx_map in cat_index.items():
        ordered = sorted(idx_map.items(), key=lambda kv: kv[1])
        pos_to_code[dim_id] = [code for code, _ in ordered]

    strides = [1] * len(sizes)
    for i in range(len(sizes) - 2, -1, -1):
        strides[i] = strides[i + 1] * sizes[i + 1]

    rows = []
    values = payload["value"]
    keys = values.keys() if isinstance(values, dict) else range(len(values))
    for key in keys:
        flat_idx = int(key)
        val = values[key] if isinstance(values, dict) else values[flat_idx]
        if val is None:
            continue
        coords = {}
        remainder = flat_idx
        for dim_id, stride in zip(dim_ids, strides):
            pos = remainder // stride
            remainder %= stride
            coords[dim_id] = pos_to_code[dim_id][pos]
        rows.append({
            "geo": coords.get("geo"),
            "coicop": coords.get("coicop"),
            "unit": coords.get("unit"),
            "date": coords.get("time"),
            "value": val,
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Eurostat HICP pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Keep the full 1996->present history (default keeps last 24 months)")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"Eurostat HICP  mode={mode}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    frames = []
    for coicop in COICOP_GROUPS:
        print(f"  Fetching coicop={coicop}...")
        payload = fetch_coicop(coicop)
        df = parse_jsonstat(payload)
        if not df.empty:
            frames.append(df)
            print(f"    {len(df):,} observations")
        time.sleep(1)

    if not frames:
        print("  No data downloaded.")
        return

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"] + "-01", errors="coerce")
    out["series_id"] = out["geo"] + "." + out["coicop"] + "." + out["unit"]
    out = out.dropna(subset=["geo", "coicop", "date", "value"])
    print(f"  Parsed {len(out):,} total observations")

    if not args.backfill:
        cutoff = (now - datetime.timedelta(days=30 * INCREMENTAL_MONTHS)).date()
        out = out[out["date"].dt.date >= cutoff]
        print(f"  Incremental: kept {len(out):,} rows since {cutoff}")

    if out.empty:
        print("  Nothing to write.")
        return

    out["fetched_at"] = now.isoformat()
    out = out.sort_values(["geo", "coicop", "date"]).reset_index(drop=True)

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"eurostat_hicp_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['geo'].nunique()} geos, "
          f"{out['coicop'].nunique()} coicop groups)")

    print("\n--- EUROSTAT HICP PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
