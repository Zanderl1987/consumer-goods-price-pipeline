#!/usr/bin/env python3
"""
IMF Primary Commodity Prices Pipeline — PCPS, 100+ global benchmark prices.

Keyless SDMX 3.0 REST API. Verified live 2026-08-04:
  Dataflow : IMF.RES,PCPS,~ (agency IMF.RES, dataflow PCPS, version "~"=latest)
  Data URL : https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.RES/PCPS/~/{key}
  Key      : COUNTRY.INDICATOR.DATA_TRANSFORMATION.FREQUENCY
             (TIME_PERIOD is the observation dimension, not part of the key)

Gotchas found live (none of this matched the generic SDMX docs/blog
examples, which describe a different dataflow's dimension order):
  - COUNTRY is NOT "W00" (the usual SDMX "world" code) for this dataflow —
    it's "G001". Confirmed by a full wildcard query and reading back the
    actual series dimension values in the response's `structures` block.
  - DATA_TRANSFORMATION values: INDEX, INDEX_PCH, INDEX_PCHY, USD. Fetching
    both INDEX and USD gives index-level series for the aggregate groups
    (PALLFNF, PAGRI, ...) and absolute per-unit USD prices for the ~120
    single-commodity series (PALUM, PBEEF, PCOPP, ...) in one call.
  - startPeriod/endPeriod query params are silently ignored by this
    dataflow's data endpoint — it always returns full history (back to
    1980-1992 depending on series). Filtering to an incremental window is
    done client-side after parsing.
  - Response is SDMX-JSON (compact), not CSV — format=csv is not honored.
    Series/observation values are position-indexed against the `structures`
    block's per-dimension `values` arrays; see parse_sdmx_json() below.

Indicator list (136 codes total; a subset covering consumer-relevant food/
metal/energy commodities is fetched — see INDICATORS below) resolved live
via the CL_PCPS_INDICATOR codelist.

CLI:
  python imf_commodities_pipeline.py             # incremental (last 24 months)
  python imf_commodities_pipeline.py --backfill  # full available history

Outputs:
  storage/raw/imf/commodities/imf_commodities_{mode}_{YYYYMMDD}.parquet
  (CATALOG: imf_commodities)
"""

import argparse
import datetime
import os

import pandas as pd
import requests

from storage_utils import write_partitioned

BASE_URL = "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.RES/PCPS/~"
COUNTRY = "G001"
TRANSFORMATIONS = ["INDEX", "USD"]
FREQUENCY = "M"

# Aggregate indices + individual commodities most relevant to a
# consumer-goods price tracker (food, energy, metals headline groups).
INDICATORS = [
    "PALLFNF", "PAGRI", "PFANDB", "PBEVE", "PFOOD", "PCERE", "PVOIL",
    "PMEAT", "PSUGA", "PRAWM", "PALLMETA", "PNRG",
    "PWHEAMT", "PMAIZMT", "PRICENPQ", "PSOYB", "PBARL",
    "PBEEF", "PPOULT", "PSMEA", "PPORK",
    "PCOFFOTM", "PCOFFROB", "PCOCO", "PSUGAISA", "PBANSOP",
    "PCOALAU", "PNGASUS", "PNGASEU", "POILAPSP", "POILBRE", "POILWTI",
    "PALUM", "PCOPP", "PIORECR", "PGOLD", "PSILVER", "PNICK", "PZINC",
]

OUTPUT_DIR = os.path.join("storage", "raw", "imf", "commodities")
INCREMENTAL_MONTHS = 24


def fetch() -> dict:
    indicator_key = "+".join(INDICATORS)
    transform_key = "+".join(TRANSFORMATIONS)
    key = f"{COUNTRY}.{indicator_key}.{transform_key}.{FREQUENCY}"
    r = requests.get(f"{BASE_URL}/{key}", timeout=120)
    r.raise_for_status()
    return r.json()


def parse_sdmx_json(payload: dict) -> pd.DataFrame:
    if not payload or "data" not in payload:
        return pd.DataFrame()

    data = payload["data"]
    datasets = data.get("dataSets", [])
    if not datasets or "series" not in datasets[0]:
        return pd.DataFrame()

    struct = data["structures"][0]
    series_dims = struct["dimensions"]["series"]
    obs_dim = struct["dimensions"]["observation"][0]
    time_values = [v["value"] for v in obs_dim["values"]]

    dim_values = {d["id"]: [v.get("id", v.get("value")) for v in d["values"]] for d in series_dims}
    dim_order = [d["id"] for d in series_dims]

    rows = []
    for series_key, series_obj in datasets[0]["series"].items():
        idxs = [int(i) for i in series_key.split(":")]
        coords = {dim_order[i]: dim_values[dim_order[i]][idxs[i]] for i in range(len(dim_order))}
        for obs_idx_str, obs_val in series_obj.get("observations", {}).items():
            value = obs_val[0] if isinstance(obs_val, list) else obs_val
            if value is None:
                continue
            time_period = time_values[int(obs_idx_str)]
            rows.append({
                "country": coords.get("COUNTRY"),
                "indicator": coords.get("INDICATOR"),
                "data_transformation": coords.get("DATA_TRANSFORMATION"),
                "frequency": coords.get("FREQUENCY"),
                "date": time_period,
                "value": value,
            })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="IMF PCPS commodity prices pipeline")
    parser.add_argument("--backfill", action="store_true",
                        help="Keep full available history (default keeps last 24 months)")
    args = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y%m%d")
    mode = "backfill" if args.backfill else "incremental"
    print(f"IMF Primary Commodity Prices  mode={mode}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"  Fetching {len(INDICATORS)} indicators x {len(TRANSFORMATIONS)} transformations...")
    payload = fetch()
    out = parse_sdmx_json(payload)
    if out.empty:
        print("  No data parsed.")
        return

    out["date"] = pd.to_datetime(out["date"].str.replace("-M", "-", regex=False) + "-01", errors="coerce")
    out["series_id"] = out["indicator"].astype(str) + "." + out["data_transformation"].astype(str)
    out = out.dropna(subset=["indicator", "date", "value"])
    print(f"  Parsed {len(out):,} observations")

    if not args.backfill:
        cutoff = (now - datetime.timedelta(days=30 * INCREMENTAL_MONTHS)).date()
        out = out[out["date"].dt.date >= cutoff]
        print(f"  Incremental: kept {len(out):,} rows since {cutoff}")

    if out.empty:
        print("  Nothing to write.")
        return

    out["fetched_at"] = now.isoformat()
    out = out.sort_values(["indicator", "data_transformation", "date"]).reset_index(drop=True)

    path = write_partitioned(
        out, OUTPUT_DIR,
        f"imf_commodities_{mode}_{today}.parquet",
    )
    print(f"  -> {path}  ({len(out):,} rows, {out['indicator'].nunique()} indicators)")

    print("\n--- IMF COMMODITIES PIPELINE COMPLETE ---")


if __name__ == "__main__":
    main()
