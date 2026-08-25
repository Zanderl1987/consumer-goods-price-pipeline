#!/usr/bin/env python3
"""
Unified Pipeline Runner - runs all consumer-goods price pipelines in
dependency order.

Stages
------
  Stage 1  - Free/public sources (BLS, USDA, EIA, international stats)
  Stage 2  - Retail / e-commerce APIs (Kroger, Walmart, eBay, Open Food Facts)
  Stage 3  - Derived (baskets, blended indexes built from Stage 1/2 output)

Usage
-----
  python run_all.py                        # incremental run (all stages)
  python run_all.py --backfill             # full available history
  python run_all.py --stage 1              # free/public sources only
  python run_all.py --only bls_avg_prices,usda_ams
  python run_all.py --skip kroger
  python run_all.py --dry-run              # print commands, don't execute
  python run_all.py --no-validate          # skip post-run validation
  python run_all.py --no-compact           # skip post-run curated compaction
"""

import argparse
import datetime
import os
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field

from dotenv import load_dotenv

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO_ROOT)

import curated
from validate import validate_table
from logging_utils import get_logger, log_pipeline_failure

load_dotenv()

log = get_logger("run_all")


# ── Pipeline registry ─────────────────────────────────────────────────────────

@dataclass
class PipelineSpec:
    name:             str
    file:             str
    desc:             str
    stage:            int
    tables:           list = field(default_factory=list)
    requires_env:     list = field(default_factory=list)
    backfill_args:    list = field(default_factory=list)
    incremental_args: list = field(default_factory=list)
    timeout:          int  = 600   # seconds; override for slow pipelines


PIPELINES: list[PipelineSpec] = [
    # ── Stage 1 - Free / public government sources ───────────────────────────
    PipelineSpec(
        name="bls_cpi",
        file="bls_cpi_pipeline.py",
        desc="BLS CPI detailed indexes by item and area + PPI",
        stage=1,
        tables=["bls_cpi", "bls_ppi"],
        backfill_args=["--backfill"],
        timeout=900,
    ),
    PipelineSpec(
        name="bls_avg_prices",
        file="bls_avg_prices_pipeline.py",
        desc="BLS average retail prices - grocery staples, energy, ~80 items",
        stage=1,
        tables=["bls_avg_prices"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    PipelineSpec(
        name="usda_ams",
        file="usda_ams_pipeline.py",
        desc="USDA AMS market news - wholesale terminal + retail fruit/veg (incl. avocados)",
        stage=1,
        tables=["usda_ams_wholesale", "usda_ams_retail"],
        requires_env=["USDA_AMS_API_KEY"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    PipelineSpec(
        name="usda_nass_prices",
        file="usda_nass_prices_pipeline.py",
        desc="USDA NASS prices received (farm) and paid (inputs)",
        stage=1,
        tables=["usda_prices_received", "usda_prices_paid"],
        requires_env=["USDA_NASS_API_KEY"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    PipelineSpec(
        name="noaa_seafood_landings",
        file="noaa_seafood_landings_pipeline.py",
        desc="NOAA FOSS commercial seafood landings - ex-vessel (dockside) prices",
        stage=1,
        tables=["noaa_seafood_landings"],
        backfill_args=["--backfill"],
        timeout=300,
    ),
    PipelineSpec(
        name="eia_energy",
        file="eia_energy_prices_pipeline.py",
        desc="EIA retail energy - gasoline/diesel, electricity, natural gas",
        stage=1,
        tables=["eia_gas_retail", "eia_gas_spot", "eia_electricity_price", "eia_natgas_price"],
        requires_env=["EIA_API_KEY"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    PipelineSpec(
        name="fred_consumer",
        file="fred_consumer_prices_pipeline.py",
        desc="FRED consumer price series - used cars, tires, housing, retail",
        stage=1,
        tables=["fred_consumer_prices", "fred_used_cars"],
        requires_env=["FRED_API_KEY"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    # ── Stage 1 - Additional CPI / Inflation Sources ───────────────────────
    PipelineSpec(
        name="fred_cpi",
        file="fred_cpi_pipeline.py",
        desc="FRED CPI series - broader consumer price index data",
        stage=1,
        tables=["fred_cpi"],
        requires_env=["FRED_API_KEY"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    PipelineSpec(
        name="apininja_inflation",
        file="apininja_inflation_pipeline.py",
        desc="API-Ninjas inflation rate data (global)",
        stage=1,
        tables=["apininja_inflation"],
        requires_env=["APININJA_API_KEY"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    # ── Stage 2 - Retail / e-commerce (free registration) ───────────────────
    PipelineSpec(
        name="openfoodfacts",
        file="openfoodfacts_pipeline.py",
        desc="Open Prices - real barcode/category-level price observations, receipts + price tags (keyless)",
        stage=2,
        tables=["openfoodfacts_prices"],
        backfill_args=["--backfill"],
        timeout=300,
    ),
    PipelineSpec(
        name="statcan_retail_prices",
        file="statcan_retail_prices_pipeline.py",
        desc="Statistics Canada retail prices - absolute CAD prices, milk to household goods (keyless)",
        stage=1,
        tables=["statcan_retail_prices"],
        backfill_args=["--backfill"],
        timeout=300,
    ),
    PipelineSpec(
        name="wfp_food_prices",
        file="wfp_food_prices_pipeline.py",
        desc="WFP global food prices - 98 countries, per-market retail/wholesale (keyless)",
        stage=1,
        tables=["wfp_food_prices"],
        backfill_args=["--backfill"],
        timeout=1800,
    ),
    PipelineSpec(
        name="kroger",
        file="kroger_pipeline.py",
        desc="Kroger grocery catalog prices, ZIP-localized (5 tracked regions)",
        stage=2,
        tables=["kroger_products"],
        requires_env=["KROGER_CLIENT_ID", "KROGER_CLIENT_SECRET"],
        backfill_args=["--backfill"],
        timeout=900,
    ),
    PipelineSpec(
        name="eurostat_hicp",
        file="eurostat_hicp_pipeline.py",
        desc="Eurostat Harmonised Index of Consumer Prices - EU/EFTA, indices only (keyless)",
        stage=1,
        tables=["eurostat_hicp"],
        backfill_args=["--backfill"],
        timeout=300,
    ),
    PipelineSpec(
        name="oecd_cpi",
        file="oecd_cpi_pipeline.py",
        desc="OECD consumer price indices, ~38 economies (keyless)",
        stage=1,
        tables=["oecd_cpi"],
        backfill_args=["--backfill"],
        timeout=300,
    ),
    PipelineSpec(
        name="fao_prices",
        file="fao_prices_pipeline.py",
        desc="FAO national food-CPI + meat/livestock producer prices (keyless, CC BY-NC-SA)",
        stage=1,
        tables=["fao_food_prices", "fao_meat_prices"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    PipelineSpec(
        name="worldbank_pinksheet",
        file="worldbank_pinksheet_pipeline.py",
        desc="World Bank Pink Sheet - ~70 global commodity benchmark prices (keyless)",
        stage=1,
        tables=["worldbank_pinksheet"],
        backfill_args=["--backfill"],
        timeout=300,
    ),
    PipelineSpec(
        name="imf_commodities",
        file="imf_commodities_pipeline.py",
        desc="IMF PCPS primary commodity benchmark prices (keyless)",
        stage=1,
        tables=["imf_commodities"],
        backfill_args=["--backfill"],
        timeout=300,
    ),
    PipelineSpec(
        name="ers_specialty_crops",
        file="ers_specialty_crops_pipeline.py",
        desc="USDA ERS specialty crops - fruit/nuts + veg prices & trade (keyless)",
        stage=1,
        tables=["ers_fruit_nut_prices", "ers_veg_prices",
                "ers_fruit_nut_trade", "ers_veg_trade"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    PipelineSpec(
        name="fews_net",
        file="fews_net_pipeline.py",
        desc="FEWS NET Data Warehouse - market prices in food-insecure countries (keyless)",
        stage=1,
        tables=["fews_net_food_prices"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    PipelineSpec(
        name="bestbuy",
        file="bestbuy_products_pipeline.py",
        desc="Best Buy electronics/appliance prices incl. sale/clearance",
        stage=2,
        tables=["bestbuy_products"],
        requires_env=["BESTBUY_API_KEY"],
        backfill_args=["--backfill"],
        timeout=600,
    ),
    PipelineSpec(
        name="cms_drug_pricing",
        file="cms_drug_pricing_pipeline.py",
        desc="CMS Medicare Part D spending by drug - program cost, not retail (keyless)",
        stage=1,
        tables=["cms_drug_pricing"],
        backfill_args=["--backfill"],
        timeout=300,
    ),
    # ── Stage 3 - Derived ────────────────────────────────────────────────────
    # Reserved for basket/aggregate builders as data accumulates.
    #
    # PLANNED (add once endpoints are confirmed - see docs/SOURCES.md):
    #   ebay (Browse API), walmart (Product API, no free tier - rejected),
    #   amazon (rejected), numbeo (paid - rejected), hospital_prices.
]


# ── Run result ─────────────────────────────────────────────────────────────────

@dataclass
class RunResult:
    name:     str
    status:   str    # PASS | FAIL | SKIP | DRY RUN
    duration: float  # seconds
    note:     str    # skip reason or error context
    val_warnings: int = 0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _check_env(spec: PipelineSpec) -> str | None:
    """Return a skip reason if any required env var is missing, else None."""
    missing = [v for v in spec.requires_env if not os.environ.get(v)]
    if missing:
        return f"missing env: {', '.join(missing)}"
    return None


def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s:02d}s"


def run_pipeline(
    spec: PipelineSpec,
    backfill: bool,
    dry_run: bool,
    validate: bool,
) -> RunResult:
    skip_reason = _check_env(spec)
    if skip_reason:
        print(f"  SKIP -- {skip_reason}")
        return RunResult(spec.name, "SKIP", 0.0, skip_reason)

    script = os.path.join(REPO_ROOT, spec.file)
    if not os.path.exists(script):
        reason = f"{spec.file} not found"
        print(f"  SKIP -- {reason}")
        return RunResult(spec.name, "SKIP", 0.0, reason)

    cmd = [sys.executable, script]
    cmd += spec.backfill_args if backfill else spec.incremental_args

    if dry_run:
        cmd_str = " ".join(os.path.basename(c) if i < 2 else c for i, c in enumerate(cmd))
        print(f"  DRY RUN: {cmd_str}")
        return RunResult(spec.name, "DRY RUN", 0.0, cmd_str)

    t0 = time.time()
    try:
        result = subprocess.run(
            cmd, timeout=spec.timeout, capture_output=True,
            text=True, encoding="utf-8",
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
        duration = time.time() - t0
        output = (result.stdout or "") + (result.stderr or "")
        if output:
            print(output, end="" if output.endswith("\n") else "\n")
        if result.returncode != 0:
            log.error("%s failed: exit %d", spec.name, result.returncode)
            fail_path = log_pipeline_failure(
                spec.name, output or "(no output captured)"
            )
            return RunResult(spec.name, "FAIL", duration,
                              f"exit {result.returncode} -- log: {fail_path}")
    except subprocess.TimeoutExpired as exc:
        duration = time.time() - t0
        output = (exc.stdout or "") + (exc.stderr or "")
        if output:
            print(output, end="" if output.endswith("\n") else "\n")
        log.error("%s timed out after %ds", spec.name, spec.timeout)
        fail_path = log_pipeline_failure(
            spec.name, output or "(no output captured before timeout)")
        return RunResult(spec.name, "FAIL", duration,
                          f"timed out after {spec.timeout}s -- log: {fail_path}")
    except Exception as exc:
        duration = time.time() - t0
        log.exception("%s raised an unexpected error before completing", spec.name)
        fail_path = log_pipeline_failure(spec.name, traceback.format_exc())
        return RunResult(spec.name, "FAIL", duration, f"{exc} -- log: {fail_path}")

    # Post-run validation
    val_warnings = 0
    if validate and spec.tables:
        for table in spec.tables:
            vr = validate_table(table)
            if not vr.passed:
                print(f"\n  [VALIDATE] {table}: {len(vr.errors)} error(s)")
                for c in vr.errors:
                    print(f"    {c}")
            elif vr.warnings:
                val_warnings += len(vr.warnings)

    return RunResult(spec.name, "PASS", duration, "", val_warnings)


# ── Summary ────────────────────────────────────────────────────────────────────

def _print_summary(results: list[RunResult], backfill: bool, start_time: float) -> None:
    mode      = "BACKFILL" if backfill else "INCREMENTAL"
    wall_time = _fmt_duration(time.time() - start_time)
    now       = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    icons = {"PASS": "+", "FAIL": "!", "SKIP": "-", "DRY RUN": "?"}

    print(f"\n{'=' * 62}")
    print(f"  Run Summary -- {mode} -- {now}  ({wall_time} total)")
    print(f"{'=' * 62}")

    for r in results:
        icon = icons.get(r.status, "?")
        dur  = _fmt_duration(r.duration) if r.duration else "-"
        warn = f"  [{r.val_warnings} val warn]" if r.val_warnings else ""
        note = f"  {r.note}" if r.note and r.status not in ("PASS",) else ""
        print(f"  {icon} {r.status:8s}  {r.name:28s}  {dur:>6s}{warn}{note}")

    pass_n  = sum(1 for r in results if r.status == "PASS")
    fail_n  = sum(1 for r in results if r.status == "FAIL")
    skip_n  = sum(1 for r in results if r.status == "SKIP")
    total_w = sum(r.val_warnings for r in results)

    print(f"\n  {pass_n} PASS  |  {fail_n} FAIL  |  {skip_n} SKIP", end="")
    if total_w:
        print(f"  |  {total_w} validation warning(s)", end="")
    print()

    log_fn = log.warning if fail_n else log.info
    log_fn("%s run complete (%s): %d PASS, %d FAIL, %d SKIP, %d validation warning(s)",
           mode, wall_time, pass_n, fail_n, skip_n, total_w)
    for r in results:
        if r.status == "FAIL":
            log.warning("  FAILED: %s -- %s", r.name, r.note)


# ── Curated compaction ───────────────────────────────────────────────────────

def compact_curated(passed_specs: list[PipelineSpec]) -> None:
    """
    Rebuild deduplicated curated snapshots for the tables that just ran.

    Pipelines append a fresh dated Parquet file each run; left alone, the query
    layer would glob those alongside every prior file and double-count rows.
    Compacting here keeps storage/curated/ (which query.py reads by default)
    in sync with the raw layer after every run. Only tables whose pipeline
    PASSed are touched - no point re-reading unchanged tables.
    """
    tables = sorted({t for spec in passed_specs for t in spec.tables})
    if not tables:
        return

    print(f"\n-- Curated Compaction ({len(tables)} table(s)) --")
    try:
        df = curated.compact_all(tables=tables, verbose=True)
    except Exception as exc:  # noqa: BLE001 - never let compaction sink a run
        print(f"  ! compaction error: {exc}")
        return
    if df is not None and not df.empty:
        removed = int(df["removed"].sum())
        print(f"  Compacted {len(df)} table(s); removed {removed:,} duplicate row(s).")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run all consumer-goods price pipelines in dependency order.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--backfill", action="store_true",
                        help="Pass --backfill to every pipeline that supports it.")
    parser.add_argument("--stage", type=int, choices=[1, 2, 3],
                        help="Run only pipelines in the given stage (1=gov, 2=retail APIs, 3=derived).")
    parser.add_argument("--only", help="Comma-separated pipeline names to run (e.g. bls_avg_prices,usda_ams).")
    parser.add_argument("--skip", help="Comma-separated pipeline names to skip.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would run without executing.")
    parser.add_argument("--no-validate", action="store_true", help="Skip post-run validation checks.")
    parser.add_argument("--no-compact", action="store_true", help="Skip post-run curated compaction.")
    args = parser.parse_args()

    # Build filtered pipeline list
    pipelines = list(PIPELINES)
    if args.stage:
        pipelines = [p for p in pipelines if p.stage == args.stage]
    if args.only:
        only_set  = {n.strip() for n in args.only.split(",")}
        pipelines = [p for p in pipelines if p.name in only_set]
        unknown   = only_set - {p.name for p in PIPELINES}
        if unknown:
            print(f"Warning: unknown pipeline names in --only: {sorted(unknown)}")
    if args.skip:
        skip_set  = {n.strip() for n in args.skip.split(",")}
        pipelines = [p for p in pipelines if p.name not in skip_set]
        unknown   = skip_set - {p.name for p in PIPELINES}
        if unknown:
            print(f"Warning: unknown pipeline names in --skip: {sorted(unknown)}")

    if not pipelines:
        print("No pipelines selected. Check --stage / --only / --skip arguments.")
        return 1

    mode = "BACKFILL" if args.backfill else "INCREMENTAL"
    validate = not args.no_validate
    compact = not args.no_compact
    start_time = time.time()

    print(f"\n{'=' * 62}")
    print("  Consumer-Goods Price Pipeline Runner")
    print(f"  Mode: {mode}  |  Pipelines: {len(pipelines)}  |  "
          f"Validate: {validate}  |  Compact: {compact}")
    print(f"{'=' * 62}")

    # Stage-grouped run
    current_stage = 0
    results: list[RunResult] = []

    for spec in pipelines:
        if spec.stage != current_stage:
            current_stage = spec.stage
            labels = {1: "Free / Public Government Sources", 2: "Retail / E-commerce APIs", 3: "Derived"}
            print(f"\n-- Stage {current_stage}: {labels.get(current_stage, '')} --")

        print(f"\n>>  {spec.name}  --  {spec.desc}")
        result = run_pipeline(spec, args.backfill, args.dry_run, validate)
        results.append(result)

    # Rebuild curated snapshots for the tables that ran, so the query layer
    # (which prefers curated files) stays in sync with the new raw files.
    if compact and not args.dry_run:
        spec_by_name = {p.name: p for p in PIPELINES}
        passed_specs = [spec_by_name[r.name] for r in results if r.status == "PASS"]
        compact_curated(passed_specs)

    _print_summary(results, args.backfill, start_time)

    return 0 if all(r.status in ("PASS", "SKIP", "DRY RUN") for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
