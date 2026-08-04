#!/usr/bin/env python3
"""
Data Validation Layer — checks Parquet outputs for schema correctness,
null rates, date sanity, row count plausibility, and value ranges.

Usage
-----
    # Full system health check (all tables with data on disk):
    python validate.py

    # Single table:
    python validate.py --table bls_avg_prices

    # From inside a pipeline, right before writing:
    from validate import validate_df
    result = validate_df("bls_avg_prices", df)
    if not result.passed:
        print(result)

    # Programmatic full check:
    from validate import validate_all
    summary = validate_all()
    print(summary[summary["status"] == "FAIL"])
"""

import argparse
import datetime
import glob as _glob_mod
import os
import sys
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO_ROOT)
import query as q


# ── Severity ──────────────────────────────────────────────────────────────────

class Severity(Enum):
    OK      = "OK"
    WARNING = "WARN"
    ERROR   = "ERROR"


# ── Per-check result ──────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    name:     str
    severity: Severity
    message:  str

    @property
    def passed(self) -> bool:
        return self.severity != Severity.ERROR

    def __str__(self) -> str:
        icon = {"OK": "+", "WARN": "!", "ERROR": "X"}[self.severity.value]
        return f"  [{self.severity.value:5s}] {icon} {self.name}: {self.message}"


# ── Aggregate result for one table ────────────────────────────────────────────

@dataclass
class ValidationResult:
    table:  str
    checks: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def errors(self) -> list:
        return [c for c in self.checks if c.severity == Severity.ERROR]

    @property
    def warnings(self) -> list:
        return [c for c in self.checks if c.severity == Severity.WARNING]

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        e, w = len(self.errors), len(self.warnings)
        lines = [f"\n{'='*60}", f"  {status}  {self.table}  -- {e} error(s), {w} warning(s)", "=" * 60]
        lines += [str(c) for c in self.checks]
        return "\n".join(lines)


# ── Schema registry ───────────────────────────────────────────────────────────
# required      — columns that MUST be present                  → ERROR if missing
# critical_nn   — subset that MUST NOT be >50% null            → ERROR if mostly null
# date_col      — column for future-date check (None = skip)
# value_ranges  — {col: (lo, hi)}                              → WARN if violated

SCHEMAS: dict[str, dict] = {
    "bls_cpi": {
        "required":    ["series_id", "date", "value"],
        "critical_nn": ["series_id", "date", "value"],
        "date_col":    "date",
    },
    "bls_avg_prices": {
        "required":    ["series_id", "item", "date", "price"],
        "critical_nn": ["series_id", "item", "date", "price"],
        "date_col":    "date",
        "value_ranges": {"price": (0, 100000)},
    },
    "bls_ppi": {
        "required":    ["series_id", "date", "value"],
        "critical_nn": ["series_id", "date", "value"],
        "date_col":    "date",
    },
    "usda_ams_wholesale": {
        "required":    ["commodity", "location", "date", "price"],
        "critical_nn": ["commodity", "date", "price"],
        "date_col":    "date",
        "value_ranges": {"price": (0, 100000)},
    },
    "usda_ams_retail": {
        "required":    ["commodity", "unit", "date", "price"],
        "critical_nn": ["commodity", "date", "price"],
        "date_col":    "date",
        "value_ranges": {"price": (0, 100000)},
    },
    "usda_prices_received": {
        "required":    ["commodity", "date", "value"],
        "critical_nn": ["commodity", "date", "value"],
        "date_col":    "date",
    },
    "usda_prices_paid": {
        "required":    ["commodity", "date", "value"],
        "critical_nn": ["commodity", "date", "value"],
        "date_col":    "date",
    },
    "eia_gas_retail": {
        "required":    ["duoarea", "product", "date", "price_usd_gallon"],
        "critical_nn": ["duoarea", "product", "date", "price_usd_gallon"],
        "date_col":    "date",
        "value_ranges": {"price_usd_gallon": (0, 20)},
    },
    "eia_gas_spot": {
        "required":    ["series", "date", "price_usd_gallon"],
        "critical_nn": ["series", "date", "price_usd_gallon"],
        "date_col":    "date",
        "value_ranges": {"price_usd_gallon": (0, 20)},
    },
    "eia_electricity_price": {
        "required":    ["series_id", "date", "cents_per_kwh"],
        "critical_nn": ["series_id", "date", "cents_per_kwh"],
        "date_col":    "date",
        "value_ranges": {"cents_per_kwh": (0, 200)},
    },
    "eia_natgas_price": {
        "required":    ["series_id", "date", "price"],
        "critical_nn": ["series_id", "date", "price"],
        "date_col":    "date",
        "value_ranges": {"price": (0, 1000)},
    },
    "kroger_products": {
        "required":    ["upc", "product_name", "price", "fetched_at"],
        "critical_nn": ["upc", "product_name", "price"],
        "value_ranges": {"price": (0, 100000)},
    },
    "bestbuy_products": {
        "required":    ["sku", "product_name", "price", "fetched_at"],
        "critical_nn": ["sku", "product_name", "price"],
        "value_ranges": {"price": (0, 100000)},
    },
    "walmart_products": {
        "required":    ["product_id", "product_name", "price", "fetched_at"],
        "critical_nn": ["product_id", "price"],
        "value_ranges": {"price": (0, 100000)},
    },
    "ebay_listings": {
        "required":    ["item_id", "title", "price", "fetched_at"],
        "critical_nn": ["item_id", "price"],
        "value_ranges": {"price": (0, 10000000)},
    },
    "openfoodfacts_prices": {
        "required":    ["id", "price", "currency", "date", "fetched_at"],
        "critical_nn": ["id", "price", "currency", "date"],
        "date_col":    "date",
        "value_ranges": {"price": (0, 100000)},
    },
    "cms_drug_pricing": {
        "required":    ["drug_name", "brand_name", "spending_year", "avg_spend_per_dosage_unit"],
        "critical_nn": ["drug_name", "spending_year"],
        "value_ranges": {"avg_spend_per_dosage_unit": (0, 100000)},
    },
    "hospital_prices": {
        "required":    ["hospital_name", "item_name", "price"],
        "critical_nn": ["hospital_name", "item_name"],
        "value_ranges": {"price": (0, 100000000)},
    },
    "eurostat_hicp": {
        "required":    ["series_id", "date", "value"],
        "critical_nn": ["series_id", "date", "value"],
        "date_col":    "date",
    },
    "oecd_cpi": {
        "required":    ["series_id", "date", "value"],
        "critical_nn": ["series_id", "date", "value"],
        "date_col":    "date",
    },
    "statcan_retail_prices": {
        "required":    ["item", "date", "price"],
        "critical_nn": ["item", "date", "price"],
        "date_col":    "date",
        "value_ranges": {"price": (0, 100000)},
    },
    "wfp_food_prices": {
        "required":    ["countryiso3", "commodity", "date", "price", "usdprice"],
        "critical_nn": ["countryiso3", "commodity", "date", "price"],
        "date_col":    "date",
        "value_ranges": {"usdprice": (0, 100000)},
    },
    "fao_food_prices": {
        "required":    ["area", "item", "date", "value"],
        "critical_nn": ["area", "item", "date", "value"],
        "date_col":    "date",
    },
    "fao_meat_prices": {
        "required":    ["area", "item", "date", "value"],
        "critical_nn": ["area", "item", "date", "value"],
        "date_col":    "date",
        "value_ranges": {"value": (0, 100000)},
    },
    "worldbank_pinksheet": {
        "required":    ["series_id", "date", "value"],
        "critical_nn": ["series_id", "date", "value"],
        "date_col":    "date",
    },
    "imf_commodities": {
        "required":    ["series_id", "date", "value"],
        "critical_nn": ["series_id", "date", "value"],
        "date_col":    "date",
    },
    "fred_consumer_prices": {
        "required":    ["series_id", "date", "value"],
        "critical_nn": ["series_id", "date", "value"],
        "date_col":    "date",
    },
    "fred_used_cars": {
        "required":    ["series_id", "date", "value"],
        "critical_nn": ["series_id", "date", "value"],
        "date_col":    "date",
    },
}


# ── Checks ─────────────────────────────────────────────────────────────────────

def _check_not_empty(df: pd.DataFrame) -> CheckResult:
    if len(df) == 0:
        return CheckResult("not_empty", Severity.ERROR, "DataFrame has 0 rows")
    return CheckResult("not_empty", Severity.OK, f"{len(df):,} rows")


def _check_required_cols(df: pd.DataFrame, schema: dict) -> CheckResult:
    required = schema.get("required", [])
    missing = [c for c in required if c not in df.columns]
    if missing:
        return CheckResult("required_cols", Severity.ERROR, f"Missing columns: {missing}")
    return CheckResult("required_cols", Severity.OK, f"All {len(required)} required columns present")


def _check_null_rates(df: pd.DataFrame, schema: dict) -> list:
    results = []
    for col in schema.get("critical_nn", []):
        if col not in df.columns:
            continue
        null_pct = df[col].isna().mean()
        if null_pct > 0.5:
            results.append(CheckResult(
                f"nulls:{col}", Severity.ERROR,
                f"{col} is {null_pct:.0%} null (critical column)"
            ))
        elif null_pct > 0.05:
            results.append(CheckResult(
                f"nulls:{col}", Severity.WARNING,
                f"{col} has {null_pct:.1%} nulls"
            ))
        else:
            results.append(CheckResult(f"nulls:{col}", Severity.OK, f"{col}: {null_pct:.1%} null"))
    return results


def _check_future_dates(df: pd.DataFrame, schema: dict) -> CheckResult:
    date_col = schema.get("date_col")
    if not date_col or date_col not in df.columns:
        return CheckResult("future_dates", Severity.OK, "no date column to check")
    today = datetime.date.today()
    try:
        dates = pd.to_datetime(df[date_col], errors="coerce")
        future = int((dates.dt.date > today).sum())
        if future > 0:
            pct = future / len(df)
            return CheckResult(
                "future_dates", Severity.WARNING,
                f"{future} rows ({pct:.1%}) have {date_col} > today"
            )
    except Exception:
        pass
    return CheckResult("future_dates", Severity.OK, f"{date_col}: no future dates")


def _check_value_ranges(df: pd.DataFrame, schema: dict) -> list:
    results = []
    for col, (lo, hi) in schema.get("value_ranges", {}).items():
        if col not in df.columns:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        out_of_range = int(((numeric < lo) | (numeric > hi)).sum())
        if out_of_range > 0:
            results.append(CheckResult(
                f"range:{col}", Severity.WARNING,
                f"{out_of_range} values outside [{lo}, {hi}]"
            ))
        else:
            results.append(CheckResult(f"range:{col}", Severity.OK, f"all values in [{lo}, {hi}]"))
    return results


def _latest_file(glob_path: str) -> list[str]:
    """
    List matching files sorted OLDEST to NEWEST by modification time.

    Filenames aren't a reliable date proxy — sort by mtime so any future
    "pick the latest file" caller doesn't inherit that trap.
    """
    files = _glob_mod.glob(glob_path.replace("/", os.sep), recursive=True)
    return sorted(files, key=os.path.getmtime)


def _check_row_count(table: str, df: pd.DataFrame) -> CheckResult:
    """Warn if the new DataFrame is less than 50% the size of the most recent snapshot."""
    glob_path = q.CATALOG.get(table, "")
    existing = _latest_file(glob_path)
    if not existing:
        return CheckResult("row_count", Severity.OK, f"{len(df):,} rows (no prior snapshot to compare)")
    try:
        prev = pd.read_parquet(existing[-1])
        prev_n = len(prev)
        new_n  = len(df)
        if prev_n == 0:
            return CheckResult("row_count", Severity.OK, f"{new_n:,} rows (prior snapshot was empty)")
        ratio = new_n / prev_n
        if ratio < 0.5:
            return CheckResult(
                "row_count", Severity.WARNING,
                f"{new_n:,} rows — {ratio:.0%} of prior snapshot ({prev_n:,}) — possible data loss"
            )
        return CheckResult(
            "row_count", Severity.OK,
            f"{new_n:,} rows ({ratio:.0%} vs prior {prev_n:,})"
        )
    except Exception as exc:
        return CheckResult("row_count", Severity.WARNING, f"{len(df):,} rows (prior load failed: {exc})")


def _check_fetched_at(df: pd.DataFrame, max_age_hours: float = 2.0) -> CheckResult:
    if "fetched_at" not in df.columns:
        return CheckResult("fetched_at", Severity.OK, "no fetched_at column")
    try:
        ts = pd.to_datetime(df["fetched_at"], utc=True, errors="coerce").max()
        if pd.isna(ts):
            return CheckResult("fetched_at", Severity.WARNING, "fetched_at is all NaT")
        now = datetime.datetime.now(datetime.timezone.utc)
        age_h = (now - ts).total_seconds() / 3600
        if age_h > max_age_hours:
            return CheckResult(
                "fetched_at", Severity.WARNING,
                f"newest fetched_at is {age_h:.1f}h ago (threshold {max_age_hours}h)"
            )
        return CheckResult("fetched_at", Severity.OK, f"newest fetched_at is {age_h:.1f}h ago")
    except Exception as exc:
        return CheckResult("fetched_at", Severity.WARNING, f"fetched_at parse error: {exc}")


# ── Public API ─────────────────────────────────────────────────────────────────

def validate_df(
    table: str,
    df: pd.DataFrame,
    check_freshness: bool = True,
    max_age_hours: float = 2.0,
) -> ValidationResult:
    """
    Validate a freshly-fetched DataFrame before writing to Parquet.

    Call inside a pipeline right before df.to_parquet(...):

        result = validate_df("bls_avg_prices", df)
        if not result.passed:
            print(result)
        df.to_parquet(path, compression="snappy")

    Parameters
    ----------
    table           : CATALOG table name
    df              : DataFrame to validate
    check_freshness : warn when fetched_at is older than max_age_hours
    max_age_hours   : freshness threshold in hours (default 2)
    """
    if table not in SCHEMAS:
        return ValidationResult(table, [
            CheckResult("schema", Severity.WARNING, f"No schema defined for '{table}' — skipping validation")
        ])
    schema = SCHEMAS[table]
    checks = []
    checks.append(_check_not_empty(df))
    checks.append(_check_required_cols(df, schema))
    checks.extend(_check_null_rates(df, schema))
    checks.append(_check_future_dates(df, schema))
    checks.extend(_check_value_ranges(df, schema))
    checks.append(_check_row_count(table, df))
    if check_freshness:
        checks.append(_check_fetched_at(df, max_age_hours))
    return ValidationResult(table, checks)


def validate_table(table: str) -> ValidationResult:
    """
    Load the latest snapshot of a table from disk and validate it.

    Skips the freshness check (historical files are expected to be old).
    Returns a warning result if no files exist yet.
    """
    if table not in q.CATALOG:
        return ValidationResult(table, [
            CheckResult("catalog", Severity.ERROR, f"'{table}' not in CATALOG")
        ])
    glob_path = q.CATALOG[table]
    files = _latest_file(glob_path)
    if not files:
        return ValidationResult(table, [
            CheckResult("files", Severity.WARNING, "No parquet files on disk yet")
        ])
    try:
        df = pd.read_parquet(files[-1])
    except Exception as exc:
        return ValidationResult(table, [
            CheckResult("read", Severity.ERROR, f"Failed to read {os.path.basename(files[-1])}: {exc}")
        ])
    return validate_df(table, df, check_freshness=False)


def validate_all() -> pd.DataFrame:
    """
    Run validate_table() on every CATALOG entry and return a summary DataFrame
    with columns: table | status | errors | warnings.
    """
    rows = []
    for table in q.CATALOG:
        result = validate_table(table)
        rows.append({
            "table": table,
            "status": "PASS" if result.passed else "FAIL",
            "errors": len(result.errors),
            "warnings": len(result.warnings),
        })
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate Parquet outputs against expected schemas.")
    ap.add_argument("--table", help="validate a single table")
    ap.add_argument("--all", action="store_true", help="show all tables, including no-data tables")
    args = ap.parse_args()

    if args.table:
        print(validate_table(args.table))
        return 0 if validate_table(args.table).passed else 1

    summary = validate_all()
    no_data = [t for t, row in summary.set_index("table").iterrows()
               if "no files" in str(row.to_dict())]
    summary_display = summary.copy()
    if not args.all:
        summary_display = summary[~summary["table"].isin(
            [t for t in q.CATALOG if not _glob_mod.glob(q.CATALOG[t].replace("/", os.sep), recursive=True)]
        )]

    if summary_display.empty:
        print("No tables with data on disk yet. Run a pipeline first.")
        return 0

    print(summary_display.to_string(index=False))
    fails = summary[summary["status"] == "FAIL"]
    if not fails.empty:
        print(f"\n{len(fails)} table(s) FAILED validation:")
        for t in fails["table"]:
            print(validate_table(t))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
