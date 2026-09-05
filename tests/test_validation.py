"""Unit tests for the validation layer."""

import os
import sys

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from validate import (
    CheckResult,
    Severity,
    validate_df,
    validate_table,
)


class TestCheckResult:
    def test_passed_property(self):
        ok = CheckResult("x", Severity.OK, "fine")
        warn = CheckResult("x", Severity.WARNING, "watch")
        err = CheckResult("x", Severity.ERROR, "broken")
        assert ok.passed and warn.passed and not err.passed


class TestValidateDf:
    def test_empty_df_fails_not_empty(self):
        df = pd.DataFrame()
        result = validate_df("bls_cpi", df)
        assert not result.passed
        assert any(c.name == "not_empty" for c in result.errors)

    def test_missing_required_column_fails(self):
        df = pd.DataFrame({"series_id": ["X"], "date": ["2026-01-01"]})
        result = validate_df("bls_cpi", df)
        assert not result.passed
        assert any(c.name == "required_cols" for c in result.errors)

    def test_good_df_passes(self):
        df = pd.DataFrame({
            "series_id": ["X"],
            "date": ["2026-01-01"],
            "value": [100.0],
            "fetched_at": ["2026-01-02T00:00:00"],
        })
        result = validate_df("bls_cpi", df)
        assert result.passed

    def test_value_range_warns(self):
        df = pd.DataFrame({
            "duoarea": ["NUS"], "product": ["EPMR"],
            "date": ["2026-01-01"], "price_usd_gallon": [9999.0],
            "fetched_at": ["2026-01-02T00:00:00"],
        })
        result = validate_df("eia_gas_retail", df)
        assert any(c.name.startswith("range:") and c.severity == Severity.WARNING
                   for c in result.checks)

    def test_value_range_errors_when_flagged(self):
        df = pd.DataFrame({
            "period_date": ["2026-01-01"], "country": ["SS"], "market": ["Juba"],
            "product": ["Millet"], "price_type": ["Retail"], "value": [999999999.0],
            "fetched_at": ["2026-01-02T00:00:00"],
        })
        result = validate_df("fews_net_food_prices", df)
        assert not result.passed
        assert any(c.name == "range:value" and c.severity == Severity.ERROR
                   for c in result.errors)

    def test_unknown_table_warns_not_errors(self):
        df = pd.DataFrame({"a": [1]})
        result = validate_df("no_such_table", df)
        assert result.passed  # warning-only


class TestValidateTable:
    def test_unknown_catalog_table_errors(self):
        result = validate_table("no_such_table")
        assert not result.passed
        assert any(c.name == "catalog" for c in result.errors)
