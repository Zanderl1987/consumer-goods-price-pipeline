"""Unit tests for the curated dedup layer."""

import os
import sys

import pandas as pd
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import curated


class TestDedup:
    def test_full_row_dedup_on_unknown_key(self):
        df = pd.DataFrame({
            "series_id": ["X", "X"],
            "date": ["2026-01-01", "2026-01-01"],
            "value": [1.0, 1.0],
        })
        out = curated.dedup("some_table", df)
        assert len(out) == 1

    def test_natural_key_keeps_latest(self):
        df = pd.DataFrame({
            "series_id": ["X", "X"],
            "date": ["2026-01-01", "2026-01-01"],
            "value": [1.0, 2.0],
            "fetched_at": ["2026-01-02T00:00:00", "2026-01-03T00:00:00"],
        })
        out = curated.dedup("bls_cpi", df)
        assert len(out) == 1
        assert out["value"].iloc[0] == 2.0  # newest fetch wins

    def test_partial_key_falls_back_to_full_row(self):
        # bls_avg_prices key is [series_id, date]; missing series_id -> full-row
        df = pd.DataFrame({
            "item": ["Milk", "Milk", "Eggs"],
            "date": ["2026-01-01", "2026-01-01", "2026-01-01"],
            "price": [3.5, 3.5, 4.2],
        })
        out = curated.dedup("bls_avg_prices", df)
        assert len(out) == 2  # Milk duplicate collapsed, Eggs kept

    def test_empty_df_passthrough(self):
        out = curated.dedup("bls_cpi", pd.DataFrame())
        assert out.empty

    def test_keys_covers_major_tables(self):
        for table in ["bls_cpi", "bls_avg_prices", "eia_gas_retail", "fred_consumer_prices"]:
            assert table in curated.KEYS
