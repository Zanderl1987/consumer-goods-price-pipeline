"""Tests for ers_specialty_crops_pipeline — ERS fruit/nuts + veg price index + trade."""

import os
import sys
from io import StringIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import ers_specialty_crops_pipeline as ers

# ── Synthetic fixtures ────────────────────────────────────────────────────────

PRICE_CSV = """\
Category,Jan 2024,Feb 2024,Mar 2024
Fruit & Tree Nuts - CPI,100.0,101.2,102.5
Fruit & Tree Nuts - PPI,200.0,201.0,202.0
Fresh Fruit - Retail Price,1.99,2.05,2.10
"""

TRADE_CSV = """\
Commodity,Partner Country,Trade Flow,Value (1000 USD),Volume (1000 lbs),Date
Apples,Canada,Imports,5000,3000,Jan 2024
Apples,Mexico,Exports,8000,6000,Feb 2024
"""


def _mock_get_csv(url: str) -> pd.DataFrame:
    """Simulate ERS CSV download by parsing the fixture."""
    if "price" in url:
        return pd.read_csv(StringIO(PRICE_CSV))
    elif "trade" in url:
        return pd.read_csv(StringIO(TRADE_CSV))
    return pd.DataFrame()


# ── Price index parsing ──────────────────────────────────────────────────────

class TestParsePriceIndex:
    def test_melts_to_long_format(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_price_index("https://example.com/price.csv", "Test")
        assert not df.empty
        assert "series_id" in df.columns
        assert "date" in df.columns
        assert "value" in df.columns
        # 3 categories x 3 months = 9 rows
        assert len(df) == 9

    def test_series_id_snake_cased(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_price_index("https://example.com/price.csv", "Test")
        ids = df["series_id"].unique()
        assert any("fruit" in s for s in ids)
        assert all("_" not in s[0] for s in ids)  # no leading underscore

    def test_dates_parsed(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_price_index("https://example.com/price.csv", "Test")
        assert df["date"].dt.year.min() == 2024

    def test_empty_on_no_data(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", lambda url: pd.DataFrame())
        df = ers._parse_price_index("https://example.com/price.csv", "Test")
        assert df.empty

    def test_fetched_at_added(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_price_index("https://example.com/price.csv", "Test")
        assert "fetched_at" in df.columns


# ── Trade parsing ─────────────────────────────────────────────────────────────

class TestParseTrade:
    def test_columns_normalized(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_trade("https://example.com/trade.csv", "Test")
        assert not df.empty
        # Columns should be lowercased/snake_cased
        assert all(c == c.lower() for c in df.columns)

    def test_source_and_fetched_at(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_trade("https://example.com/trade.csv", "Test")
        assert (df["source"] == "Test").all()
        assert "fetched_at" in df.columns

    def test_empty_on_no_data(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", lambda url: pd.DataFrame())
        df = ers._parse_trade("https://example.com/trade.csv", "Test")
        assert df.empty
