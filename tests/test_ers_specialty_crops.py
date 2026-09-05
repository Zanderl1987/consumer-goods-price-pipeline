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

# Live tidy schema verified 2026-09-05 (seriesID unique per seriesType; 0 codes span types)
TIDY_PRICE_CSV = """\
seriesType,seriesID,seriesName,year,monthNumber,unit,value
Consumer Price Index,CUUR0000SAF1131,Bananas,2026,7,Index,332.0
Consumer Price Index,CUUR0000SAF1131,Bananas,2026,6,Index,329.9
Producer Price Index,PCU3114233114235,Dried fruits and vegetables,2026,7,Index,105.4
Average retail price,APU0000711211,Bananas,2026,7,Dollars per pound,0.710
"""


def _mock_get_csv(url: str) -> pd.DataFrame:
    """Simulate ERS CSV download by parsing the fixture."""
    if "tidy" in url:
        return pd.read_csv(StringIO(TIDY_PRICE_CSV))
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


# ── Tidy live schema (2026-09-05) ─────────────────────────────────────────────

class TestParsePriceIndexTidy:
    def test_long_format_with_series_code(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_price_index("https://example.com/tidy.csv", "Fruit & Tree Nuts")
        assert not df.empty
        assert "series_id" in df.columns
        assert "commodity" in df.columns
        assert "series_type" in df.columns
        assert "date" in df.columns
        assert "value" in df.columns
        assert "fetched_at" in df.columns

    def test_series_id_is_stable_code_not_name(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_price_index("https://example.com/tidy.csv", "Fruit & Tree Nuts")
        assert df["series_id"].iloc[0] == "CUUR0000SAF1131"
        assert df["series_id"].nunique() == 3  # CPI(Jun+Jul), PPI, retail

    def test_series_type_kept(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_price_index("https://example.com/tidy.csv", "Fruit & Tree Nuts")
        assert (df["series_type"] == "Consumer Price Index").sum() == 2
        assert (df["series_type"] == "Producer Price Index").sum() == 1
        assert (df["series_type"] == "Average retail price").sum() == 1

    def test_month_start_date_and_numeric_value(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_price_index("https://example.com/tidy.csv", "Fruit & Tree Nuts")
        assert pd.api.types.is_datetime64_any_dtype(df["date"])
        assert df["date"].dt.is_month_start.all()
        assert df["value"].dtype.kind == "f"
        assert df.loc[df["series_id"] == "APU0000711211", "value"].max() == 0.710

    def test_key_unique_per_series_month(self, monkeypatch):
        monkeypatch.setattr(ers, "_get_csv", _mock_get_csv)
        df = ers._parse_price_index("https://example.com/tidy.csv", "Fruit & Tree Nuts")
        assert df.duplicated(subset=["series_id", "date"]).sum() == 0


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
