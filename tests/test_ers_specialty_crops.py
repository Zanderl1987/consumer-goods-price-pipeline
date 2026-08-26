import io

import pandas as pd
import pytest
import requests

import ers_specialty_crops_pipeline as ers


def _price_csv() -> str:
    return (
        '"seriesType","seriesID","seriesName","year","monthNumber","unit","value"\n'
        '"Average retail price","APU0000711211","Bananas",2026,7,"Dollars per pound",0.65\n'
        '"Consumer Price Index","CUUR0000SAF111","Citrus fruits",2026,7,"Base 1982-84=100",251.2\n'
        '"Producer Price Index","WPU01130201","Grapefruit",2025,12,"Base 1982=100",180.4\n'
    )


def _trade_csv() -> str:
    return (
        '"Trade","GeographicDesc","Year","MonthNumber","MarketYear","Group","Subgroup",'
        '"MarketSegment","CommodityName","CommodityDetail","UnitType","UnitDesc","Amount"\n'
        '"Import","Afghanistan",2026,6,"2025/26","Fruit and tree nuts","Noncitrus","Dried",'
        '"Apricots","Unspecified","Value","Thousand dollars",25.693\n'
    )


class TestParsePrices:
    def test_parse_prices_tidy_long(self):
        raw = pd.read_csv(io.StringIO(_price_csv()))
        out = ers.parse_prices(raw)
        assert len(out) == 3
        assert set(out.columns) == {"date", "commodity", "series_id", "series_type", "unit", "value"}
        assert set(out["series_type"]) == {
            "Average retail price", "Consumer Price Index", "Producer Price Index",
        }
        row = out[out["commodity"] == "Bananas"].iloc[0]
        assert row["date"] == pd.Timestamp("2026-07-01")
        assert row["value"] == pytest.approx(0.65)
        assert row["unit"] == "Dollars per pound"

    def test_parse_prices_drops_bad_rows(self):
        raw = pd.read_csv(io.StringIO(_price_csv() + '"Average retail price","","Bananas",2026,13,"x",\n'))
        out = ers.parse_prices(raw)
        assert len(out) == 3

    def test_parse_prices_handles_empty(self):
        assert ers.parse_prices(pd.DataFrame()).empty


class TestParseTrade:
    def test_parse_trade_long(self):
        raw = pd.read_csv(io.StringIO(_trade_csv()))
        out = ers.parse_trade(raw)
        assert len(out) == 1
        row = out.iloc[0]
        assert row["date"] == pd.Timestamp("2026-06-01")
        assert row["trade_flow"] == "Import"
        assert row["partner_country"] == "Afghanistan"
        assert row["amount"] == pytest.approx(25.693)
        assert row["unit_desc"] == "Thousand dollars"

    def test_parse_trade_handles_empty(self):
        assert ers.parse_trade(pd.DataFrame()).empty


class TestDownload:
    def test_download_csv_follows_url_without_cache_buster(self, monkeypatch):
        captured = {}

        class FakeResponse:
            status_code = 200
            content = _price_csv().encode()
            text = ""

        def fake_get(url, timeout=None, headers=None):
            captured["url"] = url
            return FakeResponse()

        monkeypatch.setattr(ers.requests, "get", fake_get)
        df = ers.download_csv(ers.FRUIT_PRICES_URL)
        assert captured["url"] == ers.FRUIT_PRICES_URL
        assert len(df) == 3

    def test_download_csv_non_200_returns_empty(self, monkeypatch):
        class FakeResponse:
            status_code = 404
            content = b""
            text = "not found"

        monkeypatch.setattr(ers.requests, "get", lambda *a, **k: FakeResponse())
        assert ers.download_csv("https://example.invalid/x.csv").empty

    def test_download_csv_retries_on_request_error(self, monkeypatch):
        calls = {"n": 0}

        def flaky_get(*a, **k):
            calls["n"] += 1
            if calls["n"] < 3:
                raise requests.ConnectionError("boom")
            class FakeResponse:
                status_code = 200
                content = _price_csv().encode()
                text = ""
            return FakeResponse()

        monkeypatch.setattr(ers.requests, "get", flaky_get)
        monkeypatch.setattr(ers.time, "sleep", lambda s: None)
        assert len(ers.download_csv(ers.FRUIT_PRICES_URL)) == 3


class TestIncrementalFilter:
    def test_incremental_keeps_recent_window_only(self):
        now = pd.Timestamp("2026-08-24").to_pydatetime()
        df = pd.DataFrame({
            "date": pd.to_datetime(["2019-01-01", "2026-07-01", "2026-08-01"]),
        })
        kept = ers._filter_incremental(df, now, ers.INCREMENTAL_MONTHS)
        assert kept["date"].tolist() == [pd.Timestamp("2026-07-01"), pd.Timestamp("2026-08-01")]
