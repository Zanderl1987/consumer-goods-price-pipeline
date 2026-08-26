import io

import pandas as pd
import pytest
import requests

import fews_net_pipeline as fews


def _fdw_csv() -> str:
    return (
        "geographic_group,fewsnet_region,country,admin_1,admin_2,market,cpcv2,product,"
        "price_type,product_source,collection_schedule,start_date,period_date,value,"
        "currency,unit,latitude,longitude,is_staple_food\n"
        "ethiopia,EW,Ethiopia,Amhara,South Gondar,Debre Tabor,pmaizeis,Maize (white),"
        "Retail,monthly_reported,Monthly,2026-07-01,2026-07-01,32.5,ETB,KG,11.7333,38.0167,True\n"
        "yemen,YE,Yemen,Sana'a,,Sana'a City,rwheatis,Wheat (local),"
        "Wholesale,monthly_reported,Monthly,2026-07-15,2026-07-01,850.0,YER,KG,15.3547,44.2067,False\n"
    )


class TestParseFrame:
    def test_parse_frame_tidy_schema(self):
        raw = pd.read_csv(io.StringIO(_fdw_csv()))
        out = fews.parse_frame(raw)
        assert len(out) == 2
        assert set(out.columns) == {
            "period_date", "country", "admin_1", "admin_2", "market", "cpcv2",
            "product", "price_type", "value", "currency", "unit",
            "latitude", "longitude", "source",
        }
        row = out[out["country"] == "Ethiopia"].iloc[0]
        assert row["period_date"] == pd.Timestamp("2026-07-01")
        assert row["market"] == "Debre Tabor"
        assert row["price_type"] == "Retail"
        assert row["value"] == pytest.approx(32.5)
        assert row["currency"] == "ETB"
        assert row["source"] == "fews_net"

    def test_parse_frame_drops_incomplete_rows(self):
        raw = pd.read_csv(io.StringIO(_fdw_csv()))
        raw.loc[0, "value"] = None
        raw.loc[0, "market"] = None
        out = fews.parse_frame(raw)
        assert len(out) == 1
        assert out.iloc[0]["country"] == "Yemen"

    def test_parse_frame_handles_empty(self):
        assert fews.parse_frame(pd.DataFrame()).empty


class TestDownload:
    def test_incremental_filters_by_start_date(self, monkeypatch):
        captured = {}

        class FakeResponse:
            status_code = 200
            content = _fdw_csv().encode()
            text = ""

        def fake_get(url, params=None, timeout=None, headers=None):
            captured["url"] = url
            captured["params"] = params
            return FakeResponse()

        monkeypatch.setattr(fews.requests, "get", fake_get)
        df = fews.download_prices(backfill=False)
        assert captured["url"] == fews.FDW_URL
        assert captured["params"]["fields"] == "simple"
        assert "start_date" in captured["params"]
        assert len(df) == 2

    def test_backfill_has_no_start_date_floor(self, monkeypatch):
        captured = {}

        class FakeResponse:
            status_code = 200
            content = _fdw_csv().encode()
            text = ""

        def fake_get(url, params=None, timeout=None, headers=None):
            captured["params"] = params
            return FakeResponse()

        monkeypatch.setattr(fews.requests, "get", fake_get)
        fews.download_prices(backfill=True)
        assert "start_date" not in captured["params"]

    def test_non_200_returns_empty(self, monkeypatch):
        class FakeResponse:
            status_code = 502
            content = b""
            text = "Bad Gateway"

        monkeypatch.setattr(fews.requests, "get", lambda *a, **k: FakeResponse())
        assert fews.download_prices(backfill=True).empty

    def test_retries_on_timeout_then_succeeds(self, monkeypatch):
        calls = {"n": 0}

        def flaky_get(*a, **k):
            calls["n"] += 1
            if calls["n"] < 3:
                raise requests.ReadTimeout("slow")
            class FakeResponse:
                status_code = 200
                content = _fdw_csv().encode()
                text = ""
            return FakeResponse()

        monkeypatch.setattr(fews.requests, "get", flaky_get)
        monkeypatch.setattr(fews.time, "sleep", lambda s: None)
        assert len(fews.download_prices(backfill=False)) == 2
