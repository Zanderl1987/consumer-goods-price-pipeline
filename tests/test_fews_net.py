"""Tests for fews_net_pipeline - FEWS NET Data Warehouse market prices."""

import io
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import fews_net_pipeline as fn

# Synthetic fixture shaped like the real 63-column export (fields=body).
SAMPLE_CSV = """\
country,country_code,market,product,cpcv2,period_date,price_type,value,unit,currency
Sudan,SD,Khartoum Market,Wheat,0.1,2024-01-31,Wholesale,150.0,USD/MT,USD
Sudan,SD,Khartoum Market,Sorghum,0.2,2024-01-31,Retail,80.0,USD/MT,USD
South Sudan,SS,Juba Market,Maize,0.3,2024-02-29,Producer,120.0,SSP/MT,SSP
"""


def _fake_response(body: str, status: int = 200):
    resp = MagicMock()
    resp.status_code = status
    resp.content = body.encode()
    resp.text = body
    if body.lstrip().startswith(("[", "{")):
        resp.json.return_value = __import__("json").loads(body)
    return resp


class TestCountryCodes:
    def test_list_form(self, monkeypatch):
        monkeypatch.setattr(fn.requests, "get",
                            lambda *a, **k: _fake_response('[{"country_code":"KE"},{"country_code":"ET"}]'))
        assert fn._country_codes() == ["ET", "KE"]

    def test_dict_value_form(self, monkeypatch):
        body = '{"value":[{"country_code":"KE"},{"country_code":"ET"}]}'
        monkeypatch.setattr(fn.requests, "get", lambda *a, **k: _fake_response(body))
        assert fn._country_codes() == ["ET", "KE"]

    def test_skips_missing_codes(self, monkeypatch):
        body = '[{"country_code":"KE"},{"market":"no code"},{"country_code":""}]'
        monkeypatch.setattr(fn.requests, "get", lambda *a, **k: _fake_response(body))
        assert fn._country_codes() == ["KE"]

    def test_failure_returns_empty(self, monkeypatch):
        monkeypatch.setattr(fn.requests, "get",
                            lambda *a, **k: (_ for _ in ()).throw(fn.requests.RequestException("down")))
        monkeypatch.setattr(fn.time, "sleep", lambda s: None)
        assert fn._country_codes() == []


class TestFetchCountry:
    def test_success_parses_csv(self, monkeypatch):
        monkeypatch.setattr(fn.requests, "get", lambda *a, **k: _fake_response(SAMPLE_CSV))
        df = fn._fetch_country("SD", None)
        assert not df.empty
        assert len(df) == 3

    def test_params_include_fields_body_and_dataset(self, monkeypatch):
        captured = {}

        def fake_get(url, **kwargs):
            captured.update(kwargs.get("params", {}))
            return _fake_response(SAMPLE_CSV)

        monkeypatch.setattr(fn.requests, "get", fake_get)
        fn._fetch_country("ET", "2020-01-01")
        assert captured["dataset"] == fn.DATASET
        assert captured["country"] == "ET"
        assert captured["format"] == "csv"
        assert captured["fields"] == "body"
        assert captured["start_date"] == "2020-01-01"

    def test_incremental_omits_start_date(self, monkeypatch):
        captured = {}

        def fake_get(url, **kwargs):
            captured.update(kwargs.get("params", {}))
            return _fake_response(SAMPLE_CSV)

        monkeypatch.setattr(fn.requests, "get", fake_get)
        fn._fetch_country("ET", None)
        assert "start_date" not in captured

    def test_500_followed_by_success(self, monkeypatch):
        calls = []

        def fake_get(url, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                return _fake_response("", 500)
            return _fake_response(SAMPLE_CSV)

        monkeypatch.setattr(fn.requests, "get", fake_get)
        monkeypatch.setattr(fn.time, "sleep", lambda s: None)
        df = fn._fetch_country("ET", None)
        assert len(df) == 3

    def test_persistent_500_returns_empty(self, monkeypatch):
        monkeypatch.setattr(fn.requests, "get", lambda *a, **k: _fake_response("", 500))
        monkeypatch.setattr(fn.time, "sleep", lambda s: None)
        assert fn._fetch_country("ET", None).empty

    def test_timeout_returns_empty(self, monkeypatch):
        def fake_get(url, **kwargs):
            raise fn.requests.Timeout("timed out")

        monkeypatch.setattr(fn.requests, "get", fake_get)
        monkeypatch.setattr(fn.time, "sleep", lambda s: None)
        assert fn._fetch_country("ET", None).empty

    def test_header_only_empty_body_returns_empty(self, monkeypatch):
        monkeypatch.setattr(fn.requests, "get",
                            lambda *a, **k: _fake_response("country,market,value\n"))
        assert fn._fetch_country("ET", None).empty


class TestNormalize:
    def test_snake_case_and_bookkeeping(self):
        df = pd.read_csv(io.StringIO(SAMPLE_CSV))
        result = fn._normalize(df)
        assert all(c == c.lower() for c in result.columns)
        assert "source" in result.columns
        assert "fetched_at" in result.columns
        assert (result["source"] == "fews_net").all()

    def test_pipeline_drops_nan_key_rows(self):
        df = pd.read_csv(io.StringIO(SAMPLE_CSV))
        df.loc[1, "period_date"] = None
        result = fn._normalize(df)
        assert len(result) == len(df) - 1

    def test_empty_input(self):
        assert fn._normalize(pd.DataFrame()).empty


class TestMain:
    def test_has_main(self):
        assert callable(fn.main)

    def test_aborts_without_fitting_helpers(self, monkeypatch):
        monkeypatch.setattr(fn, "_country_codes", lambda: [])
        with patch("fews_net_pipeline.write_partitioned") as wp:
            fn.main(backfill=True)
        wp.assert_not_called()