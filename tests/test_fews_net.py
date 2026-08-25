"""Tests for fews_net_pipeline — FEWS NET Data Warehouse market prices."""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import fews_net_pipeline as fn

# ── Synthetic fixtures ────────────────────────────────────────────────────────

SAMPLE_CSV = """\
Country,Admin 1,Admin 2,Market,Product,Market Price Factor,Period Date,Price Type,Value,Unit,Currency
Sudan,Khartoum,,Khartoum Market,Wheat,Wholesale,2024-01-15,Wholesale,150.0,USD/MT,USD
Sudan,Khartoum,,Khartoum Market,Sorghum,Retail,2024-01-15,Retail,80.0,USD/MT,USD
South Sudan,Juba,,Juba Market,Maize,Producer,2024-02-01,Producer,120.0,SSP/MT,SSP
"""


class TestNormalizeColumns:
    def test_snake_case_columns(self):
        df = pd.read_csv(__import__("io").StringIO(SAMPLE_CSV))
        result = fn._normalize_columns(df)
        assert all(c == c.lower() for c in result.columns)
        assert "source" in result.columns
        assert "fetched_at" in result.columns

    def test_empty_input(self):
        result = fn._normalize_columns(pd.DataFrame())
        assert result.empty

    def test_source_label(self):
        df = pd.read_csv(__import__("io").StringIO(SAMPLE_CSV))
        result = fn._normalize_columns(df)
        assert (result["source"] == "fews_net").all()


class TestFetchData:
    def test_success(self, monkeypatch):
        def fake_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            resp.content = SAMPLE_CSV.encode()
            resp.text = SAMPLE_CSV
            return resp

        monkeypatch.setattr(fn.requests, "get", fake_get)
        df = fn._fetch_data()
        assert not df.empty
        assert len(df) == 3

    def test_server_error_returns_empty(self, monkeypatch):
        attempt = [0]

        def fake_get(url, **kwargs):
            attempt[0] += 1
            resp = MagicMock()
            resp.status_code = 502
            resp.content = b""
            return resp

        monkeypatch.setattr(fn.requests, "get", fake_get)
        monkeypatch.setattr(fn.time, "sleep", lambda s: None)
        df = fn._fetch_data()
        assert df.empty

    def test_timeout_returns_empty(self, monkeypatch):
        def fake_get(url, **kwargs):
            raise fn.requests.Timeout("timed out")

        monkeypatch.setattr(fn.requests, "get", fake_get)
        monkeypatch.setattr(fn.time, "sleep", lambda s: None)
        df = fn._fetch_data()
        assert df.empty

    def test_start_date_passed_as_param(self, monkeypatch):
        captured = {}

        def fake_get(url, **kwargs):
            captured.update(kwargs.get("params", {}))
            resp = MagicMock()
            resp.status_code = 200
            resp.content = b"col1,col2\na,b\n"
            resp.text = "col1,col2\na,b\n"
            return resp

        monkeypatch.setattr(fn.requests, "get", fake_get)
        fn._fetch_data(start_date="2020-01-01")
        assert captured.get("start_date") == "2020-01-01"
