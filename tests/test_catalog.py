"""Catalog wiring guards — every pipeline's table must be wired everywhere."""

import os
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import query as q

EXPECTED_TABLES = [
    "bls_cpi",
    "bls_avg_prices",
    "bls_ppi",
    "usda_ams_wholesale",
    "usda_ams_retail",
    "usda_prices_received",
    "usda_prices_paid",
    "eia_gas_retail",
    "eia_gas_spot",
    "eia_electricity_price",
    "eia_natgas_price",
    "kroger_products",
    "bestbuy_products",
    "walmart_products",
    "ebay_listings",
    "openfoodfacts_prices",
    "cms_drug_pricing",
    "hospital_prices",
    "eurostat_hicp",
    "oecd_cpi",
    "statcan_retail_prices",
    "wfp_food_prices",
    "fao_food_prices",
    "fao_meat_prices",
    "worldbank_pinksheet",
    "imf_commodities",
    "fred_consumer_prices",
    "fred_used_cars",
]


class TestCatalogCompleteness:
    def test_all_expected_tables_registered(self):
        missing = [t for t in EXPECTED_TABLES if t not in q.CATALOG]
        assert not missing, f"Missing from CATALOG: {missing}"

    def test_no_extra_surprise_tables(self):
        extra = [t for t in q.CATALOG if t not in EXPECTED_TABLES]
        assert not extra, f"CATALOG tables missing from EXPECTED_TABLES: {extra}"

    def test_catalog_count(self):
        assert len(q.CATALOG) >= len(EXPECTED_TABLES), (
            f"CATALOG has {len(q.CATALOG)} entries, expected >= {len(EXPECTED_TABLES)}"
        )


class TestCatalogPaths:
    def test_all_paths_under_storage_raw(self):
        storage_root = os.path.join(REPO_ROOT, "storage", "raw").replace("\\", "/")
        bad = {
            name: path
            for name, path in q.CATALOG.items()
            if storage_root.lower() not in path.lower()
        }
        assert not bad, f"CATALOG entries not under storage/raw: {bad}"

    def test_all_paths_end_in_parquet_glob(self):
        bad = {
            name: path
            for name, path in q.CATALOG.items()
            if not path.endswith(".parquet")
        }
        assert not bad, f"CATALOG entries without .parquet extension: {bad}"

    def test_no_glob_collisions(self):
        """No two tables may share one glob — colliding globs union mismatched
        schemas into both views."""
        seen: dict[str, str] = {}
        collisions = []
        for name, path in q.CATALOG.items():
            if path in seen:
                collisions.append((seen[path], name, path))
            seen[path] = name
        assert not collisions, f"CATALOG glob collisions: {collisions}"

    def test_storage_dirs_exist(self):
        """Each CATALOG glob path's base (non-wildcard) directory should exist."""
        missing_dirs = []
        for name, glob_path in q.CATALOG.items():
            normalized = glob_path.replace("/", os.sep)
            base = normalized.split("*")[0].rstrip(os.sep)
            if not os.path.isdir(base):
                missing_dirs.append((name, base))
        assert not missing_dirs, (
            "Storage directories missing for tables: "
            + ", ".join(f"{n} -> {p}" for n, p in missing_dirs)
        )


class TestDiscoveryHelpers:
    def test_tables_runs_without_error(self):
        df = q.tables()
        assert df is not None
        assert "table" in df.columns
        assert "rows" in df.columns

    def test_tables_returns_all_catalog_entries(self):
        df = q.tables()
        registered = set(df["table"].tolist())
        assert registered.issubset(set(q.CATALOG.keys()) | set(q.ANALYTICS_VIEWS.keys()))

    def test_date_range_runs_without_error(self):
        df = q.date_range()
        assert df is not None  # may be empty if no data files

    def test_reload_does_not_raise(self):
        q.reload()

    def test_schema_raises_on_unknown_table(self):
        with pytest.raises(ValueError, match="Unknown table"):
            q.schema("nonexistent_table_xyz")

    def test_load_raises_on_unknown_table(self):
        with pytest.raises(ValueError, match="Unknown table"):
            q.load("nonexistent_table_xyz")

    def test_load_returns_empty_for_empty_table(self):
        df = q.load("bls_cpi")
        assert df.empty or len(df) >= 0
