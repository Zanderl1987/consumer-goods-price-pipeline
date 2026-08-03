"""Pipeline-module wiring guard — every registered pipeline file must exist
and expose the expected CLI entrypoint. Plus per-pipeline smoke checks."""

import importlib.util
import os
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from run_all import PIPELINES

PIPELINE_MODULES = {p.file: p for p in PIPELINES}


def _load_module(name: str):
    spec = importlib.util.spec_from_file_location(name, name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestPipelineFilesExist:
    @pytest.mark.parametrize("filename", sorted(PIPELINE_MODULES))
    def test_pipeline_file_exists(self, filename):
        path = os.path.join(REPO_ROOT, filename)
        assert os.path.exists(path), f"run_all.py references missing file: {filename}"

    @pytest.mark.parametrize("filename", sorted(PIPELINE_MODULES))
    def test_pipeline_imports_and_has_main(self, filename):
        module = _load_module(os.path.join(REPO_ROOT, filename))
        assert hasattr(module, "main"), f"{filename} has no main()"


class TestPipelineSpecSanity:
    def test_all_tables_registered_in_catalog(self):
        import query as q
        bad = []
        for p in PIPELINES:
            for table in p.tables:
                if table not in q.CATALOG:
                    bad.append((p.name, table))
        assert not bad, f"PipelineSpec tables missing from CATALOG: {bad}"

    def test_pipeline_names_unique(self):
        names = [p.name for p in PIPELINES]
        assert len(names) == len(set(names)), f"Duplicate pipeline names: {names}"

    def test_stages_valid(self):
        assert all(p.stage in (1, 2, 3) for p in PIPELINES)


class TestSeedPipelineConfig:
    def test_bls_avg_prices_has_items(self):
        m = _load_module(os.path.join(REPO_ROOT, "bls_avg_prices_pipeline.py"))
        assert hasattr(m, "AVG_PRICE_SERIES")
        assert len(m.AVG_PRICE_SERIES) >= 10

    def test_fred_consumer_has_series(self):
        m = _load_module(os.path.join(REPO_ROOT, "fred_consumer_prices_pipeline.py"))
        assert hasattr(m, "CONSUMER_SERIES")
        assert len(m.CONSUMER_SERIES) >= 10

    def test_curated_keys_cover_catalog(self):
        """Every CATALOG table either has a KEYS entry or is explicitly acceptable
        as full-row dedup (absent = fine). This just ensures curated imports."""
        import curated
        assert isinstance(curated.KEYS, dict)

    def test_validate_schemas_cover_catalog(self):
        import validate
        missing = [t for t in __import__("query").CATALOG if t not in validate.SCHEMAS]
        assert not missing, f"validate.py SCHEMAS missing tables: {missing}"
