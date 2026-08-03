"""Unit tests for the logging helpers."""

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from logging_utils import get_logger, log_pipeline_failure


class TestLogging:
    def test_get_logger_returns_named_logger(self):
        logger = get_logger("test")
        assert logger.name == "test"

    def test_log_pipeline_failure_does_not_raise(self):
        try:
            log_pipeline_failure("bls_cpi", "boom")
        except Exception as exc:  # pragma: no cover
            assert False, f"log_pipeline_failure raised: {exc}"
