"""Unit tests for the Hive-partitioned Parquet writer."""

import os
import sys
import tempfile

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from storage_utils import write_partitioned, find_parquet_files


class TestWritePartitioned:
    def test_writes_hive_partition_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            df = pd.DataFrame({
                "item": ["Milk", "Eggs"],
                "price": [3.5, 4.2],
                "fetched_at": ["2026-01-05T00:00:00", "2026-02-05T00:00:00"],
            })
            path = write_partitioned(df, tmp, "test_table_20260105.parquet")
            # Partition key is the MAX fetched_at -> 2026-02
            assert os.path.sep + "year=2026" + os.path.sep + "month=02" in path
            assert os.path.exists(path)

    def test_reads_back_with_pandas(self):
        with tempfile.TemporaryDirectory() as tmp:
            df = pd.DataFrame({
                "item": ["Milk"],
                "price": [3.5],
                "fetched_at": ["2026-01-05T00:00:00"],
            })
            write_partitioned(df, tmp, "t.parquet")
            files = find_parquet_files(tmp)
            assert len(files) == 1
            back = pd.read_parquet(files[0])
            assert back["item"].iloc[0] == "Milk"

    def test_fetched_at_absent_uses_now(self):
        with tempfile.TemporaryDirectory() as tmp:
            df = pd.DataFrame({"item": ["Bread"], "price": [2.0]})
            path = write_partitioned(df, tmp, "t.parquet")
            assert os.path.exists(path)
