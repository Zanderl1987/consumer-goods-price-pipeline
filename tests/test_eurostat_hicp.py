import os
import tempfile
import duckdb
import pytest

from eurostat_hicp_pipeline import run_pipeline as eurostat_run

def test_eurostat_hicp_pipeline_creates_table():
    # Create a temporary duckdb file
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_eurostat.db")
        eurostat_run(db_path)
        con = duckdb.connect(db_path)
        # Verify table exists
        tables = con.execute("SHOW TABLES").fetchall()
        assert ("hicp",) in tables
        # Verify at least one row
        count = con.execute("SELECT COUNT(*) FROM hicp").fetchone()[0]
        assert count > 0
        con.close()
