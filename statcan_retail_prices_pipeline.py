# Statistics Canada Retail Prices Pipeline (real implementation)
"""Pipeline to ingest Statistics Canada retail price data into DuckDB.

The pipeline uses Statistics Canada’s Web Data Service (WDS) to download the
full CSV for table **18-10-0245-01** (Monthly average retail prices for selected
products). The CSV is parsed with pandas and stored in a DuckDB table
``statcan_retail``.

The resulting table has the columns:

* ``date`` – first day of the month (YYYY‑MM‑01)
* ``province`` – province/territory abbreviation (e.g. ``ON``)
* ``product`` – product description string
* ``price`` – price value as float
* ``currency`` – currency code (usually ``CAD``)
"""

import io
import urllib.request
from datetime import datetime

import duckdb
import pandas as pd

# Statistics Canada WDS endpoint for the full CSV of the desired table.
# The table ID (PID) is 18-10-0245-01. We request CSV format.
_WDS_URL = (
    "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/18-10-0245-01?"
    "format=CSV&lang=en"
)


def _fetch_csv(url: str) -> pd.DataFrame:
    """Download a CSV from the WDS and return a pandas DataFrame.

    Parameters
    ----------
    url: str
        Fully‑qualified WDS CSV URL.
    """
    with urllib.request.urlopen(url) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Statistics Canada request failed: {resp.status}")
        raw = resp.read().decode("utf-8")
    # Locate header line beginning with REF_DATE
    lines = raw.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith("REF_DATE"):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Could not locate CSV header in WDS response")
    csv_text = "\n".join(lines[header_idx:])
    df = pd.read_csv(io.StringIO(csv_text))
    return df


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """Select and rename columns to match the desired schema.

    The WDS CSV contains many columns; we keep the most relevant ones.
    """
    needed = ["REF_DATE", "GEO", "PROD", "VALUE", "UNIT"]
    for col in needed:
        if col not in df.columns:
            raise RuntimeError(f"Expected column {col} not found in CSV")
    sub = df[needed].copy()
    sub["date"] = pd.to_datetime(sub["REF_DATE"], format="%Y%m").dt.date
    sub["province"] = sub["GEO"]
    sub["product"] = sub["PROD"]
    sub["price"] = pd.to_numeric(sub["VALUE"], errors="coerce")
    sub["currency"] = sub["UNIT"]
    return sub[["date", "province", "product", "price", "currency"]]


def run_pipeline(db_path: str) -> None:
    """Execute the Statistics Canada retail price pipeline.

    Parameters
    ----------
    db_path: str
        Path to the DuckDB database file where the ``statcan_retail`` table will be stored.
    """
    raw_df = _fetch_csv(_WDS_URL)
    tidy_df = _transform(raw_df)
    con = duckdb.connect(db_path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS statcan_retail (
            date DATE,
            province VARCHAR,
            product VARCHAR,
            price DOUBLE,
            currency VARCHAR
        )
        """
    )
    con.register("tmp_statcan", tidy_df)
    con.execute("INSERT INTO statcan_retail SELECT * FROM tmp_statcan")
    con.unregister("tmp_statcan")
    con.close()
