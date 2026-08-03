"""
DuckDB query layer over the consumer-goods-price-pipeline Parquet store.

Usage
-----
    import query as q

    # Load a table (returns a pandas DataFrame)
    df = q.load("bls_avg_prices", item="Milk")
    df = q.load("usda_ams_retail", start="2025-01-01")
    df = q.load("eia_gas_retail", area="NUS")
    df = q.load("bls_cpi", series_id="CUUR0000SAF111")

    # Raw SQL — table names match the keys in CATALOG
    df = q.sql(
        "SELECT item, date, price FROM bls_avg_prices "
        "WHERE item IN ('Milk, fresh, whole', 'Eggs, grade A, large')"
    )

    # Discovery
    q.tables()                      # all tables with row counts
    q.schema("bls_avg_prices")      # column names and types
    q.date_range()                  # min/max dates across all tables
    q.reload()                      # refresh views after a pipeline run

Run directly to see a full summary:
    python query.py
"""

import glob as _glob_mod
import os
import duckdb
import pandas as pd

_STORAGE_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage", "raw")
_CURATED_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage", "curated")

# When True (default), a table's view reads its deduplicated curated snapshot
# (storage/curated/<table>/<table>.parquet) if one exists, falling back to the
# raw glob otherwise. Set q.USE_CURATED = False then q.reload() to force raw.
USE_CURATED = True


def _glob(relative: str) -> str:
    return os.path.join(_STORAGE_ROOT, relative).replace("\\", "/")


def _curated_file(table: str) -> str:
    return os.path.join(_CURATED_ROOT, table, f"{table}.parquet").replace("\\", "/")


# ---------------------------------------------------------------------------
# Table catalog — maps logical names to Parquet glob patterns
# ---------------------------------------------------------------------------
# Every *_pipeline.py must register its output table(s) here, in validate.py
# SCHEMAS, in run_all.py PipelineSpec, in curated.py KEYS, and in the tests —
# see CLAUDE.md "Adding a new pipeline — wiring checklist".

CATALOG: dict[str, str] = {
    # ── Government CPI / PPI (BLS) ──────────────────────────────────────────
    "bls_cpi":                  _glob("bls/cpi/**/*.parquet"),
    "bls_avg_prices":           _glob("bls/avg_prices/**/*.parquet"),
    "bls_ppi":                  _glob("bls/ppi/**/*.parquet"),
    # ── USDA — agricultural & food prices ───────────────────────────────────
    "usda_ams_wholesale":       _glob("usda/ams_wholesale/**/*.parquet"),
    "usda_ams_retail":          _glob("usda/ams_retail/**/*.parquet"),
    "usda_prices_received":     _glob("usda/prices_received/**/*.parquet"),
    "usda_prices_paid":         _glob("usda/prices_paid/**/*.parquet"),
    # ── Energy — retail consumer prices (EIA) ───────────────────────────────
    "eia_gas_retail":           _glob("eia/gas_retail/**/*.parquet"),
    "eia_gas_spot":             _glob("eia/gas_spot/**/*.parquet"),
    "eia_electricity_price":    _glob("eia/electricity_price/**/*.parquet"),
    "eia_natgas_price":         _glob("eia/natgas_price/**/*.parquet"),
    # ── Retail grocery / e-commerce APIs ────────────────────────────────────
    "kroger_products":          _glob("kroger/products/**/*.parquet"),
    "walmart_products":         _glob("walmart/products/**/*.parquet"),
    "ebay_listings":            _glob("ebay/listings/**/*.parquet"),
    "openfoodfacts_prices":     _glob("openfoodfacts/prices/**/*.parquet"),
    # ── Healthcare / pharma prices ──────────────────────────────────────────
    "cms_drug_pricing":         _glob("cms/drug_pricing/**/*.parquet"),
    "hospital_prices":          _glob("cms/hospital_prices/**/*.parquet"),
    # ── International consumer prices ───────────────────────────────────────
    "eurostat_hpcp":            _glob("eurostat/hpcp/**/*.parquet"),
    "oecd_cpi":                 _glob("oecd/cpi/**/*.parquet"),
    "statcan_retail_prices":    _glob("statcan/retail_prices/**/*.parquet"),
    "fao_food_prices":          _glob("fao/food_prices/**/*.parquet"),
    "fao_meat_prices":          _glob("fao/meat_prices/**/*.parquet"),
    "worldbank_pinksheet":      _glob("worldbank/pinksheet/**/*.parquet"),
    "imf_commodities":          _glob("imf/commodities/**/*.parquet"),
    # ── US macro retail / consumer series (FRED) ────────────────────────────
    "fred_consumer_prices":     _glob("fred/consumer_prices/**/*.parquet"),
    "fred_used_cars":           _glob("fred/used_cars/**/*.parquet"),
}


# ---------------------------------------------------------------------------
# Analytics cross-table views
# ---------------------------------------------------------------------------

ANALYTICS_VIEWS: dict[str, str] = {
    # Example: weekly blended "grocery basket" — average retail prices joined
    # to CPI for the same items. Real views land here as pipelines go live.
}


_CON: duckdb.DuckDBPyConnection | None = None


def _con() -> duckdb.DuckDBPyConnection:
    global _CON
    if _CON is None:
        _CON = duckdb.connect()
        _register_views(_CON)
    return _CON


def _register_views(con: duckdb.DuckDBPyConnection) -> None:
    """
    Register a DuckDB view for every catalog entry that has data.

    Prefers the deduplicated curated snapshot (storage/curated/<table>/...) when
    one exists and USE_CURATED is True; otherwise reads the raw dated-file glob.
    Curated reads are clean of the cross-run row duplication inherent in the raw
    layer — see curated.py.
    """
    for name, glob_path in CATALOG.items():
        curated = _curated_file(name)
        if USE_CURATED and os.path.exists(curated.replace("/", os.sep)):
            con.execute(f"""
                CREATE OR REPLACE VIEW {name} AS
                SELECT * FROM read_parquet('{curated}')
            """)
            continue
        if not _glob_mod.glob(glob_path.replace("/", os.sep), recursive=True):
            continue
        # union_by_name tolerates schema drift across incremental files
        # hive_partitioning reads year=/month= directory structure as virtual columns
        con.execute(f"""
            CREATE OR REPLACE VIEW {name} AS
            SELECT * FROM read_parquet('{glob_path}', union_by_name=True, hive_partitioning=True)
        """)

    # Analytics cross-table views
    for name, sql_text in ANALYTICS_VIEWS.items():
        try:
            con.execute(f"CREATE OR REPLACE VIEW {name} AS {sql_text}")
        except Exception:
            # Base views may not exist yet — skip silently
            pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def reload() -> None:
    """Re-register all views. Call after a pipeline run drops new files."""
    global _CON
    _CON = None
    _con()


def sql(query: str) -> pd.DataFrame:
    """Execute raw SQL against the registered views. Returns a DataFrame."""
    return _con().execute(query).df()


def load(
    table: str,
    symbol: "str | list[str] | None" = None,
    series_id: "str | list[str] | None" = None,
    metric: "str | None" = None,
    item: "str | None" = None,
    start: "str | None" = None,
    end: "str | None" = None,
    columns: "list[str] | None" = None,
    limit: "int | None" = None,
) -> pd.DataFrame:
    """
    Load a table with optional push-down filters. Returns a DataFrame.

    Parameters
    ----------
    table     : table name — one of the keys in CATALOG
    symbol    : str or list  — filter WHERE symbol = / IN (...)
    series_id : str or list  — filter WHERE series_id = / IN (...)
    metric    : str          — filter WHERE metric = '...'
    item      : str          — filter WHERE item = '...'   (avg prices tables)
    start     : 'YYYY-MM-DD' — filter WHERE date >= start
    end       : 'YYYY-MM-DD' — filter WHERE date <= end
    columns   : list of column names to SELECT (default: all)
    limit     : int          — LIMIT N rows (default: no limit)
    """
    all_tables = set(CATALOG) | set(ANALYTICS_VIEWS)
    if table not in all_tables:
        raise ValueError(f"Unknown table '{table}'. Available: {sorted(all_tables)}")

    select = ", ".join(columns) if columns else "*"
    clauses: list[str] = []

    if symbol is not None:
        if isinstance(symbol, str):
            clauses.append(f"symbol = '{symbol}'")
        else:
            quoted = ", ".join(f"'{s}'" for s in symbol)
            clauses.append(f"symbol IN ({quoted})")

    if series_id is not None:
        if isinstance(series_id, str):
            clauses.append(f"series_id = '{series_id}'")
        else:
            quoted = ", ".join(f"'{s}'" for s in series_id)
            clauses.append(f"series_id IN ({quoted})")

    if metric is not None:
        clauses.append(f"metric = '{metric}'")

    if item is not None:
        clauses.append(f"item = '{item}'")

    if start is not None:
        clauses.append(f"date >= '{start}'")
    if end is not None:
        clauses.append(f"date <= '{end}'")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_clause = f"LIMIT {limit}" if limit else ""
    try:
        return sql(f"SELECT {select} FROM {table} {where} {limit_clause}".strip())
    except duckdb.CatalogException:
        # View not registered — table exists in CATALOG but has no files on disk yet
        return pd.DataFrame()


def schema(table: str) -> pd.DataFrame:
    """Return column names and DuckDB types for a table."""
    all_tables = set(CATALOG) | set(ANALYTICS_VIEWS)
    if table not in all_tables:
        raise ValueError(f"Unknown table '{table}'. Available: {sorted(all_tables)}")
    return sql(f"DESCRIBE {table}")


def tables() -> pd.DataFrame:
    """List all catalog entries with row counts. 'no data' = no files on disk yet."""
    rows = []
    for name in list(CATALOG) + list(ANALYTICS_VIEWS):
        try:
            count = _con().execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            rows.append({"table": name, "rows": f"{count:,}",
                         "type": "analytics" if name in ANALYTICS_VIEWS else "base"})
        except Exception:
            rows.append({"table": name, "rows": "no data",
                         "type": "analytics" if name in ANALYTICS_VIEWS else "base"})
    return pd.DataFrame(rows)


def date_range(table: "str | None" = None) -> pd.DataFrame:
    """
    Return min/max date for each table (or a single table if specified).
    Tables without a 'date' column are skipped.
    """
    targets = [table] if table else list(CATALOG.keys())
    rows = []
    for name in targets:
        try:
            r = _con().execute(
                f"SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM {name}"
            ).fetchone()
            rows.append({"table": name, "min_date": r[0], "max_date": r[1]})
        except Exception:
            pass
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("\n=== Table Inventory ===")
    t = tables()
    print(t.to_string(index=False))

    print("\n=== Date Ranges ===")
    dr = date_range()
    if not dr.empty:
        print(dr.to_string(index=False))
    else:
        print("(no tables with date columns found)")
