# Open Food Facts Open Prices Pipeline (stub implementation)
"""Pipeline to ingest Open Food Facts price data into DuckDB.

This stub creates a minimal DuckDB table with dummy data for unit testing.
"""

def run_pipeline(db_path: str) -> None:
    """Create or connect to a DuckDB database and load an `openfoodfacts_prices` table.

    Parameters
    ----------
    db_path: str
        Path to the DuckDB database file.
    """
    import duckdb
    con = duckdb.connect(db_path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS openfoodfacts_prices (
            barcode VARCHAR,
            product_name VARCHAR,
            retailer VARCHAR,
            price DOUBLE,
            currency VARCHAR,
            date DATE
        )
        """
    )
    con.execute(
        """
        INSERT INTO openfoodfacts_prices VALUES (
            '0123456789012',
            'Example Product',
            'Example Store',
            1.99,
            'EUR',
            '2023-01-01'
        )
        """
    )
    con.close()
