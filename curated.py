"""
Curated layer — deduplicated, compacted snapshots of every raw table.

Why this exists
---------------
Every pipeline writes a *new* dated Parquet file on each run, and the query
layer (query.py) globs **all** of them with union_by_name=True. Because the
incremental pipelines re-fetch overlapping windows, the same logical row is
written many times. Any COUNT/AVG/SUM computed in analytics/ would silently
run over those duplicates.

This module collapses each raw table down to one row per natural key (keeping
the most recently fetched version) and writes a single compacted file to
`storage/curated/<table>/<table>.parquet`. query.py prefers the curated file
when it exists (see query._register_views), so the whole stack reads clean
data with no API changes.

Usage
-----
    python curated.py                 # compact every table that has raw data
    python curated.py --table bls_cpi # compact one table
    python curated.py --summary       # show row reduction per table, no writes
    python curated.py --check         # exit 1 if any table has >5% duplication

    import curated
    curated.compact("bls_avg_prices")     # -> path written
    curated.compact_all()                 # -> summary DataFrame
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import query as q

CURATED_ROOT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "storage", "curated"
)

# Columns that are pipeline bookkeeping, never part of a natural key, and not
# meaningful for "is this the same row" comparisons.
_BOOKKEEPING = {"fetched_at", "year", "month"}

# ---------------------------------------------------------------------------
# Natural-key registry
# ---------------------------------------------------------------------------
# For a table listed here, dedup keeps one row per key tuple — the row with the
# newest `fetched_at` (a later fetch reflects a restatement/correction). Tables
# absent from this map fall back to FULL-ROW dedup: identical rows re-fetched on
# later runs collapse to one, which already removes the bulk of the redundancy
# without risking data loss from a wrong key guess.
KEYS: dict[str, list[str]] = {
    # BLS CPI/PPI — one value per series per period
    "bls_cpi":                ["series_id", "date"],
    "bls_avg_prices":         ["series_id", "date"],
    "bls_ppi":                ["series_id", "date"],
    # USDA AMS market news — deliberately NOT keyed (falls back to full-row
    # dedup). Confirmed live 2026-08-04: terminal-market reports can carry
    # multiple simultaneous real quotes (different shippers/vendors) that
    # are identical across every field the API exposes -- commodity,
    # variety, grade, size, unit, organic, origin, location, date -- yet
    # have genuinely different prices (e.g. three "Anise" quotes at the
    # same Atlanta market on the same day: $37.50, $33.50, $39/$43.25 avg,
    # with no distinguishing field between them anywhere in the response).
    # No natural key can separate real multi-vendor quotes from actual
    # re-fetched duplicates here; full-row dedup only removes exact
    # repeats, which is the correct, non-data-losing behavior for this
    # source.
    # USDA NASS prices — one value per commodity/date
    # description distinguishes same-commodity/date series variants (e.g.
    # PRICE RECEIVED "$/BU" vs "PCT OF PARITY" vs "10 YEAR AVG") --
    # commodity+date alone silently collapsed them together (found live
    # 2026-08-04, a 99%+ row-count collapse on this pipeline's first run).
    "usda_prices_received":   ["commodity", "description", "date"],
    "usda_prices_paid":       ["commodity", "description", "date"],
    # NOAA FOSS commercial landings — deliberately NOT keyed, same reasoning
    # as usda_ams above. Confirmed live 2026-08-14: ~1,328 rows (0.8% of the
    # commercial table) carry species="WITHHELD FOR CONFIDENTIALITY",
    # species_tsn=0 -- NOAA's small-cell suppression collapses genuinely
    # different real species/vessels into one identical placeholder per
    # state/region/year/source, with a real (aggregated) dollars/pounds
    # value. species+state+region+year+source still collided on these rows;
    # no field anywhere distinguishes them. Full-row dedup only removes
    # exact re-fetched duplicates, which is correct here.
    # ERS specialty crops — one value per BLS series per month (series_id is
    # the APU/CU/WPU identifier and already encodes series_type)
    "ers_fruit_nut_prices":   ["series_id", "date"],
    "ers_veg_prices":         ["series_id", "date"],
    # ERS trade — one flow amount per partner/commodity/detail/unit/month
    "ers_fruit_nut_trade":    ["trade_flow", "partner_country", "commodity", "commodity_detail",
                               "unit_type", "date"],
    "ers_veg_trade":          ["trade_flow", "partner_country", "commodity", "commodity_detail",
                               "unit_type", "date"],
    # EIA energy — one price per area/product/date
    "eia_gas_retail":         ["duoarea", "product", "date"],
    "eia_gas_spot":           ["series", "date"],
    "eia_electricity_price":  ["series_id", "date"],
    "eia_natgas_price":       ["series_id", "date"],
    # Retail / e-commerce — one price observation per product per snapshot
    "kroger_products":        ["upc", "store_id", "fetched_at"],
    "kroger_catalog":         ["upc", "store_id"],
    "bestbuy_products":       ["sku", "fetched_at"],
    "walmart_products":       ["product_id", "fetched_at"],
    "ebay_listings":          ["item_id", "fetched_at"],
    "openfoodfacts_prices":   ["id"],
    # Healthcare — CMS Part D Spending by Drug has no NDC column (it's
    # aggregated to brand/generic/manufacturer); corrected from the
    # originally-reserved NDC-based key after live verification 2026-08-04.
    "cms_drug_pricing":       ["brand_name", "generic_name", "manufacturer", "spending_year"],
    "hospital_prices":        ["hospital_name", "cms_certification_number", "item_name", "payer"],
    # International — one value per series/date
    "eurostat_hicp":          ["series_id", "date"],
    "oecd_cpi":               ["series_id", "date"],
    "statcan_retail_prices":  ["item", "city", "date"],
    "wfp_food_prices":        ["market_id", "commodity_id", "date", "pricetype"],
    # FEWS NET — one observation per market/product/price-type/period; cpcv2
    # is the commodity code, product its name (kept out of the key so a
    # rename doesn't fork history). country included because market names
    # are not globally unique across monitored markets.
    "fews_net_food_prices":   ["country", "market", "cpcv2", "price_type", "period_date"],
    # FAO CP/PP domains are per-country (area); corrected from the
    # originally-reserved item-only key after live verification 2026-08-04.
    "fao_food_prices":        ["area", "item", "date"],
    "fao_meat_prices":        ["area", "item", "date"],
    "worldbank_pinksheet":    ["series_id", "date"],
    "imf_commodities":        ["series_id", "date"],
    # FRED — one value per series per date
    "fred_consumer_prices":   ["series_id", "date"],
    "fred_used_cars":         ["series_id", "date"],
    "fred_cpi":               ["series_id", "date"],
}


def _curated_path(table: str) -> str:
    return os.path.join(CURATED_ROOT, table, f"{table}.parquet").replace("\\", "/")


class _raw_reads:
    """
    Context manager that forces query.py to read the RAW dated-file globs.

    Compaction must always source from raw — never from a (possibly stale or
    previously mis-keyed) curated snapshot — so that re-running curated.py
    rebuilds cleanly from the ground truth and is genuinely idempotent.
    """

    def __enter__(self):
        self._prev = q.USE_CURATED
        q.USE_CURATED = False
        q.reload()
        return self

    def __exit__(self, *exc):
        q.USE_CURATED = self._prev
        q.reload()
        return False


def _dedup_subset(table: str, df: pd.DataFrame) -> list[str]:
    """
    Return the column list to dedup on.

    A configured natural key is used only when EVERY one of its columns is
    present — a partial key would be too coarse and silently merge distinct
    rows. When the key doesn't fully match the data, fall back to full-row
    dedup, which only removes exact re-fetched duplicates and can never lose
    a genuinely distinct row.
    """
    key = KEYS.get(table)
    if key and all(c in df.columns for c in key):
        return key
    return [c for c in df.columns if c not in _BOOKKEEPING]


def _sort_recency(df: pd.DataFrame) -> pd.DataFrame:
    """Sort so the freshest version of a key sorts last (kept by keep='last')."""
    order = [c for c in ("fetched_at", "filed", "filing_date", "filed_date", "last_refreshed") if c in df.columns]
    if not order:
        return df
    return df.sort_values(order, kind="stable")


def dedup(table: str, df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate a raw DataFrame on its natural key, keeping the latest version."""
    if df.empty:
        return df
    subset = _dedup_subset(table, df)
    out = _sort_recency(df).drop_duplicates(subset=subset, keep="last")
    return out.reset_index(drop=True)


def compact(table: str) -> "str | None":
    """
    Read all raw files for `table`, dedup, and write the curated snapshot.

    Returns the curated file path, or None if the table has no raw data.
    """
    if table not in q.CATALOG:
        raise ValueError(f"Unknown table '{table}'. Available: {sorted(q.CATALOG)}")

    with _raw_reads():
        raw = q.load(table)
    if raw.empty:
        return None

    clean = dedup(table, raw)
    out_path = _curated_path(table)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    clean.to_parquet(out_path, index=False, compression="snappy")
    q.reload()
    return out_path


def compact_all(tables: "list[str] | None" = None, verbose: bool = True) -> pd.DataFrame:
    """
    Compact every table (or a given subset) that has raw data.

    Returns a summary DataFrame: table | raw_rows | curated_rows | removed | pct_removed.
    """
    names = tables if tables is not None else list(q.CATALOG)
    rows = []
    with _raw_reads():
        for name in names:
            try:
                raw = q.load(name)
            except Exception as e:  # noqa: BLE001 — surface, keep going
                if verbose:
                    print(f"  {name:28s} ERROR {str(e)[:50]}")
                continue
            if raw.empty:
                continue
            clean = dedup(name, raw)
            out_path = _curated_path(name)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            clean.to_parquet(out_path, index=False, compression="snappy")

            removed = len(raw) - len(clean)
            pct = round(100 * removed / len(raw), 1) if len(raw) else 0.0
            rows.append({
                "table": name,
                "raw_rows": len(raw),
                "curated_rows": len(clean),
                "removed": removed,
                "pct_removed": pct,
            })
            if verbose:
                flag = "  <-- dupes" if pct >= 5 else ""
                print(f"  {name:28s} {len(raw):>10,} -> {len(clean):>10,}  (-{pct:>4.1f}%){flag}")

    # _raw_reads.__exit__ reloads views; curated files now take precedence
    return pd.DataFrame(rows)


def summary(tables: "list[str] | None" = None) -> pd.DataFrame:
    """Dry-run: report duplication per table without writing curated files."""
    names = tables if tables is not None else list(q.CATALOG)
    rows = []
    with _raw_reads():
        loaded = {}
        for name in names:
            try:
                df = q.load(name)
            except Exception:
                continue
            if not df.empty:
                loaded[name] = df
    for name, raw in loaded.items():
        clean = dedup(name, raw)
        removed = len(raw) - len(clean)
        rows.append({
            "table": name,
            "raw_rows": len(raw),
            "curated_rows": len(clean),
            "removed": removed,
            "pct_removed": round(100 * removed / len(raw), 1) if len(raw) else 0.0,
            "keyed": name in KEYS,
        })
    df = pd.DataFrame(rows)
    return df.sort_values("pct_removed", ascending=False).reset_index(drop=True) if not df.empty else df


def main() -> int:
    ap = argparse.ArgumentParser(description="Compact raw tables into deduplicated curated snapshots.")
    ap.add_argument("--table", help="compact a single table")
    ap.add_argument("--summary", action="store_true", help="report duplication, write nothing")
    ap.add_argument("--check", action="store_true", help="exit 1 if any table has >5%% duplication")
    args = ap.parse_args()

    if args.summary or args.check:
        df = summary([args.table] if args.table else None)
        if df.empty:
            print("No tables with raw data found.")
            return 0
        print(df.to_string(index=False))
        if args.check:
            bad = df[df["pct_removed"] > 5.0]
            if not bad.empty:
                print(f"\n{len(bad)} table(s) exceed 5% duplication. Run `python curated.py` to compact.")
                return 1
        return 0

    if args.table:
        path = compact(args.table)
        print(f"Wrote {path}" if path else f"No raw data for '{args.table}'.")
        return 0

    print("Compacting all tables with raw data...\n")
    df = compact_all()
    if df.empty:
        print("No tables with raw data found.")
        return 0
    total_removed = int(df["removed"].sum())
    total_raw = int(df["raw_rows"].sum())
    print(f"\nDone. {len(df)} tables compacted, {total_removed:,} duplicate rows removed "
          f"({100*total_removed/total_raw:.1f}% of {total_raw:,}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
