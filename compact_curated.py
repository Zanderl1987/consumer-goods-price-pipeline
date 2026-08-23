#!/usr/bin/env python3
"""Compact curated snapshots for all pipelines.
Runs the curated.compact_all function on every table defined in the
pipeline registry (run_all.py). This is useful when you have already
executed the data ingestion pipelines and want to de‑duplicate the
curated layer without re‑running the entire pipeline suite.
"""

import sys
import time
from pathlib import Path

# Ensure the repository root is on the PYTHONPATH
REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

from run_all import PIPELINES
from curated import compact_all
from logging_utils import get_logger

log = get_logger("compact_curated")

def main() -> int:
    # Gather the unique set of table names across all pipeline specs
    tables = sorted({tbl for spec in PIPELINES for tbl in spec.tables})
    if not tables:
        print("[compact_curated] No tables declared in PIPELINES – nothing to compact.")
        return 0

    print(f"[compact_curated] Compacting {len(tables)} curated table(s)...")
    start = time.time()
    try:
        df = compact_all(tables=tables, verbose=True)
    except Exception as exc:
        log.exception("Compaction failed: %s", exc)
        print(f"! Compaction error: {exc}")
        return 1

    if df is not None and not df.empty:
        removed = int(df["removed"].sum())
        print(f"  ✔ Completed – removed {removed:,} duplicate row(s) across {len(df)} table(s).")
    else:
        print("  ✔ Completed – no duplicates found.")
    print(f"[compact_curated] Finished in {time.time() - start:.1f}s")
    return 0

if __name__ == "__main__":
    sys.exit(main())
