"""Load checkpoints — no S3 required (uses the local sample fixture).

These cover the PROVIDED ``load.py``; they should be green out of the box. They
also document the shape: the raw load keeps the duplicate row (you dedup later).
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from de_pipeline import load


def test_load_characters_creates_table(con: duckdb.DuckDBPyConnection, raw_dir: Path) -> None:
    count = load.load_characters(con, raw_dir=raw_dir)
    assert count == 6  # 6 raw rows — id 3 is in here twice; don't dedup at load
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    assert "raw_characters" in tables


def test_load_all_returns_counts(con: duckdb.DuckDBPyConnection, raw_dir: Path) -> None:
    counts = load.load_all(con, raw_dir=raw_dir)
    assert counts == {"raw_characters": 6}
