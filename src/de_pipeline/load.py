"""Stage 3 — load the raw landed file into DuckDB, AS-IS.

PROVIDED — same shape as Weeks 1-2. Open the shared DuckDB database and load the
raw characters file into ``raw_characters`` without cleaning anything. The nested
``origin`` / ``location`` objects land as structs and ``episode`` lands as a
list — that's fine, transform.py deals with it (the "T" in ELT).
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from de_pipeline.fetch import RAW_DIR

# The DuckDB database file the whole pipeline shares.
DB_PATH = Path("data/warehouse.duckdb")


def connect(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open (creating it if needed) the DuckDB database and return the connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def load_characters(con: duckdb.DuckDBPyConnection, raw_dir: Path = RAW_DIR) -> int:
    """Load ``raw_dir/characters.json`` into ``raw_characters``; return the count."""
    con.execute("DROP TABLE IF EXISTS raw_characters")
    con.execute(
        "CREATE TABLE raw_characters AS SELECT * FROM read_json_auto(?)",
        [str(raw_dir / "characters.json")],
    )
    return con.execute("SELECT count(*) FROM raw_characters").fetchone()[0]


def load_all(con: duckdb.DuckDBPyConnection, raw_dir: Path = RAW_DIR) -> dict[str, int]:
    """Load the raw file(s); return ``{"raw_characters": <count>}``."""
    return {
        "raw_characters": load_characters(con, raw_dir),
    }


if __name__ == "__main__":
    # uv run python -m de_pipeline.load
    con = connect()
    print("loaded:", load_all(con))
