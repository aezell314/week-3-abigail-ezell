"""Stage 3 of the pipeline — loads the raw landed file into DuckDB, AS-IS.

Opens the shared DuckDB database and loads the raw characters file into ``raw_characters`` without
cleaning anything.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from de_pipeline.fetch import RAW_DIR

# The DuckDB database file the whole pipeline shares.
DB_PATH = Path("data/warehouse.duckdb")


def connect(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Opens (creating it if needed) the DuckDB database and returns the connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def load_characters(con: duckdb.DuckDBPyConnection, raw_dir: Path = RAW_DIR) -> int:
    """Loads ``raw_dir/characters.json`` into ``raw_characters``; returns the count."""
    con.execute("DROP TABLE IF EXISTS raw_characters")
    con.execute(
        "CREATE TABLE raw_characters AS SELECT * FROM read_json_auto(?)",
        [str(raw_dir / "characters.json")],
    )
    return con.execute("SELECT count(*) FROM raw_characters").fetchone()[0]


def load_all(con: duckdb.DuckDBPyConnection, raw_dir: Path = RAW_DIR) -> dict[str, int]:
    """Loads the raw file(s); returns ``{"raw_characters": <count>}``."""
    return {
        "raw_characters": load_characters(con, raw_dir),
    }


if __name__ == "__main__":
    # uv run python -m de_pipeline.load
    con = connect()
    print("loaded:", load_all(con))
