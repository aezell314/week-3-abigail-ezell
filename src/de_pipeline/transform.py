"""Stage 4 — the warehouse transforms.

PROVIDED — this is the Week 2 toolkit applied to the character data, so you can
keep your attention on the new API ingestion. Study it; you don't rewrite it.
(It's still worth reading: every move here is one you made by hand in Week 2.)

The raw table has the usual API-shaped quirks:

  raw_characters
    - ``id`` can repeat if pages shifted under you mid-fetch (pagination drift)
      -> keep one row per id.
    - ``status`` is "Alive" / "Dead" / "unknown" (and the odd blank) -> normalize.
    - ``origin`` and ``location`` are nested structs ``{name, url}`` -> flatten.
    - ``created`` is an ISO-8601 timestamp string -> parse to a DATE.
    - ``episode`` is a LIST of episode URLs -> count them, and (in Polars) explode
      them to see which episodes have the most characters.

The four transforms mirror Week 2 one-for-one:
  dedupe_characters    -> ROW_NUMBER() dedup (one row per id)
  clean_characters     -> normalize/flatten/parse, derive episode_count
  species_summary      -> group + aggregate, with a bound `min_count` parameter
  episode_appearances  -> explode a list column in POLARS (the tool-choice moment)
"""

from __future__ import annotations

import duckdb


def dedupe_characters(con: duckdb.DuckDBPyConnection) -> int:
    """Build ``characters_deduped`` with exactly one row per ``id``.

    APIs don't promise a stable order across pages: if a record is added while
    you're paging, the same id can land on two pages. ROW_NUMBER() over the id
    keeps one row per id. The fixture duplicates are identical; ORDER BY id
    does not choose a deterministic winner if same-id payloads disagree.
    Unlike Week 2, this source has no update/version field to select the latest
    record. Deduplication also cannot recover records missed during pagination.
    """
    con.execute(
        """
        CREATE OR REPLACE TABLE characters_deduped AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS _rn
            FROM raw_characters
        )
        SELECT * EXCLUDE (_rn)
        FROM ranked
        WHERE _rn = 1
        """
    )
    return con.execute("SELECT count(*) FROM characters_deduped").fetchone()[0]


def clean_characters(con: duckdb.DuckDBPyConnection) -> int:
    """Build ``clean_characters`` from ``characters_deduped``:

      - normalize ``status`` (lower/trim, blanks -> 'unknown');
      - flatten the nested ``origin`` / ``location`` structs to ``origin_name`` /
        ``location_name``;
      - parse ``created`` (ISO-8601 text) into a real DATE;
      - derive ``episode_count`` = number of episodes the character appears in.
    """
    con.execute(
        """
        CREATE OR REPLACE TABLE clean_characters AS
        SELECT
            id,
            name,
            CASE
                WHEN status IS NULL OR TRIM(status) = '' THEN 'unknown'
                ELSE LOWER(TRIM(status))
            END AS status,
            NULLIF(TRIM(species), '')                AS species,
            NULLIF(TRIM(gender), '')                 AS gender,
            origin.name                              AS origin_name,
            location.name                            AS location_name,
            TRY_CAST(created AS TIMESTAMP)::DATE      AS created_date,
            len(episode)                             AS episode_count
        FROM characters_deduped
        """
    )
    return con.execute("SELECT count(*) FROM clean_characters").fetchone()[0]


def species_summary(con: duckdb.DuckDBPyConnection, min_count: int = 1) -> int:
    """Build ``species_summary`` — one row per species with ``character_count``,
    ``alive_count`` and ``location_count`` (distinct locations) — from
    ``clean_characters``.

    ``min_count`` is a threshold (keep species with at least that many
    characters) and is passed to SQL as a BOUND parameter (``$min_count``), never
    an f-string — same discipline as Week 2.
    """
    con.execute(
        """
        CREATE OR REPLACE TABLE species_summary AS
        SELECT
            species,
            count(*)                                              AS character_count,
            count(*) FILTER (WHERE status = 'alive')              AS alive_count,
            count(DISTINCT location_name)                         AS location_count
        FROM clean_characters
        WHERE species IS NOT NULL
        GROUP BY species
        HAVING count(*) >= $min_count
        """,
        {"min_count": min_count},
    )
    return con.execute("SELECT count(*) FROM species_summary").fetchone()[0]


def episode_appearances(con: duckdb.DuckDBPyConnection) -> int:
    """Build ``episode_appearances`` (columns ``episode_id``, ``appearance_count``)
    — how many distinct characters appear in each episode, one row per episode.

    PROVIDED in Polars, applying Week 2's tool-choice pattern.
    ``episode`` is a list column; exploding it
    into one row per (character, episode) and counting reads more naturally as a
    DataFrame op than in SQL. Dedup characters by id first (pagination drift),
    pull the episode id out of each URL, explode, group, count.
    """
    import polars as pl

    chars = con.execute("SELECT id, episode FROM raw_characters").pl()

    out = (
        chars.unique(subset="id", keep="first")
        .explode("episode")
        .filter(pl.col("episode").is_not_null())
        # episode URLs look like ".../api/episode/42" — keep the trailing number.
        .with_columns(
            pl.col("episode").str.extract(r"(\d+)$").cast(pl.Int64).alias("episode_id")
        )
        .group_by("episode_id")
        .agg(pl.len().alias("appearance_count"))
    )

    con.register("episode_src", out)
    con.execute("CREATE OR REPLACE TABLE episode_appearances AS SELECT * FROM episode_src")
    con.unregister("episode_src")
    return con.execute("SELECT count(*) FROM episode_appearances").fetchone()[0]


def run_transforms(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Run every transform in order; return ``{table_name: row_count}``."""
    return {
        "characters_deduped": dedupe_characters(con),
        "clean_characters": clean_characters(con),
        "species_summary": species_summary(con),
        "episode_appearances": episode_appearances(con),
    }
