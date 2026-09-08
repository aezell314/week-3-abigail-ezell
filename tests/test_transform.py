"""Transform checkpoints — no S3 required (uses the ``loaded_con`` fixture).

These cover the PROVIDED ``transform.py``; they're green out of the box. Read
them as a guided tour of the Week 2 toolkit applied to API data — and trace the
sample in conftest.py once so the numbers make sense.
"""

from __future__ import annotations

import duckdb

from de_pipeline import transform


def test_dedupe_characters_keeps_one_row_per_id(loaded_con: duckdb.DuckDBPyConnection) -> None:
    rows = transform.dedupe_characters(loaded_con)
    assert rows == 5  # 6 raw rows -> 5 distinct ids (id 3 was duplicated)

    dupes = loaded_con.execute(
        "SELECT id, count(*) FROM characters_deduped GROUP BY id HAVING count(*) > 1"
    ).fetchall()
    assert dupes == []


def test_clean_characters_normalizes_flattens_and_parses(
    loaded_con: duckdb.DuckDBPyConnection,
) -> None:
    transform.dedupe_characters(loaded_con)
    rows = transform.clean_characters(loaded_con)
    assert rows == 5

    # status normalized: lower-cased, and the blank one became 'unknown'.
    statuses = {
        r[0] for r in loaded_con.execute("SELECT DISTINCT status FROM clean_characters").fetchall()
    }
    assert statuses == {"alive", "dead", "unknown"}

    # nested origin/location flattened to plain columns.
    origin_1 = loaded_con.execute(
        "SELECT origin_name FROM clean_characters WHERE id = 1"
    ).fetchone()[0]
    assert origin_1 == "Earth (C-137)"

    # created (ISO text) became a real DATE, none NULL.
    null_dates = loaded_con.execute(
        "SELECT count(*) FROM clean_characters WHERE created_date IS NULL"
    ).fetchone()[0]
    assert null_dates == 0

    # episode_count derived from the list length.
    epc_1 = loaded_con.execute(
        "SELECT episode_count FROM clean_characters WHERE id = 1"
    ).fetchone()[0]
    assert epc_1 == 3


def test_species_summary_aggregates_per_species(loaded_con: duckdb.DuckDBPyConnection) -> None:
    transform.dedupe_characters(loaded_con)
    transform.clean_characters(loaded_con)
    rows = transform.species_summary(loaded_con)
    assert rows == 2  # Human and Alien

    human = loaded_con.execute(
        "SELECT character_count, alive_count, location_count "
        "FROM species_summary WHERE species = 'Human'"
    ).fetchone()
    assert human == (3, 3, 2)  # 3 humans, all alive, across {Citadel, Earth}

    alien = loaded_con.execute(
        "SELECT character_count, alive_count FROM species_summary WHERE species = 'Alien'"
    ).fetchone()
    assert alien == (2, 0)  # 2 aliens, none 'alive' (one dead, one unknown)


def test_species_summary_min_count_is_a_bound_param(loaded_con: duckdb.DuckDBPyConnection) -> None:
    transform.dedupe_characters(loaded_con)
    transform.clean_characters(loaded_con)
    # Only species with >= 3 characters — just Human qualifies. The threshold must
    # reach SQL as a BOUND parameter ($min_count), not an f-string.
    rows = transform.species_summary(loaded_con, min_count=3)
    assert rows == 1
    only = loaded_con.execute("SELECT species FROM species_summary").fetchone()[0]
    assert only == "Human"


def test_episode_appearances_explodes_in_polars(loaded_con: duckdb.DuckDBPyConnection) -> None:
    rows = transform.episode_appearances(loaded_con)
    assert rows == 3  # episodes 1, 2, 3

    counts = dict(
        loaded_con.execute(
            "SELECT episode_id, appearance_count FROM episode_appearances"
        ).fetchall()
    )
    # ep 1 -> {1,2,3}; ep 2 -> {1,2,4,5}; ep 3 -> {1,4}  (after dedup by id)
    assert counts == {1: 3, 2: 4, 3: 2}


def test_run_transforms_returns_all_counts(loaded_con: duckdb.DuckDBPyConnection) -> None:
    result = transform.run_transforms(loaded_con)
    assert result == {
        "characters_deduped": 5,
        "clean_characters": 5,
        "species_summary": 2,
        "episode_appearances": 3,
    }
