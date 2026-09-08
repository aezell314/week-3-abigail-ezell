"""Shared pytest fixtures.

These build a tiny, local sample of the API's character data so the load and
transform tests run WITHOUT the network or S3. The sample mirrors the real shape
(nested ``origin`` / ``location`` structs, an ``episode`` list, "Alive"/"Dead"/
"unknown" statuses) and includes the kinds of mess the transforms must survive.

Trace it once and every number in the tests makes sense:

  characters (6 raw rows -> 5 distinct ids):
    id 1  Rick    Human   Alive    loc Citadel       episodes [1,2,3]
    id 2  Morty   Human   Alive    loc Earth         episodes [1,2]
    id 3  Bird    Alien   Dead     loc Bird World    episodes [1]      <- appears TWICE
    id 3  Bird    Alien   Dead     loc Bird World    episodes [1]      <- dup (pagination drift)
    id 4  Summer  Human   Alive    loc Earth         episodes [2,3]
    id 5  Meeseek Alien   ""       loc unknown       episodes [2]      <- blank status -> unknown

  After dedup: 5 characters.
  Species:  Human = {1, 2, 4} (all alive, locations {Citadel, Earth});
            Alien = {3, 5}    (0 alive, locations {Bird World, unknown}).
  Episode appearances (after dedup by id):
            ep 1 -> {1, 2, 3} = 3;  ep 2 -> {1, 2, 4, 5} = 4;  ep 3 -> {1, 4} = 2.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

EP = "https://rickandmortyapi.com/api/episode"


def _character(cid, name, status, species, gender, origin, location, created, episodes):
    return {
        "id": cid,
        "name": name,
        "status": status,
        "species": species,
        "type": "",
        "gender": gender,
        "origin": {"name": origin, "url": ""},
        "location": {"name": location, "url": ""},
        "image": "",
        "episode": [f"{EP}/{n}" for n in episodes],
        "url": f"https://rickandmortyapi.com/api/character/{cid}",
        "created": created,
    }


SAMPLE_CHARACTERS = [
    _character(1, "Rick Sanchez", "Alive", "Human", "Male",
               "Earth (C-137)", "Citadel of Ricks", "2017-11-04T18:48:46.250Z", [1, 2, 3]),
    _character(2, "Morty Smith", "Alive", "Human", "Male",
               "Earth (C-137)", "Earth (Replacement)", "2017-11-04T18:50:21.651Z", [1, 2]),
    _character(3, "Birdperson", "Dead", "Alien", "Male",
               "Bird World", "Bird World", "2017-11-05T09:00:00.000Z", [1]),
    # Same id 3 again — the API handed back a duplicate while we were paging.
    _character(3, "Birdperson", "Dead", "Alien", "Male",
               "Bird World", "Bird World", "2017-11-05T09:00:00.000Z", [1]),
    _character(4, "Summer Smith", "Alive", "Human", "Female",
               "Earth (Replacement)", "Earth (Replacement)", "2017-11-05T10:00:00.000Z", [2, 3]),
    # Blank status -> normalized to 'unknown'; unknown origin/location.
    _character(5, "Mr. Meeseeks", "", "Alien", "Male",
               "unknown", "unknown", "2017-11-06T11:00:00.000Z", [2]),
]


@pytest.fixture
def raw_dir(tmp_path: Path) -> Path:
    """A temp directory holding a tiny characters.json fixture."""
    (tmp_path / "characters.json").write_text(json.dumps(SAMPLE_CHARACTERS, indent=2))
    return tmp_path


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB connection."""
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture
def loaded_con(con: duckdb.DuckDBPyConnection, raw_dir: Path) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with ``raw_characters`` populated from the sample,
    duplicates and all — so the transform tests run without finishing the
    pipeline. Lands exactly like the real raw load (structs + list intact)."""
    con.execute(
        "CREATE TABLE raw_characters AS SELECT * FROM read_json_auto(?)",
        [str(raw_dir / "characters.json")],
    )
    return con
