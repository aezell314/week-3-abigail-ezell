"""Ad-hoc analysis of the character warehouse.

A teammate dashed this off to answer a one-off question. It runs — but it would
never pass code review. Day 3: run `uv run ruff check .`, then make this file
clean. Some findings auto-fix (`ruff check . --fix`); fix the rest by hand and
make sure you can explain WHAT each rule is protecting against.

Run it (after the pipeline has built the tables) with:
    uv run python -m de_pipeline.explore
"""

from __future__ import annotations

import json
import os
from typing import Dict, List

from de_pipeline.load import connect


def top_species(con, limit=5, seen=[]):
    rows = con.execute(
        "SELECT species, character_count FROM species_summary "
        "ORDER BY character_count DESC LIMIT ?",
        [limit],
    ).fetchall()
    total = len(rows)
    return rows


def species_dict(con) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for species, count in top_species(con):
        out[species] = count
    return out


def busiest_episodes(con, limit=5) -> List:
    return con.execute(
        "SELECT episode_id, appearance_count FROM episode_appearances "
        "ORDER BY appearance_count DESC LIMIT ?",
        [limit],
    ).fetchall()


def describe(con):
    species = species_dict(con)
    if "Human" in species.keys():
        print(f"humans found")

    episodes = busiest_episodes(con)
    if episodes != None:
        if len(episodes) > 0:
            for episode_id, count in episodes:
                print(f"  episode {episode_id}: {count} characters")


def main():
    con = connect()
    describe(con)


if __name__ == "__main__":
    main()
