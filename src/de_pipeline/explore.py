"""Ad-hoc analysis of the character warehouse.
Run it (after the pipeline has built the tables) with:
    uv run python -m de_pipeline.explore
"""

from __future__ import annotations

from de_pipeline.load import connect


def top_species(con, limit=5, seen=None):
    rows = con.execute(
        "SELECT species, character_count FROM species_summary "
        "ORDER BY character_count DESC LIMIT ?",
        [limit],
    ).fetchall()
    return rows


def species_dict(con) -> dict[str, int]:
    out: dict[str, int] = {}
    for species, count in top_species(con):
        out[species] = count
    return out


def busiest_episodes(con, limit=5) -> list:
    return con.execute(
        "SELECT episode_id, appearance_count FROM episode_appearances "
        "ORDER BY appearance_count DESC LIMIT ?",
        [limit],
    ).fetchall()


def describe(con):
    species = species_dict(con)
    if "Human" in species:
        print("humans found")

    episodes = busiest_episodes(con)
    if episodes is not None and len(episodes) > 0:
        for episode_id, count in episodes:
            print(f"  episode {episode_id}: {count} characters")


def main():
    con = connect()
    describe(con)


if __name__ == "__main__":
    main()
