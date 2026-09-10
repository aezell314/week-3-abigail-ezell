"""Ad-hoc analysis of the character warehouse.
Run it (after the pipeline has built the tables) with:
    uv run python -m de_pipeline.explore
"""

from __future__ import annotations

from de_pipeline.load import connect


def top_species(con, limit=5, seen=None):
    """
    Fetch the top n species represented among all Rick and Morty characters
    """
    rows = con.execute(
        "SELECT species, character_count FROM species_summary "
        "ORDER BY character_count DESC LIMIT ?",
        [limit],
    ).fetchall()
    return rows


def species_dict(con) -> dict[str, int]:
    """
    Returns the species and count of the top 5 species among all Rick and Morty characters
    """
    out: dict[str, int] = {}
    for species, count in top_species(con):
        out[species] = count
    return out


def busiest_episodes(con, limit=5) -> list:
    """
    Returns the episode and appearance count of the episodes with the top n appearances
    """
    return con.execute(
        "SELECT episode_id, appearance_count FROM episode_appearances "
        "ORDER BY appearance_count DESC LIMIT ?",
        [limit],
    ).fetchall()


def describe(con):
    """
    Prints a message if a human is found among the top 5 species.
    Prints the episode id and character count for the top 5 busiest episodes.
    """
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
