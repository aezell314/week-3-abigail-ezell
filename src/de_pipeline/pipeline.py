"""Day 3 — wire the whole thing into one end-to-end run.

This is the entry point for ``uv run de-pipeline``. Week 3 adds ONE stage at the
front — ingest from the API into S3 — and the rest is the pipeline you already
know:

    ingest (API -> S3) -> fetch (S3 -> local) -> load (-> DuckDB) -> transform

Print a short, human-readable summary so a person can see what happened
(how many characters landed, the row counts per table, a headline number or two).
"""

from __future__ import annotations

# The stages you'll orchestrate. `api` is the new one you wrote this week; the
# rest are provided (carried from Weeks 1-2).
from de_pipeline import api, fetch, load, transform  # noqa: F401


def main() -> None:
    """Run the full pipeline end to end and print a summary."""
    raise NotImplementedError("Day 3: orchestrate ingest -> fetch -> load -> transform")


if __name__ == "__main__":
    main()
