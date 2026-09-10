"""Connects the api, fetch, load, and transform modules into one end-to-end pipeline.
"""

from __future__ import annotations

# The stages you'll orchestrate. `api` is the new one you wrote this week; the
# rest are provided (carried from Weeks 1-2).
from de_pipeline import api, fetch, load, transform  # noqa: F401


def main() -> None:
    """Runs the full pipeline end to end: ingests the character data from the API,
    lands to S3 bucket, fetches the raw data from S3, opens a DuckDB
    connection, loads the raw tables, runs the transforms, and prints a summary."""
    print("1. Ingesting data from API and uploading to S3...")
    totalchars = api.ingest()
    print(f"Ingested {totalchars} total characters from the API.")

    print("2. Fetching raw files...")
    paths = fetch.fetch_all()

    for name, path in paths.items():
        print(f"Successfully downloaded {name} to {path}")

    print("3. Loading raw files into a DuckDB warehouse...")
    con = load.connect()
    loads = load.load_all(con)
    for name, rows in loads.items():
        print(f"Successfully loaded table \'{name}\' with {rows} rows")

    print("4. Cleaning and aggregating raw data using DuckDB and Polars...")
    transforms = transform.run_transforms(con)
    for name, rows in transforms.items():
        print(f"Successfully loaded table \'{name}\' with {rows} rows")

    con.close()

if __name__ == "__main__":
    main()
