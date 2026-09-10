"""Stage 2 of the pipeline — fetch the raw landed file from S3.
"""

from __future__ import annotations

from pathlib import Path

from de_pipeline.config import get_s3_client, settings

# Where downloaded raw files land. (data/ is git-ignored.)
RAW_DIR = Path("data/raw")


def fetch_object(key: str, dest_dir: Path = RAW_DIR) -> Path:
    """Downloads the object ``key`` from the bucket into ``dest_dir``; returns the
    local path it was written to."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / key
    client = get_s3_client()
    client.download_file(settings.bucket, key, str(dest))
    return dest


def fetch_all(dest_dir: Path = RAW_DIR) -> dict[str, Path]:
    """Downloads the landed source file(s); returns ``{"characters": <path>}``."""
    return {
        "characters": fetch_object(settings.characters_key, dest_dir),
    }


if __name__ == "__main__":
    # uv run python -m de_pipeline.fetch
    paths = fetch_all()
    for name, path in paths.items():
        print(f"fetched {name} -> {path} ({path.stat().st_size:,} bytes)")
