"""Stage 2 — fetch the raw landed file from S3.

PROVIDED — carried over from Weeks 1-2, study it but you don't rewrite it. This
is the part of the pipeline that stays exactly the same when the *source* changes
from a file drop to an API: once ``api.ingest()`` has landed ``characters.json``
in S3, this downloads it into ``data/raw/`` like always.
"""

from __future__ import annotations

from pathlib import Path

from de_pipeline.config import get_s3_client, settings

# Where downloaded raw files land. (data/ is git-ignored.)
RAW_DIR = Path("data/raw")


def fetch_object(key: str, dest_dir: Path = RAW_DIR) -> Path:
    """Download the object ``key`` from the bucket into ``dest_dir``; return the
    local path it was written to."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / key
    client = get_s3_client()
    client.download_file(settings.bucket, key, str(dest))
    return dest


def fetch_all(dest_dir: Path = RAW_DIR) -> dict[str, Path]:
    """Download the landed source file(s); return ``{"characters": <path>}``."""
    return {
        "characters": fetch_object(settings.characters_key, dest_dir),
    }


if __name__ == "__main__":
    # uv run python -m de_pipeline.fetch
    paths = fetch_all()
    for name, path in paths.items():
        print(f"fetched {name} -> {path} ({path.stat().st_size:,} bytes)")
