"""Fetch checkpoint — talks to the real S3 (RustFS) store.

Skipped automatically if RustFS isn't running, so the rest of the suite still
works offline. To run it: `docker compose up -d`, then ingest once (or run the
full pipeline), then `uv run pytest`. Covers the PROVIDED ``fetch.py``.
"""

from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from botocore.config import Config

from de_pipeline import fetch
from de_pipeline.config import settings


def _s3_available() -> bool:
    client = boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name=settings.region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=1,
            read_timeout=1,
            retries={"max_attempts": 1},
        ),
    )
    try:
        # Reachable AND the raw file has actually been landed (i.e. you've run
        # an ingest). Otherwise there's nothing to fetch yet — skip, don't fail.
        client.head_bucket(Bucket=settings.bucket)
        client.head_object(Bucket=settings.bucket, Key=settings.characters_key)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _s3_available(),
    reason="RustFS not reachable or nothing landed yet — `docker compose up -d` then ingest",
)


def test_fetch_all_downloads_the_landed_file(tmp_path: Path) -> None:
    paths = fetch.fetch_all(dest_dir=tmp_path)

    assert set(paths) == {"characters"}
    assert paths["characters"].exists()
    # A real ingest lands a non-trivial file.
    assert paths["characters"].stat().st_size > 1000
