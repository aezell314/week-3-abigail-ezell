"""Central configuration, read from environment variables (or a local .env).

Provided for you — you should not need to change it. New this week: the API
settings (``api_base_url`` / ``api_token``) alongside the same S3 settings and
client you've used since Week 1.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from dotenv import load_dotenv

# Load variables from a .env file in the project root, if present. Real
# environment variables always win over .env values.
load_dotenv()


@dataclass(frozen=True)
class Settings:
    # The source API.
    api_base_url: str
    api_token: str
    # The S3 raw landing zone.
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str
    bucket: str
    characters_key: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Read settings from the environment once and cache them."""
    return Settings(
        api_base_url=os.getenv("API_BASE_URL", "https://rickandmortyapi.com/api"),
        api_token=os.getenv("API_TOKEN", ""),
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        access_key=os.getenv("S3_ACCESS_KEY", "rustfsadmin"),
        secret_key=os.getenv("S3_SECRET_KEY", "rustfsadmin"),
        region=os.getenv("S3_REGION", "us-east-1"),
        bucket=os.getenv("S3_BUCKET", "raw"),
        characters_key=os.getenv("CHARACTERS_KEY", "characters.json"),
    )


def get_s3_client() -> BaseClient:
    """Return a boto3 S3 client configured for the local RustFS endpoint."""
    s = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=s.endpoint_url,
        aws_access_key_id=s.access_key,
        aws_secret_access_key=s.secret_key,
        region_name=s.region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=5,
            read_timeout=10,
            retries={"max_attempts": 2, "mode": "standard"},
        ),
    )


# Convenience singleton so callers can simply `from ... import settings`.
settings = get_settings()
