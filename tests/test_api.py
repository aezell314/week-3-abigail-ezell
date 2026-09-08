"""Day 1-2 checkpoints for the API ingest layer — the heart of Week 3.

These run OFFLINE. Instead of hitting the real network, they drive your httpx
client with ``httpx.MockTransport``: a fake transport that answers requests from
canned responses you control. That lets us simulate pagination and a 429 rate
limit deterministically — no waiting, no luck, no real API hammering. (This is
also how you'd test API code in a real job.)
"""

from __future__ import annotations

import json

import httpx
import pytest
from tenacity import RetryError

from de_pipeline import api
from de_pipeline.config import settings

# --------------------------------------------------------------------------- #
# Test doubles: three canned pages, and a fake S3 client.
# --------------------------------------------------------------------------- #


def _page(results, next_url):
    return {
        "info": {"count": 5, "pages": 3, "next": next_url, "prev": None},
        "results": results,
    }


# 3 pages, 5 characters total; the last page's ``info.next`` is null.
PAGES = [
    _page([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}], "https://api.test/character?page=2"),
    _page([{"id": 3, "name": "C"}, {"id": 4, "name": "D"}], "https://api.test/character?page=3"),
    _page([{"id": 5, "name": "E"}], None),
]


def pages_client(pages=PAGES) -> httpx.Client:
    """A client whose fake transport serves ``pages`` by the ``?page=`` param."""
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.rstrip("/").endswith("/character")
        page = int(request.url.params["page"])
        return httpx.Response(200, json=pages[page - 1])

    return api.build_client(transport=httpx.MockTransport(handler))


def flaky_client(retry_after="0", fail_times=1):
    """Like ``pages_client`` but page 1 first answers 429 ``fail_times`` times
    (with a ``Retry-After`` header) before succeeding. Returns (client, state)."""
    state = {"fails": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.rstrip("/").endswith("/character")
        page = int(request.url.params["page"])
        if page == 1 and state["fails"] < fail_times:
            state["fails"] += 1
            return httpx.Response(
                429, headers={"Retry-After": retry_after}, json={"error": "slow down"}
            )
        return httpx.Response(200, json=PAGES[page - 1])

    return api.build_client(transport=httpx.MockTransport(handler)), state


class FakeS3:
    """A stand-in for the boto3 S3 client that records ``put_object`` calls."""

    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def head_bucket(self, Bucket):  # noqa: N803 (boto3's capitalized kwargs)
        return {}

    def create_bucket(self, Bucket):  # noqa: N803
        return {}

    def put_object(self, Bucket, Key, Body):  # noqa: N803
        self.objects[(Bucket, Key)] = Body


# --------------------------------------------------------------------------- #
# Day 1 — build a client, make the first call, land it raw
# --------------------------------------------------------------------------- #


def test_build_client_sends_auth_header_when_token_set() -> None:
    client = api.build_client(token="secret123")
    # httpx header access is case-insensitive.
    assert client.headers.get("Authorization") == "Bearer secret123"


def test_build_client_omits_auth_header_without_token() -> None:
    client = api.build_client(token="")
    assert "authorization" not in client.headers


def test_fetch_page_returns_results_and_info() -> None:
    data = api.fetch_page(1, client=pages_client())
    assert data["info"]["count"] == 5
    assert [r["id"] for r in data["results"]] == [1, 2]


def test_land_to_s3_uploads_one_json_object() -> None:
    fake = FakeS3()
    count = api.land_to_s3([{"id": 1}, {"id": 2}], s3_client=fake)
    assert count == 2

    key = (settings.bucket, settings.characters_key)
    assert key in fake.objects  # it landed at the configured bucket/key
    # ...as one raw JSON array, untouched.
    assert json.loads(fake.objects[key]) == [{"id": 1}, {"id": 2}]


# --------------------------------------------------------------------------- #
# Day 2 — pagination + rate limits
# --------------------------------------------------------------------------- #


def test_fetch_all_walks_every_page() -> None:
    results = api.fetch_all_characters(client=pages_client())
    # All three pages, in order, until info.next was null.
    assert [r["id"] for r in results] == [1, 2, 3, 4, 5]


def test_fetch_all_retries_on_429_then_succeeds(monkeypatch) -> None:
    # Don't actually sleep during the test.
    monkeypatch.setattr("time.sleep", lambda _s: None)
    client, state = flaky_client(retry_after="0", fail_times=2)

    results = api.fetch_all_characters(client=client)

    assert [r["id"] for r in results] == [1, 2, 3, 4, 5]
    assert state["fails"] == 2  # it really did get throttled twice and recover


def test_retry_after_header_is_honored(monkeypatch) -> None:
    # Capture how long the retry waits instead of really sleeping.
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", lambda s: sleeps.append(s))
    client, _state = flaky_client(retry_after="3", fail_times=1)

    api.fetch_all_characters(client=client)

    # It waited the 3 seconds the server asked for — not a guessed interval.
    assert 3.0 in sleeps


def test_ingest_fetches_all_and_lands() -> None:
    fake = FakeS3()
    count = api.ingest(client=pages_client(), s3_client=fake)

    assert count == 5
    key = (settings.bucket, settings.characters_key)
    assert key in fake.objects
    assert len(json.loads(fake.objects[key])) == 5


def test_fetch_page_retries_transport_error_with_exponential_backoff(monkeypatch):
    sleeps = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    attempts = []

    def handler(request):
        attempts.append(request)
        if len(attempts) <= 3:
            raise httpx.ConnectError("temporary failure", request=request)
        return httpx.Response(200, json=PAGES[0])

    with api.build_client(transport=httpx.MockTransport(handler)) as client:
        assert api.fetch_page(1, client=client) == PAGES[0]
    assert len(attempts) == 4
    assert sleeps == [1, 2, 4]


def test_fetch_page_backs_off_without_retry_after_and_stops(monkeypatch):
    sleeps = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    attempts = []

    def handler(request):
        attempts.append(request)
        return httpx.Response(429, json={"error": "slow down"})

    with (
        api.build_client(transport=httpx.MockTransport(handler)) as client,
        pytest.raises((api.RateLimitError, RetryError)),
    ):
        api.fetch_page(1, client=client)
    assert len(attempts) == api.MAX_ATTEMPTS
    assert sleeps == [min(2**n, 30) for n in range(api.MAX_ATTEMPTS - 1)]


@pytest.mark.parametrize("status", [401, 404, 500])
def test_fetch_page_does_not_retry_other_http_errors(monkeypatch, status):
    sleeps = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    attempts = []

    def handler(request):
        attempts.append(request)
        return httpx.Response(status, json={"error": "request failed"})

    with (
        api.build_client(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        api.fetch_page(1, client=client)
    assert len(attempts) == 1
    assert sleeps == []


def test_fetch_all_stops_when_next_is_null():
    # Stale metadata must not send us past the explicit end of pagination.
    pages = [_page([{"id": 1}], None)]
    with pages_client(pages) as client:
        assert api.fetch_all_characters(client=client) == [{"id": 1}]
        assert not client.is_closed


def test_zero_retry_after_does_not_fall_back_to_backoff(monkeypatch):
    sleeps = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    client, _state = flaky_client(retry_after="0")
    with client:
        api.fetch_page(1, client=client)
    assert all(wait == 0 for wait in sleeps)
