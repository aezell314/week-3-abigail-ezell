"""Ingest data from a real HTTP API and land it raw in S3.

Source: the Rick and Morty API — https://rickandmortyapi.com/documentation/
  GET /character?page=N  ->  {"info": {"count", "pages", "next", "prev"},
                              "results": [ {id, name, status, species, ...}, ... ]}
  The response is paginated 20 per page; ``info.next`` is the URL of the next
  page, or null on the last page.

Docs:
  - httpx client:     https://www.python-httpx.org/api/#client
  - httpx params:     https://www.python-httpx.org/quickstart/#passing-parameters-in-urls
  - tenacity:         https://tenacity.readthedocs.io/en/latest/  (see @retry, wait, stop)
  - boto3 put_object: https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html
"""

from __future__ import annotations

import json
from email.utils import parsedate_to_datetime

import httpx
from botocore.exceptions import ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from de_pipeline.config import get_s3_client, settings

USER_AGENT = "nss-intro-to-de/week-3"
DEFAULT_TIMEOUT = httpx.Timeout(10.0)
MAX_ATTEMPTS = 5


class RateLimitError(Exception):
    """Raised when the API answers 429 (Too Many Requests). Provided for you.

    It carries the server's ``Retry-After`` value (seconds) when one was sent, so
    your retry logic can wait exactly as long as the server asked instead of
    guessing.
    """

    def __init__(self, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(f"rate limited (retry_after={retry_after})")


# --------------------------------------------------------------------------- #
# Day 1 — build a client and make the first call
# --------------------------------------------------------------------------- #


def build_client(
    *,
    base_url: str | None = settings.api_base_url,
    token: str | None = settings.api_token,
    transport: httpx.BaseTransport | None = None,
) -> httpx.Client:
    """Return an ``httpx.Client`` pointed at the API.

    - Default ``base_url`` to ``settings.api_base_url`` and ``token`` to
      ``settings.api_token`` (import them from ``de_pipeline.config``).
    - Always send a ``User-Agent`` and ``Accept: application/json``.
    - Auth: if (and only if) a token is set, add
      ``Authorization: Bearer <token>`` — a common API auth pattern.
      Leave it blank for the public API; wiring it is the point, and the tests
      check you send it (and that you DON'T when there's no token).
    - Use ``DEFAULT_TIMEOUT``.

    Pass ``transport`` straight through to ``httpx.Client(transport=...)``. You
    won't use it in normal runs — it's the injection point the tests use to drive
    your client with a fake transport instead of the network.
    """
    client = httpx.Client()
    headers = {"User-Agent": "Week3App/1.0.0", "Accept": "application/json"}
    if token:
      headers["authorization"] = f"Bearer {token}"
    client = httpx.Client(headers=headers, timeout=DEFAULT_TIMEOUT, transport=transport)
    return client

def custom_wait_strategy(retry_state):
    """
    Honors the Retry-After header if present in RateLimitError;
    otherwise, falls back to exponential backoff.
    """
    # Check if the last failed attempt threw a RateLimitError
    if retry_state.outcome.failed:
        exception = retry_state.outcome.exception()
        if isinstance(exception, RateLimitError) and exception.retry_after:
            try:
                # Handle numeric seconds (e.g., "5" or "0")
                return float(exception.retry_after)
            except ValueError:
                # Handle HTTP-date headers (e.g., "Wed, 21 Oct 2015 07:28:00 GMT")
                try:
                    target_time = parsedate_to_datetime(exception.retry_after)
                    delay = (target_time - target_time.now(target_time.tzinfo)).total_seconds()
                    return max(0.0, delay)
                except Exception:
                    # Malformed header fallback
                    pass

    # Fallback to standard exponential backoff: 1s, 2s, 4s, ... capped at 30s
    fallback = wait_exponential(multiplier=1, min=1, max=30)
    return fallback(retry_state=retry_state)

@retry(
    retry=retry_if_exception_type((RateLimitError, httpx.TransportError)),
    stop=stop_after_attempt(MAX_ATTEMPTS),
    wait=custom_wait_strategy,
    reraise=True
)
def fetch_page(page: int = 1, *, client: httpx.Client | None = None) -> dict:
    """Fetch one page of characters and return the parsed JSON dict.

    Day 1 (happy path): GET ``/character?page=<page>`` (pass ``page`` as a query
    param, don't build the string yourself), raise on HTTP errors, return
    ``response.json()``. If ``client`` is None, build one with ``build_client()``
    (and close it when you're done).

    Day 2 (make it resilient): a 429 means "slow down", not "fail". When you see
    ``response.status_code == 429``, read the ``Retry-After`` header and raise
    ``RateLimitError(...)`` instead of returning — then wrap this function with a
    tenacity ``@retry`` so it waits and tries again. Retry on ``RateLimitError``
    and ``httpx.TransportError``; stop after ``MAX_ATTEMPTS``; for the wait, honor
    ``Retry-After`` when present and otherwise back off exponentially (1s, 2s,
    4s, ... capped at 30s). Support numeric seconds (including zero); HTTP-date
    headers and malformed values are optional extensions.
    Tip: a custom ``wait`` callable receives the retry state, so
    it can pull ``retry_after`` off the raised ``RateLimitError``.
    """
    should_close = False
    if client is None:
        client = build_client()
        should_close = True

    try:
        params = {"page": page}
        response = client.get(
            url=settings.api_base_url + "/character",
            params=params
        )

        # Handle 429 Rate Limits before raise_for_status
        if response.status_code == 429:
            raise RateLimitError(response.headers.get("Retry-After"))

        response.raise_for_status()
        return response.json()

    finally:
        if should_close:
            client.close()


# --------------------------------------------------------------------------- #
# Day 2 — pagination: walk every page
# --------------------------------------------------------------------------- #

def fetch_all_characters(*, client: httpx.Client | None = None) -> list[dict]:
    """Fetch EVERY character by walking the pages until there are no more.

    Start at page 1; after each page, ``info.next`` tells you whether another
    page exists (it's the next page's URL, or null on the last page). Collect all
    the ``results`` into one list and return it. Build ONE client and reuse it
    across pages (pass it into ``fetch_page``) so you're not paying connection
    setup on every request. Close clients you create; leave supplied clients open.
    """
    should_close = client is None
    if client is None:
        client = build_client()

    try:
        all_characters = []
        page = 1

        while True:
            data = fetch_page(page, client=client)

            all_characters.extend(data["results"])

            if not data["info"]["next"]:
                break

            page += 1

        return all_characters

    finally:
        if should_close:
            client.close()

# --------------------------------------------------------------------------- #
# Day 1/2 — land the raw response in S3 (the "land raw" principle)
# --------------------------------------------------------------------------- #


def land_to_s3(records: list[dict],
                *,
                s3_client=None,
                key: str | None = settings.characters_key) -> int:
    """Upload ``records`` to S3 as one JSON array, untransformed; return the count.

    This is the Week 1 S3 move in reverse: instead of downloading, you upload the
    raw API payload to ``s3://<bucket>/<characters_key>`` so the downstream
    pipeline can read it like any other landed file.

      - default ``s3_client`` to ``config.get_s3_client()`` and ``key`` to
        ``settings.characters_key``;
      - make sure the bucket exists (head it, create it on failure — like the
        Week 1-2 seed script);
      - ``json.dumps`` the records, encode to bytes, and ``put_object`` them.

    Land it RAW — don't clean or reshape here; that's transform.py's job.
    """
    if s3_client is None:
        s3_client = get_s3_client()

    if key is None:
        key = settings.characters_key

    bucket = settings.bucket

    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError:
        s3_client.create_bucket(Bucket=bucket)
        print(f"created bucket '{bucket}'")

    json_string = json.dumps(records).encode('utf-8')
    s3_client.put_object(
      Bucket=bucket,
      Key=key,
      Body=json_string
    )
    return len(records)


def ingest(*, client: httpx.Client | None = None, s3_client=None) -> int:
    """The capstone: fetch all characters from the API and land them raw in S3.

    This is "API-fetch-to-S3" — the one new stage at the front of the pipeline.
    Call ``fetch_all_characters`` then ``land_to_s3``; return how many landed.
    """
    fullchars = fetch_all_characters(client=client)
    return land_to_s3(fullchars, s3_client=s3_client)


if __name__ == "__main__":
    # Quick manual check (once Day 1 is done):  uv run python -m de_pipeline.api
    first = fetch_page(1)
    info = first["info"]
    print(f"page 1: {len(first['results'])} of {info['count']} characters, {info['pages']} pages")
    print("first character:", first["results"][0]["name"], "/", first["results"][0]["status"])
