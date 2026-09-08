"""Days 1-2 — the real work: ingest from a real HTTP API and land it raw in S3.

Until now the data simply *appeared* in S3. This week you go get it yourself,
from a live API, and meet the ugly realities that come with that: auth,
pagination, and rate limits. The destination is unchanged — you land the raw API
records into S3 (the "land raw" principle), and the rest of the pipeline
(fetch -> load -> transform) reads from S3 exactly as before.

Source: the Rick and Morty API — https://rickandmortyapi.com/documentation/
  GET /character?page=N  ->  {"info": {"count", "pages", "next", "prev"},
                              "results": [ {id, name, status, species, ...}, ... ]}
  The response is paginated 20 per page; ``info.next`` is the URL of the next
  page, or null on the last page.

THE ARC OF THE WEEK
  Day 1  First API call. Build an httpx client (with auth wired the way real
         APIs need), fetch ONE page, look at the raw shape, and land raw to S3.
  Day 2  Pagination + rate limits. Walk every page until ``info.next`` is null,
         and make the request resilient: retry on 429 with tenacity, honoring
         the server's ``Retry-After`` header, with exponential backoff otherwise.
  Day 3  Wire it into pipeline.py, clean everything with ruff.

You implement every function below. ``RateLimitError`` is provided to get you
started — it's the exception you'll raise on a 429 and retry around.

Docs:
  - httpx client:     https://www.python-httpx.org/api/#client
  - httpx params:     https://www.python-httpx.org/quickstart/#passing-parameters-in-urls
  - tenacity:         https://tenacity.readthedocs.io/en/latest/  (see @retry, wait, stop)
  - boto3 put_object: https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html
"""

from __future__ import annotations

import httpx

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
    base_url: str | None = None,
    token: str | None = None,
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
    raise NotImplementedError("Day 1: build the httpx client (with auth wired)")


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
    raise NotImplementedError("Day 1: GET one page; Day 2: add 429 retry/backoff")


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
    raise NotImplementedError("Day 2: paginate until info.next is null")


# --------------------------------------------------------------------------- #
# Day 1/2 — land the raw response in S3 (the "land raw" principle)
# --------------------------------------------------------------------------- #


def land_to_s3(records: list[dict], *, s3_client=None, key: str | None = None) -> int:
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
    raise NotImplementedError("Day 1/2: put the raw JSON array to S3")


def ingest(*, client: httpx.Client | None = None, s3_client=None) -> int:
    """The capstone: fetch all characters from the API and land them raw in S3.

    This is "API-fetch-to-S3" — the one new stage at the front of the pipeline.
    Call ``fetch_all_characters`` then ``land_to_s3``; return how many landed.
    """
    raise NotImplementedError("Day 2: fetch_all_characters -> land_to_s3")


if __name__ == "__main__":
    # Quick manual check (once Day 1 is done):  uv run python -m de_pipeline.api
    first = fetch_page(1)
    info = first["info"]
    print(f"page 1: {len(first['results'])} of {info['count']} characters, {info['pages']} pages")
    print("first character:", first["results"][0]["name"], "/", first["results"][0]["status"])
