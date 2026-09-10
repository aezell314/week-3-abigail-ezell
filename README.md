# Week 3 --- API Ingestion Pipeline

## Project overview

This project extends the existing ELT pipeline from [week 2](https://github.com/aezell314/week-2-abigail-ezell) with a live HTTP
ingestion stage. Instead of beginning with data that has already been placed in S3, the pipeline now retrieves character data directly from the [Rick and Morty API](https://rickandmortyapi.com/documentation), handles the operational concerns that come with API ingestion, lands the unmodified records in S3-compatible object storage, and then passes them through the existing DuckDB and Polars transformation workflow.

The completed pipeline handles:

-   HTTP client configuration and optional bearer-token authentication
-   Multi-page API pagination
-   `429 Too Many Requests` responses and `Retry-After`
-   Exponential retry behavior for transient failures
-   Raw JSON landing in S3-compatible storage
-   Loading raw character records into DuckDB
-   Deduplication, cleaning, summarization, and list expansion
-   End-to-end pipeline orchestration
-   Linting and automated test coverage

## Architecture

``` text
        Rick & Morty API                 S3 / RustFS              DuckDB
 ┌──────────────────────────┐          ┌──────────────┐        ┌───────────────┐
 │ GET /character?page=1    │  land    │ characters  │ fetch  │ raw_characters│
 │ GET /character?page=2    │ ───────► │ .json (raw) │ ─────► │       ↓       │
 │ ...                      │          └──────────────┘        │ clean/summary │
 │ 429 → back off & retry   │                                  │ episodes      │
 │ until info.next = null   │                                  │ (Polars)      │
 └──────────────────────────┘                                  └───────────────┘
        httpx · tenacity
```

The `api.py` module represents the HTTP ingestion stage. It retrieves all character
pages, applies authentication when configured, handles retryable
failures, and writes the combined raw records to object storage. From
that point forward, the existing S3, DuckDB, and Polars components take
over.

## Repository structure

  -------------------------------------------------------------------------
  Path                                Purpose
  ----------------------------------- -------------------------------------
  `docker-compose.yml`                Runs local S3-compatible
                                      [RustFS](https://rustfs.com/) as the
                                      raw landing zone.

  `src/de_pipeline/config.py`         Defines API and S3 settings and
                                      clients.

  `src/de_pipeline/api.py`            Implements HTTP ingestion,
                                      authentication, pagination, retries,
                                      and raw S3 landing.

  `src/de_pipeline/fetch.py`          Downloads the landed raw file from
                                      S3-compatible storage.

  `src/de_pipeline/load.py`           Loads raw records into DuckDB.

  `src/de_pipeline/transform.py`      Deduplicates, cleans, summarizes, and
                                      expands the loaded data.

  `src/de_pipeline/explore.py`        Provides a lightweight exploration of
                                      the transformed results.

  `src/de_pipeline/pipeline.py`       Orchestrates the complete
                                      `ingest → fetch → load → transform`
                                      workflow.

  `tests/`                            Covers API behavior, loading,
                                      transformation, S3 interaction, and
                                      pipeline orchestration.
  -------------------------------------------------------------------------

## Setup

The project uses [`uv`](https://docs.astral.sh/uv/) for the Python
environment and Docker for the local object store.

``` bash
uv sync
cp .env.example .env
docker compose up -d
```

RustFS exposes the local S3-compatible service on ports `9000` and
`9001`. After startup, its status can be checked with:

``` bash
docker compose ps
```

It may take roughly 20--30 seconds for the service to report as healthy.

There is no data-generation or S3-seeding step. The Rick and Morty API
is the source, and the ingestion stage populates the bucket.

The API is public and does not require a key. The client nevertheless
supports optional bearer-token authentication:

``` text
Authorization: Bearer <token>
```

For a normal live run, `API_TOKEN` can remain blank.

On Windows, the environment file can be copied with:

``` text
copy .env.example .env
```

## API ingestion

### HTTP client and authentication

`build_client()` creates a reusable `httpx.Client` pointed at the
configured API base URL. When an API token is configured, the client
sends it as a bearer token. When no token is present, the authorization
header is omitted.

This behavior is covered independently so the authentication path can be
verified without requiring credentials against the public API.

### Page retrieval

`fetch_page()` requests a character page from:

``` text
/character?page=<page>
```

Successful responses are returned as decoded JSON. Non-retryable HTTP
errors fail immediately, while rate-limit responses and transport
failures enter the retry path.

A live request can be exercised with:

``` bash
uv run python -m de_pipeline.api
```

### Pagination

`fetch_all_characters()` begins at page 1 and follows the API's
`info.next` value until it becomes `null`. The same HTTP client is
reused across requests, and each page's `results` collection is
accumulated into one character dataset.

### Rate limiting and retries

The ingestion layer treats `429 Too Many Requests` as a retryable
condition rather than an immediate failure.

When the server supplies a numeric `Retry-After` header, that value
determines the delay before the next attempt. When the header is absent,
the retry strategy uses exponential waits of 1, 2, 4, ... seconds with a maximum wait of 30 seconds.

Transient transport failures are also retried. Retry attempts are capped
by `MAX_ATTEMPTS`, while other HTTP errors fail without retrying.

The retry behavior is implemented with `tenacity`, keeping attempt
limits and backoff policy explicit instead of maintaining a custom retry
loop.

Tests simulate `429` responses and transport behavior with a fake HTTP
transport, so retry handling can be validated without intentionally
throttling the public API.

## Raw landing strategy

After pagination completes, `land_to_s3()` writes the combined character
records to object storage as a single JSON array.

The raw records are preserved before transformation. The API's page-level `info` envelopes and
HTTP headers are not stored; the `results` from all pages are combined into the raw object.

Each ingest replaces the same S3 object. Keeping an untouched raw copy provides a replay point for the rest of
the pipeline. If transformation logic changes or fails, the downstream
stages can be rerun against the landed source data without making another series of API calls.

`ingest()` combines the two operations:

``` text
fetch all characters → land raw JSON
```

## Loading and transformation

Once the API data has been landed, the pipeline returns to the
established ELT workflow.

The raw object is fetched from S3-compatible storage and loaded into
DuckDB. The transformation layer then applies the existing
data-engineering patterns to the character and episode schema, including
deduplication, normalization, summarization, and list expansion with
Polars.

The load and transform behavior can be checked directly with:

``` bash
uv run pytest tests/test_load.py tests/test_transform.py
```

## Pipeline orchestration

`pipeline.py` connects all stages into one executable workflow:

``` text
ingest → fetch → load → transform
```

The pipeline reports the number of records landed and the row counts
produced by the load and transformation stages. The DuckDB connection is
closed when processing finishes.

The full pipeline can be run with RustFS available:

``` bash
uv run de-pipeline
```

The resulting warehouse data can then be inspected with:

``` bash
uv run python -m de_pipeline.explore
```

## Code quality

The exploratory script and the rest of the project were cleaned up with
Ruff. This removed issues such as unused imports, dead variables, and
non-idiomatic comparisons while preserving the script's behavior.

Linting can be run with:

``` bash
uv run ruff check .
```

Safe automatic fixes can be applied with:

``` bash
uv run ruff check . --fix
```

The finished project is expected to remain lint-clean.

## Testing

The test suite covers the API layer, S3 landing behavior, loading,
transformations, and end-to-end orchestration.

Most tests do not require network access or S3. API tests use a fake
HTTP transport, which makes authentication, pagination, rate-limit
handling, and retry timing deterministic.

The S3 fetch test uses the real local object store and skips when RustFS
is unavailable or when no raw file has been landed yet.

Useful test commands include:

``` bash
uv run pytest
uv run pytest tests/test_api.py
uv run pytest tests/test_api.py::test_build_client_sends_auth_header_when_token_set
uv run pytest tests/test_api.py::test_build_client_omits_auth_header_without_token
uv run pytest tests/test_api.py::test_fetch_page_returns_results_and_info
uv run pytest tests/test_api.py::test_fetch_all_walks_every_page
uv run pytest tests/test_api.py::test_fetch_all_retries_on_429_then_succeeds
uv run pytest tests/test_api.py::test_retry_after_header_is_honored
uv run pytest tests/test_api.py::test_ingest_fetches_all_and_lands
uv run pytest tests/test_pipeline.py
```

A focused retry-related run is also available:

``` bash
uv run pytest tests/test_api.py -k "fetch_page or retry_after or retries"
```

## Verification

The completed project can be verified with:

``` bash
uv run pytest
uv run ruff check .
uv run de-pipeline
uv run python -m de_pipeline.explore
```

A successful end-to-end run retrieves the live character dataset, lands
the raw records in S3-compatible storage, loads them into DuckDB, and
builds the transformed warehouse tables.

Running the test suite again after the live pipeline execution also
allows the S3-backed checkpoint to run against the landed object instead
of skipping.

## Working commands

``` bash
uv run pytest                          # run the full test suite
uv run pytest tests/test_api.py        # run API tests
uv run ruff check .                    # lint the project
uv run ruff check . --fix              # apply safe Ruff fixes
uv run python -m de_pipeline.api       # exercise live API ingestion
uv run de-pipeline                     # run the complete pipeline
uv run python -m de_pipeline.explore   # inspect transformed results
```

## Troubleshooting

### RustFS remains `starting` or `unhealthy`

Allow roughly 20--30 seconds for startup, then inspect the container:

``` bash
docker compose ps
docker compose logs rustfs
```

### Ports 9000 or 9001 are already in use

Another RustFS instance may already be running. Stop the earlier
container or change the host-side port in `docker-compose.yml` and
update `S3_ENDPOINT_URL` in `.env` to match.

### Live API requests fail with `httpx.ConnectError` or timeouts

The live ingestion commands require internet access. The API unit tests
do not, because they use a fake transport.

### A live request receives `429 Too Many Requests`

The retry layer should honor `Retry-After` when present and fall back to
exponential backoff otherwise. 

### VS Code does not detect the virtual environment

Use the Command Palette and select:

``` text
Python: Select Interpreter
```

Then choose the interpreter under `.venv`.

## Result

The project now has a complete ingestion-to-transformation path built
around a live HTTP source. The API layer handles authentication,
pagination, transient failures, and rate limiting; raw records are
preserved in object storage before transformation; and the existing
DuckDB and Polars workflow processes the landed data into
warehouse-ready tables.

The result is a reusable pipeline structure in which source acquisition
is cleanly separated from downstream ELT logic. That separation makes
the raw data replayable, keeps API concerns isolated, and allows the
transformation stages to evolve without repeatedly calling the source
system.
