# Week 3 — Go Get the Data Yourself (APIs)

Weeks 1-2 the data just *showed up* in S3. Real life isn't like that. This week
you go get it from a live **HTTP API** and meet the ugly realities that come with
it — **auth, pagination, and rate limits** — then you land the raw character
records in S3 and let the pipeline you already built carry it the rest of the way.

```
        Rick & Morty API                 S3 / RustFS           DuckDB
 ┌──────────────────────────┐  land raw  ┌──────────────┐ fetch ┌──────────────┐
 │ GET /character?page=1     │ ─────────► │ characters   │ ────► │ raw_characters│
 │ GET /character?page=2     │  (Day 1-2) │ .json (raw)  │ load  │  ↓ transform  │
 │ ... 429? back off & retry │            └──────────────┘       │ clean/summary │
 │ ... until info.next=null  │                                   │ episodes(Polars)
 └──────────────────────────┘                                    └──────────────┘
   httpx · tenacity                        (the "land raw"        (PROVIDED — the
   YOU WRITE THIS (api.py)                  principle)             Week 2 toolkit)
```

The big idea: **only the front of the pipeline changes.** You swap "read a file
from S3" for "fetch from an API, then land it to S3." Everything downstream —
fetch, load, transform — is the same ELT you already know, and it's **provided**
so you can spend the week on the part that's actually new.

### How this builds on Weeks 1 and 2

- **Week 1:** download from S3, load raw data into DuckDB, then transform it.
- **Week 2:** deduplicate with window functions, normalize values, bind SQL
  parameters, and explode lists in Polars.
- **Week 3:** apply those patterns to characters and episodes while you build
  the new HTTP ingestion stage. Auth and retry behavior are tested with fake
  responses; pagination also runs against the live API.

This repo is self-contained: you do not need to copy your earlier solutions.
The provided modules adapt familiar techniques to a new schema; they are not
identical copies of Week 2. SQL is inline here for reading alongside the Python;
Week 2's SQL-file refactor remains a useful organization pattern. First run
`uv run pytest tests/test_load.py tests/test_transform.py` and trace the small
fixture in `tests/conftest.py`: 6 raw records become 5 characters, 2 species,
and 3 episode summary rows.

## What's in this repo

| Path | What it is |
| --- | --- |
| `docker-compose.yml` | Local S3-compatible store ([RustFS](https://rustfs.com)) — the raw landing zone. |
| `src/de_pipeline/config.py` | API + S3 settings and clients. **Provided — don't change.** |
| `src/de_pipeline/api.py` | **Days 1-2, you write this** — httpx ingest: auth, pagination, rate limits, land-to-S3. |
| `src/de_pipeline/fetch.py` | Download the landed file from S3. **Provided (Week 1-2).** |
| `src/de_pipeline/load.py` | Load raw into DuckDB. **Provided (Week 1-2).** |
| `src/de_pipeline/transform.py` | dedup / clean / summarize / explode. **Provided (the Week 2 toolkit).** |
| `src/de_pipeline/explore.py` | A teammate's quick-and-dirty analysis script. **Day 3 — clean it up.** |
| `src/de_pipeline/pipeline.py` | **Day 3, you wire this** — ingest → fetch → load → transform. |
| `tests/` | Checkpoints. The API tests run offline (no network) via a fake transport. |

## One-time setup

You need [`uv`](https://docs.astral.sh/uv/) and Docker. Do these **in order**.

```bash
uv sync                       # 1. create the environment (.venv/)
cp .env.example .env          # 2. config (defaults already match; API needs no key)
docker compose up -d          # 3. start RustFS (the raw landing zone) on :9000 / :9001
```

> _Confirm step 3:_ wait ~20-30s, then `docker compose ps` shows **healthy**.
> There's no `generate_data` / `seed_s3` this week — **the API is your source**,
> and your `api.ingest()` is what fills the bucket.

> The Rick and Morty API is public and needs no key. We use it because it's open
> and requires no signup. You will still wire optional bearer-token auth
> (`Authorization: Bearer <token>`) and check it with a fake token in tests.
> Leave `API_TOKEN` blank for the live run.

### VS Code

Install the Python and **Ruff** extensions if you do not already have them.
Ruff shows lint inline; the **Testing** panel (beaker icon) runs the checkpoints with a click.

## The week, day by day

**The rhythm, same as always: write a function → run its test → green → move on.**
Run `uv run pytest` first to see the map. The API tests use a *fake transport*,
so they can run without the network or real rate-limiting. They initially fail
with `NotImplementedError`; the provided load/transform tests should pass.
Ruff initially reports intentional findings in `explore.py` for Day 3.

> Most checkpoints need no network and no S3. The `test_fetch.py` test talks to
> real S3 and skips itself when RustFS is down or no file has landed yet.
> Both `uv run python -m de_pipeline.api` and `uv run de-pipeline` call the live API.

### Day 1 — your first API call (`api.py`)

The goal: make one real request, see what comes back, and wire auth.

1. **Build the client.** Implement `build_client()` — an `httpx.Client` pointed at
   `settings.api_base_url`, sending `Authorization: Bearer <token>` *when* a token
   is set. Checkpoints:
   ```bash
   uv run pytest tests/test_api.py::test_build_client_sends_auth_header_when_token_set
   uv run pytest tests/test_api.py::test_build_client_omits_auth_header_without_token
   ```
2. **Fetch one page.** Implement `fetch_page()` (happy path for now): GET
   `/character?page=<page>`, raise on HTTP errors, return `response.json()`.
   ```bash
   uv run pytest tests/test_api.py::test_fetch_page_returns_results_and_info
   ```
   See it for real:
   ```bash
   uv run python -m de_pipeline.api
   ```
3. **Land it raw.** Implement `land_to_s3()` — upload the records to S3 as one raw
   JSON array (`put_object`). This is the Week 1 S3 move in reverse. Checkpoint:
   ```bash
   uv run pytest tests/test_api.py::test_land_to_s3_uploads_one_json_object
   ```

> **The "land raw" principle.** Preserve every character record unchanged,
> including nested fields and duplicates, before transforming anything. Combine
> the pages' `results` into one JSON array; this exercise does not save the
> `info` envelopes or HTTP headers. Each ingest replaces the same S3 object.
> If a transform is wrong, you re-run it against the raw copy
> instead of re-hitting (and re-rate-limiting) the API. Raw is your replay buffer.

### Day 2 — pagination + rate limits (`api.py`)

One page isn't the dataset. And hammering an API gets you a `429`.

1. **Walk every page.** Implement `fetch_all_characters()` — start at page 1 and
   keep going while `info.next` isn't null, collecting all `results`. Reuse one
   client across pages.
   ```bash
   uv run pytest tests/test_api.py::test_fetch_all_walks_every_page
   ```
2. **Survive rate limits.** A `429 Too Many Requests` means *slow down*, not
   *fail*. Make `fetch_page` turn a 429 into a retry that honors the server's
   `Retry-After` header, backing off exponentially otherwise — with **tenacity**.
   ```bash
   uv run pytest tests/test_api.py::test_fetch_all_retries_on_429_then_succeeds
   uv run pytest tests/test_api.py::test_retry_after_header_is_honored
   ```
   Also retry transport failures, stop after `MAX_ATTEMPTS` total attempts,
   and let other HTTP errors fail immediately. For this exercise, support
   numeric `Retry-After` seconds (including zero); use exponential waits of
   1, 2, 4, ... seconds, capped at 30 seconds, when the header is absent.
   HTTP-date headers and malformed values are optional extensions.
   ```bash
   uv run pytest tests/test_api.py -k "fetch_page or retry_after or retries"
   ```
3. **Tie ingest together.** Implement `ingest()` — `fetch_all_characters()` then
   `land_to_s3()`. This is the whole new front-of-pipeline stage.
   ```bash
   uv run pytest tests/test_api.py::test_ingest_fetches_all_and_lands
   uv run pytest tests/test_api.py  # all Day 1-2 checkpoints
   ```

> The fake transport supplies the 429s; do not hammer the public API to trigger
> throttling. See the [API documentation](https://rickandmortyapi.com/documentation)
> for the real response schema and pagination contract.
>
> **Why honor `Retry-After`?** The server is telling you exactly how long to wait.
> A fixed `sleep(5)` either wastes time or comes back too early and gets throttled
> again. Read the header. And why a library (tenacity) instead of a hand-rolled
> loop? Backoff, jitter, attempt caps, and "retry only these errors" are easy to
> get subtly wrong — this is exactly the kind of thing you don't reinvent.

### Day 3 — code quality + consolidation (`pipeline.py`, `explore.py`, ruff)

Working isn't the same as *shippable*. Today you put on the code-reviewer hat.

1. **Clean up with ruff.** Run the linter across the project:
   ```bash
   uv run ruff check .
   ```
   `explore.py` is a teammate's quick exploratory script — it works, but it's full
   of the small stuff that fails review (unused imports, `== None`, dead
   variables, ...). Fix every finding until `ruff check` is clean. Some you can
   auto-fix; do the rest by hand and understand each one.
   ```bash
   uv run ruff check . --fix      # let it fix the safe ones, then read what's left
   ```
2. **Wire the pipeline.** Implement `main()` in `pipeline.py`:
   `ingest → fetch → load → transform`, printing the landed count and the
   dictionaries of loaded/transformed row counts. Close the DuckDB connection
   when finished. Checkpoint (offline):
   ```bash
   uv run pytest tests/test_pipeline.py
   ```
3. **Run it end to end** (RustFS up):
   ```bash
   uv run de-pipeline
   ```
4. **Review the result.** Run `uv run python -m de_pipeline.explore` and
   explain two lint fixes and why raw records land before transformation.
5. **Final checkpoint** — whole suite green and the linter clean:
   ```bash
   uv run pytest
   uv run ruff check .
   ```

> **Where this is heading:** you now have hand-written transforms *and* a real
> ingestion layer. Next we meet **dbt** — the framework the industry uses to
> organize exactly the kind of SQL transforms you've been writing by hand.

## Working commands

```bash
uv run pytest                              # whole suite
uv run pytest tests/test_api.py            # the API checkpoints (offline)
uv run pytest tests/test_api.py::test_name # one checkpoint
uv run ruff check .                        # lint (the Day-3 tool)
uv run ruff check . --fix                  # auto-fix the safe findings
uv run python -m de_pipeline.api           # hit the real API once, by hand
uv run de-pipeline                         # the whole thing, end to end
```

## How you'll know you're done

`uv run pytest` is green, `uv run ruff check .` reports no problems, and
`uv run de-pipeline` runs the whole thing — ingesting live characters, landing
them in S3, and building the warehouse tables. Run the suite again after that
live run so the S3 checkpoint passes rather than skips. Confirm the exploration
script still works after your lint cleanup.

## Troubleshooting

- **`docker compose ps` shows `unhealthy`/`starting`** — give it 20-30s; check
  `docker compose logs rustfs`.
- **Port 9000/9001 already in use** — usually an earlier week's RustFS. Stop it
  (`cd ../week-2 && docker compose down`) or change the left-hand port in
  `docker-compose.yml` and update `S3_ENDPOINT_URL` in `.env`.
- **`httpx.ConnectError` / timeouts on the live run** — you need internet for the
  real API. The tests don't (they use a fake transport), so develop against those.
- **A 429 in the wild** — that's the lesson, not a bug. Your tenacity retry should
  wait and recover. If it gives up, check you're honoring `Retry-After`.
- **Windows** — use `copy .env.example .env`. If VS Code doesn't pick up the env,
  Command Palette → *Python: Select Interpreter* → the one under `.venv`.
