# The Rust pilot

The dashboard's access layer in Rust — the session cookie, roles, tabs and
the rate limit in front of every route — and one endpoint behind it,
`GET /api/revenue/trend` on its Gold branch, each answering exactly as the
Python app does over the same Postgres. It exists to measure what a port
costs and what it buys. **It answers no one.** `docker-compose.yml` can run it
(service `rust-api`, profile `rust`), and nginx sends it nothing until
`nginx/rust/routes.conf` names a route — see "Switching a route" below. It
checks sessions as the app does, and it is still a pilot nobody has reviewed
for exposure; it listens on `127.0.0.1:8090` unless `KS_PILOT_LISTEN` says
otherwise.

```
rust/
├── Cargo.toml, Cargo.lock, rust-toolchain.toml   one workspace, one compiler (1.99.1)
├── contract/access.json    the app's routes and their gates — GENERATED, see below
├── crates/ks-api/          the service (axum, tokio, sqlx, serde)
│   ├── src/access/         web/routes/auth.py and core/permissions.py
│   │   ├── routing.rs      Starlette's route order, 404/405 before any gate, Python's `$`
│   │   ├── signer.rs       itsdangerous URLSafeTimedSerializer.loads, failure for failure
│   │   ├── pybase64.rs     its lenient base64 (CPython 3.14's non-strict decoder)
│   │   ├── pyjson.rs       json.loads as CPython's scanner reads a payload
│   │   ├── cookie.rs       Starlette's Cookie parsing; paths.rs uvicorn's path decoding
│   │   ├── session.rs      _resolve_session: the user list, the bot's list, asyncpg's BIGINT
│   │   ├── permissions.rs  the role matrix, the seed Python would write, the tab override
│   │   ├── gate.rs         api_gate, require_user/admin/permission/any_permission
│   │   ├── store.rs        the three Postgres reads, the revision check first
│   │   └── ratelimit.rs    slowapi's fixed window
│   ├── src/handover.rs     where it steps aside: the 501 nginx turns into Python's answer
│   ├── src/trend.rs        the endpoint: route + service + repository in one
│   ├── src/gold.rs         core/pg_gold_read.fetch_series, statement for statement
│   ├── src/validate.rs     core/validators.py, messages to the character
│   ├── src/params.rs       FastAPI/pydantic query parsing and its 422s
│   ├── src/period.rs       core/filters.parse_period, Kyiv "today"
│   ├── src/pyrt.rs         CPython's int/float, round(), sum(), repr(), strip()
│   ├── src/strptime.rs     datetime.strptime(s, "%Y-%m-%d"), regex for regex
│   ├── src/pytables.rs     GENERATED: Unicode facts CPython 3.14 answers with
│   ├── gen_pytables.py     writes pytables.rs and tests/fixtures/python_semantics.jsonl
│   ├── gen_access_fixtures.py   writes tests/fixtures/access_semantics.jsonl
│   └── tests/              the port against 8 638 CPython answers
├── crates/ks-loadgen/      the benchmark's closed-loop client
└── bench/                  bench.sh, the Python side (py_app.py), paths.txt
```

## What it answers

**Every request is routed and gated as the app routes and gates it.** The
routes, in Starlette's order, with their dependencies in the order FastAPI
runs them, come from `contract/access.json`, which
`tests/rust_access_contract.py` exports from `web.main.app` and
`tests/unit/test_rust_access_contract.py` holds to it: a route added or
re-gated without the file following fails the suite. A refusal is the app's
to the byte — 401 `Authentication required`, 403 with the gate's words, 404
and 405 before any gate, the app's 500 where `_resolve_session` raises.

Past the gates it answers two endpoints: the trend (below), with its
30-a-minute limit, and the SPA catch-all's JSON 404 for an unmatched `/api`
path. **Any other route a request is let through to is a 501** naming it —
the Python app serves it. The first dependency that is not a gate ends the
gates, as it ends what this compares: on three SMS routes the audience
parameters are parsed before the `sms` permission is asked.

The trend: the same status and body as `web/routes/api/analytics.py` for
every request on the Gold branch — no `category_id`, no `brand`, no
promocode, a source with a Gold channel (none, 1, 2, 4) — the validators'
400s, FastAPI's 422s and the 500s Python raises on a date it cannot parse or
a window outside the years 1–9999 included, and the stored forecast with its
badge (`include_forecast`, read from `app.revenue_predictions`). 501 for the
line-level filter, a promocode and source 3; 503 when a statement fails, as
under `KS_READ_FALLBACK=off`. The month badge's 80% range and Black Friday
line are not the trend's: the page asks `/api/revenue/forecast` for them,
which stays in Python.

**Where its own answer could be wrong for a reason the request does not
carry, it hands the request to Python** — a 501 naming the reason, which
nginx does not pass on (`src/handover.rs`): the schema at another revision
than the contract's (every migration window), Postgres not answering the
revision check every read asks first, no pooled connection within
`KS_PILOT_POOL_WAIT` (2.5 s), a statement its own role stopped (the role's
`statement_timeout` or `lock_timeout`, which Python's role does not have),
and the session's own reads — the user list, the bot's list, the role matrix
— failing. Python answers those requests as it would have: its 401, its 500,
its fallback to the code's matrix, or the data its own reads found.

What it never does: issue a session (the Telegram sign-in stays in Python),
write anything (the role matrix Python seeds on its first read is computed
here instead), or accept a WebSocket.

## Build, run, test

No toolchain on the host is needed; `rust:1.99.1-slim-bookworm` is enough.

```bash
docker build -f Dockerfile.rust -t ks-rust-pilot rust
docker run --rm -p 127.0.0.1:8090:8090 -e KS_PG_DSN=postgresql://… \
    -e DASHBOARD_SECRET_KEY=… -e KS_HARDCODED_ADMIN_IDS=… \
    -e KS_USER_STORE=postgres -e KS_BOT_STORE=postgres ks-rust-pilot
cd rust && cargo fmt --all --check && cargo clippy --workspace --all-targets --all-features -- -D warnings \
    && cargo test --workspace --all-features
```

Configuration shares the Python app's names where they mean the same:
`KS_PG_DSN`, `KS_PG_POOL_MIN` (1), `KS_PG_POOL_MAX` (5), `KS_PG_TIMEOUT` (30 s),
`KS_PILOT_POOL_WAIT` (2.5 s; the pilot's own, where asyncpg waits 30 s),
the signing key `DASHBOARD_SECRET_KEY` — `BOT_TOKEN` when that is empty, as
`web/routes/auth.py` falls back — and `KS_USER_STORE` / `KS_BOT_STORE`, which
must both say `postgres`: access is read where production keeps it, and from
nowhere else. `KS_HARDCODED_ADMIN_IDS` carries `core.permissions.ADMIN_USER_IDS`,
comma-separated; the contract holds only their SHA-256, so it is not another
file that lists them, and the pilot refuses to start on ids that do not hash
to it:

```bash
python -c "from core.permissions import ADMIN_USER_IDS as a; print(','.join(map(str, sorted(a))))"
```

The trend's "now" can be frozen only in the harness image —
`docker build --target harness`, the `test-clock` feature — with
`KS_PILOT_TEST_CLOCK=1` and an `X-Pilot-Now: <RFC 3339>` header. The service
image has no such door, and neither image lets anything set the clock the
session check reads.

## The comparison

Two harnesses, one Postgres: the Python app in-process, the pilot over HTTP.
`.github/workflows/rust-pilot.yml` runs both on every pull request; locally,
with the pilot's harness image running:

```bash
KS_PG_DSN=postgresql://… KS_RUST_PILOT_URL=http://127.0.0.1:8090 TZ=UTC pytest -q \
    tests/integration/test_rust_pilot_trend.py tests/integration/test_rust_pilot_access.py
```

- `test_rust_pilot_trend.py` seeds ~42 000 orders through production's own
  Silver and Gold derivations (`tests/rust_pilot_fixture.py`) and sends 3 347
  trend requests at frozen instants either side of the Kyiv midnights that
  matter.
- `test_rust_pilot_access.py` sends cookie states × users × tab sets × every
  route × `sales_type`, the matrix empty and customised, each store
  unreadable, routing probes and the rate limit. For a route the pilot does
  not port, only the gates' decision is compared: Python's endpoint is
  replaced in the test process by one that keeps the route's gates, and its
  pass must be Rust's 501.

The CPython fixtures cover what HTTP cannot reach on realistic data —
`round()` ties, `sum()`'s compensation, base64 that skips junk, payloads only
the signing key can produce. Regenerate them, and the contract, only on the
Python production runs:

```bash
IMAGE="<python:3.14-slim + requirements-dev.lock>"
docker run --rm -v "$PWD:/app" -w /app $IMAGE python rust/crates/ks-api/gen_pytables.py
docker run --rm -v "$PWD:/app" -w /app $IMAGE python rust/crates/ks-api/gen_access_fixtures.py
docker run --rm -v "$PWD:/app" -w /app $IMAGE python -m tests.rust_access_contract
```

## The benchmark

```bash
rust/bench/bench.sh        # ~10 minutes; leaves nothing behind
```

Both servers check the session and the limit: the load runs as a hardcoded
admin (a signature check, no read) and as an approved viewer (the session
read from Postgres), each request with its own `X-Real-IP`. `paths.txt` is
these 216 requests plus four explicit ranges:

```bash
for p in today yesterday week last_week month last_month last_7_days last_28_days; do
  for s in retail b2b all; do for src in "" 1 4; do
    for c in previous_period year_ago month_ago; do
      q="period=$p&sales_type=$s"; [ -n "$src" ] && q="$q&source_id=$src"
      echo "/api/revenue/trend?$q&compare_type=$c"
done; done; done; done
```

## Switching a route

Everything a route needs to move is already in place and routes nothing:

- **The service.** `rust-api` in `docker-compose.yml`, in the `rust` profile, so
  a plain `docker compose pull` / `up -d` never touches it. It reads Postgres as
  `ks_rust_api` (below), publishes no port, and its healthcheck (`ks-api healthcheck`)
  asks `/readyz`: Postgres answers that role and the schema is at the
  contract's revision. An image older than the schema shows up unhealthy.
- **The switch.** `nginx/rust/routes.conf`, mounted as a directory and empty.
  One line per route switches it —
  `location = /api/revenue/trend { include /etc/nginx/rust/proxy.conf; }` —
  and deleting the line takes it back.
- **The fallback.** `proxy.conf` hands a request back to web on the pilot's own
  501 ("the Python app serves it") and on the 502 nginx makes when the pilot
  does not answer, and passes every other answer through. `ua=` and `us=` in
  nginx's log name both hops of a handed-back request.
- **The deploy.** `deploy.yml` builds this image only when `rust/**` or
  `Dockerfile.rust` changed, on an arm64 runner, and deploys it in its own job:
  pull, `up` under the profile, wait for healthy, `nginx -t`, `nginx -s reload`.
  A change to `routes.conf` alone is that job's reload and nothing else
  (`deploy/changed_components.py` decides).

## The guards (plan, revision 3, step 2)

Four things stand between the pilot and a wrong answer nobody notices.
Nothing here is on production until the owner's yes; the role is created by
hand.

- **`Vary: Accept-Encoding`**, as the app's stack sets it: on every answer
  with a body, compressed or not, because its `BaseHTTPMiddleware`s hand GZip
  a stream and GZip marks any stream; not on an empty body, the timeout's own
  504 or an unhandled exception's 500 (`vary` in `src/http.rs`). nginx's
  `gzip_vary` covers only what it would compress, a 200, 403 or 404 of 256
  bytes and up. Both harnesses compare the header; the first port, by GZip's
  500-byte minimum, differed on 11 140 answers.
- **The watchdog**, in the bot's canary (`check_rust_api` in
  `bot/canary.py`), every 15 minutes where `KS_RUST_API_URL` says where the
  pilot is (`http://ks-rust-api:8090` in compose). It asks `/readyz` and the
  pilot's own `/stats` — every answer of the last 15 minutes and since
  start, each 501 by the branch it names (`src/stats.rs`) — and warns when
  the container does not answer (`rust_api_unreachable`), is not ready
  (`rust_api_not_ready`, the reason named), or handed more than 5% of at
  least 20 requests to Python for a reason of its own: revision, database,
  pool, session, timeout (`rust_api_handover_share`). Hand-overs by design —
  a route not ported, a filter left to Python — are not counted. A warning,
  not an outage: nginx answers from Python meanwhile. The first two wait for
  a second probe, as `health_*` does, because a deploy recreates the
  container.
- **The drift sensor**: `test_python_answers_the_trend_as_recorded` in
  `tests/integration/test_rust_pilot_trend.py`, the `drift` job of
  `.github/workflows/rust-pilot.yml`. Python's answers to the
  harness's 5 087 requests, on the harness's seed, against the digests
  recorded in `tests/integration/golden/rust_trend_answers.json`; no Rust is
  built. A Python change that moves the trend's answer fails the pull request
  that made it. Then: port it to `crates/ks-api` and record again with
  `KS_TREND_GOLDEN=write` (the module says how).
- **Its own role**, `deploy/rust/ks_rust_api.sql`: SELECT on the six tables
  the pilot reads (`tests/unit/test_rust_api_role.py` holds the list to its
  statements), read-only sessions, five connections, `statement_timeout` 3 s
  and `lock_timeout` 1 s. A statement the role stops is handed to Python
  (`timeout`), not answered 503: Python's role has no such limit. The pool
  waits 2.5 s for a connection (`KS_PILOT_POOL_WAIT`). CI's harness reads as
  this role; compose names it with `KS_RUST_API_PASSWORD` from `.env`, empty
  by default so that a missing password costs the Rust step and not every
  compose command.
