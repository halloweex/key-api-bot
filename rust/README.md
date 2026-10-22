# The Rust pilot

The dashboard's access layer in Rust — the session cookie, roles, tabs and
the rate limit in front of every route — and one endpoint behind it,
`GET /api/revenue/trend` on its Gold branch, each answering exactly as the
Python app does over the same Postgres. It exists to measure what a port
costs and what it buys. **It is not deployed.** It checks sessions as the app
does, and it is still a pilot nobody has reviewed for exposure; it listens on
`127.0.0.1:8090` unless `KS_PILOT_LISTEN` says otherwise.

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
│   └── tests/              the port against 8 631 CPython answers
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
a window outside the years 1–9999 included. 501 for the line-level filter,
a promocode, source 3 and the forecast overlay; 503 when Postgres does not
answer, as under `KS_READ_FALLBACK=off`.

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
  Silver and Gold derivations (`tests/rust_pilot_fixture.py`) and sends 3 260
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
