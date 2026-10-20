# The Rust pilot

One endpoint of the dashboard in Rust — `GET /api/revenue/trend`, the Gold
branch, without the forecast overlay — answering exactly as the Python app
does over the same Postgres. It exists to measure what a port costs and what
it buys. **It is not deployed, and must not be reachable by a person**: it
checks no session, so `sales_type=internal`, admin-only behind the Python
app's `api_gate`, is served to whoever can connect. It listens on
`127.0.0.1:8090` unless `KS_PILOT_LISTEN` says otherwise.

```
rust/
├── Cargo.toml, Cargo.lock, rust-toolchain.toml   one workspace, one compiler (1.99.1)
├── crates/ks-api/          the service (axum, tokio, sqlx, serde)
│   ├── src/trend.rs        the endpoint: route + service + repository in one
│   ├── src/gold.rs         core/pg_gold_read.fetch_series, statement for statement
│   ├── src/validate.rs     core/validators.py, messages to the character
│   ├── src/params.rs       FastAPI/pydantic query parsing and its 422s
│   ├── src/period.rs       core/filters.parse_period, Kyiv "today"
│   ├── src/pyrt.rs         CPython's int/float, round(), sum(), repr(), strip()
│   ├── src/strptime.rs     datetime.strptime(s, "%Y-%m-%d"), regex for regex
│   ├── src/pytables.rs     GENERATED: Unicode facts CPython 3.14 answers with
│   ├── gen_pytables.py     writes pytables.rs and tests/fixtures/*.jsonl
│   └── tests/              the port against 2 925 CPython answers
├── crates/ks-loadgen/      the benchmark's closed-loop client
└── bench/                  bench.sh, the Python side (py_app.py), paths.txt
```

## What it answers

The same status and body as `web/routes/api/analytics.py` for every request
on the Gold branch: no `category_id`, no `brand`, no promocode, and a source
with a Gold channel (none, 1, 2, 4). That includes the validators' 400s,
FastAPI's 422s and the 500s Python raises on a date it cannot parse or a
window that leaves the years 1–9999.

**501** for what the pilot leaves to Python, after every check Python makes
first: a line-level filter (`category_id`, `brand`), a non-blank promocode,
source 3 (Opencart, no Gold channel), and `include_forecast=true` wherever the
route would attempt the overlay. **503** when Postgres does not answer — the
Python app's answer under `KS_READ_FALLBACK=off`, which production runs.

## Build, run, test

No toolchain on the host is needed; `rust:1.99.1-slim-bookworm` is enough.

```bash
docker build -f Dockerfile.rust -t ks-rust-pilot rust
docker run --rm -e KS_PG_DSN=postgresql://… -p 127.0.0.1:8090:8090 ks-rust-pilot
cd rust && cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```

Configuration shares the Python app's names where they mean the same:
`KS_PG_DSN`, `KS_PG_POOL_MIN` (1), `KS_PG_POOL_MAX` (5), `KS_PG_TIMEOUT` (30 s).
`KS_PILOT_TEST_CLOCK=1` lets an `X-Pilot-Now: <RFC 3339>` header set "now" —
the harness's hook for Kyiv midnights, never for anything a person reads.

## The comparison

`tests/integration/test_rust_pilot_trend.py` seeds ~42 000 orders through
production's own Silver and Gold derivations (`tests/rust_pilot_fixture.py`),
then sends 3 260 requests to the Python app in-process and to the pilot over
HTTP, at frozen instants either side of the Kyiv midnights that matter. It
fails on any difference in status, content type or JSON (keys sorted, `0` and
`0.0` told apart). `.github/workflows/rust-pilot.yml` runs it on every pull
request; locally:

```bash
KS_PG_DSN=postgresql://… KS_RUST_PILOT_URL=http://127.0.0.1:8090 \
    TZ=UTC pytest -q tests/integration/test_rust_pilot_trend.py
```

The CPython fixtures cover what the HTTP comparison cannot see on realistic
data — `round()` on exact ties and `sum()`'s compensation change no rounded
total there. Regenerate them only on the Python production runs:

```bash
docker run --rm -v "$PWD:/app" -w /app <python:3.14-slim + requirements-dev.lock> \
    python rust/crates/ks-api/gen_pytables.py
```

## The benchmark

```bash
rust/bench/bench.sh        # ~6 minutes; leaves nothing behind
```

`paths.txt` is these 216 requests plus four explicit ranges:

```bash
for p in today yesterday week last_week month last_month last_7_days last_28_days; do
  for s in retail b2b all; do for src in "" 1 4; do
    for c in previous_period year_ago month_ago; do
      q="period=$p&sales_type=$s"; [ -n "$src" ] && q="$q&source_id=$src"
      echo "/api/revenue/trend?$q&compare_type=$c"
done; done; done; done
```
