#!/usr/bin/env bash
# К2's harness on one machine (plan, revision 4, step 1): chain 6a's
# dictionary written by Python and by the Rust worker from the same bytes, on
# one throwaway Postgres 17.2, compared with tolerance zero by
# tests/integration/test_rust_k2_expense_types.py.
#
#   rust/harness/k2-expense-types.sh            build, run, tear down
#   KEEP=1 rust/harness/k2-expense-types.sh     leave the store running
#   K2_TESTS='tests/…::test_x' rust/harness/k2-expense-types.sh   only those
#
# Nothing here reaches production, KeyCRM or the network beyond the images.
# Docker only: no cargo or Python on the host. The Rust build reuses К1's
# cargo cache volumes; the Python side runs in PY_IMAGE, which must be
# python:3.14-slim with requirements-dev.lock installed (what CI installs).
# Every container is removed with -v: anonymous volumes go with it.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
out=${OUT:-${TMPDIR:-/tmp}/ks-k2-harness}
net=ks-k2-net
pg=ks-k2-pg
PY_IMAGE=${PY_IMAGE:-ks-pilot-py:ga4}
RUST_IMAGE=${RUST_IMAGE:-rust:1-slim-bookworm}
PG_IMAGE=postgres:17.2-alpine
REGISTRY=${CARGO_REGISTRY_VOLUME:-ks-pilot-cargo-registry}
RUSTUP=${RUSTUP_VOLUME:-ks-pilot-rustup}
TARGET=${CARGO_TARGET_VOLUME:-ks-k1-cargo-target}
# Throwaway credentials for a throwaway store, never anything real.
secret=k2-harness-only

teardown() {
    [ "${KEEP:-0}" = 1 ] && return 0
    docker rm -f -v "$pg" >/dev/null 2>&1 || true
    docker network rm "$net" >/dev/null 2>&1 || true
}
trap teardown EXIT

mkdir -p "$out/bin"

echo "== build: ks-worker (release)"
docker run --rm -v "$REGISTRY:/usr/local/cargo/registry" -v "$RUSTUP:/usr/local/rustup" \
    -v "$TARGET:/src/target" -v "$root/rust:/src" -v "$out/bin:/out" -w /src \
    -e CARGO_TERM_COLOR=never "$RUST_IMAGE" \
    sh -c 'cargo build --release --locked -p ks-worker && cp target/release/ks-worker /out/'

echo "== store"
docker network create "$net" >/dev/null 2>&1 || true
docker rm -f -v "$pg" >/dev/null 2>&1 || true
docker run -d --name "$pg" --network "$net" \
    -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD="$secret" -e POSTGRES_DB=ks \
    -e POSTGRES_INITDB_ARGS="--locale=C.UTF-8 --encoding=UTF8" \
    -v "$root/postgres/initdb:/docker-entrypoint-initdb.d:ro" "$PG_IMAGE" >/dev/null
for _ in $(seq 1 90); do
    docker logs "$pg" 2>&1 | grep -q "PostgreSQL init process complete" && break
    sleep 1
done
for _ in $(seq 1 60); do
    docker exec "$pg" pg_isready -U postgres -d ks >/dev/null 2>&1 && break
    sleep 1
done
docker exec "$pg" psql -U postgres -tAc "ALTER ROLE ks_app WITH PASSWORD '$secret'" >/dev/null

echo "== migrations"
docker run --rm --init --network "$net" -e TZ=UTC \
    -e KS_PG_DSN="postgresql://ks_app:$secret@$pg:5432/ks" \
    -v "$root:/app" -w /app "$PY_IMAGE" sh -c 'alembic upgrade head >/dev/null'

# The worker's own login, from the file production runs, so the shadow ships
# as it here and a grant it lacks fails here.
echo "== the worker's login"
docker exec -i "$pg" psql -U postgres -d ks -q -v ON_ERROR_STOP=1 -v password="$secret" \
    -f - < "$root/deploy/rust/ks_rust_worker.sql"

echo "== the harness"
docker run --rm --init --network "$net" -e TZ=UTC \
    -e KS_PG_DSN="postgresql://ks_app:$secret@$pg:5432/ks" \
    -e KS_K2_WORKER_PG_DSN="postgresql://ks_rust_worker:$secret@$pg:5432/ks" \
    -e KS_RUST_WORKER_BIN=/rust/ks-worker \
    -v "$root:/app" -v "$out/bin:/rust:ro" -w /app "$PY_IMAGE" \
    sh -c "python -m pytest -v -p no:cacheprovider ${K2_TESTS:-tests/integration/test_rust_k2_expense_types.py tests/unit/test_rust_k2_contract.py}"
