#!/usr/bin/env bash
# К1's harness on one machine (plan, revision 4): Python's ch_sync and the
# Rust worker's, one throwaway Postgres 17.2 and one ClickHouse 24.8,
# compared with tolerance zero by tests/integration/test_rust_k1_ch_sync.py.
#
#   rust/harness/k1-ch-sync.sh            build, run, tear down
#   KEEP=1 rust/harness/k1-ch-sync.sh     leave the two stores running
#
# Nothing here reaches production, KeyCRM or the network beyond the images.
# Docker only: no cargo or Python on the host. The Rust build reuses the
# cargo cache volumes named below; the Python side runs in PY_IMAGE, which
# must be python:3.14-slim with requirements-dev.lock, libgomp1 and
# fonts-dejavu-core installed (what CI installs). Every container is removed
# with -v: anonymous volumes go with it.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
out=${OUT:-${TMPDIR:-/tmp}/ks-k1-harness}
net=ks-k1-net
pg=ks-k1-pg
ch=ks-k1-ch
PY_IMAGE=${PY_IMAGE:-ks-pilot-py:ga4}
RUST_IMAGE=${RUST_IMAGE:-rust:1-slim-bookworm}
PG_IMAGE=postgres:17.2-alpine
CH_IMAGE=clickhouse/clickhouse-server:24.8.14.39-alpine
REGISTRY=${CARGO_REGISTRY_VOLUME:-ks-pilot-cargo-registry}
RUSTUP=${RUSTUP_VOLUME:-ks-pilot-rustup}
TARGET=${CARGO_TARGET_VOLUME:-ks-k1-cargo-target}
# Throwaway credentials for throwaway stores, never anything real.
secret=k1-harness-only

teardown() {
    [ "${KEEP:-0}" = 1 ] && return 0
    docker rm -f -v "$pg" "$ch" >/dev/null 2>&1 || true
    docker network rm "$net" >/dev/null 2>&1 || true
}
trap teardown EXIT

mkdir -p "$out/bin"
rm -f "$out/report.jsonl"

echo "== build: ks-worker (release)"
docker run --rm -v "$REGISTRY:/usr/local/cargo/registry" -v "$RUSTUP:/usr/local/rustup" \
    -v "$TARGET:/src/target" -v "$root/rust:/src" -v "$out/bin:/out" -w /src \
    -e CARGO_TERM_COLOR=never "$RUST_IMAGE" \
    sh -c 'cargo build --release --locked -p ks-worker && cp target/release/ks-worker /out/'

echo "== stores"
docker network create "$net" >/dev/null 2>&1 || true
docker rm -f -v "$pg" "$ch" >/dev/null 2>&1 || true
docker run -d --name "$pg" --network "$net" \
    -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD="$secret" -e POSTGRES_DB=ks \
    -e POSTGRES_INITDB_ARGS="--locale=C.UTF-8 --encoding=UTF8" \
    -v "$root/postgres/initdb:/docker-entrypoint-initdb.d:ro" "$PG_IMAGE" >/dev/null
docker run -d --name "$ch" --network "$net" --ulimit nofile=262144:262144 \
    -e CLICKHOUSE_USER=ks_app -e CLICKHOUSE_PASSWORD="$secret" \
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 "$CH_IMAGE" >/dev/null
for _ in $(seq 1 90); do
    docker logs "$pg" 2>&1 | grep -q "PostgreSQL init process complete" && break
    sleep 1
done
for _ in $(seq 1 60); do
    docker exec "$pg" pg_isready -U postgres -d ks >/dev/null 2>&1 && break
    sleep 1
done
docker exec "$pg" psql -U postgres -tAc "ALTER ROLE ks_app WITH PASSWORD '$secret'" >/dev/null
for _ in $(seq 1 60); do
    docker exec "$ch" clickhouse-client --user ks_app --password "$secret" -q "SELECT 1" \
        >/dev/null 2>&1 && break
    sleep 1
done
for database in silver gold history; do
    docker exec "$ch" clickhouse-client --user ks_app --password "$secret" \
        -q "CREATE DATABASE IF NOT EXISTS $database"
done

echo "== migrations, then the harness"
docker run --rm --init --network "$net" -e TZ=UTC \
    -e KS_PG_DSN="postgresql://ks_app:$secret@$pg:5432/ks" \
    -e KS_CH_URL="http://$ch:8123" -e KS_CH_USER=ks_app -e KS_CH_PASSWORD="$secret" \
    -e KS_RUST_WORKER_BIN=/rust/ks-worker -e KS_K1_REPORT=/out/report.jsonl \
    -v "$root:/app" -v "$out/bin:/rust:ro" -v "$out:/out" -w /app "$PY_IMAGE" \
    sh -c 'alembic upgrade head >/dev/null && python -m pytest -v -p no:cacheprovider \
        tests/integration/test_rust_k1_ch_sync.py tests/unit/test_rust_k1_contract.py'

echo "== report: $out/report.jsonl"
cat "$out/report.jsonl"
