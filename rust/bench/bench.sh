#!/usr/bin/env bash
# The Rust pilot's benchmark: the same 220 requests (paths.txt), the same
# Postgres and the same single CPU, against uvicorn serving production's app
# (py_app.py) and against ks-api. A laptop tool: it runs nothing on a deployed
# host, publishes no port, and its network is --internal (no route out).
#
#     rust/bench/bench.sh
#
# Knobs: BENCH_DURATION (30 s a run), BENCH_WARMUP (5 s), BENCH_CONCURRENCY
# ("1 16"), BENCH_SERVER_CPUS ("1" — production gives web `cpus: 1.0`, and a
# cpuset avoids the quota's throttling jitter), BENCH_DB_CPUS ("2-3"),
# BENCH_CLIENT_CPUS ("4-7"), BENCH_OUT (a new temp directory).
#
# Per server it records VmRSS before the first request and after the pool is
# open (at rest), and VmHWM after the runs (under load); per concurrency and
# caller, the load generator's JSON line: throughput and p50/p90/p95/p99/max.
#
# Both servers check the session and the rate limit, as production does. The
# load runs twice: as a hardcoded admin, whose session is a signature and no
# read, and as an approved viewer, whose session reads Postgres. Every request
# has its own X-Real-IP, so the 30-a-minute limit is checked and never trips.
#
# Everything it creates it removes on exit — containers with their volumes,
# the network, the images and its own buildx builder, which is how the build
# cache it fills is bounded. A run therefore builds from cold, ~2-3 minutes.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${BENCH_OUT:-$(mktemp -d)}"
DURATION="${BENCH_DURATION:-30}"
WARMUP="${BENCH_WARMUP:-5}"
CONCURRENCY="${BENCH_CONCURRENCY:-1 16}"
SERVER_CPUS="${BENCH_SERVER_CPUS:-1}"
DB_CPUS="${BENCH_DB_CPUS:-2-3}"
CLIENT_CPUS="${BENCH_CLIENT_CPUS:-4-7}"

NET=ks-bench-net
PG=ks-bench-pg
PW=bench-only
DSN="postgresql://ks_app:$PW@$PG:5432/ks"
SECRET=bench-only-not-a-secret
VIEWER=9100000999
BUILDER=ks-bench-builder
IMAGES=(ks-bench-rust ks-bench-loadgen ks-bench-migrate ks-bench-python)
CTX="$(mktemp -d)"

cleanup() {
    docker rm -f -v ks-bench-rust ks-bench-python "$PG" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
    docker image rm "${IMAGES[@]}" >/dev/null 2>&1 || true
    docker buildx rm "$BUILDER" >/dev/null 2>&1 || true
    rm -rf "$CTX"
}
trap cleanup EXIT
mkdir -p "$OUT"
echo "results: $OUT"

build() {  # tag, then `docker buildx build` arguments
    local tag=$1
    shift
    docker buildx build --builder "$BUILDER" -q -t "$tag" --load "$@" >/dev/null
}

echo "== images"
docker buildx create --name "$BUILDER" --driver docker-container >/dev/null
build ks-bench-rust -f "$REPO/Dockerfile.rust" "$REPO/rust"
build ks-bench-loadgen -f "$REPO/Dockerfile.rust" --target loadgen "$REPO/rust"
build ks-bench-migrate -f "$REPO/Dockerfile.migrate" "$REPO"
# Production's interpreter and runtime lock, without the frontend stage of
# Dockerfile.web: the base is read out of Dockerfile.web, digest and all.
base="$(grep -m1 -o 'python:3\.14-slim@sha256:[0-9a-f]*' "$REPO/Dockerfile.web")"
cp "$REPO/requirements.lock" "$CTX/"
cat > "$CTX/Dockerfile" <<EOF
FROM $base
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \\
    && rm -rf /var/lib/apt/lists/*
COPY requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir -r /tmp/requirements.lock
EOF
build ks-bench-python "$CTX"
docker image inspect ks-bench-rust ks-bench-python --format '{{.RepoTags}} {{.Size}}' \
    | tee "$OUT/images.txt"

echo "== postgres, migrated and seeded"
docker network create --internal "$NET" >/dev/null
docker run -d --name "$PG" --network "$NET" --cpuset-cpus "$DB_CPUS" \
    -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD="$PW" -e POSTGRES_DB=ks \
    -e POSTGRES_INITDB_ARGS="--locale=C.UTF-8 --encoding=UTF8" \
    -v "$REPO/postgres/initdb:/docker-entrypoint-initdb.d:ro" postgres:17.2-alpine >/dev/null
# The entrypoint's own line, then the server: see ci.yml for the race this avoids.
for _ in $(seq 1 90); do
    docker logs "$PG" 2>&1 | grep -q "PostgreSQL init process complete" && break
    sleep 1
done
for _ in $(seq 1 60); do
    docker exec "$PG" pg_isready -U postgres -d ks >/dev/null 2>&1 && break
    sleep 1
done
docker exec "$PG" psql -U postgres -tAc "ALTER ROLE ks_app WITH PASSWORD '$PW'" >/dev/null
docker run --rm --network "$NET" -e KS_PG_DSN="$DSN" ks-bench-migrate >/dev/null
docker run --rm --network "$NET" -e KS_PG_DSN="$DSN" -e PYTHONDONTWRITEBYTECODE=1 \
    -v "$REPO:/app:ro" -w /app ks-bench-python python -m tests.rust_pilot_fixture

# The admin ids and the cookies, minted by the app's own serializer: the ids
# come from core.permissions and are written nowhere.
docker exec "$PG" psql -U postgres -d ks -qc \
    "INSERT INTO app.dashboard_users (user_id, role, status) VALUES ($VIEWER, 'viewer', 'approved') ON CONFLICT (user_id) DO NOTHING"
minted=$(docker run --rm -e DASHBOARD_SECRET_KEY="$SECRET" -e KS_DUCKDB=off \
    -e PYTHONDONTWRITEBYTECODE=1 -v "$REPO:/app:ro" -w /app ks-bench-python python -c "
from core.permissions import ADMIN_USER_IDS as ids
from web.routes.auth import session_serializer as s
print(','.join(map(str, sorted(ids))))
print(s.dumps({'user_id': min(ids), 'role': 'admin'}))
print(s.dumps({'user_id': $VIEWER, 'role': 'viewer'}))
" 2>/dev/null)
# Line by line, the way bash 3.2 (macOS) can read it.
ADMIN_IDS=$(printf '%s\n' "$minted" | sed -n 1p)
ADMIN_COOKIE=$(printf '%s\n' "$minted" | sed -n 2p)
VIEWER_COOKIE=$(printf '%s\n' "$minted" | sed -n 3p)

rss() {  # VmRSS and VmHWM (kB) of PID 1 in container $1
    docker run --rm --pid "container:$1" ks-bench-python \
        sh -c 'grep -E "^Vm(RSS|HWM):" /proc/1/status' | awk '{printf "%s%s ", $1, $2}'
}

load() {  # server, port, concurrency, warmup, duration, label, caller
    local cookie=$ADMIN_COOKIE
    [ "$7" = viewer ] && cookie=$VIEWER_COOKIE
    docker run --rm --network "$NET" --cpuset-cpus "$CLIENT_CPUS" \
        -v "$REPO/rust/bench/paths.txt:/paths.txt:ro" ks-bench-loadgen \
        --target "http://$1:$2" --paths /paths.txt --concurrency "$3" \
        --warmup "$4" --duration "$5" --label "$6" \
        --header "Cookie: dashboard_session=$cookie" --rotate-real-ip
}

measure() {  # name, port, then `docker run` arguments
    local name=$1 port=$2
    shift 2
    docker run -d --name "$name" --network "$NET" --cpuset-cpus "$SERVER_CPUS" "$@" >/dev/null
    for _ in $(seq 1 60); do
        docker run --rm --network "$NET" ks-bench-python python -c \
            "import socket; socket.create_connection(('$name', $port), 1)" >/dev/null 2>&1 && break
        sleep 1
    done
    sleep 2
    echo "$name at_rest_before_first_request $(rss "$name")" | tee -a "$OUT/rss.txt"
    load "$name" "$port" 1 0 2 "$name-prime" admin >/dev/null
    sleep 2
    echo "$name at_rest_pool_open $(rss "$name")" | tee -a "$OUT/rss.txt"
    for caller in admin viewer; do
        for c in $CONCURRENCY; do
            load "$name" "$port" "$c" "$WARMUP" "$DURATION" "$name-$caller-c$c" "$caller" \
                | tee -a "$OUT/latency.jsonl"
        done
    done
    echo "$name after_load $(rss "$name")" | tee -a "$OUT/rss.txt"
    docker rm -f -v "$name" >/dev/null
}

echo "== rust"
measure ks-bench-rust 8090 -e KS_PG_DSN="$DSN" -e DASHBOARD_SECRET_KEY="$SECRET" \
    -e KS_HARDCODED_ADMIN_IDS="$ADMIN_IDS" -e KS_USER_STORE=postgres -e KS_BOT_STORE=postgres \
    ks-bench-rust

echo "== python"
# The app makes web/static-v2 at import; it is gitignored, so it is made here
# and the tree is mounted read-only.
mkdir -p "$REPO/web/static-v2"
measure ks-bench-python 8080 \
    -e KS_PG_DSN="$DSN" -e KS_READ_GOLD=postgres -e KS_DUCKDB=off -e TZ=Europe/Kyiv \
    -e DASHBOARD_SECRET_KEY="$SECRET" -e KS_ALERTS_DISABLED=1 \
    -e KS_USER_STORE=postgres -e KS_BOT_STORE=postgres \
    -e PYTHONDONTWRITEBYTECODE=1 -v "$REPO:/app:ro" -w /app ks-bench-python \
    uvicorn rust.bench.py_app:app --host 0.0.0.0 --port 8080 --lifespan off

echo "== done: $OUT"
