#!/usr/bin/env bash
# One or more test files against a throwaway PostgreSQL, in the production image.
#
# WHY THIS EXISTS
#
# `deploy/gate_with_stores.sh` runs the whole suite and takes thirteen minutes.
# That is the right thing before a deploy and the wrong thing while iterating
# on one fixture: the /expenses port needed two attempts at a seed, and each
# cost eleven minutes to learn a column was called `name` and not `full_name`.
# This runs the same image against the same schema in about seventy seconds —
# twenty of suite and fifty of image build.
#
# It is NOT a substitute for the gate. It runs what you name, so it cannot see
# what you broke elsewhere — which is exactly how the /traffic port broke four
# tests in a file it never touched. Run the gate before pushing.
#
# THE BUILD, AND WHEN TO SKIP IT
#
# `tests/` is mounted, `core/` is baked in. So a loop that only edits a test
# needs no rebuild and `SKIP_BUILD=1` is honest there — that is the twenty
# second version. A loop that edits `core/` and skips the build is testing the
# previous image and will be told nothing about it, which is the defect that
# cost `gate_with_stores.sh` a run on 2026-09-12. Building is therefore the
# default in both scripts, and skipping it is the thing you say out loud.
#
# HOW
#
#   bash deploy/quick_gate.sh tests/integration/test_expenses_two_engines.py
#   bash deploy/quick_gate.sh "tests/unit/test_a.py tests/unit/test_b.py"
#   SKIP_BUILD=1 bash deploy/quick_gate.sh tests/unit/test_a.py   # fixture only
#
# ClickHouse is deliberately absent: the tests that need it skip themselves,
# and starting it doubles the setup for files that almost never want it. Use
# the full gate for those.
set -uo pipefail

REPO="${REPO:-/opt/key-api-bot}"
IMAGE="${IMAGE:-keycrm-web:gate}"
NET=ks-quick
PG=quick-pg
# Throwaway credentials for a throwaway store: this container exists for the
# length of one run, publishes no ports, and is on a network of its own.
PW=quick-only

FILES="${1:?usage: quick_gate.sh \"<test file> [test file ...]\"}"

cleanup() {
    docker rm -f -v "$PG" >/dev/null 2>&1 || true
    # This gate builds the web image too, so it fills the same cache
    # `gate_with_stores.sh` bounds — and it is the one run by hand, over and
    # over, while iterating on a test. The bound lived only in the sibling
    # until 2026-09-17, and the cache went 1.6 GB / 90 entries to 2.5 GB / 752
    # in three days. Same shape as the `-v` that leaked here for five days:
    # a bound written into one script is not written into its siblings.
    #
    # `--max-used-space`, not `prune -f` — see the sibling for why emptying a
    # cache that rebounds manufactures next week's WARN. Same default and the
    # same variable, because two gates filling one cache must not disagree
    # about how big it is allowed to get.
    docker builder prune -f --max-used-space "${GATE_CACHE_MAX:-4GB}" \
        >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
}
# One gate on this host at a time, of either kind. The stores have fixed
# names and both gates build the same image tag, so two runs at once — two
# sessions, or a run started before the last one finished — migrate one
# database, test each other's image, and each one's cleanup removes the other's
# stores. Seen 2026-09-24: a gate from a second checkout died at the migration
# on a duplicate `alembic_version`, and its cleanup took the running gate's
# Postgres with it. So the second run waits here instead; the lock goes when
# this shell exits, however it exits.
exec 9>/tmp/ks-gate.lock
if command -v flock >/dev/null 2>&1; then
    flock 9
fi

trap cleanup EXIT

cleanup

# Built, not assumed current — `gate_with_stores.sh` carries the full reason.
if [ "${SKIP_BUILD:-0}" = "1" ]; then
    echo "QUICK: SKIP_BUILD=1 — using $IMAGE as it stands"
    docker image inspect "$IMAGE" >/dev/null 2>&1 \
        || { echo "QUICK: no such image: $IMAGE"; exit 1; }
else
    echo "building $IMAGE from $REPO…"
    docker build -q -f "$REPO/Dockerfile.web" -t "$IMAGE" "$REPO" >/dev/null \
        || { echo "QUICK: image build failed"; exit 1; }
fi

docker network create "$NET" >/dev/null

docker run -d --name "$PG" --network "$NET" \
    -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD="$PW" -e POSTGRES_DB=ks \
    -e POSTGRES_INITDB_ARGS="--locale=C.UTF-8 --encoding=UTF8" \
    -v "$REPO/postgres/initdb:/docker-entrypoint-initdb.d:ro" \
    postgres:17.2-alpine >/dev/null

for _ in $(seq 1 30); do
    docker exec "$PG" psql -U postgres -d ks -tAc "SELECT 1" >/dev/null 2>&1 && break
    sleep 2
done
docker exec "$PG" psql -U postgres -tAc "ALTER ROLE ks_app WITH PASSWORD '$PW'" >/dev/null

# Head, like production: `require_revision` refuses any mismatch.
docker run --rm --user root --network "$NET" \
    -e "KS_PG_DSN=postgresql://ks_app:$PW@$PG:5432/ks" \
    -v "$REPO/migrations:/app/migrations:ro" \
    -v "$REPO/alembic.ini:/app/alembic.ini:ro" \
    --entrypoint sh "$IMAGE" -c \
    'pip install -q alembic >/dev/null 2>&1; cd /app && python -m alembic upgrade head' \
    || { echo "QUICK: migrations failed"; exit 1; }

docker run --rm --user root --network "$NET" \
    -e "KS_PG_DSN=postgresql://ks_app:$PW@$PG:5432/ks" \
    -v "$REPO/tests:/app/tests:ro" \
    -v "$REPO/web/frontend/src:/app/web/frontend/src:ro" \
    -v "$REPO/pytest.ini:/app/pytest.ini:ro" \
    -v "$REPO/requirements-dev.lock:/app/requirements-dev.lock:ro" \
    -v "$REPO/migrations:/app/migrations:ro" \
    -v "$REPO/alembic.ini:/app/alembic.ini:ro" \
    --entrypoint sh "$IMAGE" -c "
        pip install -q --no-warn-script-location -r /app/requirements-dev.lock \
            >/tmp/pip.log 2>&1 || { tail -5 /tmp/pip.log; exit 90; }
        cd /app && python -m pytest -q $FILES 2>&1 | tail -40
    "
