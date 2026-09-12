#!/usr/bin/env bash
# Run the suite inside the production image, with a real PostgreSQL and a real
# ClickHouse beside it.
#
# WHY THIS EXISTS
#
# The differential tests — the ones that prove DuckDB, PostgreSQL and
# ClickHouse answer the same question the same way — skip themselves when
# `KS_PG_DSN` and `KS_CH_URL` are unset. Every deploy before 2026-08-31 ran the
# gate without them, so **80 tests skipped**, including every proof that the
# ported /sms audience and the cohort matrix agree between engines. A check
# that does not run is not a check, and those were the checks the two ports
# rested on.
#
# With the stores up: 2823 passed, 6 skipped. Without: 2739 passed, 80 skipped.
# The first deploy that ran it this way found a test that had never executed
# and could not have passed — `ks_app` may not create temporary tables, because
# `initdb` revokes TEMP along with everything else it takes from PUBLIC.
#
# HOW
#
#   bash deploy/gate_with_stores.sh          # from /opt/key-api-bot on the VPS
#
# Exits non-zero if the suite does. Tears the stores down either way — they are
# throwaway, on their own network, and touch neither `ks-postgres` nor
# `ks-clickhouse`.
set -uo pipefail

REPO="${REPO:-/opt/key-api-bot}"
# BUILT below, never pulled — see the block after `cleanup`. A registry image
# here is the defect this default replaced.
IMAGE="${IMAGE:-keycrm-web:gate}"
NET=ks-gate
PG=gate-pg
CH=gate-ch
# Throwaway credentials for throwaway stores: these containers exist for the
# length of one suite run, publish no ports, and are on a network of their own.
PW=gate-only

cleanup() {
    # `-v`, or the gate leaks two volumes per run. Both images declare a VOLUME,
    # so each `docker run` mints an anonymous one; removing the container without
    # `-v` orphans it. Measured on the VPS 2026-09-09: **173 dangling volumes,
    # 7.13 GB**, which is what took the disk watchdog from WARN on 07.09 to
    # CRITICAL on 08.09 — the gate had quietly become the largest thing growing
    # on the host.
    #
    # Safe by construction, which is why it is this flag and not a prune:
    # `docker rm -v` removes only the anonymous volumes attached to the named
    # containers. It cannot reach a named volume — and one of the orphans here
    # *is* named (`key-api-bot_app-data`), so a blanket `docker volume prune`
    # would be a different and much worse operation.
    docker rm -f -v "$PG" "$CH" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup

# ─── The image is built here, from the working tree ─────────────────────────
#
# It used to default to `halloweex/keycrm-web:latest` — the *published* image —
# with the repository's `tests/` mounted over it. So the gate ran the branch's
# tests against the previous release's `core/`, and said nothing about it.
#
# On 2026-09-12 that surfaced as an import error, because the change under test
# added a new module and the published image did not have it. That was luck: a
# change that only edits an existing module would have gone green while testing
# code that was never in the branch. This script is what decides whether
# something ships, so it must not be able to test the wrong code — and a check
# that can quietly check the wrong thing is the failure mode the whole
# `KS_PG_DSN` block above exists to prevent.
#
# Building rather than verifying, deliberately: a comparison of the image's
# `core/` against the tree would still leave a way to be wrong, and only tells
# you afterwards. Measured on the VPS with a warm layer cache and an unchanged
# tree: **48 s**, against a suite that takes thirteen minutes.
#
# `SKIP_BUILD=1` reuses whatever `$IMAGE` names — for a second run on a tree
# that has not moved, or to gate an image you pulled on purpose.
if [ "${SKIP_BUILD:-0}" = "1" ]; then
    echo "GATE: SKIP_BUILD=1 — using $IMAGE as it stands"
    docker image inspect "$IMAGE" >/dev/null 2>&1 \
        || { echo "GATE: no such image: $IMAGE"; exit 1; }
else
    echo "building $IMAGE from $REPO…"
    docker build -q -f "$REPO/Dockerfile.web" -t "$IMAGE" "$REPO" >/dev/null \
        || { echo "GATE: image build failed"; exit 1; }
fi

docker network create "$NET" >/dev/null

docker run -d --name "$PG" --network "$NET" \
    -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD="$PW" -e POSTGRES_DB=ks \
    -e POSTGRES_INITDB_ARGS="--locale=C.UTF-8 --encoding=UTF8" \
    -v "$REPO/postgres/initdb:/docker-entrypoint-initdb.d:ro" \
    postgres:17.2-alpine >/dev/null

# The production version, deliberately: ClickHouse's SQL surface differs
# between releases, and this gate exists to catch exactly that class of thing.
docker run -d --name "$CH" --network "$NET" \
    -e CLICKHOUSE_PASSWORD="$PW" \
    clickhouse/clickhouse-server:24.8.14.39-alpine >/dev/null

echo "waiting for the stores…"
for _ in $(seq 1 30); do
    docker exec "$PG" psql -U postgres -d ks -tAc "SELECT 1" >/dev/null 2>&1 \
        && docker exec "$CH" clickhouse-client --password "$PW" -q "SELECT 1" >/dev/null 2>&1 \
        && break
    sleep 2
done
docker exec "$PG" psql -U postgres -tAc "ALTER ROLE ks_app WITH PASSWORD '$PW'" >/dev/null

# The schema the application demands. `require_revision` refuses any mismatch,
# so the suite needs head here just as production does.
docker run --rm --user root --network "$NET" \
    -e "KS_PG_DSN=postgresql://ks_app:$PW@$PG:5432/ks" \
    -v "$REPO/migrations:/app/migrations:ro" \
    -v "$REPO/alembic.ini:/app/alembic.ini:ro" \
    --entrypoint sh "$IMAGE" -c \
    'pip install -q alembic >/dev/null 2>&1; cd /app && python -m alembic upgrade head' \
    || { echo "GATE: migrations failed"; exit 1; }

# Repo files the runtime image deliberately does not carry. A dozen tests read
# the Dockerfiles, the lockfiles, the CI workflow and the nginx config; they
# are not runtime tests, and without these they fail on collection rather than
# on substance. `nginx/` joined the list on 2026-09-06, when a test asserting
# the cache headers passed on the laptop and failed here for want of the file —
# add the mount when you add the reader.
docker run --rm --user root --network "$NET" \
    -e "KS_PG_DSN=postgresql://ks_app:$PW@$PG:5432/ks" \
    -e "KS_CH_URL=http://$CH:8123" -e KS_CH_USER=default -e "KS_CH_PASSWORD=$PW" \
    -v "$REPO/tests:/app/tests:ro" \
    -v "$REPO/pytest.ini:/app/pytest.ini:ro" \
    -v "$REPO/migrations:/app/migrations:ro" \
    -v "$REPO/alembic.ini:/app/alembic.ini:ro" \
    -v "$REPO/.github:/app/.github:ro" \
    -v "$REPO/deploy:/app/deploy:ro" \
    -v "$REPO/docker-compose.yml:/app/docker-compose.yml:ro" \
    -v "$REPO/nginx:/app/nginx:ro" \
    -v "$REPO/Dockerfile:/app/Dockerfile:ro" \
    -v "$REPO/Dockerfile.web:/app/Dockerfile.web:ro" \
    -v "$REPO/Dockerfile.migrate:/app/Dockerfile.migrate:ro" \
    -v "$REPO/requirements.txt:/app/requirements.txt:ro" \
    -v "$REPO/requirements.lock:/app/requirements.lock:ro" \
    -v "$REPO/requirements-dev.txt:/app/requirements-dev.txt:ro" \
    -v "$REPO/requirements-dev.lock:/app/requirements-dev.lock:ro" \
    -v "$REPO/web/frontend/src:/app/web/frontend/src:ro" \
    --entrypoint sh "$IMAGE" -c '
        pip install -q --no-warn-script-location -r /app/requirements-dev.lock \
            >/tmp/pip.log 2>&1 || { tail -5 /tmp/pip.log; exit 90; }
        cd /app
        # To a FILE, never a pipe: `| tail` returns tail′s status and one
        # release went out red because of it.
        pytest -q > /tmp/suite.txt 2>&1
        RC=$?
        echo "SUITE_RC=$RC"
        grep "^FAILED" /tmp/suite.txt | head -20
        tail -3 /tmp/suite.txt
        exit $RC
    '
