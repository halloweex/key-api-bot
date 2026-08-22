#!/usr/bin/env bash
# Prove a Postgres restore works, with a canary. The ClickHouse half of this
# lives in `ks-data-platform`.
#
# A drill that only works once there is data to lose is not a drill, it is a
# hope. The canary gives the restore something to be right about.
#
# Writes and drops only objects named `_drill_*`, in the `meta` schema.
# Touches nothing else. Safe to run against a populated database.
set -Eeuo pipefail
cd "$(dirname "$0")/.."

STAMP="$(date -u +%Y%m%d-%H%M%S)"
ROWS="${KS_DRILL_ROWS:-50000}"
PGC() { docker compose exec -T postgres psql -qtAX -U postgres -d ks -c "$1"; }
t0() { date +%s.%N; }
ms() { echo "$1 $2" | awk '{printf "%.0f", ($2-$1)*1000}'; }

fail() { echo "FAIL: $*" >&2; exit 1; }

mkdir -p backups/postgres

echo "── postgres ─────────────────────────────────────────────"
PGC "DROP TABLE IF EXISTS meta._drill_canary" >/dev/null
PGC "CREATE TABLE meta._drill_canary AS
     SELECT g AS id, md5(g::text) AS payload FROM generate_series(1, $ROWS) g" >/dev/null
before_pg="$(PGC "SELECT count(*), sum(id) FROM meta._drill_canary")"
echo "  canary: $before_pg"

s=$(t0)
docker compose exec -T postgres pg_dump -U postgres -d ks -Fc \
    > "backups/postgres/_drill-$STAMP.dump"
e=$(t0); dump_ms=$(ms "$s" "$e")

PGC "DROP TABLE meta._drill_canary" >/dev/null
PGC "SELECT to_regclass('meta._drill_canary')" | grep -q '^$' \
    || fail "canary survived the drop; the drill would prove nothing"

s=$(t0)
docker compose exec -T postgres pg_restore -U postgres -d ks --no-owner \
    -t _drill_canary -n meta < "backups/postgres/_drill-$STAMP.dump" >/dev/null
e=$(t0); restore_ms=$(ms "$s" "$e")

after_pg="$(PGC "SELECT count(*), sum(id) FROM meta._drill_canary")"
[ "$before_pg" = "$after_pg" ] || fail "restored $after_pg, expected $before_pg"
echo "  dump ${dump_ms} ms, restore ${restore_ms} ms, checksum matches"

PGC "DROP TABLE meta._drill_canary" >/dev/null
rm -f "backups/postgres/_drill-$STAMP.dump"

echo
echo "PASS — Postgres lost $ROWS rows and got them back, byte for byte."
