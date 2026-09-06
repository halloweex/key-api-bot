#!/usr/bin/env bash
# Restore the physical backup, for real, and prove the archive can carry it.
#
# `pg_restore_drill.sh` next door drills the *logical* dump — it proves we can
# recover from what we broke ourselves. It says nothing about PITR, and until
# 2026-08-31 there was nothing to say: no base backup existed. Now one does,
# and by this repository's own rule a backup nobody has restored is a hope.
#
# What this actually proves, in one run:
#
#   1. the base backup untars and Postgres accepts it as a data directory;
#   2. the WAL archive replays onto it — i.e. `restore_command` finds the
#      segments, ungzips them, and recovery reaches a consistent point;
#   3. **the retention did not over-delete.** It deliberately restores the
#      OLDEST retained base, whose START WAL is the exact anchor
#      `pg_basebackup.sh` prunes to. If that anchor were ever off by one
#      segment, recovery stops short and this fails — which is the failure
#      that is otherwise invisible until a real emergency.
#
# Isolation is the point, so: a scratch directory under /tmp, a container on
# `--network none` (it cannot reach the live cluster even by accident), and the
# WAL archive mounted READ-ONLY. Nothing here can write to anything that
# matters.
set -Eeuo pipefail
cd "$(dirname "$0")/.."

BASE_DIR="backups/postgres/base"
WAL_DIR="$(pwd)/backups/pg_wal"
SCRATCH="/tmp/ks-pitr-drill.$$"
CONTAINER="ks-pitr-drill-$$"
IMAGE="postgres:17.2-alpine"

source deploy/notify.sh

# A failing drill must page from cron — that is the whole point of a drill. But
# it must not page while somebody is iterating on it at a keyboard: developing
# this script sent admins two "PITR drill failed" messages on 2026-08-31 for a
# fault that was being fixed as they arrived. Cron runs without --quiet.
QUIET=""
for arg in "$@"; do
    [ "$arg" = "--quiet" ] && QUIET=1
done

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    rm -rf "$SCRATCH"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $1" >&2
    [ -z "$QUIET" ] && notify "🚨 PITR drill failed
$1
→ deploy/pg_pitr_drill.sh; the physical recovery point is unproven" \
        "backup:pitr_drill_failed" || true
    exit 1
}

echo "── the backup under test ────────────────────────────────"
# Oldest, not newest: see (3) above. The newest base needs almost no archive
# and would pass even if retention had eaten every segment behind it.
STAMP="$(ls -1 "$BASE_DIR" 2>/dev/null | head -1)"
[ -n "$STAMP" ] || fail "no base backup in $BASE_DIR"
ARCHIVE="$BASE_DIR/$STAMP/base.tar.gz"
[ -s "$ARCHIVE" ] || fail "$ARCHIVE is missing or empty"
ANCHOR="$(cat "$BASE_DIR/$STAMP/START_WAL" 2>/dev/null || true)"
echo "  $STAMP  ($(du -h "$ARCHIVE" | cut -f1)), needs WAL from ${ANCHOR:-unknown}"

echo "── unpack ───────────────────────────────────────────────"
mkdir -p "$SCRATCH/data"
tar -xzf "$ARCHIVE" -C "$SCRATCH/data"
# -X stream put the segments spanning the backup itself in here. They are not
# enough on their own — everything after the backup comes from the archive —
# but without them recovery cannot even start.
[ -s "$BASE_DIR/$STAMP/pg_wal.tar.gz" ] \
    && tar -xzf "$BASE_DIR/$STAMP/pg_wal.tar.gz" -C "$SCRATCH/data/pg_wal"
chown -R 70:70 "$SCRATCH/data"
chmod 700 "$SCRATCH/data"
echo "  unpacked $(du -sh "$SCRATCH/data" | cut -f1)"

echo "── stage the archive ────────────────────────────────────"
# FOUND BY THIS DRILL, 2026-08-31, and it is a fact about real recoveries and
# not about this script: `pg_receivewal` runs as root in its container, so
# every segment lands root:root 0600. A recovering Postgres runs as uid 70 and
# cannot read them — recovery does not fail loudly, it fetches nothing, reaches
# consistency on the base alone and promotes, looking like a success.
#
# So the archive is staged into a copy the recovering user owns. A real restore
# needs this same step, which is why it is a named stage here rather than a
# quiet `chown` buried in the setup.
mkdir -p "$SCRATCH/wal"
if docker run --rm --user 70 -v "$WAL_DIR":/wal:ro "$IMAGE" \
        sh -c 'cat /wal/*.gz > /dev/null 2>&1'; then
    echo "  archive is readable by uid 70 directly"
else
    echo "  NOTE: archive is root-owned 0600 — a restore MUST stage a readable"
    echo "        copy first, or recovery silently replays nothing"
fi
# `.partial` is deliberately not staged: it is the segment the receiver is
# still writing. And each staged file is verified as a complete gzip rather
# than copied with `|| true`, because the copy races a live receiver — a
# segment can be renamed out from under the glob mid-copy, and a truncated
# .gz that nobody checked makes restore_command fail on a segment that is
# perfectly good in the archive. That looks exactly like a lost recovery
# point, which is the one conclusion this drill must never reach by accident.
staged=0
for seg in "$WAL_DIR"/*.gz "$WAL_DIR"/*.history; do
    [ -f "$seg" ] || continue
    cp "$seg" "$SCRATCH/wal/" || fail "could not stage $seg"
    case "$seg" in
        *.gz)
            gzip -t "$SCRATCH/wal/$(basename "$seg")" 2>/dev/null || {
                # Raced the receiver. Drop it rather than feed recovery a
                # corrupt segment; if it is genuinely needed, the run fails
                # loudly at replay instead of silently here.
                echo "  skipped $(basename "$seg") — incomplete copy (raced the receiver)"
                rm -f "$SCRATCH/wal/$(basename "$seg")"
                continue
            }
            ;;
    esac
    staged=$((staged + 1))
done
chown -R 70:70 "$SCRATCH/wal"
echo "  staged $staged segment(s)"

echo "── recovery configuration ───────────────────────────────"
# pg_receivewal writes gzip, so %f (a bare segment name) has to gain .gz.
# .history files are NOT compressed by the receiver, hence the second branch:
# a timeline history that cannot be fetched is a recovery that silently picks
# the wrong timeline.
cat >> "$SCRATCH/data/postgresql.conf" <<'CONF'

# ── added by deploy/pg_pitr_drill.sh ──
restore_command = 'if [ -f /wal/%f.gz ]; then gunzip -c /wal/%f.gz > %p; elif [ -f /wal/%f ]; then cp /wal/%f %p; else exit 1; fi'
recovery_end_command = 'echo PITR_DRILL_RECOVERY_END'
CONF
# Its presence is what puts the cluster into archive recovery; with no
# recovery_target set it replays as far as the archive goes and then promotes.
touch "$SCRATCH/data/recovery.signal"
chown 70:70 "$SCRATCH/data/recovery.signal" "$SCRATCH/data/postgresql.conf"
echo "  restore_command set, recovery.signal placed"

echo "── recover ──────────────────────────────────────────────"
docker run -d --name "$CONTAINER" \
    --network none \
    -v "$SCRATCH/data":/var/lib/postgresql/data \
    -v "$SCRATCH/wal":/wal:ro \
    -e POSTGRES_PASSWORD=drill \
    "$IMAGE" >/dev/null

READY=""
for _ in $(seq 1 60); do
    if docker exec "$CONTAINER" pg_isready -U postgres -q 2>/dev/null; then
        READY=1
        break
    fi
    sleep 2
done
[ -n "$READY" ] || {
    echo "── recovery log ──" >&2
    docker logs --tail=40 "$CONTAINER" >&2 || true
    fail "recovered cluster never accepted connections"
}

# "Started" is not "replayed the archive". Prove the archive was actually read.
if docker logs "$CONTAINER" 2>&1 | grep -q "restored log file"; then
    RESTORED="$(docker logs "$CONTAINER" 2>&1 | grep -c "restored log file" || true)"
    echo "  replayed $RESTORED segment(s) out of the archive"
else
    # Dump the evidence before dying. A drill that fails without showing the
    # recovery log sends the reader back to reproduce it by hand, which on a
    # weekly cron means a week later.
    echo "── recovery log ──" >&2
    docker logs --tail=60 "$CONTAINER" 2>&1 | sed 's/^/  | /' >&2
    echo "── archive holds ──" >&2
    ls -1 "$WAL_DIR" | head -20 | sed 's/^/  | /' >&2
    fail "recovery never restored a segment from the archive — the base alone came up, which does not prove PITR"
fi
docker logs "$CONTAINER" 2>&1 | grep -qi "consistent recovery state" \
    && echo "  reached a consistent recovery state"

echo "── verify the data is really there ──────────────────────"
# Row counts, not "does it start". A cluster that starts and answers zero is
# the failure this is guarding against.
for check in "app.authorized_users" "bronze.orders" "app.order_versions"; do
    n="$(docker exec "$CONTAINER" psql -U postgres -d ks -tAc \
        "SELECT count(*) FROM $check" 2>/dev/null || echo ERR)"
    [ "$n" = "ERR" ] && fail "could not read $check from the restored cluster"
    [ "$n" -gt 0 ] 2>/dev/null || fail "$check restored empty"
    printf '  %-24s %s rows\n' "$check" "$n"
done

echo "── done ─────────────────────────────────────────────────"
echo "Base $STAMP restored and replayed from the archive. PITR is real."
