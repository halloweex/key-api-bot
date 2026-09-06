#!/usr/bin/env bash
# The half of PITR that was missing.
#
# Found 2026-08-31: `pg-receivewal` had been streaming since 22.08 into
# backups/pg_wal — 910 segments, 3.9 GB, ~450 MB/day — and there was **no base
# backup anywhere**. WAL is only replayable onto a base backup, so the archive
# could not restore anything: the real recovery point was the 06:50 logical
# dump, i.e. RPO of a day, not the five minutes decided on 2026-08-22. The
# runbook described "base backup + WAL segments" as an existing chain; it was
# describing an intention.
#
# This script is the other link, and it also makes the archive prunable for the
# first time. The two are one script on purpose: the anchor for deleting WAL is
# the oldest base backup's own START WAL LOCATION, so the only moment retention
# is provably safe is immediately after a base backup has succeeded. Splitting
# them into two cron entries invites the pruner to run when the maker did not.
#
# NOT a replacement for `pg_backup.sh`. That is a logical dump and recovers
# from what we broke ourselves — a bad migration, a wrong DELETE — because it
# can be restored selectively into a running cluster. This is a physical copy
# and recovers from what broke under us, to any second in the window. Same
# pairing the sibling script already describes; it just now has both halves.
#
# Cost, measured on this cluster: pgdata is 294 MB, so a compressed base backup
# is tens of MB. Daily is affordable, and daily is what keeps the WAL window —
# and therefore the disk — short.
set -Eeuo pipefail
cd "$(dirname "$0")/.."

STAMP="$(date -u +%Y%m%d-%H%M%S)"
# Three days of PITR window. Raise it and the WAL archive grows with it: at
# ~450 MB/day the window is what sizes the directory, not the backups.
RETAIN="${KS_BASE_RETAIN:-3}"
BASE_DIR="backups/postgres/base"
DRY_RUN=""
[ "${1:-}" = "--dry-run" ] && DRY_RUN="-n"

# Recipients and the kill switch, same as every other host-cron script.
source deploy/notify.sh

fail() {
    echo "FAIL: $1" >&2
    notify "🚨 Postgres base backup failed
$1
→ deploy/pg_basebackup.sh; until it passes, WAL cannot be replayed" \
        "backup:pg_basebackup_failed" || true
    exit 1
}
trap 'fail "unexpected error on line $LINENO"' ERR

echo "── base backup ──────────────────────────────────────────"
mkdir -p "$BASE_DIR"

# Runs *inside* the postgres container, writing to the ./backups/postgres mount
# it already has. That is the whole reason this needs no compose change and no
# container recreate — and a recreate is a real cost here (lesson D3).
#
# -h 127.0.0.1 rather than the local socket: 20-replication-hba.sh grants
# `host replication postgres`, and there is no `local replication` line.
# The password comes from the container's own environment, so it is never on
# this host's command line or in this host's process table.
# -X stream takes its own temporary slot, so a base backup never touches
# ks_pitr and cannot stall the receiver it depends on.
# -c fast checkpoints immediately instead of spreading it over minutes.
docker compose exec -T postgres sh -c '
    set -e
    PGPASSWORD="$POSTGRES_PASSWORD" pg_basebackup \
        --host=127.0.0.1 --username=postgres \
        --pgdata=/backups/base/'"$STAMP"' \
        --format=tar --gzip --wal-method=stream --checkpoint=fast
' || fail "pg_basebackup did not complete"

ARCHIVE="$BASE_DIR/$STAMP/base.tar.gz"
# A backup that failed halfway still leaves a directory. Refuse to call that a
# backup, and — more importantly — refuse to let it anchor WAL retention.
[ -s "$ARCHIVE" ] || { rm -rf "${BASE_DIR:?}/$STAMP"; fail "$ARCHIVE is empty"; }
echo "  $STAMP  $(du -h "$ARCHIVE" | cut -f1)"

echo "── the segment this backup needs ────────────────────────"
# backup_label lives inside the tar. Read it once, here, and leave the answer
# beside the backup: retention runs often and must not depend on re-opening a
# gzipped archive to re-learn a fact that never changes.
LABEL="$(docker compose exec -T postgres \
    tar -xzOf "/backups/base/$STAMP/base.tar.gz" backup_label 2>/dev/null || true)"
SEGMENT="$(printf '%s' "$LABEL" | python3 -c '
import sys
sys.path.insert(0, "deploy")
from wal_retention import parse_backup_label
seg = parse_backup_label(sys.stdin.read())
print(seg or "", end="")
')"
if [ -z "$SEGMENT" ]; then
    # Not fatal: the backup itself is good. But without the anchor this backup
    # cannot license any deletion, and `wal_retention.py` will refuse the whole
    # run rather than prune around it.
    echo "  WARN: could not read START WAL LOCATION — retention will stand down" >&2
else
    printf '%s\n' "$SEGMENT" > "$BASE_DIR/$STAMP/START_WAL"
    echo "  needs WAL from $SEGMENT"
fi

echo "── retention ────────────────────────────────────────────"
# Newest N kept. Deliberately not `find -mtime`: the count is what bounds the
# WAL window, and an age would let a week of failed runs quietly empty this.
ls -1t "$BASE_DIR" 2>/dev/null | tail -n +$((RETAIN + 1)) | while read -r old; do
    [ -n "$old" ] || continue
    echo "  dropping base backup $old"
    rm -rf "${BASE_DIR:?}/${old:?}"
done
echo "  keeping the newest $RETAIN base backup(s)"

echo "── WAL archive ──────────────────────────────────────────"
# The decision is `wal_retention.py` — pure, tested, and refuses on anything it
# cannot prove. The deletion is pg_archivecleanup, which knows about timelines,
# .partial and .history. Neither job belongs to the other.
if KEEP="$(python3 deploy/wal_retention.py "$BASE_DIR" 2>&1)"; then
    BEFORE="$(du -sh backups/pg_wal 2>/dev/null | cut -f1)"
    # -x .gz because pg_receivewal is configured with --compress=gzip:6, so
    # every finished segment carries that suffix and cleanup would otherwise
    # recognise none of them.
    docker compose exec -T pg-receivewal \
        pg_archivecleanup $DRY_RUN -x .gz /wal "$KEEP" \
        || fail "pg_archivecleanup failed against $KEEP"
    AFTER="$(du -sh backups/pg_wal 2>/dev/null | cut -f1)"
    echo "  kept from $KEEP${DRY_RUN:+ (dry run)}: $BEFORE → $AFTER"
else
    # Printed by the helper, on stderr, saying which condition refused.
    echo "  standing down: $KEEP"
fi

echo "── done ─────────────────────────────────────────────────"
echo "Prove it restores:  deploy/pg_restore_drill.sh"
