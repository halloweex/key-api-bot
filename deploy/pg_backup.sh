#!/usr/bin/env bash
# Back up Postgres. The ClickHouse half of this lives in `ks-data-platform`,
# because that is where ClickHouse lives — see deploy/postgres-move-runbook.md.
#
# Rule 2 of the migration charter: backups and a rehearsed restore are set up
# before data arrives, not after. Data loss during a migration is the one
# outcome later work cannot fix.
#
# This is a logical dump, and it is NOT the recovery point. `pg-receivewal`
# streams WAL continuously into backups/pg_wal and gives an RPO of seconds;
# this dump is the other half of the pair, and the two cover different
# failures — the dump recovers from what we broke ourselves, the WAL from what
# broke under us. Neither replaces the other.
set -Eeuo pipefail
cd "$(dirname "$0")/.."

STAMP="$(date -u +%Y%m%d-%H%M%S)"
RETAIN="${KS_BACKUP_RETAIN:-14}"

mkdir -p backups/postgres

echo "── postgres ─────────────────────────────────────────────"
# Roles live outside any single database, so they are dumped separately or a
# restore comes back with tables nobody can log in to read.
docker compose exec -T postgres pg_dumpall -U postgres --roles-only \
    > "backups/postgres/roles-$STAMP.sql"
docker compose exec -T postgres pg_dump -U postgres -d ks -Fc \
    > "backups/postgres/ks-$STAMP.dump"

# A dump that failed halfway still leaves a file. Refuse to call that a backup,
# and refuse to let retention count it as one of the copies we are keeping.
for f in "backups/postgres/roles-$STAMP.sql" "backups/postgres/ks-$STAMP.dump"; do
    if [ ! -s "$f" ]; then
        rm -f "$f"
        echo "FAIL: $f is empty — dump did not complete" >&2
        exit 1
    fi
done
ls -la "backups/postgres/ks-$STAMP.dump" | awk '{print "  ks-'"$STAMP"'.dump", $5, "bytes"}'

echo "── retention ────────────────────────────────────────────"
# Deliberately not `find -delete` on the whole tree: a glob that matches what it
# was meant to match is worth more than a clever one that matches everything.
#
# backups/pg_wal is NOT pruned here, and the reason written here until
# 2026-08-31 was wrong: `max_slot_wal_keep_size` bounds the *server's* own
# pg_wal under the replication slot and has no opinion about the directory
# `pg_receivewal` writes into. Nothing pruned it, so it reached 910 segments
# and 3.9 GB at ~450 MB/day, unbounded.
#
# The instinct behind the wrong reason was right, though: deleting segments
# from underneath the archive is how a recovery point stops existing without
# anyone noticing. What licenses a deletion is not an age but a base backup —
# so retention lives in `deploy/pg_basebackup.sh`, anchored to the oldest one
# still held, and refuses when there is none.
ls -1t backups/postgres/ks-*.dump    2>/dev/null | tail -n +$((RETAIN + 1)) | xargs -r rm -f
ls -1t backups/postgres/roles-*.sql  2>/dev/null | tail -n +$((RETAIN + 1)) | xargs -r rm -f
echo "  keeping the newest $RETAIN of each"
