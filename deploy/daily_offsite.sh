#!/usr/bin/env bash
# Nightly: take a logical snapshot of last night's backup and ship it off-site.
#
# This is what takes off-host RPO from a week to a day. The weekly compact
# already ships its own export, but weekly was only ever the cadence the
# compact happened to run at — nothing about the artifact required it.
#
# The source is the newest nightly backup, not the live database, and that is
# the whole trick. The backup is already a consistent copy and nothing writes
# to it again, so this reads it with no lock and no contention: the running
# system does not notice the daily job at all. compact_duckdb.py addresses
# /app/data by module constant, so the backup is hard-linked into place as
# analytics.duckdb — instant, no second copy on disk, and the proven export
# path runs unmodified.
#
# Install:
#   45 2 * * *  /opt/key-api-bot/deploy/daily_offsite.sh
#
# 02:45 UTC is after the 01:30 backup and after the Sunday compact finishes
# (~02:01), so it never races either.
set -Eeuo pipefail

cd "$(dirname "$0")/.."

CONFIG="deploy/backup.env"
# shellcheck source=/dev/null
[ -f "$CONFIG" ] && . "$CONFIG"

BACKUP_DIR="${BACKUP_LOCAL_DIR:-data/backups}"
IMAGE="${BACKUP_RESTORE_IMAGE:-halloweex/keycrm-web:latest}"
WORK="data/.snapshot_tmp"

EX_UNCONFIGURED=78

cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

_env_value() { grep -m1 "^$1=" .env 2>/dev/null | cut -d= -f2- | tr -d '\r' || true; }

notify() {
    local text="$1" token chat
    token="$(_env_value BOT_TOKEN)"
    chat="${BACKUP_ALERT_CHAT_ID:-$(_env_value ADMIN_USER_IDS | cut -d, -f1 | tr -d '[:space:]' || true)}"
    [ -n "$token" ] && [ -n "$chat" ] || { echo "cannot alert: credentials missing from .env" >&2; return 0; }
    curl -sS -m 15 -o /dev/null \
        --data-urlencode "chat_id=$chat" \
        --data-urlencode "text=$text" \
        "https://api.telegram.org/bot${token}/sendMessage" || echo "alert delivery failed" >&2
}

on_error() {
    local rc=$?
    trap - ERR
    set +e
    if [ "$rc" -ne "$EX_UNCONFIGURED" ]; then
        notify "$(printf '%s\n\n%s\n\n%s' \
            "❌ Daily snapshot FAILED" \
            "No fresh copy left the server tonight. The most recent off-site archive is still there; this is about it getting older, not disappearing." \
            "$(hostname) · $(date '+%F %H:%M')")"
    fi
    exit "$rc"
}
trap on_error ERR

LATEST="$(ls -t "$BACKUP_DIR"/analytics-*.duckdb 2>/dev/null | head -1 || true)"
if [ -z "$LATEST" ]; then
    echo "no nightly backup in $BACKUP_DIR — has db_backup run?" >&2
    exit 1
fi

# A backup older than a day means db_backup stopped, and shipping it under
# tonight's date would paper over that.
AGE_H=$(( ( $(date +%s) - $(stat -c %Y "$LATEST") ) / 3600 ))
if [ "$AGE_H" -gt "${BACKUP_MAX_LOCAL_AGE_HOURS:-30}" ]; then
    echo "newest backup is ${AGE_H}h old — db_backup has stopped; not shipping a stale snapshot as today's" >&2
    exit 1
fi
echo "source: $(basename "$LATEST") (${AGE_H}h old)"

rm -rf "$WORK"
mkdir -p "$WORK"
# Hard link, not a copy: same inode, instant, and no second multi-GB file on a
# disk this whole exercise exists to stop filling.
ln "$LATEST" "$WORK/analytics.duckdb"

# Carry yesterday's counts in so the validator can compare against them. $WORK
# is recreated every night, and the container sees nothing outside it.
[ -f data/.last_snapshot.json ] && cp data/.last_snapshot.json "$WORK/.last_snapshot.json"

docker run --rm \
    --memory=6500m \
    -e DUCKDB_MEMORY_LIMIT=4GB \
    -v "$PWD/$WORK:/app/data" \
    -v "$PWD/deploy/snapshot_export.py:/app/deploy/snapshot_export.py:ro" \
    -v "$PWD/core/snapshot_validation.py:/app/core/snapshot_validation.py:ro" \
    --env-file .env \
    "$IMAGE" \
    python /app/deploy/snapshot_export.py

# The validator writes .last_snapshot.json inside the container's /app/data,
# which is $WORK and about to be deleted. Keep it where tomorrow can read it.
if [ -f "$WORK/.last_snapshot.json" ]; then
    mv -f "$WORK/.last_snapshot.json" data/.last_snapshot.json
fi

BACKUP_EXPORT_DIR="$WORK/export_parquet" ./deploy/offsite_parquet.sh
