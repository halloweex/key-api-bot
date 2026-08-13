#!/usr/bin/env bash
# Restore drill: proves the off-site archive can actually rebuild the warehouse.
#
# Pulls the newest archive from the Storage Box — NOT the local export — because
# the point is to exercise the copy that survives losing this server. Unpacks it
# into a throwaway directory, rebuilds a database from the Parquet, and runs the
# compaction script's own validations: every table's row count against the
# manifest, the orders date range, total revenue to within ₴1, and primary-key
# uniqueness on nine tables.
#
# Touches nothing canonical. The live database, the daily backups and the local
# export are never opened; the drill writes only inside its own temp directory,
# and phase 4 (the swap) is not called.
#
# Run it after setting backups up, and quarterly after that. docs/backup_runbook.md
# has said "test a restore quarterly" since the backup job was written, and the
# reason it never happened is that nobody had a command to run. This is it.
set -Eeuo pipefail

cd "$(dirname "$0")/.."

CONFIG="deploy/backup.env"
# shellcheck source=/dev/null
[ -f "$CONFIG" ] && . "$CONFIG"

SSH_PORT="${BACKUP_SSH_PORT:-23}"
SSH_KEY="${BACKUP_SSH_KEY:-$HOME/.ssh/storagebox_ed25519}"
REMOTE="${BACKUP_REMOTE:-}"
REMOTE_DIR="${BACKUP_REMOTE_DIR:-key-api-bot}"
IMAGE="${BACKUP_RESTORE_IMAGE:-halloweex/keycrm-web:latest}"
MAX_AGE_HOURS="${BACKUP_MAX_ARCHIVE_AGE_HOURS:-216}"   # 9 days: weekly + slack

SSH_OPTS=(-p "$SSH_PORT" -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)

if [ -z "$REMOTE" ]; then
    echo "FAIL: BACKUP_REMOTE is not set — there is no off-site copy to test." >&2
    echo "      See deploy/backup.env.example and docs/offsite_backup_setup.md" >&2
    exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# --- pick the newest off-site archive ---------------------------------------
LATEST="$(printf 'cd %s\nls -1\n' "$REMOTE_DIR" \
  | sftp -b - "${SSH_OPTS[@]}" "$REMOTE" 2>/dev/null \
  | grep -o 'ks-warehouse-[0-9]\{8\}-[0-9]\{6\}\.tar' | sort -r | head -1)"

if [ -z "$LATEST" ]; then
  echo "FAIL: no archives found at $REMOTE:$REMOTE_DIR" >&2
  exit 1
fi
echo "restoring from off-site: $LATEST"

printf 'cd %s\nget %s %s/\n' "$REMOTE_DIR" "$LATEST" "$TMP" \
  | sftp -b - "${SSH_OPTS[@]}" "$REMOTE" >/dev/null

# A backup that stopped happening looks exactly like a backup that works, until
# you need it — so check how old the newest one is. (GNU date; on the server.)
HUMAN="$(echo "$LATEST" | sed -E 's/ks-warehouse-(....)(..)(..)-(..)(..)(..).*/\1-\2-\3 \4:\5:\6/')"
if TAKEN_AT="$(date -u -d "$HUMAN" +%s 2>/dev/null)"; then
  AGE_HOURS=$(( ( $(date +%s) - TAKEN_AT ) / 3600 ))
  echo "archive age: ${AGE_HOURS}h"
  [ "$AGE_HOURS" -le "$MAX_AGE_HOURS" ] \
    || echo "WARNING: newest off-site archive is over $(( MAX_AGE_HOURS / 24 ))d old — is the weekly compact still running it?" >&2
fi

# --- unpack into a scratch data dir -----------------------------------------
mkdir -p "$TMP/data"
tar -xf "$TMP/$LATEST" -C "$TMP/data"

if [ ! -f "$TMP/data/export_parquet/_manifest.json" ]; then
  echo "FAIL: archive has no export_parquet/_manifest.json" >&2
  exit 1
fi

PARQUET_COUNT="$(find "$TMP/data/export_parquet" -name '*.parquet' | wc -l | tr -d ' ')"
echo "unpacked: $PARQUET_COUNT parquet files"

if [ -f "$TMP/data/env.gpg" ]; then
  echo "archive carries an encrypted .env (not decrypted by this drill)"
else
  echo "NOTE: archive carries no .env — a real restore would need the config rebuilt by hand" >&2
fi

# --- rebuild and validate ---------------------------------------------------
# COMPACT_AUTO_SWAP is deliberately absent: restore_from_export.py never calls
# phase 4, and this makes that true twice.
echo "rebuilding in $IMAGE ..."
docker run --rm \
  --memory=6500m \
  -e DUCKDB_MEMORY_LIMIT=4GB \
  -v "$TMP/data:/app/data" \
  --env-file .env \
  "$IMAGE" \
  python /app/deploy/restore_from_export.py

echo
echo "PASS — the off-site archive rebuilt a validated warehouse."
echo "Scratch directory discarded; nothing canonical was touched."
