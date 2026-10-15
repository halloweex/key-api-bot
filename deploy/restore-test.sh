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
#
# In the week of silence (stage 5, KS_DUCKDB=off in .env) it still runs: its
# sidecar is started with `-e KS_DUCKDB=on`, which docker ranks above
# `--env-file`, because the only DuckDB file it can open is the one it builds
# in its scratch directory. And the newest archive is then frozen, not stale:
# the nightly snapshot stands down for the week, so its age is the week's.
set -Eeuo pipefail

cd "$(dirname "$0")/.."

CONFIG="deploy/backup.env"
# shellcheck source=/dev/null
[ -f "$CONFIG" ] && . "$CONFIG"

# KS_DUCKDB out of .env by exact name (deploy/duckdb_env.sh), never listed.
# shellcheck source=/dev/null
source deploy/duckdb_env.sh
ks_duckdb_env_read .env
if [ "$KS_DUCKDB_ENV" = refused ]; then
  echo "FAIL: .env writes KS_DUCKDB with export, or with a space around its = (or after a colon), and docker run --env-file refuses the whole file over it: the rebuild cannot start from it, whatever -e says. Write KS_DUCKDB=off (or on) as a line of its own." >&2
  exit 1
fi

SSH_PORT="${BACKUP_SSH_PORT:-23}"
SSH_KEY="${BACKUP_SSH_KEY:-$HOME/.ssh/storagebox_ed25519}"
REMOTE="${BACKUP_REMOTE:-}"
REMOTE_DIR="${BACKUP_REMOTE_DIR:-key-api-bot}"
IMAGE="${BACKUP_RESTORE_IMAGE:-halloweex/keycrm-web:latest}"
MAX_AGE_HOURS="${BACKUP_MAX_ARCHIVE_AGE_HOURS:-36}"   # nightly + slack

# Two arrays, because the port flag is not the same letter in both tools:
# ssh (and so rsync -e ssh) takes -p, sftp takes -P and uses -p for "preserve
# permissions". Sharing one array silently sent `-p 23` to sftp, which read 23
# as a hostname, printed its usage and exited 1 — so rsync shipped the archive
# and every sftp call after it failed. Do not merge these back together.
SSH_OPTS=(-p "$SSH_PORT" -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)
SFTP_OPTS=(-P "$SSH_PORT" -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)

if [ -z "$REMOTE" ]; then
    echo "FAIL: BACKUP_REMOTE is not set — there is no off-site copy to test." >&2
    echo "      See deploy/backup.env.example and docs/offsite_backup_setup.md" >&2
    exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# --- pick the newest off-site archive ---------------------------------------
LATEST="$(printf 'cd %s\nls -1\n' "$REMOTE_DIR" \
  | sftp -b - "${SFTP_OPTS[@]}" "$REMOTE" 2>/dev/null \
  | grep -o 'ks-warehouse-[0-9]\{8\}-[0-9]\{6\}\.tar' | sort -r | head -1)"

if [ -z "$LATEST" ]; then
  echo "FAIL: no archives found at $REMOTE:$REMOTE_DIR" >&2
  exit 1
fi
echo "restoring from off-site: $LATEST"

printf 'cd %s\nget %s %s/\n' "$REMOTE_DIR" "$LATEST" "$TMP" \
  | sftp -b - "${SFTP_OPTS[@]}" "$REMOTE" >/dev/null

# A backup that stopped happening looks exactly like a backup that works, until
# you need it — so check how old the newest one is. (GNU date; on the server.)
HUMAN="$(echo "$LATEST" | sed -E 's/ks-warehouse-(....)(..)(..)-(..)(..)(..).*/\1-\2-\3 \4:\5:\6/')"
if TAKEN_AT="$(date -u -d "$HUMAN" +%s 2>/dev/null)"; then
  AGE_HOURS=$(( ( $(date +%s) - TAKEN_AT ) / 3600 ))
  echo "archive age: ${AGE_HOURS}h"
  if ks_duckdb_stands_down; then
    echo "archive frozen since the week of silence began: .env says KS_DUCKDB=off, so the nightly snapshot stands down and this is the last archive it shipped"
  elif [ "$AGE_HOURS" -gt "$MAX_AGE_HOURS" ]; then
    echo "WARNING: newest off-site archive is over $(( MAX_AGE_HOURS / 24 ))d old — is the weekly compact still running it?" >&2
  fi
fi

# --- unpack into a scratch data dir -----------------------------------------
mkdir -p "$TMP/data"
tar -xf "$TMP/$LATEST" -C "$TMP/data"

# The container runs as a non-root user and needs to both read the archive and
# write the rebuilt database beside it, so this needs w as well as rX — read
# alone gets as far as "Permission denied" on analytics_clean.duckdb.
#
# Widening the whole tree rather than chowning to the image's UID keeps this
# working whoever the image runs as. It is safe because $TMP itself stays 0700
# root: nothing else on the host can traverse into this directory, and Docker
# resolves the bind mount as root at mount time rather than walking the path as
# the container user.
chmod -R a+rwX "$TMP/data"

if [ ! -f "$TMP/data/export_parquet/_manifest.json" ]; then
  echo "FAIL: archive has no export_parquet/_manifest.json" >&2
  exit 1
fi

PARQUET_COUNT="$(find "$TMP/data/export_parquet" -name '*.parquet' | wc -l | tr -d ' ')"
echo "unpacked: $PARQUET_COUNT parquet files"

# An off-site copy of a SQLite file proves nothing until it opens. A torn copy
# - which is what `cp` of a live database produces - passes every size and
# presence check, and fails only when somebody needs it.
if [ -f "$TMP/data/bot.db" ]; then
  BOT_REPORT="$(python3 - "$TMP/data/bot.db" <<'SQLITE_CHECK'
import sqlite3
import sys

db = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
ok = db.execute("PRAGMA integrity_check").fetchone()[0]
users = db.execute("SELECT COUNT(*) FROM authorized_users").fetchone()[0]
prefs = db.execute("SELECT COUNT(*) FROM user_preferences").fetchone()[0]
print(f"{ok}|{users}|{prefs}")
SQLITE_CHECK
  )" || { echo "FAIL: bot.db in the archive would not open" >&2; exit 1; }
  BOT_OK="${BOT_REPORT%%|*}"
  if [ "$BOT_OK" != "ok" ]; then
    echo "FAIL: bot.db integrity_check says: $BOT_OK" >&2
    exit 1
  fi
  echo "bot.db: integrity ok, $(echo "$BOT_REPORT" | cut -d'|' -f2) authorized users, $(echo "$BOT_REPORT" | cut -d'|' -f3) preference rows"
else
  echo "NOTE: archive carries no bot.db - bot users, milestones and languages would have to be rebuilt by hand" >&2
fi

if [ -f "$TMP/data/env.gpg" ]; then
  echo "archive carries an encrypted .env (not decrypted by this drill)"
else
  echo "NOTE: archive carries no .env — a real restore would need the config rebuilt by hand" >&2
fi

# --- rebuild and validate ---------------------------------------------------
# COMPACT_AUTO_SWAP is deliberately absent: restore_from_export.py never calls
# phase 4, and this makes that true twice.
echo "rebuilding in $IMAGE ..."
# deploy/ is not in the image, and should not be: the image carries what the
# service runs, and a restore drill is operator tooling. Mounting it keeps the
# drill in step with the checkout without an image rebuild, and a real restore
# has the repo anyway — the archive records the deploy SHA precisely so you can
# check out the commit that wrote it.
#
# `-e KS_DUCKDB=on`: under KS_DUCKDB=off in .env the switch would refuse the
# rebuild before it imported a row, and the drill that proves the archive
# rebuilds would stop working for the week. `-e` outranks `--env-file`
# whichever comes first (measured, engine 24.0.7), and the sidecar sees only
# $TMP/data — never the live file, which the week keeps still. Under `on` it
# changes nothing.
docker run --rm \
  --memory=6500m \
  -e DUCKDB_MEMORY_LIMIT=4GB \
  -v "$TMP/data:/app/data" \
  -v "$PWD/deploy/restore_from_export.py:/app/deploy/restore_from_export.py:ro" \
  --env-file .env \
  -e KS_DUCKDB=on \
  "$IMAGE" \
  python /app/deploy/restore_from_export.py

echo
echo "PASS — the off-site archive rebuilt a validated warehouse."
echo "Scratch directory discarded; nothing canonical was touched."
