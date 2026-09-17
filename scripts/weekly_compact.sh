#!/bin/bash
# Automated weekly DuckDB compact.
# Runs via host cron (not in-container) so it can stop/start docker containers.
# Stops web/bot/meili to free RAM, runs compact_duckdb.py in a sidecar, swaps
# the DB file atomically, restarts everything. On failure, source DB stays
# intact and services are restarted on the old DB.
#
# Install: copy to /opt/key-api-bot/scripts/weekly_compact.sh on the host.
# Cron: 0 2 * * 0  /opt/key-api-bot/scripts/weekly_compact.sh
# Logs: /var/log/keycrm-compact.log
set -u

DATA_DIR="/opt/key-api-bot/data"
COMPOSE_DIR="/opt/key-api-bot"
LOG="/var/log/keycrm-compact.log"
HEALTH_URL="https://ksanalytics.duckdns.org/api/health"
TIMEOUT_SEC=1800

log() {
    echo "[$(date -Iseconds)] $*" | tee -a "$LOG"
}

# Step 07 of the alerts rework: the shared notifier — kill switch, instance
# signature, best-effort row in the alert archive. Notably, this script is
# THE argument for the archive living in Postgres: while the compact has web
# and bot stopped, ks-postgres is the one store still standing to record
# what the cron said.
ENV_FILE="$COMPOSE_DIR/.env"
source "$COMPOSE_DIR/deploy/notify.sh"

cleanup_artifacts() {
    rm -f "$DATA_DIR/analytics_clean.duckdb" "$DATA_DIR/analytics_clean.duckdb.wal" 2>/dev/null
    rm -rf "$DATA_DIR/export_parquet" 2>/dev/null
    docker rm -f -v duckdb-compact 2>/dev/null
}

start_services() {
    cd "$COMPOSE_DIR" && docker compose up -d
}

abort() {
    log "ABORT: $*"
    notify "🚨 Compact aborted: $*"
    cleanup_artifacts
    start_services
    exit 1
}

# The sidecar colours its level tags; neither the log file nor Telegram wants
# the escape codes.
strip_ansi() {
    sed $'s/\033\\[[0-9;]*m//g'
}

# The sequence restore and its read-back are the part of a compact whose
# evidence leaves with the sidecar: `docker rm -v` takes its log, and on
# 2026-09-17 nobody could say from this host whether the burn of 13.09 had
# reached disk. Copied on success, on failure and on a timeout alike, before
# anything removes the container, as whole sections, so an image older than
# this wrapper — whose restore lines read differently and which has no
# sequence check at all — still leaves what it printed. Never a reason to
# abort.
#
# Finding no section means one of two things, and the line says which. A
# sidecar that got past the restore ("Flushing WAL" follows it in every image,
# "ALL VALIDATIONS PASSED" follows everything) and printed none is an older
# image. One that did not get that far — a preflight refusal, a failed import,
# an OOM kill in the export, a stall — simply never restored anything, and
# pointing the reader at image drift there sends them after the wrong fault.
copy_sequence_evidence() {
    local logs lines
    logs=$(docker logs duckdb-compact 2>&1 | strip_ansi || true)
    lines=$(printf '%s\n' "$logs" \
        | sed -n -e '/Restoring sequences:/,/Flushing WAL/p' \
                 -e '/Sequence check/,/Derived table placeholders/p' || true)
    if [ -n "$lines" ]; then
        log "Sequences, as the sidecar printed them:"
        printf '%s\n' "$lines" >> "$LOG"
    elif printf '%s\n' "$logs" | grep -qE 'Flushing WAL|ALL VALIDATIONS PASSED'; then
        log "Sequences: the sidecar printed no restore section (an image older than this wrapper?)"
    else
        log "Sequences: none restored — the sidecar did not get as far as the restore"
    fi
}

# The one line the Telegram alert quotes: the sidecar's last ERROR or FAIL
# line, which from this version on is phase 3's summary of every failure, with
# its timestamp and level tag taken off. An ERROR tag with nothing after it is
# skipped: the script logs "\nDO NOT SWAP" that way, and quoting the empty tag
# line is how an older image's alert would otherwise say nothing. Bounded and
# flattened to printable ASCII, so a byte cut cannot split a character into
# invalid UTF-8 and cost the alert. Credentials in a URL and anything shaped
# like a bot token are masked — the sidecar runs with the whole .env and a
# driver error can quote a DSN. `$` and backticks go too: notify.sh archives
# the text inside a $$-quoted SQL literal. Empty when nothing matches, and the
# alert then says what it said before.
failure_reason() {
    docker logs duckdb-compact 2>&1 | strip_ansi \
        | grep -E '^\[[0-9:]+\] +ERROR +[^ ]|FAIL|Error|Exception' | tail -n 1 \
        | LC_ALL=C tr -cd '[:print:]' | LC_ALL=C tr -d '$`' \
        | sed -E 's#^\[[0-9:]+\] +[A-Z]+ +##; s#://[^/@[:space:]]+@#://***@#g; s#[0-9]{6,}:[A-Za-z0-9_-]{30,}#***#g' \
        | cut -c1-300 || true
}

log "=== WEEKLY COMPACT START ==="
SIZE_BEFORE=$(du -h "$DATA_DIR/analytics.duckdb" 2>/dev/null | cut -f1)
DISK_BEFORE=$(df -h / | awk 'NR==2 {print $5}')
log "DB size: $SIZE_BEFORE | disk: $DISK_BEFORE used"

# Remove the backup from the previous successful compact so we don't accumulate
# .old files. Compact validates internally before swap, so rollback is rare.
rm -f "$DATA_DIR/analytics.duckdb.old"

cleanup_artifacts

log "Stopping services..."
cd "$COMPOSE_DIR"
if ! docker compose stop --timeout 30 web bot meilisearch; then
    abort "failed to stop services"
fi
sleep 3

log "Running compact in sidecar (auto-swap enabled)..."
# COMPACT_AUTO_SWAP=1: sidecar performs the atomic file swap inside the
# script after Phase 3 validation passes. Removes the May 2026 failure
# mode where the operator's SSH died between validation and the manual
# `mv` commands, leaving services pointed at the old DB for 12h.
if ! docker run -d --name duckdb-compact \
    --memory=6500m \
    -e DUCKDB_MEMORY_LIMIT=5GB \
    -e COMPACT_AUTO_SWAP=1 \
    -v "$DATA_DIR:/app/data" \
    --env-file "$COMPOSE_DIR/.env" \
    halloweex/keycrm-web:latest \
    python /app/scripts/compact_duckdb.py > /dev/null; then
    abort "failed to launch compact container"
fi

# Wait for compact to finish (max TIMEOUT_SEC)
ELAPSED=0
while docker ps --filter name=duckdb-compact --format '{{.Status}}' | grep -q '^Up'; do
    sleep 30
    ELAPSED=$((ELAPSED + 30))
    if [ "$ELAPSED" -ge "$TIMEOUT_SEC" ]; then
        # The container is removed next and its log with it, so what it printed
        # is copied first — a sidecar that stalls in the final CHECKPOINT or the
        # swap has already printed its restore and its read-back.
        copy_sequence_evidence
        log "Compact still running after ${TIMEOUT_SEC}s. Last log:"
        docker logs duckdb-compact --tail 30 2>&1 | strip_ansi | tee -a "$LOG"
        REASON=$(failure_reason)
        docker rm -f -v duckdb-compact 2>/dev/null
        abort "compact timeout after ${TIMEOUT_SEC}s${REASON:+: $REASON}"
    fi
done

EXIT_CODE=$(docker inspect duckdb-compact --format '{{.State.ExitCode}}' 2>/dev/null || echo "?")
copy_sequence_evidence
if [ "$EXIT_CODE" != "0" ]; then
    log "Compact exited $EXIT_CODE. Last log:"
    docker logs duckdb-compact --tail 30 2>&1 | strip_ansi | tee -a "$LOG"
    REASON=$(failure_reason)
    abort "compact failed (exit $EXIT_CODE)${REASON:+: $REASON}"
fi

if ! docker logs duckdb-compact 2>&1 | grep -q "ALL VALIDATIONS PASSED"; then
    log "Validation didn't pass. Last log:"
    docker logs duckdb-compact --tail 30 2>&1 | strip_ansi | tee -a "$LOG"
    REASON=$(failure_reason)
    abort "compact validation did not pass${REASON:+: $REASON}"
fi

# Phase 4 (atomic swap) ran inside the sidecar. Verify the post-condition:
# canonical analytics.duckdb is the freshly-compacted file, .old is the backup.
if [ -f "$DATA_DIR/analytics_clean.duckdb" ]; then
    abort "analytics_clean.duckdb still present after auto-swap — sidecar swap failed"
fi
if [ ! -f "$DATA_DIR/analytics.duckdb" ]; then
    abort "analytics.duckdb missing after swap"
fi

NEW_SIZE=$(du -h "$DATA_DIR/analytics.duckdb" | cut -f1)
log "Auto-swap OK. New canonical DB: $NEW_SIZE"

log "Starting services..."
start_services
sleep 30

# Health check (best-effort — Silver/Gold rebuild takes ~3 min on first refresh)
if curl -fsS --max-time 5 "$HEALTH_URL" > /dev/null 2>&1; then
    log "Health check OK"
else
    log "Health check pending (Silver/Gold warming up — normal)"
fi

docker rm -v duckdb-compact 2>/dev/null || true

# Ship the Parquet export off the box while it is fresh.
#
# Here, and not in an independent cron, because this is the moment the export
# exists and has just been proven: phase 3 validated it row-for-row and phase 4
# rebuilt the live database out of it. It runs after the health check so a slow
# upload cannot extend the downtime.
#
# Best-effort on purpose. This script has no `set -e`, and a failed upload must
# never turn a successful compact into an aborted one — abort() would delete the
# export. The push does its own alerting; the status only rides along in the
# weekly summary so the message says whether a copy actually left the machine.
OFFSITE_STATUS="skipped"
if [ -x "$COMPOSE_DIR/deploy/offsite_parquet.sh" ]; then
    log "Pushing Parquet export off-site..."
    if "$COMPOSE_DIR/deploy/offsite_parquet.sh" >> "$LOG" 2>&1; then
        OFFSITE_STATUS="✅ off-site"
    else
        RC=$?
        # 78 = EX_UNCONFIGURED: the export is fine, there is nowhere to send it.
        # A different sentence from "the push failed", and worth keeping apart.
        if [ "$RC" -eq 78 ]; then
            OFFSITE_STATUS="⚠️ off-site not configured"
        else
            OFFSITE_STATUS="❌ off-site FAILED (rc=$RC)"
        fi
    fi
    log "Off-site: $OFFSITE_STATUS"
fi

SIZE_AFTER=$(du -h "$DATA_DIR/analytics.duckdb" | cut -f1)
DISK_AFTER=$(df -h / | awk 'NR==2 {print $5}')
log "Done: $SIZE_BEFORE → $SIZE_AFTER | disk: $DISK_BEFORE → $DISK_AFTER used"

# The floor this compact just left is the cheapest regression signal available:
# a post-compact size is the database with every dead byte removed, so nothing
# periodic remains to cancel. If it rises, something is being retained that was
# not retained before — a different statement from "the database grew".
#
# Runs after the Done: line, because that line is what it reads. Best-effort:
# this script has no `set -e`, and a judgement about last week must never turn
# a successful compact into a failed one.
FLOOR_STATUS=""
if [ -x "$COMPOSE_DIR/deploy/compact_floor_check.py" ]; then
    FLOOR_OUT=$(python3 "$COMPOSE_DIR/deploy/compact_floor_check.py" "$LOG" 2>&1)
    FLOOR_RC=$?
    log "Floor check: $FLOOR_OUT"
    [ "$FLOOR_RC" -ne 0 ] && FLOOR_STATUS="
⚠️ $FLOOR_OUT"
fi
notify "✅ Weekly compact: ${SIZE_BEFORE} → ${SIZE_AFTER}, disk ${DISK_BEFORE} → ${DISK_AFTER}
${OFFSITE_STATUS}${FLOOR_STATUS}"

log "=== WEEKLY COMPACT END ==="
