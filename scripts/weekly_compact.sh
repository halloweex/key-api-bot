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

# Telegram alert via the bot's credentials in .env (best-effort, never fails the script)
notify() {
    local msg="$1"
    [ -f "$COMPOSE_DIR/.env" ] || return 0
    local token id
    token=$(grep -E '^BOT_TOKEN=' "$COMPOSE_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"')
    id=$(grep -E '^ADMIN_USER_IDS=' "$COMPOSE_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"' | cut -d, -f1)
    if [ -n "$token" ] && [ -n "$id" ]; then
        curl -fsS --max-time 10 \
            "https://api.telegram.org/bot${token}/sendMessage" \
            -d "chat_id=${id}" \
            -d "text=${msg}" > /dev/null 2>&1 || true
    fi
}

cleanup_artifacts() {
    rm -f "$DATA_DIR/analytics_clean.duckdb" "$DATA_DIR/analytics_clean.duckdb.wal" 2>/dev/null
    rm -rf "$DATA_DIR/export_parquet" 2>/dev/null
    docker rm -f duckdb-compact 2>/dev/null
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

log "Running compact in sidecar..."
if ! docker run -d --name duckdb-compact \
    --memory=6500m \
    -e DUCKDB_MEMORY_LIMIT=3GB \
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
        docker rm -f duckdb-compact 2>/dev/null
        abort "compact timeout after ${TIMEOUT_SEC}s"
    fi
done

EXIT_CODE=$(docker inspect duckdb-compact --format '{{.State.ExitCode}}' 2>/dev/null || echo "?")
if [ "$EXIT_CODE" != "0" ]; then
    log "Compact exited $EXIT_CODE. Last log:"
    docker logs duckdb-compact --tail 30 2>&1 | tee -a "$LOG"
    abort "compact failed (exit $EXIT_CODE)"
fi

if ! docker logs duckdb-compact 2>&1 | grep -q "ALL VALIDATIONS PASSED"; then
    log "Validation didn't pass. Last log:"
    docker logs duckdb-compact --tail 30 2>&1 | tee -a "$LOG"
    abort "compact validation did not pass"
fi

if [ ! -f "$DATA_DIR/analytics_clean.duckdb" ]; then
    abort "analytics_clean.duckdb missing after compact"
fi

CLEAN_SIZE=$(du -h "$DATA_DIR/analytics_clean.duckdb" | cut -f1)
log "Compact OK. Clean DB: $CLEAN_SIZE"

log "Atomic swap..."
mv "$DATA_DIR/analytics.duckdb" "$DATA_DIR/analytics.duckdb.old"
rm -f "$DATA_DIR/analytics.duckdb.wal"
mv "$DATA_DIR/analytics_clean.duckdb" "$DATA_DIR/analytics.duckdb"

log "Starting services..."
start_services
sleep 30

# Health check (best-effort — Silver/Gold rebuild takes ~3 min on first refresh)
if curl -fsS --max-time 5 "$HEALTH_URL" > /dev/null 2>&1; then
    log "Health check OK"
else
    log "Health check pending (Silver/Gold warming up — normal)"
fi

docker rm duckdb-compact 2>/dev/null || true

SIZE_AFTER=$(du -h "$DATA_DIR/analytics.duckdb" | cut -f1)
DISK_AFTER=$(df -h / | awk 'NR==2 {print $5}')
log "Done: $SIZE_BEFORE → $SIZE_AFTER | disk: $DISK_BEFORE → $DISK_AFTER used"
notify "✅ Weekly compact: ${SIZE_BEFORE} → ${SIZE_AFTER}, disk ${DISK_BEFORE} → ${DISK_AFTER}"

log "=== WEEKLY COMPACT END ==="
