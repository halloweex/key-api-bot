#!/usr/bin/env bash
# Daily: has an off-site copy left this machine recently enough?
#
# This runs from the HOST crontab, not the application scheduler, and that is
# the whole design. An in-process check cannot attest to its own liveness: a
# monitor that has stopped running and one with nothing to report emit exactly
# the same silence, and this codebase has already been on the wrong side of
# that. Host cron is a separate failure domain with the better record here. The
# check that asks "is the backup still happening" must not depend on the
# process it is checking.
#
# It also speaks on the day it is installed, before anything has been shipped,
# and the answer will be red. That is correct and deliberate: an alert whose
# first reading is green teaches nobody what it looks like when it fires, and a
# requirement nothing ever states out loud is one that quietly stops being met.
#
# Install:
#   0 9 * * *  /opt/key-api-bot/deploy/offsite_check.sh
set -Eeuo pipefail

cd "$(dirname "$0")/.."

CONFIG="deploy/backup.env"
# shellcheck source=/dev/null
[ -f "$CONFIG" ] && . "$CONFIG"

MARKER="${BACKUP_MARKER:-data/.offsite_last_ok}"
# deploy/daily_offsite.sh ships nightly, so this is one missed run plus slack.
# It was 216h when the push rode the weekly compact; a threshold left at a
# cadence the system no longer has is a check that has stopped checking.
MAX_AGE_HOURS="${BACKUP_MAX_ARCHIVE_AGE_HOURS:-36}"

# `|| true` is load-bearing: grep exits 2 when .env is absent and 1 when the key
# is, pipefail propagates that, and under `set -e` the caller dies with grep's
# status instead of reaching its own `exit 1`. A missing token must degrade to
# "cannot alert", not to a different exit code than the one the check meant.
_env_value() { grep -m1 "^$1=" .env 2>/dev/null | cut -d= -f2- | tr -d '\r' || true; }

alert_chat_id() {
    if [ -n "${BACKUP_ALERT_CHAT_ID:-}" ]; then
        printf '%s' "$BACKUP_ALERT_CHAT_ID"
        return 0
    fi
    _env_value ADMIN_USER_IDS | cut -d, -f1 | tr -d '[:space:]' || true
}

notify() {
    local text="$1" token chat
    token="$(_env_value BOT_TOKEN)"
    chat="$(alert_chat_id)"
    if [ -z "$token" ] || [ -z "$chat" ]; then
        echo "cannot alert: BOT_TOKEN or ADMIN_USER_IDS missing from .env" >&2
        return 0
    fi
    curl -sS -m 15 -o /dev/null \
        --data-urlencode "chat_id=$chat" \
        --data-urlencode "text=$text" \
        "https://api.telegram.org/bot${token}/sendMessage" || \
        echo "alert delivery failed" >&2
}

_where() { printf '%s · %s' "$(hostname)" "$(date '+%F %H:%M')"; }

if [ ! -f "$MARKER" ]; then
    echo "CRITICAL: no off-site copy has ever succeeded (no $MARKER)"
    notify "$(printf '%s\n\n%s\n\n%s\n\n%s' \
        "🚨 Warehouse: off-site copy age = never" \
        "The database, its local backups and the export all share one volume, so any failure that takes the volume takes every copy with it." \
        "Set it up: docs/offsite_backup_setup.md — then run deploy/offsite_parquet.sh and deploy/restore-test.sh" \
        "$(_where)")"
    exit 1
fi

AGE_HOURS=$(( ( $(date +%s) - $(stat -c %Y "$MARKER") ) / 3600 ))

if [ "$AGE_HOURS" -gt "$MAX_AGE_HOURS" ]; then
    echo "CRITICAL: newest off-site copy is ${AGE_HOURS}h old (max ${MAX_AGE_HOURS}h)"
    notify "$(printf '%s\n\n%s\n%s\n\n%s\n\n%s' \
        "🚨 Warehouse: off-site copy is stale" \
        "Newest copy: ${AGE_HOURS}h old" \
        "Allowed: ${MAX_AGE_HOURS}h" \
        "The weekly compact ships it — check /var/log/keycrm-compact.log and run deploy/offsite_parquet.sh by hand." \
        "$(_where)")"
    exit 1
fi

echo "off-site copy age: ${AGE_HOURS}h (ok, max ${MAX_AGE_HOURS}h)"
