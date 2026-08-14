#!/usr/bin/env bash
# Daily: are the instruments alive, and has a copy left this machine?
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
# The in-app disk watchdog touches this on every 6-hourly sample. Two periods
# plus slack: later than this and it has stopped, whatever it last logged.
WATCHDOG_MARKER="${WATCHDOG_MARKER:-data/health/watchdog_last_sample}"
WATCHDOG_MAX_AGE_HOURS="${WATCHDOG_MAX_AGE_HOURS:-14}"
# deploy/daily_offsite.sh ships nightly, so this is one missed run plus slack.
# It was 216h when the push rode the weekly compact; a threshold left at a
# cadence the system no longer has is a check that has stopped checking.
MAX_AGE_HOURS="${BACKUP_MAX_ARCHIVE_AGE_HOURS:-36}"

# `|| true` is load-bearing: grep exits 2 when .env is absent and 1 when the key
# is, pipefail propagates that, and under `set -e` the caller dies with grep's
# status instead of reaching its own `exit 1`. A missing token must degrade to
# "cannot alert", not to a different exit code than the one the check meant.
_env_value() { grep -m1 "^$1=" .env 2>/dev/null | cut -d= -f2- | tr -d '\r' || true; }

# Technical alerts go to every admin, not the first one. The app-side path
# (bot/main.py:88, core/telegram_alerts.py:122) has always broadcast to the
# whole list; these host-cron scripts took `cut -d, -f1`, so one admin saw
# "the off-site copy did not leave" and "the watchdog stopped" while both saw
# the data-quality digest. Nobody decided that — the two paths were simply
# never compared.
alert_chat_ids() {
    if [ -n "${BACKUP_ALERT_CHAT_ID:-}" ]; then
        printf '%s' "$BACKUP_ALERT_CHAT_ID" | tr ',' '\n'
        return 0
    fi
    _env_value ADMIN_USER_IDS | tr ',' '\n' | tr -d '[:space:]' || true
}

notify() {
    local text="$1" token chat sent=0
    token="$(_env_value BOT_TOKEN)"
    if [ -z "$token" ]; then
        echo "cannot alert: BOT_TOKEN missing from .env" >&2
        return 0
    fi
    while read -r chat; do
        [ -n "$chat" ] || continue
        sent=1
        # Never let a failed notification change the script's own outcome, and
        # never let one unreachable admin stop the others being told.
        curl -sS -m 15 -o /dev/null \
            --data-urlencode "chat_id=$chat" \
            --data-urlencode "text=$text" \
            "https://api.telegram.org/bot${token}/sendMessage" || \
            echo "alert delivery failed for one recipient" >&2
    done <<< "$(alert_chat_ids)"
    [ "$sent" -eq 1 ] || echo "cannot alert: ADMIN_USER_IDS is empty" >&2
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

# --- is the in-app watchdog still sampling? -----------------------------------
# This is the check that did not exist. Its subject is not a threshold being
# crossed but a job having quietly stopped, and it cannot live in the process
# it is judging.
if [ -f "$WATCHDOG_MARKER" ]; then
    WD_AGE=$(( ( $(date +%s) - $(stat -c %Y "$WATCHDOG_MARKER") ) / 3600 ))
    WD_LINE="${WD_AGE}h ago"
else
    # No marker is not the same fact as a stopped watchdog, and conflating them
    # would page after every deploy — several times a day — which is the crying
    # wolf this check was added to replace, not to reproduce.
    #
    # The sampler fires on a cron inside the app, so a container younger than
    # one period has legitimately not reached its first run yet. Judge the
    # absence against uptime, and only call it stopped once it has had time.
    UP_H=$(( ( $(date +%s) - $(date -d "$(docker inspect -f '{{.State.StartedAt}}' "${WATCHDOG_CONTAINER:-keycrm-web}" 2>/dev/null)" +%s 2>/dev/null || echo 0) ) / 3600 ))
    if [ "$UP_H" -lt "$WATCHDOG_MAX_AGE_HOURS" ] 2>/dev/null; then
        WD_AGE=0
        WD_LINE="pending first sample (container up ${UP_H}h)"
    else
        WD_AGE=9999
        WD_LINE="never, and the container has been up ${UP_H}h"
    fi
fi

if [ "$WD_AGE" -gt "$WATCHDOG_MAX_AGE_HOURS" ]; then
    echo "CRITICAL: disk watchdog last sampled $WD_LINE (max ${WATCHDOG_MAX_AGE_HOURS}h)"
    notify "$(printf '%s\n\n%s\n\n%s\n\n%s' \
        "🚨 The disk watchdog has stopped sampling" \
        "Last sample: $WD_LINE. It runs every 6h, so this is not a late run." \
        "It has done this before and went unnoticed for eleven weeks, because a monitor that stops and a monitor with nothing to report look identical. That is what this line exists to tell apart." \
        "$(_where)")"
    exit 1
fi

# Speak on a good day too. A check that only ever appears when something is
# wrong teaches nobody what its silence means, and its own absence becomes
# invisible — which is the failure it is here to prevent, applied to itself.
echo "instruments ok: off-site ${AGE_HOURS}h, watchdog ${WD_LINE}"
notify "$(printf '%s\n%s\n%s' \
    "🫀 Приборы в порядке" \
    "внешняя копия: ${AGE_HOURS}ч назад (порог ${MAX_AGE_HOURS}ч)" \
    "сторож диска: ${WD_LINE} (порог ${WATCHDOG_MAX_AGE_HOURS}ч)")"
