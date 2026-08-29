#!/usr/bin/env bash
# The one Telegram notifier for host-cron shell — transport T6 of the alerts
# inventory, brought under the same rules as the Python transports on step 07:
#
#   * the kill switch: KS_ALERTS_DISABLED in .env suppresses, loudly;
#   * the signature: every message ends with "· $KS_INSTANCE" (hostname if
#     unset), so a phantom from a restored backup names its machine;
#   * the archive: a best-effort event row via the ks-postgres container —
#     which is the one store still alive while weekly_compact has web and bot
#     stopped, the argument that put the ledger in Postgres in the first
#     place. Failure to archive never fails the notification, and failure to
#     notify never fails the calling script.
#
# Usage:  source this file after ENV_FILE (or COMPOSE_DIR) is known, then
#         notify "text" [condition_key]
# Recipients: BACKUP_ALERT_CHAT_ID from the caller's environment overrides
# ADMIN_USER_IDS from .env — offsite_check's existing contract, kept.

_ks_env_file() {
    if [ -n "${ENV_FILE:-}" ] && [ -f "${ENV_FILE}" ]; then
        printf '%s' "$ENV_FILE"
    elif [ -n "${COMPOSE_DIR:-}" ] && [ -f "${COMPOSE_DIR}/.env" ]; then
        printf '%s' "${COMPOSE_DIR}/.env"
    elif [ -f .env ]; then
        printf '%s' ".env"
    fi
}

# `|| true` is load-bearing: grep exits 2 when the file is absent and 1 when
# the key is — neither may kill a `set -e` caller.
_ks_env_value() {
    local file
    file="$(_ks_env_file)"
    [ -n "$file" ] || return 0
    grep -m1 "^$1=" "$file" 2>/dev/null | cut -d= -f2- | tr -d '"\r' || true
}

_ks_instance() {
    local name
    name="$(_ks_env_value KS_INSTANCE)"
    printf '%s' "${name:-$(hostname)}"
}

_ks_alerts_disabled() {
    case "$(_ks_env_value KS_ALERTS_DISABLED | tr '[:upper:]' '[:lower:]')" in
        1|true|yes) return 0 ;;
        *) return 1 ;;
    esac
}

_ks_chat_ids() {
    if [ -n "${BACKUP_ALERT_CHAT_ID:-}" ]; then
        printf '%s' "$BACKUP_ALERT_CHAT_ID" | tr ',' '\n'
        return 0
    fi
    # NOT `tr -d '[:space:]'`: that deletes the newlines the first tr just
    # made, welding every admin id into one invalid chat_id. Inherited from
    # offsite_check.sh, where BACKUP_ALERT_CHAT_ID happened to mask it;
    # found the day this file was live-tested without the override.
    _ks_env_value ADMIN_USER_IDS | tr ',' '\n' | tr -d ' \t\r' || true
}

_ks_archive_event() {
    # Best-effort, and only when docker + the container are around: cron on a
    # host without the stack (or during a Postgres restart) skips silently.
    local key="$1" delivered="$2" text="$3"
    command -v docker >/dev/null 2>&1 || return 0
    docker exec -i ks-postgres psql -U ks_app -d ks -q \
        -c "INSERT INTO app.alert_events (condition_key, event_type, instance, delivered_to, message) VALUES (\$\$${key}\$\$, 'fired', \$\$$(_ks_instance)\$\$, ${delivered}, left(\$\$${text}\$\$, 4000))" \
        >/dev/null 2>&1 || true
}

notify() {
    local text="$1" key="${2:-hostcron}" token chat delivered=0 attempted=0
    if _ks_alerts_disabled; then
        echo "notify suppressed (KS_ALERTS_DISABLED): ${text:0:80}" >&2
        return 0
    fi
    text="${text}"$'\n\n'"· $(_ks_instance)"
    token="$(_ks_env_value BOT_TOKEN)"
    if [ -z "$token" ]; then
        echo "cannot alert: BOT_TOKEN missing from .env" >&2
        return 0
    fi
    while read -r chat; do
        [ -n "$chat" ] || continue
        attempted=1
        # Never let a failed notification change the script's own outcome,
        # and never let one unreachable admin stop the others being told.
        # -f is load-bearing: without it an HTTP 400 from Telegram counts
        # as a delivery, and the archive would swear somebody heard us.
        if curl -sSf -m 15 -o /dev/null \
            --data-urlencode "chat_id=$chat" \
            --data-urlencode "text=$text" \
            "https://api.telegram.org/bot${token}/sendMessage"; then
            delivered=$((delivered + 1))
        else
            echo "alert delivery failed for one recipient" >&2
        fi
    done <<< "$(_ks_chat_ids)"
    [ "$attempted" -eq 1 ] || echo "cannot alert: ADMIN_USER_IDS is empty" >&2
    _ks_archive_event "$key" "$delivered" "$text"
    return 0
}
