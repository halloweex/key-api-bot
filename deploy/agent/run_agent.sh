#!/usr/bin/env bash
# Track Б, step 08: the host-side agent runner.
#
# Triggered by ks-alert-agent.path whenever the spool has tasks. Runs OUTSIDE
# the app containers — the host is the one place that has docker logs for
# both containers, the Anthropic key outside any image, and a life while the
# weekly compact has web and bot stopped.
#
# The agent's power is its tool allowlist, not its prompt: read-only
# commands only, Postgres through the ks_readonly role (SELECT-only by
# grants), five-minute hard timeout, ten diagnoses a day. Its report goes
# out through deploy/notify.sh — signature, kill switch, archive — as a
# second message after the alert it explains.
set -u

COMPOSE_DIR="/opt/key-api-bot"
SPOOL="$COMPOSE_DIR/data/alert-tasks"
ENV_FILE="$COMPOSE_DIR/.env"
LOG="$SPOOL/agent.log"
DAILY_BUDGET=10
export PATH="$HOME/.local/bin:/usr/local/bin:$PATH"
export HOME="${HOME:-/root}"

mkdir -p "$SPOOL/pending" "$SPOOL/processing" "$SPOOL/done"
source "$COMPOSE_DIR/deploy/notify.sh"

log() { printf '%s %s\n' "$(date -Is)" "$*" >> "$LOG"; }

api_key() { grep -m1 '^ANTHROPIC_API_KEY=' "$ENV_FILE" | cut -d= -f2- | tr -d '"\r'; }

family_for() {
    case "$1" in
        warehouse:*)                          echo warehouse ;;
        mirror_*|dq_*|ch_*|order_versions_*|freshness_*) echo mirror ;;
        disk:*)                               echo disk ;;
        health_*|cert_*|alerting_*)           echo health ;;
        *)                                    echo default ;;
    esac
}

process_one() {
    local task="$1" base key bucket conditions report family rc
    base="$(basename "$task")"
    mv "$task" "$SPOOL/processing/$base" 2>/dev/null || return 0
    task="$SPOOL/processing/$base"

    # Daily budget: at the measured alert rate (~7 CRITICAL days a month)
    # ten runs a day is headroom; hitting it means an alert storm, and an
    # agent burning API money on a storm diagnoses nothing new.
    local today runs
    today="$(date +%F)"
    runs="$(ls -1 "$SPOOL/done/${today}"*.report.md 2>/dev/null | wc -l)"
    if [ "$runs" -ge "$DAILY_BUDGET" ]; then
        log "budget exhausted ($runs/$DAILY_BUDGET), task $base parked as skipped"
        mv "$task" "$SPOOL/done/$base.skipped"
        return 0
    fi

    bucket="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['bucket'])" "$task" 2>/dev/null || echo unknown)"
    conditions="$(python3 -c "import json,sys; print(' '.join(json.load(open(sys.argv[1]))['conditions']))" "$task" 2>/dev/null || echo "")"
    family="$(family_for "${conditions%% *}")"
    log "task $base: bucket=$bucket family=$family"

    local prompt
    prompt="$(cat "$COMPOSE_DIR/deploy/agent/runbooks/_common.md" \
                  "$COMPOSE_DIR/deploy/agent/runbooks/${family}.md")
=== ЗАДАЧА (JSON) ===
$(cat "$task")"

    # The allowlist IS the safety model: prompt injection from a log line can
    # at worst waste a diagnosis, never touch anything. ks_readonly cannot
    # write even if asked nicely.
    report="$(ANTHROPIC_API_KEY="$(api_key)" timeout 300 claude -p "$prompt" \
        --model sonnet \
        --max-turns 25 \
        --allowedTools \
            "Bash(docker logs:*)" \
            "Bash(docker ps:*)" \
            "Bash(docker exec -i ks-postgres psql -U ks_readonly:*)" \
            "Bash(docker exec ks-postgres psql -U ks_readonly:*)" \
            "Bash(curl -s:*)" \
            "Bash(df:*)" \
            "Bash(du:*)" \
            "Bash(echo:*)" \
            "Bash(openssl:*)" \
        2>>"$LOG")"
    rc=$?

    if [ $rc -ne 0 ] || [ -z "$report" ]; then
        log "task $base: agent failed rc=$rc"
        notify "🔎 Агент-диагност не справился с $bucket (rc=$rc) — смотри $SPOOL/agent.log" "agent:failed"
        mv "$task" "$SPOOL/done/$base.failed"
        return 0
    fi

    printf '%s\n' "$report" > "$SPOOL/done/${base%.json}.report.md"
    mv "$task" "$SPOOL/done/$base"

    # Telegram gets a clamped copy; the archive keeps it whole under its own
    # event type, next to the fired event it explains.
    local clamped
    clamped="$(printf '%s' "$report" | head -c 3400)"
    notify "🔎 Диагноз ($bucket):
$clamped" "agent:diagnosis"
    docker exec -i ks-postgres psql -U ks_app -d ks -q \
        -c "INSERT INTO app.alert_events (condition_key, event_type, instance, message) VALUES (\$\$${bucket}\$\$, 'diagnosed', \$\$$(_ks_instance)\$\$, left(\$\$${report}\$\$, 4000))" \
        >/dev/null 2>&1 || log "task $base: archive write failed (report kept on disk)"
    log "task $base: diagnosed, $(printf '%s' "$report" | wc -c) bytes"
}

# The path-unit fires on "directory not empty" edges; draining the whole
# queue here means a burst of tasks cannot outrun the trigger.
while true; do
    next="$(ls -1 "$SPOOL"/pending/*.json 2>/dev/null | head -1)" || true
    [ -n "${next:-}" ] || break
    process_one "$next"
done
exit 0
