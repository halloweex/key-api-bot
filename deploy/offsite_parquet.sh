#!/usr/bin/env bash
# Weekly off-site copy of the warehouse, shipped as Parquet.
#
# What leaves the machine is not the database file. compact_duckdb.py's
# phase1_export already writes data/export_parquet/ on every compact — each
# non-derived table as ZSTD Parquet, plus a _manifest.json carrying row counts,
# sequence values and business checksums — and phase2_import + phase3_validate
# rebuild a live database from exactly that artifact, validating every row
# count, the orders date range and total revenue before anything is swapped in.
# That path runs every week and rebuilds the canonical database from this
# artifact. Neither the artifact nor the restore is new here; only the transport.
#
# The size is the whole argument. The export is a small fraction of the .duckdb
# it came from, because the overwhelming majority of that file is derived tables
# the app rebuilds from bronze in seconds. That ratio is what makes an off-site
# copy affordable to keep for a year, and — the part that matters — cheap enough
# that deploy/restore-test.sh can pull the whole thing back and prove it
# restores, quickly, as often as you like. An untested backup is a guess.
#
# Called from scripts/weekly_compact.sh after the health check, so a slow
# upload cannot extend the compact's downtime. Safe to run by hand: it only
# reads, and it writes nothing outside a temp dir and one marker file.
#
# Config: deploy/backup.env (git-ignored, see backup.env.example). Without it
# this exits EX_UNCONFIGURED and says which of the two things went wrong —
# "the export is fine, shipping is not set up" is a different sentence from
# "the backup failed", and conflating them teaches the reader to ignore both.
#
# -E matters: without errtrace the ERR trap is not inherited by functions, so a
# command failing inside run_push would abort the shell without ever alerting.
set -Eeuo pipefail

cd "$(dirname "$0")/.."   # repo root == /opt/key-api-bot on the server

CONFIG="deploy/backup.env"
# shellcheck source=/dev/null
[ -f "$CONFIG" ] && . "$CONFIG"

EXPORT_DIR="${BACKUP_EXPORT_DIR:-data/export_parquet}"
MARKER="${BACKUP_MARKER:-data/.offsite_last_ok}"
RETAIN="${BACKUP_RETAIN:-8}"                # weekly cadence → ~2 months
SSH_PORT="${BACKUP_SSH_PORT:-23}"           # Hetzner Storage Box speaks SSH on 23
SSH_KEY="${BACKUP_SSH_KEY:-$HOME/.ssh/storagebox_ed25519}"
REMOTE="${BACKUP_REMOTE:-}"                 # u123456-sub1@u123456.your-storagebox.de
REMOTE_DIR="${BACKUP_REMOTE_DIR:-key-api-bot}"
ENV_PASSFILE="${BACKUP_ENV_PASSFILE:-}"     # unset → ship data without secrets

# The export is only as fresh as the last compact, which runs weekly. Anything
# older than this means the compact stopped and we would be shipping a stale
# artifact under a today's-date filename — the exact shape of a backup that
# looks fine until you need it.
MAX_EXPORT_AGE_DAYS="${BACKUP_MAX_EXPORT_AGE_DAYS:-8}"

SSH_OPTS=(-p "$SSH_PORT" -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)

# Off-site not configured: a real problem, but a different one from "the push
# failed", and the alert wording depends on telling them apart.
EX_UNCONFIGURED=78

LOG="$(mktemp)"
STEP_FILE="$(mktemp)"
STAGE="$(mktemp -d)"
ARCHIVE_DIR="$(mktemp -d)"
# Everything this script builds is temporary and cleaned up here. The point of
# the exercise is to stop keeping copies on this disk, so it must not leave any.
trap 'rm -rf "$LOG" "$STEP_FILE" "$STAGE" "$ARCHIVE_DIR"' EXIT

# run_push's output is captured into $LOG, and the ERR trap fires while that
# redirection is still in effect — so the handler must write to the real stdout
# and stderr, kept here as fds 3 and 4, or its output disappears into the log it
# is trying to report.
exec 3>&1 4>&2

step() { printf '%s' "$1" >"$STEP_FILE"; }

# --- alerting ---------------------------------------------------------------
# Credentials come from .env so they are not duplicated here, and are never
# echoed into the log.

# `|| true` is load-bearing: grep exits 2 when .env is absent and 1 when the key
# is, pipefail propagates that, and under `set -e` the caller dies with grep's
# status. Here that would corrupt the exit code the wrapper reads to tell
# EX_UNCONFIGURED apart from a real failure.
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
    # Never let a failed notification change the script's own outcome.
    curl -sS -m 15 -o /dev/null \
        --data-urlencode "chat_id=$chat" \
        --data-urlencode "text=$text" \
        "https://api.telegram.org/bot${token}/sendMessage" || \
        echo "alert delivery failed" >&2
}

_where() { printf '%s · %s' "$(hostname)" "$(date '+%F %H:%M')"; }

_error_lines() {
    grep -vE '(^staging|^shipping|^pruned)' "$LOG" 2>/dev/null \
        | grep -v '^[[:space:]]*$' | tail -n 4
}

notify_offsite_unconfigured() {
    notify "$(printf '%s\n\n%s\n%s\n\n%s\n\n%s\n%s\n\n%s' \
        "⚠️ Warehouse: off-site copy is not set up" \
        "The Parquet export exists and is current — on this server only:" \
        "  $EXPORT_DIR" \
        "The daily backups and this export share a volume with the database they copy, so any failure that takes the volume takes all of them together." \
        "To fix, on the server:" \
        "  cp deploy/backup.env.example deploy/backup.env, fill in BACKUP_REMOTE and BACKUP_SSH_KEY, then run deploy/offsite_parquet.sh" \
        "$(_where)")"
}

notify_failure() {
    local step errors
    step="$(cat "$STEP_FILE" 2>/dev/null || echo '?')"
    errors="$(_error_lines)"
    notify "$(printf '%s\n\n%s\n%s\n\n%s\n\n%s' \
        "❌ Warehouse off-site push FAILED" \
        "Failed at: $step" \
        "${errors:-(no error output captured)}" \
        "No copy left the server this week. Check: /var/log/keycrm-compact.log" \
        "$(_where)")"
}

# --- the actual work --------------------------------------------------------

run_push() {
    step "check export freshness"
    local manifest="$EXPORT_DIR/_manifest.json"
    if [ ! -f "$manifest" ]; then
        echo "no export found at $manifest — has the weekly compact ever run?" >&2
        return 1
    fi

    # mtime rather than the manifest's own exported_at string: same information,
    # no date parsing, and it cannot disagree with the file actually on disk.
    local age_days
    age_days=$(( ( $(date +%s) - $(stat -c %Y "$manifest") ) / 86400 ))
    if [ "$age_days" -gt "$MAX_EXPORT_AGE_DAYS" ]; then
        echo "export is ${age_days}d old (max ${MAX_EXPORT_AGE_DAYS}d) — the compact has stopped running; refusing to ship a stale copy under a fresh name" >&2
        return 1
    fi

    local stamp
    stamp="$(date -u -d "@$(stat -c %Y "$manifest")" +%Y%m%d-%H%M%S)"
    local name="ks-warehouse-$stamp.tar"

    step "stage archive"
    # Archive layout mirrors the tree a restore needs, so the tar can be
    # unpacked straight into a scratch data dir:
    #   export_parquet/{*.parquet,_manifest.json,_deploy.json}
    #   env.gpg
    mkdir -p "$STAGE/export_parquet"
    cp -a "$EXPORT_DIR/." "$STAGE/export_parquet/"

    # Parquet is not self-describing: phase2_import rebuilds the schema from the
    # application's own DDL, so a restore needs a compatible commit. Without the
    # SHA recorded here, a restore in a year is a bisect.
    cat >"$STAGE/export_parquet/_deploy.json" <<JSON
{
  "deploy_sha": "$(git rev-parse HEAD 2>/dev/null || echo unknown)",
  "hostname": "$(hostname)",
  "archived_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "export_mtime_utc": "$(date -u -d "@$(stat -c %Y "$manifest")" +%Y-%m-%dT%H:%M:%SZ)",
  "restore": "deploy/restore-test.sh, or compact_duckdb.py phase2_import + phase3_validate"
}
JSON

    # The host's .env and the laptop's have already diverged — each holds keys
    # the other lacks — so neither is a complete recovery source. A restored
    # warehouse with no config is a database nothing can start against.
    local env_state="skipped (BACKUP_ENV_PASSFILE unset)"
    if [ -n "$ENV_PASSFILE" ]; then
        if [ ! -r "$ENV_PASSFILE" ]; then
            echo "BACKUP_ENV_PASSFILE is set to $ENV_PASSFILE but it is not readable" >&2
            return 1
        fi
        step "encrypt .env"
        gpg --symmetric --cipher-algo AES256 --batch --yes \
            --passphrase-file "$ENV_PASSFILE" \
            -o "$STAGE/env.gpg" .env
        env_state="included (AES256)"
    else
        echo "note: .env not shipped, BACKUP_ENV_PASSFILE unset — a restore will need the config rebuilt by hand" >&2
    fi

    # Already ZSTD inside; compressing the tar again buys nothing.
    tar -cf "$ARCHIVE_DIR/$name" -C "$STAGE" .
    local size
    size="$(stat -c %s "$ARCHIVE_DIR/$name")"
    echo "staging: $name ($(( size / 1024 )) KB), .env $env_state"

    step "off-site push"
    if [ -z "$REMOTE" ]; then
        echo "BACKUP_REMOTE is not set — this export exists only on this server." >&2
        return "$EX_UNCONFIGURED"
    fi

    rsync --archive --quiet \
        -e "ssh ${SSH_OPTS[*]}" \
        "$ARCHIVE_DIR/$name" "$REMOTE:$REMOTE_DIR/"
    echo "shipping: $REMOTE:$REMOTE_DIR/$name"

    step "prune off-site"
    # Storage Box offers no usable remote shell, so this goes through sftp;
    # deliberately NOT `rsync --delete`, which would mirror a wiped local
    # directory onto the off-site copy and erase the whole history.
    local remote_files old seen kept
    remote_files="$(printf 'cd %s\nls -1\n' "$REMOTE_DIR" \
        | sftp -b - "${SSH_OPTS[@]}" "$REMOTE" 2>/dev/null \
        | grep -o 'ks-warehouse-[0-9]\{8\}-[0-9]\{6\}\.tar' | sort -r)"

    old="$(printf '%s\n' "$remote_files" | tail -n +$((RETAIN + 1)))"
    if [ -n "$old" ]; then
        { printf 'cd %s\n' "$REMOTE_DIR"; printf 'rm %s\n' $old; } \
            | sftp -b - "${SSH_OPTS[@]}" "$REMOTE" >/dev/null
    fi

    seen="$(printf '%s\n' "$remote_files" | grep -c . || true)"
    kept=$(( seen < RETAIN ? seen : RETAIN ))
    echo "pruned: $kept archives held off-site, keeping $RETAIN"

    step "write marker"
    # deploy/offsite_check.sh reads this. A push that stops happening looks
    # exactly like one that works, so something outside this script has to
    # notice the date stop moving.
    date -u +%Y-%m-%dT%H:%M:%SZ >"$MARKER"
}

# Deliberately NOT `if run_push; then`: bash disables errexit for the whole body
# of a function invoked in a condition, so a failed stage would carry on to the
# next step and the alert would name the wrong one — or none at all. An ERR trap
# keeps fail-fast semantics inside run_push.
on_error() {
    local rc=$?
    # Disarm first: with errtrace inherited, any non-zero command inside this
    # handler would re-enter it and spin forever.
    trap - ERR
    set +e
    {
        cat "$LOG"
        if [ "$rc" -eq "$EX_UNCONFIGURED" ]; then
            notify_offsite_unconfigured
        else
            notify_failure
        fi
    } >&4 2>&4
    exit "$rc"
}
trap on_error ERR

run_push >"$LOG" 2>&1

cat "$LOG"
echo "Off-site copy done."
echo "Prove it restores:  deploy/restore-test.sh"
