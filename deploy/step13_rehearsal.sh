#!/usr/bin/env bash
# Step 13 rehearsal (DN-29): the switch, the stand-down and the way back,
# exercised on COPIES of production, beside production, touching none of it.
#
# WHAT IT PROVES — the eight points of PR #268, one table:
#   P1  the flip with every precondition set: mode postgres, no
#       warehouse_refresh job, the writer recorded, exactly one resolve
#   P2  DuckDB frozen while Postgres derives: warehouse_dirty and the last
#       warehouse refresh unchanged, the derivation journal growing
#   P3  web killed inside the Postgres derivation: the owed state survives and
#       the first tick after the restart rebuilds it
#   P4  the design's mutations are caught: a deleted Silver row, a landed
#       order, an unknown sales_type
#   P5  dq_mirror_landing and dq_integrity_check under the stand-down
#   P6  two restarts, one resolve — each one a stop and a start the
#       container's own state shows, the stop before one of them the
#       rehearsal's SIGKILL (exit 137)
#   P7  one precondition broken: runs as duckdb, the canary pages
#   P8  the way back: a full DuckDB rebuild owed, held, released
# plus D1 (what P3's kill cost DuckDB's indexes: the rows reh-web wrote after
# the graceful stop before it — one integrity run and the fixture's fourth
# version — read through the product's reader after the kill and deleted at
# the end through every index on their tables that holds them, and every
# single-column index swept at both stops. An index holds no row with a NULL
# key: the fixture has no buyer and no manager, so it is in two of the four
# composite indexes on orders, and D1 names the ones it could not ask.
# DuckDB 1.5.5 can lose index entries across a SIGKILL, so P4 and P5 read
# their runs at that earlier stop), K0 (KeyCRM never called) and Z0 (nothing
# else on the host moved).
# Every stop of reh-web gets production's grace (STOP_GRACE_S), so it is the
# stop a deploy makes, and records the container's state after it, so one
# that outruns the grace is recorded as the kill it is: P6 and D1 read F5s's
# and F7's, P8 the two around the way back.
#
# HOW IT STAYS AWAY FROM PRODUCTION
#   - Its own containers only, every one named reh-*, on its own network
#     `reh-net`, created --internal: no route out at all, so KeyCRM, Telegram
#     and every production container are unreachable by construction. No
#     published port. No `docker compose` — production's project is never
#     addressed.
#   - Production data enters as copies only: the newest nightly Postgres dump
#     (deploy/pg_backup.sh writes backups/postgres/ks-*.dump), streamed into
#     reh-pg over stdin, and the newest DuckDB backup (the web job db_backup
#     writes data/backups/analytics-*.duckdb, complete before it is renamed —
#     never the live file, whose copy is torn), copied into /root/reh-step13
#     (mode 700). Both copies go at the end. Production's .env is read for the
#     write-chain flags alone, each by its exact name.
#   - reh-web runs with KS_ALERTS_DISABLED=1 and no BOT_TOKEN, against a
#     KeyCRM stub (deploy/step13_rehearsal/keycrm_stub.py) that answers like
#     an account with nothing in it. Nothing in the application switches the
#     sync off, so the base URL is what keeps KeyCRM's quota untouched; K0
#     proves it from the logs.
#   - The canary is judged by importing bot/canary.py inside reh-web against
#     its own /api/health. Nothing is sent.
#   - Hard memory caps on every container (reh-web 1.5g with DuckDB at 768MB,
#     reh-pg 512m, reh-ch 1.5g), each one the kernel's first choice if the
#     host runs short anyway (--oom-score-adj 1000), a start guard on
#     MemAvailable and on the disk the copies will fill, and a watchdog that
#     kills the rehearsal's containers itself before the live web could be
#     the one starved. /tmp/ks-gate.lock is taken first, so no gate runs
#     beside it, and nothing left running in the background keeps it.
#   - The image production runs, never pulled (`--pull never` everywhere): a
#     pull would change what the next `up -d` starts. `--build` builds
#     reh-web:local from the tree instead.
#
# USAGE (on the host, as root, from a window the guard accepts — Tue–Fri,
# 13:15–14:00 Kyiv in practice; ~75 min):
#   bash /opt/key-api-bot/deploy/step13_rehearsal.sh
#   ... --keep           leave the containers stopped and the copies in place
#                        (D1 then does not delete the rows it asks of: UNKNOWN)
#   ... --cleanup-only   remove whatever a killed run left, and stop
#   ... --any-hour       skip the window guard
#   ... --build          rehearse the tree instead of the deployed image
#   ... --local          on a laptop, over synthetic data (seed_synthetic.py);
#                        refused on the host, whose guards it skips
#
# Exit: 0 every point PASS, 1 any FAIL, 2 nothing failed but something is
# UNKNOWN, 3 the rehearsal could not be set up. The table, with ids, counts
# and keys only, also lands in step13-rehearsal-<stamp>.txt beside the
# rehearsal's directory (/root on the host).
set -Eeuo pipefail

SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$SELF_DIR/.." && pwd)"
# probe.py, keycrm_stub.py and seed_synthetic.py, mounted read-only at /reh as
# a directory: a single-file bind mount keeps the inode it was given, and an
# editor or a `git pull` that replaces the file leaves the container reading
# a deleted one — seen on the first local run.
HELPER_DIR="$SELF_DIR/step13_rehearsal"
SQL_DIR="$HELPER_DIR/sql"
PROD_ENV="$REPO/.env"

# ─── Names. Every container and the network are reh-*, and nothing else is
# ever named: tests/unit/test_step13_rehearsal_script.py fails on any docker
# command aimed at a name that is not one of these.
REH_NET=reh-net
REH_PG=reh-pg
REH_CH=reh-ch
REH_KEYCRM=reh-keycrm
REH_WEB=reh-web
REH_SEED=reh-seed
REH_PROBE=reh-probe
REH_MIGRATE=reh-migrate

# Production's own versions, so the dump is restored into the server that
# wrote it: docker-compose.yml's postgres, and the ClickHouse the stores'
# gate pins as production's (both held there by the script's tests).
PG_IMAGE=postgres:17.2-alpine
CH_IMAGE=clickhouse/clickhouse-server:24.8.14.39-alpine

# The grace every graceful stop of reh-web gets: production's. Its web service
# sets no stop_grace_period, so a deploy's `up -d` stops it as compose does by
# default — SIGTERM, and SIGKILL 10 s later. A stop that outruns it here ends
# exit 137, the state a kill leaves, and is judged as the kill a deploy would
# have made, never as the checkpoint the phase wanted (probe.py `stop_kind`).
STOP_GRACE_S=10
# How long a `docker kill` may take to show as an exit before the kill is
# taken as one that did not land.
KILL_WAIT_S=30

# The caps, and what the start guard asks the host to have free beyond them.
WEB_MEM=1536m
PG_MEM=512m
CH_MEM=1536m
STUB_MEM=64m
CAPS_MIB=3648
MEM_HEADROOM_MIB=1024
MEM_FLOOR_MIB=768
# The live web's disk watchdog warns at 75% (core/disk_monitor.py,
# WARN_DISK_PCT). The rehearsal ends under it, a point below: projected at the
# start, watched while it runs.
DISK_CEIL_PCT=74
WATCH_INTERVAL_S=10
MEMINFO=/proc/meminfo
DOCKER_ROOT=""

LOCAL=0
KEEP=0
CLEANUP_ONLY=0
BUILD=0
ANY_HOUR=0
for arg in "$@"; do
    case "$arg" in
        --local) LOCAL=1 ;;
        --keep) KEEP=1 ;;
        --cleanup-only) CLEANUP_ONLY=1 ;;
        --build) BUILD=1 ;;
        --any-hour) ANY_HOUR=1 ;;
        -h|--help) awk 'NR > 1 && /^set -Eeuo/ { exit } NR > 1' "$0"; exit 0 ;;
        *) echo "unknown argument: $arg" >&2; exit 3 ;;
    esac
done

if [ "$LOCAL" = 1 ]; then
    REH_ROOT="${REH_ROOT:-${TMPDIR:-/tmp}/reh-step13}"
    REH_IMAGE="${REH_IMAGE:-reh-web:local}"
    REH_MIGRATE_IMAGE="${REH_MIGRATE_IMAGE:-reh-migrate:local}"
    FLOOR_S="${REH_FLOOR_S:-60}"
    HEALTH_TIMEOUT=600
    STEP_TIMEOUT=600
    RELEASE_TIMEOUT=900
else
    REH_ROOT=/root/reh-step13
    REH_IMAGE="${REH_IMAGE:-halloweex/keycrm-web:latest}"
    REH_MIGRATE_IMAGE="${REH_MIGRATE_IMAGE:-halloweex/keycrm-migrate:latest}"
    FLOOR_S="${REH_FLOOR_S:-120}"
    HEALTH_TIMEOUT=1500
    STEP_TIMEOUT=1500
    RELEASE_TIMEOUT=2400
fi
if [ "$BUILD" = 1 ]; then
    REH_IMAGE=reh-web:local
    REH_MIGRATE_IMAGE=reh-migrate:local
fi
REH_ROOT="${REH_ROOT%/}"
REPORT_DIR="$(dirname "$REH_ROOT")"
DATA_DIR="$REH_ROOT/data"
LOG_DIR="$REH_ROOT/logs"
KEYCRM_DIR="$REH_ROOT/keycrm"
EV="$REH_ROOT/evidence"
SEED_DIR="$REH_ROOT/seed"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
REPORT="$REPORT_DIR/step13-rehearsal-$STAMP.txt"
MAIN_PID=$$

say() { printf '[reh %s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { say "ABORT: $*"; exit 3; }

# The one directory this script deletes, and the guard on it: a suffix of its
# own, never the repository, never production's tree, never `/`.
safe_root() {
    case "$REH_ROOT" in
        */reh-step13) ;;
        *) die "REH_ROOT must end in /reh-step13 (is $REH_ROOT)" ;;
    esac
    case "$REH_ROOT/" in
        "$REPO"/*|/opt/key-api-bot/*|/reh-step13/) die "REH_ROOT may not be $REH_ROOT" ;;
    esac
}
safe_root

# --local skips every guard the host has — the window, MemAvailable, the
# watchdog — and may build an image with nothing capping it. A laptop's mode:
# on the host (root, or the deployed tree there) it is refused before the
# lock, before any container, before anything at all.
if [ "$LOCAL" = 1 ] && { [ "$(id -u)" = 0 ] || [ -e /opt/key-api-bot ]; }; then
    die "--local is for a laptop, and this looks like the host (root, or /opt/key-api-bot exists): run without it"
fi

# ─── Cleanup ──────────────────────────────────────────────────────────────────
#
# `-v`, as every removal in this repository: both store images declare a
# VOLUME, and a removal without it orphans one per run. Leading and trailing,
# like the gates, so a run that was SIGKILLed is cleared by the next.
BUILT=0
WATCHDOG_PID=""
kill_all() {
    # Every rehearsal container stopped at once, volumes kept: the
    # watchdog's own first move.
    docker kill "$REH_WEB" "$REH_SEED" "$REH_KEYCRM" "$REH_CH" "$REH_PG" "$REH_PROBE" "$REH_MIGRATE" >/dev/null 2>&1 || true
}

remove_all() {
    docker rm -f -v "$REH_WEB" "$REH_SEED" "$REH_KEYCRM" "$REH_CH" "$REH_PG" "$REH_PROBE" "$REH_MIGRATE" >/dev/null 2>&1 || true
    docker network rm "$REH_NET" >/dev/null 2>&1 || true
    if [ "$BUILT" = 1 ]; then
        # The cache the build added, capped where it is filled: the gates' bound.
        docker builder prune -f --max-used-space "${GATE_CACHE_MAX:-4GB}" >/dev/null 2>&1 || true
    fi
    safe_root
    rm -rf "$REH_ROOT"
}

cleanup() {
    local rc=$?
    # Once, and whole: a second signal — the watchdog's, a second Ctrl-C —
    # must not cut the removal short and leave copies of production behind.
    trap '' TERM INT HUP
    if [ -n "$WATCHDOG_PID" ]; then
        kill "$WATCHDOG_PID" >/dev/null 2>&1 || true
        WATCHDOG_PID=""
    fi
    if [ "$KEEP" = 1 ]; then
        docker stop -t 30 "$REH_WEB" "$REH_SEED" "$REH_KEYCRM" "$REH_CH" "$REH_PG" >/dev/null 2>&1 || true
        printf '\033[31m%s\033[0m\n' \
            "--keep: copies of production are left behind, customer names and phone numbers among them — $REH_ROOT (the DuckDB copy), and the stopped $REH_PG and $REH_CH with their anonymous volumes (Postgres restored whole from the dump; ClickHouse's Silver). Remove all three with: bash $0 --cleanup-only" >&2
        return "$rc"
    fi
    remove_all
    return "$rc"
}

# ─── One rehearsal on this host at a time, and no gate beside it ──────────────
#
# The gates' lock (deploy/gate_with_stores.sh): their stores are named, they
# build one tag, and a rehearsal holding the host's memory for an hour is a
# bad neighbour for a suite. `-n`: a gate that holds it is a reason to come
# back later, not to wait an hour in front of it.
exec 9>/tmp/ks-gate.lock
if command -v flock >/dev/null 2>&1; then
    flock -n 9 || die "a gate or another rehearsal holds /tmp/ks-gate.lock"
elif [ "$LOCAL" = 1 ]; then
    say "no flock on this machine; --local runs without the host lock"
else
    die "flock is required on the host"
fi
trap cleanup EXIT
trap 'exit 143' TERM INT HUP

# What a killed run left behind goes first, whatever --keep says.
remove_all
if [ "$CLEANUP_ONLY" = 1 ]; then
    KEEP=0
    say "cleanup done"
    exit 0
fi

# ─── Guards (the host only) ───────────────────────────────────────────────────

next_instant_in() {
    # Seconds until the next HH:MM in timezone $1 (empty: the host clock).
    # GNU date, the host's.
    local tz="$1" hm="$2" now at
    now="$(date +%s)"
    if [ -n "$tz" ]; then
        at="$(TZ="$tz" date -d "today $hm" +%s)"
    else
        at="$(date -d "today $hm" +%s)"
    fi
    [ "$at" -lt "$now" ] && at=$((at + 86400))
    echo $((at - now))
}

window_guard() {
    [ "$ANY_HOUR" = 1 ] && { say "window guard skipped (--any-hour)"; return 0; }
    local hm left hour
    # reh-web's own crons (Kyiv) — the copy runs them too — and the sync's
    # off-hours interval, which would stretch every landing to five minutes.
    hour="$(TZ=Europe/Kyiv date +%H)"
    hour=$((10#$hour))
    if [ "$hour" -lt 7 ] || [ "$hour" -ge 21 ]; then
        die "Kyiv hour $hour: inside the sync's off-hours or too close to them (--any-hour overrides)"
    fi
    for hm in 01:00 03:00 03:30 04:00 04:30 05:00 05:15 05:30 07:00 07:30 09:00 09:30 09:45 13:00 19:00; do
        left="$(next_instant_in Europe/Kyiv "$hm")"
        [ "$left" -lt 6000 ] && die "a reh-web cron at $hm Kyiv falls inside the next 100 min (--any-hour overrides)"
    done
    # The host's crons, on the host clock: backups, drills, the compact.
    for hm in 02:00 02:45 03:30 06:50 07:20 07:40 08:10 08:40 09:00; do
        left="$(next_instant_in "" "$hm")"
        [ "$left" -lt 6000 ] && die "a host cron at $hm (host clock) falls inside the next 100 min (--any-hour overrides)"
    done
    say "window guard: clear for 100 min"
}

memory_guard() {
    local avail
    avail="$(awk '/^MemAvailable:/ {print int($2 / 1024)}' /proc/meminfo)"
    [ "$avail" -ge $((CAPS_MIB + MEM_HEADROOM_MIB)) ] \
        || die "MemAvailable ${avail} MiB < caps ${CAPS_MIB} + ${MEM_HEADROOM_MIB} MiB"
    say "memory guard: ${avail} MiB available"
}

disk_used_pct() {
    # df's Use% of the filesystem holding $1: used over used + available,
    # which reads higher than a statvfs total (the root reserve) — the
    # cautious one of the two.
    df -Pk "$1" | awk 'NR==2 && $3 + $4 > 0 {print int(($3 * 100 + $3 + $4 - 1) / ($3 + $4))}'
}

disk_guard() {
    # $1 the DuckDB backup, $2 the Postgres dump. What the run will write:
    # the DuckDB copy and as much again for its growth and spill (P8's full
    # rebuild under a 768 MB memory limit); the dump restored — a custom-format
    # dump is compressed, so five times its size, and 1 GiB of WAL beside it;
    # ClickHouse's Silver and the logs, 1.5 GiB. All of it projected onto
    # each filesystem that will hold part of it — the copies' and Docker's —
    # as if it all landed there, and held under DISK_CEIL_PCT: a run that
    # would end over the live monitor's WARN pages production's admins about
    # the rehearsal's own files.
    local backup_kb dump_kb need_kb fs used avail pct
    backup_kb="$(du -k "$1" | awk '{print $1}')"
    dump_kb="$(du -k "$2" | awk '{print $1}')"
    need_kb=$(( backup_kb * 2 + dump_kb * 5 + (1024 + 1536) * 1024 ))
    for fs in "$REPORT_DIR" "$DOCKER_ROOT"; do
        [ -n "$fs" ] || continue
        read -r used avail < <(df -Pk "$fs" | awk 'NR==2 {print $3, $4}')
        [ "$avail" -ge "$need_kb" ] || die "not enough disk on $fs: ${avail} KB free, ${need_kb} KB needed"
        pct=$(( (used + need_kb) * 100 / (used + avail) ))
        [ "$pct" -lt "$DISK_CEIL_PCT" ] \
            || die "disk on $fs would end at ${pct}% used with the copies (${need_kb} KB): ${DISK_CEIL_PCT}% is the ceiling, the live monitor warns at 75%"
        say "disk guard: $fs ends at ~${pct}% at most"
    done
}

watch_memory() {
    # In the background for as long as the shell it guards is alive, and
    # launched without fd 9 (see the launch). Memory and disk both: either
    # one running out is the live stack's problem before it is ours.
    local min=999999 avail fs pct why fired=0
    while kill -0 "$MAIN_PID" 2>/dev/null; do
        why=""
        avail="$(awk '/^MemAvailable:/ {print int($2 / 1024)}' "$MEMINFO" 2>/dev/null || true)"
        if [ -n "$avail" ]; then
            if [ "$avail" -lt "$min" ]; then
                min="$avail"
                echo "$min" > "$REH_ROOT/mem.min" 2>/dev/null || true
            fi
            if [ "$avail" -lt "$MEM_FLOOR_MIB" ]; then
                why="MemAvailable ${avail} MiB under ${MEM_FLOOR_MIB}"
            fi
        fi
        for fs in "$REPORT_DIR" "$DOCKER_ROOT"; do
            [ -n "$fs" ] || continue
            pct="$(disk_used_pct "$fs" 2>/dev/null || true)"
            if [ -n "$pct" ] && [ "$pct" -ge "$DISK_CEIL_PCT" ]; then
                why="${why:+$why; }disk on $fs at ${pct}%"
            fi
        done
        if [ -n "$why" ]; then
            echo "[reh] $why: killing the rehearsal's containers, then its shell" >&2
            # Itself, and first. The shell runs its TERM trap only once its
            # foreground command returns — a docker exec bounded at 25 min,
            # pg_restore at nothing — and until then the containers keep
            # every byte they hold. Killed, they free it now, and whatever
            # the shell was waiting on returns with them.
            kill_all
            if [ "$fired" = 0 ]; then
                kill -TERM "$MAIN_PID" 2>/dev/null || true
                fired=1
            fi
        fi
        sleep "$WATCH_INTERVAL_S"
    done
    # The shell is gone without its EXIT trap — SIGKILL, the kernel's OOM
    # killer — so nothing else will remove the copies of production it
    # started, or stop them holding 3.6 GB.
    echo "[reh] the rehearsal's shell died without cleaning up: removing what it started" >&2
    if [ "$KEEP" = 1 ]; then
        kill_all
    else
        remove_all
    fi
}

rand() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

# ─── Helpers ──────────────────────────────────────────────────────────────────

pgq() {
    # One statement on the copy, as postgres inside reh-pg; empty on failure.
    docker exec -i -e PGAPPNAME=reh-rehearsal "$REH_PG" \
        psql -U postgres -d ks -XAtq -F'|' -v ON_ERROR_STOP=1 -c "$1" 2>>"$LOG_DIR/psql.err" || true
}

pgfile() {
    # One of the rehearsal's mutation files, onto the copy alone.
    local file="$1"
    shift
    docker exec -i -e PGAPPNAME=reh-rehearsal "$REH_PG" \
        psql -U postgres -d ks -XAtq -v ON_ERROR_STOP=1 "$@" < "$SQL_DIR/$file" 2>>"$LOG_DIR/psql.err"
}

wprobe() {
    # The helper inside the process under test, which publishes no port.
    docker exec "$REH_WEB" python /reh/probe.py "$@"
}

running() {
    [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || echo false)" = true ]
}

oom_killed() {
    [ "$(docker inspect -f '{{.State.OOMKilled}}' "$1" 2>/dev/null || echo false)" = true ]
}

state_of() {
    # $1 container, $2 (optional) more members, `"k": v, ...`: its state as
    # one JSON object — whether it runs, how its last stop ended, when it
    # last started. P3's and P6's proof that a stop and a start happened, and
    # which kind: `docker start` on a running container is a no-op that says
    # nothing, so a record of a restart must carry what the container shows.
    # StartedAt moves on every start; RestartCount moves only for a restart
    # policy, which these do not have.
    docker inspect -f '{"running": {{.State.Running}}, "exit_code": {{.State.ExitCode}}, "oom_killed": {{.State.OOMKilled}}, "started_at": "{{.State.StartedAt}}", "finished_at": "{{.State.FinishedAt}}", "restart_count": {{.RestartCount}}'"${2:+, $2}"'}' \
        "$1" 2>/dev/null || echo null
}

wait_stopped() {
    # $1 container, $2 timeout seconds: until it no longer runs. `docker
    # kill` returns once the signal is sent, before the exit is recorded.
    local name="$1" deadline=$((SECONDS + $2))
    while running "$name"; do
        [ "$SECONDS" -lt "$deadline" ] || return 1
        sleep 0.5
    done
}

sigkilled() {
    # $1 a `state_of` file: stopped, exit 137, and not by the kernel's OOM
    # killer. After `kill_web`, the rehearsal's own SIGKILL and nothing else;
    # the judges read the same file and decide for themselves (`stop_kind`).
    [ "$(json_field "$1" running)" = false ] \
        && [ "$(json_field "$1" exit_code)" = 137 ] \
        && [ "$(json_field "$1" oom_killed)" = false ]
}

kill_web() {
    # $1 container, $2 a file for its state after the kill. SIGKILL, then the
    # exit as Docker records it (`docker kill` returns before that), and the
    # state with what was asked; succeeds only when the container shows the
    # rehearsal's SIGKILL. A container that was not running is not killed.
    local was=false
    running "$1" && was=true
    docker kill "$1" >/dev/null 2>&1 || true
    wait_stopped "$1" "$KILL_WAIT_S" || say "$1 still runs $KILL_WAIT_S s after the kill"
    state_of "$1" "\"asked\": \"kill\", \"was_running\": $was" > "$2"
    [ "$was" = true ] && sigkilled "$2"
}

start_stopped() {
    # $1 container: a start only where something stopped. On a running
    # container `docker start` does nothing, and nothing it did may be
    # recorded as a restart.
    running "$1" || docker start "$1" >/dev/null
}

max_int() {
    # The largest of the arguments that are whole numbers; 0 when none is.
    local m=0 v
    for v in "$@"; do
        case "$v" in
            ''|*[!0-9]*) ;;
            *) [ "$v" -gt "$m" ] && m="$v" ;;
        esac
    done
    echo "$m"
}

wait_health() {
    # $1 container, $2 timeout seconds. Fails when the container dies first.
    local name="$1" deadline=$((SECONDS + $2))
    while [ "$SECONDS" -lt "$deadline" ]; do
        running "$name" || return 1
        if docker exec "$name" python /reh/probe.py wait-health --timeout 20 >/dev/null 2>&1; then
            return 0
        fi
    done
    return 1
}

log_count() {
    # Lines of file $1 containing the fixed string $2.
    grep -cF -- "$2" "$1" 2>/dev/null || true
}

save_log() {
    docker logs "$1" > "$LOG_DIR/$2.log" 2>&1 || true
}

poll_pg() {
    # $1 SQL, $2 timeout seconds, $3 interval: the first non-empty answer.
    local sql="$1" deadline=$((SECONDS + $2)) out
    while [ "$SECONDS" -lt "$deadline" ]; do
        out="$(pgq "$sql")"
        if [ -n "$out" ]; then
            printf '%s' "$out"
            return 0
        fi
        sleep "${3:-2}"
    done
    return 1
}

restart_record() {
    # P6's record of one restart of the flipped reh-web: $1 the snapshot it
    # answered with, $2 the file, $3 the `state_of` file written right after
    # the stop before it. Written only for a restart the container shows —
    # that stop left it stopped, and it runs now — and with both states in
    # it, the start's read here: P6 reads the stop's kind out of the
    # container's state (exit 0, the rehearsal's SIGKILL, a grace that ran
    # out), never out of the script. The log lines are counted over every
    # start of the container, so each record says how many there have been
    # by then.
    if [ "$(json_field "$3" running)" != false ] || ! running "$REH_WEB"; then
        say "no restart of $REH_WEB to record in $(basename "$2"): it was not stopped, or does not run"
        return 0
    fi
    printf '{"stop": %s, "start": %s, "snapshot": %s, "resolved_events": %s, "recorded_lines": %s, "resolved_lines": %s}\n' \
        "$(cat "$3")" \
        "$(state_of "$REH_WEB")" \
        "$(cat "$1" 2>/dev/null || echo null)" \
        "$(pgq "$RESOLVED_SQL")" \
        "$(log_count "$LOG_DIR/flip.log" 'warehouse writer recorded as postgres')" \
        "$(grep -F 'suppressed (KS_ALERTS_DISABLED)' "$LOG_DIR/flip.log" | grep -cF 'Resolved:' || true)" \
        > "$2"
}

p3_record() {
    # P3's evidence once the blocked TRUNCATE was seen: $1 the file, $2 the
    # kill's `state_of` file, then requested and built at the kill, the
    # fixture version Silver held, and the three reads after it (JSON). The
    # kill's state goes in whole: P3 and D1 judge a kill the container
    # showed, not one the script sent.
    printf '{"seen_blocked": true, "stop": %s, "requested": %s, "built": %s, "silver_version": %s, "after_kill": %s, "first_after_restart": %s, "built_after": %s}\n' \
        "$(cat "$2" 2>/dev/null || echo null)" "${3:-null}" "${4:-null}" "${5:-null}" \
        "${6:-null}" "${7:-null}" "${8:-null}" > "$1"
}

window_run() {
    # F5w: $1 the floor — the newest DQ run F5s's checkpoint holds, or F4's
    # own when that was not read. Triggers one integrity run and prints its
    # id only when the trigger ran and the run is above the floor: a failed
    # trigger, or a read of the newest run that comes back with an older
    # one, is no run written after the checkpoint.
    local floor="$1" before id
    before="$(wprobe dq-last integrity 2>/dev/null | grep -o '"run_id": [0-9]*' | grep -o '[0-9]*' || true)"
    floor="$(max_int "$floor" "$before")"
    if ! wprobe trigger-wait dq_integrity_check --timeout "$STEP_TIMEOUT" > "$EV/f5w_integrity_job.json" 2>&1; then
        say "F5w: the integrity job did not run (see f5w_integrity_job.json)"
        return 0
    fi
    id="$(wprobe wait-dq integrity --after "$floor" --timeout 60 2>/dev/null \
        | tee "$EV/f5w_integrity_live.json" | grep -o '"run_id": [0-9]*' | grep -o '[0-9]*' || true)"
    if [ -n "$id" ] && [ "$id" -gt "$floor" ]; then
        echo "$id"
    fi
    return 0
}

json_field() {
    # A scalar out of a one-object JSON file, without jq (the host has none).
    grep -o "\"$2\": [^,}]*" "$1" 2>/dev/null | head -n 1 | sed -e "s/^\"$2\": //" -e 's/^"//' -e 's/"$//'
}

others() {
    # Every container on the host that is not the rehearsal's, with what a
    # restart or an OOM kill changes: Z0's evidence. An id and a name alone
    # would read a live web the kernel killed and Docker restarted as
    # "unchanged" — same id, same name. Read-only: `docker inspect` of the
    # ids `docker ps` listed; one removed between the two calls is skipped.
    local ids
    ids="$(docker ps -aq --no-trunc)"
    [ -n "$ids" ] || return 0
    # One word per id, on purpose.
    # shellcheck disable=SC2086
    { docker inspect -f '{{.Id}} {{.Name}} {{.State.StartedAt}} {{.RestartCount}} {{.State.OOMKilled}}' \
        $ids 2>/dev/null || true; } | sed 's# /# #' | { grep -v ' reh-' || true; } | sort
}

as_json_list() {
    awk 'BEGIN { printf "[" } { gsub(/"/, ""); printf "%s\"%s\"", (NR > 1 ? ", " : ""), $0 } END { printf "]" }'
}

# The stores' secrets, minted for this run and gone with it. Not one of them
# is production's: the restore keeps the copy's role names and initdb sets
# their passwords from these.
PG_PW="$(rand)"
APP_PW="$(rand)"
RO_PW="$(rand)"
CH_PW="$(rand)"
SESSION_KEY="$(rand)"
APP_DSN="postgresql://ks_app:$APP_PW@$REH_PG:5432/ks"

# reh-web's environment: an explicit list, never an env file. The copy's
# readers come from the image itself (`probe.py readers`), so the list cannot
# drift from the code under test; the chain flags are read from production's
# .env by exact name, nothing else of it. Every other switch the cutover's
# preconditions name is set here, and a test runs the tree's own
# `evaluate_preconditions` over this list with F1's and phase 0's additions:
# KS_GOALS_HISTORY came with chain 7b, was missing, and held the flip back.
WEB_ENV=(
    -e TZ=Europe/Kyiv
    -e KS_ROLE=web
    -e KS_INSTANCE=reh-step13
    -e KS_ALERTS_DISABLED=1
    -e KEYCRM_API_KEY=reh-step13-stub-key-not-keycrm
    -e "KEYCRM_BASE_URL=http://$REH_KEYCRM:8080/v1"
    -e "DASHBOARD_SECRET_KEY=$SESSION_KEY"
    -e ADMIN_USER_IDS=1
    -e MEILI_URL=http://reh-nomeili.invalid:7700
    -e DUCKDB_MEMORY_LIMIT=768MB
    -e "KS_PG_DSN=$APP_DSN"
    -e "KS_CH_URL=http://$REH_CH:8123"
    -e KS_CH_USER=default
    -e "KS_CH_PASSWORD=$CH_PW"
    -e KS_BOT_STORE=postgres
    -e KS_USER_STORE=postgres
    -e KS_MIRROR_LANDING=1
    -e KS_PG_DERIVE=own
    -e KS_DQ_PG_WAREHOUSE=on
    -e KS_UTM_PARSE=postgres
    -e KS_GOALS_HISTORY=silver
    -e KS_READ_COHORTS=clickhouse
    -e "KS_PG_SILVER_INTERVAL_S=$FLOOR_S"
)

start_web() {
    # $1 container name; the rest are extra -e flags for this phase.
    local name="$1"
    shift
    docker run -d --name "$name" --network "$REH_NET" --pull never --oom-score-adj 1000 --restart no \
        --memory "$WEB_MEM" --memory-swap "$WEB_MEM" --cpus 1.0 \
        --log-opt max-size=50m --log-opt max-file=2 \
        -v "$DATA_DIR:/app/data" \
        -v "$HELPER_DIR:/reh:ro" \
        "${WEB_ENV[@]}" "${READER_ENV[@]}" ${CHAIN_ENV[@]+"${CHAIN_ENV[@]}"} "$@" \
        "$REH_IMAGE" >/dev/null
}

probe_offline() {
    # The DuckDB copy, read while no reh-web holds it: `--network none`.
    docker run --rm --name "$REH_PROBE" --network none --pull never --oom-score-adj 1000 \
        --memory 1g --memory-swap 1g --log-opt max-size=10m \
        -v "$DATA_DIR:/app/data" \
        -v "$HELPER_DIR:/reh:ro" \
        --entrypoint python "$REH_IMAGE" /reh/probe.py "$@"
}

probe_keycrm_dir() {
    # Writes the stub's one order; touches nothing but the stub's directory.
    docker run --rm --name "$REH_PROBE" --network none --pull never --oom-score-adj 1000 \
        --memory 128m --memory-swap 128m --log-opt max-size=10m \
        -v "$KEYCRM_DIR:/keycrm" \
        -v "$HELPER_DIR:/reh:ro" \
        --entrypoint python "$REH_IMAGE" /reh/probe.py "$@"
}

stop_web() {
    # $1 container, $2 log name, $3 a file for its state after the stop —
    # every stop of reh-web passes one (the seed's alone does not), and a
    # test holds the script to it. Graceful within production's grace, so
    # DuckDB is closed — and checkpointed — where a deploy would close it,
    # and killed where a deploy would kill it. The state says which, with
    # what was asked and how long the stop took.
    local was=false t0
    save_log "$1" "$2"
    running "$1" && was=true
    t0=$SECONDS
    docker stop -t "$STOP_GRACE_S" "$1" >/dev/null 2>&1 || true
    if [ -n "${3:-}" ]; then
        state_of "$1" "\"asked\": \"stop\", \"was_running\": $was, \"grace_s\": $STOP_GRACE_S, \"stop_s\": $((SECONDS - t0))" > "$3"
    fi
    save_log "$1" "$2"
}

# ─── S: set up ────────────────────────────────────────────────────────────────

if [ "$LOCAL" = 0 ]; then
    [ "$(id -u)" = 0 ] || die "run as root on the host"
    window_guard
    memory_guard
    DOCKER_ROOT="$(docker info -f '{{.DockerRootDir}}' 2>/dev/null || true)"
    [ -n "$DOCKER_ROOT" ] || die "docker info names no DockerRootDir"
fi

install -d -m 700 "$REH_ROOT"
install -d -m 700 "$LOG_DIR" "$EV" "$SEED_DIR"
install -d -m 755 "$DATA_DIR" "$KEYCRM_DIR"
: > "$LOG_DIR/psql.err"
others > "$REH_ROOT/others.before"

if [ "$BUILD" = 1 ] || { [ "$LOCAL" = 1 ] && ! docker image inspect "$REH_IMAGE" >/dev/null 2>&1; }; then
    say "building $REH_IMAGE and $REH_MIGRATE_IMAGE from $REPO"
    BUILT=1
    docker build -q -f "$REPO/Dockerfile.web" -t reh-web:local "$REPO" >/dev/null || die "image build failed"
    docker build -q -f "$REPO/Dockerfile.migrate" -t reh-migrate:local "$REPO" >/dev/null || die "migrate image build failed"
fi
docker image inspect "$REH_IMAGE" >/dev/null 2>&1 || die "no image $REH_IMAGE on this host (never pulled here)"
docker image inspect "$REH_MIGRATE_IMAGE" >/dev/null 2>&1 || die "no image $REH_MIGRATE_IMAGE on this host"
docker image inspect "$PG_IMAGE" >/dev/null 2>&1 || die "no image $PG_IMAGE on this host"
docker image inspect "$CH_IMAGE" >/dev/null 2>&1 || die "no image $CH_IMAGE on this host"

APP_UID="$(docker run --rm --name "$REH_PROBE" --network none --pull never --oom-score-adj 1000 --memory 64m --memory-swap 64m \
    --log-opt max-size=10m --entrypoint id "$REH_IMAGE" -u)"
APP_GID="$(docker run --rm --name "$REH_PROBE" --network none --pull never --oom-score-adj 1000 --memory 64m --memory-swap 64m \
    --log-opt max-size=10m --entrypoint id "$REH_IMAGE" -g)"
if [ "$LOCAL" = 0 ]; then
    chown "$APP_UID:$APP_GID" "$DATA_DIR" "$KEYCRM_DIR"
    # With fd 9 closed: a child that inherits the host lock holds it for as
    # long as it lives, and it outlives a shell that was SIGKILLed — every
    # gate would then wait in `flock 9` for good, and --cleanup-only refuse.
    watch_memory 9>&- &
    WATCHDOG_PID=$!
fi

image_names() {
    # The reader switches or the chain flags, as the image under test declares them.
    docker run --rm --name "$REH_PROBE" --network none --pull never --oom-score-adj 1000 --memory 256m --memory-swap 256m \
        --log-opt max-size=10m -v "$HELPER_DIR:/reh:ro" \
        --entrypoint python "$REH_IMAGE" /reh/probe.py readers --list "$1"
}
READER_ENV=()
for name in $(image_names readers); do
    READER_ENV+=(-e "$name=postgres")
done
[ "${#READER_ENV[@]}" -gt 10 ] || die "the image named no warehouse readers"
CHAIN_ENV=()
CHAIN_NOTE=""
for name in $(image_names chains); do
    value=""
    if [ "$LOCAL" = 1 ]; then
        case " ${REH_LOCAL_CHAIN_FLAGS:-KS_WRITE_EXPENSES=postgres} " in
            *" $name=postgres "*) value=postgres ;;
        esac
    elif [ -f "$PROD_ENV" ]; then
        # Exact name, anchored, first match: the soak's rule. Nothing else of
        # production's .env is read.
        value="$(grep -m1 "^${name}=" "$PROD_ENV" | cut -d= -f2- | tr -d '"'"'"' \r' || true)"
    fi
    if [ -n "$value" ]; then
        CHAIN_ENV+=(-e "$name=$value")
        CHAIN_NOTE="$CHAIN_NOTE $name=$value"
    fi
done

docker network create --internal "$REH_NET" >/dev/null

start_pg() {
    docker run -d --name "$REH_PG" --network "$REH_NET" --pull never --oom-score-adj 1000 --restart no \
        --memory "$PG_MEM" --memory-swap "$PG_MEM" --cpus 0.5 --shm-size 128m \
        --log-opt max-size=20m --log-opt max-file=2 \
        -e POSTGRES_USER=postgres -e "POSTGRES_PASSWORD=$PG_PW" -e POSTGRES_DB=ks \
        -e POSTGRES_INITDB_ARGS="--locale=C.UTF-8 --encoding=UTF8" \
        -e "KS_APP_PASSWORD=$APP_PW" -e "KS_READONLY_PASSWORD=$RO_PW" \
        -v "$REPO/postgres/initdb/10-roles-and-schemas.sql:/docker-entrypoint-initdb.d/10-roles-and-schemas.sql:ro" \
        -v "$REPO/postgres/initdb/15-extensions.sql:/docker-entrypoint-initdb.d/15-extensions.sql:ro" \
        -v "$REPO/postgres/initdb/30-app.sql:/docker-entrypoint-initdb.d/30-app.sql:ro" \
        "$PG_IMAGE" -c shared_buffers=128MB -c work_mem=8MB \
        -c maintenance_work_mem=64MB -c max_connections=40 >/dev/null
    local deadline=$((SECONDS + 120))
    while [ "$SECONDS" -lt "$deadline" ]; do
        # Ready means the init scripts have run and the server restarted.
        if docker exec "$REH_PG" psql -U postgres -d ks -XAtqc \
                "SELECT 1 FROM pg_roles WHERE rolname = 'ks_app'" 2>/dev/null | grep -q 1 \
           && docker exec "$REH_PG" pg_isready -U postgres -q 2>/dev/null; then
            sleep 2
            return 0
        fi
        sleep 2
    done
    die "reh-pg did not come up"
}

migrate() {
    docker run --rm --name "$REH_MIGRATE" --network "$REH_NET" --pull never --oom-score-adj 1000 \
        --memory 256m --memory-swap 256m --log-opt max-size=10m \
        -e "KS_PG_DSN=$APP_DSN" \
        "$REH_MIGRATE_IMAGE" alembic upgrade head > "$LOG_DIR/migrate.log" 2>&1 \
        || die "alembic upgrade head failed on the copy (see $LOG_DIR/migrate.log)"
}

start_stub() {
    docker run -d --name "$REH_KEYCRM" --network "$REH_NET" --pull never --oom-score-adj 1000 --restart no \
        --memory "$STUB_MEM" --memory-swap "$STUB_MEM" --cpus 0.2 \
        --log-opt max-size=20m --log-opt max-file=2 \
        -v "$HELPER_DIR:/reh:ro" \
        -v "$KEYCRM_DIR:/keycrm:ro" \
        --entrypoint python "$REH_IMAGE" /reh/keycrm_stub.py 8080 >/dev/null
}

# ─── --local: a synthetic production, built the way production was ───────────
#
# Never production's bytes: seed_synthetic.py invents a KeyCRM account, the
# stub serves it, and a reh-web over an empty directory syncs it — the landing
# mirror, the backfills, both derivations, the nightly backup — exactly as
# production once synced the real one. Then the rehearsal proper restores
# that dump and that backup, by the same path as on the host.
seed_local() {
    say "local: seeding a synthetic account"
    start_pg
    migrate
    docker run --rm --name "$REH_PROBE" --network none --pull never --oom-score-adj 1000 \
        --memory 256m --memory-swap 256m --log-opt max-size=10m \
        -v "$KEYCRM_DIR:/out" -v "$HELPER_DIR:/reh:ro" \
        --entrypoint python "$REH_IMAGE" /reh/seed_synthetic.py /out \
        > "$LOG_DIR/seed-generate.log" 2>&1 || die "synthetic generation failed"
    start_stub
    start_web "$REH_SEED"
    wait_health "$REH_SEED" "$HEALTH_TIMEOUT" || { save_log "$REH_SEED" seed; die "the seed web never answered"; }
    docker exec "$REH_SEED" python /reh/probe.py api POST \
        '/api/mirror/backfill/orders?background=false' > "$LOG_DIR/seed-backfill-orders.json" 2>&1 \
        || say "local: orders backfill answered non-2xx (see the log)"
    docker exec "$REH_SEED" python /reh/probe.py api POST \
        /api/mirror/backfill/expenses > "$LOG_DIR/seed-backfill-expenses.json" 2>&1 \
        || say "local: expenses backfill answered non-2xx (see the log)"
    # Synthetic history is minutes old; production's is not. Age the landing
    # stamps a day, so the twins' settle grace treats it as settled history.
    pgq "UPDATE bronze.orders SET mirrored_at = mirrored_at - interval '1 day'" >/dev/null
    docker exec "$REH_SEED" python /reh/probe.py api POST /api/warehouse/refresh \
        > "$LOG_DIR/seed-refresh.json" 2>&1 || say "local: the seed refresh answered non-2xx"
    docker exec "$REH_SEED" python /reh/probe.py trigger-wait db_backup --timeout 300 \
        > "$LOG_DIR/seed-backup.json" 2>&1 || die "the seed backup did not run"
    stop_web "$REH_SEED" seed
    docker exec "$REH_PG" pg_dump -U postgres -d ks -Fc > "$SEED_DIR/ks-$STAMP.dump" \
        || die "pg_dump of the seed failed"
    # The newest complete backup, exactly as the host picks one.
    BACKUP_SRC="$(ls -1t "$DATA_DIR"/backups/analytics-*.duckdb 2>/dev/null | head -n 1 || true)"
    [ -n "$BACKUP_SRC" ] || die "the seed wrote no DuckDB backup"
    cp "$BACKUP_SRC" "$SEED_DIR/analytics-seed.duckdb"
    docker rm -f -v "$REH_SEED" "$REH_KEYCRM" "$REH_PG" >/dev/null 2>&1 || true
    rm -rf "$DATA_DIR" "$KEYCRM_DIR"
    install -d -m 755 "$DATA_DIR" "$KEYCRM_DIR"
    DUMP="$SEED_DIR/ks-$STAMP.dump"
    BACKUP="$SEED_DIR/analytics-seed.duckdb"
}

if [ "$LOCAL" = 1 ]; then
    seed_local
else
    # Newest first, as deploy/pg_offsite.sh picks the dump it ships.
    DUMP="$(ls -1t "$REPO"/backups/postgres/ks-*.dump 2>/dev/null | head -n 1 || true)"
    BACKUP="$(ls -1t "$REPO"/data/backups/analytics-*.duckdb 2>/dev/null | head -n 1 || true)"
    [ -n "$DUMP" ] && [ -s "$DUMP" ] || die "no Postgres dump under $REPO/backups/postgres"
    [ -n "$BACKUP" ] && [ -s "$BACKUP" ] || die "no DuckDB backup under $REPO/data/backups"
    for f in "$DUMP" "$BACKUP"; do
        age=$(( $(date +%s) - $(stat -c %Y "$f") ))
        [ "$age" -le 108000 ] || die "$(basename "$f") is $((age / 3600)) h old: over 30 h"
    done
    disk_guard "$BACKUP" "$DUMP"
fi

say "S: restoring $(basename "$DUMP") into $REH_PG"
start_pg
# Ownership kept: the tables belong to ks_app, which initdb created, and
# --no-owner would leave it unable to TRUNCATE Silver. Over stdin, as the
# restore drill does; the copy's roles file (password hashes) is never read.
set +e
docker exec -i "$REH_PG" pg_restore -U postgres -d ks < "$DUMP" > "$LOG_DIR/restore.log" 2>&1
RESTORE_RC=$?
set -e
RESTORE_ERRORS="$(grep -c '^pg_restore: error' "$LOG_DIR/restore.log" || true)"
RESTORE_UNEXPECTED="$(grep '^pg_restore: error' "$LOG_DIR/restore.log" | grep -cv 'already exists' || true)"
[ "$RESTORE_UNEXPECTED" = 0 ] || die "pg_restore: $RESTORE_UNEXPECTED unexpected error(s) (rc $RESTORE_RC; see $LOG_DIR/restore.log)"
migrate
REVISION="$(pgq "SELECT version_num FROM meta.alembic_version")"
[ -n "$REVISION" ] || REVISION="$(pgq "SELECT version_num FROM alembic_version")"
PG_ORDERS="$(pgq "SELECT count(*) FROM bronze.orders")"
[ "${PG_ORDERS:-0}" -gt 0 ] || die "the restored copy holds no orders"

say "S: ClickHouse, the DuckDB copy, the KeyCRM stub"
docker run -d --name "$REH_CH" --network "$REH_NET" --pull never --oom-score-adj 1000 --restart no \
    --memory "$CH_MEM" --memory-swap "$CH_MEM" --cpus 0.5 \
    --log-opt max-size=20m --log-opt max-file=2 \
    -e "CLICKHOUSE_PASSWORD=$CH_PW" \
    "$CH_IMAGE" >/dev/null
if [ "$LOCAL" = 1 ]; then
    cp "$BACKUP" "$DATA_DIR/analytics.duckdb"
else
    install -m 600 -o "$APP_UID" -g "$APP_GID" "$BACKUP" "$DATA_DIR/analytics.duckdb"
fi
start_stub
deadline=$((SECONDS + 120))
until docker exec "$REH_CH" clickhouse-client --password "$CH_PW" -q "SELECT 1" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || die "reh-ch did not come up"
    sleep 2
done

# The copy's latch markers, from the copy's owner rows — so its two halves of
# each latch agree, as they do on the host. Production's markers are not read.
docker run --rm --name "$REH_PROBE" --network "$REH_NET" --pull never --oom-score-adj 1000 \
    --memory 512m --memory-swap 512m --log-opt max-size=10m \
    -e "KS_PG_DSN=$APP_DSN" \
    -v "$DATA_DIR:/app/data" \
    -v "$HELPER_DIR:/reh:ro" \
    --entrypoint python "$REH_IMAGE" /reh/probe.py derive-latches > "$EV/latches.json" 2>"$LOG_DIR/latches.err" \
    || die "the latch markers could not be derived from the copy's owner rows"

IMAGE_ID="$(docker image inspect -f '{{.Id}}' "$REH_IMAGE" | cut -c8-19)"
VERSION="$(docker run --rm --name "$REH_PROBE" --network none --pull never --oom-score-adj 1000 --memory 64m --memory-swap 64m \
    --log-opt max-size=10m --entrypoint cat "$REH_IMAGE" /app/VERSION 2>/dev/null || echo '?')"
PG_MAX="$(pgq "SELECT max(id) || ' / ' || to_char(max(ordered_at) AT TIME ZONE 'Europe/Kyiv', 'YYYY-MM-DD HH24:MI') FROM bronze.orders")"
HEADER1="Step 13 rehearsal · $(hostname 2>/dev/null || echo '?') · $(date -u '+%F %H:%M UTC') · image $IMAGE_ID ($VERSION) · dump $(basename "$DUMP") · backup $(basename "$BACKUP")"

# ─── Phase 0: everything but KS_READ_FALLBACK=off (P7) ───────────────────────
#
# First, while the copy has no writer recorded: after a flip, the same start
# would be the way back, and would spend P8's question.
say "phase 0: every precondition but KS_READ_FALLBACK (P7)"
P0_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
start_web "$REH_WEB" -e KS_WRITE_WAREHOUSE=postgres
if wait_health "$REH_WEB" "$HEALTH_TIMEOUT"; then
    wprobe snapshot > "$EV/p7_snapshot.json" || true
    wprobe canary > "$EV/p7_canary.json" 2>>"$LOG_DIR/probe.err" || true
    save_log "$REH_WEB" phase0
    if grep -qF 'precondition(s) of the switch are unmet' "$LOG_DIR/phase0.log"; then
        echo '{"unmet": true}' > "$EV/p7_log.json"
    else
        echo '{"unmet": false}' > "$EV/p7_log.json"
    fi
    wprobe api GET /api/warehouse/status > "$EV/p7_status_full.json" 2>/dev/null || true
    # The catch-ups this boot queued run here, on the copy's journal ages, so
    # the phases after the flip are not shared with them.
    for pair in dq_integrity_check:integrity dq_mirror_landing:mirror_landing dq_reconciliation:reconciliation; do
        job="${pair%%:*}"
        layer="${pair##*:}"
        if grep -qF "$job (layer" "$LOG_DIR/phase0.log"; then
            before="$(wprobe dq-last "$layer" 2>/dev/null | grep -o '"run_id": [0-9]*' | grep -o '[0-9]*' || true)"
            say "phase 0: waiting for the $job catch-up"
            wprobe wait-dq "$layer" --after "${before:-0}" --timeout "$STEP_TIMEOUT" >/dev/null 2>&1 \
                || say "phase 0: the $job catch-up did not finish in time"
        fi
    done
else
    say "phase 0: reh-web never answered"
fi
stop_web "$REH_WEB" phase0 "$EV/p0_stop_state.json"
probe_offline duckdb-facts --db /app/data/analytics.duckdb --gate /app/data/alert-gate-web.json \
    > "$EV/d0.json" 2>>"$LOG_DIR/probe.err" || true
probe_offline seed-gate --gate /app/data/alert-gate-web.json > "$EV/seed_gate.json" 2>>"$LOG_DIR/probe.err" || true
docker rm -f -v "$REH_WEB" >/dev/null 2>&1 || true

DK_MAX="$(json_field "$EV/d0.json" max_order_id)"
DK_ORDERED="$(json_field "$EV/d0.json" orders)"
HEADER2="copy gap: postgres max id / ordered_at $PG_MAX; duckdb max id ${DK_MAX:-?} over ${DK_ORDERED:-?} orders · revision ${REVISION:-?} · chain flags:${CHAIN_NOTE:- none} · floor ${FLOOR_S}s · restore errors ${RESTORE_ERRORS} (all 'already exists')"

# ─── F1: the flip, with every precondition (P1) ──────────────────────────────
say "F1: the flip"
F1_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
N0="$(pgq "SELECT COALESCE(max(id), 0) FROM meta.derivation_runs")"
start_web "$REH_WEB" -e KS_WRITE_WAREHOUSE=postgres -e KS_READ_FALLBACK=off
FLIPPED=0
if wait_health "$REH_WEB" "$HEALTH_TIMEOUT"; then
    sleep 5
    wprobe snapshot > "$EV/f1_snapshot.json" || true
    save_log "$REH_WEB" flip
    printf '{"not_registered": %s, "holds": %s, "recorded": %s}\n' \
        "$(log_count "$LOG_DIR/flip.log" 'warehouse_refresh not registered: KS_WRITE_WAREHOUSE=postgres')" \
        "$(log_count "$LOG_DIR/flip.log" 'every precondition holds: DuckDB no longer derives')" \
        "$(log_count "$LOG_DIR/flip.log" 'warehouse writer recorded as postgres')" > "$EV/f1_log.json"
    RESOLVED_SQL="SELECT count(*) FROM app.alert_events WHERE event_type = 'resolved' AND instance = 'reh-step13' AND message LIKE '%DuckDB derivation retired (KS_WRITE_WAREHOUSE=postgres)%'"
    printf '{"resolved_events": %s}\n' "$(pgq "$RESOLVED_SQL")" > "$EV/f1_pg.json"
    # The flip as P1's judge reads it, out of the same file: the writer's
    # mode and nothing else. A grep for `"mode": "postgres"` matched
    # `utm_parse` — postgres before any flip — and ran F2 to B over a
    # process that had not flipped.
    if docker exec -i "$REH_WEB" python /reh/probe.py writer-mode \
            < "$EV/f1_snapshot.json" >/dev/null 2>>"$LOG_DIR/probe.err"; then
        FLIPPED=1
    fi
fi

if [ "$FLIPPED" = 1 ]; then
    # ─── F2: three landings, each derived (P2, P4b) ───────────────────────────
    say "F2: ClickHouse sync, then three versions of one order"
    wprobe trigger-wait ch_sync --timeout "$STEP_TIMEOUT" > "$EV/f2_ch_sync.json" 2>&1 || say "F2: ch_sync did not finish"
    PG_MAXID="$(pgq "SELECT COALESCE(max(id), 0) FROM bronze.orders")"
    FIX_ID=$(( (PG_MAXID > ${DK_MAX:-0} ? PG_MAXID : ${DK_MAX:-0}) + 1 ))
    PRODUCT_ID="$(pgq "SELECT min(id) FROM bronze.products")"
    LAND_TIMEOUT=$((300 + FLOOR_S + 120))
    DERIVE_TIMEOUT=$((FLOOR_S + 240))
    items=""
    for k in 1 2 3; do
        req_before="$(pgq "SELECT requested FROM meta.derivation_signal WHERE layer = 'warehouse'")"
        last_run="$(pgq "SELECT COALESCE(max(id), 0) FROM meta.derivation_runs")"
        probe_keycrm_dir fixture --id "$FIX_ID" --version "$k" --out /keycrm ${PRODUCT_ID:+--product-id "$PRODUCT_ID"} \
            > "$EV/fixture_v$k.json" 2>>"$LOG_DIR/probe.err"
        status="$(json_field "$EV/fixture_v$k.json" status_id)"
        say "F2: version $k (status $status) served; waiting for it to land"
        if poll_pg "SELECT 1 FROM bronze.orders WHERE id = $FIX_ID AND status_id = $status" "$LAND_TIMEOUT" 3 >/dev/null; then
            req_after="$(pgq "SELECT requested FROM meta.derivation_signal WHERE layer = 'warehouse'")"
            item="$(poll_pg "SELECT json_build_object(
                    'k', $k, 'status', $status,
                    'requested_before', $req_before, 'requested_after', $req_after,
                    'silver_status', (SELECT status_id FROM silver.orders WHERE id = $FIX_ID),
                    'latency_s', round(EXTRACT(EPOCH FROM r.started_at - b.mirrored_at)::numeric, 1),
                    'run', json_build_object('id', r.id, 'trigger', r.trigger,
                        'requested_seen', r.requested_seen,
                        'validation_passed', r.validation_passed, 'error', r.error))
                FROM meta.derivation_runs r, bronze.orders b
                WHERE b.id = $FIX_ID AND r.id > $last_run AND r.ended_at IS NOT NULL
                  AND r.requested_seen >= $req_after
                ORDER BY r.id LIMIT 1" "$DERIVE_TIMEOUT" 3 || true)"
            if [ -z "$item" ]; then
                item="{\"k\": $k, \"status\": $status, \"requested_before\": $req_before, \"requested_after\": $req_after, \"run\": null}"
            fi
            items="${items:+$items, }$item"
        else
            say "F2: version $k never landed"
        fi
        wprobe snapshot > "$EV/flip_snap_f2_$k.json" 2>/dev/null || true
    done
    printf '[%s]\n' "$items" > "$EV/p4b.json"

    # ─── F3: the reclassify door under postgres (sets up P8's branch) ─────────
    say "F3: reclassify in Postgres alone"
    wprobe api POST /api/traffic/reclassify > "$EV/f3_reclassify.json" 2>&1 || say "F3: reclassify answered non-2xx"

    # ─── F4: two mutations, each alone before the run that must see it ──────
    #
    # One at a time, so each run sees exactly its own: a Silver row missing
    # when the integrity twins look, and Postgres Gold one hryvnia off its own
    # Silver when dq_mirror_landing looks. Both at once made ClickHouse's
    # comparison find the deleted row's day in both of Gold's grains, and
    # `compare_gold_cells` raised sorting a roll-up key (source_id NULL)
    # beside a fine one — a latent defect, reported, not this rehearsal's.
    say "F4: a Silver row deleted, integrity; then a Gold fine row bumped, mirror-landing"
    poll_pg "SELECT 1 FROM meta.derivation_signal WHERE layer = 'warehouse' AND requested <= built" \
        "$DERIVE_TIMEOUT" 3 >/dev/null || say "F4: a derivation is still owed"
    GRACE=$(( (FLOOR_S + 59) / 60 + 15 ))
    T_DELETE="$(pgq "SELECT now()")"
    DELETED="$(pgfile p4a_delete_silver_row.sql -v "grace=$GRACE" | head -n 1 || true)"
    say "F4: deleted silver.orders ${DELETED:-nothing}"
    int_before="$(wprobe dq-last integrity 2>/dev/null | grep -o '"run_id": [0-9]*' | grep -o '[0-9]*' || true)"
    wprobe trigger-wait dq_integrity_check --timeout "$STEP_TIMEOUT" > "$EV/f4_integrity_job.json" 2>&1 || true
    INT_RUN="$(wprobe wait-dq integrity --after "${int_before:-0}" --timeout 60 2>/dev/null \
        | tee "$EV/f4_integrity_live.json" | grep -o '"run_id": [0-9]*' | grep -o '[0-9]*' || true)"
    BETWEEN="$(pgq "SELECT count(*) FROM meta.derivation_runs WHERE started_at >= '$T_DELETE'::timestamptz")"
    wprobe api POST /api/warehouse/refresh > "$EV/f4_repair_silver.json" 2>&1 || say "F4: the repair refresh answered non-2xx"
    RESTORED=false
    if [ -n "$DELETED" ] && [ -n "$(pgq "SELECT 1 FROM silver.orders WHERE id = $DELETED")" ]; then
        RESTORED=true
    fi
    T_BUMP="$(pgq "SELECT now()")"
    BUMPED="$(pgfile p5_bump_gold_fine_row.sql | head -n 1 || true)"
    say "F4: bumped gold ${BUMPED:-nothing}"
    ml_before="$(wprobe dq-last mirror_landing 2>/dev/null | grep -o '"run_id": [0-9]*' | grep -o '[0-9]*' || true)"
    wprobe trigger-wait dq_mirror_landing --timeout "$STEP_TIMEOUT" > "$EV/f4_mirror_job.json" 2>&1 || true
    ML_RUN="$(wprobe wait-dq mirror_landing --after "${ml_before:-0}" --timeout 60 2>/dev/null \
        | tee "$EV/f4_mirror_live.json" | grep -o '"run_id": [0-9]*' | grep -o '[0-9]*' || true)"
    # The job's own word on which checks it asked (P5): absent findings
    # cannot say a comparison stood down.
    docker logs "$REH_WEB" 2>&1 | grep -F 'Mirror reconciliation complete' > "$EV/mirror_complete.log" || true
    AFTER_BUMP="$(pgq "SELECT count(*) FROM meta.derivation_runs WHERE started_at >= '$T_BUMP'::timestamptz")"
    wprobe snapshot > "$EV/f4_snapshot.json" 2>/dev/null || true
    cp "$EV/f4_snapshot.json" "$EV/flip_snap_f4.json" 2>/dev/null || true
    printf '{"integrity": %s, "mirror_landing": %s}\n' "${INT_RUN:-null}" "${ML_RUN:-null}" > "$EV/f4_runs.json"
    sleep 13   # the refresh endpoint's rate limit is 5 a minute
    wprobe api POST /api/warehouse/refresh > "$EV/f4_repair_gold.json" 2>&1 || say "F4: the repair refresh answered non-2xx"
    printf '{"deleted_id": %s, "derivations_between": %s, "restored": %s, "bumped": "%s", "derivations_after_bump": %s}\n' \
        "${DELETED:-null}" "${BETWEEN:-null}" "$RESTORED" "${BUMPED:-}" "${AFTER_BUMP:-null}" > "$EV/p4a.json"

    # ─── F5: an unknown sales_type (P4c) ─────────────────────────────────────
    say "F5: one manager's orders given a sales_type no code knows"
    MID="$(pgq "SELECT manager_id FROM silver.orders
                 WHERE manager_id IS NOT NULL AND manager_id <> 15
                   AND NOT is_return AND is_active_source AND grand_total > 0
                 GROUP BY manager_id ORDER BY count(*), manager_id LIMIT 1")"
    if [ -n "$MID" ]; then
        alert_before="$(log_count <(docker logs "$REH_WEB" 2>&1) 'Postgres Gold: revenue in an unknown sales_type')"
        pgfile p4c_unknown_sales_type.sql -v "mid=$MID" >/dev/null
        sleep 13   # the refresh endpoint's rate limit is 5 a minute
        wprobe api POST /api/warehouse/refresh > "$EV/f5_refresh_on.json" 2>&1 || true
        WITH="$(pgq "SELECT json_build_object('run_id', id,
                    'partition_exhaustive', (validation->>'partition_exhaustive')::boolean,
                    'unknown_sales_types', validation->'unknown_sales_types')
                FROM meta.derivation_runs ORDER BY id DESC LIMIT 1")"
        sleep 3
        alert_after="$(log_count <(docker logs "$REH_WEB" 2>&1) 'Postgres Gold: revenue in an unknown sales_type')"
        pgfile p4c_drop.sql >/dev/null
        sleep 13
        wprobe api POST /api/warehouse/refresh > "$EV/f5_refresh_off.json" 2>&1 || true
        AFTER="$(pgq "SELECT json_build_object('run_id', id,
                    'partition_exhaustive', (validation->>'partition_exhaustive')::boolean)
                FROM meta.derivation_runs ORDER BY id DESC LIMIT 1")"
        printf '{"manager_id": %s, "with_trigger": %s, "log_alert": %s, "after_drop": %s}\n' \
            "$MID" "${WITH:-null}" "$((alert_after - alert_before))" "${AFTER:-null}" > "$EV/p4c.json"
    else
        echo '{"manager_id": null, "why": "no manager outside b2b holds an active order"}' > "$EV/p4c.json"
    fi
    wprobe snapshot > "$EV/flip_snap_f5.json" 2>/dev/null || true

    # ─── F5s: a graceful stop before any kill (P4a, P5; restart 1 of P6) ─────
    #
    # F4's two DQ runs are read here, before F6 kills anything. DuckDB 1.5.5
    # drops from a CREATE INDEX the rows a SIGKILL caught in the WAL, when the
    # first checkpoint after the restart is one it takes on its own (at close,
    # or past wal_autocheckpoint) and nothing touched the table first — and
    # `fetch_run_issues` reads through `idx_dqi_run`. Read after the kill,
    # P4a and P5 would judge what that defect left of their runs; read here,
    # read-only and before the kill, they judge the runs as the product wrote
    # them — a read-only open sees every row the WAL holds, so they need
    # nothing of this stop beyond its being before the kill. The stop is the
    # one a deploy makes (production's grace); a graceful one is a
    # checkpoint, which puts F4's runs out of the kill's reach, so they say
    # nothing about what it costs: D1 asks that of the rows written after
    # this stop (F5w, F6). The kill stays where P3 needs it, inside a live
    # derivation, and F7's read stays after it for P2 and P6.
    say "F5s: a graceful stop, F4's DQ runs and every index read, a start"
    stop_web "$REH_WEB" flip "$EV/f5s_stop_state.json"
    RUN_ARGS=()
    [ -n "$INT_RUN" ] && RUN_ARGS+=(--run "$INT_RUN")
    [ -n "$ML_RUN" ] && RUN_ARGS+=(--run "$ML_RUN")
    probe_offline duckdb-facts --db /app/data/analytics.duckdb --gate /app/data/alert-gate-web.json \
        ${RUN_ARGS[@]+"${RUN_ARGS[@]}"} > "$EV/d_pre.json" 2>>"$LOG_DIR/probe.err" || true
    start_stopped "$REH_WEB"
    if wait_health "$REH_WEB" "$HEALTH_TIMEOUT"; then
        sleep 5
        wprobe snapshot > "$EV/flip_snap_r1.json" 2>/dev/null || true
        save_log "$REH_WEB" flip
        restart_record "$EV/flip_snap_r1.json" "$EV/p6_restart1.json" "$EV/f5s_stop_state.json"
    fi

    # ─── F5w: rows written after that checkpoint, for D1 ─────────────────────
    #
    # A kill can cost only what reached DuckDB after the last checkpoint. One
    # integrity run is written here, into the DQ journal the defect bites —
    # nothing the product runs at start touches that table again — and F6
    # lands the fixture's fourth version, an order and its lines, whose table
    # carries four of the composite indexes — the order is in two of them,
    # having no buyer and no manager. F7 reads them after the kill with the
    # product's reader; the end deletes them through every index holding them.
    # The run must be above every run F5s's checkpoint holds: F4's two, and
    # the newest the read there found, whatever the live API answers now.
    say "F5w: one integrity run written after F5s's checkpoint (D1)"
    WIN_RUN="$(window_run "$(max_int "$(json_field "$EV/d_pre.json" max_dq_run_id)" "$INT_RUN" "$ML_RUN")")"
    [ -n "$WIN_RUN" ] || say "F5w: no integrity run was written after the checkpoint"

    # ─── F6: kill inside the derivation, start again (P3, restart 2 of P6) ───
    say "F6: holding Gold's TRUNCATE, then killing reh-web inside the derivation"
    poll_pg "SELECT 1 FROM meta.derivation_signal WHERE layer = 'warehouse' AND requested <= built" \
        "$DERIVE_TIMEOUT" 3 >/dev/null || true
    docker exec -d -e PGAPPNAME=reh-lock "$REH_PG" psql -U postgres -d ks -Xqc \
        "BEGIN; LOCK TABLE gold.daily_revenue IN ACCESS SHARE MODE; SELECT pg_sleep(900);"
    poll_pg "SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
              WHERE a.application_name = 'reh-lock' AND l.granted
                AND l.relation = 'gold.daily_revenue'::regclass" 30 1 >/dev/null || say "F6: the lock was not taken"
    probe_keycrm_dir fixture --id "$FIX_ID" --version 4 --out /keycrm ${PRODUCT_ID:+--product-id "$PRODUCT_ID"} \
        > "$EV/fixture_v4.json" 2>>"$LOG_DIR/probe.err"
    V4_STATUS="$(json_field "$EV/fixture_v4.json" status_id)"
    printf '{"run_id": %s, "order_id": %s, "order_status": %s}\n' \
        "${WIN_RUN:-null}" "$FIX_ID" "${V4_STATUS:-null}" > "$EV/d1_window.json"
    BLOCKED=""
    deadline=$((SECONDS + LAND_TIMEOUT + DERIVE_TIMEOUT))
    while [ "$SECONDS" -lt "$deadline" ]; do
        BLOCKED="$(pgq "SELECT pid FROM pg_stat_activity
                         WHERE usename = 'ks_app' AND wait_event_type = 'Lock'
                           AND query ILIKE 'TRUNCATE gold.daily_revenue%' LIMIT 1")"
        [ -n "$BLOCKED" ] && break
        sleep 0.5
    done
    # KILLED is what the container shows after the kill, not that it was
    # sent. It only spares the reads that mean nothing without a kill: P3
    # and D1 read the same state out of p3.json and decide for themselves.
    KILLED=0
    echo null > "$EV/f6_kill_state.json"
    if [ -n "$BLOCKED" ]; then
        AT_KILL="$(pgq "SELECT requested || '|' || built FROM meta.derivation_signal WHERE layer = 'warehouse'")"
        JK="$(pgq "SELECT COALESCE(max(id), 0) FROM meta.derivation_runs")"
        SILVER_AT_KILL="$(pgq "SELECT status_id FROM silver.orders WHERE id = $FIX_ID")"
        if kill_web "$REH_WEB" "$EV/f6_kill_state.json"; then
            KILLED=1
            say "F6: killed with requested|built $AT_KILL; Silver at status $SILVER_AT_KILL"
        else
            say "F6: reh-web was not left SIGKILLed: $(cat "$EV/f6_kill_state.json")"
        fi
    else
        say "F6: the blocked TRUNCATE was not seen; nothing was killed"
    fi
    pgq "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
          WHERE application_name = 'reh-lock' OR usename = 'ks_app'" >/dev/null
    save_log "$REH_WEB" flip
    if [ "$KILLED" = 1 ]; then
        AFTER_KILL="$(pgq "SELECT json_build_object('requested', requested, 'built', built,
                    'rows_above', (SELECT count(*) FROM meta.derivation_runs WHERE id > $JK))
                FROM meta.derivation_signal WHERE layer = 'warehouse'")"
    fi
    start_stopped "$REH_WEB"
    FIRST=""
    if wait_health "$REH_WEB" "$HEALTH_TIMEOUT"; then
        sleep 5
        wprobe snapshot > "$EV/flip_snap_r2.json" 2>/dev/null || true
        if [ "$KILLED" = 1 ]; then
            FIRST="$(poll_pg "SELECT json_build_object('id', id, 'trigger', trigger,
                        'requested_seen', requested_seen, 'validation_passed', validation_passed,
                        'error', error)
                    FROM meta.derivation_runs WHERE id > $JK AND ended_at IS NOT NULL
                    ORDER BY id LIMIT 1" $((FLOOR_S + 180)) 3 || true)"
        fi
        save_log "$REH_WEB" flip
        # P6's restart after the kill, written under KILLED alone — the
        # SIGKILL the container showed — and with that kill's state in it.
        # With no kill, `start_stopped` started nothing (or a container the
        # OOM killer took), and a record here once let P6 pass
        # "graceful,kill,graceful" on a kill that was never sent.
        if [ "$KILLED" = 1 ]; then
            restart_record "$EV/flip_snap_r2.json" "$EV/p6_restart2.json" "$EV/f6_kill_state.json"
        fi
    fi
    if [ -n "$BLOCKED" ]; then
        BUILT_AFTER="$(pgq "SELECT built FROM meta.derivation_signal WHERE layer = 'warehouse'")"
        silver_version=null
        [ "$SILVER_AT_KILL" = "$V4_STATUS" ] && silver_version=4
        p3_record "$EV/p3.json" "$EV/f6_kill_state.json" "${AT_KILL%%|*}" "${AT_KILL##*|}" \
            "$silver_version" "${AFTER_KILL:-}" "${FIRST:-}" "${BUILT_AFTER:-}"
    else
        echo '{"seen_blocked": false, "stop": null, "why": "no TRUNCATE gold.daily_revenue waited on the lock in time"}' > "$EV/p3.json"
    fi

    # ─── F7: stop, read DuckDB, start again (P2, D1, restart 3 of P6) ────────
    #
    # The read after the kill: P2's frozen state, P6's gate file, and D1's —
    # the window's run through the product's reader, the window's rows by a
    # scan with every index on their tables and which of them hold the rows,
    # and every index swept again.
    # D1 needs this stop to be a graceful one: its close is the checkpoint
    # that writes what the kill cost, and a read after any other stop sees
    # the rows the WAL still holds.
    say "F7: a graceful stop, the DuckDB copy read after the kill, and a third start"
    F7_STOP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '{"runs_ok": %s}\n' "$(pgq "SELECT count(*) FROM meta.derivation_runs WHERE id > $N0 AND error IS NULL")" > "$EV/p2_pg.json"
    stop_web "$REH_WEB" flip "$EV/f7_stop_state.json"
    probe_offline duckdb-facts --db /app/data/analytics.duckdb --gate /app/data/alert-gate-web.json \
        --window-order "$FIX_ID" ${WIN_RUN:+--window-run "$WIN_RUN"} \
        > "$EV/d1.json" 2>>"$LOG_DIR/probe.err" || true
    printf '{"refreshing": %s, "sync_ticks": %s}\n' \
        "$(log_count "$LOG_DIR/flip.log" 'Warehouse dirty — refreshing')" \
        "$(docker logs --since "$F1_START" --until "$F7_STOP" "$REH_KEYCRM" 2>&1 | grep -cF 'REH-KEYCRM GET /v1/order?' || true)" \
        > "$EV/p2_log.json"
    start_stopped "$REH_WEB"
    if wait_health "$REH_WEB" "$HEALTH_TIMEOUT"; then
        sleep 30
        wprobe snapshot > "$EV/flip_snap_r3.json" 2>/dev/null || true
        save_log "$REH_WEB" flip
        restart_record "$EV/flip_snap_r3.json" "$EV/p6_restart3.json" "$EV/f7_stop_state.json"
    fi
    BASE_URL="$(wprobe keycrm-url 2>/dev/null || true)"
    # The stop the way back starts from: production's is the deploy that
    # unsets the variable, and one that outruns its grace is a SIGKILL P8
    # must say it started from.
    stop_web "$REH_WEB" flip "$EV/b_pre_stop_state.json"
    docker rm -f -v "$REH_WEB" >/dev/null 2>&1 || true

    # ─── B: the way back (P8) ─────────────────────────────────────────────────
    say "B: KS_WRITE_WAREHOUSE unset — the way back"
    start_web "$REH_WEB" -e KS_READ_FALLBACK=off
    OOM=false
    START_SNAP=null
    FINAL_SNAP=null
    if wait_health "$REH_WEB" "$HEALTH_TIMEOUT"; then
        wprobe snapshot > "$EV/b_start.json" 2>/dev/null || true
        START_SNAP="$(cat "$EV/b_start.json" 2>/dev/null || echo null)"
        deadline=$((SECONDS + RELEASE_TIMEOUT))
        while [ "$SECONDS" -lt "$deadline" ]; do
            if ! running "$REH_WEB"; then
                oom_killed "$REH_WEB" && OOM=true
                break
            fi
            docker logs "$REH_WEB" 2>&1 | grep -qF 'warehouse writer is duckdb again' && break
            sleep 10
        done
        if running "$REH_WEB"; then
            sleep 5
            wprobe snapshot > "$EV/b_final.json" 2>/dev/null || true
            FINAL_SNAP="$(cat "$EV/b_final.json" 2>/dev/null || echo null)"
        fi
    else
        oom_killed "$REH_WEB" && OOM=true
    fi
    stop_web "$REH_WEB" back "$EV/b_stop_state.json"
    probe_offline duckdb-facts --db /app/data/analytics.duckdb > "$EV/dz.json" 2>>"$LOG_DIR/probe.err" || true
    # D1's last question, and the copy's last use: the window's rows deleted
    # through every index on their tables that holds them (none holds a row
    # with a NULL key). Composite indexes serve no read
    # in DuckDB 1.5.5 and answer only a write — a DELETE takes each row out
    # of each index at commit, and an entry the kill cost is DuckDB's FATAL
    # "Failed to delete all rows from index". The DELETE finds its rows by
    # the id's text, which no index serves: through an index that lost them
    # it would find none and report success. It changes the copy, so it runs
    # on one about to be removed, and --keep keeps the copy as it is instead.
    if [ "$KEEP" = 1 ]; then
        echo '{"skipped": "--keep leaves the copy as it is"}' > "$EV/d1_delete.json"
    elif [ "$KILLED" = 1 ]; then
        probe_offline window-delete --db /app/data/analytics.duckdb --window-order "$FIX_ID" \
            ${WIN_RUN:+--window-run "$WIN_RUN"} > "$EV/d1_delete.json" 2>>"$LOG_DIR/probe.err" || true
    fi
    printf '{"oom": %s, "start": %s, "final": %s, "log": {"way_back": %s, "emptied": %s, "full_tick": %s, "released": %s}}\n' \
        "$OOM" "$START_SNAP" "$FINAL_SNAP" \
        "$(log_count "$LOG_DIR/back.log" 'warehouse writer back to duckdb from postgres')" \
        "$(log_count "$LOG_DIR/back.log" 'silver_order_utm is emptied')" \
        "$(log_count "$LOG_DIR/back.log" 'Warehouse dirty — refreshing (changed_ids=full)')" \
        "$(log_count "$LOG_DIR/back.log" 'warehouse writer is duckdb again')" \
        > "$EV/p8.json"
else
    say "F1: no flip — P2–P6 and P8 cannot be rehearsed; P7 and K0 still answer"
    BASE_URL="$(wprobe keycrm-url 2>/dev/null || true)"
    stop_web "$REH_WEB" flip "$EV/f1_stop_state.json"
fi

# ─── Z: K0, Z0, the table ─────────────────────────────────────────────────────
say "Z: the verdicts"
STUB_REQUESTS="$(docker logs "$REH_KEYCRM" 2>&1 | grep -c '^REH-KEYCRM ' || true)"
KEYCRM_APP=0
for f in "$LOG_DIR"/*.log; do
    n="$(grep -c 'keycrm\.app' "$f" 2>/dev/null || true)"
    KEYCRM_APP=$((KEYCRM_APP + ${n:-0}))
done
printf '{"internal": %s, "base_url": "%s", "stub_requests": %s, "keycrm_app_lines": %s}\n' \
    "$(docker network inspect -f '{{.Internal}}' "$REH_NET" 2>/dev/null || echo null)" \
    "${BASE_URL:-}" "${STUB_REQUESTS:-0}" "$KEYCRM_APP" > "$EV/k0.json"
others > "$REH_ROOT/others.after"
MEM_MIN=null
[ -f "$REH_ROOT/mem.min" ] && MEM_MIN="$(cat "$REH_ROOT/mem.min")"
printf '{"before": %s, "after": %s, "min_mem_available_mib": %s}\n' \
    "$(as_json_list < "$REH_ROOT/others.before")" "$(as_json_list < "$REH_ROOT/others.after")" \
    "$MEM_MIN" > "$EV/z0.json"

ROWS="$(docker run --rm --name "$REH_PROBE" --network none --pull never --oom-score-adj 1000 \
    --memory 256m --memory-swap 256m --log-opt max-size=10m \
    -v "$EV:/ev:ro" \
    -v "$HELPER_DIR:/reh:ro" \
    --entrypoint python "$REH_IMAGE" /reh/probe.py judge --evidence /ev --floor "$FLOOR_S" 2>>"$LOG_DIR/probe.err" || true)"
if [ -z "$ROWS" ]; then
    ROWS="ALL|UNKNOWN|the judge did not run (see $LOG_DIR/probe.err)"
fi

render() {
    echo "$HEADER1"
    echo "$HEADER2"
    echo
    printf '%s\n' "$ROWS" | awk '
        { lines[NR] = $0; c = $0; sub(/\|.*/, "", c); if (length(c) > w) w = length(c) }
        END {
            fmt = "%-" w "s  %-7s  %s\n"
            printf fmt, "CHECK", "VERDICT", "DETAIL"
            for (i = 1; i <= NR; i++) {
                c = lines[i]; sub(/\|.*/, "", c)
                rest = substr(lines[i], length(c) + 2)
                v = rest; sub(/\|.*/, "", v)
                printf fmt, c, v, substr(rest, length(v) + 2)
            }
        }'
    echo
    echo "$PASSES PASS, $FAILS FAIL, $UNKNOWNS UNKNOWN"
}
FAILS="$(printf '%s\n' "$ROWS" | awk -F'|' '$2 == "FAIL" { n++ } END { print n + 0 }')"
UNKNOWNS="$(printf '%s\n' "$ROWS" | awk -F'|' '$2 == "UNKNOWN" { n++ } END { print n + 0 }')"
PASSES="$(printf '%s\n' "$ROWS" | awk -F'|' '$2 == "PASS" { n++ } END { print n + 0 }')"
render
render > "$REPORT"
chmod 600 "$REPORT"
say "the table is also in $REPORT"

if [ "$FAILS" -gt 0 ]; then
    exit 1
fi
if [ "$UNKNOWNS" -gt 0 ]; then
    exit 2
fi
exit 0
