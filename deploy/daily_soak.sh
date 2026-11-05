#!/usr/bin/env bash
# The daily soak report: every check, one command, one table, Postgres only.
#
# The soak checks are the only detectors for a lost derivation mark (D4, D5),
# stale Silver rows (D7) and a halted order intake (S0), the one daily summary
# of every write chain's health that does not depend on the canary (O2–O5,
# B2–B5, M2, I2–I5, E2), and the one daily verdict on the DuckDB file's hash
# (P1, until the file is deleted). Run by hand from a checklist they drift: a
# query pasted from yesterday's terminal, a precondition skipped on a busy
# morning. So each check is a file under deploy/daily_soak/ that returns its
# own verdict, and this script only runs them and adds them up.
#
# It was deploy/stage4_soak.sh until step 14's S14-20 (owner decision 7 of the
# step-14 plan). What went, and when: F1 and P2–P4 in S14-02 (the clocks the
# owner closed on 2026-10-20); H2 in S14-10; E1, I1, B1, H1, M1 and O1 (the
# hourly copies and mirrors out of DuckDB stood down) in S14-13; the twins'
# pairing in S14-14. And in S14-20 every gate the checks had on a KS_WRITE_*
# flag, a chain's latch marker, KS_DUCKDB and the flip-day times: every chain
# writes Postgres since S14-00 whatever its flag or its marker says, so a check
# judges its tables unconditionally.
#
# THREE VERDICTS, AND THE THIRD IS THE POINT
# PASS and FAIL are the obvious two. UNKNOWN is a check that could not see:
# the evidence it reads is a record that has stopped moving, a heavy job was
# holding the lock at the moment it looked, or its query did not run at all.
# Reading evidence nobody vouches for as PASS is exactly the silence this
# report exists to break.
#
# READ-ONLY, AND HOST-SIDE
# Nothing here writes anywhere: no file, no temp file, no alert, no row. Every
# psql session starts with `default_transaction_read_only=on`; the checks run
# as `ks_readonly` except the two sequence reads, which name `ks_app` in their
# first line because only a sequence's owner (or a superuser) can read it,
# and the owner is the narrower of the two (see 14_e2_expenses_allocator.sql).
# It lives here and in no image: it inspects containers from outside them.
#
# Usage, on the host:
#   deploy/daily_soak.sh
#   SOAK_BUYERS_OVERRIDE_FLOOR=<n> deploy/daily_soak.sh
# The second form also holds B3 to at least <n> human overrides of a gender
# verdict — the count a previous report printed (25_b3_gender_coverage.sql).
#
# Exit: 0 when every check passes, 1 on any FAIL, 2 when nothing failed but at
# least one check is UNKNOWN.
set -Eeuo pipefail

cd "$(dirname "$0")/.."

PG_CONTAINER="ks-postgres"
WEB_CONTAINER="keycrm-web"
SQL_DIR="deploy/daily_soak"
# S0's log window. The orders job ticks every 60 s, so a live failure writes a
# line a minute and 30 min cannot miss it, while a failure fixed an hour ago no
# longer reads as FAIL. A recreate starts a new log, so a run minutes after a
# deploy sees only the new container's lines.
LOG_WINDOW="30m"
# What the orders job says when it dies. (It also matched a KS_WRITE_* nobody
# could read, "is not understood; expected", until step 14's S14-11 stopped
# reading the flags.)
LOG_PATTERN='Job incremental_sync failed'
# B3's optional floor of human overrides (usage above); empty is no floor.
BUYERS_OVERRIDE_FLOOR="${SOAK_BUYERS_OVERRIDE_FLOOR:-}"

# Every verdict, one "check|verdict|detail" per line.
ROWS=""

add_row() {
    # A detail is one line in the table whatever the query or the log put in it.
    ROWS="${ROWS}$1|$2|$(printf '%s' "$3" | tr '\r\n' '  ')"$'\n'
}

# ── the DuckDB file (P1, OD-17 (a)) ───────────────────────────────────────────
# Judged whatever KS_DUCKDB says: since step 14's S14-00 web runs `off` on any
# value, and nothing in an image built from main can open the file at all
# (S14-18, S14-19). P1 stays until the file is deleted (the final stage's Z2).
#
# The file's hash, as the hourly host cron recorded it
# (deploy/duckdb_silence_check.sh). `--status` reads the record and nothing
# else: it hashes nothing — a multi-GB file is the cron's to hash, not this
# report's — and writes nothing, which this report promises above. The record
# reaches the SQL as variables so P1 judges it on the same clock as
# everything else.
SILENCE_CHECK="deploy/duckdb_silence_check.sh"
FILE_LAST=error FILE_SINCE="" FILE_SINCE_REASON="" FILE_CHECKED_AT="" FILE_MISSING_AT=""
silence_record() {
    local out kv
    out="$("$SILENCE_CHECK" --status 2>&1 || true)"
    case "$out" in
        NORECORD*) FILE_LAST=none ;;
        STATUS\ *)
            for kv in $out; do
                case "$kv" in
                    last=*) FILE_LAST="${kv#last=}" ;;
                    since=*) FILE_SINCE="${kv#since=}" ;;
                    since_reason=*) FILE_SINCE_REASON="${kv#since_reason=}" ;;
                    checked_at=*) FILE_CHECKED_AT="${kv#checked_at=}" ;;
                    missing_at=none) ;;
                    missing_at=*) FILE_MISSING_AT="${kv#missing_at=}" ;;
                esac
            done ;;
        *) FILE_LAST=error ;;
    esac
}
silence_record

# ── S0, the log half ──────────────────────────────────────────────────────────
# A failing orders job writes a line minutes before any watermark ages past a
# threshold, so this is the fastest signal that intake has stopped.
log_check() {
    local name="S0 order intake (log)" running lines hits last
    if ! running="$(docker inspect -f '{{.State.Running}}' "$WEB_CONTAINER" 2>/dev/null)"; then
        add_row "$name" UNKNOWN "no container named $WEB_CONTAINER on this host"
        return 0
    fi
    if [ "$running" != "true" ]; then
        add_row "$name" FAIL "$WEB_CONTAINER is not running: nothing is taking orders in"
        return 0
    fi
    # Read first and grep second: in one pipeline a failed `docker logs` would
    # be counted as zero matching lines, which is a PASS.
    if ! lines="$(docker logs "$WEB_CONTAINER" --since "$LOG_WINDOW" 2>&1)"; then
        add_row "$name" UNKNOWN "docker logs failed: $(printf '%s' "$lines" | tr '\n' ' ' | cut -c1-200)"
        return 0
    fi
    hits="$(printf '%s\n' "$lines" | grep -cE "$LOG_PATTERN" || true)"
    if [ "$hits" -gt 0 ]; then
        last="$(printf '%s\n' "$lines" | grep -E "$LOG_PATTERN" | tail -n 1 | cut -c1-240)"
        add_row "$name" FAIL "$hits line(s) in $LOG_WINDOW; read the web log around them (an .env edited since the deploy first). Last: $last"
    else
        add_row "$name" PASS "no failed incremental_sync in $LOG_WINDOW"
    fi
}

# ── one SQL check ─────────────────────────────────────────────────────────────
run_check() {
    local file="$1" name user="ks_readonly" out rc rows
    name="$(basename "$file" .sql)"
    if grep -qx -- '-- soak:run-as ks_app' "$file"; then
        user="ks_app"
    fi
    if out="$(docker exec -i -e "PGOPTIONS=-c default_transaction_read_only=on" \
            "$PG_CONTAINER" psql -U "$user" -d ks -X -A -t -F '|' \
            -v ON_ERROR_STOP=1 \
            -v buyers_override_floor="$BUYERS_OVERRIDE_FLOOR" \
            -v duckdb_file_last="$FILE_LAST" \
            -v duckdb_file_since="$FILE_SINCE" \
            -v duckdb_file_since_reason="$FILE_SINCE_REASON" \
            -v duckdb_file_checked_at="$FILE_CHECKED_AT" \
            -v duckdb_file_missing_at="$FILE_MISSING_AT" \
            < "$file" 2>&1)"; then
        rc=0
    else
        rc=$?
    fi
    if [ "$rc" -ne 0 ]; then
        # A check that did not run has not passed. It has not failed either:
        # FAIL names a state of the system, and this is a state of the check.
        add_row "$name" UNKNOWN "the query did not run (psql exit $rc): $(printf '%s' "$out" | tr '\n' ' ' | cut -c1-300)"
        return 0
    fi
    # Only lines shaped like a verdict count; anything else psql printed is not one.
    rows="$(printf '%s\n' "$out" | awk -F'|' 'NF >= 3 && ($2 == "PASS" || $2 == "FAIL" || $2 == "UNKNOWN")')"
    if [ -z "$rows" ]; then
        add_row "$name" UNKNOWN "returned no verdict: $(printf '%s' "$out" | tr '\n' ' ' | cut -c1-300)"
        return 0
    fi
    ROWS="${ROWS}${rows}"$'\n'
}

log_check

found=0
for file in "$SQL_DIR"/*.sql; do
    [ -f "$file" ] || continue
    found=$((found + 1))
    run_check "$file"
done
if [ "$found" -eq 0 ]; then
    add_row "daily_soak" UNKNOWN "no checks found in $SQL_DIR"
fi

# ── the table ─────────────────────────────────────────────────────────────────
echo "Daily soak report · $(hostname 2>/dev/null || echo '?') · $(date -u '+%F %H:%M UTC')"
echo "DuckDB file record: $FILE_LAST${FILE_SINCE:+ since $FILE_SINCE}${FILE_CHECKED_AT:+, checked $FILE_CHECKED_AT}${FILE_MISSING_AT:+, last missing $FILE_MISSING_AT}${BUYERS_OVERRIDE_FLOOR:+; override floor $BUYERS_OVERRIDE_FLOOR}"
echo
printf '%s' "$ROWS" | awk '
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

fails="$(printf '%s' "$ROWS" | awk -F'|' '$2 == "FAIL" { n++ } END { print n + 0 }')"
unknowns="$(printf '%s' "$ROWS" | awk -F'|' '$2 == "UNKNOWN" { n++ } END { print n + 0 }')"
passes="$(printf '%s' "$ROWS" | awk -F'|' '$2 == "PASS" { n++ } END { print n + 0 }')"
echo
echo "$passes PASS, $fails FAIL, $unknowns UNKNOWN"

if [ "$fails" -gt 0 ]; then
    exit 1
fi
if [ "$unknowns" -gt 0 ]; then
    exit 2
fi
exit 0
