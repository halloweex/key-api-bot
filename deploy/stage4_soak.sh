#!/usr/bin/env bash
# Stage 4 soak report: every check in the soak checklist, one command, one table.
#
# The soak checks are the only detectors for a lost derivation mark (D4, D5),
# stale Silver rows (D7), the chain-8 stand-down (E1) and a halted order intake
# (S0), and they have to run every day for weeks. Run by hand from a checklist
# they drift: a query pasted from yesterday's terminal, a precondition skipped
# on a busy morning. So each check is a file under deploy/stage4_soak/ that
# returns its own verdict, and this script only runs them and adds them up.
#
# THREE VERDICTS, AND THE THIRD IS THE POINT
# PASS and FAIL are the obvious two. UNKNOWN is a check that could not see:
# the evidence it reads is a copy that has stopped moving, the flag it depends
# on could not be read, a heavy job was holding the lock at the moment it
# looked, or its query did not run at all. Several checks read the DQ journal,
# which reaches Postgres only through the hourly `replicate_operational`, and a
# replication that fails freezes that journal at its last clean run. Reading a
# frozen journal as PASS is exactly the silence this report exists to break.
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
#   deploy/stage4_soak.sh
#   SOAK_INVENTORY_FLIP_AT='2026-10-01 10:00+03' deploy/stage4_soak.sh
# The second form is for chain 1's flip day; see 15_i1_inventory_copy_stood_down.sql.
#
# Exit: 0 when every check passes, 1 on any FAIL, 2 when nothing failed but at
# least one check is UNKNOWN.
set -Eeuo pipefail

cd "$(dirname "$0")/.."

PG_CONTAINER="ks-postgres"
WEB_CONTAINER="keycrm-web"
SQL_DIR="deploy/stage4_soak"
# S0's log window. The orders job ticks every 60 s, so a live failure writes a
# line a minute and 30 min cannot miss it, while a failure fixed an hour ago no
# longer reads as FAIL. An .env edit takes effect through a recreate, which
# starts a new log, so the checklist's +2 and +15 min runs see only the new
# container's lines.
LOG_WINDOW="30m"
# What the orders job says when it dies, and what a KS_WRITE_* typo says.
LOG_PATTERN='Job incremental_sync failed|is not understood; expected'

# Every verdict, one "check|verdict|detail" per line.
ROWS=""

add_row() {
    # A detail is one line in the table whatever the query or the log put in it.
    ROWS="${ROWS}$1|$2|$(printf '%s' "$3" | tr '\r\n' '  ')"$'\n'
}

# ── the flags the SQL cannot read ─────────────────────────────────────────────
# A check that depends on a flag gets it as a psql variable: 1, 0, invalid or
# unknown. Asked by exact name with `printenv NAME`, never by listing the
# environment — the container's environment carries every secret on the host.
# Trimmed and lower-cased the way the application reads it.
flag_state() {
    local name="$1" on="$2" off="$3" value
    if [ "$(docker inspect -f '{{.State.Running}}' "$WEB_CONTAINER" 2>/dev/null || true)" != "true" ]; then
        echo unknown
        return 0
    fi
    # printenv exits 1 for an unset name, which is a value here, not an error.
    value="$(docker exec "$WEB_CONTAINER" printenv "$name" 2>/dev/null || true)"
    value="$(printf '%s' "$value" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' \
        | tr '[:upper:]' '[:lower:]')"
    if [ -z "$value" ] || [ "$value" = "$off" ]; then
        echo 0
    elif [ "$value" = "$on" ]; then
        echo 1
    else
        echo invalid
    fi
}

# ── the latch, which outranks the flag ────────────────────────────────────────
# Since DN-06 a chain that has written Postgres keeps writing Postgres whatever
# KS_WRITE_* says (owner decision OD-19 (a)), so a report that read the flag
# alone would call the chain "not applicable" in the one state the latch exists
# for: latched, and the variable put back by an operator expecting a rollback.
# Every I-check would then be a PASS about a chain nobody is watching, I1 —
# which asks whether the hourly copy is overwriting the six inventory tables —
# included.
#
# The marker file is what routes the writes, so it is what is read, by its
# path rather than through /api/health: no HTTP, no session, and it answers
# while the dashboard does not. `tests/unit/test_stage4_soak_script.py` pins
# the path against `core.chain_latch.MARKER_DIR` so the two cannot drift.
MARKER_DIR_IN_CONTAINER="/app/data/write-chain-owners"

latch_state() {
    local chain="$1"
    if [ "$(docker inspect -f '{{.State.Running}}' "$WEB_CONTAINER" 2>/dev/null || true)" != "true" ]; then
        echo unknown
        return 0
    fi
    if docker exec "$WEB_CONTAINER" test -f "$MARKER_DIR_IN_CONTAINER/$chain" 2>/dev/null; then
        echo 1
    else
        echo 0
    fi
}

INVENTORY_ON="$(flag_state KS_WRITE_INVENTORY postgres duckdb)"
INVENTORY_LATCHED="$(latch_state pg_inventory_write)"
DQ_PG_WAREHOUSE_ON="$(flag_state KS_DQ_PG_WAREHOUSE on off)"
INVENTORY_FLIP_AT="${SOAK_INVENTORY_FLIP_AT:-}"

# The latch wins, exactly as `writes_postgres()` resolves it — including over
# `invalid`, because a latched chain with a misspelt variable goes on writing
# Postgres and its stand-down still has to hold. The typo does not disappear
# from the report: S0 greps the log for "is not understood", where the shipper
# writes it every hour.
INVENTORY_NOTE=""
if [ "$INVENTORY_LATCHED" = "1" ] && [ "$INVENTORY_ON" != "1" ]; then
    INVENTORY_NOTE=" (latched: chain 1 owns its tables in Postgres, and KS_WRITE_INVENTORY says otherwise — only scripts/chain_copy_back.py undoes that)"
    INVENTORY_ON=1
fi

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
        add_row "$name" FAIL "$hits line(s) in $LOG_WINDOW; restore the last .env value first, then investigate. Last: $last"
    else
        add_row "$name" PASS "no failed incremental_sync and no unreadable KS_WRITE_* in $LOG_WINDOW"
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
            -v inventory_on="$INVENTORY_ON" \
            -v inventory_flip_at="$INVENTORY_FLIP_AT" \
            -v dq_pg_warehouse_on="$DQ_PG_WAREHOUSE_ON" \
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
    add_row "stage4_soak" UNKNOWN "no checks found in $SQL_DIR"
fi

# ── the table ─────────────────────────────────────────────────────────────────
echo "Stage 4 soak report · $(hostname 2>/dev/null || echo '?') · $(date -u '+%F %H:%M UTC')"
echo "flags as the checks see them: inventory_on=$INVENTORY_ON (KS_WRITE_INVENTORY)${INVENTORY_NOTE}, dq_pg_warehouse_on=$DQ_PG_WAREHOUSE_ON (KS_DQ_PG_WAREHOUSE)${INVENTORY_FLIP_AT:+, inventory flip at $INVENTORY_FLIP_AT}"
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
