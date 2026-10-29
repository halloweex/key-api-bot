#!/usr/bin/env bash
# Has the DuckDB file changed? The week of silence's second detector (OD-17 (a)).
#
# Stage 5 waits for seven days in which nothing opened the DuckDB file. Web's
# side of that is the tripwire (`KS_DUCKDB=off`, core/duckdb_switch.py): it
# refuses and counts every open web attempts. It cannot see anything outside
# web — a one-off container with KS_DUCKDB=on, the weekly compaction, a script
# run by hand, a restore pointed at the wrong path — and the file is where all
# of those would show. So this hashes it, from the host, and compares the hash
# with the last one it recorded.
#
# It NEVER WRITES THE DUCKDB FILE, and it never opens it with DuckDB either:
# sha256 reads the bytes (O_RDONLY, no lock), so a check cannot itself be the
# change it reports. The `.wal` beside the file is hashed too when it exists —
# a write that has not been checkpointed lives there, not in the file. Hashing
# a file web holds open and is writing (KS_DUCKDB=on) can read a torn copy;
# that only ever reads as CHANGED, which under `on` the file is anyway.
#
# FOUR MODES, THREE OF WHICH WRITE NOTHING
#   (default)  hash, compare, and record: the cron run. The only mode that
#              writes, and only under $STATE_DIR.
#   --peek     hash and compare; write nothing. For a human.
#   --status   read the record; hash nothing, write nothing. What
#              deploy/stage4_soak.sh calls: hashing a multi-GB file is the
#              cron's job, not the daily report's.
#   --crontab  print the week's change to root's crontab, as a diff: the two
#              lines it retires (the Sunday compaction, the nightly DuckDB
#              snapshot), the one it adds (this check), and the way back.
#              Reads no file, hashes nothing, writes nothing, and installs
#              nothing — a crontab is changed by a person, from that print
#              (CLAUDE.md, "Starting the week of silence"). A `-`/`+` before
#              each line is what makes pasting the whole print into
#              `crontab -e` fail instead of installing a retired line.
#
# ONE LINE, AND THE EXIT CODE SAYS THE SAME
#   UNCHANGED file=… since=… age_s=… …    exit 0
#   CHANGED   …                           exit 1
#   BASELINE  (no record to compare with) exit 2
#   MISSING   (the file is gone)          exit 3
#   ERROR     …                           exit 4
# `since` is the first check that saw the content the file holds now, and
# `since_reason` says whether that check was the first ever (baseline) or saw a
# change. A change happened somewhere between `previous_checked_at` and
# `since`; the record is honest about that interval, never more precise. What
# it cannot see: a change and its exact reversal between two checks (A→B→A) —
# the hourly cadence below is what bounds that. A MISSING file never
# overwrites the recorded hash, so a file that comes back is judged against
# what it was; deleting it is a DROP (owner decision OD-11 (a)) and the soak
# fails on it whatever KS_DUCKDB says. And a MISSING is never forgotten when
# the file comes back: `missing_at`, the last check that found it gone, is
# carried by every later run and published by `--status`. A file moved away
# and back byte for byte reads UNCHANGED, rightly — the bytes are what they
# were — but for the hours it was gone nothing could vouch for it, so the soak
# fails the day it was missing in and the week of silence starts again after
# it. Without the key the episode lived only in `history`, which nothing
# reads (review of 02.10).
#
# STATE, AND ITS BOUND
# $STATE_DIR/state is key=value, written atomically (temp file and rename in
# the same directory), mode 600 under a 700 directory, and parsed — never
# sourced. $STATE_DIR/history gets one line per recording check and is cut to
# its last $HISTORY_MAX lines by the same write that grows it (CLAUDE.md:
# anything that accumulates declares its bound where it is created).
#
# Install at the start of the week of silence (not installed by anything here;
# `--crontab` prints this line with the two the week retires):
#   17 * * * *  /opt/key-api-bot/deploy/duckdb_silence_check.sh >/dev/null 2>&1
# Its output is in the record; deploy/stage4_soak.sh reads it (P1).
set -Eeuo pipefail
umask 077

HERE="$(cd "$(dirname "$0")" && pwd)"
DUCKDB_FILE="${DUCKDB_FILE:-$HERE/../data/analytics.duckdb}"
STATE_DIR="${DUCKDB_SILENCE_STATE_DIR:-/root/duckdb-silence}"
HISTORY_MAX="${DUCKDB_SILENCE_HISTORY_MAX:-500}"
STATE="$STATE_DIR/state"
HISTORY="$STATE_DIR/history"

MODE=record
case "${1:-}" in
    "") ;;
    --peek) MODE=peek ;;
    --status) MODE=status ;;
    --crontab) MODE=crontab ;;
    *) echo "ERROR usage: $(basename "$0") [--peek|--status|--crontab]"; exit 4 ;;
esac

# ── the crontab, printed ──────────────────────────────────────────────────────
# Before the lock, the record and the file: this mode touches none of them.
# Each line is the one its script's own header documents, with the path of
# the checkout this runs from — tests/unit/test_week_of_silence_host.py holds
# the two together.
if [ "$MODE" = crontab ]; then
    root="$(cd "$HERE/.." && pwd)"
    retire_compact="0 2 * * 0  $root/scripts/weekly_compact.sh"
    retire_snapshot="45 2 * * *  $root/deploy/daily_offsite.sh"
    add_silence="17 * * * *  $root/deploy/duckdb_silence_check.sh >/dev/null 2>&1"
    cat <<CRONTAB
# The week of silence: root's crontab, as a diff. Printed, never installed:
# change it by hand with \`crontab -e\`, one line at a time (CLAUDE.md,
# "Starting the week of silence"). An installed line may carry a redirect
# after its command; it is the command that names it.
#
# First keep a copy, and find the lines:
#   crontab -l > /root/crontab.bak-week-of-silence-\$(date -u +%Y%m%d-%H%M%S)
#   crontab -l | grep -nF -e scripts/weekly_compact.sh -e deploy/daily_offsite.sh -e deploy/duckdb_silence_check.sh
#
# STARTING THE WEEK — retire two (step 4, from the Ark run's SOURCE RELEASED),
# add one (step 7, after KS_DUCKDB=off and up -d):
- $retire_compact
- $retire_snapshot
+ $add_silence
# then run it once by hand, so its record starts now (BASELINE, exit 2):
#   $root/deploy/duckdb_silence_check.sh
#
# Unchanged: offsite_check.sh (09:00; reports the DuckDB copy frozen under
# off), pg_offsite.sh (07:40; carries .env since OD-S5-6), and every other line.
#
# THE WAY BACK (KS_DUCKDB=on again) — the reverse:
+ $retire_compact
+ $retire_snapshot
- $add_silence
# The record under $STATE_DIR stays as it is: history, not a lever.
CRONTAB
    exit 0
fi

# The clock, in whole seconds. Overridable for the tests only.
NOW="${DUCKDB_SILENCE_NOW:-$(date -u +%s)}"

iso() {
    date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ
}

die() { echo "ERROR $*"; exit 4; }

sha() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum -- "$1" | cut -d' ' -f1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 -- "$1" | cut -d' ' -f1
    else
        die "no sha256sum or shasum on this host"
    fi
}

size_of() { stat -c %s -- "$1" 2>/dev/null || stat -f %z -- "$1"; }

# Two recording runs at once (the cron and a hand) would each read the record
# and the later write would lose the other's change count. One at a time where
# the host has flock, which a Linux host does; the two read-only modes need no
# lock. The lock file is one empty file, so it declares its own bound.
if [ "$MODE" = record ] && command -v flock >/dev/null 2>&1; then
    mkdir -p -m 700 "$STATE_DIR"
    exec 9>"$STATE_DIR/.lock"
    flock -n 9 || die "another check holds $STATE_DIR/.lock"
fi

# ── the record ────────────────────────────────────────────────────────────────
# Only these keys, and only values made of these characters: a state file that
# was edited into something else is an ERROR, not an instruction.
KEYS="version file hash wal size since_epoch since since_reason checked_epoch checked_at previous_checked_at last changes missing_at"
for k in $KEYS; do printf -v "r_$k" '%s' ""; done
HAVE_RECORD=0
if [ -f "$STATE" ]; then
    [ -r "$STATE" ] || die "state $STATE is not readable"
    while IFS='=' read -r key value; do
        [ -n "$key" ] || continue
        case " $KEYS " in *" $key "*) ;; *) die "state $STATE has an unknown key '$key'" ;; esac
        case "$value" in *[!A-Za-z0-9:._/+-]*) die "state $STATE has a malformed $key" ;; esac
        printf -v "r_$key" '%s' "$value"
    done < "$STATE"
    [ -n "$r_hash" ] && [ -n "$r_since_epoch" ] || die "state $STATE has no hash or since"
    HAVE_RECORD=1
fi

emit() {
    # verdict, then key=value pairs, one line.
    local verdict="$1"; shift
    echo "$verdict $*"
}

if [ "$MODE" = status ]; then
    if [ "$HAVE_RECORD" -eq 0 ]; then
        emit NORECORD "state=$STATE"
        exit 2
    fi
    emit STATUS "last=${r_last:-UNKNOWN} since=$r_since since_reason=${r_since_reason:-baseline}" \
        "checked_at=$r_checked_at age_s=$((NOW - r_since_epoch))" \
        "checked_age_s=$((NOW - ${r_checked_epoch:-$r_since_epoch})) changes=${r_changes:-0}" \
        "missing_at=${r_missing_at:-none}"
    exit 0
fi

# ── the file ──────────────────────────────────────────────────────────────────
record() {
    # Write the record and one history line. Never anywhere but $STATE_DIR.
    [ "$MODE" = record ] || return 0
    mkdir -p -m 700 "$STATE_DIR"
    local tmp
    tmp="$(mktemp "$STATE_DIR/.state.XXXXXX")"
    {
        echo "version=1"
        echo "file=$DUCKDB_FILE"
        echo "hash=$n_hash"
        echo "wal=$n_wal"
        echo "size=$n_size"
        echo "since_epoch=$n_since_epoch"
        echo "since=$(iso "$n_since_epoch")"
        echo "since_reason=$n_since_reason"
        echo "checked_epoch=$NOW"
        echo "checked_at=$(iso "$NOW")"
        echo "previous_checked_at=$n_previous"
        echo "last=$1"
        echo "changes=$n_changes"
        echo "missing_at=$n_missing_at"
    } > "$tmp"
    chmod 600 "$tmp"
    mv -f "$tmp" "$STATE"
    printf '%s %s %s %s\n' "$(iso "$NOW")" "$1" "$n_hash" "$n_wal" >> "$HISTORY"
    if [ "$(wc -l < "$HISTORY")" -gt "$HISTORY_MAX" ]; then
        tmp="$(mktemp "$STATE_DIR/.history.XXXXXX")"
        tail -n "$HISTORY_MAX" "$HISTORY" > "$tmp"
        chmod 600 "$tmp"
        mv -f "$tmp" "$HISTORY"
    fi
}

n_previous="${r_checked_at:-}"
n_missing_at="${r_missing_at:-}"
if [ ! -e "$DUCKDB_FILE" ]; then
    if [ "$HAVE_RECORD" -eq 1 ]; then
        # The recorded content stays the reference: a file that comes back is
        # judged against what it was, not baselined as if nothing happened.
        n_hash="$r_hash"; n_wal="$r_wal"; n_size="$r_size"
        n_since_epoch="$r_since_epoch"; n_since_reason="${r_since_reason:-baseline}"
        n_changes="${r_changes:-0}"
        n_missing_at="$(iso "$NOW")"
        record MISSING
    fi
    emit MISSING "file=$DUCKDB_FILE last_hash=${r_hash:-none} last_checked_at=${r_checked_at:-never}"
    exit 3
fi
[ -f "$DUCKDB_FILE" ] && [ -r "$DUCKDB_FILE" ] || die "$DUCKDB_FILE is not a readable file"

n_hash="$(sha "$DUCKDB_FILE")"
n_wal=none
[ -f "$DUCKDB_FILE.wal" ] && n_wal="$(sha "$DUCKDB_FILE.wal")"
n_size="$(size_of "$DUCKDB_FILE")"

if [ "$HAVE_RECORD" -eq 0 ]; then
    n_since_epoch="$NOW"; n_since_reason=baseline; n_changes=0
    record BASELINE
    emit BASELINE "file=$DUCKDB_FILE since=$(iso "$NOW") sha256=$n_hash wal=$n_wal size=$n_size"
    exit 2
fi

if [ "$n_hash" = "$r_hash" ] && [ "$n_wal" = "$r_wal" ]; then
    n_since_epoch="$r_since_epoch"; n_since_reason="${r_since_reason:-baseline}"
    n_changes="${r_changes:-0}"
    record UNCHANGED
    emit UNCHANGED "file=$DUCKDB_FILE since=$(iso "$n_since_epoch") since_reason=$n_since_reason" \
        "age_s=$((NOW - n_since_epoch)) sha256=$n_hash wal=$n_wal" \
        "changes=$n_changes${n_missing_at:+ missing_at=$n_missing_at}"
    exit 0
fi

n_since_epoch="$NOW"; n_since_reason=change; n_changes=$(( ${r_changes:-0} + 1 ))
record CHANGED
emit CHANGED "file=$DUCKDB_FILE since=$(iso "$NOW") changed_after=${r_checked_at:-unknown}" \
    "sha256=$n_hash was=$r_hash wal=$n_wal was_wal=$r_wal size=$n_size was_size=${r_size:-?}" \
    "changes=$n_changes${n_missing_at:+ missing_at=$n_missing_at}"
exit 1
