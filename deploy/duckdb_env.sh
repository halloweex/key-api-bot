#!/usr/bin/env bash
# KS_DUCKDB as the host's cron scripts read it: out of .env, by exact name.
#
# Sourced by scripts/weekly_compact.sh, deploy/daily_offsite.sh and
# deploy/offsite_check.sh — the three host-cron scripts the week of silence
# changes (stage 5, PR-10). It defines and does not act, and it sets no shell
# options: errexit is the caller's.
#
# WHY .env AND NOT WEB
# The compaction and the nightly snapshot start their sidecars with
# `docker run --env-file .env`, so what refuses them under `off` is the file,
# not web's environment and not web's verdict (core/duckdb_off.py). A host
# script that asked web instead would be asking the wrong process — and the
# compaction asks while web is stopped. Read by exact name: nothing here
# prints, lists or exports the environment, whose other lines are secrets.
#
# TWO ANSWERS, BECAUSE TWO QUESTIONS
#   KS_DUCKDB_ENV           on | off | invalid — the value as
#                           `docker run --env-file` hands it to a sidecar and
#                           the switch then reads it: the last line naming the
#                           key, everything after the first `=`, quotes KEPT
#                           (docker does not strip them, so `"off"` is a value
#                           the switch does not understand, and it runs on).
#                           Unset or empty is `on`. deploy/stage4_soak.sh's P4
#                           reads the file the same way.
#   KS_DUCKDB_ENV_MEANT_OFF 1 when that line says off, quoted or not — what
#                           the person who wrote it meant. This is what a host
#                           script stands down on: compose strips the quotes,
#                           so web runs off, and a compaction that ran because
#                           the quotes made the value unreadable to its sidecar
#                           would rewrite the file in the week it must not
#                           change. Standing down is the harmless direction —
#                           a week's compaction or one night's DuckDB snapshot
#                           — and the line says the quotes must go
#                           (ks_duckdb_env_note).
#   KS_DUCKDB_ENV_VALUE     the value as written, carriage return removed, cut
#                           to printable characters and 40 of them — it is
#                           quoted in log lines, and `.env` is not a file to
#                           echo.
#
# A .env that cannot be read answers `on` with KS_DUCKDB_ENV_READ=0: the
# script then does what it did before stage 5, which is to run and fail where
# it always failed (a sidecar's `--env-file` refuses a missing file before
# anything starts).

ks_duckdb_env_read() {   # [env file, default .env]
    local file="${1:-.env}" line value bare
    KS_DUCKDB_ENV=on
    KS_DUCKDB_ENV_MEANT_OFF=0
    KS_DUCKDB_ENV_VALUE=""
    KS_DUCKDB_ENV_READ=0
    [ -f "$file" ] && [ -r "$file" ] || return 0
    KS_DUCKDB_ENV_READ=1
    line="$(grep -E '^KS_DUCKDB=' "$file" 2>/dev/null | tail -n 1 || true)"
    [ -n "$line" ] || return 0
    value="$(printf '%s' "${line#KS_DUCKDB=}" | tr -d '\r')"
    KS_DUCKDB_ENV_VALUE="$(printf '%s' "$value" | LC_ALL=C tr -cd '[:print:]' | cut -c1-40)"
    bare="$(printf '%s' "$value" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' \
        | tr '[:upper:]' '[:lower:]')"
    case "$bare" in
        off) KS_DUCKDB_ENV=off ;;
        ""|on) KS_DUCKDB_ENV=on ;;
        *) KS_DUCKDB_ENV=invalid ;;
    esac
    case "$bare" in
        \"*\"|\'*\') bare="${bare#?}"; bare="${bare%?}" ;;
    esac
    bare="$(printf '%s' "$bare" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    [ "$bare" = off ] && KS_DUCKDB_ENV_MEANT_OFF=1
    return 0
}

# 0 when a host script that would open, compact or ship the DuckDB file must
# stand down: .env says off, the way its writer meant it.
ks_duckdb_stands_down() {
    [ "${KS_DUCKDB_ENV_MEANT_OFF:-0}" = 1 ]
}

# The sentence every stand-down appends after its own: what a quoted value
# means, and what a web held back on `on` means. The second is
# core.duckdb_switch.HELD_BACK_NOTE's, in the host's words, because these
# scripts cannot import it: the process they stand down for reads `.env` and
# not web's verdict, so while web is held back the file it goes on writing is
# neither compacted nor shipped — a hold is to be cleared, not lived in.
ks_duckdb_env_note() {
    local note=""
    if [ "$KS_DUCKDB_ENV" != off ] && ks_duckdb_stands_down; then
        note="KS_DUCKDB is written with quotes ($KS_DUCKDB_ENV_VALUE): docker run --env-file keeps them, so a sidecar would read a value the switch does not understand and run on, and the soak's P4 fails on it; write KS_DUCKDB=off. "
    fi
    printf '%s' "${note}If web is held back (/api/health duckdb_switch.held_back), the week has not started: meet those keys or set KS_DUCKDB=on."
}
