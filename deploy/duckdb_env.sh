#!/usr/bin/env bash
# KS_DUCKDB as the host's cron scripts read it: out of .env, by exact name.
#
# Sourced by scripts/weekly_compact.sh, deploy/daily_offsite.sh and
# deploy/offsite_check.sh — the three host-cron scripts the week of silence
# changes (stage 5, PR-10). It defines and does not act, and it sets no shell
# options: errexit is the caller's. Bash 3.2 and up, because a laptop runs
# the tests.
#
# WHY .env AND NOT WEB
# The compaction and the nightly snapshot start their sidecars with
# `docker run --env-file .env`, so what refuses them under `off` is the file,
# not web's environment and not web's verdict (core/duckdb_off.py). A host
# script that asked web instead would be asking the wrong process — and the
# compaction asks while web is stopped. Read by exact name: nothing here
# prints, lists or exports the environment, whose other lines are secrets.
#
# TWO PARSERS READ THE FILE, AND THEY DISAGREE
# compose's `env_file:` is web's environment; `docker run --env-file` is a
# sidecar's. On a bare `KS_DUCKDB=off` they agree. On most other ways of
# writing it they do not, and each answer below is read the way its parser
# reads the file, then by the switch's own rule (`strip().lower()`: empty is
# `on`, anything but on or off is a value it does not understand and runs
# `on` over):
#
#   docker  the last line that is, after leading whitespace, `KS_DUCKDB=`:
#           everything after that `=`, kept as written — quotes, and an inline
#           comment, included. A line naming the key with whitespace before
#           its `=` (or before a colon), or with `export` in front, makes it
#           refuse the WHOLE file ("poorly formatted environment") and start
#           nothing. `KS_DUCKDB:off` it skips.
#   compose leading whitespace and an `export ` skipped; `=` or `:` as the
#           separator, with spaces either side; a quoted value is what is
#           inside the quotes, trailing comment and all outside them dropped;
#           an unquoted one is cut at the first " #" and trimmed. The last
#           line naming the key wins. NOT modelled, and read as written: `$`
#           interpolation, the escapes of a double-quoted value, and a quoted
#           value that runs past its line — none of which writes `off`.
#
# Every form tests/unit/test_week_of_silence_host.ENV_FORMS lists was MEASURED
# against both (compose v2.23.3, engine 24.0.7), not reasoned about; the
# first version of this reader was reasoned about, and four ways compose reads
# as off — an inline comment, leading whitespace, `export`, spaces around `=`
# — made it run the compaction in the week (review of PR-10).
#
# THE ANSWERS
#   KS_DUCKDB_ENV           on | off | invalid | refused — docker's reading:
#                           what a sidecar runs. `refused` starts no sidecar.
#                           Unset or empty is `on`.
#   KS_DUCKDB_ENV_WEB       on | off | invalid — compose's reading: what web
#                           runs (unless web holds it back, below).
#   KS_DUCKDB_ENV_MEANT_OFF 1 when either says off. This is what a host script
#                           stands down on. Compose's off is web's: the week
#                           has started and the file must not change, and a
#                           sidecar docker hands quotes, a comment or nothing
#                           would run `on` over it — or never start, after the
#                           script had stopped web and deleted the last
#                           backup. Docker's off is the sidecar's own refusal:
#                           the run cannot succeed, so it must not begin.
#                           Standing down is the harmless direction either way
#                           — a week's compaction or one night's DuckDB
#                           snapshot — and the stand-down's line says which
#                           line to write (ks_duckdb_env_note).
#   KS_DUCKDB_ENV_VALUE     docker's value as written, carriage return
#                           removed, cut to printable characters and 40 of
#                           them — it is quoted in log lines, and `.env` is
#                           not a file to echo.
#
# A .env that cannot be read answers `on` with KS_DUCKDB_ENV_READ=0: the
# script then does what it did before stage 5, which is to run and fail where
# it always failed (a sidecar's `--env-file` refuses a missing file before
# anything starts).

_ks_duckdb_ltrim() {   # <text> → the text without leading whitespace
    local s="$1"
    printf '%s' "${s#"${s%%[![:space:]]*}"}"
}

_ks_duckdb_rtrim() {   # <text> → the text without trailing whitespace
    local s="$1"
    printf '%s' "${s%"${s##*[![:space:]]}"}"
}

# The switch's rule over one parser's value: on | off | invalid.
_ks_duckdb_switch_reads() {   # <value>
    local bare
    bare="$(_ks_duckdb_rtrim "$(_ks_duckdb_ltrim "$1")" | tr '[:upper:]' '[:lower:]')"
    case "$bare" in
        off) printf off ;;
        ""|on) printf on ;;
        *) printf invalid ;;
    esac
}

# Compose's value from what follows the separator, already left-trimmed.
_ks_duckdb_compose_value() {   # <text>
    local s="$1" q c prev="" i inner
    case "$s" in
        \"*|\'*)
            q="${s:0:1}"
            for (( i = 1; i < ${#s}; i++ )); do
                c="${s:i:1}"
                if [ "$c" = "$q" ] && [ "$prev" != "\\" ]; then
                    inner="${s:1:i-1}"
                    while [ "${inner%"$q"}" != "$inner" ]; do inner="${inner%"$q"}"; done
                    while [ "${inner#"$q"}" != "$inner" ]; do inner="${inner#"$q"}"; done
                    printf '%s' "$inner"
                    return 0
                fi
                prev="$c"
            done
            # Unterminated on its line: compose would read on into the next
            # lines. Not modelled; read as written, which is not off.
            printf '%s' "$s" ;;
        *)
            _ks_duckdb_rtrim "${s%% #*}" ;;
    esac
}

ks_duckdb_env_read() {   # [env file, default .env]
    local file="${1:-.env}" lines line rest name docker_value="" docker_seen=0 \
        refused=0 web_value=""
    KS_DUCKDB_ENV=on
    KS_DUCKDB_ENV_WEB=on
    KS_DUCKDB_ENV_MEANT_OFF=0
    KS_DUCKDB_ENV_VALUE=""
    KS_DUCKDB_ENV_READ=0
    [ -f "$file" ] && [ -r "$file" ] || return 0
    KS_DUCKDB_ENV_READ=1
    # Every line either parser could take for the key, and no other line.
    lines="$(grep -E '^[[:space:]]*(export[[:space:]]+)?KS_DUCKDB[[:space:]]*[=:]' \
        "$file" 2>/dev/null | tr -d '\r' || true)"
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        rest="$(_ks_duckdb_ltrim "$line")"
        # docker run --env-file
        case "$rest" in
            KS_DUCKDB=*)
                docker_value="${rest#KS_DUCKDB=}"
                docker_seen=1 ;;
            *)
                name="${rest%%=*}"
                case "$name" in *[[:blank:]]*) refused=1 ;; esac ;;
        esac
        # compose
        if [[ $rest =~ ^export[[:space:]]+ ]]; then
            rest="${rest:${#BASH_REMATCH[0]}}"
        fi
        rest="$(_ks_duckdb_ltrim "${rest#KS_DUCKDB}")"
        rest="$(_ks_duckdb_ltrim "${rest#?}")"
        web_value="$(_ks_duckdb_compose_value "$rest")"
    done <<EOF
$lines
EOF
    if [ "$docker_seen" = 1 ]; then
        KS_DUCKDB_ENV_VALUE="$(printf '%s' "$docker_value" | LC_ALL=C tr -cd '[:print:]' | cut -c1-40)"
        KS_DUCKDB_ENV="$(_ks_duckdb_switch_reads "$docker_value")"
    fi
    if [ "$refused" = 1 ]; then
        KS_DUCKDB_ENV=refused
    fi
    KS_DUCKDB_ENV_WEB="$(_ks_duckdb_switch_reads "$web_value")"
    if [ "$KS_DUCKDB_ENV" = off ] || [ "$KS_DUCKDB_ENV_WEB" = off ]; then
        KS_DUCKDB_ENV_MEANT_OFF=1
    fi
    return 0
}

# 0 when a host script that would open, compact or ship the DuckDB file must
# stand down: .env says off, read either way.
ks_duckdb_stands_down() {
    [ "${KS_DUCKDB_ENV_MEANT_OFF:-0}" = 1 ]
}

# The sentence every stand-down appends after its own: what a line the two
# parsers read differently costs, and what a web held back on `on` means. The
# second is core.duckdb_switch.HELD_BACK_NOTE's, in the host's words, because
# these scripts cannot import it: the process they stand down for reads `.env`
# and not web's verdict, so while web is held back the file it goes on writing
# is neither compacted nor shipped — a hold is to be cleared, not lived in.
ks_duckdb_env_note() {
    local note="" bare="write KS_DUCKDB=off as a line of its own, nothing before it and nothing after"
    if ks_duckdb_stands_down; then
        if [ "$KS_DUCKDB_ENV_WEB" != off ]; then
            note="KS_DUCKDB in .env reads off to docker run --env-file and $KS_DUCKDB_ENV_WEB to compose, whose last line for it is a KS_DUCKDB: line docker skips: web runs on, and a sidecar would be refused; make it one line, KS_DUCKDB=off or KS_DUCKDB=on. "
        else
            case "$KS_DUCKDB_ENV" in
                invalid) note="KS_DUCKDB is written with quotes or a comment ($KS_DUCKDB_ENV_VALUE): compose drops them and web runs off, but docker run --env-file keeps them, so a sidecar would read a value the switch does not understand and run on, and the soak's P4 fails on it; $bare. " ;;
                refused) note="KS_DUCKDB is written with export, or with a space or a colon before its value: compose reads it and web runs off, but docker run --env-file refuses the whole file, so no sidecar starts from it — not this one, not the restore drill — and the soak's P4 fails on it; $bare. " ;;
                on) note="KS_DUCKDB is written KS_DUCKDB:off, which compose reads and docker run --env-file skips: web runs off, but a sidecar would run on, and the soak's P4 fails on it; $bare. " ;;
            esac
        fi
    fi
    printf '%s' "${note}If web is held back (/api/health duckdb_switch.held_back), the week has not started: meet those keys or set KS_DUCKDB=on."
}
