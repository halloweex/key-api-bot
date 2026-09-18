#!/usr/bin/env bash
# Ship the Postgres dumps off this host, encrypted.
#
# Until this existed, nothing carried Postgres off the machine. The off-site
# archive held the DuckDB Parquet export, a frozen bot.db and env.gpg
# (deploy/offsite_parquet.sh) and not one byte of Postgres. So
# `app.order_versions` — the only record that an order ever changed status —
# `app.stock_movements` — the only record that a quantity ever changed — the
# SMS roster, the dashboard access list and, since revision 0031, the ad spend
# somebody types in by hand all lived on exactly one disk, behind
# `deploy/pg_backup.sh`'s fourteen local copies, which share the volume with
# the cluster they copy. Any failure that takes the volume takes all fifteen.
#
# OD-01, decided by the owner on 17.09.2026: option (b) — the same Hetzner
# Storage Box the Parquet archive already uses, **gpg-encrypted** with the
# passphrase file already escrowed for env.gpg. Encryption is not optional and
# this script refuses without it: a ks-*.dump carries ~6 000 customer phone
# numbers and every dashboard account. A second provider is option (c) and
# later work; "the remote" below is where it attaches.
#
# WHAT ONE RUN SHIPS
#   ks-<stamp>.dump.gpg     the newest logical dump, encrypted
#   roles-<stamp>.sql.gpg   the roles file from the same run, encrypted
#   ks-<stamp>.sha256       sha256 of those two, in the clear
#
# The manifest is deliberately NOT encrypted. It names two files and their
# hashes and carries nothing else, and a verification that needs the
# passphrase is a verification that can only ever run from this host — which
# is the one machine whose loss the whole exercise is about.
#
# VERIFYING THE REMOTE COPY MEANS DOWNLOADING IT. The Storage Box offers no
# usable remote shell — offsite_parquet.sh learned that and says so — so the
# only way to know what arrived is to fetch the bytes back and hash them here.
# That doubles the transfer, and it is the whole difference between a backup
# and a hope: an upload that returns success having left a truncated file is
# precisely the failure this item exists to make impossible. The fetched copy
# is then decrypted and compared with the dump on this disk, because a sha256
# of ciphertext is satisfied by a file encrypted under a passphrase nobody
# holds any more.
#
# CRON, proposed:  40 7 * * *  /opt/key-api-bot/deploy/pg_offsite.sh
# That is the one gap in the host's morning. pg_backup starts 06:50 and is
# finished within a couple of minutes; the Monday restore drill at 07:20 is
# short; the base backup is 08:10 and the Monday PITR drill 08:40; and
# offsite_check speaks at 09:00, so a shipment that failed is already an
# 80-minute-old fact by the time the marker is read. It is nowhere near 02:00
# Sunday (the compaction, which stops the containers), 02:45 (the nightly
# export) or 03:30 (the other project's backup on this host).
#
# -E matters: without errtrace the ERR trap is not inherited by functions, so
# a command failing inside run_push would abort the shell without alerting.
set -Eeuo pipefail

cd "$(dirname "$0")/.."

CONFIG="deploy/backup.env"
# shellcheck source=/dev/null
[ -f "$CONFIG" ] && . "$CONFIG"

# Where the copies live and how to reach them — shared verbatim with
# deploy/pg_restore_drill.sh --from-remote, which has to find exactly what this
# writes. Sourced after backup.env, from the repository root.
# shellcheck source=/dev/null
source deploy/pg_offsite_lib.sh

MARKER="${BACKUP_PG_MARKER:-data/.pg_offsite_last_ok}"
# Fourteen off-site, the same count pg_backup.sh keeps locally. The two
# numbers answer different questions — how far back a mistake can be undone,
# and how much of that survives the machine — but nothing yet argues for them
# to differ, and a remote copy is ~50 MB.
RETAIN="${BACKUP_PG_RETAIN:-14}"
# pg_backup runs daily; this runs ~50 minutes after it. Thirty hours is one
# missed run plus slack, the same number deploy/daily_offsite.sh uses against
# the DuckDB backup for the same reason.
MAX_DUMP_AGE_HOURS="${BACKUP_PG_MAX_DUMP_AGE_HOURS:-30}"
LOCK_FILE="${BACKUP_PG_LOCK:-data/.pg_offsite.lock}"
LOG_FILE="${BACKUP_PG_OFFSITE_LOG:-data/logs/pg_offsite.log}"
# The bound lives here, in the change that creates the file, rather than in a
# logrotate stanza somebody has to remember to write: six things on this host
# grew without a limit and every one was found by a watchdog instead of being
# declared at creation.
LOG_MAX_LINES="${BACKUP_PG_OFFSITE_LOG_LINES:-2000}"

# Off-site not configured is a real problem and a different one from "the push
# failed"; the alert wording depends on telling them apart.
EX_UNCONFIGURED=78

LOG="$(mktemp)"
STEP_FILE="$(mktemp)"
STAGE="$(mktemp -d)"
VERIFY="$(mktemp -d)"

finish() {
    # Disarmed and un-erroring: this runs on the way out of a failed run too,
    # and with errtrace inherited any non-zero command here would re-enter the
    # ERR handler, or replace the exit code the caller is about to read.
    trap - ERR
    set +e
    # Append first, then clean up: the log is the only thing that outlives the
    # run, and it must carry the failing run as much as the healthy one.
    if [ -s "$LOG" ]; then
        mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null || true
        { printf '=== %s ===\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"; cat "$LOG"; } \
            >> "$LOG_FILE" 2>/dev/null || true
        if [ -f "$LOG_FILE" ] && [ "$(wc -l < "$LOG_FILE")" -gt "$LOG_MAX_LINES" ]; then
            tail -n "$LOG_MAX_LINES" "$LOG_FILE" > "$LOG_FILE.trim" \
                && mv "$LOG_FILE.trim" "$LOG_FILE"
        fi
    fi
    rm -rf "$LOG" "$STEP_FILE" "$STAGE" "$VERIFY"
}
trap finish EXIT

# run_push's output is captured into $LOG and the ERR trap fires while that
# redirection is in effect, so the handler writes to the real stdout and
# stderr, kept here as fds 3 and 4.
exec 3>&1 4>&2

step() { printf '%s' "$1" >"$STEP_FILE"; }

# --- alerting ---------------------------------------------------------------
# Step 07 of the alerts rework: one notifier for all host-cron shell — the
# kill switch, the instance signature, and a best-effort row in the archive.
source deploy/notify.sh

notify_unconfigured() {
    notify "$(printf '%s\n\n%s\n%s\n\n%s' \
        "⚠️ Postgres backups are not shipped anywhere" \
        "The dumps exist on this server only: $DUMP_DIR" \
        "Missing: $1" \
        "→ fill it into deploy/backup.env, then run deploy/pg_offsite.sh")" \
        "backup:pg_offsite_unconfigured"
}

notify_failure() {
    local step errors
    step="$(cat "$STEP_FILE" 2>/dev/null || echo '?')"
    errors="$(grep -v '^[[:space:]]*$' "$LOG" 2>/dev/null | tail -n 3)"
    notify "$(printf '%s\n\n%s\n%s\n\n%s' \
        "❌ Postgres off-site shipment FAILED" \
        "Failed at: $step" \
        "${errors:-(no error output captured)}" \
        "→ deploy/pg_offsite.sh; no encrypted copy of the cluster left this host")" \
        "backup:pg_offsite_failed"
}

# --- the actual work --------------------------------------------------------

run_push() {
    step "choose the dump"
    local dump roles stamp roles_stamp age_h
    dump="$(ls -1t "$DUMP_DIR"/ks-*.dump 2>/dev/null | head -1 || true)"
    if [ -z "$dump" ]; then
        echo "no dump in $DUMP_DIR — has deploy/pg_backup.sh ever run?" >&2
        return 1
    fi
    # pg_backup.sh deletes a dump that came out empty and exits, so an empty
    # file here means something else wrote it. Either way it is not a backup,
    # and shipping it would put a fresh name on nothing.
    if [ ! -s "$dump" ]; then
        echo "$dump is empty — that is not a backup, refusing to ship it" >&2
        return 1
    fi

    stamp="$(basename "$dump" .dump)"; stamp="${stamp#ks-}"
    roles="$DUMP_DIR/roles-$stamp.sql"
    # The pair must come from one run of pg_backup.sh. It writes both under one
    # stamp and deletes whichever came out empty, so the newest roles file can
    # legitimately be *older* than the newest dump — and a restore given last
    # week's roles is a cluster with tables nobody can log in to read. Refuse
    # rather than ship a mismatched pair under one manifest.
    roles_stamp="$(ls -1t "$DUMP_DIR"/roles-*.sql 2>/dev/null | head -1 || true)"
    roles_stamp="$(basename "${roles_stamp:-roles-none.sql}" .sql)"
    roles_stamp="${roles_stamp#roles-}"
    if [ ! -s "$roles" ]; then
        echo "roles-$stamp.sql is missing or empty; newest roles file is '$roles_stamp'" >&2
        echo "the roles dump is older than the newest dump — pg_dumpall failed on $stamp" >&2
        return 1
    fi

    # A dump older than a day means pg_backup stopped, and shipping it tonight
    # would paper over that: the marker would move, offsite_check would go
    # quiet, and the newest copy off-site would keep aging under a fresh
    # arrival time. Same argument as daily_offsite.sh's local-age guard.
    age_h=$(( ( $(date +%s) - $(_mtime "$dump") ) / 3600 ))
    if [ "$age_h" -gt "$MAX_DUMP_AGE_HOURS" ]; then
        echo "newest dump is ${age_h}h old (max ${MAX_DUMP_AGE_HOURS}h) — pg_backup has stopped; not shipping a stale cluster as today's" >&2
        return 1
    fi
    echo "staging: ks-$stamp.dump ($(( $(_size "$dump") / 1024 )) KB, ${age_h}h old) + roles-$stamp.sql"

    step "check the configuration"
    if [ -z "$REMOTE" ]; then
        echo "BACKUP_REMOTE is not set — these dumps exist on this server only." >&2
        CONFIG_MISSING="BACKUP_REMOTE"
        return "$EX_UNCONFIGURED"
    fi
    # OD-01 (b). Not a preference: the dump holds the SMS roster's phone
    # numbers and the dashboard access list, and the Storage Box is a
    # sub-account on the same provider as the VPS.
    if [ -z "$ENV_PASSFILE" ] || [ ! -r "$ENV_PASSFILE" ]; then
        echo "BACKUP_ENV_PASSFILE is unset or unreadable; these dumps carry customer phone numbers and are not shipped in the clear." >&2
        CONFIG_MISSING="BACKUP_ENV_PASSFILE (readable)"
        return "$EX_UNCONFIGURED"
    fi

    step "encrypt"
    # --passphrase-file, never --passphrase: an argument is visible in this
    # host's process table to every user on it for as long as gpg runs, and so
    # is an environment variable through /proc. Same call shape as the env.gpg
    # that already rides in the Parquet archive, same passphrase, so a restore
    # needs one escrowed secret and not two.
    local enc_dump="ks-$stamp.dump.gpg" enc_roles="roles-$stamp.sql.gpg" manifest="ks-$stamp.sha256"
    gpg --symmetric --cipher-algo AES256 --batch --yes \
        --passphrase-file "$ENV_PASSFILE" -o "$STAGE/$enc_dump" "$dump"
    gpg --symmetric --cipher-algo AES256 --batch --yes \
        --passphrase-file "$ENV_PASSFILE" -o "$STAGE/$enc_roles" "$roles"
    [ -s "$STAGE/$enc_dump" ] && [ -s "$STAGE/$enc_roles" ] \
        || { echo "gpg produced an empty file" >&2; return 1; }

    # sha256sum's own format, so an operator can `sha256sum -c` it by hand
    # after downloading. Hashes of the *ciphertext*: that is what travels, and
    # therefore what a verification can compare without the passphrase.
    { printf '%s  %s\n' "$(_sha256 "$STAGE/$enc_dump")" "$enc_dump"
      printf '%s  %s\n' "$(_sha256 "$STAGE/$enc_roles")" "$enc_roles"
    } > "$STAGE/$manifest"

    step "check what is already off-site"
    remote_prepare
    local newest_remote
    newest_remote="$(remote_stamps | head -1 || true)"
    # Strictly newer, not "different": re-running today's shipment after a
    # failure has to be able to finish, and every step here is idempotent. But
    # a *newer* copy already up means this host is offering an older cluster
    # than the one off-site — a restored backups/ directory, a clock that went
    # backwards — and count retention would then age the good copies out.
    if [ -n "$newest_remote" ] && [ "$newest_remote" \> "$stamp" ]; then
        echo "off-site already holds ks-$newest_remote, newer than ks-$stamp; refusing to ship an older cluster" >&2
        return 1
    fi

    step "upload"
    remote_put "$STAGE/$enc_dump" "$enc_dump"
    remote_put "$STAGE/$enc_roles" "$enc_roles"
    remote_put "$STAGE/$manifest" "$manifest"
    echo "shipping: $REMOTE:$PG_REMOTE_DIR/{$enc_dump,$enc_roles,$manifest}"

    step "verify the remote copy"
    # Download it back and hash it. See the header: there is no remote shell,
    # so this is the only sha256 of the remote copy that exists.
    local name want got
    remote_get "$manifest" "$VERIFY/$manifest"
    if ! cmp -s "$STAGE/$manifest" "$VERIFY/$manifest"; then
        echo "the manifest that came back is not the one that went up" >&2
        return 1
    fi
    for name in "$enc_dump" "$enc_roles"; do
        remote_get "$name" "$VERIFY/$name"
        want="$(awk -v n="$name" '$2 == n {print $1}' "$STAGE/$manifest")"
        got="$(_sha256 "$VERIFY/$name")"
        if [ -z "$want" ] || [ "$want" != "$got" ]; then
            echo "sha256 mismatch on $name: sent $want, off-site holds $got" >&2
            return 1
        fi
        echo "verified: $name $got"
    done

    # And it opens. The hashes above say the bytes that landed are the bytes
    # that were sent; they say nothing about whether anyone can read them,
    # because a sha256 of ciphertext is perfectly happy with a file encrypted
    # under a passphrase nobody holds any more. The realistic way that
    # happens is $ENV_PASSFILE being rotated, truncated or restored from
    # another host: from that night on every shipment would verify, the marker
    # would be written and the 36 h alarm would stay quiet for ever.
    #
    # What this proves is that the copy off-site opens with the passphrase
    # file THIS host holds, and decrypts to the dump on this disk byte for
    # byte. It cannot prove the escrowed copy of that passphrase still matches
    # — nothing on this machine can — and it does not prove the dump restores.
    # Those are the operator's escrow check and the Monday drill respectively.
    local plain
    plain="$VERIFY/plain.dump"
    if ! gpg --batch --yes --quiet --passphrase-file "$ENV_PASSFILE" \
            -o "$plain" -d "$VERIFY/$enc_dump"; then
        echo "the copy that landed off-site will not decrypt with $ENV_PASSFILE" >&2
        return 1
    fi
    if [ "$(_sha256 "$plain")" != "$(_sha256 "$dump")" ]; then
        echo "the copy off-site decrypts to something other than $dump" >&2
        return 1
    fi
    # Straight away rather than at the end of the run: this is a second
    # plaintext copy of ~6 000 phone numbers, and the only reason to have made
    # it has just been served.
    rm -f "$plain"
    echo "verified: $enc_dump opens with $ENV_PASSFILE and matches $(basename "$dump")"

    step "write marker"
    # Only here, and only on this path: deploy/offsite_check.sh reads this file
    # and an upload that stopped working looks exactly like one that works. A
    # marker written before verification would be the check swearing to a copy
    # nobody has read back.
    mkdir -p "$(dirname "$MARKER")"
    date -u +%Y-%m-%dT%H:%M:%SZ >"$MARKER"

    step "prune off-site"
    # Count, never age. Deleting by age removes the copy you most want on the
    # week the maker was broken — and the diagnostic agent recommended the age
    # form twice in one week, both times over a recovery point. The count is
    # anchored to what this run shipped: if the stamp we just verified is not
    # among the ones being kept, something is wrong with the listing and
    # nothing is deleted.
    local stamps kept old s junk
    # Debris first, and before RETAIN is even read: a `.part` is this script's
    # own litter — remote_put writes one and renames it away, so one still
    # lying there belongs to a run that never came back — and it must not
    # survive a typo in backup.env that stands the count retention down. The
    # same sweep takes a ciphertext whose manifest never landed, which is the
    # same interrupted upload seen one file earlier.
    if junk="$(remote_debris)"; then
        for s in $junk; do
            [ -n "$s" ] || continue
            remote_rm "$s"
            echo "swept: $s (debris of an interrupted upload)"
        done
    else
        echo "could not list $PG_REMOTE_DIR — nothing swept" >&2
    fi
    # A retention nobody can read is not a retention of zero. Found by running
    # this rather than reading it: `head -n 0` is legal on GNU and an error on
    # BSD, so a typo in backup.env failed the whole run on one platform and
    # asked for every copy to be deleted on the other. Neither is an answer to
    # "how many do we keep" — the copy is already up and verified, so the
    # shipment stands and only the deletion stands down.
    # Trimmed first: `BACKUP_PG_RETAIN=14 ` with a trailing space is a typo
    # that means fourteen, and standing down from retention over it would
    # leave the remote growing for a reason nobody would look for.
    RETAIN="$(printf '%s' "$RETAIN" | tr -d '[:space:]')"
    case "$RETAIN" in
        ''|*[!0-9]*)
            echo "BACKUP_PG_RETAIN=$RETAIN is not a number — nothing pruned" >&2
            return 0 ;;
    esac
    if [ "$RETAIN" -lt 1 ]; then
        echo "BACKUP_PG_RETAIN=$RETAIN would keep nothing — nothing pruned; set it to at least 1" >&2
        return 0
    fi
    # A listing that fails here is not a failed shipment: the copy is up and
    # verified and the marker is written. It costs one night of pruning, and
    # the anchor below refuses to delete anything it cannot see.
    if ! stamps="$(remote_stamps)"; then
        echo "could not list $PG_REMOTE_DIR after the upload — nothing pruned" >&2
        stamps=""
    fi
    kept="$(printf '%s\n' "$stamps" | head -n "$RETAIN")"
    old="$(printf '%s\n' "$stamps" | tail -n +$((RETAIN + 1)))"
    if ! printf '%s\n' "$kept" | grep -qx -- "$stamp"; then
        echo "the copy just shipped ($stamp) is not among the newest $RETAIN off-site — not pruning" >&2
        old=""
    fi
    for s in $old; do
        [ -n "$s" ] || continue
        remote_rm "ks-$s.dump.gpg" "roles-$s.sql.gpg" "ks-$s.sha256"
        echo "pruned: ks-$s"
    done
    echo "held off-site: $(printf '%s\n' "$kept" | grep -c . || true) of $RETAIN"
}

# Deliberately NOT `if run_push; then`: bash disables errexit for the whole
# body of a function invoked in a condition, so a failed stage would carry on
# to the next one and the alert would name the wrong step, or none.
CONFIG_MISSING=""
on_error() {
    local rc=$?
    # Disarm first: with errtrace inherited, any non-zero command in this
    # handler would re-enter it and spin.
    trap - ERR
    set +e
    {
        cat "$LOG"
        if [ "$rc" -eq "$EX_UNCONFIGURED" ]; then
            notify_unconfigured "${CONFIG_MISSING:-deploy/backup.env}"
        else
            notify_failure
        fi
    } >&4 2>&4
    exit "$rc"
}
trap on_error ERR

# One at a time. Cron fires this daily and a slow night — the verification
# downloads everything it uploaded — must not meet the next morning's run
# half way through its own upload, where two `rename`s race for one name.
# `flock` is util-linux and the host has it; a laptop may not, and there it is
# a warning rather than a refusal because there is no cron to collide with.
if command -v flock >/dev/null 2>&1; then
    mkdir -p "$(dirname "$LOCK_FILE")"
    exec 9>"$LOCK_FILE"
    if ! flock -n 9; then
        echo "another deploy/pg_offsite.sh holds $LOCK_FILE — leaving it to finish"
        exit 0
    fi
else
    echo "note: flock is not installed, running without the concurrency lock" >&2
fi

run_push >"$LOG" 2>&1

cat "$LOG"
echo "Postgres is off-site, encrypted."
echo "Prove it restores:  deploy/pg_restore_drill.sh --from-remote"
