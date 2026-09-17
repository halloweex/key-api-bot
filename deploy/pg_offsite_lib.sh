#!/usr/bin/env bash
# Where the off-site Postgres copies live, and how to reach them.
#
# Sourced by deploy/pg_offsite.sh, which writes them, and by
# deploy/pg_restore_drill.sh --from-remote, which reads them back. It exists
# because those two must agree on the naming and the transport exactly: a
# shipper and a drill that each spelled the remote path for themselves would
# be a drill proving the wrong directory is restorable, and the drill is the
# only thing that says the shipping works at all.
#
# It defines and does not act. Source it AFTER deploy/backup.env, from the
# repository root — neither caller wants a library that cd's underneath it,
# and only the callers know whether backup.env being absent is fatal.
#
# ONE TRANSPORT, WITH ROOM FOR THE SECOND. OD-01 chose the Hetzner Storage Box
# that already holds the Parquet archive (b) and named a second provider as
# later work (c) — the Storage Box is the same company as the VPS, so it
# survives this machine and not this supplier. When (c) arrives it is another
# arm of the `case` statements below and nothing above them changes. Only the
# arm in use is written: a transport nobody exercises is one nobody has tested.

DUMP_DIR="${BACKUP_PG_DUMP_DIR:-backups/postgres}"
SSH_PORT="${BACKUP_SSH_PORT:-23}"           # Storage Box speaks SSH on 23
SSH_KEY="${BACKUP_SSH_KEY:-$HOME/.ssh/storagebox_ed25519}"
REMOTE="${BACKUP_REMOTE:-}"
# Its own directory beside the Parquet archive, so retention here can never
# name a file retention there minted, whatever either glob is edited into.
PG_REMOTE_DIR="${BACKUP_PG_REMOTE_DIR:-${BACKUP_REMOTE_DIR:-key-api-bot}/postgres}"
ENV_PASSFILE="${BACKUP_ENV_PASSFILE:-}"
REMOTE_KIND="${BACKUP_PG_REMOTE_KIND:-sftp}"

# Two option arrays would be needed if this also spoke ssh, for the reason
# offsite_parquet.sh records: sftp takes -P for the port and reads -p as
# "preserve permissions", so one shared array silently sent a hostname of 23.
# This path speaks only sftp, so there is one array and it is sftp's.
SFTP_OPTS=(-P "$SSH_PORT" -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)

# `stat -c` and `sha256sum` are GNU; the host is Linux and cron is where this
# normally runs. But the first shipment and every investigation after a bad
# night happen at a keyboard, sometimes a laptop's, so each carries its BSD
# spelling as a fallback. Two two-line shims are cheaper than a backup script
# that can only be run on the machine that is on fire.
_mtime() { stat -c %Y "$1" 2>/dev/null || stat -f %m "$1"; }
_size()  { stat -c %s "$1" 2>/dev/null || stat -f %z "$1"; }
_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

_sftp_batch() { sftp -b - "${SFTP_OPTS[@]}" "$REMOTE"; }

remote_prepare() {
    case "$REMOTE_KIND" in
        sftp)
            # sftp's mkdir does not create parents, and a leading '-' is its
            # "ignore this failing" prefix — both usually already exist.
            printf -- '-mkdir %s\n-mkdir %s\n' \
                "${PG_REMOTE_DIR%/*}" "$PG_REMOTE_DIR" | _sftp_batch >/dev/null ;;
        *) echo "unknown BACKUP_PG_REMOTE_KIND=$REMOTE_KIND" >&2; return 1 ;;
    esac
}

# Uploaded under .part and renamed on success. A truncated upload already
# carrying its final name is indistinguishable, in the listing retention
# reads, from a good one — and would age a good copy out.
remote_put() {   # <local path> <remote name>
    case "$REMOTE_KIND" in
        sftp)
            printf -- '-rm %s/%s.part\nput %s %s/%s.part\n-rm %s/%s\nrename %s/%s.part %s/%s\n' \
                "$PG_REMOTE_DIR" "$2" \
                "$1" "$PG_REMOTE_DIR" "$2" \
                "$PG_REMOTE_DIR" "$2" \
                "$PG_REMOTE_DIR" "$2" "$PG_REMOTE_DIR" "$2" | _sftp_batch >/dev/null ;;
        *) echo "unknown BACKUP_PG_REMOTE_KIND=$REMOTE_KIND" >&2; return 1 ;;
    esac
}

remote_get() {   # <remote name> <local path>
    case "$REMOTE_KIND" in
        sftp) printf -- 'get %s/%s %s\n' "$PG_REMOTE_DIR" "$1" "$2" | _sftp_batch >/dev/null ;;
        *) echo "unknown BACKUP_PG_REMOTE_KIND=$REMOTE_KIND" >&2; return 1 ;;
    esac
}

remote_list() {
    case "$REMOTE_KIND" in
        sftp) printf -- 'cd %s\nls -1\n' "$PG_REMOTE_DIR" | _sftp_batch 2>/dev/null ;;
        *) echo "unknown BACKUP_PG_REMOTE_KIND=$REMOTE_KIND" >&2; return 1 ;;
    esac
}

# '-rm' rather than 'rm': retention names all three files of a stamp from its
# own templates, and a stamp whose roles half never made it up must not turn
# a prune into a failed run.
remote_rm() {    # <remote name>...
    local name
    case "$REMOTE_KIND" in
        sftp)
            for name in "$@"; do printf -- '-rm %s/%s\n' "$PG_REMOTE_DIR" "$name"; done \
                | _sftp_batch >/dev/null ;;
        *) echo "unknown BACKUP_PG_REMOTE_KIND=$REMOTE_KIND" >&2; return 1 ;;
    esac
}

# One remote name per line, with any directory prefix and stray whitespace
# removed. Whole lines are the whole point of this function existing: what
# reads it is retention, and `grep -o` over the raw listing matched
# `ks-<stamp>.dump.gpg` *inside* `ks-<stamp>.dump.gpg.part` — the debris an
# upload that died part-way leaves behind. Each such phantom then spent a
# retention slot and pushed a real copy out: reproduced at BACKUP_PG_RETAIN=4
# with five leftovers, which left exactly one genuine dump off-site.
#
# `sftp -b` echoes the commands it runs, so this deliberately does not try to
# separate names from noise — the anchored patterns below do that, and neither
# `cd <dir>` nor `ls -1` can be mistaken for one of ours.
remote_names() {
    remote_list \
        | tr -d '\r' \
        | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's:.*/::'
}

# The stamp a remote name belongs to, and nothing at all if the name is not one
# this family minted. Every deletion below goes through here, which is what
# lets retention delete by count without ever being able to name a file it did
# not ship.
remote_stamp_of() {   # <remote name>
    local name="$1" stamp=""
    case "$name" in
        ks-*.dump.gpg)   stamp="${name#ks-}";    stamp="${stamp%.dump.gpg}" ;;
        roles-*.sql.gpg) stamp="${name#roles-}"; stamp="${stamp%.sql.gpg}" ;;
        ks-*.sha256)     stamp="${name#ks-}";    stamp="${stamp%.sha256}" ;;
        *) return 0 ;;
    esac
    case "$stamp" in
        [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9][0-9][0-9])
            printf '%s' "$stamp" ;;
    esac
}

# The stamps of the shipments already off-site, newest first — keyed on the
# manifest, which pg_offsite.sh uploads last and only once both ciphertexts
# have landed. So a stamp listed here is a complete shipment by construction,
# which is what retention counts and what the drill picks up; a half-finished
# upload is not a copy and must not be mistaken for one.
remote_stamps() {
    remote_names \
        | grep -E '^ks-[0-9]{8}-[0-9]{6}\.sha256$' \
        | sed -e 's/^ks-//' -e 's/\.sha256$//' \
        | sort -r
}

# What this family minted and that no longer means anything: a `.part` from an
# upload that never came back, and a ciphertext whose manifest never landed —
# which is the same event seen one file earlier. Both are files only
# pg_offsite.sh creates, so removing them is its business and not a sweeper's:
# six things on this host grew without a bound and every one was found by a
# watchdog instead of being declared where it was created.
#
# A manifest with no ciphertext beside it is deliberately NOT swept. It is the
# record that a shipment happened, count retention removes it in its turn, and
# a rule that deletes the evidence of a copy is not one to reach for.
remote_debris() {
    local names name base stamp
    names="$(remote_names)" || return 1
    while IFS= read -r name; do
        [ -n "$name" ] || continue
        base="${name%.part}"
        stamp="$(remote_stamp_of "$base")"
        [ -n "$stamp" ] || continue
        if [ "$base" != "$name" ]; then
            printf '%s\n' "$name"
        elif [ "$base" != "ks-$stamp.sha256" ] \
             && ! printf '%s\n' "$names" | grep -qxF "ks-$stamp.sha256"; then
            printf '%s\n' "$name"
        fi
    done <<< "$names"
}
