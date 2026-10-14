#!/usr/bin/env bash
# The second Ark: freeze the static DuckDB file, ship it off this host
# encrypted, fetch it back and prove the copy that landed reads without the
# application. By hand, once, at the start of the week of silence — never from
# cron, and nothing in this repository schedules it.
#
# The Ark (deploy/ark_freeze.py) is the warehouse frozen so it can be read
# after the code that wrote it is gone: the byte copy, Parquet for every table
# and view, schema.sql, manifest.json and RESTORE.md. The first was taken on
# 2026-08-23, before Postgres held a landing row, and went off-site by hand
# (deploy/step05-preflight-runbook.md, B4). The second is the last picture of
# DuckDB anybody will take: web and the bot stopped, the file static, and
# KS_DUCKDB about to go `off` — step 3 of the week's order in the stage-5
# plan, as one command.
#
# OD-S5-6, the owner's decision of 10.10.2026: the same Storage Box as the
# Postgres dumps, gpg with the same escrowed passphrase file — mandatory, the
# Ark carries every customer's phone number — and two Arks kept.
#
# WHAT ONE RUN DOES, IN ORDER
#   1. Refuses whatever it can see is wrong before any Ark or upload exists:
#      no remote or no passphrase file (78); a .wal beside the source, --out
#      under ./data or anywhere in the repository, too little disk, no image,
#      the lock held (2).
#   2. --source: freezes the file into <out>/<stamp> in a throwaway container
#      of the image production runs (--network none, --pull never, the file
#      mounted alone and read-only), hashing it before and after. The byte
#      copy must equal both hashes, or the file moved under the freeze — it
#      was not static — and the half-made Ark is deleted. Then it prints
#      SOURCE RELEASED: nothing after that line reads the source again.
#   3. Verifies the local Ark at all three levels — L0 checksums, L1 the byte
#      copy opens and counts, L2 schema.sql replayed into an empty engine and
#      every table's Parquet loaded into the table it creates. A freeze that
#      does not verify is kept for inspection and not shipped.
#   4. Packs <stamp>/ and ark_freeze.py into one tar.gz (RESTORE.md tells a
#      stranger to verify with it, and the repository may be gone by then),
#      encrypts it, records the plaintext's sha256 and deletes the plaintext.
#   5. Uploads ark-<stamp>.tar.gz.gpg under .part and renames it.
#   6. Downloads it back. It must hash to what was sent; it must decrypt with
#      the passphrase file to the very tarball that was encrypted; unpacked,
#      every file must equal the local Ark's; and ark_freeze.py --verify
#      (L0–L2) must pass on the DOWNLOADED copy, run with the ark_freeze.py
#      that copy carries.
#   7. Only then uploads ark-<stamp>.sha256 — the sha256 of the ciphertext, in
#      the clear — and reads it back. A manifest off-site is the record that
#      its Ark came back and verified: retention counts nothing else, and a
#      ciphertext without one is debris, swept by the next run. A manifest
#      that comes back different is taken down again, not left to be counted.
#   8. Sweeps its own debris and keeps BACKUP_ARK_RETAIN (2) of its own Arks,
#      by count, anchored to the one it has just verified. A deletion that
#      fails is reported and stands down; the Ark is up and verified, so the
#      run still succeeds.
#
# AN ARK ALREADY RECORDED OFF-SITE IS NEVER SENT AGAIN. gpg's output differs
# on every run, so a second upload is a different file, and the transport
# removes the old one before it can rename the new one into place: the
# verified copy would be gone before its replacement was verified, and a
# damaged replacement would leave none. So `--ark` on a stamp whose manifest
# is off-site sends nothing. It fetches that copy back and verifies it as in
# 6, against its manifest instead of a hash taken here, then prunes. A copy
# that does not verify is not replaced either: replacing it is a decision
# about the only record of a verified shipment, and the run says how to take
# it — remove ark-<stamp>.sha256 off-site by hand and run `--ark` again.
#
# ON THE HOST, AS ROOT — the week's step 3
#   cd /opt/key-api-bot
#   docker compose stop web bot        # step 2: from here the file is static
#   deploy/ark_ship.sh --source data/analytics.duckdb
#   # SOURCE RELEASED means the file is not read again; the rest of the run
#   # reads only the Ark. Under KS_DUCKDB=off web never opens the file, so the
#   # week's later steps need not wait for the upload.
#   chattr +i -R /root/ark/key-api-bot/<stamp>    # once it says ARK SHIPPED
#
# A run that failed after its freeze is finished without freezing again:
#   deploy/ark_ship.sh --ark /root/ark/key-api-bot/<stamp>
#
# A .wal beside the source is a file somebody still holds, or one closed
# uncleanly: the byte copy would not carry what the log holds. Start web and
# stop it again (DuckDB checkpoints on a clean close), or freeze the newest
# nightly backup instead — `--source data/backups/analytics-<stamp>.duckdb`.
#
# WHERE THINGS GO
#   Local Ark   <out>/<stamp>, where <out> is --out, else BACKUP_ARK_DIR, else
#               /root/ark/key-api-bot. The Ark is plaintext, so ark_freeze.py
#               makes <stamp> mode 700 and every file in it 600, root's, from
#               the moment it exists — the container's umask is 0022 whatever
#               this script's is. <out> itself is the operator's choice and is
#               left as it is: a new one is made 700, one that exists may be a
#               directory other things use. Never under ./data: the disk
#               watchdog books growth there as `other` and pages at +0.75 GB a
#               week. Outside it, the Ark is `unattributed`, judged by
#               persistence — an Ark over ~2 GB will still read as growth
#               there, and that is this Ark, on purpose. Never anywhere else
#               in the repository either: its tree is public, `.gitignore`
#               covers data/ and nothing an Ark is made of, and one `git add .`
#               would publish every customer's phone number.
#   Scratch     <out>/.ship.XXXXXX, removed on every exit. At its peak it holds
#               the ciphertext, then the decrypted tarball and the unpacked
#               copy beside the Ark: the disk check asks for 4x the source
#               (3x an existing Ark) free before anything starts.
#   Off-site    BACKUP_ARK_REMOTE_DIR, else <BACKUP_REMOTE_DIR>/ark — the
#               directory the first Ark went to, as a plain
#               ark-20260823T212918Z.tar.gz, before OD-01 made encryption
#               mandatory. That name is not one this script mints: it is
#               never counted, swept or pruned here.
#
# THE TRANSPORT is deploy/pg_offsite_lib.sh's, sourced as is. Its functions
# address $PG_REMOTE_DIR, which this script points at the Ark's directory —
# never the dumps' and never the Parquet archive's, so no family's retention
# can list another's files whatever any glob is edited into.
#
# EXIT  0 shipped and verified (a deletion the prune could not make is named,
#       not failed) · 1 a step failed (it says which; the local Ark is kept)
#       · 2 refused, nothing frozen or sent · 78 not configured
#
# NOT HERE: cron, alerts (whoever runs this is reading it), chattr (the
# operator's, once the run says ARK SHIPPED), deleting the local Ark, and
# env.gpg — that moves into deploy/pg_offsite.sh, PR-10's half of OD-S5-6.
set -Eeuo pipefail
umask 077

EX_REFUSED=2
EX_UNCONFIGURED=78

usage() {
    cat <<'EOF'
usage: deploy/ark_ship.sh --source FILE [--out DIR]
       deploy/ark_ship.sh --ark DIR [--out DIR]

  --source FILE  freeze FILE into a new Ark, then ship and verify it. FILE
                 must be static: web stopped, or a nightly backup
  --ark DIR      ship and verify an Ark already frozen (a run that failed
                 after its freeze) without freezing again
  --out DIR      where Arks and the scratch space live: BACKUP_ARK_DIR,
                 else /root/ark/key-api-bot. Never under ./data, nor
                 anywhere else in the repository, whose tree is public

Run by hand, as root, on the host. See the header for the week's runbook.
EOF
}

refuse() { printf 'ark_ship: refused: %s\n' "$*" >&2; exit "$EX_REFUSED"; }
unconfigured() { printf 'ark_ship: not configured: %s\n' "$*" >&2; exit "$EX_UNCONFIGURED"; }

# An absolute path for one that may not exist yet. Arguments are resolved
# against the caller's directory, before this script moves to the repository
# root: `--source data/analytics.duckdb` typed in /opt/key-api-bot must not
# quietly mean something else when typed anywhere else.
_abspath() {
    local p="$1" tail="" base
    case "$p" in /*) ;; *) p="$PWD/$p" ;; esac
    while [ "$p" != "/" ] && [ "${p%/}" != "$p" ]; do p="${p%/}"; done
    while [ ! -d "$p" ]; do
        tail="/${p##*/}$tail"
        p="${p%/*}"
        [ -n "$p" ] || p="/"
    done
    base="$(cd "$p" && pwd -P)"
    [ "$base" = "/" ] && base=""
    printf '%s%s' "$base" "${tail:-}"
}

SOURCE="" ARK_IN="" OUT_ARG=""
while [ $# -gt 0 ]; do
    case "$1" in
        --source|--ark|--out)
            if [ $# -lt 2 ] || [ -z "$2" ]; then usage >&2; exit "$EX_REFUSED"; fi
            case "$1" in
                --source) SOURCE="$(_abspath "$2")" ;;
                --ark)    ARK_IN="$(_abspath "$2")" ;;
                --out)    OUT_ARG="$(_abspath "$2")" ;;
            esac
            shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; exit "$EX_REFUSED" ;;
    esac
done
# No default action. A cron line or a stray invocation without arguments does
# nothing but print the usage — this is a tool somebody runs on purpose.
if [ -z "$SOURCE$ARK_IN" ] || { [ -n "$SOURCE" ] && [ -n "$ARK_IN" ]; }; then
    usage >&2
    exit "$EX_REFUSED"
fi

cd "$(dirname "$0")/.."
REPO="$(pwd -P)"
FREEZER="$REPO/deploy/ark_freeze.py"

CONFIG="deploy/backup.env"
# shellcheck source=/dev/null
[ -f "$CONFIG" ] && . "$CONFIG"

# shellcheck source=/dev/null
source deploy/pg_offsite_lib.sh

DUMPS_REMOTE_DIR="$PG_REMOTE_DIR"
ARK_REMOTE_DIR="${BACKUP_ARK_REMOTE_DIR:-${BACKUP_REMOTE_DIR:-key-api-bot}/ark}"
PG_REMOTE_DIR="$ARK_REMOTE_DIR"
# Two, the owner's number: the first Ark is not this family's and is not
# counted, so in practice this keeps the second Ark and one re-freeze of it.
RETAIN="${BACKUP_ARK_RETAIN:-2}"
# The image production runs, never pulled: its DuckDB is the version that wrote
# the file, and a pull would change what the next `up -d` starts.
IMAGE="${BACKUP_ARK_IMAGE:-${BACKUP_RESTORE_IMAGE:-halloweex/keycrm-web:latest}}"
MEM="${BACKUP_ARK_MEMORY:-6500m}"
OUT_ROOT="${OUT_ARG:-$(_abspath "${BACKUP_ARK_DIR:-/root/ark/key-api-bot}")}"

DOCKER_RUN=(docker run --rm --pull never --network none --user 0
            --oom-score-adj 1000 --memory "$MEM" --memory-swap "$MEM"
            --log-opt max-size=10m --entrypoint python3)

is_stamp() {
    case "$1" in
        [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z) return 0 ;;
    esac
    return 1
}

# ── refuse before anything exists ─────────────────────────────────────────────

if [ -z "$REMOTE" ]; then
    unconfigured "BACKUP_REMOTE is not set in $CONFIG — there is nowhere to ship the Ark"
fi
if [ -z "$ENV_PASSFILE" ] || [ ! -r "$ENV_PASSFILE" ]; then
    unconfigured "BACKUP_ENV_PASSFILE is unset or unreadable; the Ark carries every customer's phone number and is not shipped in the clear (OD-S5-6)"
fi

_trimmed() { local d="$1"; while [ "${d%/}" != "$d" ]; do d="${d%/}"; done; printf '%s' "$d"; }
case "$(_trimmed "$ARK_REMOTE_DIR")" in
    "$(_trimmed "$DUMPS_REMOTE_DIR")"|"$(_trimmed "${BACKUP_REMOTE_DIR:-key-api-bot}")")
        refuse "BACKUP_ARK_REMOTE_DIR=$ARK_REMOTE_DIR is another family's directory; the Ark ships to its own" ;;
esac

DATA_ABS="$(_abspath data)"
case "$OUT_ROOT/" in
    "$DATA_ABS/"*)
        refuse "--out $OUT_ROOT is under ./data, where the disk watchdog pages growth as 'other' at +0.75 GB a week; put the Ark outside it (default /root/ark/key-api-bot)" ;;
esac
# The rest of the tree is worse, not better: ./data is at least ignored by git.
# A relative BACKUP_ARK_DIR resolves here, so this is reached without anybody
# typing the repository's path.
case "$OUT_ROOT/" in
    "$REPO/"*)
        refuse "--out $OUT_ROOT is inside the repository ($REPO), whose tree is public; the Ark is plaintext, every customer's phone number, and .gitignore does not cover it — one 'git add .' would publish it. Put it outside (default /root/ark/key-api-bot)" ;;
esac
# docker -v splits on ':' and an sftp batch on whitespace.
for p in "$OUT_ROOT" "$SOURCE" "$ARK_IN" "$REPO"; do
    case "$p" in
        *[[:space:]:]*) refuse "path '$p' carries whitespace or ':', which docker -v and sftp batches cannot carry" ;;
    esac
done

if [ -n "$SOURCE" ]; then
    [ -f "$SOURCE" ] || refuse "no such file: $SOURCE"
    [ -s "$SOURCE" ] || refuse "$SOURCE is empty — that is not a warehouse"
    if [ -e "$SOURCE.wal" ]; then
        refuse "$SOURCE.wal exists: the file is held open, or was closed uncleanly, and the byte copy would not carry what the log holds. Start web and stop it cleanly, or freeze the newest nightly backup instead"
    fi
    # The Ark is ~1.3x its source; the shipment's peak adds the ciphertext
    # and then the decrypted tarball and the unpacked copy.
    NEED_KB=$(( ( $(_size "$SOURCE") / 1024 + 1 ) * 4 ))
else
    [ -d "$ARK_IN" ] || refuse "no such directory: $ARK_IN"
    is_stamp "${ARK_IN##*/}" \
        || refuse "$ARK_IN is not an Ark: its name is not a stamp ark_freeze.py writes (YYYYMMDDTHHMMSSZ)"
    [ -f "$ARK_IN/manifest.json" ] || refuse "$ARK_IN holds no manifest.json — not a finished Ark"
    NEED_KB=$(( $(du -sk "$ARK_IN" | cut -f1) * 3 ))
fi

docker image inspect "$IMAGE" >/dev/null 2>&1 \
    || refuse "no image $IMAGE on this host. The Ark is frozen and verified by the DuckDB production runs, and this does not pull"

mkdir -p "$OUT_ROOT"
AVAIL_KB="$(df -Pk "$OUT_ROOT" | awk 'NR == 2 {print $4}')"
case "$AVAIL_KB" in ''|*[!0-9]*) refuse "cannot read the free space under $OUT_ROOT" ;; esac
if [ "$AVAIL_KB" -lt "$NEED_KB" ]; then
    refuse "$(( AVAIL_KB / 1024 )) MB free under $OUT_ROOT, $(( NEED_KB / 1024 )) MB needed for the Ark and its shipment"
fi

# One at a time: two runs would race their renames for one remote name, and
# each one's prune would count the other's half-finished upload.
if command -v flock >/dev/null 2>&1; then
    exec 9>"$OUT_ROOT/.ark_ship.lock"
    flock -n 9 || refuse "another deploy/ark_ship.sh holds $OUT_ROOT/.ark_ship.lock"
else
    echo "note: flock is not installed, running without the concurrency lock" >&2
fi

# ── the work ──────────────────────────────────────────────────────────────────

WORK="$(mktemp -d "$OUT_ROOT/.ship.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

STEP="start"
ARK_DIR=""
STAMP=""
SOURCE_SHA=""
PLAIN_SHA=""
ENC_SHA=""
ENC_SAID="sent"   # where ENC_SHA came from, for the message that compares it
ALREADY=""        # set when this Ark's manifest was off-site before the run

step() { STEP="$1"; printf '── %s\n' "$1"; }

# Returns non-zero so the ERR trap below names the step; the message is the
# reason a human reads.
fail() { printf '%s\n' "$*" >&2; return 1; }

# A failure is exit 1 whatever the failing command returned: 2 and 78 promise
# that nothing was frozen or sent, and past this point that is no longer true.
on_error() {
    trap - ERR
    set +e
    printf 'ark_ship: FAILED at: %s\n' "$STEP" >&2
    if [ -n "$ALREADY" ]; then
        printf '%s\n' \
            "ark-$STAMP is recorded off-site as verified by its manifest, ark-$STAMP.sha256, and a recorded copy is not replaced." \
            "If the transport failed, run again. If the copy off-site is damaged, remove ark-$STAMP.sha256 from $ARK_REMOTE_DIR by hand; the next --ark then ships the Ark anew." >&2
    fi
    if [ -n "$ARK_DIR" ] && [ -d "$ARK_DIR" ]; then
        printf 'the local Ark is kept: %s\n' "$ARK_DIR" >&2
        printf 'finish without freezing again:  deploy/ark_ship.sh --ark %s\n' "$ARK_DIR" >&2
    fi
    exit 1
}
trap on_error ERR

# The Ark directories under a root, one per line, in stamp order.
stamps_in() {
    local d
    for d in "$1"/*/; do
        d="${d%/}"
        d="${d##*/}"
        if is_stamp "$d"; then printf '%s\n' "$d"; fi
    done
    return 0
}

verify_in_container() {   # <ark dir> <the ark_freeze.py to verify it with>
    "${DOCKER_RUN[@]}" \
        -v "$1:/ark:ro" \
        -v "$2:/ark_freeze.py:ro" \
        "$IMAGE" /ark_freeze.py --verify /ark
}

freeze() {
    step "freeze $SOURCE"
    local name before after copied had new="" s
    name="${SOURCE##*/}"
    before="$(_sha256 "$SOURCE")"
    had=" $(stamps_in "$OUT_ROOT" | tr '\n' ' ')"

    # The file alone, read-only: the container sees nothing else in ./data, and
    # anything written beside the file lands in its own layer and goes with it.
    if ! "${DOCKER_RUN[@]}" \
            -v "$SOURCE:/src/$name:ro" \
            -v "$OUT_ROOT:/out" \
            -v "$FREEZER:/ark_freeze.py:ro" \
            "$IMAGE" /ark_freeze.py --source "/src/$name" --out /out; then
        for s in $(stamps_in "$OUT_ROOT"); do
            case "$had" in *" $s "*) ;; *) rm -rf "${OUT_ROOT:?}/$s"; echo "removed the half-made $OUT_ROOT/$s" >&2 ;; esac
        done
        fail "ark_freeze.py could not freeze $SOURCE"
    fi

    for s in $(stamps_in "$OUT_ROOT"); do
        case "$had" in *" $s "*) ;; *) new="${new:+$new }$s" ;; esac
    done
    case "$new" in
        '')   fail "the freeze reported success and left no new Ark under $OUT_ROOT" ;;
        *' '*) fail "the freeze left more than one new Ark under $OUT_ROOT: $new" ;;
    esac
    STAMP="$new"
    ARK_DIR="$OUT_ROOT/$STAMP"

    # Static is proved, not assumed: the byte copy is taken last, after every
    # table has been exported, so a writer during the export changes the file
    # between these two hashes and the copy matches neither the Parquet nor
    # the counts. Such an Ark is a picture of no moment at all.
    after="$(_sha256 "$SOURCE")"
    copied="$(_sha256 "$ARK_DIR/analytics.duckdb.frozen")"
    if [ "$before" != "$after" ] || [ "$copied" != "$before" ]; then
        rm -rf "$ARK_DIR"
        ARK_DIR=""
        fail "$SOURCE moved while it was being frozen (sha256 before $before, after $after, byte copy $copied): it is not static. Stop whatever holds it and run again; the half-made Ark was deleted"
    fi
    SOURCE_SHA="$before"
    printf 'SOURCE RELEASED %s: nothing below reads %s again (sha256 %s)\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$SOURCE" "$SOURCE_SHA"
}

pack_and_encrypt() {
    step "pack and encrypt"
    local plain="$WORK/ark-$STAMP.tar.gz" enc="ark-$STAMP.tar.gz.gpg"
    tar -czf "$plain" -C "${ARK_DIR%/*}" "$STAMP" -C "$REPO/deploy" ark_freeze.py
    PLAIN_SHA="$(_sha256 "$plain")"
    # --passphrase-file, never --passphrase: an argument is in the process
    # table for as long as gpg runs. Same call and same passphrase as the
    # Postgres dumps, so a restore needs one escrowed secret and not two.
    gpg --symmetric --cipher-algo AES256 --batch --yes \
        --passphrase-file "$ENV_PASSFILE" -o "$WORK/$enc" "$plain"
    # The plaintext tarball is a third copy of every phone number on this
    # disk, and its only use — the hash above — has been served.
    rm -f "$plain"
    [ -s "$WORK/$enc" ] || fail "gpg produced an empty file"
    ENC_SHA="$(_sha256 "$WORK/$enc")"
    # sha256sum's own format, so it can be checked by hand after a download.
    # The ciphertext's hash: what travels, and what a check can compare
    # without the passphrase.
    printf '%s  %s\n' "$ENC_SHA" "$enc" > "$WORK/ark-$STAMP.sha256"
    echo "packed: $enc ($(( $(_size "$WORK/$enc") / 1024 )) KB)"
}

# Whether this Ark is recorded off-site already. A listing that cannot be read
# stops the run before anything is sent: shipping blind could send over the
# verified copy this question exists to protect.
look_off_site() {
    step "look for this Ark off-site"
    local names
    remote_prepare
    names="$(remote_names)" \
        || fail "could not list $ARK_REMOTE_DIR, so whether ark-$STAMP is already there is unknown; nothing was sent"
    if grep -qxF "ark-$STAMP.sha256" <<< "$names"; then ALREADY=1; fi
    return 0
}

upload() {
    step "upload"
    local enc="ark-$STAMP.tar.gz.gpg"
    # The ciphertext alone. Its manifest goes up only once the copy has come
    # back and verified (record_shipment): until then this is a file and not
    # a shipment, and a run that dies here leaves debris the next one sweeps.
    remote_put "$WORK/$enc" "$enc"
    rm -f "$WORK/$enc"
    echo "shipped: $REMOTE:$ARK_REMOTE_DIR/$enc"
}

# Fetches the copy of $STAMP off-site and proves it is this Ark: it hashes to
# $ENC_SHA; it decrypts with the passphrase file — to the very tarball that was
# encrypted, when this run encrypted one; unpacked, it is $STAMP/ and an
# ark_freeze.py, equal to the local Ark file for file; and it verifies L0-L2
# with the ark_freeze.py it carries.
fetch_and_verify() {
    local enc="ark-$STAMP.tar.gz.gpg" back="$WORK/back" got
    mkdir "$back"

    # There is no remote shell on a Storage Box, so the only sha256 of the
    # remote copy that can exist is one taken of its bytes, here.
    remote_get "$enc" "$back/$enc"
    got="$(_sha256 "$back/$enc")"
    [ "$got" = "$ENC_SHA" ] \
        || fail "sha256 mismatch on $enc: $ENC_SAID $ENC_SHA, off-site holds $got"
    echo "verified: $enc $got"

    # A sha256 of ciphertext is satisfied by a file encrypted under a
    # passphrase nobody holds any more. It must open with the file this host
    # holds, into the very tarball that was encrypted.
    gpg --batch --yes --quiet --passphrase-file "$ENV_PASSFILE" \
        -o "$back/ark.tar.gz" -d "$back/$enc" \
        || fail "the copy that landed off-site will not decrypt with $ENV_PASSFILE"
    rm -f "$back/$enc"
    if [ -n "$PLAIN_SHA" ]; then
        [ "$(_sha256 "$back/ark.tar.gz")" = "$PLAIN_SHA" ] \
            || fail "the copy off-site decrypts to something other than the tarball that was encrypted"
    fi

    mkdir "$back/x"
    tar -C "$back/x" -xzf "$back/ark.tar.gz"
    rm -f "$back/ark.tar.gz"
    if [ ! -d "$back/x/$STAMP" ] || [ ! -f "$back/x/ark_freeze.py" ]; then
        fail "the archive that came back does not hold $STAMP/ and ark_freeze.py"
    fi
    diff -rq "$ARK_DIR" "$back/x/$STAMP" \
        || fail "the copy that came back is not the Ark: the files above differ"
    # The verifier must be the one that went up — when this run sent it. A
    # copy an earlier run sent carries the ark_freeze.py of its day, which this
    # checkout may have moved past; that one is what a stranger will have,
    # and the verification below runs with it either way.
    if [ -z "$ALREADY" ]; then
        cmp -s "$FREEZER" "$back/x/ark_freeze.py" \
            || fail "the ark_freeze.py that came back is not the one that went up"
    fi
    echo "verified: the downloaded copy equals $ARK_DIR, file for file"

    step "verify the downloaded copy (L0-L2)"
    # The plan's bar, and the reason for all of the above: the copy that is
    # off-site — not the one on this disk — restores into an empty engine,
    # with the verifier it carries, which is what a stranger will have.
    verify_in_container "$back/x/$STAMP" "$back/x/ark_freeze.py" \
        || fail "the downloaded copy did not verify"
    # Plaintext again; its only use has been served.
    rm -rf "$back"
}

verify_off_site() {
    step "verify the copy off-site"
    fetch_and_verify
}

# The manifest goes up last of all, once the copy has come back and verified:
# a stamp with a manifest is a verified shipment, and that is all retention
# counts. It is read back like everything else, and one that comes back
# different is taken down again — a record that does not say what was
# verified would be counted, and a check by hand against it would fail.
record_shipment() {
    step "record the shipment"
    local manifest="ark-$STAMP.sha256"
    remote_put "$WORK/$manifest" "$manifest"
    remote_get "$manifest" "$WORK/$manifest.back"
    if ! cmp -s "$WORK/$manifest" "$WORK/$manifest.back"; then
        remote_rm "$manifest" || true
        fail "the manifest that came back is not the one that went up; it was taken down again, and the ciphertext stays unrecorded until the Ark is shipped again"
    fi
    echo "recorded: $REMOTE:$ARK_REMOTE_DIR/$manifest"
}

# An Ark recorded off-site already: nothing is sent over it (see the header).
# Its copy is fetched back and held to the same bar as a fresh shipment,
# against the hash its manifest records rather than one taken here.
verify_recorded() {
    step "verify the copy already off-site"
    local manifest="ark-$STAMP.sha256" name="" rest=""
    echo "ark-$STAMP is recorded off-site already: not sent again, fetched back and verified"
    remote_get "$manifest" "$WORK/$manifest"
    read -r ENC_SHA name rest < "$WORK/$manifest" || true
    if ! [[ "$ENC_SHA" =~ ^[0-9a-f]{64}$ ]] || [ "$name" != "ark-$STAMP.tar.gz.gpg" ] || [ -n "$rest" ]; then
        fail "$manifest off-site is not a manifest this script writes"
    fi
    ENC_SAID="its manifest says"
    fetch_and_verify
}

# What this script minted, and nothing else: ark-<stamp>.tar.gz.gpg and
# ark-<stamp>.sha256. The first Ark's ark-20260823T212918Z.tar.gz is not one.
ark_stamp_of() {   # <remote name>
    local name="$1" stamp=""
    case "$name" in
        ark-*.tar.gz.gpg) stamp="${name#ark-}"; stamp="${stamp%.tar.gz.gpg}" ;;
        ark-*.sha256)     stamp="${name#ark-}"; stamp="${stamp%.sha256}" ;;
        *) return 0 ;;
    esac
    if is_stamp "$stamp"; then printf '%s' "$stamp"; fi
    return 0
}

# Verified shipments, newest first — keyed on the manifest, which goes up only
# once its copy has come back and verified.
ark_stamps() {
    remote_names \
        | grep -E '^ark-[0-9]{8}T[0-9]{6}Z\.sha256$' \
        | sed -e 's/^ark-//' -e 's/\.sha256$//' \
        | sort -r
}

# Its own litter only: a .part from an upload that never came back, and a
# ciphertext with no manifest — a copy that never came back verified, or whose
# run died before it could record it. A manifest without its ciphertext is the
# record that a shipment happened and is left for count retention.
ark_debris() {
    local names name base stamp
    names="$(remote_names)" || return 1
    while IFS= read -r name; do
        [ -n "$name" ] || continue
        base="${name%.part}"
        stamp="$(ark_stamp_of "$base")"
        [ -n "$stamp" ] || continue
        if [ "$base" != "$name" ]; then
            printf '%s\n' "$name"
        elif [ "$base" != "ark-$stamp.sha256" ] \
             && ! grep -qxF "ark-$stamp.sha256" <<< "$names"; then
            printf '%s\n' "$name"
        fi
    done <<< "$names"
}

HELD="?"
STUCK=""
prune() {
    step "prune off-site"
    local junk s stamps kept old
    # The Ark is up, verified and recorded by now, so nothing here may fail
    # the run: a listing nobody can read, a count nobody can read and a
    # deletion that does not happen each stand the deletion down and say so.
    # (A prune that failed the run once sent the operator to ship again.)
    #
    # Debris before RETAIN is even read: a typo in backup.env must not keep
    # this script's own litter on the box.
    if junk="$(ark_debris)"; then
        for s in $junk; do
            [ -n "$s" ] || continue
            if remote_rm "$s"; then
                echo "swept: $s (debris of an upload that never came back verified)"
            else
                echo "could not delete $s — left off-site; the next run sweeps it" >&2
            fi
        done
    else
        echo "could not list $ARK_REMOTE_DIR — nothing swept" >&2
    fi
    # Count, never age, and a count nobody can read is not zero.
    RETAIN="$(printf '%s' "$RETAIN" | tr -d '[:space:]')"
    case "$RETAIN" in
        ''|*[!0-9]*) echo "BACKUP_ARK_RETAIN=$RETAIN is not a number — nothing pruned" >&2; return 0 ;;
    esac
    if [ "$RETAIN" -lt 1 ]; then
        echo "BACKUP_ARK_RETAIN=$RETAIN would keep nothing — nothing pruned; set it to at least 1" >&2
        return 0
    fi
    if ! stamps="$(ark_stamps)"; then
        echo "could not list $ARK_REMOTE_DIR after the upload — nothing pruned" >&2
        stamps=""
    fi
    kept="$(printf '%s\n' "$stamps" | head -n "$RETAIN")"
    old="$(printf '%s\n' "$stamps" | tail -n +$((RETAIN + 1)))"
    # Anchored to what this run verified: if it is not among the newest N,
    # the listing is wrong — or a newer Ark is up — and nothing is deleted.
    if ! printf '%s\n' "$kept" | grep -qx -- "$STAMP"; then
        echo "the Ark just shipped ($STAMP) is not among the newest $RETAIN off-site — not pruning" >&2
        old=""
    fi
    for s in $old; do
        [ -n "$s" ] || continue
        # The manifest first: a prune cut short then leaves a ciphertext with
        # no record — debris, swept next time — and never a record of a copy
        # that is no longer there, which retention would go on counting.
        if remote_rm "ark-$s.sha256" "ark-$s.tar.gz.gpg"; then
            echo "pruned: ark-$s"
        else
            echo "could not delete ark-$s — left off-site; the next run prunes it, or delete it by hand" >&2
            STUCK="${STUCK:+$STUCK }ark-$s"
        fi
    done
    HELD="$(printf '%s\n' "$kept" | grep -c . || true)"
    return 0
}

summary() {
    local others
    # Its own .part too: a sweep that could not delete one has said so above,
    # and the run now reaches this line after it.
    others="$(remote_names 2>/dev/null \
        | grep -E '^ark-' \
        | grep -vE '^ark-[0-9]{8}T[0-9]{6}Z\.(tar\.gz\.gpg|sha256)(\.part)?$' \
        | tr '\n' ' ' || true)"
    echo
    echo "ARK SHIPPED AND VERIFIED"
    echo "  ark       $ARK_DIR ($(( $(du -sk "$ARK_DIR" | cut -f1) / 1024 )) MB on this disk, plaintext)"
    if [ -n "$SOURCE_SHA" ]; then
        echo "  source    $SOURCE  sha256 $SOURCE_SHA"
        echo "            (the silence check's first record of this file should carry the same hash)"
    fi
    echo "  off-site  $ARK_REMOTE_DIR/ark-$STAMP.tar.gz.gpg  sha256 $ENC_SHA"
    if [ -n "$ALREADY" ]; then
        echo "  sent      not again: recorded off-site by an earlier run, and a recorded copy"
        echo "            is never sent over"
        echo "  verified  fetched back, sha256 equal to its manifest, decrypted, equal to the"
    else
        echo "  verified  fetched back, sha256 equal, decrypted to the tarball sent, equal to the"
    fi
    echo "            local Ark file for file, L0-L2 OK on the downloaded copy"
    echo "  held      $HELD of $RETAIN Arks minted by this script"
    [ -z "$STUCK" ] || echo "  not pruned $STUCK (the deletion failed: the next run prunes them, or delete them by hand)"
    [ -z "$others" ] || echo "  also here $others(not minted here; never counted or touched)"
    echo "next:  chattr +i -R $ARK_DIR"
}

if [ -n "$SOURCE" ]; then
    freeze
else
    ARK_DIR="$ARK_IN"
    STAMP="${ARK_IN##*/}"
fi

step "verify the local Ark (L0-L2)"
verify_in_container "$ARK_DIR" "$FREEZER" \
    || fail "the Ark at $ARK_DIR did not verify; it is kept for inspection and not shipped"

look_off_site
if [ -n "$ALREADY" ]; then
    verify_recorded
else
    pack_and_encrypt
    upload
    verify_off_site
    record_shipment
fi
prune
summary
