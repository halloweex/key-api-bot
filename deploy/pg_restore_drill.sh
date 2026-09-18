#!/usr/bin/env bash
# Prove a Postgres restore works, with a canary. The ClickHouse half of this
# lives in `ks-data-platform`.
#
# A drill that only works once there is data to lose is not a drill, it is a
# hope. The canary gives the restore something to be right about.
#
# TWO DRILLS, AND THE SECOND ONE IS THE HONEST ONE.
#
#   (default)       the local dump: take one, drop the canary, put it back.
#                   It proves pg_dump/pg_restore work and that this cluster
#                   can be repaired from a file on its own disk.
#   --from-remote   the copy that survives this machine: fetch the newest
#                   file deploy/pg_offsite.sh shipped, verify its sha256,
#                   decrypt it, restore it into a throwaway container and
#                   count rows against live.
#
# The first says nothing about the second, and the second is the one that
# matters the day the disk does not come back. It was added with DN-09,
# because a copy nobody has restored is a copy nobody knows they have.
#
# CRON for the remote half, proposed:
#   20 8 * * 1  /opt/key-api-bot/deploy/pg_restore_drill.sh --from-remote
# Weekly, like the two drills it sits between, and 08:20 is the gap in the
# host's Monday: the local drill is at 07:20, the shipment at 07:40, the base
# backup at 08:10 and the PITR drill at 08:40, and offsite_check speaks at
# 09:00 — so a drill that failed is a 40-minute-old fact by the time anyone
# reads the morning's other verdicts. It downloads ~50 MB and restores it into
# a throwaway container, which is why it is weekly and not nightly.
#
# Writes and drops only objects named `_drill_*`, in the `meta` schema.
# Touches nothing else. Safe to run against a populated database. The remote
# drill is stricter still: it only SELECTs from live, through `ks_readonly`.
set -Eeuo pipefail
cd "$(dirname "$0")/.."

STAMP="$(date -u +%Y%m%d-%H%M%S)"
ROWS="${KS_DRILL_ROWS:-50000}"
PGC() { docker compose exec -T postgres psql -qtAX -U postgres -d ks -c "$1"; }
t0() { date +%s.%N; }
ms() { echo "$1 $2" | awk '{printf "%.0f", ($2-$1)*1000}'; }

source deploy/notify.sh

# A failing drill has to reach a human, or it is a script somebody remembers to
# run. That was the state this one shipped in: --from-remote printed FAIL: to
# root's local mail on a host where nothing reads it, while its sibling
# deploy/pg_pitr_drill.sh has alerted under backup:pitr_drill_failed since it
# was written. The failure that makes it matter is a rotated $ENV_PASSFILE:
# every nightly shipment still verifies (it hashes ciphertext), so this drill
# is the only instrument that would ever say so.
#
# Armed only on the --from-remote path. The local drill next door dumps and
# restores this host's own cluster, which is a different claim with its own
# cron and no history of anyone watching it fail; giving it a key here would
# start paging about a script on the strength of a guess at what its failures
# mean. That is a separate decision and is deliberately not taken.
#
# --quiet, copied from pg_pitr_drill.sh along with its reason: developing that
# script sent admins two "PITR drill failed" messages on 2026-08-31 for a
# fault that was being fixed as they arrived. Cron runs without it.
DRILL_NOTIFY=""
DRILL_QUIET=""
for arg in "$@"; do
    [ "$arg" = "--quiet" ] && DRILL_QUIET=1
done

fail() {
    # Disarm first: with errtrace inherited the ERR trap below would re-enter
    # this handler on any non-zero command inside it.
    trap - ERR
    echo "FAIL: $*" >&2
    if [ -n "$DRILL_NOTIFY" ] && [ -z "$DRILL_QUIET" ]; then
        notify "$(printf '%s\n%s\n%s' \
            "🚨 The off-site Postgres copy did not restore" \
            "$*" \
            "→ deploy/pg_restore_drill.sh --from-remote; the only copy that survives this host is unproven")" \
            "backup:pg_offsite_drill_failed" || true
    fi
    exit 1
}

# ── the remote drill ─────────────────────────────────────────────────────────
# What a restored dump is checked against, and in which direction it may
# differ. A dump is taken at 06:50 and this runs later, so the copy is always
# BEHIND live and equality is the wrong criterion — demanding it would fail
# every morning for the most ordinary reason there is. The rule instead:
#
#   restored <= live, and (live - restored) within max(N rows, P% of live)
#
# with the direction relaxed for one table. app.order_versions and
# app.stock_movements are append-only, pinned by a test that parses the
# repository for an UPDATE or DELETE against either; meta.chain_watermarks
# gains keys and never loses one. For those three a restored count ABOVE live
# means rows have disappeared from the live cluster since the dump, which is
# the alarming direction and is reported as such. app.manual_expenses is the
# exception: the /expenses form deletes rows, so it may legitimately shrink,
# and there the rule is on the absolute gap in either direction.
DRILL_TABLES=(
    "app.order_versions:grows"
    "app.manual_expenses:either"
    "app.stock_movements:grows"
    "meta.chain_watermarks:grows"
)
# Absolute floor first, because two of these tables are small and a percentage
# of a small number is not a margin. app.order_versions gains ~100 rows a day
# and app.stock_movements a few hundred, so a day of lag is well inside both.
DRILL_MARGIN_ROWS="${KS_DRILL_MARGIN_ROWS:-2000}"
DRILL_MARGIN_PCT="${KS_DRILL_MARGIN_PCT:-2}"
DRILL_IMAGE="${KS_DRILL_IMAGE:-postgres:17.2-alpine}"

drill_from_remote() {
    DRILL_NOTIFY=1
    # Everything here either ends in `fail` or is a command whose failure
    # would otherwise end the shell silently under `set -e` — a gpg that is
    # not installed, a `docker` that is not. The trap turns that class into
    # the same alert as an explicit refusal, which is the only difference a
    # reader of the message cares about.
    trap 'fail "unexpected error at line $LINENO"' ERR

    CONFIG="deploy/backup.env"
    # shellcheck source=/dev/null
    [ -f "$CONFIG" ] && . "$CONFIG"
    # shellcheck source=/dev/null
    source deploy/pg_offsite_lib.sh

    [ -n "$REMOTE" ] || fail "BACKUP_REMOTE is not set — there is no off-site copy to drill"
    [ -n "$ENV_PASSFILE" ] && [ -r "$ENV_PASSFILE" ] \
        || fail "BACKUP_ENV_PASSFILE is unset or unreadable — the copies are encrypted (OD-01)"

    local scratch container stamp manifest enc_dump enc_roles want got
    scratch="$(mktemp -d)"
    container="ks-pg-remote-drill-$$"
    # -v, always: this image mints an anonymous volume for its pgdata on every
    # run, and 173 of them on this host once took the disk watchdog to
    # CRITICAL. The bound belongs to whoever creates the container.
    #
    # EXIT rather than RETURN, which is the trap this wanted to be and would
    # have leaked a container every time: `fail` exits the shell, so a RETURN
    # trap on this function would only fire on the path where nothing went
    # wrong — the one path that does not need cleaning up after.
    # shellcheck disable=SC2064
    trap "docker rm -f -v '$container' >/dev/null 2>&1 || true; rm -rf '$scratch'" EXIT

    echo "── the copy under test ──────────────────────────────────"
    stamp="$(remote_stamps | head -1 || true)"
    [ -n "$stamp" ] || fail "nothing shipped to $PG_REMOTE_DIR yet — run deploy/pg_offsite.sh first"
    manifest="ks-$stamp.sha256"
    enc_dump="ks-$stamp.dump.gpg"
    enc_roles="roles-$stamp.sql.gpg"
    remote_get "$manifest" "$scratch/$manifest" || fail "could not fetch $manifest"
    remote_get "$enc_dump" "$scratch/$enc_dump" || fail "could not fetch $enc_dump"
    echo "  $stamp  ($(( $(_size "$scratch/$enc_dump") / 1024 )) KB encrypted)"

    echo "── verify and decrypt ───────────────────────────────────"
    # Before decrypting, not after: a corrupted download that gpg happens to
    # accept would otherwise be restored and counted, and the drill would
    # report on bytes nobody proved were the bytes that were sent.
    want="$(awk -v n="$enc_dump" '$2 == n {print $1}' "$scratch/$manifest")"
    got="$(_sha256 "$scratch/$enc_dump")"
    [ -n "$want" ] || fail "$manifest does not mention $enc_dump"
    [ "$want" = "$got" ] || fail "sha256 mismatch on $enc_dump: manifest $want, downloaded $got"
    gpg --batch --yes --quiet --passphrase-file "$ENV_PASSFILE" \
        -o "$scratch/ks.dump" -d "$scratch/$enc_dump" \
        || fail "could not decrypt $enc_dump with BACKUP_ENV_PASSFILE"
    [ -s "$scratch/ks.dump" ] || fail "decryption produced an empty file"
    echo "  sha256 matches, decrypted $(( $(_size "$scratch/ks.dump") / 1024 )) KB"

    # The roles file rides along in the same shipment, so the drill loads it:
    # a dump restored without its roles is a cluster with tables nobody can
    # log in to read, and finding that out during a recovery is too late.
    if remote_get "$enc_roles" "$scratch/$enc_roles" 2>/dev/null; then
        want="$(awk -v n="$enc_roles" '$2 == n {print $1}' "$scratch/$manifest")"
        got="$(_sha256 "$scratch/$enc_roles")"
        [ "$want" = "$got" ] || fail "sha256 mismatch on $enc_roles"
        gpg --batch --yes --quiet --passphrase-file "$ENV_PASSFILE" \
            -o "$scratch/roles.sql" -d "$scratch/$enc_roles" \
            || fail "could not decrypt $enc_roles"
    else
        fail "$enc_roles is not off-site beside the dump"
    fi

    echo "── restore into a throwaway cluster ─────────────────────"
    # --network none: it cannot reach the live cluster even by accident, and
    # nothing here should want to. Everything travels over `docker exec` stdin
    # rather than a mount, so the decrypted dump never has to be made readable
    # to anyone but root on this host.
    docker run -d --name "$container" --network none \
        -e POSTGRES_PASSWORD=drill -e POSTGRES_DB=ks "$DRILL_IMAGE" >/dev/null \
        || fail "could not start $DRILL_IMAGE"
    local ready=""
    for _ in $(seq 1 60); do
        if docker exec "$container" pg_isready -U postgres -q 2>/dev/null; then ready=1; break; fi
        sleep 2
    done
    [ -n "$ready" ] || fail "the throwaway cluster never accepted connections"

    docker exec -i "$container" psql -U postgres -d ks -q >/dev/null 2>&1 \
        < "$scratch/roles.sql" || echo "  note: some roles did not apply (the source cluster's own are expected to)"
    # pg_restore's exit code is deliberately not the verdict. A logical dump
    # restored into a fresh cluster routinely complains about objects that
    # belong to the source installation, and a drill that died on those would
    # tell you nothing about whether the data came back. The row counts below
    # are the verdict; the error count is context for reading them.
    local restore_log errors
    restore_log="$scratch/pg_restore.log"
    docker exec -i "$container" pg_restore -U postgres -d ks --no-owner \
        > "$restore_log" 2>&1 < "$scratch/ks.dump" || true
    errors="$(grep -c '^pg_restore: error' "$restore_log" 2>/dev/null || true)"
    echo "  restored, ${errors:-0} pg_restore error line(s)"

    echo "── restored against live ────────────────────────────────"
    echo "  rule: restored <= live within max(${DRILL_MARGIN_ROWS} rows, ${DRILL_MARGIN_PCT}% of live)"
    local entry table direction live restored gap allow verdict bad=0
    for entry in "${DRILL_TABLES[@]}"; do
        table="${entry%%:*}"; direction="${entry##*:}"
        restored="$(docker exec "$container" psql -U postgres -d ks -tAc \
            "SELECT count(*) FROM $table" 2>/dev/null || echo ERR)"
        # ks_readonly, never ks_app and never the superuser: the drill reads
        # live and must not be able to write to it even by a typo.
        live="$(docker exec -i ks-postgres psql -U ks_readonly -d ks -tAc \
            "SELECT count(*) FROM $table" 2>/dev/null || echo ERR)"
        if [ "$restored" = ERR ] || [ "$live" = ERR ]; then
            printf '  %-26s %s\n' "$table" "UNREADABLE (restored=$restored live=$live)"
            bad=1
            continue
        fi
        gap=$(( live - restored ))
        allow=$(( live * DRILL_MARGIN_PCT / 100 ))
        [ "$allow" -lt "$DRILL_MARGIN_ROWS" ] && allow="$DRILL_MARGIN_ROWS"
        verdict=ok
        if [ "$gap" -lt 0 ] && [ "$direction" = grows ]; then
            # Not lag: the dump holds rows live no longer does, and all three
            # of these tables only ever gain rows.
            verdict="ROWS MISSING FROM LIVE"
        elif [ "${gap#-}" -gt "$allow" ]; then
            verdict="GAP TOO LARGE (allowed $allow)"
        fi
        printf '  %-26s restored %-9s live %-9s gap %-8s %s\n' \
            "$table" "$restored" "$live" "$gap" "$verdict"
        [ "$verdict" = ok ] || bad=1
        if [ "$live" = 0 ]; then
            echo "      note: live holds no rows, so this comparison proves nothing yet"
        fi
    done
    [ "$bad" -eq 0 ] || fail "the off-site copy does not match live within the stated margin"

    echo
    echo "PASS — the copy off this machine restores, and its rows agree with live."
}

for arg in "$@"; do
    if [ "$arg" = "--from-remote" ]; then
        drill_from_remote
        exit 0
    fi
done

mkdir -p backups/postgres

echo "── postgres ─────────────────────────────────────────────"
PGC "DROP TABLE IF EXISTS meta._drill_canary" >/dev/null
PGC "CREATE TABLE meta._drill_canary AS
     SELECT g AS id, md5(g::text) AS payload FROM generate_series(1, $ROWS) g" >/dev/null
before_pg="$(PGC "SELECT count(*), sum(id) FROM meta._drill_canary")"
echo "  canary: $before_pg"

s=$(t0)
docker compose exec -T postgres pg_dump -U postgres -d ks -Fc \
    > "backups/postgres/_drill-$STAMP.dump"
e=$(t0); dump_ms=$(ms "$s" "$e")

PGC "DROP TABLE meta._drill_canary" >/dev/null
PGC "SELECT to_regclass('meta._drill_canary')" | grep -q '^$' \
    || fail "canary survived the drop; the drill would prove nothing"

s=$(t0)
docker compose exec -T postgres pg_restore -U postgres -d ks --no-owner \
    -t _drill_canary -n meta < "backups/postgres/_drill-$STAMP.dump" >/dev/null
e=$(t0); restore_ms=$(ms "$s" "$e")

after_pg="$(PGC "SELECT count(*), sum(id) FROM meta._drill_canary")"
[ "$before_pg" = "$after_pg" ] || fail "restored $after_pg, expected $before_pg"
echo "  dump ${dump_ms} ms, restore ${restore_ms} ms, checksum matches"

PGC "DROP TABLE meta._drill_canary" >/dev/null
rm -f "backups/postgres/_drill-$STAMP.dump"

echo
echo "PASS — Postgres lost $ROWS rows and got them back, byte for byte."
