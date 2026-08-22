#!/bin/sh
# Let the WAL receiver in. Idempotent, and runnable against a live server.
#
# The official image writes a pg_hba.conf that covers ordinary connections and
# says nothing about replication ones. `pg_receivewal` opens a replication
# connection, so it is refused with "no pg_hba.conf entry for replication
# connection" — a message that names the file rather than the missing line, and
# arrives only at run time, in a restart loop.
#
# pg_hba is not a GUC: there is no ALTER SYSTEM for it, and initdb scripts run
# only on an empty data directory. So this script has to work in both worlds —
# dropped into docker-entrypoint-initdb.d for a fresh volume, and run by hand
# against a cluster that already exists:
#
#     docker compose exec -T postgres sh < postgres/initdb/20-replication-hba.sh
#
# Scoped to `postgres` because that is the only account with REPLICATION, and
# to the container network because rule 12 keeps 5432 off the host entirely.
set -eu

HBA="${PGDATA:-/var/lib/postgresql/data}/pg_hba.conf"
LINE="host    replication     postgres        all                     scram-sha-256"

if grep -qE '^[[:space:]]*host[[:space:]]+replication[[:space:]]+postgres' "$HBA"; then
    echo "pg_hba: replication entry already present"
else
    printf '\n# Added by 20-replication-hba.sh — the WAL receiver (PITR).\n%s\n' "$LINE" >> "$HBA"
    echo "pg_hba: replication entry added"
fi

# Reload rather than restart: pg_hba is re-read on SIGHUP, and a restart here
# would be an outage bought for nothing.
if pg_isready -q 2>/dev/null; then
    psql -U "${POSTGRES_USER:-postgres}" -d postgres -Atc 'SELECT pg_reload_conf()' >/dev/null
    echo "pg_hba: reloaded"

    # The slot is created here, not by `pg_receivewal --create-slot`, because
    # that flag is a one-shot action: it makes the slot and *exits zero*. Under
    # `restart: unless-stopped` that is a container which starts, succeeds,
    # stops, and starts again — a restart loop that reports success and streams
    # nothing. Creating it once here leaves the receiver with a single job.
    psql -U "${POSTGRES_USER:-postgres}" -d postgres -Atc \
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'ks_pitr'" \
        | grep -q 1 \
      && echo "slot: ks_pitr already present" \
      || {
        psql -U "${POSTGRES_USER:-postgres}" -d postgres -Atc \
            "SELECT pg_create_physical_replication_slot('ks_pitr', true)" >/dev/null
        echo "slot: ks_pitr created"
      }
fi
