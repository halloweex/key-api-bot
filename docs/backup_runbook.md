# Backup & Restore Runbook (A9-1)

Two databases leave this machine, and they leave by different routes. The
warehouse is below; **Postgres is the one that cannot be rebuilt from
anything**, so start with it if you are reading this during a recovery.

## Postgres off-site (DN-09)

`app.order_versions` is the only record that an order ever changed status,
`app.stock_movements` the only record that a quantity ever did, and beside
them sit the dashboard access list and the SMS roster. KeyCRM cannot supply
any of it. Until 2026-09-18 none of it left the machine at all:
`deploy/pg_backup.sh` keeps fourteen dumps on the same volume as the cluster
they copy, so any failure that takes the volume takes all fifteen.

`deploy/pg_offsite.sh` ships the newest dump nightly at 07:40 to the same
Storage Box as the Parquet archive, into its own directory:

| file | what it is |
|---|---|
| `key-api-bot/postgres/ks-<stamp>.dump.gpg` | the `pg_dump -Fc` dump, encrypted |
| `key-api-bot/postgres/roles-<stamp>.sql.gpg` | the roles file from the same run |
| `key-api-bot/postgres/ks-<stamp>.sha256` | sha256 of both, **in the clear** |

Encryption is mandatory and the shipper refuses without it (OD-01, decided
17.09.2026): these files carry ~6 000 customer phone numbers and every
dashboard account, and the box is a sub-account on the same provider as the
VPS. The passphrase is the one already escrowed for `env.gpg` —
`BACKUP_ENV_PASSFILE`, `/root/.backup_pass` on the server, and a copy of it
somewhere that is not this box. The manifest is deliberately *not* encrypted:
a verification that needs the passphrase could only ever run from the machine
whose loss this is about.

**To open a copy** (any machine with the passphrase):

```bash
sftp -P 23 -i ~/.ssh/storagebox_ed25519 <sub-account>  # ls key-api-bot/postgres
# then, for the stamp you fetched:
sha256sum -c ks-<stamp>.sha256
gpg --batch --quiet --passphrase-file /root/.backup_pass -o ks.dump -d ks-<stamp>.dump.gpg
gpg --batch --quiet --passphrase-file /root/.backup_pass -o roles.sql -d roles-<stamp>.sql.gpg
psql -U postgres -d postgres -f roles.sql      # roles first, or nobody can read the tables
pg_restore -U postgres -d ks --no-owner ks.dump
```

**The rehearsal is `deploy/pg_restore_drill.sh --from-remote`**, Mondays at
08:20. It does exactly the above into a throwaway `postgres:17.2-alpine` and
counts four tables against live, and it alerts on failure under
`backup:pg_offsite_drill_failed`. It is the only instrument that would notice
a rotated passphrase: the nightly shipment verifies by hashing *ciphertext*,
so a copy nobody can open still hashes correctly.

**The alarm** is `deploy/offsite_check.sh`, which reads
`data/.pg_offsite_last_ok` — written only after the uploaded bytes have been
fetched back, hashed and decrypted — and alerts above 36 h. Every run that
refused says which step refused, in `data/logs/pg_offsite.log`.

## What is backed up (the warehouse)
`data/analytics.duckdb` — the warehouse. It holds data that is **NOT recoverable
from KeyCRM** and would be permanently lost if the file is lost.

This list used to name six low-volume settings tables — RBAC, goals, hand-entered
expenses, anti-duplicate state. Audited, most of them turned out to be empty,
and the nightly backup had been reporting success against them without anyone
learning that, because the validator asserts only `SELECT COUNT(*) FROM orders > 0`.

Meanwhile the data that genuinely cannot be re-fetched was not on the list at
all. It is observational — produced by watching over time, so no API call
reproduces it:

| table | why it is gone forever |
|---|---|
| `inventory_sku_history` | daily per-SKU stock snapshots; the API returns *current* stock only |
| `stock_movements` | deltas observed between two polls — not a KeyCRM field |
| `sms_campaign_members` | target/holdout assignment; the only copy of a campaign's control group |
| `marketing_optouts` | people who withdrew consent. The failure mode is not data loss but contacting them again |
| `warehouse_refreshes`, `data_quality_*`, `reconciliation_log` | the audit trail proving the figures were checked |
| `users`, `role_permissions` | dashboard RBAC |

Which is why the off-site archive draws its scope from `DERIVED_TABLES`, a
constant the code maintains and the compact validates, rather than from a list
in a markdown file. **A backup scoped by prose rots. This one had.**

Bronze/Silver/Gold are rebuildable from KeyCRM, but a full re-sync costs
thousands of API calls — dominated by one request per buyer — and is the third
line of defence, not the second.

## How the backup works
A daily scheduler job (`db_backup`, 04:30 Europe/Kyiv) calls
`DuckDBStore.backup_database(keep=2)`:
1. Acquires the store lock + `CHECKPOINT` (folds the WAL into the main file).
2. Copies the file **while the lock is held** → byte-consistent snapshot (no
   concurrent writer). Copy runs in the executor so the event loop isn't blocked.
3. Validates the copy read-only (`SELECT COUNT(*) FROM orders` > 0).
4. Atomically renames into `data/backups/analytics-YYYYmmdd-HHMMSS.duckdb`.
5. Prunes to the newest 2. Disk-space guarded (needs ~1.1× the DB size free);
   aborts + Telegram-alerts otherwise. Any failure alerts admins.

Trade-off: the lock is held for the copy (tens of seconds on a multi-GB DB) — that
is why it runs at 04:30. RPO ≈ 24h, RTO = file copy + container restart.

## Off-host copy
The job above writes to the **same volume** as the live DB, so on its own it
protects against logical corruption and nothing else: any failure that takes the
volume takes every copy with it, however many are retained.

This section used to ask for off-host replication as an "owner action". It is
now a script instead, because a requirement whose enforcement mechanism is that
someone remembers is not an enforcement mechanism — and a runbook line, unlike a
cron entry, cannot tell you whether it ran.

**Setup: `docs/offsite_backup_setup.md`.** In short — the weekly compact calls
`deploy/offsite_parquet.sh`, which ships `data/export_parquet/` (the whole
non-derived warehouse as ZSTD Parquet, plus the encrypted `.env`) to a Hetzner
Storage Box and prunes to `BACKUP_RETAIN` archives. At this size off-site
retention is close to free — a year of weekly archives is a rounding error on
the smallest box they sell — so be generous with it. The retention that costs
anything is the one on the server.

- **RPO 7 days** for the off-host copy, since it rides the weekly compact.
  Daily is a scheduling change once the backup object itself moves to Parquet.
- **Restore drill: `deploy/restore-test.sh`** — pulls the newest archive back
  from the Storage Box and rebuilds a validated warehouse in a throwaway
  container. Quarterly. An untested backup is a hypothesis.
- **Staleness: `deploy/offsite_check.sh`** in the host crontab, daily. Alerts if
  no copy has left in 36 h — this line said "9 days" until 2026-09-18, a
  threshold the script stopped using when the push moved off the weekly
  compact to `deploy/daily_offsite.sh`. It lives in host cron rather than the app
  scheduler because an in-process check cannot attest to its own liveness: a
  monitor that has stopped running and one with nothing to report emit the same
  silence, and this codebase has already been on the wrong side of that.

Until `deploy/backup.env` exists on the server the push exits `EX_UNCONFIGURED`
and says so, in those words — "the export is fine, shipping is not set up" is a
different sentence from "the backup failed". Keeping them apart is the whole
reason the alert stays worth reading; a recurring message that overstates its
own severity teaches the reader to scroll past it.

## Restore (the warehouse; Postgres is in its own section at the top)
```bash
ssh root@<vps>
cd /opt/key-api-bot
docker compose stop web bot          # release the single-writer lock
cp data/analytics.duckdb data/analytics.duckdb.broken    # keep the bad one
cp data/backups/analytics-<stamp>.duckdb data/analytics.duckdb
rm -f data/analytics.duckdb.wal      # stale WAL from the broken file
docker compose up -d web bot
# verify
curl -s localhost/api/health | jq '.status'
```
On startup the warehouse re-validates and the model re-trains. If only Bronze is
intact but Silver/Gold are suspect, trigger a full refresh instead of restoring.
