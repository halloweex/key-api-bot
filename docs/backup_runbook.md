# DuckDB Backup & Restore Runbook (A9-1)

## What is backed up
`data/analytics.duckdb` — the warehouse. It holds data that is **NOT recoverable
from KeyCRM** and would be permanently lost if the file is lost:
- `revenue_goals` (team targets)
- `manual_expenses` (ad / salary spend entered by hand)
- `users`, `role_permissions`, `user_preferences` (dashboard RBAC)
- `celebrated_milestones` (anti-duplicate state)

Bronze/Silver/Gold are rebuildable from KeyCRM, but a full re-sync is slow and the
above tables are gone forever without a backup.

## How the backup works
A daily scheduler job (`db_backup`, 04:30 Europe/Kyiv) calls
`DuckDBStore.backup_database(keep=7)`:
1. Acquires the store lock + `CHECKPOINT` (folds the WAL into the main file).
2. Copies the file **while the lock is held** → byte-consistent snapshot (no
   concurrent writer). Copy runs in the executor so the event loop isn't blocked.
3. Validates the copy read-only (`SELECT COUNT(*) FROM orders` > 0).
4. Atomically renames into `data/backups/analytics-YYYYmmdd-HHMMSS.duckdb`.
5. Prunes to the newest 7. Disk-space guarded (needs ~1.1× the DB size free);
   aborts + Telegram-alerts otherwise. Any failure alerts admins.

Trade-off: the lock is held for the copy (tens of seconds on a multi-GB DB) — that
is why it runs at 04:30. RPO ≈ 24h, RTO = file copy + container restart.

## ⚠️ OFF-HOST replication is still required for real DR
The job above writes to the **same Hetzner disk** as the live DB. A disk/host loss
takes both. Wire up an off-host copy of `data/backups/` (owner action):

```bash
# Example: nightly rclone to a Hetzner Storage Box / S3 bucket, after 04:30.
# crontab -e on the host:
45 4 * * *  rclone copy /opt/key-api-bot/data/backups remote:ks-duckdb-backups --max-age 25h
```
Keep ≥14 daily off-host copies. Test a restore quarterly.

## Restore
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
