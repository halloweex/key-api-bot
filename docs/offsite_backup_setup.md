# Off-site backup setup

One-time, on the server. The daily backup writes to the same volume as the
database it copies, so on its own it defends against logical corruption and
nothing else — any failure that takes the volume takes the copies with it.
This is the step that fixes that.

## What gets shipped, and why it is small

`scripts/compact_duckdb.py::phase1_export` already writes `data/export_parquet/`
on every weekly compact: each non-derived table as ZSTD Parquet plus a
`_manifest.json` of row counts, sequence values and business checksums. It is a
small fraction of the `.duckdb` file it came from, because the overwhelming
majority of that file is derived tables (`gold_daily_*`, `silver_*`) which the
app rebuilds from bronze in seconds.

So the archive is not a compromise — it is the whole irreplaceable asset,
including tables the old runbook never named: `inventory_sku_history` (daily
stock snapshots; the KeyCRM API returns *current* stock only),
`stock_movements` (deltas observed between two polls, not a KeyCRM field),
`sms_campaign_members` (the only copy of a campaign's control assignment) and
`marketing_optouts` (people who withdrew consent — the failure mode if it is
lost is not data loss but contacting them again).

Scope is drawn by `DERIVED_TABLES`, a constant the code maintains and the
compact validates, rather than by a list in a markdown file. A prose list rots,
and that one had: it named a handful of low-volume settings tables while the
observational data that genuinely cannot be re-fetched went unmentioned.

## 1. Storage Box

Hetzner console → Storage Box → order **BX11** (1 TB). Then in its settings:

- enable **SSH support** (rsync/sftp over SSH on port **23**, not 22)
- Sub-accounts → create one with **Read/Write** and its own directory

## 2. Key

Passphrase-less, so cron can use it unattended. The sub-account is scoped to a
single directory, so the blast radius is that directory.

```bash
ssh-keygen -t ed25519 -N '' -f /root/.ssh/storagebox_ed25519 \
    -C "ks-backup@$(hostname)"
```

The main account takes its public key in the console. **A sub-account has no
such field** — its key is a file you write into its home directory, using the
main account's own sftp access:

```bash
printf -- '-mkdir key-api-bot/.ssh
put /root/.ssh/storagebox_ed25519.pub key-api-bot/.ssh/authorized_keys
chmod 700 key-api-bot/.ssh
chmod 600 key-api-bot/.ssh/authorized_keys
' | sftp -b - -P 23 -i /root/.ssh/storagebox_ed25519 \
        -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
        uXXXXXX@uXXXXXX.your-storagebox.de
```

Four things there are load-bearing:

- **`-b -`** — batch mode, commands on stdin. A Storage Box has no shell, so
  `ssh host 'command'` cannot work at all; everything goes through sftp.
- **`-P 23`, capital.** Lowercase `-p` means *preserve permissions* to sftp,
  and the `23` after it is then read as a hostname. This is the bug that shipped
  an archive and then silently failed to prune it.
- **the `-` before `mkdir`** — ignore failure of that one command. Without it a
  re-run stops on "directory exists" and never writes the key.
- **relative paths.** The main account lands in `/home`, so `key-api-bot/.ssh`
  is the sub-account's home. The sub-account itself lands *inside* that
  directory, which is why its `BACKUP_REMOTE_DIR` is `.` and not the folder name.

Then verify — and verify like cron will, not like a human would:

```bash
printf 'pwd\nls -1\n' | sftp -b - -P 23 -i /root/.ssh/storagebox_ed25519 \
    -o BatchMode=yes \
    uXXXXXX-subN@uXXXXXX-subN.your-storagebox.de
```

`BatchMode=yes` forbids falling back to a password. An interactive login proves
nothing here: it will prompt, you will type the password, and it will look
configured while cron still cannot get in. Note the host is the sub-account's
own — the main domain refuses that login.

## 3. Passphrase for the config copy

The archive carries `.env` encrypted, because a restored warehouse with no
config is a database nothing can start against — and the host's `.env` and the
laptop's have already drifted apart, each holding keys the other lacks, so
neither is a complete recovery source on its own.

```bash
head -c 32 /dev/urandom | base64 > /root/.backup_pass
chmod 600 /root/.backup_pass
```

**Keep a copy of that passphrase somewhere that is not this machine.** It is the
one secret that cannot ride along with the thing it decrypts.

## 4. Config

```bash
cd /opt/key-api-bot
cp deploy/backup.env.example deploy/backup.env
$EDITOR deploy/backup.env      # BACKUP_REMOTE, BACKUP_SSH_KEY, BACKUP_ENV_PASSFILE
```

`deploy/backup.env` is git-ignored, and that is what keeps it: the deploy runs
`git reset --hard <sha>`, so tracked files are replaced on every release and
ignored ones survive.

## 5. Prove it

```bash
deploy/offsite_parquet.sh     # ships one archive now, ~8 MB
deploy/restore-test.sh        # pulls it BACK and rebuilds a validated warehouse
```

`restore-test.sh` deliberately restores from the Storage Box rather than the
local export: the copy worth testing is the one that survives losing this
server. It runs the compaction script's own `phase2_import` + `phase3_validate`
— row counts against the manifest, orders date range, total revenue to within
₴1, primary-key uniqueness on nine tables — inside a throwaway container, and
never calls the swap. Nothing canonical is touched.

## 6. Staleness check

```bash
crontab -e
# add:
0 9 * * *  /opt/key-api-bot/deploy/offsite_check.sh
# and, for the Postgres half (DN-09) — the shipment, then its weekly rehearsal:
40 7 * * *  /opt/key-api-bot/deploy/pg_offsite.sh
20 8 * * 1  /opt/key-api-bot/deploy/pg_restore_drill.sh --from-remote
```

07:40 and 08:20 are the two gaps in this host's morning: `pg_backup.sh` is at
06:50 and finishes within a couple of minutes, the local restore drill is at
07:20, the base backup at 08:10, the PITR drill at 08:40, and `offsite_check`
speaks at 09:00 — so a shipment or a drill that failed is already an
80-minute-old fact by the time the morning's verdicts are read. Nothing here
is near 02:00 Sunday, when the compaction stops the containers.

Both new entries alert on their own through `deploy/notify.sh`
(`backup:pg_offsite_failed`, `backup:pg_offsite_drill_failed`), so neither
depends on anybody reading root's local mail. The drill takes `--quiet` when
you are iterating on it at a keyboard; cron runs without it.

`offsite_check.sh` reads the marker `data/.offsite_last_ok` and alerts if no
copy has left in 36 h, and since DN-09 also reads
`data/.pg_offsite_last_ok` on the same threshold. Its three checks each record
a verdict and the run continues: on a host where the 07:40 cron is not
installed yet the Postgres marker is absent every morning, and under the old
fail-fast shape that would have silenced the disk-watchdog check indefinitely.
It runs from the **host** crontab rather than the app scheduler on
purpose: an in-process check cannot attest to its own liveness, and a monitor
that has gone quiet is indistinguishable from one with nothing to report. Host
cron is a separate failure domain, and on this box it has the better record.

## 7. Check the other services on the same host

Any sibling project sharing this machine has the same exposure and needs the
same treatment. Where one already ships a `backup.env.example` and a push
script, configuring it is a five-minute job against the Storage Box you just
created — give it its own `BACKUP_REMOTE_DIR`, then run its backup and its
restore drill once each.

Worth checking specifically: a push script that distinguishes "not configured"
from "failed" will exit quietly in the unconfigured state while still producing
local archives, and local archives look exactly like working backups. Read the
exit code, not the directory listing.

## Restoring for real

Not a drill — the machine is gone and you are rebuilding.

1. Fetch the newest archive from the Storage Box and unpack it.
2. `gpg --decrypt env.gpg > .env` with the passphrase you kept off-box.
3. Check `export_parquet/_deploy.json` for `deploy_sha` and check out that
   commit. Parquet is not self-describing: `phase2_import` rebuilds the schema
   from the application's own DDL, so restoring an old snapshot against newer
   code can silently drop columns the export has and the target does not. The
   column-coverage report inside phase 2 prints exactly which.
4. Run `deploy/restore_from_export.py` against a data directory containing
   `export_parquet/`, then move `analytics_clean.duckdb` into place as
   `analytics.duckdb` and start the stack.
5. Bronze is now current as of the archive. The incremental sync closes the gap
   from KeyCRM on its own; a full rebuild from the API instead costs thousands
   of requests, dominated by one call per buyer, and is the third line of
   defence rather than the second.
6. **Then restore Postgres**, which is a different directory on the same box
   and the half nothing can re-fetch: `key-api-bot/postgres/`, three files per
   stamp, and the roles file goes in before the dump or the restored cluster
   has tables nobody can log in to read. The commands are in
   `docs/backup_runbook.md` under "Postgres off-site"; they are the same ones
   `deploy/pg_restore_drill.sh --from-remote` runs every Monday, so they are
   not being tried for the first time here.
