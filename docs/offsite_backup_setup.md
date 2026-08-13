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

cat /root/.ssh/storagebox_ed25519.pub \
    | ssh -p23 uXXXXXX-sub1@uXXXXXX.your-storagebox.de install-ssh-key
```

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
```

It reads the marker `data/.offsite_last_ok` and alerts if no copy has left in
9 days. It runs from the **host** crontab rather than the app scheduler on
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
