# Preflight for step 05

Everything else standing between here and the first landing write to Postgres
was closed in code on 2026-08-23 (#123–#128). Two things could not be: one
reads the production warehouse, the other writes a file to the server's disk.

**Both were done on 2026-08-23.** What follows is how, and how to do them
again — the second Ark is still owed, in the deploy that removes DuckDB.

---

## The rule that governs every command here

**Never open the live `analytics.duckdb`.** DuckDB is single-writer: a second
connection either fails or, worse, a byte copy taken while the application is
writing produces a torn file that reads exactly like corruption. Every read
below goes to a **backup**, mounted `:ro` so a mistake cannot become a write.

The one command that mounts `data/` read-write is the Ark, because it has to
create `data/ark/`. It still reads only the backup.

### And do not paste a heredoc

The first version of this runbook told you to. The terminal indented the
closing `PY`, which a `<<'PY'` heredoc never terminates on, so the shell sat
waiting and printed nothing — which looks exactly like a query that returned
no rows. Ship a file instead.

---

## A. Is the reconciliation actually running?

`dq_reconciliation` is the instrument the owner's acceptance criterion stands
on: zero discrepancies **against KeyCRM**. A criterion whose instrument does
not run is not a criterion, and this one failed 57 of 84 runs on
`429 Too Many Attempts`.

```bash
scp deploy/dq_history.py root@<host>:/tmp/dq.py

ssh root@<host> 'docker run --rm \
  -v /opt/key-api-bot/data:/data:ro \
  -v /tmp/dq.py:/dq.py:ro \
  halloweex/keycrm-web:latest python3 /dq.py'
```

It reads the newest backup and prints three sections: reconciliation runs by
month, every run in the last 30 days with its error, and any findings from the
landing→Silver arc.

### Answered 2026-08-23 — closed, with a named cause

```
== reconciliation runs by month ==
  2026-05  runs   9  ok   0  429   9
  2026-06  runs  26  ok   1  429  25
  2026-07  runs  27  ok   6  429  21
  2026-08  runs  23  ok  21  429   2
```

Both August failures are **2026-08-05 and 2026-08-06**. The last 429 in the
whole history is 2026-08-06, and every run since — seventeen of them — carries
a verdict.

It did not stop on its own, which is what this runbook originally assumed and
said out loud. It was fixed by **`6a9b82b` (#37), 2026-08-07**, and the fix
names the failure in `core/keycrm.py:247`:

> 429 and 5xx are transient. Raised as `KeyCRMAPIError` they were not in
> `retryable_exceptions`, so a single rate-limit reply anywhere in a long
> pagination aborted the whole job — this is what killed most reconciliation
> runs.

The errors still in the table are the old exception type, from before the fix.
A cause, a dated commit, and seventeen clean runs after it is a stronger
answer than any calendar window.

### If you are reading this later and the numbers have moved

* **Any `429` after 2026-08-07** — the retry path regressed. Look at
  `_retry_after_seconds` and at what `retryable_exceptions` contains, not at
  the size of the retry budget.
* **Far fewer than ~30 runs in 30 days** — the job is not firing daily, a
  different fault with the same consequence. `docker compose logs --tail=200
  web | grep -i reconcil`.
* `data_quality_runs` survives the weekly compaction — it is not in
  `DERIVED_TABLES` — so the history really is there to read.

---

## B. The Ark

A frozen copy of the warehouse as it was before Postgres ever held a landing
row. Taken **twice in the whole migration**, never rotated: once before the
first landing write, once in the deploy that removes DuckDB.

It is not a backup. Backups rotate — `keep=2` for the file — and rotation is
deletion on a schedule. This is the opposite requirement.

### B1. Freeze

```bash
cd /opt/key-api-bot
BK="$(basename "$(ls -t data/backups/analytics-*.duckdb | head -1)")"
ls -lh "data/backups/$BK"      # check the age; the nightly runs 04:30 Kyiv
```

If that is older than you want the Ark to be, take a fresh backup first. It
holds the store lock for the duration of the copy, so do it off peak — and
note `keep=2` prunes the oldest as it goes:

```bash
docker compose exec web python3 -c "
import asyncio
from core.duckdb_store import get_store
async def main():
    s = await get_store(); print(await s.backup_database(keep=2))
asyncio.run(main())
"
```

Then freeze. `deploy/` is **not** copied into the web image, so the script is
mounted from the host checkout:

```bash
docker run --rm \
  -v /opt/key-api-bot/data:/data \
  -v /opt/key-api-bot/deploy:/deploy:ro \
  halloweex/keycrm-web:latest \
  python3 /deploy/ark_freeze.py --source "/data/backups/$BK" --out /data/ark
```

The last lines print the stamp. **Write it down.**

### B2. Verify — not optional

The Ark's whole claim is that it can be read *without the application*. This
is the only thing that tests that claim.

```bash
STAMP=<paste it>
docker run --rm \
  -v /opt/key-api-bot/data:/data \
  -v /opt/key-api-bot/deploy:/deploy:ro \
  halloweex/keycrm-web:latest \
  python3 /deploy/ark_freeze.py --verify "/data/ark/$STAMP"
```

All three levels must say OK:

| level | what it proves |
|---|---|
| L0 | checksums match the manifest — the files are intact |
| L1 | the byte copy opens and its row counts match |
| L2 | `schema.sql` + parquet replay into a **fresh** database |

**If L2 fails, stop.** L0 and L1 passing means you have a copy of a file. Only
L2 means you have something readable in five years, after the format and this
codebase have both moved on.

### B3. Make it immutable

```bash
chattr +i -R "/opt/key-api-bot/data/ark/$STAMP"
lsattr -d "/opt/key-api-bot/data/ark/$STAMP"   # expect ----i---------
```

Nothing in this repository prunes `data/ark` — `backup_database` touches only
`analytics-*.duckdb`, the compaction only `analytics_clean` and
`export_parquet`, the off-site job only its own work directory. `chattr +i` is
belt to that braces, and it also survives a careless `rm -rf data/*`.

Undo with `chattr -i -R`.

### B4. Get it off this disk — DONE 2026-08-24

A copy on the same disk survives a deletion and does not survive the disk. The
off-site job ships `ks-warehouse-*.tar` and will **not** pick this up.

```bash
STAMP=20260823T212918Z
cd /opt/key-api-bot && . deploy/backup.env
SSH_OPTS=(-p "${BACKUP_SSH_PORT:-23}" -i "$BACKUP_SSH_KEY" \
          -o BatchMode=yes -o StrictHostKeyChecking=accept-new)

tar -C data/ark -czf "/tmp/ark-$STAMP.tar.gz" "$STAMP"
sha256sum "/tmp/ark-$STAMP.tar.gz"

# `ark/`, not the archive directory itself. Nothing there is pruned today —
# `deploy/offsite_parquet.sh` selects victims with
# `grep -o 'ks-warehouse-[0-9]\{8\}-[0-9]\{6\}\.tar'`, which cannot match
# this name — but relying on somebody never widening that regex is not a plan.
printf 'cd %s\n-mkdir ark\n' "$BACKUP_REMOTE_DIR" \
    | sftp -b - -P "${BACKUP_SSH_PORT:-23}" -i "$BACKUP_SSH_KEY" "$BACKUP_REMOTE"
rsync --archive --partial -e "ssh ${SSH_OPTS[*]}" \
    "/tmp/ark-$STAMP.tar.gz" "$BACKUP_REMOTE:${BACKUP_REMOTE_DIR}/ark/"

# Prove it, then delete the tarballs. A size match is not a read.
rsync --archive -e "ssh ${SSH_OPTS[*]}" \
    "$BACKUP_REMOTE:${BACKUP_REMOTE_DIR}/ark/ark-$STAMP.tar.gz" /tmp/ark-rt.tar.gz
sha256sum "/tmp/ark-$STAMP.tar.gz" /tmp/ark-rt.tar.gz   # must agree
tar -tzf /tmp/ark-rt.tar.gz | wc -l                     # 64
rm -f "/tmp/ark-$STAMP.tar.gz" /tmp/ark-rt.tar.gz
```

**Result, 2026-08-24.** 571 MB of Ark compressed to **208 642 742 bytes**;
shipped, pulled back, and both copies hash to
`66b4065704936def5a9c3852a29a5367d4741ba315d1420231c90bb441c820a6`. The
returned archive opens and lists its 64 entries, so what is over there is a
readable file and not a length that matches. Local tarballs deleted; the Ark
itself stays on disk under `chattr +i`, which is now the *second* copy rather
than the only one.

**What this does and does not buy.** It survives this disk, this filesystem and
this host. It does not survive the provider: the Storage Box is Hetzner, same
as the VPS, so one account-level event still takes both. Naming that is not a
plan to fix it — for a frozen pre-migration snapshot the disk was the risk
worth closing — but nobody should read "off-site" as more than it is.

**Retention.** Nothing prunes it, here or there. That is deliberate: this is
one cold artifact taken once, not a rotation, and the day it needs deleting is
the day the migration is finished and somebody decides so on purpose.

### The first Ark, 2026-08-23

```
/opt/key-api-bot/data/ark/20260823T212918Z
source  analytics-20260823-043000.duckdb  (556 MB)
result  45 tables, 12 views, 570 MB
verify  L0 59 files 0 bad · L1 45 tables 0 mismatched · L2 45 replayed 0 mismatched
        VERIFY OK — the archive is readable without the application
chattr  ----i---------
```

B4 done 2026-08-24 — see above. The Ark now exists twice.

---

## C. What the landing→Silver arc found

`dq_history.py` prints this in its third section.

**Mind the backup's age.** The arc check runs at 01, 07, 13 and 19 Kyiv; the
nightly backup is taken at 04:30, so a backup only ever holds runs older than
itself. On the day the check shipped, the backup predated it entirely and the
script printed "the arc is clean" — when what it meant was "this file is older
than the check". Absence of findings in a backup taken before the first run is
not evidence of anything, and reading it as evidence is the mistake this
paragraph exists to stop.

`silver_missing_rows` is CRITICAL and also sends a Telegram alert.
`silver_orphan_rows` and `silver_row_values` are WARN and reach a human
through the 09:00 digest. Value drift is the interesting one: it says how many
Silver rows disagree with what the projection produces today — which also
answers whether #101's `is_active_source` fix ever reached the whole history.

---

## When this is done

Step 05 can start writing. The order and the shape are settled — see the
artifact *«Двойная запись и сверка»* and §7a of
`.planning/POSTGRES_SESSION_HANDOFF.md`. The first table is not `orders`: it is
`categories` and `products`, cheap and recoverable, so the mechanism is proved
on something whose loss costs one resync.
