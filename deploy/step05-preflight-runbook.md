# Preflight for step 05: the two things that must happen on the host

Everything else standing between here and the first landing write to Postgres
was closed in code on 2026-08-23 (#123–#128). These two cannot be: one needs to
read the production warehouse, the other needs to write a file to the server's
disk, and neither is reachable from a laptop.

Run them in this order. **B is the point of no return** — after the first
landing write to Postgres, "what was in DuckDB" stops being reproducible, and
the Ark is the only thing that keeps it.

Everything below was rehearsed before it was written: the queries against a
real warehouse, the Ark against a real database with the real 48-table schema,
and the container invocations in the exact form given here.

---

## Before you start

```bash
cd /opt/key-api-bot
git log --oneline -1          # expect 3.0.105 or later
df -h /                       # the Ark wants ~250 MB; the compaction aborts under 3 GB
docker compose ps -a --format 'table {{.Service}}\t{{.Status}}'
```

`ks-migrate` and `ks-postgres-bootstrap` showing `Exited (0)` is their healthy
steady state, not a fault — they are one-off containers.

### The rule that governs every command here

**Never open the live `analytics.duckdb`.** DuckDB is single-writer: a second
connection either fails or, worse, a byte copy taken while the application is
writing produces a torn file that reads exactly like corruption. Every read
below goes to a **backup**, and the mount is `:ro` so a mistake cannot become a
write.

The one command that mounts `data/` read-write is the Ark, because it has to
create `data/ark/`. It still reads only the backup.

---

## A. Confirm the `429` is really gone

`dq_reconciliation` failed 57 of 84 runs on `429 Too Many Attempts` —
May 0/9, June 1/26, July 6/27, August 20/22. The trend looks fixed, but **no
commit fixed it**: it stopped on its own, which is a different fact and can
stop being true on its own too.

This matters because the owner's acceptance criterion for the migration is
reconciliation against KeyCRM. A criterion whose instrument does not run is not
a criterion.

```bash
cd /opt/key-api-bot
BK="$(basename "$(ls -t data/backups/analytics-*.duckdb | head -1)")"
echo "reading $BK"

docker run --rm -v /opt/key-api-bot/data:/data:ro \
  halloweex/keycrm-web:latest python3 - "$BK" <<'PY'
import sys, duckdb
c = duckdb.connect(":memory:")
c.execute(f"ATTACH '/data/backups/{sys.argv[1]}' AS a (READ_ONLY)"); c.execute("USE a")

print("\n== reconciliation runs by month ==")
for r in c.execute("""
    SELECT strftime(started_at,'%Y-%m') AS month,
           COUNT(*) AS runs,
           COUNT(*) FILTER (WHERE error_message IS NULL AND status <> 'FAILED') AS ok,
           COUNT(*) FILTER (WHERE error_message ILIKE '%429%')                  AS rate_limited,
           COUNT(*) FILTER (WHERE error_message IS NOT NULL
                              AND error_message NOT ILIKE '%429%')              AS other_err
    FROM data_quality_runs WHERE layer='reconciliation'
    GROUP BY 1 ORDER BY 1
""").fetchall():
    print(f"  {r[0]}  runs {r[1]:>3}  ok {r[2]:>3}  429 {r[3]:>3}  other {r[4]:>3}")

print("\n== every run in the last 30 days ==")
rows = c.execute("""
    SELECT strftime(started_at,'%Y-%m-%d %H:%M') AS at, status,
           COALESCE(substr(error_message,1,60),'') AS err
    FROM data_quality_runs
    WHERE layer='reconciliation' AND started_at > now() - INTERVAL '30 days'
    ORDER BY started_at DESC
""").fetchall()
print(f"  {len(rows)} runs")
for r in rows:
    print(f"  {r[0]}  {r[1]:<9} {r[2]}")
PY
```

### Reading the result

* **30 days, every run `PASS`/`WARN`, zero `429`** — closed. The instrument
  works, and step 05's acceptance has something to stand on.
* **Any `429` in the last 30 days** — not closed, and step 05 should not start.
  The fix is a rate limit or a backoff in the reconciliation's KeyCRM calls,
  not a bigger retry budget.
* **Far fewer than ~30 runs** — the job is not firing daily, which is a
  different fault with the same consequence. Check
  `docker compose logs --tail=200 web | grep -i reconcil`.

Note the window: `data_quality_runs` survives the weekly compaction (it is not
in `DERIVED_TABLES`), so 30 days of history really are there. The **backup** is
up to 24 h old, so a run from this morning may not be in it yet.

---

## B. Take the Ark

The frozen copy of the warehouse as it was before Postgres ever held a landing
row. Taken **twice in the whole migration**, never rotated: this one now, and a
second one in the deploy that removes DuckDB.

It is not a backup. Backups rotate — `keep=2` for the file — and rotation is
deletion on a schedule. This is the opposite requirement.

### B1. Freeze

```bash
cd /opt/key-api-bot
BK="$(basename "$(ls -t data/backups/analytics-*.duckdb | head -1)")"
ls -lh "data/backups/$BK"     # check the age: the nightly runs 04:30 Kyiv
```

If that backup is older than you want the Ark to be, take a fresh one first —
it holds the store lock for the duration of the copy, so do it off peak:

```bash
docker compose exec web python3 -c "
import asyncio
from core.duckdb_store import get_store
async def main():
    s = await get_store()
    print(await s.backup_database(keep=2))
asyncio.run(main())
"
BK="$(basename "$(ls -t data/backups/analytics-*.duckdb | head -1)")"
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

Expect roughly, for a 187 MB warehouse:

```
INFO  source  /data/backups/analytics-....duckdb  (187 MB)
INFO  exporting 48 tables and 12 views
INFO  copying the database file
INFO  hashing
OK    48 tables, 12 views, ... MB
OK    ark complete: /data/ark/<STAMP>
```

**Write down `<STAMP>`** — the last lines print it, and the next two steps need
it.

### B2. Verify

Not optional. The Ark's whole claim is that it can be read *without the
application*, and this is the only thing that tests that claim.

```bash
STAMP=<paste it>
docker run --rm \
  -v /opt/key-api-bot/data:/data \
  -v /opt/key-api-bot/deploy:/deploy:ro \
  halloweex/keycrm-web:latest \
  python3 /deploy/ark_freeze.py --verify "/data/ark/$STAMP"
```

Three levels, and all three must say OK:

```
OK  L0 files: NN checked, 0 bad          checksums match the manifest
OK  L1 frozen db: 48 tables, 0 mismatched the byte copy opens and counts match
OK  L2 schema+parquet: 48 tables replayed schema.sql + parquet rebuild into a
                                          fresh database, independent of DuckDB's
                                          own file format and of this codebase
OK  VERIFY OK — the archive is readable without the application
```

**If L2 fails, stop.** L0 and L1 passing means you have a copy of a file; only
L2 means you have something you can still read in five years.

### B3. Make it immutable

```bash
chattr +i -R "/opt/key-api-bot/data/ark/$STAMP"
lsattr -d "/opt/key-api-bot/data/ark/$STAMP"   # expect ----i---------
```

Nothing in this repository prunes `data/ark` — checked: `backup_database`
touches only `analytics-*.duckdb`, the compaction only `analytics_clean` and
`export_parquet`, the off-site job only its own work directory. `chattr +i` is
belt to that braces, and it also stops a careless `rm -rf data/*`.

To undo later: `chattr -i -R <path>`.

### B4. Get it off the disk

A copy on the same disk survives a deletion and does not survive the disk. The
off-site job ships `data/backups/analytics-*.duckdb` and will **not** pick this
up. Ship it once, by hand, wherever `deploy/backup.env` points — or anywhere
else, as long as it is not this machine.

```bash
tar -C /opt/key-api-bot/data/ark -czf "/tmp/ark-$STAMP.tar.gz" "$STAMP"
ls -lh "/tmp/ark-$STAMP.tar.gz"
# then move it off the host, and delete the tarball
```

---

## C. Optional: what the landing→Silver check found

Shipped 2026-08-23 and it has run in production. Its findings reach a human
through the 09:00 digest, but they can be read directly.

The check runs at 01, 07, 13 and 19 Kyiv; the nightly backup is taken at 04:30,
so it holds the 01:00 run.

```bash
cd /opt/key-api-bot
BK="$(basename "$(ls -t data/backups/analytics-*.duckdb | head -1)")"
docker run --rm -v /opt/key-api-bot/data:/data:ro \
  halloweex/keycrm-web:latest python3 - "$BK" <<'PY'
import sys, duckdb
c = duckdb.connect(":memory:")
c.execute(f"ATTACH '/data/backups/{sys.argv[1]}' AS a (READ_ONLY)"); c.execute("USE a")
for r in c.execute("""
    SELECT strftime(r.started_at,'%Y-%m-%d %H:%M') AS at,
           i.check_name, i.severity, i.count, substr(i.description,1,90)
    FROM data_quality_issues i
    JOIN data_quality_runs r USING (run_id)
    WHERE i.check_name LIKE 'silver_%'
    ORDER BY r.started_at DESC LIMIT 20
""").fetchall():
    print(f"  {r[0]}  {r[1]:<22} {r[2]:<8} {r[3]:>6}  {r[4]}")
else:
    print("  (no rows means the arc is clean, which is the expected answer)")
PY
```

`silver_missing_rows` is CRITICAL and would also have sent a Telegram alert.
`silver_orphan_rows` and `silver_row_values` are WARN and only appear in the
digest. Value drift, if any, is the interesting one: it says how many Silver
rows disagree with what the projection produces today — which also answers
whether #101's `is_active_source` fix ever reached the whole history.

---

## When all of this is done

Step 05 can start writing. The order and the shape are settled — see the
artifact *«Двойная запись и сверка»* and §7a of
`.planning/POSTGRES_SESSION_HANDOFF.md`. The first table is not `orders`: it is
`categories` and `products`, cheap and recoverable, so the mechanism is proved
on something whose loss costs one resync.
