"""How many delivery callbacks a second can each store actually absorb?

**SAFETY, WRITTEN AFTER THIS SCRIPT DESTROYED PRODUCTION DATA.** On 2026-09-06
it was run inside `keycrm-web` to measure on the real runtime. `KS_PG_DSN`
there points at production, and the fixture began with `TRUNCATE` of the three
SMS tables — which, since the writer moved to Postgres that morning, were the
system of record. Two campaigns and 10 867 frozen roster rows went, and were
only recoverable because DuckDB still held a frozen copy.

Two changes so it cannot happen again, and neither is a warning in a comment:

* **It never truncates.** Everything it writes is scoped to one campaign named
  with a random suffix, and only that campaign's rows are removed afterwards.
  Run against production now and the worst case is a few hundred rows that
  clean themselves up.
* **The Postgres half refuses to run without `KS_BENCH_PG=1`.** Reaching a
  database at all is opt-in, so inheriting a production DSN from a container's
  environment does nothing by itself.

The 2026-08-27 investigation measured the DuckDB *write* at 0.334 ms and
concluded the database was not the bottleneck. That measured the work, not the
waiting: `DuckDBStore` is a singleton and `connection()` takes one
process-wide `asyncio.Lock`, so every callback queues behind every other
callback — and behind every dashboard query and the warehouse rebuild that
runs every two minutes.

Arrival is ~143/sec in a 7-second burst. This measures what each path sustains
under that concurrency, and what happens when something else holds the lock
for a second the way a rebuild does.
"""
import asyncio
import os
import sys
import time
import uuid

sys.path.insert(0, os.getcwd())

CONCURRENCY = 50
CALLS = 500

# Scoped to this run. Nothing outside it is ever touched.
CAMPAIGN = f"_bench_{uuid.uuid4().hex[:8]}"


async def seed_duckdb(store):
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO sms_campaigns (campaign, ltv_basis, sales_type,"
            " holdout_pct, criteria) VALUES (?,'revenue','retail',10,'{}')",
            [CAMPAIGN])
        for i in range(CALLS):
            conn.execute(
                "INSERT INTO sms_campaign_members (campaign, buyer_id, phone,"
                " tier, assignment, orders_at_export, message_id,"
                " delivery_status) VALUES (?,?,?,'VIP','target',1,?,'Accepted')",
                [CAMPAIGN, i, f"38050{i:07d}", f"{CAMPAIGN}-m-{i}"])


async def drive(store, label, competitor=None):
    """Fire CALLS deliveries at CONCURRENCY, optionally against a lock holder."""
    sem = asyncio.Semaphore(CONCURRENCY)
    errors = []

    async def one(i):
        async with sem:
            try:
                await store.record_sms_delivery(
                    message_id=f"{CAMPAIGN}-m-{i}", status="DELIVRD", delivered=True,
                    delivered_at=None, event_id=f"{CAMPAIGN}-{label}-e-{i}")
            except Exception as exc:  # noqa: BLE001
                errors.append(repr(exc)[:70])

    stop = asyncio.Event()
    comp_task = asyncio.create_task(competitor(store, stop)) if competitor else None

    started = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(CALLS)))
    elapsed = time.perf_counter() - started

    stop.set()
    if comp_task:
        await comp_task

    rate = CALLS / elapsed
    print(f"  {label:38} {elapsed:6.2f}s  {rate:7.1f} req/s"
          + (f"  errors={len(errors)} {errors[:1]}" if errors else ""))
    return rate


async def rebuild_like(store, stop):
    """A stand-in for the warehouse rebuild: takes the store lock for ~1s at a
    time, which is what it genuinely does every two minutes."""
    while not stop.is_set():
        async with store.connection() as conn:
            conn.execute("SELECT 1")
            await asyncio.sleep(1.0)      # holding the lock, as a rebuild does
        await asyncio.sleep(0.05)


async def main():
    import shutil
    import tempfile

    from core.duckdb_store import DuckDBStore

    tmp = tempfile.mkdtemp()
    store = DuckDBStore(db_path=os.path.join(tmp, "bench.duckdb"))
    await store.connect()
    try:
        await seed_duckdb(store)
        print(f"\n{CALLS} delivery callbacks, {CONCURRENCY} in flight\n")

        print("DuckDB — the path production uses today:")
        os.environ.pop("KS_SMS_STORE", None)
        quiet = await drive(store, "alone")

        # fresh event ids, same rows
        async with store.connection() as conn:
            conn.execute("DELETE FROM sms_dlr_events")
        busy = await drive(store, "while a rebuild holds the lock",
                           competitor=rebuild_like)

        # ── the same burst through Postgres ──────────────────────────────
        # Opt-in, and deliberately not just "is a DSN set": inside any
        # application container one is, and it is production's.
        dsn = os.getenv("KS_PG_DSN")
        if dsn and os.getenv("KS_BENCH_PG", "").strip() not in ("1", "true", "yes"):
            print("\nPostgres half skipped — set KS_BENCH_PG=1 to allow it to "
                  "write to the database KS_PG_DSN names.")
            dsn = None
        pg_quiet = pg_busy = None
        if dsn:
            import asyncpg
            from unittest.mock import AsyncMock, patch

            pool = await asyncpg.create_pool(dsn, min_size=8, max_size=20)
            async with pool.acquire() as c:
                # No TRUNCATE. Only this run's own campaign is created, and the
                # `finally` below removes exactly it.
                await c.execute(
                    "INSERT INTO app.sms_campaigns (campaign, ltv_basis,"
                    " sales_type, holdout_pct, criteria)"
                    " VALUES ($1,'revenue','retail',10,'{}')", CAMPAIGN)
                await c.executemany(
                    "INSERT INTO app.sms_campaign_members (campaign, buyer_id,"
                    " phone, tier, assignment, orders_at_export, message_id,"
                    " delivery_status)"
                    " VALUES ($1, $2, $3, 'VIP', 'target', 1, $4, 'Accepted')",
                    [(CAMPAIGN, i, f"38050{i:07d}", f"{CAMPAIGN}-m-{i}")
                     for i in range(CALLS)])

            os.environ["KS_SMS_STORE"] = "postgres"
            print("\nPostgres — the path the flag switches to:")
            with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
                 patch("core.pg.require_revision", new=AsyncMock()):
                pg_quiet = await drive(store, "alone")
                async with pool.acquire() as c:
                    # Only this run's bindings, so a rerun starts clean without
                    # touching a single row that belongs to anybody else.
                    await c.execute(
                        "DELETE FROM app.sms_dlr_events WHERE event_id LIKE $1",
                        f"%{CAMPAIGN}%")
                pg_busy = await drive(store, "while a rebuild holds the lock",
                                      competitor=rebuild_like)
            os.environ.pop("KS_SMS_STORE", None)
            async with pool.acquire() as c:
                await c.execute(
                    "DELETE FROM app.sms_dlr_events WHERE event_id LIKE $1",
                    f"%{CAMPAIGN}%")
                await c.execute(
                    "DELETE FROM app.sms_campaign_members WHERE campaign = $1",
                    CAMPAIGN)
                await c.execute("DELETE FROM app.sms_campaigns WHERE campaign = $1",
                                CAMPAIGN)
            await pool.close()

        print(f"\n  arrival during a send is ~143/s")
        def verdict(r):
            return "ABSORBS" if r and r >= 143 else "CANNOT KEEP UP"
        print(f"  DuckDB   quiet {verdict(quiet):14} under a rebuild {verdict(busy)}")
        if pg_quiet:
            print(f"  Postgres quiet {verdict(pg_quiet):14} under a rebuild {verdict(pg_busy)}")
    finally:
        await store.close()
        shutil.rmtree(tmp, ignore_errors=True)


asyncio.run(main())
