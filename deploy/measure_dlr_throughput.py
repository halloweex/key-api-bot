"""How many delivery callbacks a second can each store actually absorb?

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

sys.path.insert(0, os.getcwd())

CONCURRENCY = 50
CALLS = 500


async def seed_duckdb(store):
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO sms_campaigns (campaign, ltv_basis, sales_type,"
            " holdout_pct, criteria) VALUES ('bench','revenue','retail',10,'{}')")
        for i in range(CALLS):
            conn.execute(
                "INSERT INTO sms_campaign_members (campaign, buyer_id, phone,"
                " tier, assignment, orders_at_export, message_id,"
                " delivery_status) VALUES ('bench',?,?,'VIP','target',1,?, 'Accepted')",
                [i, f"38050{i:07d}", f"m-{i}"])


async def drive(store, label, competitor=None):
    """Fire CALLS deliveries at CONCURRENCY, optionally against a lock holder."""
    sem = asyncio.Semaphore(CONCURRENCY)
    errors = []

    async def one(i):
        async with sem:
            try:
                await store.record_sms_delivery(
                    message_id=f"m-{i}", status="DELIVRD", delivered=True,
                    delivered_at=None, event_id=f"{label}-e-{i}")
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
        dsn = os.getenv("KS_PG_DSN")
        pg_quiet = pg_busy = None
        if dsn:
            import asyncpg
            from unittest.mock import AsyncMock, patch

            pool = await asyncpg.create_pool(dsn, min_size=8, max_size=20)
            async with pool.acquire() as c:
                await c.execute("TRUNCATE app.sms_campaigns, app.sms_campaign_members,"
                                " app.sms_dlr_events")
                await c.execute("INSERT INTO app.sms_campaigns (campaign, ltv_basis,"
                                " sales_type, holdout_pct, criteria)"
                                " VALUES ('bench','revenue','retail',10,'{}')")
                await c.executemany(
                    "INSERT INTO app.sms_campaign_members (campaign, buyer_id, phone,"
                    " tier, assignment, orders_at_export, message_id, delivery_status)"
                    " VALUES ('bench',$1,$2,'VIP','target',1,$3,'Accepted')",
                    [(i, f"38050{i:07d}", f"m-{i}") for i in range(CALLS)])

            os.environ["KS_SMS_STORE"] = "postgres"
            print("\nPostgres — the path the flag switches to:")
            with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
                 patch("core.pg.require_revision", new=AsyncMock()):
                pg_quiet = await drive(store, "alone")
                async with pool.acquire() as c:
                    await c.execute("TRUNCATE app.sms_dlr_events")
                pg_busy = await drive(store, "while a rebuild holds the lock",
                                      competitor=rebuild_like)
            os.environ.pop("KS_SMS_STORE", None)
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
