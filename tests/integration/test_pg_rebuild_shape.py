"""The derivation's write shape, against a real Postgres (the review's A23).

Silver was rebuilt as `TRUNCATE`, an INSERT that wrote `is_new_customer =
FALSE` on every row, then a full-table UPDATE that rewrote nearly every row
again (pass 2); Gold as `TRUNCATE` and one INSERT. Measured on 47 000 orders:
two passes wrote 36.4 MB of WAL against 16.0 MB for pass 1 alone, a trivial
read waited 0.55 s during a plain rebuild and 3.98 s behind a queued
`TRUNCATE`. Since A23 each layer is `DELETE` and one INSERT under a bounded
`lock_timeout`, in one transaction.

Held here:

* **the rows are the old two passes' rows**, Silver and Gold alike, on a
  fixture that takes every branch pass 2 had (EXCEPT both ways is empty);
* **no statement rewrites a row**: what the rebuilds send a real connection is
  recorded, and none is an UPDATE or a TRUNCATE;
* **a reader never holds a rebuild back**, and keeps its snapshot of the
  previous rows;
* **a conflicting lock costs the run, not the job**: the rebuild gives up at
  `lock_timeout`, the derivation journals a failed run with the debt still
  owed, and the next tick, once the lock is gone, rebuilds.
"""
from __future__ import annotations

import asyncio
import os
import re
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from tests.integration.pg_tab_seed import DSN, needs_postgres, tab_in_postgres

asyncpg = pytest.importorskip("asyncpg")

pytestmark = needs_postgres

RETAIL_MANAGER = 97701       # classified retail from 2020
UNCLASSIFIED_MANAGER = 97702  # in nobody's list: internal
B2B = 15                     # core.duckdb_constants.B2B_MANAGER_ID

# Silver's pass 2 as it ran until A23 (`silver_pass2_sql(dialect=POSTGRES)`
# at 57fd7c22, its SQL comments left out): the reference the single INSERT
# must reproduce. Pass 1 was `silver_select_sql(POSTGRES)`, which is still
# the code's and pinned.
OLD_PASS2 = """
UPDATE silver.orders AS s SET
    buyer_first_order_date = fo.first_order_date,
    is_new_customer = CASE
        WHEN s.buyer_id IS NOT NULL
             AND NOT s.is_return
             AND s.is_active_source
             AND s.order_date = fo.first_order_date
        THEN TRUE ELSE FALSE
    END
FROM (
    SELECT buyer_id, MIN(order_date) AS first_order_date
    FROM silver.orders
    WHERE buyer_id IS NOT NULL
      AND NOT is_return
    GROUP BY buyer_id
) fo
WHERE s.buyer_id = fo.buyer_id
"""


def _old_pass1() -> str:
    from core.pg_silver import SILVER_COLUMNS, SILVER_TABLE
    from core.sql_dialect import POSTGRES
    from core.warehouse_sql import silver_select_sql

    return (f"INSERT INTO {SILVER_TABLE} ({', '.join(SILVER_COLUMNS)})\n"
            f"SELECT {silver_select_sql(POSTGRES)}\nFROM {POSTGRES.orders} o")


def _branch_orders():
    """Every input pass 2 decided on, named. `(id, source, status_id,
    status_group_id, total, ordered_at, buyer, manager, promocode)`."""
    day = datetime(2026, 6, 10, 9, 0, tzinfo=timezone.utc)
    late = datetime(2026, 6, 10, 22, 30, tzinfo=timezone.utc)   # 11 June in Kyiv
    rows = [
        # A buyer whose first order was on a retired source (Opencart, 3),
        # then Instagram: the Instagram order is not their first.
        (95000001, 3, 1, 1, 400, day - timedelta(days=30), 95100001, None, None),
        (95000002, 1, 1, 1, 500, day, 95100001, None, None),
        # A cancelled first order (group 6), then a real one: the real one is.
        (95000003, 1, 19, 6, 300, day - timedelta(days=5), 95100002, None, None),
        (95000004, 1, 1, 1, 310, day, 95100002, None, None),
        # A return by the legacy status list (no group): never new.
        (95000005, 2, 19, None, 120, day, 95100003, None, None),
        # A buyer with returns only: no first order at all.
        (95000006, 4, 22, 6, 90, day, 95100004, None, None),
        # Two orders on the buyer's first day, two sources: both new.
        (95000007, 1, 1, 1, 200, day, 95100005, None, None),
        (95000008, 4, 1, 1, 210, day + timedelta(hours=3), 95100005, None, None),
        # No buyer.
        (95000009, 4, 1, 1, 150, day, None, None, None),
        # The exhibition, a b2b order, an internal one, a classified retail
        # manager's, and a promocode.
        (95000010, 5, 1, 1, 800, day, 95100006, RETAIL_MANAGER, None),
        (95000011, 1, 1, 1, 9000, day, 95100007, B2B, None),
        (95000012, 2, 1, 1, 0, day, 95100008, UNCLASSIFIED_MANAGER, None),
        (95000013, 1, 1, 1, 640, day, 95100009, RETAIL_MANAGER, "PROMO10"),
        # An order whose Kyiv date is the day after its UTC one, first for its
        # buyer by the Kyiv date only.
        (95000014, 4, 1, 1, 700, late, 95100010, None, None),
        (95000015, 1, 1, 1, 710, late + timedelta(hours=2), 95100010, None, None),
        # A first order on an inactive source that is also the buyer's only
        # one: is_new_customer is FALSE (inactive), the date is still theirs.
        (95000016, 3, 1, 1, 50, day, 95100011, None, None),
    ]
    return rows


async def _populate(conn, pool, *, random_orders=4000):
    await conn.execute(
        "INSERT INTO app.manager_classifications (manager_id, is_retail, valid_from)"
        " VALUES ($1, TRUE, DATE '2020-01-01')", RETAIL_MANAGER)
    await conn.executemany(
        "INSERT INTO bronze.orders (id, source_id, status_id, status_group_id,"
        " grand_total, ordered_at, buyer_id, manager_id, promocode)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", _branch_orders())
    # And a deterministic crowd over every combination at once.
    await conn.execute("SELECT setseed(0.23)")
    await conn.execute(
        """
        INSERT INTO bronze.orders (id, source_id, status_id, status_group_id,
                                   grand_total, ordered_at, buyer_id, manager_id,
                                   promocode)
        SELECT 95200000 + g,
               (ARRAY[1, 1, 2, 3, 4, 4, 5])[1 + floor(random() * 7)::int],
               CASE WHEN r < 0.12 THEN 19 ELSE 1 END,
               CASE WHEN r < 0.06 THEN 6 WHEN r < 0.12 THEN NULL ELSE 1 END,
               round((random() * 3000)::numeric, 2),
               timestamptz '2025-06-01 00:00+00' + random() * interval '400 days',
               CASE WHEN random() < 0.05 THEN NULL
                    ELSE 95300000 + floor(random() * 900)::int END,
               (ARRAY[NULL, NULL, NULL, 15, $1, $2])[1 + floor(random() * 6)::int],
               CASE WHEN random() < 0.1 THEN 'CODE' END
          FROM (SELECT g, random() AS r FROM generate_series(1, $3) g) s
        """, RETAIL_MANAGER, UNCLASSIFIED_MANAGER, random_orders)


@pytest_asyncio.fixture
async def landed(monkeypatch):
    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        yield pool


async def _snapshot(conn, table, into):
    await conn.execute(f"DROP TABLE IF EXISTS {into}")
    await conn.execute(f"CREATE TEMP TABLE {into} AS SELECT * FROM {table}")


async def _except_both_ways(conn, table, temp):
    one = await conn.fetchval(f"SELECT count(*) FROM (SELECT * FROM {table} EXCEPT ALL"
                              f" SELECT * FROM {temp}) d")
    other = await conn.fetchval(f"SELECT count(*) FROM (SELECT * FROM {temp} EXCEPT ALL"
                                f" SELECT * FROM {table}) d")
    return one, other


class TestTheRowsAreTheTwoPassesRows:
    @pytest.mark.asyncio
    async def test_silver_and_gold_equal_the_old_shape(self, landed):
        """Mutation: drop `AND p.is_active_source` from the recompute's
        is_new_customer, or join the baseline on the inactive-source filter —
        the Opencart buyer's Instagram order turns new and Silver and Gold
        differ."""
        from core.pg_gold import GOLD_TABLE, insert_sql as gold_insert, rebuild_gold
        from core.pg_silver import SILVER_TABLE, rebuild_silver

        await rebuild_silver(landed)
        await rebuild_gold(landed)
        async with landed.acquire() as conn:
            await _snapshot(conn, SILVER_TABLE, "new_silver")
            await _snapshot(conn, GOLD_TABLE, "new_gold")
            async with conn.transaction():
                await conn.execute(f"DELETE FROM {SILVER_TABLE}")
                await conn.execute(_old_pass1())
                await conn.execute(OLD_PASS2)
                await conn.execute(f"DELETE FROM {GOLD_TABLE}")
                await conn.execute(gold_insert())
            silver_rows = await conn.fetchval(f"SELECT count(*) FROM {SILVER_TABLE}")
            gold_rows = await conn.fetchval(f"SELECT count(*) FROM {GOLD_TABLE}")
            assert silver_rows == 4000 + len(_branch_orders())
            assert gold_rows > 1000
            assert await _except_both_ways(conn, SILVER_TABLE, "new_silver") == (0, 0)
            assert await _except_both_ways(conn, GOLD_TABLE, "new_gold") == (0, 0)

            # The fixture took the branches it names, read off what was built.
            by_id = {r["id"]: r for r in await conn.fetch(
                "SELECT id, is_new_customer, buyer_first_order_date, sales_type,"
                " is_return FROM new_silver WHERE id < 95200000")}
        assert by_id[95000002]["is_new_customer"] is False            # Opencart first
        assert str(by_id[95000002]["buyer_first_order_date"]) == "2026-05-11"
        assert by_id[95000004]["is_new_customer"] is True             # after a cancel
        assert by_id[95000005]["is_return"] and not by_id[95000005]["is_new_customer"]
        assert by_id[95000006]["buyer_first_order_date"] is None      # returns only
        assert by_id[95000007]["is_new_customer"] and by_id[95000008]["is_new_customer"]
        assert by_id[95000009]["buyer_first_order_date"] is None      # no buyer
        assert {by_id[i]["sales_type"] for i in (95000010, 95000011, 95000012, 95000013)} \
            == {"exhibition", "b2b", "internal", "retail"}
        assert str(by_id[95000014]["buyer_first_order_date"]) == "2026-06-11"
        assert by_id[95000016]["is_new_customer"] is False
        assert str(by_id[95000016]["buyer_first_order_date"]) == "2026-06-10"


class _Recording:
    """A real pool whose connections record every statement sent."""

    def __init__(self, pool):
        self.pool, self.sql = pool, []

    def acquire(self):
        outer = self

        class _Acquire:
            async def __aenter__(self):
                self.cm = outer.pool.acquire()
                conn = await self.cm.__aenter__()
                return _Conn(conn, outer.sql)

            async def __aexit__(self, *exc):
                return await self.cm.__aexit__(*exc)
        return _Acquire()


class _Conn:
    def __init__(self, conn, sql):
        self._conn, self._sql = conn, sql

    def transaction(self, **kw):
        return self._conn.transaction(**kw)

    async def execute(self, query, *args):
        self._sql.append(query)
        return await self._conn.execute(query, *args)

    async def fetchval(self, query, *args):
        self._sql.append(query)
        return await self._conn.fetchval(query, *args)


class TestNoStatementRewritesARow:
    @pytest.mark.asyncio
    async def test_the_rebuilds_send_no_update_and_no_truncate(self, landed):
        """Mutation: put pass 2 back after the INSERT, or `TRUNCATE` back as
        the clear."""
        from core.pg_gold import rebuild_gold
        from core.pg_silver import rebuild_silver

        recording = _Recording(landed)
        silver = await rebuild_silver(recording)
        gold = await rebuild_gold(recording)
        assert silver["rows"] > 4000 and gold["rows"] > 1000
        assert not [s for s in recording.sql if re.search(r"\b(UPDATE|TRUNCATE)\b", s)
                    and re.search(r"\b(silver\.orders|gold\.daily_revenue)\b", s)]
        writes = [s for s in recording.sql
                  if re.search(r"\b(UPDATE|TRUNCATE|DELETE|INSERT)\s+(INTO\s+|FROM\s+)?"
                               r"(silver\.orders|gold\.daily_revenue)\b", s)]
        assert [re.search(r"\b(UPDATE|TRUNCATE|DELETE|INSERT)\b", s).group(1) for s in writes] \
            == ["DELETE", "INSERT", "DELETE", "INSERT"]


async def _hold(dsn, sql, held: asyncio.Event, release: asyncio.Event, seen: list,
                lock=None):
    """A second session that takes a lock (or a snapshot) and keeps it."""
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction(isolation="repeatable_read"):
            if lock:
                await conn.execute(lock)
            seen.append(await conn.fetchval(sql))
            held.set()
            await release.wait()
            seen.append(await conn.fetchval("SELECT count(*) FROM silver.orders"))
    finally:
        await conn.close()


class TestAReaderNeverHoldsTheRebuildBack:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("table", ["silver.orders", "gold.daily_revenue"])
    async def test_a_reader_in_a_long_transaction_does_not_block_it(self, landed, table):
        """Mutation: `TRUNCATE` back — it queues behind the reader's ACCESS
        SHARE and the rebuild does not finish while the reader holds it."""
        from core.pg_gold import rebuild_gold
        from core.pg_silver import rebuild_silver

        await rebuild_silver(landed)
        await rebuild_gold(landed)
        held, release, seen = asyncio.Event(), asyncio.Event(), []
        reader = asyncio.create_task(_hold(
            DSN, f"SELECT count(*) FROM {table}", held, release, seen))
        try:
            await asyncio.wait_for(held.wait(), 10)
            rebuild = rebuild_silver if table == "silver.orders" else rebuild_gold
            started = time.monotonic()
            await asyncio.wait_for(rebuild(landed), 4)
            assert time.monotonic() - started < 4
        finally:
            release.set()
            await reader
        # The reader's snapshot still saw the rows it started with.
        before = seen[0]
        assert before > 0
        if table == "silver.orders":
            assert seen[1] == before


class TestAConflictingLockCostsTheRunNotTheJob:
    @pytest.fixture
    def quick(self, monkeypatch):
        from core import pg_silver

        # `raising=False`: on a tree without the bound the test then fails on
        # the wait itself, which is the defect, rather than on a name.
        monkeypatch.setattr(pg_silver, "REBUILD_LOCK_TIMEOUT", "300ms", raising=False)

    @pytest_asyncio.fixture
    async def journal_kept(self, landed):
        """What a derivation tick writes beyond the two tables, put back: the
        signal, the journal's new rows, and the watermarks the tick and its
        UTM parse stamp (`tab_in_postgres` puts Silver's and Gold's back)."""
        names = ["silver.order_utm", "meta.derivation_runs", "meta.derivation_signal"]
        async with landed.acquire() as conn:
            signal = await conn.fetchrow(
                "SELECT requested, built, built_at FROM meta.derivation_signal"
                " WHERE layer = 'warehouse'")
            last_run = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM meta.derivation_runs")
            stamps = await conn.fetch(
                "SELECT * FROM meta.mirror_state WHERE table_name = ANY($1)", names)
        yield
        async with landed.acquire() as conn:
            await conn.execute(
                "UPDATE meta.derivation_signal SET requested = $1, built = $2, built_at = $3"
                " WHERE layer = 'warehouse'", *signal)
            await conn.execute("DELETE FROM meta.derivation_runs WHERE id > $1", last_run)
            await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = ANY($1)", names)
            for row in stamps:
                columns = list(row.keys())
                await conn.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                    f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                    *row.values())

    @pytest.mark.asyncio
    @pytest.mark.parametrize("table", ["silver.orders", "gold.daily_revenue"])
    async def test_the_rebuild_gives_up_at_lock_timeout(self, landed, quick, table):
        """A session holding SHARE (an index build, an operator's `LOCK`)
        conflicts with the DELETE. The rebuild raises `LockNotAvailableError`
        at the bound and the previous rows stand. Mutation: drop the `SET
        LOCAL lock_timeout` — the rebuild waits for as long as the lock is
        held."""
        from core.pg_gold import rebuild_gold
        from core.pg_silver import rebuild_silver

        await rebuild_silver(landed)
        await rebuild_gold(landed)
        async with landed.acquire() as conn:
            before = await conn.fetchval(f"SELECT count(*) FROM {table}")
        held, release, seen = asyncio.Event(), asyncio.Event(), []
        locker = asyncio.create_task(_hold(
            DSN, "SELECT 1", held, release, seen, lock=f"LOCK TABLE {table} IN SHARE MODE"))
        try:
            await asyncio.wait_for(held.wait(), 10)
            rebuild = rebuild_silver if table == "silver.orders" else rebuild_gold
            started = time.monotonic()
            with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
                await asyncio.wait_for(rebuild(landed), 4)
            assert time.monotonic() - started < 3
        finally:
            release.set()
            await locker
        async with landed.acquire() as conn:
            assert await conn.fetchval(f"SELECT count(*) FROM {table}") == before

    @pytest.mark.asyncio
    async def test_the_derivation_journals_a_failed_run_and_the_next_tick_rebuilds(
            self, landed, quick, journal_kept, monkeypatch):
        """Not a crash: the job returns an error naming Silver, the journal
        has the run with its cause, the debt stays owed and Silver's watermark
        counts the failure; once the lock is gone the next tick rebuilds and
        clears it."""
        from core import pg_derivation
        from core.scheduler import BackgroundScheduler

        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
        monkeypatch.setattr(BackgroundScheduler, "_pg_derive_ran", False)
        monkeypatch.setattr(BackgroundScheduler, "_pg_derive_started", None)
        monkeypatch.setattr(pg_derivation, "_signal_failures", 0)
        async with landed.acquire() as conn:
            runs_before = await conn.fetchval("SELECT count(*) FROM meta.derivation_runs")
            await conn.execute(
                "UPDATE meta.derivation_signal SET requested = built + 1"
                " WHERE layer = 'warehouse'")
            owed = await conn.fetchval(
                "SELECT requested FROM meta.derivation_signal WHERE layer = 'warehouse'")
        held, release, seen = asyncio.Event(), asyncio.Event(), []
        locker = asyncio.create_task(_hold(
            DSN, "SELECT 1", held, release, seen,
            lock="LOCK TABLE silver.orders IN SHARE MODE"))
        alert = AsyncMock(return_value=1)
        try:
            await asyncio.wait_for(held.wait(), 10)
            with patch("core.alerting.raise_alert", alert), \
                 patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
                result = await asyncio.wait_for(BackgroundScheduler()._run_pg_derivation(), 20)
        finally:
            release.set()
            await locker
        assert result["status"] == "error" and result["stage"] == "silver.orders", result
        async with landed.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT error FROM meta.derivation_runs ORDER BY id DESC LIMIT 1")
            assert await conn.fetchval("SELECT count(*) FROM meta.derivation_runs") \
                == runs_before + 1
            assert "LockNotAvailableError" in run["error"] and "lock timeout" in run["error"]
            assert await conn.fetchval(
                "SELECT built < requested FROM meta.derivation_signal"
                " WHERE layer = 'warehouse'"), "a failed run settled the debt"
            failures = await conn.fetchval(
                "SELECT failures_since_ok FROM meta.mirror_state"
                " WHERE table_name = 'silver.orders'")
            assert failures and failures >= 1
        assert [c.kwargs["conditions"] for c in alert.await_args_list] \
            == [["warehouse_pg:derive_failed"]]

        with patch("core.alerting.raise_alert", AsyncMock(return_value=1)), \
             patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
            again = await BackgroundScheduler()._run_pg_derivation()
        assert again["status"] == "success", again
        async with landed.acquire() as conn:
            assert await conn.fetchval(
                "SELECT built >= $1 FROM meta.derivation_signal WHERE layer = 'warehouse'",
                owed)
            assert await conn.fetchval(
                "SELECT failures_since_ok FROM meta.mirror_state"
                " WHERE table_name = 'silver.orders'") == 0
