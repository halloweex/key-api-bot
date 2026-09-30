"""Buyer completeness, measured on a real PostgreSQL (chain 4, PR-1, step 4).

Two questions no comparison between the stores can answer — a buyer BOTH lack,
and a verdict nobody derived — asked of Postgres alone, so they hold in every
state of chain 4 and can be measured with the flag still off.

Each test empties the tables it reads and seeds its own rows inside one
transaction that is rolled back, so a scenario is only ever the rows it names,
on a database other tests share.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio

from core import mirror_reconciliation as mr
from core.gender import RULES_VERSION

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# A fixed clock, so no verdict depends on when the suite happened to run.
NOW = datetime(2030, 6, 5, 9, 0, tzinfo=timezone.utc)
LANDED_LONG_AGO = NOW - timedelta(minutes=mr.BUYER_VERDICT_GRACE_MINUTES + 1)
LANDED_JUST_NOW = NOW - timedelta(minutes=mr.BUYER_VERDICT_GRACE_MINUTES - 1)
ORDERED_LONG_AGO = NOW - timedelta(hours=mr.BUYER_MISSING_AFTER_HOURS + 1)
ORDERED_JUST_NOW = NOW - timedelta(hours=mr.BUYER_MISSING_AFTER_HOURS - 1)


@pytest_asyncio.fixture
async def conn():
    c = await asyncpg.connect(DSN)
    tx = c.transaction()
    await tx.start()
    try:
        await c.execute("SET LOCAL lock_timeout = '5s'")
        await c.execute("SET LOCAL statement_timeout = '30s'")
        for table in ("app.buyer_gender", "bronze.buyer_contacts",
                      "bronze.buyers", "silver.orders"):
            await c.execute(f"DELETE FROM {table}")
        yield c
    finally:
        await tx.rollback()
        await c.close()


async def buyer(conn, bid, *, name="Олена", landed=LANDED_LONG_AGO):
    await conn.execute(
        "INSERT INTO bronze.buyers (id, full_name, mirrored_at) VALUES ($1, $2, $3)",
        bid, name, landed)


async def verdict(conn, bid, *, version=RULES_VERSION, override=False):
    await conn.execute(
        """INSERT INTO app.buyer_gender
               (buyer_id, gender, method, confidence, decided_from,
                rules_version, override_by_human)
           VALUES ($1, 'f', 'dictionary', 'high', 'given', $2, $3)""",
        bid, version, override)


async def order(conn, oid, buyer_id, *, ordered_at=ORDERED_LONG_AGO):
    await conn.execute(
        """INSERT INTO silver.orders
               (id, source_id, status_id, grand_total, ordered_at, buyer_id,
                order_date, is_return, sales_type, is_active_source, source_name)
           VALUES ($1, 1, 1, 100, $2, $3, $4, FALSE, 'retail', TRUE, 'Instagram')""",
        oid, ordered_at, buyer_id, ordered_at.date())


async def findings(conn):
    row = await mr.buyer_completeness_row(conn, now=NOW, rules_version=RULES_VERSION)
    return {f.check_name: f for f in mr.buyer_completeness_findings(row)}


class TestAClean:
    @pytest.mark.asyncio
    async def test_empty_tables(self, conn):
        assert await findings(conn) == {}

    @pytest.mark.asyncio
    async def test_every_buyer_has_a_verdict_and_every_order_its_buyer(self, conn):
        await buyer(conn, 1)
        await verdict(conn, 1)
        await order(conn, 10, 1)
        assert await findings(conn) == {}


class TestVerdicts:
    @pytest.mark.asyncio
    async def test_a_buyer_past_the_grace_with_no_verdict_is_named(self, conn):
        await buyer(conn, 1)
        f = (await findings(conn))["buyers_without_verdict"]
        assert (f.count, f.sample_ids, f.severity.value) == (1, (1,), "WARN")

    @pytest.mark.asyncio
    async def test_one_inside_the_grace_is_not(self, conn):
        await buyer(conn, 1, landed=LANDED_JUST_NOW)
        assert await findings(conn) == {}

    @pytest.mark.asyncio
    async def test_a_verdict_of_older_rules_is_owed_a_new_one(self, conn):
        await buyer(conn, 1)
        await verdict(conn, 1, version=RULES_VERSION - 1)
        assert "buyers_without_verdict" in await findings(conn)

    @pytest.mark.asyncio
    async def test_a_human_override_is_never_owed_one(self, conn):
        await buyer(conn, 1)
        await verdict(conn, 1, version=RULES_VERSION - 1, override=True)
        assert await findings(conn) == {}


class TestMissingBuyers:
    @pytest.mark.asyncio
    async def test_an_order_a_day_old_with_no_buyer_is_named(self, conn):
        await order(conn, 10, 7)
        f = (await findings(conn))["buyers_missing_for_orders"]
        assert (f.count, f.sample_ids) == (1, (7,))

    @pytest.mark.asyncio
    async def test_a_buyer_with_no_name_is_what_the_sync_selects_too(self, conn):
        await buyer(conn, 7, name="")
        await verdict(conn, 7)
        await order(conn, 10, 7)
        assert "buyers_missing_for_orders" in await findings(conn)

    @pytest.mark.asyncio
    async def test_a_fresh_order_is_lag_not_a_finding(self, conn):
        await order(conn, 10, 7, ordered_at=ORDERED_JUST_NOW)
        assert await findings(conn) == {}

    @pytest.mark.asyncio
    async def test_the_earliest_order_decides(self, conn):
        await order(conn, 10, 7, ordered_at=ORDERED_JUST_NOW)
        await order(conn, 11, 7, ordered_at=ORDERED_LONG_AGO)
        assert "buyers_missing_for_orders" in await findings(conn)

    @pytest.mark.asyncio
    async def test_an_order_with_no_buyer_at_all_is_not_a_missing_one(self, conn):
        await order(conn, 10, None)
        assert await findings(conn) == {}


class TestBothTogether:
    @pytest.mark.asyncio
    async def test_the_two_are_told_apart(self, conn):
        await buyer(conn, 1)            # landed, no verdict
        await order(conn, 10, 2)        # ordered, never landed
        assert set(await findings(conn)) == {
            "buyers_without_verdict", "buyers_missing_for_orders"}
