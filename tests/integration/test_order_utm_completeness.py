"""The UTM completeness check against a real Postgres (DN-16).

`reconcile_order_utm_completeness` asks whether every order the UTM parser
reads has a current verdict in `silver.order_utm`. The question is all SQL —
which orders count, which verdict is stale, which clock forgives a late one —
so it is proved on a server, not a fake: a healthy table says nothing, an order
past the grace with no verdict or an old one is CRITICAL and named, and one
inside the grace is counted as INFO and never paged.

The last class runs the real DuckDB parser and the real shipper against the
same orders, because "the population is the parser's own predicate" is a claim
about two pieces of code, and only running both can hold them to it. It also
settles the one thing a reading cannot: that `parsed_at`, carried through
DuckDB and asyncpg, compares equal to the `updated_at` Postgres holds for the
same order, rather than an offset away.

HOW A SCENARIO IS ISOLATED
The check reads the whole of `bronze.orders`, so every scenario empties both
tables and seeds its own rows inside one transaction that is rolled back.
Nothing another test committed is touched, and nothing here survives. The
shipper runs on the same connection through a one-connection pool, so its own
transaction becomes a savepoint inside that one.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` and CI supply one.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import mirror_reconciliation as mr
from core import pg_order_utm
from core.data_quality import Severity
from core.duckdb_store import DuckDBStore

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

TABLE = mr.ORDER_UTM_TABLE.pg_table
GRACE = mr.SILVER_GRACE_MINUTES
# A fixed clock, so no verdict depends on when the suite happened to run.
NOW = datetime(2030, 6, 5, 4, 30, tzinfo=timezone.utc)
CUTOFF = NOW - timedelta(minutes=GRACE)
PAST = CUTOFF - timedelta(seconds=1)        # reached Postgres just too long ago
INSIDE = CUTOFF + timedelta(seconds=1)      # still inside the grace
UPDATED = datetime(2030, 6, 5, 1, 0, tzinfo=timezone.utc)
COMMENT = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: spring"


@pytest_asyncio.fixture
async def conn():
    c = await asyncpg.connect(DSN)
    tx = c.transaction()
    await tx.start()
    try:
        # Bounded: an empty-the-table DELETE waits on any row lock another
        # session holds, and a test must not hang on somebody else's session.
        await c.execute("SET LOCAL lock_timeout = '5s'")
        await c.execute("SET LOCAL statement_timeout = '30s'")
        await c.execute(f"DELETE FROM {TABLE}")
        await c.execute("DELETE FROM bronze.orders")
        yield c
    finally:
        await tx.rollback()
        await c.close()


async def order(conn, oid, *, comment=COMMENT, updated_at=UPDATED, mirrored_at=PAST):
    await conn.execute(
        """
        INSERT INTO bronze.orders
               (id, source_id, status_id, grand_total, updated_at,
                manager_comment, mirrored_at)
        VALUES ($1, 4, 1, 100, $2, $3, $4)
        """, oid, updated_at, comment, mirrored_at)


async def verdict(conn, oid, *, parsed_at=UPDATED):
    await conn.execute(
        f"""
        INSERT INTO {TABLE} (order_id, utm_source, utm_medium, utm_campaign,
                             traffic_type, platform, parsed_at)
        VALUES ($1, 'fbads', 'cpc', 'spring', 'paid_confirmed', 'facebook', $2)
        """, oid, parsed_at)


async def findings(conn, *, max_samples=10):
    row = await mr.order_utm_completeness_row(
        conn, now=NOW, grace_minutes=GRACE, max_samples=max_samples)
    return mr.order_utm_completeness_findings(row, grace_minutes=GRACE)


def by_name(issues):
    return {i.check_name: i for i in issues}


class _OneConnectionPool:
    """`pool.acquire()` that hands out the scenario's connection, so everything
    the job and the shipper write lands inside the transaction that is rolled
    back."""

    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


# ── a healthy table ───────────────────────────────────────────────────────────

class TestAHealthyTableSaysNothing:
    @pytest.mark.asyncio
    async def test_an_empty_table(self, conn):
        assert await findings(conn) == []

    @pytest.mark.asyncio
    async def test_every_commented_order_with_a_current_verdict(self, conn):
        await order(conn, 1)
        await verdict(conn, 1)
        await order(conn, 2, updated_at=UPDATED - timedelta(days=30))
        await verdict(conn, 2, parsed_at=UPDATED - timedelta(days=30))
        assert await findings(conn) == []

    @pytest.mark.asyncio
    async def test_an_order_with_no_comment_is_never_owed_a_verdict(self, conn):
        """The parser skips both, so a row is never coming. Asking for one
        would make a finding of ~14,000 orders with nothing to parse."""
        await order(conn, 3, comment=None)
        await order(conn, 4, comment="")
        assert await findings(conn) == []

    @pytest.mark.asyncio
    async def test_a_verdict_at_or_after_its_order_is_current(self, conn):
        """`parsed_at` is the order's `updated_at` when its comment was read,
        so a finished parse leaves them equal. Equal is current — the parser's
        own predicate is `>` — and so is later."""
        await order(conn, 5)
        await verdict(conn, 5, parsed_at=UPDATED)
        await order(conn, 6)
        await verdict(conn, 6, parsed_at=UPDATED + timedelta(minutes=1))
        assert await findings(conn) == []

    @pytest.mark.asyncio
    async def test_an_order_with_no_updated_at_is_not_stale(self, conn):
        """NULL compares as unknown in both engines, so the parser never
        re-reads it either; the check follows it rather than inventing a
        rule the parser does not have."""
        await order(conn, 7, updated_at=None)
        await verdict(conn, 7, parsed_at=UPDATED)
        assert await findings(conn) == []


# ── past the grace ────────────────────────────────────────────────────────────

class TestPastTheGraceIsCritical:
    @pytest.mark.asyncio
    async def test_a_commented_order_with_no_verdict_is_named(self, conn):
        await order(conn, 101)

        issues = await findings(conn)

        assert [i.check_name for i in issues] == ["pg_order_utm_missing"]
        missing = issues[0]
        assert missing.severity is Severity.CRITICAL
        assert missing.count == 1
        assert missing.sample_ids == (101,)
        assert missing.table_name == TABLE
        assert PAST.isoformat() in missing.description

    @pytest.mark.asyncio
    async def test_a_verdict_older_than_its_order_is_named(self, conn):
        await order(conn, 102)
        await verdict(conn, 102, parsed_at=UPDATED - timedelta(hours=1))

        issues = await findings(conn)

        assert [i.check_name for i in issues] == ["pg_order_utm_stale"]
        stale = issues[0]
        assert stale.severity is Severity.CRITICAL
        assert stale.count == 1
        assert stale.sample_ids == (102,)

    @pytest.mark.asyncio
    async def test_the_two_are_told_apart(self, conn):
        await order(conn, 103)
        await order(conn, 104)
        await verdict(conn, 104, parsed_at=UPDATED - timedelta(seconds=1))

        found = by_name(await findings(conn))

        assert set(found) == {"pg_order_utm_missing", "pg_order_utm_stale"}
        assert found["pg_order_utm_missing"].sample_ids == (103,)
        assert found["pg_order_utm_stale"].sample_ids == (104,)

    @pytest.mark.asyncio
    async def test_the_sample_is_capped_and_the_count_is_not(self, conn):
        for oid in range(112, 100, -1):     # 101..112, inserted highest first
            await order(conn, oid)
        # A sequential scan hands the rows back in the order they were written,
        # highest id first, so the ORDER BY inside `array_agg` is the only
        # thing that makes the sample the lowest ids. A plan that walked the
        # primary key would sort them with or without it and the test would
        # stop guarding it, so the paths that could are turned off. (Deleting
        # one of the three ORDER BYs is not caught, and cannot be: PostgreSQL
        # 17 sorts the input once for the aggregates that ask, and the third
        # reads the same sorted rows.)
        await conn.execute("SET LOCAL enable_indexscan = off")
        await conn.execute("SET LOCAL enable_bitmapscan = off")
        await conn.execute("SET LOCAL enable_mergejoin = off")

        (missing,) = await findings(conn)

        assert missing.count == 12
        assert missing.sample_ids == tuple(range(101, 111))


# ── inside the grace ──────────────────────────────────────────────────────────

class TestInsideTheGraceIsCountedNotPaged:
    @pytest.mark.asyncio
    async def test_a_missing_verdict_is_info(self, conn):
        await order(conn, 201, mirrored_at=INSIDE)

        issues = await findings(conn)

        assert [i.check_name for i in issues] == ["pg_order_utm_in_flight"]
        assert issues[0].severity is Severity.INFO
        assert issues[0].sample_ids == (201,)

    @pytest.mark.asyncio
    async def test_a_stale_verdict_is_info(self, conn):
        await order(conn, 202, mirrored_at=INSIDE)
        await verdict(conn, 202, parsed_at=UPDATED - timedelta(hours=1))

        issues = await findings(conn)

        assert [i.check_name for i in issues] == ["pg_order_utm_in_flight"]
        assert issues[0].sample_ids == (202,)

    @pytest.mark.asyncio
    async def test_the_clock_is_the_orders_arrival_not_its_edit(self, conn):
        """An order KeyCRM last touched a week ago and the mirror carried a
        minute ago is in flight. Measured from `updated_at` it would read as a
        week overdue — which is every order a backfill ships."""
        await order(conn, 203, updated_at=NOW - timedelta(days=7), mirrored_at=INSIDE)

        issues = await findings(conn)

        assert [i.check_name for i in issues] == ["pg_order_utm_in_flight"]

    @pytest.mark.asyncio
    async def test_in_flight_and_overdue_are_reported_side_by_side(self, conn):
        await order(conn, 204, mirrored_at=INSIDE)
        await order(conn, 205, mirrored_at=PAST)

        found = by_name(await findings(conn))

        assert found["pg_order_utm_in_flight"].sample_ids == (204,)
        assert found["pg_order_utm_missing"].sample_ids == (205,)
        assert found["pg_order_utm_missing"].severity is Severity.CRITICAL


# ── through the job ───────────────────────────────────────────────────────────

class TestThroughTheJob:
    @pytest.mark.asyncio
    async def test_the_job_reads_postgres_and_names_the_order(self, conn, monkeypatch):
        monkeypatch.delenv("KS_MIRROR_LANDING", raising=False)
        await order(conn, 301)
        with patch("core.pg.get_pool",
                   new=AsyncMock(return_value=_OneConnectionPool(conn))), \
             patch("core.pg.require_revision", new=AsyncMock()):
            issues = await mr.reconcile_order_utm_completeness(now=NOW)

        assert [(i.check_name, i.sample_ids) for i in issues] == [
            ("pg_order_utm_missing", (301,))]

    @pytest.mark.asyncio
    async def test_it_stands_down_with_the_mirror(self, conn, monkeypatch):
        """`reconcile_order_utm`'s rule: with the landing mirror off,
        `bronze.orders` stops moving and the question has no current answer."""
        monkeypatch.setenv("KS_MIRROR_LANDING", "0")
        await order(conn, 302)
        pool = AsyncMock()
        with patch("core.pg.get_pool", new=pool):
            assert await mr.reconcile_order_utm_completeness(now=NOW) == []
        pool.assert_not_called()


# ── the parser and the check agree ────────────────────────────────────────────

@pytest_asyncio.fixture
async def duck(tmp_path):
    store = DuckDBStore(db_path=tmp_path / "utm_completeness.duckdb")
    await store.connect()
    try:
        yield store
    finally:
        await store.close()


async def both_stores(store, conn, oid, comment, updated_at):
    """The same order header in both stores, as the sync writes it: one aware
    datetime, handed to each driver."""
    async with store.connection() as dk:
        dk.execute(
            "INSERT OR REPLACE INTO orders (id, source_id, status_id, grand_total, "
            "updated_at, manager_comment) VALUES (?, 4, 1, 100, ?, ?)",
            [oid, updated_at, comment],
        )
    await conn.execute("DELETE FROM bronze.orders WHERE id = $1", oid)
    await order(conn, oid, comment=comment, updated_at=updated_at)


def owed(issues):
    return {i for issue in issues for i in issue.sample_ids}


class TestTheParserAndTheCheckAgree:
    @pytest.mark.asyncio
    async def test_the_check_owes_exactly_what_the_parser_parses(self, duck, conn):
        edited = UPDATED + timedelta(hours=2)
        await both_stores(duck, conn, 401, COMMENT, UPDATED)
        # A comment with no UTM in it still gets a row — of NULLs — so it is
        # owed one; the classification falls back to the source there.
        await both_stores(duck, conn, 402, "Передзвонити після 18:00", UPDATED)
        await both_stores(duck, conn, 403, "", UPDATED)
        await both_stores(duck, conn, 404, None, UPDATED)
        pool = _OneConnectionPool(conn)

        before = await findings(conn, max_samples=100)
        parsed = await duck.refresh_utm_silver_layer()
        assert owed(before) == parsed == {401, 402}

        with patch("core.pg.require_revision", new=AsyncMock()):
            shipped = await pg_order_utm.ship_order_utm(duck, pool=pool)
        assert shipped["rows"] == 2, shipped
        # Carried through DuckDB's TIMESTAMPTZ and asyncpg into Postgres,
        # `parsed_at` is the same instant as the `updated_at` Postgres holds:
        # an offset anywhere on that path would read as a stale verdict here.
        assert await findings(conn) == []

        # An edit: KeyCRM moves `updated_at`, the sync writes both stores.
        await both_stores(duck, conn, 401, COMMENT + "; utm_content: v2", edited)
        after_edit = await findings(conn, max_samples=100)
        assert [i.check_name for i in after_edit] == ["pg_order_utm_stale"]
        assert owed(after_edit) == await duck.refresh_utm_silver_layer() == {401}

        with patch("core.pg.require_revision", new=AsyncMock()):
            await pg_order_utm.ship_order_utm(duck, pool=pool)
        assert await findings(conn) == []
