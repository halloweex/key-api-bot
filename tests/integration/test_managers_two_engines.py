"""The managers screen reads the same year from DuckDB and Postgres.

`GET /api/managers` sums a year of Silver per manager and sales type — the one
screen an admin classifies managers from. It rides `KS_READ_SILVER`, and this
proves the two engines hand the route the same rows in the same order.

BOTH SILVERS ARE DERIVED, NOT COPIED

The same Bronze goes into both stores and each engine builds its own Silver —
DuckDB through `refresh_warehouse_layers`, Postgres through
`core.pg_silver.rebuild_silver`, the production path. Copying DuckDB's Silver
across would hand Postgres DuckDB's Kyiv dates, and the orders near midnight
below would then prove nothing about Postgres.

THE FIXTURE THINKS IN KYIV

Every "N days ago" is a Kyiv calendar day, built as a Kyiv wall-clock time and
never as `date.today()` beside a UTC stamp — the combination that made a
fixture flaky for three hours a day twice in this repository. The window edge
is `today in Kyiv − 365 days`, and it gets four orders: 00:30 on the edge day,
whose UTC date is the day before and which is in; 23:30 the evening before,
which is out; and noon on either side.

WHAT A DIFFERENTIAL TEST CANNOT SEE

Under CI both engines sit in UTC, so a window that took its day from
`CURRENT_DATE` would be wrong identically on both. The expected rows are
therefore also computed here, in Python, from the Kyiv date — and
`tests/unit/test_no_inherited_timezone.py` forbids the spelling outright.

Skipped without `KS_PG_DSN`; CI and `deploy/gate_with_stores.sh` supply one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from core.duckdb_constants import B2B_MANAGER_ID, REVENUE_SOURCE_IDS
from core.duckdb_store import DuckDBStore

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

KYIV = ZoneInfo("Europe/Kyiv")
RETAIL_MANAGER = 22      # retail through a classification interval
STAFF_MANAGER = 34       # classified, not retail — so `internal`
RETURN_STATUS = 19

# (id, name, is_retail)
MANAGERS = [
    (B2B_MANAGER_ID, "Wholesale", False),
    (RETAIL_MANAGER, "Retail", True),
    (STAFF_MANAGER, "Staff", False),
]
# A baseline interval for each, as the DuckDB migration seeds them, so neither
# engine falls through to the `RETAIL_MANAGER_IDS` constant.
CLASSIFICATIONS = [
    (B2B_MANAGER_ID, False, date(1970, 1, 1)),
    (RETAIL_MANAGER, True, date(1970, 1, 1)),
    (STAFF_MANAGER, False, date(1970, 1, 1)),
]

PG_TABLES = ("silver.orders", "bronze.orders", "bronze.managers",
             "app.manager_classifications")


def _kyiv(day: date, hour: int, minute: int = 0) -> datetime:
    """A Kyiv wall-clock time, as the UTC instant the stores hold."""
    return datetime.combine(day, time(hour, minute), tzinfo=KYIV).astimezone(timezone.utc)


def _today_in_kyiv() -> date:
    return datetime.now(timezone.utc).astimezone(KYIV).date()


def _orders(today: date):
    """(id, source_id, status_id, grand_total, ordered_at, manager_id, buyer_id,
    the sales_type Silver should give it)."""
    edge = today - timedelta(days=365)
    return [
        (1, 1, 1, Decimal("1200.50"), _kyiv(today - timedelta(days=1), 12),
         None, 7, "retail"),
        # The brief's boundary case: 16.09 in UTC, 00:30 on 17.09 in Kyiv.
        # Fixed rather than relative, so a year after 2026-09-17 it leaves the
        # window on both engines at once — the Python expectation follows it.
        (2, 4, 1, Decimal("799.99"), datetime(2026, 9, 16, 21, 30, tzinfo=timezone.utc),
         None, 8, "retail"),
        (3, 2, 1, Decimal("2500.00"), _kyiv(today - timedelta(days=10), 12),
         RETAIL_MANAGER, 7, "retail"),
        # A return: no revenue on this screen, as before.
        (4, 1, RETURN_STATUS, Decimal("900.00"), _kyiv(today - timedelta(days=5), 12),
         RETAIL_MANAGER, 7, "retail"),
        # Opencart, retired: no revenue either.
        (5, 3, 1, Decimal("800.00"), _kyiv(today - timedelta(days=6), 12),
         RETAIL_MANAGER, 7, "retail"),
        (6, 1, 1, Decimal("7000.00"), _kyiv(today - timedelta(days=20), 12),
         B2B_MANAGER_ID, 9, "b2b"),
        # One manager, two sales types, equal money: the tie the route breaks
        # by arrival order, which is why the body is ordered.
        (7, 4, 1, Decimal("450.00"), _kyiv(today - timedelta(days=30), 12),
         STAFF_MANAGER, 9, "internal"),
        (8, 5, 1, Decimal("450.00"), _kyiv(today - timedelta(days=40), 12),
         STAFF_MANAGER, 9, "exhibition"),
        # The window edge, four ways.
        (9, 1, 1, Decimal("111.11"), _kyiv(edge, 0, 30),
         RETAIL_MANAGER, 7, "retail"),
        (10, 1, 1, Decimal("222.22"), _kyiv(edge - timedelta(days=1), 23, 30),
         RETAIL_MANAGER, 7, "retail"),
        (11, 2, 1, Decimal("333.33"), _kyiv(edge, 12),
         B2B_MANAGER_ID, 9, "b2b"),
        (12, 2, 1, Decimal("444.44"), _kyiv(edge - timedelta(days=1), 12),
         B2B_MANAGER_ID, 9, "b2b"),
    ]


def _expected(orders, today: date):
    """The screen's rows, computed without either engine."""
    floor = today - timedelta(days=365)
    totals: dict = {}
    for _id, source, status, total, at, manager, _buyer, sales_type in orders:
        if status == RETURN_STATUS or source not in REVENUE_SOURCE_IDS:
            continue
        if at.astimezone(KYIV).date() < floor:
            continue
        key = (manager, sales_type)
        totals[key] = totals.get(key, Decimal("0")) + total
    # manager_id NULLS FIRST, then sales_type — the body's own order.
    return sorted(
        ((m, s, v) for (m, s), v in totals.items()),
        key=lambda r: (r[0] is not None, r[0] or 0, r[1]),
    )


async def _seed_duckdb(store, orders):
    async with store.connection() as conn:
        conn.executemany(
            "INSERT INTO managers (id, name, is_retail) VALUES (?,?,?)", MANAGERS)
        conn.executemany(
            "INSERT INTO manager_classifications (manager_id, is_retail, valid_from)"
            " VALUES (?,?,?)", CLASSIFICATIONS)
        conn.executemany(
            "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at,"
            " buyer_id, manager_id) VALUES (?,?,?,?,?,?,?)",
            [(oid, src, status, total, at, buyer, manager)
             for oid, src, status, total, at, manager, buyer, _ in orders])
    await store.refresh_warehouse_layers(trigger="manual")


async def _seed_postgres(pool, orders):
    from core.pg_silver import rebuild_silver

    async with pool.acquire() as conn:
        for table in PG_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.executemany(
            "INSERT INTO bronze.managers (id, name, is_retail) VALUES ($1,$2,$3)",
            MANAGERS)
        await conn.executemany(
            "INSERT INTO app.manager_classifications (manager_id, is_retail, valid_from)"
            " VALUES ($1,$2,$3)", CLASSIFICATIONS)
        await conn.executemany(
            "INSERT INTO bronze.orders (id, source_id, status_id, grand_total,"
            " ordered_at, created_at, updated_at, buyer_id, manager_id)"
            " VALUES ($1,$2,$3,$4,$5,$5,$5,$6,$7)",
            [(oid, src, status, total, at, buyer, manager)
             for oid, src, status, total, at, manager, buyer, _ in orders])

    # Derived, not copied — the production path.
    with patch("core.pg.require_revision", new=AsyncMock()):
        await rebuild_silver(pool=pool)


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.delenv("KS_READ_SILVER", raising=False)
    today = _today_in_kyiv()
    orders = _orders(today)

    store = DuckDBStore(db_path=tmp_path / "managers.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    async with pool.acquire() as conn:
        # `rebuild_silver` stamps Silver's watermark; put back whatever the
        # database held, so another module's check does not read ours.
        saved = await conn.fetchrow(
            "SELECT * FROM meta.mirror_state WHERE table_name = 'silver.orders'")
    try:
        await _seed_duckdb(store, orders)
        await _seed_postgres(pool, orders)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield store, pool, orders, today
    finally:
        async with pool.acquire() as conn:
            for table in PG_TABLES:
                await conn.execute(f"DELETE FROM {table}")
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = 'silver.orders'")
            if saved is not None:
                columns = list(saved.keys())
                await conn.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(columns)})"
                    f" VALUES ({', '.join(f'${i + 1}' for i in range(len(columns)))})",
                    *saved.values())
        await pool.close()
        await store.close()


async def _both(store, monkeypatch):
    """Each engine once — and the Postgres leg with DuckDB made fatal.

    `_pg_silver` returns None on any failure and the method falls back, so a
    plain double call can pass while Postgres never answered: the two
    "engines" would agree because they were the same one.
    """
    monkeypatch.delenv("KS_READ_SILVER", raising=False)
    duck = await store.get_manager_sales_365d()

    monkeypatch.setenv("KS_READ_SILVER", "postgres")

    def _no_duckdb(*_a, **_k):
        raise AssertionError(
            "get_manager_sales_365d fell back to DuckDB — Postgres did not "
            "answer, so the comparison would have compared DuckDB with itself"
        )

    with patch.object(type(store), "connection", _no_duckdb):
        postgres = await store.get_manager_sales_365d()
    monkeypatch.delenv("KS_READ_SILVER", raising=False)
    return duck, postgres


def _plain(rows):
    """Money as Decimal, whatever each driver chose to hand back."""
    return [(r[0], r[1], Decimal(str(r[2]))) for r in rows]


class TestTheTwoEnginesAgree:
    @pytest.mark.asyncio
    async def test_the_rows_are_identical_and_in_the_same_order(
        self, both_engines, monkeypatch,
    ):
        store, _pool, _orders, _today = both_engines
        duck, postgres = await _both(store, monkeypatch)
        assert _plain(duck) == _plain(postgres)

    @pytest.mark.asyncio
    async def test_both_are_the_kyiv_year(self, both_engines, monkeypatch):
        """Against a truth neither engine computed: the edge day's first
        minute is in, the evening before is out, a return and a retired source
        bring nothing."""
        store, _pool, orders, today = both_engines
        duck, postgres = await _both(store, monkeypatch)
        expected = _expected(orders, today)
        assert _plain(duck) == expected
        assert _plain(postgres) == expected

    @pytest.mark.asyncio
    async def test_the_tie_arrives_in_the_same_order(self, both_engines, monkeypatch):
        """The route joins a manager's types by revenue and keeps arrival order
        on a tie, so the label on the screen is decided here."""
        store, _pool, _orders, _today = both_engines
        duck, postgres = await _both(store, monkeypatch)
        for rows in (duck, postgres):
            staff = [r[1] for r in rows if r[0] == STAFF_MANAGER]
            assert staff == ["exhibition", "internal"]


class TestBothSilversPlaceTheOrderOnItsKyivDay:
    @pytest.mark.asyncio
    async def test_the_boundary_orders_carry_the_same_kyiv_date(self, both_engines):
        """The window is only as Kyiv as `order_date` is. Each engine derived
        its own, and 21:30 UTC must be the next day in both."""
        store, pool, orders, _today = both_engines
        ids = [1, 2, 9, 10, 11, 12]
        async with store.connection() as conn:
            duck = dict(conn.execute(
                "SELECT id, order_date FROM silver_orders"
                f" WHERE id IN ({','.join('?' * len(ids))})", ids).fetchall())
        async with pool.acquire() as conn:
            pg = {r["id"]: r["order_date"] for r in await conn.fetch(
                "SELECT id, order_date FROM silver.orders WHERE id = ANY($1::int[])", ids)}

        by_id = {o[0]: o for o in orders}
        truth = {i: by_id[i][4].astimezone(KYIV).date() for i in ids}
        assert duck == truth
        assert pg == truth
        assert truth[2] == date(2026, 9, 17)
