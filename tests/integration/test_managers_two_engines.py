"""The managers screen reads the Kyiv year out of Postgres's Silver.

`GET /api/managers` sums a year of Silver per manager and sales type — the one
screen an admin classifies managers from. It rode `KS_READ_SILVER`, and this
proved DuckDB and Postgres handed the route the same rows in the same order.

ONE ENGINE SINCE STEP 14 (S14-06)

`get_manager_sales_365d` reads Postgres alone. Each answer is held to the rows
computed here, in Python, from the fixture — the truth this file already held
both engines to (`_expected`), and the only literal a window relative to
today can have (rule 9 of the step-14 plan: DuckDB's answer was asserted equal
to it until its side was deleted). The store's DuckDB connection is a trap.

SILVER IS DERIVED, NOT COPIED

Bronze goes into Postgres and `core.pg_silver.rebuild_silver` — the production
path — builds Silver. Copying a Silver from elsewhere would hand Postgres
somebody else's Kyiv dates, and the orders near midnight below would then
prove nothing about Postgres.

THE FIXTURE THINKS IN KYIV

Every "N days ago" is a Kyiv calendar day, built as a Kyiv wall-clock time and
never as `date.today()` beside a UTC stamp — the combination that made a
fixture flaky for three hours a day twice in this repository. The window edge
is `today in Kyiv − 365 days`, and it gets four orders: 00:30 on the edge day,
whose UTC date is the day before and which is in; 23:30 the evening before,
which is out; and noon on either side.

WHAT AN ENGINE CANNOT SEE OF ITSELF

Under CI the server sits in UTC, so a window that took its day from
`CURRENT_DATE` would be wrong there and right in production. The expected rows
are therefore computed here, in Python, from the Kyiv date — and
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
from tests.integration.pg_tab_seed import store_that_never_reaches_duckdb

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
# A baseline interval for each, as the migrations seed them, so the CASE does
# not fall through to the `RETAIL_MANAGER_IDS` constant.
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
        # window — and the Python expectation follows it.
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
async def in_postgres(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.delenv("KS_READ_SILVER", raising=False)
    today = _today_in_kyiv()
    orders = _orders(today)

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    async with pool.acquire() as conn:
        # `rebuild_silver` stamps Silver's watermark; put back whatever the
        # database held, so another module's check does not read ours.
        saved = await conn.fetchrow(
            "SELECT * FROM meta.mirror_state WHERE table_name = 'silver.orders'")
    try:
        await _seed_postgres(pool, orders)
        store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the managers screen")
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


def _plain(rows):
    """Money as Decimal, whatever the driver chose to hand back."""
    return [(r[0], r[1], Decimal(str(r[2]))) for r in rows]


class TestTheScreenReadsTheKyivYear:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("switch", [None, "duckdb", "junk-s14-06"],
                             ids=["unset", "duckdb", "junk"])
    async def test_the_rows_are_the_kyiv_year_in_the_body_s_order(
        self, in_postgres, monkeypatch, switch,
    ):
        """Against a truth no engine computed: the edge day's first minute is
        in, the evening before is out, a return and a retired source bring
        nothing — whatever `KS_READ_SILVER` says, which nothing reads since
        step 14's S14-06."""
        store, _pool, orders, today = in_postgres
        if switch is not None:
            monkeypatch.setenv("KS_READ_SILVER", switch)
        assert _plain(await store.get_manager_sales_365d()) == _expected(orders, today)

    @pytest.mark.asyncio
    async def test_the_tie_arrives_in_the_body_s_order(self, in_postgres):
        """The route joins a manager's types by revenue and keeps arrival order
        on a tie, so the label on the screen is decided here."""
        store, _pool, _orders, _today = in_postgres
        rows = await store.get_manager_sales_365d()
        staff = [r[1] for r in rows if r[0] == STAFF_MANAGER]
        assert staff == ["exhibition", "internal"]


class TestSilverPlacesTheOrderOnItsKyivDay:
    @pytest.mark.asyncio
    async def test_the_boundary_orders_carry_their_kyiv_date(self, in_postgres):
        """The window is only as Kyiv as `order_date` is: 21:30 UTC must be
        the next day."""
        _store, pool, orders, _today = in_postgres
        ids = [1, 2, 9, 10, 11, 12]
        async with pool.acquire() as conn:
            pg = {r["id"]: r["order_date"] for r in await conn.fetch(
                "SELECT id, order_date FROM silver.orders WHERE id = ANY($1::int[])", ids)}

        by_id = {o[0]: o for o in orders}
        truth = {i: by_id[i][4].astimezone(KYIV).date() for i in ids}
        assert pg == truth
        assert truth[2] == date(2026, 9, 17)
