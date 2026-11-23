"""The 05:30 reconciliation compares each order's discount, and repairs it (revision 0038).

Revision 0038 lands KeyCRM's discount with every order and versions none of
it: a discount KeyCRM moved without `updated_at`, or one a writer left stale
or NULL here — an image below 0038 rewrites an order's lines without the
discount columns — was seen by nothing.

Driven through the real `_run_dq_reconciliation`, the real Postgres arm and the
real repair, over a KeyCRM snapshot made by the job's own `_process_batch` from
KeyCRM-shaped payloads, as `test_recon_compares_expenses.py` is. Whatever else
the shared database holds compares equal: the snapshot carries it as Postgres
holds it, without discount facts. On the commit before the change no order
here was named or re-fetched.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import pg_orders_write as chain

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 991_300
(MOVED, LINES_LOST, HEADER_LOST, ROUNDED, NO_KEY, BROKEN_IDENTITY, REFUSED,
 SAME, INFLIGHT) = (BASE + i for i in range(9))
IDS = [MOVED, LINES_LOST, HEADER_LOST, ROUNDED, NO_KEY, BROKEN_IDENTITY, REFUSED,
       SAME, INFLIGHT]
OLD = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=40)
HEADER_KEYS = ("products_total", "total_discount", "discount_amount",
               "discount_percent", "discount_data")
LINE_KEYS = ("price", "total_discount", "discount_percent", "discount_amount",
             "loyalty_discount", "individual_discount")


def _line(per_unit: object = "389", qty: int = 2) -> dict:
    """980 → 591 is 389 off each unit."""
    return {"name": "Товар", "quantity": qty, "price_sold": "591", "price": "980",
            "total_discount": per_unit, "discount_percent": "39.6939",
            "discount_amount": 0, "loyalty_discount": 0, "individual_discount": 0,
            "offer": {"product_id": 701}}


def _payload(oid: int, *, updated: datetime = OLD, **over) -> dict:
    """Two units at 591 and the order 10 % off: 1182.00, 118.20 off, 1063.80."""
    p = {
        "id": oid, "source_id": 4, "status_id": 12, "status_group_id": 4,
        "grand_total": "1063.80", "ordered_at": OLD.isoformat(),
        "created_at": OLD.isoformat(), "updated_at": updated.isoformat(),
        "buyer": {"id": 5401}, "manager": {"id": 4},
        "manager_comment": None, "promocode": None,
        "products_total": "1182.00", "total_discount": "118.20",
        "discount_amount": 0, "discount_percent": 10, "discount_data": [],
        "products": [_line()], "expenses": [],
    }
    p.update(over)
    return p


def _old_shape(p: dict) -> dict:
    p = {k: v for k, v in p.items() if k not in HEADER_KEYS}
    p["products"] = [{k: v for k, v in line.items() if k not in LINE_KEYS}
                     for line in p["products"]]
    return p


# What KeyCRM serves at 05:30, and by id to the repair.
SERVED = {
    # Both figures moved by ten kopecks, `updated_at` did not; the total did
    # not move (1182.10 - 118.30 is still 1063.80).
    MOVED: _payload(MOVED, products_total="1182.10", total_discount="118.30"),
    LINES_LOST: _payload(LINES_LOST),
    HEADER_LOST: _payload(HEADER_LOST),
    # Rounded by the parse on the way in and on the way to the comparison:
    # 1182.005 (a float, read by its text) and 0.125 half away from zero,
    # the line's unit to four places before it is multiplied.
    ROUNDED: _payload(ROUNDED, products_total=1182.005, total_discount="0.125",
                      products=[_line("33.33335", 3)]),
    NO_KEY: _old_shape(_payload(NO_KEY)),
    # 100 of shipping: products_total - total_discount is not the total.
    BROKEN_IDENTITY: _payload(BROKEN_IDENTITY, grand_total="1163.80"),
    # KeyCRM moved the discount and now serves a NUL in the comment: chain 3
    # refuses the order whole, so no re-fetch could write the discount.
    REFUSED: _payload(REFUSED, total_discount="200.00", manager_comment="utm\x00"),
    SAME: _payload(SAME),
    INFLIGHT: _payload(INFLIGHT, total_discount="300.00"),
}
# What Postgres holds before the run: written from these payloads by the
# chain, then two of them broken as an image below 0038 leaves them.
HELD = {**SERVED,
        MOVED: _payload(MOVED),
        NO_KEY: _payload(NO_KEY, total_discount="99.00"),
        REFUSED: _payload(REFUSED),
        INFLIGHT: _payload(INFLIGHT)}


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM bronze.expenses WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute(
            "DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", IDS)
        await conn.execute(
            "DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", IDS)


@pytest_asyncio.fixture
async def pool():
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        saved = await conn.fetch(
            "SELECT * FROM meta.mirror_state WHERE table_name = 'bronze.orders'")
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = 'bronze.orders'")
        for row in saved:
            cols = list(row.keys())
            await conn.execute(
                f"INSERT INTO meta.mirror_state ({', '.join(cols)}) VALUES "
                f"({', '.join(f'${i + 1}' for i in range(len(cols)))})", *row.values())
    await pool.close()


async def _hold(pool, held):
    await chain.upsert_orders_with_expenses([held[oid] for oid in IDS])
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE meta.mirror_state SET backfilled_at = now() "
            "WHERE table_name = 'bronze.orders'")
        # The in-flight order moved after the watermark: what the job's
        # exclusion holds back, on both sides.
        await conn.execute("UPDATE bronze.orders SET updated_at = now() WHERE id = $1",
                           INFLIGHT)


async def _break_as_an_older_image_does(pool):
    """An image below 0038 deletes an order's lines and inserts them back
    naming its own six columns, so the line's discount columns are NULL; and
    an order it inserts holds NULL in the header's five."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE bronze.order_products SET price = NULL, total_discount = NULL, "
            "discount_percent = NULL, discount_amount = NULL, "
            "loyalty_discount = NULL, individual_discount = NULL "
            "WHERE order_id = $1", LINES_LOST)
        await conn.execute(
            "UPDATE bronze.orders SET products_total = NULL, total_discount = NULL, "
            "discount_amount = NULL, discount_percent = NULL, discount_data = NULL "
            "WHERE id = $1", HEADER_LOST)


async def _header(pool, oid):
    async with pool.acquire() as conn:
        r = await conn.fetchrow(
            "SELECT products_total, total_discount, discount_percent "
            "FROM bronze.orders WHERE id = $1", oid)
    return tuple(r) if r else None


async def _lines(pool, oid):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT price, total_discount, quantity FROM bronze.order_products "
            "WHERE order_id = $1 ORDER BY id", oid)
    return [tuple(r) for r in rows]


async def _versions(pool):
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT count(*) FROM app.order_versions WHERE order_id = ANY($1::int[])", IDS)


class _KeyCRM:
    """KeyCRM by id, as the repair asks it: the order as served at 05:30."""

    def __init__(self):
        self.asked: list = []

    async def get_order(self, order_id, include=None):
        self.asked.append(int(order_id))
        assert "products" in (include or "")
        return SERVED[int(order_id)]


async def _run(pool, sched):
    from core.reconciliation_io import _process_batch, postgres_orders_in_window

    async def snapshot(window_start, window_end, *, watermark):
        held = await postgres_orders_in_window(
            pool, window_start, window_end, watermark=watermark)
        assert set(IDS) - {INFLIGHT} <= set(held), "the fixture is not in the window"
        kc = {oid: facts for oid, facts in held.items() if oid not in IDS}
        inflight: set = set()
        _process_batch([dict(SERVED[oid], updated_at=(
                            datetime.now(timezone.utc) if oid == INFLIGHT else OLD
                        ).isoformat()) for oid in IDS],
                       kc, watermark, window_start, window_end, inflight)
        assert inflight == {INFLIGHT}
        return kc, 3, inflight

    keycrm = _KeyCRM()
    with patch("core.reconciliation_io.keycrm_orders_in_window", new=snapshot), \
         patch("core.sync_service.get_async_client", new=AsyncMock(return_value=keycrm)):
        result = await sched._run_dq_reconciliation()
    return result, keycrm


def _scheduler():
    from core.scheduler import BackgroundScheduler

    sched = BackgroundScheduler()
    sched._persist_postgres_reconciliation = AsyncMock()
    sched._reconcile_clickhouse = AsyncMock(return_value=None)
    return sched


def _named(sched):
    persisted = sched._persist_postgres_reconciliation.await_args.args[0]
    assert persisted["error"] is None
    found: dict = {}
    for d in persisted["discrepancies"]:
        for oid in d.order_ids:
            if oid in IDS:
                found.setdefault(oid, set()).add((d.diff_class.value, d.field, d.severity.value))
    return found


@pytest.mark.asyncio
async def test_an_order_whose_discount_differs_is_reported_and_repaired(pool):
    await _hold(pool, HELD)
    await _break_as_an_older_image_does(pool)
    versions_before = await _versions(pool)

    sched = _scheduler()
    result, keycrm = await _run(pool, sched)

    assert result["error"] is None
    header = {("VALUE_MISMATCH", "products_total", "WARN"),
              ("VALUE_MISMATCH", "total_discount", "WARN")}
    lines = {("VALUE_MISMATCH", "line_discount", "WARN")}
    # Moved on KeyCRM's side: both figures. Lost on ours: the header's two,
    # or the lines' alone — the header comparison could not have seen it.
    # Not named: the figures that only round alike, the order fetched
    # without the keys, the one whose total is not products_total less the
    # discount, the one chain 3 refuses, the one in flight, and the same.
    assert _named(sched) == {MOVED: header, HEADER_LOST: header, LINES_LOST: lines}

    # Re-fetched by id, in the existing batch — and nothing else of ours.
    assert sorted(keycrm.asked) == [MOVED, LINES_LOST, HEADER_LOST]
    assert result["repair"]["repaired"] == 3
    assert await _header(pool, MOVED) == (Decimal("1182.10"), Decimal("118.30"),
                                          Decimal("10.0000"))
    assert await _header(pool, HEADER_LOST) == (Decimal("1182.00"), Decimal("118.20"),
                                                Decimal("10.0000"))
    assert await _lines(pool, LINES_LOST) == [(Decimal("980.00"), Decimal("389.0000"), 2)]
    # The discount is not versioned: its repair archives nothing.
    assert await _versions(pool) == versions_before
    # Untouched: what KeyCRM said nothing about, what chain 3 refuses, and
    # what was held back.
    assert await _header(pool, NO_KEY) == (Decimal("1182.00"), Decimal("99.00"),
                                           Decimal("10.0000"))
    assert await _header(pool, REFUSED) == (Decimal("1182.00"), Decimal("118.20"),
                                            Decimal("10.0000"))
    assert await _header(pool, INFLIGHT) == (Decimal("1182.00"), Decimal("118.20"),
                                             Decimal("10.0000"))


@pytest.mark.asyncio
async def test_the_store_holds_what_the_snapshot_compares(pool):
    """Both sides through `_discount`: the rounded order is held as the
    snapshot reads it, so it is not named. Mutation: compare KeyCRM's figure
    as served."""
    await _hold(pool, HELD)
    assert await _header(pool, ROUNDED) == (Decimal("1182.01"), Decimal("0.13"),
                                            Decimal("10.0000"))
    assert await _lines(pool, ROUNDED) == [(Decimal("980.00"), Decimal("33.3334"), 3)]

    from core.reconciliation_io import postgres_discounts_of

    stored = await postgres_discounts_of(pool, [ROUNDED, LINES_LOST, BASE + 99])
    assert stored[ROUNDED] == {"products_total": Decimal("1182.01"),
                               "total_discount": Decimal("0.13"),
                               "line_discount": Decimal("100.0002")}
    assert BASE + 99 not in stored                      # not held: not asked
    await _break_as_an_older_image_does(pool)
    assert (await postgres_discounts_of(pool, [LINES_LOST]))[LINES_LOST][
        "line_discount"] is None


@pytest.mark.asyncio
async def test_a_second_run_after_the_repair_is_clean(pool):
    """The repair is what makes the finding go away: the next morning the
    same snapshot compares equal, and nothing is asked again."""
    await _hold(pool, HELD)
    await _break_as_an_older_image_does(pool)
    await _run(pool, _scheduler())

    sched = _scheduler()
    result, keycrm = await _run(pool, sched)

    assert result["error"] is None
    assert _named(sched) == {}
    assert keycrm.asked == []
