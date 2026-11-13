"""Chain 3 keeps what it deletes: `app.expense_deletions` (revision 0036), on a real server.

Owner decision O2 (a) made chain 3 delete the expenses KeyCRM no longer lists
under an order, and the INFO line was the only trace of what they were. The
analysts' captures (design revision 3.1, §3.3) keep each deleted row —
expense, order, type, amount, when — written beside the DELETE from its own
RETURNING, in the same transaction, under a savepoint of its own
(`analytics_captures.capture_in_transaction`).

Driven as the sync drives it, `pg_orders_write.upsert_orders_with_expenses`,
on payloads shaped like KeyCRM's `include=…,expenses`. Each test names the
mutation it kills.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import analytics_captures as cap
from core import pg_orders_write as chain

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TABLE = "app.expense_deletions"
BASE = 990_900
IDS = list(range(BASE, BASE + 10))
EXPENSES = (BASE * 10, BASE * 10 + 99)
T0 = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
LATER = T0 + timedelta(minutes=5)
PAID = datetime(2026, 9, 21, 8, 30, tzinfo=timezone.utc)


def _cost(eid: int, amount="10.50", type_id=1) -> dict:
    return {"id": eid, "expense_type_id": type_id, "amount": amount,
            "description": "Нова пошта", "status": "paid",
            "payment_date": PAID.isoformat().replace("+00:00", "Z"),
            "created_at": T0.isoformat().replace("+00:00", "Z")}


def _payload(oid: int, expenses, *, status: int = 12, updated: datetime = T0,
             comment="utm_source=ig") -> dict:
    return {
        "id": oid, "source_id": 1, "status_id": status, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": T0.isoformat(),
        "created_at": T0.isoformat(), "updated_at": updated.isoformat(),
        "buyer": {"id": 5401}, "manager": {"id": 4},
        "manager_comment": comment, "promocode": None,
        "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 701}}],
        "expenses": expenses,
    }


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM bronze.expenses WHERE order_id = ANY($1::int[]) "
            "OR id BETWEEN $2 AND $3", IDS, *EXPENSES)
        await conn.execute(
            "DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", IDS)
        await conn.execute(
            "DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute(
            "DELETE FROM app.expense_deletions WHERE expense_id BETWEEN $1 AND $2 "
            "OR order_id = ANY($3::int[])", *EXPENSES, IDS)
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            ["bronze.orders", "bronze.order_products", "bronze.expenses",
             cap.state_row(TABLE)])


@pytest_asyncio.fixture
async def pool(monkeypatch):
    monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    await pool.close()


async def _write(payloads, **kw):
    return await chain.upsert_orders_with_expenses(payloads, **kw)


async def _logged(pool) -> list:
    """`(expense_id, order_id, expense_type_id, amount)` kept for this file's
    orders, in expense order."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT expense_id, order_id, expense_type_id, amount FROM app.expense_deletions "
            "WHERE order_id = ANY($1::int[]) ORDER BY expense_id, deleted_at", IDS)
    return [tuple(r) for r in rows]


async def _stored(pool, oid) -> set:
    async with pool.acquire() as conn:
        return {r["id"] for r in await conn.fetch(
            "SELECT id FROM bronze.expenses WHERE order_id = $1", oid)}


async def _stamp(pool):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT last_ok_at, failures_since_ok, last_error, last_rows "
            "FROM meta.mirror_state WHERE table_name = $1", cap.state_row(TABLE))


class TestWhatIsDeletedIsKept:
    @pytest.mark.asyncio
    async def test_a_refetch_that_drops_one_expense_logs_exactly_it(self, pool):
        """KeyCRM serves the order with one of its two expenses gone: that one
        row is kept, with its order, type and amount, stamped in the deleting
        transaction; the expense still listed is not. Mutations: capture the
        order's every stored expense, or the listed one, or none."""
        kept, dropped = BASE * 10, BASE * 10 + 1
        await _write([_payload(BASE, [_cost(kept), _cost(dropped, "7.25", type_id=2)])])
        assert await _logged(pool) == [] and await _stamp(pool) is None

        async with pool.acquire() as conn:
            before = await conn.fetchval("SELECT now()")
        result, _ = await _write([_payload(BASE, [_cost(kept)])])

        assert result.expenses_deleted == 1 and await _stored(pool, BASE) == {kept}
        assert await _logged(pool) == [(dropped, BASE, 2, Decimal("7.25"))]
        async with pool.acquire() as conn:
            deleted_at = await conn.fetchval(
                "SELECT deleted_at FROM app.expense_deletions WHERE expense_id = $1",
                dropped)
            after = await conn.fetchval("SELECT now()")
        assert before <= deleted_at <= after
        stamp = await _stamp(pool)
        assert (stamp["failures_since_ok"], stamp["last_error"], stamp["last_rows"]) == \
            (0, None, 1)
        assert before <= stamp["last_ok_at"] <= after

    @pytest.mark.asyncio
    async def test_an_empty_list_logs_every_one_as_stored(self, pool):
        """An empty list deletes all of the order's expenses, and each is kept
        as the store held it — a NULL type stays NULL, an amount to the
        kopeck. Mutation: capture from the payload instead of the deleted
        rows (it lists nothing here)."""
        a, b = BASE * 10 + 2, BASE * 10 + 3
        await _write([_payload(BASE, [_cost(a, "0.01")])])
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bronze.expenses (id, order_id, amount) VALUES ($1, $2, 1234567.89)",
                b, BASE)
        result, _ = await _write([_payload(BASE, [])])
        assert result.expenses_deleted == 2
        assert await _logged(pool) == [(a, BASE, 1, Decimal("0.01")),
                                       (b, BASE, None, Decimal("1234567.89"))]
        assert (await _stamp(pool))["last_rows"] == 2

    @pytest.mark.asyncio
    async def test_the_same_expense_deleted_twice_is_two_rows(self, pool):
        """KeyCRM lists it again, then drops it again: two deletions, two
        rows. Mutation: a capture keyed on the expense alone."""
        eid = BASE * 10 + 4
        await _write([_payload(BASE, [_cost(eid)])])
        await _write([_payload(BASE, [])])
        await _write([_payload(BASE, [_cost(eid, "11.00")], status=19, updated=LATER)])
        await _write([_payload(BASE, [], status=19, updated=LATER)])
        assert await _logged(pool) == [(eid, BASE, 1, Decimal("10.50")),
                                       (eid, BASE, 1, Decimal("11.00"))]


class TestNothingDeletedNothingKept:
    @pytest.mark.asyncio
    async def test_an_idle_page_and_a_moved_expense_keep_nothing_and_stamp_nothing(self, pool):
        """The capture runs only when the DELETE deleted something: an
        identical page deletes nothing, and an expense KeyCRM moved to another
        order is rewritten, not deleted. No stamp either — the capture is
        judged on its failures, and a stamp on every tick would clear one
        within the minute. Mutation: call the capture on every group."""
        moved = BASE * 10 + 5
        page = [_payload(BASE, [_cost(moved)]), _payload(BASE + 1, [])]
        await _write(page)
        await _write(page)
        result, _ = await _write([_payload(BASE, []), _payload(BASE + 1, [_cost(moved)])])
        assert result.expenses_deleted == 0
        assert await _stored(pool, BASE + 1) == {moved}
        assert await _logged(pool) == []
        assert await _stamp(pool) is None


class TestTheCaptureNeverFailsTheWrite:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("broken, error", [
        # A DataError: the class the writer answers by retrying order by
        # order and refusing the order, so a leak would cost the deletion.
        (chain._EXPENSE_DELETIONS_CAPTURE_SQL + " WHERE 1 / 0 = 1", "DivisionByZeroError"),
        # Anything else: a leak would raise out of the writer.
        (chain._EXPENSE_DELETIONS_CAPTURE_SQL.replace("amount)", "no_such_column)"),
         "UndefinedColumnError"),
    ], ids=["data-error", "other-error"])
    async def test_a_capture_that_fails_leaves_the_deletion_and_the_order_written(
            self, pool, monkeypatch, caplog, broken, error):
        """The capture fails inside the writer's transaction: its savepoint
        rolls back alone, the failure is stamped and logged, and everything
        the writer does — the deletion, and the order header it writes AFTER
        the capture in the same transaction — commits. Mutations: no
        savepoint (the transaction is aborted and the header write fails), or
        a capture that raises into the writer."""
        dropped = BASE * 10 + 6
        await _write([_payload(BASE, [_cost(BASE * 10 + 7), _cost(dropped)])])
        monkeypatch.setattr(chain, "_EXPENSE_DELETIONS_CAPTURE_SQL", broken)

        caplog.set_level(logging.INFO)
        result, _ = await _write([_payload(BASE, [_cost(BASE * 10 + 7)],
                                           status=19, updated=LATER)])

        assert (result.expenses_deleted, result.failed, result.changed_ids) == (1, 0, [BASE])
        assert await _stored(pool, BASE) == {BASE * 10 + 7}
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT status_id FROM bronze.orders WHERE id = $1", BASE) == 19
        assert await _logged(pool) == []
        stamp = await _stamp(pool)
        assert stamp["failures_since_ok"] == 1 and stamp["last_ok_at"] is None
        assert stamp["last_error"].startswith(f"{error}: ")
        failed = [r for r in caplog.records
                  if r.name == cap.__name__ and r.levelno == logging.ERROR]
        assert len(failed) == 1 and TABLE in failed[0].getMessage()
        assert [r for r in caplog.records if r.name == chain.__name__
                and r.getMessage().startswith(f"orders: expense {dropped} ")]

    @pytest.mark.asyncio
    async def test_the_next_deletion_kept_clears_the_failure(self, pool, monkeypatch):
        """A failure is held in the stamp until a capture is kept again — not
        by an idle tick. Mutation: stamp ok without a deletion."""
        await _write([_payload(BASE, [_cost(BASE * 10 + 8)]),
                      _payload(BASE + 1, [_cost(BASE * 10 + 9)])])
        with monkeypatch.context() as m:
            m.setattr(chain, "_EXPENSE_DELETIONS_CAPTURE_SQL",
                      chain._EXPENSE_DELETIONS_CAPTURE_SQL + " WHERE 1 / 0 = 1")
            await _write([_payload(BASE, [])])
        await _write([_payload(BASE + 1, [_cost(BASE * 10 + 9)])])
        assert (await _stamp(pool))["failures_since_ok"] == 1

        await _write([_payload(BASE + 1, [])])
        stamp = await _stamp(pool)
        assert (stamp["failures_since_ok"], stamp["last_rows"]) == (0, 1)
        assert await _logged(pool) == [(BASE * 10 + 9, BASE + 1, 1, Decimal("10.50"))]

    @pytest.mark.asyncio
    async def test_a_deletion_rolled_back_takes_its_capture_with_it(self, pool, monkeypatch):
        """Same transaction as the deletion: an order Postgres itself refuses
        after its expense was deleted and captured rolls both back, in the
        batch and again order by order, while the other order's deletion and
        its capture commit. Mutation: capture in a transaction of its own."""
        await _write([_payload(BASE, [_cost(BASE * 10)]),
                      _payload(BASE + 1, [_cost((BASE + 1) * 10)])])
        monkeypatch.setattr(chain, "order_refusal", lambda row: None)
        result, _ = await _write([_payload(BASE, [], comment="utm\x00", updated=LATER),
                                  _payload(BASE + 1, [], updated=LATER)])
        assert (result.failed, result.expenses_deleted) == (1, 1)
        assert await _stored(pool, BASE) == {BASE * 10}
        assert await _logged(pool) == [((BASE + 1) * 10, BASE + 1, 1, Decimal("10.50"))]
        assert (await _stamp(pool))["failures_since_ok"] == 0


class TestTheSwitch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", ["off", "0", "false"])
    async def test_off_deletes_as_ever_and_keeps_nothing(self, pool, monkeypatch, value):
        """`KS_ANALYTICS_CAPTURES=off` stops the capture and only the
        capture: the deletion is made and counted, nothing is kept, nothing
        stamped. Mutation: a capture that ignores the switch."""
        await _write([_payload(BASE, [_cost(BASE * 10), _cost(BASE * 10 + 1)])])
        monkeypatch.setenv(cap.SWITCH_ENV, value)
        result, _ = await _write([_payload(BASE, [_cost(BASE * 10)])])
        assert result.expenses_deleted == 1 and await _stored(pool, BASE) == {BASE * 10}
        assert await _logged(pool) == []
        assert await _stamp(pool) is None
