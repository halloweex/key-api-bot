"""Chain 3's expenses follow KeyCRM (owner decision O2 (a), A5), on a real server.

Until O2 (a) an order's expenses were upserted and never deleted, and every
expense a payload carried was written on every tick:

- an expense KeyCRM removed from an order stayed in `bronze.expenses` for
  ever, in /expenses' costs and its profit;
- every tick over the 24-hour window rewrote every expense in it — a new
  `xmin` and a restamped `meta.mirror_state` row each time — so no tick that
  carried one was ever idle.

Each test here fails on the commit before the fix, except the ones that hold
what must not change (a payload without the key, a refused order), which name
the mutation they kill. The writer is driven as the sync drives it,
`pg_orders_write.upsert_orders_with_expenses`, on payloads shaped like
KeyCRM's `include=…,expenses`.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, pg_orders_write as chain

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 990_600
IDS = list(range(BASE, BASE + 10))
T0 = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
PAID = datetime(2026, 9, 21, 8, 30, tzinfo=timezone.utc)


def _cost(eid: int, amount="10.50", **over) -> dict:
    cost = {"id": eid, "expense_type_id": 1, "amount": amount,
            "description": "Нова пошта", "status": "paid",
            "payment_date": PAID.isoformat().replace("+00:00", "Z"),
            "created_at": T0.isoformat().replace("+00:00", "Z")}
    cost.update(over)
    return cost


_ABSENT = object()


def _payload(oid: int, expenses=_ABSENT, *, status: int = 12,
             updated: datetime = T0, comment="utm_source=ig") -> dict:
    p = {
        "id": oid, "source_id": 1, "status_id": status, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": T0.isoformat(),
        "created_at": T0.isoformat(), "updated_at": updated.isoformat(),
        "buyer": {"id": 5401}, "manager": {"id": 4},
        "manager_comment": comment, "promocode": None,
        "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 701}}],
    }
    if expenses is not _ABSENT:
        p["expenses"] = expenses
    return p


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM bronze.expenses WHERE order_id = ANY($1::int[]) "
            "OR id BETWEEN $2 AND $3", IDS, BASE * 10, BASE * 10 + 99)
        await conn.execute(
            "DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", IDS)
        await conn.execute(
            "DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            ["bronze.orders", "bronze.order_products", "bronze.expenses"])


@pytest_asyncio.fixture
async def pool():
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    await pool.close()


async def _costs(pool, oid):
    """`{id: (amount, xmin)}` of the order's stored expenses."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, amount, xmin::text AS x FROM bronze.expenses "
            "WHERE order_id = $1 ORDER BY id", oid)
    return {r["id"]: (str(r["amount"]), r["x"]) for r in rows}


async def _expenses_stamp(pool):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT last_ok_at, last_attempted_at, last_rows, xmin::text AS x "
            "FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")


async def _write(payloads, **kw):
    return await chain.upsert_orders_with_expenses(payloads, **kw)


class TestAnExpenseKeyCRMNoLongerListsIsDeleted:
    @pytest.mark.asyncio
    async def test_one_removed_the_other_untouched(self, pool, caplog):
        """KeyCRM serves the order with one of its two expenses gone. On main
        the dropped one stayed for ever, and was counted in /expenses' costs.
        Here it is deleted, logged with its id and amount, and counted; the
        one still listed is not even rewritten."""
        a, b = BASE * 10, BASE * 10 + 1
        await _write([_payload(BASE, [_cost(a), _cost(b, amount="7.25")])])
        kept_before = (await _costs(pool, BASE))[a]

        caplog.set_level(logging.INFO, logger="core.pg_orders_write")
        result, expense_count = await _write([_payload(BASE, [_cost(a)])])

        assert await _costs(pool, BASE) == {a: kept_before}
        assert result.expenses_deleted == 1
        assert (result.changed_ids, result.skipped_unchanged, expense_count) == ([], 1, 1)
        said = [r.getMessage() for r in caplog.records
                if r.name == "core.pg_orders_write" and r.levelno == logging.INFO
                and r.getMessage().startswith("orders: expense ")]
        assert said == [f"orders: expense {b} of order {BASE} deleted, amount 7.25: "
                        "KeyCRM no longer lists it"]

    @pytest.mark.asyncio
    async def test_an_empty_list_deletes_them_all(self, pool):
        await _write([_payload(BASE, [_cost(BASE * 10), _cost(BASE * 10 + 1)])])
        result, expense_count = await _write([_payload(BASE, [])])
        assert await _costs(pool, BASE) == {}
        assert (result.expenses_deleted, expense_count) == (2, 0)

    @pytest.mark.asyncio
    async def test_the_deletion_claims_the_owner_rows_like_any_write(self, pool):
        """A batch whose only change is a deletion is a write: it latches and
        claims, inside the transaction. Mutation: answer idle before the latch
        for a batch with nothing to upsert."""
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bronze.expenses (id, order_id, amount) VALUES ($1, $2, 5)",
                BASE * 10, BASE)
        from core import landing_rows, pg_landing

        landed = landing_rows.landed_orders([_payload(BASE)], with_products=True)
        await pg_landing.write_orders(landed.orders, landed.products,
                                      replace_products=True)
        assert not chain_latch.marker_path(chain.CHAIN).exists()

        result, _ = await _write([_payload(BASE, [])])
        assert result.expenses_deleted == 1 and await _costs(pool, BASE) == {}
        assert chain_latch.marker_path(chain.CHAIN).exists()
        assert set(await chain_latch.read_owners(pool)) == set(chain.CHAIN_TABLES)

    @pytest.mark.asyncio
    async def test_a_page_of_several_orders_deletes_each_order_s_own(self, pool):
        """Each order's listing judges its own rows: another order's expense,
        listed by its own payload, is left alone."""
        one, two = BASE, BASE + 1
        await _write([_payload(one, [_cost(one * 10), _cost(one * 10 + 1)]),
                      _payload(two, [_cost(two * 10)])])
        result, _ = await _write([_payload(one, [_cost(one * 10)]),
                                  _payload(two, [_cost(two * 10)])])
        assert set(await _costs(pool, one)) == {one * 10}
        assert set(await _costs(pool, two)) == {two * 10}
        assert result.expenses_deleted == 1


class TestWhatIsNeverDeleted:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("carried", [_ABSENT, None, {"id": 1}],
                             ids=["no-key", "null", "not-a-list"])
    async def test_a_payload_that_lists_nothing_deletes_nothing(self, pool, carried):
        """A payload fetched without `include=expenses`, or carrying something
        that is not a list, says nothing about the order's costs. Mutation:
        read an absent key as an empty list."""
        await _write([_payload(BASE, [_cost(BASE * 10)])])
        before = await _costs(pool, BASE)
        payload = _payload(BASE, status=19, updated=T0 + timedelta(minutes=5))
        if carried is not _ABSENT:
            payload["expenses"] = carried
        result, _ = await _write([payload])
        assert await _costs(pool, BASE) == before
        assert result.expenses_deleted == 0

    @pytest.mark.asyncio
    async def test_a_refused_order_keeps_its_expenses(self, pool):
        """An order Postgres would refuse is not written, and neither is the
        verdict of its payload on its costs. Mutation: act on a refused
        order's listing."""
        await _write([_payload(BASE, [_cost(BASE * 10)])])
        poisoned = _payload(BASE, [], comment="utm\x00",
                            updated=T0 + timedelta(minutes=5))
        result, _ = await _write([poisoned])
        assert result.failed == 1
        assert set(await _costs(pool, BASE)) == {BASE * 10}

    @pytest.mark.asyncio
    async def test_a_deletion_rolls_back_with_its_order(self, pool, monkeypatch):
        """Same transaction as the order: an order Postgres itself refuses,
        written order by order after the batch failed, takes its deletion
        back with it. Mutation: delete in a transaction of its own."""
        await _write([_payload(BASE, [_cost(BASE * 10)]),
                      _payload(BASE + 1, [_cost((BASE + 1) * 10)])])
        monkeypatch.setattr(chain, "order_refusal", lambda row: None)
        later = T0 + timedelta(minutes=5)
        result, _ = await _write([_payload(BASE, [], comment="utm\x00", updated=later),
                                  _payload(BASE + 1, [], updated=later)])
        assert result.failed == 1
        assert set(await _costs(pool, BASE)) == {BASE * 10}
        assert await _costs(pool, BASE + 1) == {}
        assert result.expenses_deleted == 1

    @pytest.mark.asyncio
    async def test_an_expense_moved_to_another_order_is_moved_not_deleted(self, pool):
        """KeyCRM serves the expense under its new order, and the old order
        with an empty list, in one page. Mutation: judge a stored row by its
        own order's listing alone."""
        moved = BASE * 10
        await _write([_payload(BASE, [_cost(moved)]), _payload(BASE + 1, [])])
        result, _ = await _write([_payload(BASE, []), _payload(BASE + 1, [_cost(moved)])])
        assert await _costs(pool, BASE) == {}
        assert set(await _costs(pool, BASE + 1)) == {moved}
        assert result.expenses_deleted == 0

    @pytest.mark.asyncio
    async def test_an_id_listed_under_a_refused_order_is_not_deleted(self, pool):
        """KeyCRM moved the expense to an order the parse cannot read: that
        order is refused and writes nothing, and the expense is still listed
        in the page, so the old order's empty list does not delete it.
        Mutation: read the ids to keep from the payloads that were parsed."""
        moved = BASE * 10
        await _write([_payload(BASE, [_cost(moved)])])
        unreadable = _payload(BASE + 1, [_cost(moved)])
        unreadable["grand_total"] = "not a number"
        result, _ = await _write([_payload(BASE, []), unreadable])
        assert result.failed == 1
        assert set(await _costs(pool, BASE)) == {moved}
        assert result.expenses_deleted == 0


class TestAnUnchangedExpenseIsNotRewritten:
    @pytest.mark.asyncio
    async def test_an_identical_page_writes_nothing_and_stamps_nothing(self, pool):
        """The same page again, as every tick of the 24-hour window serves
        it. On main every expense was rewritten — a new `xmin` — and
        `bronze.expenses` restamped, on every tick."""
        page = [_payload(BASE, [_cost(BASE * 10), _cost(BASE * 10 + 1, amount=3)])]
        await _write(page)
        before, stamp = await _costs(pool, BASE), await _expenses_stamp(pool)

        result, expense_count = await _write(page)

        assert await _costs(pool, BASE) == before
        assert await _expenses_stamp(pool) == stamp
        assert (result.changed_ids, result.skipped_unchanged, expense_count) == ([], 1, 2)
        assert result.expenses_deleted == 0

    @pytest.mark.asyncio
    async def test_an_idle_page_takes_no_latch(self, pool):
        """Orders in their state and expenses already stored as listed: read
        before the latch, nothing taken."""
        from core import landing_rows, pg_landing

        landed = landing_rows.landed_orders([_payload(BASE)], with_products=True)
        await pg_landing.write_orders(landed.orders, landed.products,
                                      replace_products=True)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bronze.expenses (id, order_id, expense_type_id, amount, "
                "description, status, payment_date, created_at) "
                "VALUES ($1, $2, 1, 10.50, 'Нова пошта', 'paid', $3, $4)",
                BASE * 10, BASE, PAID, T0)
        result, expense_count = await _write([_payload(BASE, [_cost(BASE * 10)])])
        assert (result.changed_ids, expense_count) == ([], 1)
        assert not chain_latch.marker_path(chain.CHAIN).exists()
        assert await chain_latch.read_owners(pool) == {}

    @pytest.mark.asyncio
    async def test_a_moved_header_leaves_identical_expenses_alone(self, pool):
        """The order's status moved, its costs did not: the header is written,
        the expense rows and their stamp are not."""
        page = [_payload(BASE, [_cost(BASE * 10)])]
        await _write(page)
        before, stamp = await _costs(pool, BASE), await _expenses_stamp(pool)

        result, _ = await _write([_payload(BASE, [_cost(BASE * 10)], status=19,
                                           updated=T0 + timedelta(minutes=5))])
        assert result.changed_ids == [BASE]
        assert await _costs(pool, BASE) == before
        assert await _expenses_stamp(pool) == stamp

    @pytest.mark.asyncio
    @pytest.mark.parametrize("change", [
        {"amount": "10.51"},
        {"expense_type_id": 2},
        {"description": "Укрпошта"},
        {"status": "not_paid"},
        {"payment_date": "2026-09-22T08:30:00Z"},
        {"created_at": "2026-09-20T12:00:01Z"},
    ], ids=lambda c: next(iter(c)))
    async def test_a_change_in_any_written_column_is_written(self, pool, change):
        """Compared column by column, money to the kopeck. Mutation: leave a
        column out of the comparison, or compare money with a tolerance."""
        await _write([_payload(BASE, [_cost(BASE * 10)])])
        stamp = await _expenses_stamp(pool)
        await _write([_payload(BASE, [_cost(BASE * 10, **change)])])
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT expense_type_id, amount, description, status, payment_date, "
                "created_at FROM bronze.expenses WHERE id = $1", BASE * 10)
        column, value = next(iter(change.items()))
        stored = row[column]
        if isinstance(stored, datetime):
            stored = stored.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        assert str(stored) == str(value)
        assert (await _expenses_stamp(pool))["last_ok_at"] > stamp["last_ok_at"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stored, served, cents", [
        ("10.50", "10.50", "10.50"),
        ("10.50", "10.5", "10.50"),
        ("10.50", 10.5, "10.50"),
        ("10.50", "10.500", "10.50"),
        # A float is its binary value, the one asyncpg sends: 2.675 is
        # 2.67499…, so `NUMERIC(12,2)` stores 2.67 — and 1.005, 1.00. Read
        # from its text it was 2.68 against the stored 2.67, so the same page
        # rewrote it on every tick (C2R-1).
        (2.675, 2.675, "2.67"),
        (1.005, 1.005, "1.00"),
        ("2.67", 2.675, "2.67"),
        ("1.00", 1.005, "1.00"),
    ], ids=["same", "short", "float", "long", "float 2.675", "float 1.005",
            "text then float 2.675", "text then float 1.005"])
    async def test_the_same_amount_in_another_spelling_is_not_a_change(
            self, pool, stored, served, cents):
        """KeyCRM serves money as text or a number; `NUMERIC(12,2)` stores
        cents. Mutations: compare the payload's value as it arrived, or read
        a float from its text."""
        await _write([_payload(BASE, [_cost(BASE * 10, amount=stored)])])
        before, stamp = await _costs(pool, BASE), await _expenses_stamp(pool)
        assert before[BASE * 10][0] == cents
        result, _ = await _write([_payload(BASE, [_cost(BASE * 10, amount=served)])])
        assert await _costs(pool, BASE) == before
        assert await _expenses_stamp(pool) == stamp
        assert result.expenses_deleted == 0

    @pytest.mark.asyncio
    async def test_a_float_and_the_text_of_its_digits_are_two_amounts(self, pool):
        """2.675 as a float is stored as 2.67, and "2.675" as text as 2.68:
        served the text after the float, the expense moved by a kopeck and is
        written. Mutation: read every amount as a float."""
        await _write([_payload(BASE, [_cost(BASE * 10, amount=2.675)])])
        await _write([_payload(BASE, [_cost(BASE * 10, amount="2.675")])])
        assert (await _costs(pool, BASE))[BASE * 10][0] == "2.68"
