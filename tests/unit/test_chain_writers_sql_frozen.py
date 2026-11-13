"""Chains 3, 6 and 6a send the statements they sent before step 14 — byte for byte.

Rule 12 of the step-14 plan: a Postgres writer's body moves unchanged, with no
clean-up of its SQL on the way. S14-09 deleted the routing round chain 3's,
chain 6's and chain 6a's writers — the flag, the preconditions, the hold, the
preflight and DuckDB's own order writers — and left the writers themselves
alone: `decide()`, the refusals, the latch and its owner rows (decision 3),
`writes:<table>` and the mirror's `WATERMARK_OK_SQL`.

This proves it. Each writer is driven through a connection that records every
call, in order — the statement's text, how many arguments or rows went with
it, every transaction it opened — and the record is held to a literal taken
on the commit before S14-09 (`d544144c`), which equals the same recording on
the plan's reference commit `57fd7c22` under production's `KS_PG_DERIVE=own`
(`tests/integration/golden/chain_writers_sql.json`). A changed statement, an
added or dropped one, a transaction more or less, a latch or owner claim
moved: each fails here, and the answer is never an edited literal.

Driven, not read: the writers render some statements from the shared parse's
column lists (`core.landing_rows`) and from `pg_landing`'s templates, so a
change upstream of the writer's own text shows here too.

Re-recorded once, under the owner's decision O1 (a) (2026-11-11: a fix of a
reproduced defect may change a pinned literal in the same PR, which lists every
changed statement): chain 3's four `orders_*` scenarios for A5, the expenses
that follow KeyCRM (O2 (a)) — the stored expenses read before the latch and
under lock — and `orders_expense_dropped` added, which pins the deletion. Every
other scenario is the literal of `d544144c`, unchanged.

Re-recorded a second time, under the owner's approval of the analysts'
warehouse (design revision 3.1, phase 1, 2026-11-13): `orders_expense_dropped`
only, whose deletion now returns `expense_type_id` too — the one statement that
moved. The captures of revision 0036 that ride on chains 3 and 6 (the expense
deletions inside chain 3's transaction, the catalogue's versions after chain
6's) are recorded switched off here (`KS_ANALYTICS_CAPTURES=off`): this pins the
chains' own statements, and each capture's place beside them is held by its own
tests, on a real server (`tests/integration/test_capture_expenses_pg.py`,
`tests/unit/test_capture_catalogue.py`).
"""
from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

GOLDEN = Path(__file__).resolve().parents[1] / "integration" / "golden" / "chain_writers_sql.json"
T0 = datetime(2026, 9, 20, 10, 0, tzinfo=timezone.utc)


class _Recorder:
    """A connection that answers what the writers ask and keeps what they sent."""

    def __init__(self, rows=None):
        self.calls = []
        self.rows = rows or (lambda sql: [])
        self.depth = 0

    def is_in_transaction(self):
        return self.depth > 0

    async def execute(self, sql, *args, **kw):
        self.calls.append(["execute", sql, len(args)])
        return "UPDATE 1"

    async def executemany(self, sql, rows, **kw):
        self.calls.append(["executemany", sql, len(list(rows))])

    async def fetch(self, sql, *args, **kw):
        self.calls.append(["fetch", sql, len(args)])
        return self.rows(sql)

    async def fetchval(self, sql, *args, **kw):
        self.calls.append(["fetchval", sql, len(args)])
        if "FOR UPDATE" in sql and "chain_watermarks" in sql:
            return '{"stamps": [], "previous": null}'
        return None

    async def fetchrow(self, sql, *args, **kw):
        self.calls.append(["fetchrow", sql, len(args)])
        return None

    def transaction(self):
        recorder = self

        class _Tx:
            async def __aenter__(self):
                recorder.calls.append(["begin"])
                recorder.depth += 1

            async def __aexit__(self, *exc):
                recorder.depth -= 1
                recorder.calls.append(["end", exc[0] is None])
                return False

        return _Tx()


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self, timeout=None):
        conn = self.conn

        @asynccontextmanager
        async def held():
            yield conn

        return held()


def _payload(oid, expenses=(), comment="utm_source=ig"):
    return {
        "id": oid, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": T0.isoformat(),
        "created_at": T0.isoformat(), "updated_at": T0.isoformat(),
        "buyer": {"id": 501}, "manager": {"id": 4},
        "manager_comment": comment, "promocode": None,
        "products": [{"id": oid * 10 + i, "name": f"Item {i}", "quantity": 1,
                      "price_sold": "50.00", "offer": {"product_id": 700 + i}}
                     for i in range(2)],
        "expenses": [{"id": e, "expense_type_id": 1, "amount": "10.00",
                      "status": "paid"} for e in expenses],
    }


async def _record(writer, rows=None):
    recorder = _Recorder(rows)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=_Pool(recorder))), \
            patch("core.pg.require_revision", new=AsyncMock()), \
            patch.dict(os.environ, {"KS_ANALYTICS_CAPTURES": "off"}):
        await writer()
    return recorder.calls


def _held(sql):
    """One order Postgres holds, an hour older than the payload."""
    if "SELECT id, updated_at FROM bronze.orders" in sql:
        return [{"id": 1, "updated_at": T0 - timedelta(hours=1)}]
    return []


def _held_with_a_dropped_expense(sql):
    """`_held`, and one expense stored under it that the payload no longer
    lists — what owner decision O2 (a) deletes (added with it, under O1 (a))."""
    if sql.startswith("SELECT") and "FROM bronze.expenses" in sql:
        return [{"id": 9, "order_id": 1, "expense_type_id": 1,
                 "amount": Decimal("10.00"), "description": None, "status": "paid",
                 "payment_date": None, "created_at": None}]
    if sql.startswith("DELETE FROM bronze.expenses"):
        return [{"id": 9, "order_id": 1, "expense_type_id": 1, "amount": Decimal("10.00")}]
    return _held(sql)


def _scenarios():
    from core import pg_catalogue_write, pg_expense_types_write, pg_orders_write
    from core.landing_rows import (
        ORDER_COLUMNS, category_rows, expense_type_rows, product_rows,
    )

    stored = {c: None for c in ORDER_COLUMNS}
    stored.update(id=1, source_id=1, status_id=12, grand_total=100)
    products = [{"id": 1, "name": "A", "category_id": 2, "sku": "S1", "price": 500,
                 "custom_fields": []},
                {"id": 2, "name": "B", "category_id": 2, "sku": "S2", "price": 300,
                 "custom_fields": []}]
    return {
        "orders_write": (lambda: pg_orders_write.upsert_orders_with_expenses(
            [_payload(1, expenses=[9]), _payload(2)]), None),
        "orders_header_only": (lambda: pg_orders_write.upsert_orders_with_expenses(
            [_payload(1)], force_update=True, skip_products=True), None),
        "orders_header_only_held": (lambda: pg_orders_write.upsert_orders_with_expenses(
            [_payload(1)], force_update=True, skip_products=True), _held),
        "orders_rewrite_held": (lambda: pg_orders_write.upsert_orders_with_expenses(
            [_payload(1)]), _held),
        "orders_expense_dropped": (lambda: pg_orders_write.upsert_orders_with_expenses(
            [_payload(1)]), _held_with_a_dropped_expense),
        "misses": (lambda: pg_orders_write.record_backfill_misses(
            {5: "gone", 6: "empty"}), None),
        "comments": (lambda: pg_orders_write.restore_manager_comments(
            {1: "utm_source=x"}), None),
        "comments_held": (lambda: pg_orders_write.restore_manager_comments(
            {1: "utm_source=x"}),
            lambda sql: [stored] if "manager_comment IS NULL" in sql else []),
        "products": (lambda: pg_catalogue_write.upsert_products(product_rows(products)), None),
        "categories": (lambda: pg_catalogue_write.upsert_categories(category_rows(
            [{"id": 1, "name": "Care", "parent_id": None}])), None),
        "expense_types": (lambda: pg_expense_types_write.upsert_expense_types(
            expense_type_rows([{"id": 1, "name": "Delivery", "alias": "delivery",
                                "is_active": True}])), None),
    }


def _golden():
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


def test_every_scenario_the_literal_holds_is_driven():
    """Kills: a scenario dropped from the test while its literal stays, or
    the other way round — either would read as covered and be unchecked."""
    assert set(_scenarios()) == set(_golden()["scenarios"])


@pytest.mark.asyncio
@pytest.mark.parametrize("name", sorted(_golden()["scenarios"]))
async def test_the_writer_sends_what_it_sent_before_step_14(name):
    """Kills: any edit of a chain-3, -6 or -6a writer's statements, their
    order, the latch's place inside the transaction, the owner claim, the
    `writes:` record, the watermark stamp or the derivation mark."""
    writer, rows = _scenarios()[name]
    assert await _record(writer, rows) == _golden()["scenarios"][name]


def test_the_two_shared_statements_are_the_literal():
    """`WATERMARK_OK_SQL` is the mirror's own watermark stamp, which chain 6
    writes on every full write and the daily comparison reads (OD-15 (a));
    `_RECORD_WRITE_SQL` is chain 6's `writes:<table>` record. Kills: an edit
    of either, even one no driven scenario reaches."""
    from core import pg_catalogue_write
    from core.pg_landing import WATERMARK_OK_SQL

    golden = _golden()
    assert WATERMARK_OK_SQL == golden["WATERMARK_OK_SQL"]
    assert pg_catalogue_write._RECORD_WRITE_SQL == golden["_RECORD_WRITE_SQL"]


@pytest.mark.asyncio
async def test_the_record_is_not_vacuous():
    """The recording sees what a writer sends: a write with an order and an
    expense opens a transaction, latches, claims, and writes both tables.
    Kills: a recorder that returned before the writer reached Postgres."""
    calls = (await _record(_scenarios()["orders_write"][0]))
    sql = " ".join(c[1] for c in calls if len(c) > 2)
    assert ["begin"] in calls
    for statement in ("INSERT INTO meta.chain_watermarks", "INSERT INTO bronze.expenses",
                      "INSERT INTO bronze.orders", "DELETE FROM bronze.order_products",
                      "INSERT INTO bronze.order_products"):
        assert statement in sql, statement
