"""The `/expenses` statements, the history gate, and the fan-out the port found.

The numbers are `tests/integration/test_expenses_two_engines.py`'s job,
against a real Postgres; that every `/expenses` route answers 503 naming
`expenses` when Postgres fails is the sweep's in
`tests/unit/test_read_fallback_http.py`. What is proved here is the
statements, the history gate — through the routes, over Postgres, below —
the shared parse, and one defect that was live on production before this
port touched anything:

`get_profit_analysis` read `FROM orders o LEFT JOIN expenses e` and summed
`o.grand_total` over the joined rows, so an order carrying two expenses had its
revenue counted twice. 157 orders have more than one. Measured on the
production catalogue: ₴16,433,644.56 reported over 90 days against a true
₴16,119,279.06 — ₴314,365.50 too much, and ₴62,000 on 2026-07-28 alone.
"""
from __future__ import annotations

import ast
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from core import read_fallback
from tests.helpers.app_client import signed_in
from tests.integration.pg_tab_seed import DSN, derive, needs_postgres, seed, tab_in_postgres

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "expenses.py"
W = (date.today() - timedelta(days=30), date.today())

READS = (
    ("get_expense_types", (), {}),
    ("get_expense_summary", W, {}),
    ("get_profit_analysis", W, {}),
    ("list_expenses", (), {}),
    ("get_ad_spend_by_platform", W, {}),
    ("get_expenses_summary", (), {}),
)


def _statements():
    src = REPOSITORY.read_text(encoding="utf-8")
    tree = ast.parse(src)
    for fn in ast.walk(tree):
        if not (isinstance(fn, ast.AsyncFunctionDef)
                and fn.name in {r[0] for r in READS}):
            continue
        nested = {
            id(v) for node in ast.walk(fn)
            if isinstance(node, ast.JoinedStr) for v in node.values
        }
        for node in ast.walk(fn):
            if isinstance(node, ast.JoinedStr):
                text = "".join(
                    v.value for v in node.values
                    if isinstance(v, ast.Constant) and isinstance(v.value, str)
                )
            elif (isinstance(node, ast.Constant)
                  and isinstance(node.value, str)
                  and id(node) not in nested):
                text = node.value
            else:
                continue
            if "SELECT" in text.upper():
                yield fn.name, node.lineno, text


class TestTheFanOutIsFixed:
    """The defect this port surfaced, pinned so it cannot return."""

    def _profit_sql(self):
        return [t for n, _, t in _statements()
                if n == "get_profit_analysis"]

    def test_the_revenue_query_folds_expenses_before_joining(self):
        sql = self._profit_sql()
        assert sql, "get_profit_analysis issues no statement"
        body = " ".join(sql)
        assert "GROUP BY order_id" in body, (
            "expenses must be aggregated per order before the join, or an "
            "order with two of them has its revenue counted twice"
        )

    def test_it_no_longer_joins_expenses_directly_to_orders(self):
        for text in self._profit_sql():
            compact = " ".join(text.split()).upper()
            assert "LEFT JOIN {EXPENSES} E ON" not in compact


class TestTheSharedParse:
    """`expense_types` is the first landing table whose parse is a real
    transformation, so it had to move before the table gained a second
    writer."""

    def test_the_localisation_key_is_resolved_from_the_alias(self):
        from core.landing_rows import expense_type_row

        row = expense_type_row({
            "id": 1, "name": "dictionaries.expense_types.delivery",
            "alias": "delivery", "is_active": True,
        })
        assert row.name == "Delivery"

    def test_without_an_alias_it_falls_back_to_the_key_itself(self):
        from core.landing_rows import expense_type_row

        row = expense_type_row({
            "id": 2, "name": "dictionaries.expense_types.courier_delivery",
            "alias": None,
        })
        assert row.name == "Courier Delivery"

    def test_an_ordinary_name_is_left_alone(self):
        from core.landing_rows import expense_type_row

        assert expense_type_row({"id": 9, "name": "Мито"}).name == "Мито"

    @pytest.mark.parametrize("raw,expected_kind", [
        ("2026-09-07T14:31:22.000000Z", "datetime"),
        ("2026-09-05 13:04:29", "datetime"),
        ("2026-09-05", "datetime"),
        (None, "none"),
        ("", "none"),
        ("not a date", "none"),
    ])
    def test_timestamps_are_parsed_not_passed_through(self, raw, expected_kind):
        """KeyCRM sends ISO strings; asyncpg refuses them for TIMESTAMPTZ and
        DuckDB accepts them, so passing the payload value straight through
        wrote one store and failed the other — 66 rows on the first batch in
        production. A parse is not finished until both stores accept it."""
        from datetime import datetime

        from core.landing_rows import expense_row

        row = expense_row(1, {"id": 9, "payment_date": raw, "created_at": raw})
        for value in (row.payment_date, row.created_at):
            if expected_kind == "datetime":
                assert isinstance(value, datetime), f"{raw!r} -> {value!r}"
            else:
                assert value is None

    def test_a_null_expenses_list_is_not_a_crash(self):
        """`order.get("expenses", [])` raised on `"expenses": null`, which
        KeyCRM is free to send. The flattening lives in one place now and
        treats it as no expenses."""
        from core.landing_rows import expense_rows

        assert expense_rows([{"id": 1, "expenses": None}]) == []


class TestHistoryMustBeAcrossBeforePostgresAnswers:
    """The mechanism that replaces "run the backfill first".

    On 2026-09-07 the flag went on while Postgres held 65 of 15,020 expenses,
    and the tab answered from it without a murmur — an empty result is not an
    exception, so nothing fell back and nothing was logged. The instruction had
    been written down, said out loud, and put in the finding's own text. None
    of that is a mechanism; `backfilled_at` is.
    """

    def setup_method(self):
        from core import pg_expenses_read as r

        r._backfilled = False
        r._checked_at = 0.0

    @pytest.mark.asyncio
    async def test_the_latch_only_ever_closes_forward(self):
        """`backfilled_at` is set and never cleared, so a true answer is worth
        remembering for the life of the process."""
        from core import pg_expenses_read as r

        pool = MagicMock()
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=True)
        acquire = MagicMock()
        acquire.__aenter__ = AsyncMock(return_value=conn)
        acquire.__aexit__ = AsyncMock(return_value=False)
        pool.acquire = MagicMock(return_value=acquire)

        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            assert await r.backfilled() is True
            assert await r.backfilled() is True
        assert conn.fetchval.await_count == 1, "a settled latch was re-read"
        # The table asked for by its name (step 14, S14-07).
        assert conn.fetchval.await_args.args[1] == "bronze.expenses"

    @pytest.mark.asyncio
    async def test_an_unreadable_watermark_is_refused(self):
        """The check that cannot read `meta.mirror_state` is refused naming
        `expenses` — the read behind it is not going to succeed either — and
        the refusal holds until the recheck window passes."""
        from core import pg_expenses_read as r

        down = OSError("connection refused")
        read_fallback.reset_counts()
        try:
            with patch("core.pg.get_pool", new=AsyncMock(side_effect=down)):
                with pytest.raises(read_fallback.ReadUnavailable) as raised:
                    await r.backfilled()
                assert raised.value.__cause__ is down
                with pytest.raises(read_fallback.ReadUnavailable):
                    await r.backfilled()
            assert read_fallback.refusals()["expenses"]["count"] == 2
        finally:
            r._check_failed = None
            read_fallback.reset_counts()


# ─── The history gate, through the routes, over Postgres ───────────────────

KYIV = ZoneInfo("Europe/Kyiv")
DAY = date(2026, 9, 10)
WINDOW = {"start_date": "2026-09-08", "end_date": "2026-09-12"}
# (order_id, grand_total, [(expense_id, expense_type_id, amount)]): two retail
# orders, one carrying two costs.
COSTED = [(1, 1000.0, [(11, 1, 100.0), (12, 2, 50.0)]),
          (2, 2000.0, [(13, 1, 200.0)])]
TYPES = [(1, "Доставка", "delivery"), (2, "Комісія", "commission")]


async def _orders_and_their_costs(conn, pool):
    """Two orders as KeyCRM lands them, Silver derived, and their costs and
    the cost types through the shared parse, as the sync lands them."""
    from core.landing_rows import (
        EXPENSE_COLUMNS, EXPENSE_TYPE_COLUMNS, expense_rows, expense_type_rows,
    )

    await seed(conn, orders=[
        (oid, 1, 1, total, datetime.combine(DAY, time(12), KYIV), oid, None, None)
        for oid, total, _ in COSTED])
    await derive(pool)
    types = expense_type_rows([{"id": i, "name": n, "alias": a, "is_active": True}
                               for i, n, a in TYPES])
    await conn.executemany(
        f"INSERT INTO bronze.expense_types ({', '.join(EXPENSE_TYPE_COLUMNS)})"
        " VALUES ($1,$2,$3,$4)", [tuple(r) for r in types])
    costs = expense_rows([
        {"id": oid, "expenses": [
            {"id": eid, "expense_type_id": tid, "amount": amount, "status": "paid"}
            for eid, tid, amount in lines]}
        for oid, _, lines in COSTED])
    await conn.executemany(
        f"INSERT INTO bronze.expenses ({', '.join(EXPENSE_COLUMNS)}) VALUES "
        f"({', '.join(f'${i}' for i in range(1, len(EXPENSE_COLUMNS) + 1))})",
        [tuple(r) for r in costs])


# The mirror's own record of `bronze.expenses`: no row at all; a mirror
# shipping deltas whose history never came across (2026-09-07: 65 of 15 020
# expenses held, and the page showed almost pure profit); the history across.
WATERMARKS = {
    "absent": None,
    "deltas_only": "INSERT INTO meta.mirror_state (table_name, last_attempted_at,"
                   " last_ok_at) VALUES ('bronze.expenses', now(), now())",
    "backfilled": "INSERT INTO meta.mirror_state (table_name, last_attempted_at,"
                  " last_ok_at, backfilled_at)"
                  " VALUES ('bronze.expenses', now(), now(), now())",
}
_THE_ROW = "FROM meta.mirror_state WHERE table_name = 'bronze.expenses'"


@asynccontextmanager
async def _costs_with(monkeypatch, watermark: str):
    """The two costed orders, `bronze.expenses`' mirror row as `watermark`
    says, and the gate as a fresh process finds it: never asked. The row the
    database held before is put back after."""
    from core import pg_expenses_read

    monkeypatch.setattr(pg_expenses_read, "_backfilled", False)
    monkeypatch.setattr(pg_expenses_read, "_checked_at", 0.0)
    monkeypatch.setattr(pg_expenses_read, "_check_failed", None)
    async with tab_in_postgres(DSN, monkeypatch, _orders_and_their_costs) as pool:
        async with pool.acquire() as conn:
            before = await conn.fetch(f"SELECT * {_THE_ROW}")
            await conn.execute(f"DELETE {_THE_ROW}")
            if WATERMARKS[watermark]:
                await conn.execute(WATERMARKS[watermark])
        try:
            yield
        finally:
            async with pool.acquire() as conn:
                await conn.execute(f"DELETE {_THE_ROW}")
                for row in before:
                    await conn.execute(
                        f"INSERT INTO meta.mirror_state ({', '.join(row.keys())}) VALUES "
                        f"({', '.join(f'${i}' for i in range(1, len(row) + 1))})",
                        *row.values())


@needs_postgres
class TestTheHistoryGateOverPostgres:
    """The mechanism that replaces "run the backfill first": the two views
    that read the mirrored costs are refused, a 503 naming `expenses`, until
    `bronze.expenses` carries `backfilled_at`; what reads only the type
    dictionary or the typed expenses has no history to miss and answers."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("watermark", ["absent", "deltas_only"])
    async def test_without_its_history_the_costs_are_refused(self, monkeypatch, watermark):
        async with _costs_with(monkeypatch, watermark):
            async with signed_in("editor") as editor:
                profit = await editor.get("/api/expenses/profit", params=WINDOW)
                summary = await editor.get("/api/expenses/summary", params=WINDOW)
                types = await editor.get("/api/expense-types")
                typed = await editor.get("/api/expenses")
        for refused in (profit, summary):
            assert refused.status_code == 503, refused.text
            assert refused.json()["surface"] == "expenses"
        assert types.status_code == 200, types.text
        assert {t["name"] for t in types.json()} == {"Доставка", "Комісія"}
        assert typed.status_code == 200, typed.text

    @pytest.mark.asyncio
    async def test_once_it_is_across_the_same_rows_answer(self, monkeypatch):
        """The control: the same orders and costs, the history across — so the
        refusals above are the gate's, not a query that cannot run."""
        async with _costs_with(monkeypatch, "backfilled"):
            async with signed_in("editor") as editor:
                profit = await editor.get("/api/expenses/profit", params=WINDOW)
                summary = await editor.get("/api/expenses/summary", params=WINDOW)
        assert profit.status_code == 200, profit.text
        assert profit.json()["metrics"]["totalRevenue"] == 3000.0
        assert profit.json()["metrics"]["totalExpenses"] == 350.0
        assert summary.status_code == 200, summary.text
