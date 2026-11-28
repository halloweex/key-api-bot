"""What the `/goals` history counts, and the one trap in how it bounds its window.

`_goals_run` reads Postgres only (step 14, S14-07). The routing, the boundary
that kept the shared goal tables off it and the other engine's statements went
with the DuckDB half; the endpoints are exercised in
`tests/integration/test_goals_two_engines.py` and
`tests/integration/test_goals_history_two_engines.py`.

The window used to be bound with `CURRENT_DATE`, which is Kyiv's day in the
`web` container and UTC's on the Postgres server: different days for three
hours out of twenty-four. A differential test cannot find that (both engines
sit in one timezone under the gate), so it is asserted on the method's tree.
"""
from __future__ import annotations

import ast
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from tests.helpers.app_client import signed_in
from tests.integration.pg_tab_seed import needs_postgres, seed, derive, tab_in_postgres


REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "goals.py"


class TestTheRoutedBodies:
    def test_the_window_is_not_bound_with_current_date(self):
        """The two engines disagree about what day `CURRENT_DATE` is for three
        hours out of twenty-four, and under the gate they sit in one timezone
        — so this cannot be a differential test.

        Asserted on the routed method's own tree, with its docstring taken out:
        the docstring explains the trap and contains the words."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        node = next(n for n in ast.walk(tree)
                    if isinstance(n, ast.AsyncFunctionDef)
                    and n.name == "get_historical_revenue")
        doc = ast.get_docstring(node, clean=False)
        strings = [n.value for n in ast.walk(node)
                   if isinstance(n, ast.Constant) and isinstance(n.value, str)
                   and n.value != doc]
        for text in strings:
            assert "CURRENT_DATE" not in text.upper(), (
                "the window is bound with CURRENT_DATE again — use "
                "core.sql_dialect.TODAY_IN_KYIV"
            )
        assert any("TODAY_IN_KYIV" in n.id
                   for n in ast.walk(node) if isinstance(n, ast.Name)), (
            "TODAY_IN_KYIV is no longer used to bound the window"
        )


class TestWhatTheHistoryCounts:
    """Silver decides what is a return and what is revenue; the goal history
    asks it and keeps no list of its own. One day, `sales_type=all`."""

    @needs_postgres
    @pytest.mark.asyncio
    async def test_returns_and_retired_sources_are_left_out_and_the_fair_is_in(
        self, monkeypatch,
    ):
        day = datetime.now(ZoneInfo("Europe/Kyiv")).replace(
            hour=12, minute=0, second=0, microsecond=0) - timedelta(days=2)
        # (id, source, status, total): an ordinary website order, the fair's,
        # one in KeyCRM's lost group under a status no list names, one in a
        # listed return status, and one on the retired shop.
        orders = [(9101, 4, 1, 1000), (9102, 5, 1, 500), (9103, 4, 99, 700),
                  (9104, 4, 19, 200), (9105, 3, 1, 300)]

        async def populate(conn, pool):
            await seed(conn, orders=[(oid, src, status, total, day, None, None, None)
                                     for oid, src, status, total in orders])
            await conn.execute(
                "UPDATE bronze.orders SET status_group_id = 6 WHERE id = 9103")
            await derive(pool)

        async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, populate):
            async with signed_in("viewer") as visitor:
                r = await visitor.get("/api/goals/history", params={
                    "period_type": "daily", "weeks_back": 1, "sales_type": "all"})

        assert r.status_code == 200, r.text
        body = r.json()
        assert (body["periodCount"], body["average"]) == (1, 1500.0)
