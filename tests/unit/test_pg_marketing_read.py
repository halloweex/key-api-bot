"""The `/marketing` statements, and the two shapes that had to be bridged.

The numbers are `tests/integration/test_marketing_two_engines.py`'s job,
against a real Postgres; that the router asks Postgres and nothing else since
step 14's S14-05 is `tests/unit/test_read_routers_postgres_only.py`'s. What
is proved here is the statements and the two places where this port is not a
table rename:

  * **the channel figures**, where the two Golds genuinely differ in shape —
    DuckDB names a channel by writing a column, Postgres carries `source_id`
    as a dimension — so `marketing_period_measures` renders both from one
    place, and the five scalars must come from the roll-up rows *only*,
    because three of them are `COUNT(DISTINCT buyer_id)` and distinct counts
    do not add up;
  * **the brand table**, which read `gold_daily_products` — DuckDB-only — and
    now reads the line level, reproducing that table's grain so `SUM(order_count)`
    keeps meaning what it has always meant.
"""
from __future__ import annotations

import ast
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import DuckDBStore
from core.sql_dialect import POSTGRES, marketing_period_measures

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "revenue.py"
WINDOW = (date.today() - timedelta(days=30), date.today())


class TestTheTwoGoldShapes:
    """The one hole in this port that is not a table name."""

    def test_both_render_eleven_items_in_one_order(self):
        """One shape since step 14's S14-07, whose DuckDB branch — a column
        per channel — went with the last DuckDB body carrying the hole."""
        items = [i for i in marketing_period_measures(POSTGRES).split(",\n")]
        assert len(items) == 11, len(items)

    def test_postgres_reads_the_source_dimension(self):
        sql = marketing_period_measures(POSTGRES)
        assert "instagram_revenue" not in sql, "no such column exists there"
        for sid in (1, 2, 4):
            assert f"SUM(revenue) FILTER (WHERE source_id = {sid})" in sql
            assert f"SUM(orders_count) FILTER (WHERE source_id = {sid})" in sql

    def test_the_scalars_come_from_the_rollup_only(self):
        """Three of the five are `COUNT(DISTINCT buyer_id)`. Folding the
        per-source rows overstates `unique_customers` in 29 of 2,107
        production cells — one buyer who used two channels in a day."""
        sql = marketing_period_measures(POSTGRES)
        head = sql.split("source_id = 1")[0]
        for measure in ("revenue", "orders_count", "unique_customers",
                        "new_customers", "returning_customers"):
            assert f"SUM({measure}) FILTER (WHERE source_id IS NULL)" in head, measure

    def test_the_rollup_predicate_comes_from_the_dialect(self):
        """Not spelled twice: `gold_revenue_rollup` is the same hole the
        inventory views use for the same difference."""
        assert POSTGRES.gold_revenue_rollup in marketing_period_measures(POSTGRES)


class TestTheBrandTableLeftTheGoldItCannotReach:
    def _marketing_sql(self):
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        node = next(n for n in ast.walk(tree)
                    if isinstance(n, ast.AsyncFunctionDef)
                    and n.name == "get_marketing_report_by_dates")
        return ast.get_source_segment(source, node) or ""

    def test_it_no_longer_names_the_duckdb_only_gold(self):
        """Statements only — the comments explain the move and name the table
        they moved away from."""
        body = "\n".join(l.split("--")[0].split("#")[0]
                          for l in self._marketing_sql().splitlines())
        assert "gold_daily_products" not in body, (
            "gold_daily_products exists only in DuckDB — naming it is the flag "
            "silently not working"
        )

    def test_it_reproduces_the_gold_grain_before_summing(self):
        """`order_count` in that table is `COUNT(DISTINCT id)` at the product-day
        grain, and the report sums it. A plain `COUNT(DISTINCT order_id)` per
        brand is a different, smaller number."""
        body = self._marketing_sql()
        assert "WITH product_days AS" in body
        for column in ("l.order_date", "l.sales_type", "l.source_id", "l.product_id",
                       "l.product_name", "l.brand", "l.category_id",
                       "l.category_name", "l.parent_category_name"):
            assert column in body, f"the grain lost {column}"
        assert "SUM(order_count)" in body

    def test_having_does_not_use_a_select_alias(self):
        """DuckDB accepts `HAVING revenue > 0`; PostgreSQL does not see a
        select alias there. The original wrote the lenient form.

        Over the statements, not the text: the first version of this failed on
        the comment that *explains* the change, which is
        [[feedback_assert_on_structure_not_prose]] for the third time in one
        session.
        """
        body = "\n".join(l.split("--")[0].split("#")[0]
                          for l in self._marketing_sql().splitlines())
        assert "HAVING SUM(product_revenue) > 0" in body
        assert "HAVING revenue" not in body


class TestNothingGoesRoundTheRouter:
    def test_neither_method_opens_the_store(self):
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for name in ("get_marketing_report_by_dates", "get_promocode_analytics"):
            node = next(n for n in ast.walk(tree)
                        if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
            body = ast.get_source_segment(source, node) or ""
            assert "self.connection()" not in body, f"{name} still opens DuckDB"

    @pytest.mark.asyncio
    async def test_every_statement_goes_to_postgres(self, tmp_path, monkeypatch):
        """The period figures, the brands and the target line — the goal read
        included, whose branch on chain 7a went with the DuckDB tail — reach
        Postgres, whatever `KS_READ_MARKETING` says, on a store whose
        connection is a trap."""
        monkeypatch.setenv("KS_READ_MARKETING", "duckdb")   # not read any more
        store = DuckDBStore(db_path=tmp_path / "never.duckdb")

        def boom(*_a, **_k):
            raise AssertionError("the marketing read reached for DuckDB")

        with patch.object(type(store), "connection", boom), \
             patch("core.pg_marketing_read.fetch",
                   new=AsyncMock(return_value=[(0,) * 11])) as fetch:
            await store.get_marketing_report_by_dates(*WINDOW)
        sent = " ".join(c.args[0] for c in fetch.await_args_list)
        assert "gold.daily_revenue" in sent
        assert "silver.order_lines" in sent
        assert "app.revenue_goals" in sent
