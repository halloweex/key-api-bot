"""Which engine answers `/marketing`, and the two shapes that had to be bridged.

The numbers are `tests/integration/test_marketing_two_engines.py`'s job. What
is proved here is the wiring and the two places where this port is not a table
rename:

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
import asyncio
import re
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_marketing_read
from core.duckdb_store import DuckDBStore
from core.sql_dialect import DUCKDB, POSTGRES, marketing_period_measures

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "revenue.py"
WINDOW = (date.today() - timedelta(days=30), date.today())
TIMEOUT_S = 20


class TestTheFlag:
    def test_it_defaults_to_duckdb(self, monkeypatch):
        monkeypatch.delenv("KS_READ_MARKETING", raising=False)
        assert pg_marketing_read.enabled() is False

    def test_postgres_turns_it_on(self, monkeypatch):
        monkeypatch.setenv("KS_READ_MARKETING", "postgres")
        assert pg_marketing_read.enabled() is True

    def test_a_typo_raises(self, monkeypatch):
        monkeypatch.setenv("KS_READ_MARKETING", "postgress")
        with pytest.raises(ValueError, match="KS_READ_MARKETING"):
            pg_marketing_read.enabled()

    def test_it_is_a_separate_switch_from_reports(self, monkeypatch):
        """Two tabs that read different things must be able to roll back
        separately: `/reports` reads only Silver, this one Gold and the goals
        as well."""
        from core import pg_reports_read

        monkeypatch.setenv("KS_READ_MARKETING", "postgres")
        monkeypatch.delenv("KS_READ_REPORTS", raising=False)
        assert pg_marketing_read.enabled() is True
        assert pg_reports_read.enabled() is False


class TestTheTwoGoldShapes:
    """The one hole in this port that is not a table name."""

    def test_both_render_eleven_items_in_one_order(self):
        for dialect in (DUCKDB, POSTGRES):
            items = [i for i in marketing_period_measures(dialect).split(",\n")]
            assert len(items) == 11, (dialect.name, len(items))

    def test_duckdb_reads_the_channel_columns(self):
        sql = marketing_period_measures(DUCKDB)
        for column in ("instagram_revenue", "telegram_revenue", "shopify_revenue",
                       "instagram_orders", "telegram_orders", "shopify_orders"):
            assert column in sql
        assert "source_id" not in sql, "DuckDB's Gold has no such dimension"

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
    @pytest.mark.parametrize("name", ("get_marketing_report_by_dates",
                                      "get_promocode_analytics"))
    async def test_it_returns_rather_than_hanging(self, tmp_path, monkeypatch, name):
        """The store lock is not reentrant and a nested acquisition hangs
        rather than raising, so a timeout is the only assertion that catches
        it."""
        monkeypatch.delenv("KS_READ_MARKETING", raising=False)
        store = DuckDBStore(db_path=tmp_path / f"m-{name}.duckdb")
        await store.connect()
        try:
            await asyncio.wait_for(getattr(store, name)(*WINDOW), timeout=TIMEOUT_S)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_postgres_fault_falls_back_to_duckdb(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KS_READ_MARKETING", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "m.duckdb")
        await store.connect()
        try:
            with patch("core.pg_marketing_read.fetch",
                       new=AsyncMock(side_effect=RuntimeError("pg is down"))):
                out = await store.get_marketing_report_by_dates(*WINDOW)
            assert out["general_sales"]["current"]["revenue"] == 0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv("KS_READ_MARKETING", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "m2.duckdb")
        await store.connect()
        try:
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
        finally:
            await store.close()
