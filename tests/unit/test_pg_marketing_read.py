"""The `/marketing` statements, and the two shapes that had to be bridged.

The numbers are `tests/integration/test_marketing_two_engines.py`'s job,
against a real Postgres; that a failure of its engine is refused naming its surface is
the route sweep in `tests/unit/test_read_fallback_http.py` (`TestEveryRoute`)'s. What
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
from pathlib import Path

from core.sql_dialect import POSTGRES, marketing_period_measures

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "revenue.py"


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

    def test_it_counts_orders_by_the_brand_cards_statement(self):
        """«Заказов» is the orders that held the brand, each once — the
        dashboard brand card's `_BRAND_ANALYTICS_SQL` (owner's decision,
        2026-11-12). Until then the report summed per-product cells, one order
        per product of the brand: NEOGEN, October 2026, 1 429 against 748."""
        from core.repositories import revenue

        body = "\n".join(l.split("--")[0].split("#")[0]
                          for l in self._marketing_sql().splitlines())
        assert "_BRAND_ANALYTICS_SQL" in body
        assert "SUM(order_count)" not in body
        assert "COUNT(DISTINCT l.order_id) AS orders" in revenue._BRAND_ANALYTICS_SQL
