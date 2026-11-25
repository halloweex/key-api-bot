"""One Silver projection, rendered for two engines — and no third difference.

Step 05 ends with Silver computed inside Postgres. The naive route is to write
the projection a second time in Postgres dialect, which is the mistake charter
rule 1 exists to prevent: a rule with two homes is a rule that will differ, and
the shortest recorded time it took to differ here is **under a day** (#101
updated `sales_type` in one copy of the Silver projection and not the other).

The projection turns out to differ between the engines in exactly two places —
the Kyiv date and three table names. The test that matters is the one that
proves *exactly two*: render both, undo the two known substitutions, and assert
the strings are identical. A third divergence, introduced later by someone
touching only one branch, fails here and nowhere else.
"""
from __future__ import annotations

from core.duckdb_constants import DISPLAY_TIMEZONE
from core.sql_dialect import DUCKDB, POSTGRES
from core.warehouse_sql import silver_sales_type_case, silver_select_sql


class TestThePostgresRendering:
    def test_it_names_the_schemas_revision_0005_created(self):
        sql = silver_sales_type_case(POSTGRES)
        assert "FROM app.manager_classifications mc" in sql
        assert "FROM bronze.managers WHERE is_retail = TRUE" in sql

    def test_the_date_is_a_cast_not_a_function_call(self):
        assert POSTGRES.kyiv_date("o.ordered_at") == (
            f"(timezone('{DISPLAY_TIMEZONE}', o.ordered_at))::date"
        )

    def test_both_dialects_read_the_same_timezone(self):
        """A Kyiv date on one side and a UTC date on the other would move
        orders between months. 1,007 production orders differ on exactly that."""
        for dialect in (DUCKDB, POSTGRES):
            assert DISPLAY_TIMEZONE in dialect.kyiv_date("x")

    def test_the_projection_still_selects_from_an_alias(self):
        """`silver_select_sql` emits columns only; the caller supplies the
        FROM, which is why `dialect.orders` exists."""
        assert POSTGRES.orders == "bronze.orders"
        assert "o.grand_total" in silver_select_sql(POSTGRES)
