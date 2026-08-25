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

from core.duckdb_constants import DISPLAY_TIMEZONE, _date_in_kyiv
from core.duckdb_store import silver_sales_type_case, silver_select_sql
from core.sql_dialect import DUCKDB, POSTGRES


def _normalise(sql: str, dialect) -> str:
    """The rendering with both known dialect differences undone."""
    out = sql.replace(
        dialect.kyiv_date("o.ordered_at"), "<<KYIV(o.ordered_at)>>"
    )
    # Longest first: `bronze.managers` contains `managers`.
    for attr in ("classifications", "managers", "orders"):
        name = getattr(dialect, attr)
        out = out.replace(name, f"<<{attr.upper()}>>")
    return out


class TestThereAreExactlyTwoDifferences:
    def test_the_sales_type_case_is_one_rule(self):
        assert _normalise(silver_sales_type_case(DUCKDB), DUCKDB) == _normalise(
            silver_sales_type_case(POSTGRES), POSTGRES
        )

    def test_the_whole_projection_is_one_rule(self):
        assert _normalise(silver_select_sql(DUCKDB), DUCKDB) == _normalise(
            silver_select_sql(POSTGRES), POSTGRES
        )

    def test_the_two_renderings_are_not_accidentally_identical(self):
        """A normalisation that flattened everything would pass the two above
        while proving nothing."""
        assert silver_select_sql(DUCKDB) != silver_select_sql(POSTGRES)


class TestNothingChangedForDuckDB:
    """Stored Silver is rebuilt from this text every two minutes. A change to
    the default rendering is a change to production data."""

    def test_the_default_is_duckdb(self):
        assert silver_select_sql() == silver_select_sql(DUCKDB)
        assert silver_sales_type_case() == silver_sales_type_case(DUCKDB)

    def test_the_date_expression_is_byte_for_byte_what_it_was(self):
        assert DUCKDB.kyiv_date("o.ordered_at") == _date_in_kyiv("o.ordered_at")

    def test_it_still_names_the_unqualified_tables(self):
        sql = silver_sales_type_case(DUCKDB)
        assert "FROM manager_classifications mc" in sql
        assert "FROM managers WHERE is_retail = TRUE" in sql
        assert "bronze." not in sql and "app." not in sql


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
