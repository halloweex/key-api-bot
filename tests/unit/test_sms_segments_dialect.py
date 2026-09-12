"""One audience query, two engines, and the proof that it stayed one.

`core/sql_dialect.py` already carries the Silver projection and the order-lines
view under charter rule 1: a rule with two homes is a rule that will differ, and
this repository has the receipt — #101 changed `sales_type` in one copy of the
Silver projection and not the other, inside a day.

The audience query is the biggest thing to come under that rule so far, and six
constructs in it looked like they would force a second home. Each has a
spelling both engines accept, so the table names are again the only difference.
The load-bearing test is `test_the_two_renderings_differ_only_in_table_names`:
undo the names and the two strings must be equal. That is what would catch a
*seventh* divergence — the one nobody predicted — which an `if postgres:` in
the body could hide indefinitely.
"""
from __future__ import annotations

import pytest

from core.repositories.customers import SmsAudienceFilters
from core.sql_dialect import DUCKDB, POSTGRES, sms_segments_select

FRAGMENTS = dict(
    ltv_column="revenue_ltv",
    sales_type_filter="AND l.sales_type = ?",
    tier_case="CASE WHEN c.revenue_ltv >= ? THEN 'VIP' END",
    arm_expr="tier_level",
    ok_tier_expr="tier_level IS NOT NULL",
    filter_sql="TRUE",
    tier_subset="",
)

def _statements(sql: str) -> str:
    """The SQL with its `--` comments removed.

    The first version of the check below failed on the comment that explains
    why QUALIFY is *not* used — [[feedback_assert_on_structure_not_prose]] in
    miniature, and worth keeping as the reason this helper exists: a grep over
    source is satisfied by prose that merely mentions the thing.
    """
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


# The five names, longest first so `bronze.buyers` cannot be half-rewritten by
# a substitution meant for something else. `app.buyer_gender` sits above it for
# the same reason: both bare forms begin with "buyer".
_NAMES = (
    ("app.marketing_optouts", "marketing_optouts"),
    ("bronze.offer_stocks", "offer_stocks"),
    ("silver.order_lines", "silver_order_lines"),
    ("app.buyer_gender", "buyer_gender"),
    ("bronze.buyers", "buyers"),
)


class TestOneBody:
    def test_the_two_renderings_differ_only_in_table_names(self):
        duck = sms_segments_select(DUCKDB, **FRAGMENTS)
        postgres = sms_segments_select(POSTGRES, **FRAGMENTS)
        assert duck != postgres, "the dialect is doing nothing at all"

        undone = postgres
        for qualified, bare in _NAMES:
            undone = undone.replace(qualified, bare)
        assert undone == duck, (
            "the two renderings differ somewhere other than the table names — "
            "that is a second home for this rule"
        )

    @pytest.mark.parametrize("dialect", (DUCKDB, POSTGRES))
    def test_each_rendering_names_its_own_tables(self, dialect):
        sql = sms_segments_select(dialect, **FRAGMENTS)
        for name in (dialect.order_lines, dialect.offer_stocks,
                     dialect.buyers, dialect.marketing_optouts):
            assert name in sql

    def test_no_placeholder_hole_was_left_unfilled(self):
        for dialect in (DUCKDB, POSTGRES):
            sql = sms_segments_select(dialect, **FRAGMENTS)
            assert "{" not in sql and "}" not in sql


class TestTheConstructsThatWouldHaveForcedTwoBodies:
    """Each of these is a DuckDB-ism that was rewritten to a portable form.

    Their absence is the assertion: if one comes back, the body has quietly
    become DuckDB-only again and the Postgres rendering is invalid SQL that
    nothing here would catch until it ran.
    """

    @pytest.mark.parametrize("banned", ("QUALIFY", "list_slice", "list_transform",
                                        "DATEDIFF"))
    def test_the_duckdb_only_spelling_is_gone(self, banned):
        assert banned not in _statements(sms_segments_select(DUCKDB, **FRAGMENTS))

    def test_filter_always_carries_its_where(self):
        """DuckDB accepts `FILTER (expr)`; PostgreSQL requires
        `FILTER (WHERE expr)`. The lenient spelling renders fine and then fails
        on the other engine only."""
        sql = sms_segments_select(POSTGRES, **FRAGMENTS)
        for chunk in sql.split("FILTER (")[1:]:
            assert chunk.lstrip().startswith("WHERE"), chunk[:60]

    def test_round_casts_before_it_rounds(self):
        """PostgreSQL has no `round(double precision, int)`."""
        sql = sms_segments_select(POSTGRES, **FRAGMENTS)
        for chunk in sql.split("ROUND(")[1:]:
            head = chunk.split(" AS ")[0]
            assert "::numeric" in head, head[:80]


class TestTheFilterPredicateTravelsToo:
    """The audience predicate is spliced into the same query, so it has to name
    the dialect's line table as well — it was hardcoded to DuckDB's."""

    def test_the_exists_names_the_table_it_was_given(self):
        filters = SmsAudienceFilters(brands=("Cosrx",))
        for table in ("silver_order_lines", "silver.order_lines"):
            sql, params = filters.predicate("revenue_ltv", "retail", table)
            assert f"FROM {table} cl" in sql
            assert params == ["retail", "Cosrx"]

    def test_an_empty_filter_set_is_not_a_predicate(self):
        """It must select exactly what the level rules alone selected, so
        campaigns built before filters existed stay reproducible."""
        assert SmsAudienceFilters().predicate("revenue_ltv", "retail") == ("TRUE", [])
