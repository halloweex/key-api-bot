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

from datetime import date

from core.repositories.customers import (
    SmsAudienceFilters, SmsAudienceGroup, audience_group_fragments,
)
from core.sql_dialect import DUCKDB, POSTGRES, sms_segments_select

# Rendered with four OR-groups, counting orders in a period every way it can
# be said — since a date, within N days, a closed range with both bounds on
# the count, and up to a date with none in it — and narrower than one another:
# every construct the groups add — the period count in `cust`, a group's own
# window, the flags, the mask, the per-group funnel — is in the text these
# tests read. The group predicates name DuckDB's line table; that fragment's
# own dialect test is `TestTheFilterPredicateTravelsToo` below.
_GROUPS = audience_group_fragments(
    [SmsAudienceGroup(365, SmsAudienceFilters(period_orders_min=2,
                                              period_from=date(2026, 1, 1))),
     SmsAudienceGroup(92, SmsAudienceFilters(period_orders_min=1, period_days=92)),
     SmsAudienceGroup(3650, SmsAudienceFilters(
         period_orders_min=1, period_orders_max=3,
         period_from=date(2025, 1, 1), period_to=date(2025, 12, 31))),
     SmsAudienceGroup(730, SmsAudienceFilters(period_orders_max=0,
                                              period_to=date(2024, 12, 31)))],
    "revenue_ltv", "retail", "silver_order_lines",
)

FRAGMENTS = dict(
    ltv_column="revenue_ltv",
    sales_type_filter="AND l.sales_type = ?",
    tier_case="CASE WHEN c.revenue_ltv >= ? THEN 'VIP' END",
    arm_expr="tier_level",
    ok_tier_expr="tier_level IS NOT NULL",
    group_predicates=_GROUPS["group_predicates"],
    period_columns=_GROUPS["period_columns"],
    tier_subset="",
)


class TestOneBody:

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

    def test_an_empty_filter_set_is_not_a_predicate(self):
        """It must select exactly what the level rules alone selected, so
        campaigns built before filters existed stay reproducible."""
        assert SmsAudienceFilters().predicate("revenue_ltv", "retail") == ("TRUE", [])
