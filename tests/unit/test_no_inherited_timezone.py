"""No shared SQL body may take its calendar day from the engine.

THE CLASS OF BUG THIS EXISTS FOR

`CURRENT_DATE` is not the same day on two engines that sit in different
timezones, and on this host they do:

    keycrm-web container   TZ=Europe/Kyiv   → DuckDB's day is Kyiv's
    ks-postgres            UTC
    ks-clickhouse          UTC

Measured on production, 2026-08-31. Between 21:00 and midnight UTC the engines
are on different dates, so anything keyed on "today" moves by a day depending
on which one answered — and where the expression is month-truncated, by a whole
month for the last three hours of a month.

**A differential test cannot catch this.** Under `deploy/gate_with_stores.sh`
every engine runs in the same timezone, so they agree with each other and are
wrong together. It shipped twice before being found — once into the eleven
`/inventory` views, once into the cohort tab, which was live on ClickHouse for
a day. Both times the spelling was right and the meaning was not.

So the check is structural and it is about *meaning*: a body that reaches more
than one engine must name the timezone rather than inherit one.

WHAT IS DELIBERATELY NOT FLAGGED

`CURRENT_TIMESTAMP` and `now()` writing into a `TIMESTAMPTZ` record an
*instant*, and an instant is the same number on every engine no matter how it
is rendered. Only the calendar day is ambiguous, so only the calendar day is
policed here.

THE CORNER THAT EXEMPTION MISSED

"No matter how it is rendered" stops being true the moment the instant leaves
the process **as a string**. DuckDB renders a `TIMESTAMPTZ` in the session's
timezone — `Europe/Kyiv` in the web container — and asyncpg hands back UTC, so
the same stored instant reaches the browser as `…T14:50:45+03:00` from one
engine and `…T11:50:45+00:00` from the other. Found on 2026-09-06 while
porting `get_stock_summary`, by running it against a real DuckDB rather than
by reading it. `TestAnInstantOnTheWireIsNormalised` is the guard.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from core.duckdb_constants import DISPLAY_TIMEZONE
from core.sql_dialect import (
    CLICKHOUSE_ANALYTICS, DUCKDB, DUCKDB_ANALYTICS, POSTGRES, TODAY_IN_KYIV,
    inventory_view_selects, sms_segments_select,
)
from tests.sql_helper import strip_comments_and_literals

# Spellings that ask the engine what day it is.
INHERITED = ("CURRENT_DATE", "CURRENT_DATE()", "today()", "curdate()")

SMS_FRAGMENTS = dict(
    ltv_column="revenue_ltv",
    sales_type_filter="",
    tier_case="CASE WHEN 1 = 1 THEN 'VIP' END",
    arm_expr="'ALL'",
    ok_tier_expr="TRUE",
    filter_sql="",
    tier_subset="",
)


def _rendered_bodies():
    """(label, sql) for every body that reaches more than one engine."""
    for dialect in (DUCKDB, POSTGRES):
        for name, sql in inventory_view_selects(dialect):
            yield f"inventory:{dialect.name}:{name}", sql
        yield f"sms_segments:{dialect.name}", sms_segments_select(
            dialect, **SMS_FRAGMENTS)

    import core.sql_dialect as dialects
    cohort_bodies = (
        "cohort_retention_select", "enhanced_cohort_retention_select",
        "days_to_second_purchase_select", "cohort_ltv_select",
        "at_risk_customers_select",
    )
    for dialect in (DUCKDB_ANALYTICS, CLICKHOUSE_ANALYTICS):
        for fn_name in cohort_bodies:
            yield f"cohort:{dialect.name}:{fn_name}", getattr(dialects, fn_name)(
                dialect, sales_type_filter="", months_back=12)


BODIES = list(_rendered_bodies())


def test_the_scan_finds_the_bodies_at_all():
    """A guard on the guard: an empty list would pass every assertion below."""
    assert len(BODIES) >= 30
    assert any("inventory:" in label for label, _ in BODIES)
    assert any("cohort:clickhouse" in label for label, _ in BODIES)
    assert any("sms_segments:postgres" in label for label, _ in BODIES)


@pytest.mark.parametrize("label,sql", BODIES, ids=[b[0] for b in BODIES])
def test_no_shared_body_asks_the_engine_what_day_it_is(label, sql):
    code = strip_comments_and_literals(sql)
    for spelling in INHERITED:
        assert spelling not in code, (
            f"{label} takes its day from whichever timezone the engine happens "
            f"to be in; name the zone instead"
        )


@pytest.mark.parametrize("label,sql", BODIES, ids=[b[0] for b in BODIES])
def test_every_body_that_needs_today_names_the_zone(label, sql):
    """The other half: a body may legitimately not mention today at all, but
    one that does must say whose."""
    code = strip_comments_and_literals(sql)
    if "now()" not in code:
        return
    assert DISPLAY_TIMEZONE in sql, f"{label} uses a clock without naming the zone"


class TestTheTwoSpellingsStayApart:
    """The zone is named the same way everywhere it can be, and differently
    only where an engine refuses the shared form."""

    def test_duckdb_and_postgres_share_one_expression(self):
        assert "AT TIME ZONE" in TODAY_IN_KYIV
        assert DISPLAY_TIMEZONE in TODAY_IN_KYIV
        assert DUCKDB_ANALYTICS.today == TODAY_IN_KYIV

    def test_clickhouse_gets_the_only_form_it_parses(self):
        # Measured on 24.8.14.39, production's version: it rejects
        # `AT TIME ZONE` outright, so this is a spelling hole and not a
        # second definition of what "today" means.
        assert "toTimeZone" in CLICKHOUSE_ANALYTICS.today
        assert "AT TIME ZONE" not in CLICKHOUSE_ANALYTICS.today
        assert DISPLAY_TIMEZONE in CLICKHOUSE_ANALYTICS.today


class TestTheRoutedQueriesToo:
    """The view bodies are not the only thing that reaches two engines: the
    repository's own routed queries do as well."""

    def test_no_routed_inventory_query_inherits_a_day(self):
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        offenders = []
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ("_inventory_rows", "_inventory_batch")):
                continue
            for literal in ast.walk(node):
                if (isinstance(literal, ast.Constant)
                        and isinstance(literal.value, str)
                        and "SELECT" in literal.value.upper()):
                    code = strip_comments_and_literals(literal.value)
                    if re.search(r"\bCURRENT_DATE\b", code):
                        offenders.append(literal.lineno)
        assert not offenders, (
            f"routed query takes its day from the engine at line(s) {offenders}"
        )

    def test_the_sms_audience_filter_names_the_zone(self):
        from core.repositories.customers import SmsAudienceFilters

        sql, _params = SmsAudienceFilters(
            brands=("Cosrx",), bought_within_days=30,
        ).predicate("revenue_ltv", "retail")
        assert TODAY_IN_KYIV in sql
        assert "CURRENT_DATE" not in strip_comments_and_literals(sql)


class TestAnInstantOnTheWireIsNormalised:
    """A timestamp that becomes a string must be pinned to UTC first.

    Behavioural, not structural: the failure is in what the driver renders, so
    nothing in the source distinguishes a safe `.isoformat()` from an unsafe
    one. Driving one engine under two session timezones is enough to catch it
    and needs no second engine — which matters, because the differential test
    that would also catch it only runs where a PostgreSQL is available.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("session_tz", ("UTC", "Europe/Kyiv",
                                            "Pacific/Kiritimati"))
    async def test_the_stock_summary_stamp_does_not_move_with_the_session(
        self, tmp_path, session_tz,
    ):
        from datetime import date, datetime, timezone

        from core.duckdb_store import DuckDBStore

        stamp = datetime(2026, 9, 6, 11, 50, 45, tzinfo=timezone.utc)
        store = DuckDBStore(db_path=tmp_path / f"tz-{session_tz.replace('/', '-')}.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                conn.execute(f"SET TimeZone='{session_tz}'")
                conn.execute(
                    "INSERT INTO offer_stocks (id, sku, price, purchased_price,"
                    " quantity, reserve) VALUES (1, 'S-1', 500, 250, 7, 0)")
                conn.execute(
                    "INSERT INTO sku_inventory_status (offer_id, product_id, sku,"
                    " name, brand, category_id, quantity, reserve, price,"
                    " purchased_price, first_seen_at, updated_at)"
                    " VALUES (1, 101, 'S-1', 'Name', 'B', 1, 7, 0, 500, 250, ?, ?)",
                    [date(2026, 1, 1), stamp])

            summary = await store.get_stock_summary(limit=5)
            assert summary["lastSync"] == "2026-09-06T11:50:45+00:00", (
                f"the session timezone reached the wire: {summary['lastSync']}"
            )
        finally:
            await store.close()
