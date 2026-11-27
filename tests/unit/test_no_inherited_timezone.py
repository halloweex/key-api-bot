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

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_constants import DISPLAY_TIMEZONE
from core.sql_dialect import (
    CLICKHOUSE_ANALYTICS, DUCKDB, POSTGRES, TODAY_IN_KYIV,
    inventory_view_selects, sms_segments_select,
)
from core.repositories.customers import SmsAudienceFilters
from tests.integration.pg_tab_seed import DSN, needs_postgres
from tests.sql_helper import strip_comments_and_literals

# Spellings that ask the engine what day it is.
INHERITED = ("CURRENT_DATE", "CURRENT_DATE()", "today()", "curdate()")


def _days_it_inherits(sql: str) -> list:
    """The spellings in `sql` that ask the engine what day it is."""
    code = strip_comments_and_literals(sql)
    return [spelling for spelling in INHERITED if spelling in code]


def _reads_the_clock_without_its_zone(sql: str) -> bool:
    """A body may legitimately not mention today at all, but one that reads
    the clock must say whose day it means."""
    return "now()" in strip_comments_and_literals(sql) and DISPLAY_TIMEZONE not in sql


SMS_FRAGMENTS = dict(
    ltv_column="revenue_ltv",
    sales_type_filter="",
    tier_case="CASE WHEN 1 = 1 THEN 'VIP' END",
    arm_expr="'ALL'",
    ok_tier_expr="TRUE",
    # A group counting orders in the last N days, so the period column's
    # notion of today is in the text this reads.
    group_predicates=["(period_orders_1 >= ?)"],
    period_columns=[
        SmsAudienceFilters(period_days=92).period_count("period_orders_1")[0],
    ],
    tier_subset="",
)


def _rendered_bodies():
    """(label, sql) for every body that reaches more than one engine."""
    from core.repositories.revenue import _MANAGER_SALES_365D_SQL
    from core.sql_dialect import render_tables

    for dialect in (DUCKDB, POSTGRES):
        for name, sql in inventory_view_selects(dialect):
            yield f"inventory:{dialect.name}:{name}", sql
        yield f"sms_segments:{dialect.name}", sms_segments_select(
            dialect, **SMS_FRAGMENTS)
        # The managers screen's year. It used to start from `CURRENT_DATE`,
        # which on the Postgres server is UTC's day.
        yield f"manager_sales_365d:{dialect.name}", render_tables(
            _MANAGER_SALES_365D_SQL, dialect)

    # The audience's content filter, spliced into the audience above: "bought
    # within N days" counts back from today.
    sql, _params = SmsAudienceFilters(
        brands=("BrandA",), bought_within_days=30,
    ).predicate("revenue_ltv", "retail")
    yield "sms_audience_filter:bought_within_days", sql

    import core.sql_dialect as dialects
    cohort_bodies = (
        "cohort_retention_select", "enhanced_cohort_retention_select",
        "days_to_second_purchase_select", "cohort_ltv_select",
        "at_risk_customers_select",
    )
    # ClickHouse's alone since step 14's S14-07, which took DuckDB's analytics
    # dialect with the cohorts' fallback.
    for dialect in (CLICKHOUSE_ANALYTICS,):
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
    assert _days_it_inherits(sql) == [], (
        f"{label} takes its day from whichever timezone the engine happens "
        f"to be in; name the zone instead"
    )


@pytest.mark.parametrize("label,sql", BODIES, ids=[b[0] for b in BODIES])
def test_every_body_that_needs_today_names_the_zone(label, sql):
    assert not _reads_the_clock_without_its_zone(sql), (
        f"{label} uses a clock without naming the zone")


class TestTheTwoSpellingsStayApart:
    """The zone is named the same way everywhere it can be, and differently
    only where an engine refuses the shared form."""

    def test_duckdb_and_postgres_share_one_expression(self):
        """(DuckDB's analytics dialect said `TODAY_IN_KYIV` too, until step
        14's S14-07 took it.)"""
        assert "AT TIME ZONE" in TODAY_IN_KYIV
        assert DISPLAY_TIMEZONE in TODAY_IN_KYIV

    def test_clickhouse_gets_the_only_form_it_parses(self):
        # Measured on 24.8.14.39, production's version: it rejects
        # `AT TIME ZONE` outright, so this is a spelling hole and not a
        # second definition of what "today" means.
        assert "toTimeZone" in CLICKHOUSE_ANALYTICS.today
        assert "AT TIME ZONE" not in CLICKHOUSE_ANALYTICS.today
        assert DISPLAY_TIMEZONE in CLICKHOUSE_ANALYTICS.today


class _Recorded:
    """`core.pg.get_pool`'s answer that writes down every statement sent
    through it, then sends it on to `pool`."""

    _SENDS = ("fetch", "fetchrow", "fetchval", "execute", "executemany")

    def __init__(self, pool, sent: list):
        self._pool, self._sent = pool, sent

    @asynccontextmanager
    async def acquire(self, *args, **kwargs):
        async with self._pool.acquire(*args, **kwargs) as conn:
            yield _Recorded(conn, self._sent)

    def __getattr__(self, name):
        attr = getattr(self._pool, name)
        if name not in self._SENDS:
            return attr

        async def send(sql, *args, **kwargs):
            self._sent.append(sql)
            return await attr(sql, *args, **kwargs)
        return send


# Values for path parameters, where the route checks them.
PATH_VALUES = {"abc_class": "A"}
# Queries a route sends only when asked for something other than its
# defaults: one request per entry, beside the one with none.
VARIANTS = {"/api/stocks/trend": [{"granularity": "monthly"}]}


async def _what_the_inventory_tab_sends() -> list:
    """(route, statement) for every statement every GET route of the
    inventory tab sends Postgres, asked as an admin."""
    import re

    from tests.helpers.app_client import ConnectionPerAcquire, signed_in
    from tests.routes_helper import iter_endpoints
    from web.main import app

    routes = sorted(e.path for e in iter_endpoints(app) if "GET" in e.methods
                    and e.route.endpoint.__module__ == "web.routes.api.inventory")
    sent: list = []
    found = []
    async with signed_in("admin") as admin:
        with patch("core.pg.get_pool", new=AsyncMock(
                return_value=_Recorded(ConnectionPerAcquire(DSN), sent))):
            for route in routes:
                url = re.sub(r"\{(\w+)\}", lambda m: PATH_VALUES[m.group(1)], route)
                for params in [{}, *VARIANTS.get(route, [])]:
                    before = len(sent)
                    response = await admin.get(url, params=params)
                    assert response.status_code == 200, (
                        url, params, response.status_code, response.text[:300])
                    found += [(route, sql) for sql in sent[before:]]
    return found


class TestTheRoutedQueriesToo:
    """The view bodies are not the only SQL that needs today: the inventory
    tab's routes send queries of their own. Every statement they send is held
    to the same two rules, read off the connection rather than out of the
    source, so a query that moves to another method is still seen."""

    @needs_postgres
    @pytest.mark.asyncio
    async def test_no_query_the_inventory_tab_sends_inherits_a_day(self):
        sent = await _what_the_inventory_tab_sends()
        assert sent, "the inventory tab sent nothing: has it moved?"
        inherits = [(route, _days_it_inherits(sql)) for route, sql in sent
                    if _days_it_inherits(sql)]
        assert inherits == [], f"routed queries take their day from the engine: {inherits}"
        unnamed = [route for route, sql in sent if _reads_the_clock_without_its_zone(sql)]
        assert unnamed == [], f"routed queries read the clock without naming the zone: {unnamed}"


class TestAnInstantOnTheWireIsNormalised:
    """A timestamp that becomes a string must be pinned to UTC first.

    Behavioural, not structural: the failure is in what the driver renders, so
    nothing in the source distinguishes a safe `.isoformat()` from an unsafe
    one. Driving the engine under several session timezones is enough to
    catch it. Found on DuckDB, which rendered a `TIMESTAMPTZ` in the session's
    zone; since step 14's S14-07 `/inventory` reads Postgres alone, so the
    session driven is Postgres' (`server_settings`), against a real server.
    Skipped without `KS_PG_DSN`, which CI supplies.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("session_tz", ("UTC", "Europe/Kyiv",
                                            "Pacific/Kiritimati"))
    async def test_the_stock_summary_stamp_does_not_move_with_the_session(
        self, tmp_path, monkeypatch, session_tz,
    ):
        import os
        from datetime import date, datetime, timezone
        from unittest.mock import AsyncMock, patch

        import asyncpg

        from tests.integration.pg_tab_seed import store_that_never_reaches_duckdb

        dsn = os.getenv("KS_PG_DSN")
        if not dsn:
            pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
        stamp = datetime(2026, 9, 6, 11, 50, 45, tzinfo=timezone.utc)
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2,
                                         server_settings={"timezone": session_tz})
        tables = ("app.sku_inventory_status", "bronze.offer_stocks")

        async def _empty():
            async with pool.acquire() as conn:
                for table in tables:
                    await conn.execute(f"TRUNCATE {table}")

        try:
            await _empty()
            async with pool.acquire() as conn:
                assert await conn.fetchval("SHOW timezone") == session_tz
                await conn.execute(
                    "INSERT INTO bronze.offer_stocks (id, sku, price, purchased_price,"
                    " quantity, reserve) VALUES (1, 'S-1', 500, 250, 7, 0)")
                await conn.execute(
                    "INSERT INTO app.sku_inventory_status (offer_id, product_id, sku,"
                    " name, brand, category_id, quantity, reserve, price,"
                    " purchased_price, first_seen_at, updated_at)"
                    " VALUES (1, 101, 'S-1', 'Name', 'B', 1, 7, 0, 500, 250, $1, $2)",
                    date(2026, 1, 1), stamp)
            store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "/inventory")
            with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
                    patch("core.pg.require_revision", new=AsyncMock()):
                summary = await store.get_stock_summary(limit=5)
            assert summary["lastSync"] == "2026-09-06T11:50:45+00:00", (
                f"the session timezone reached the wire: {summary['lastSync']}"
            )
        finally:
            await _empty()
            await pool.close()
