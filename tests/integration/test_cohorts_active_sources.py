"""The owner's decision O5 on the engine production runs the cohorts on.

`tests/unit/test_cohort_active_sources.py` holds the five cohort bodies to
their answers on DuckDB in memory; production renders the same text for
ClickHouse (`CLICKHOUSE_ANALYTICS`) and reads it through `core.ch_cohorts`,
typed as the tab reads it. This seeds the same orders into a ClickHouse
`silver.orders` shaped like the shipped copy (`is_active_source Bool`) and
asks every body again: the Opencart buyer stays in March's cohort, and the
Opencart money is in no cell.

Skipped without `KS_CH_URL`, the contract every ClickHouse-touching test here
has: point it at a throwaway 24.8 server, production's version.
"""
from __future__ import annotations

import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

import core.sql_dialect as dialects
from core.sql_dialect import CLICKHOUSE_ANALYTICS
from tests.unit.test_cohort_active_sources import (
    BODIES, EXPECTED, ORDERS, RETAIL, RETIRED_MONEY, SECOND_ON_A_RETIRED_SOURCE,
    SECOND_ON_A_RETIRED_SOURCE_EXPECTED,
)

needs_clickhouse = pytest.mark.skipif(
    not os.getenv("KS_CH_URL"), reason="needs a live ClickHouse at KS_CH_URL",
)
pytestmark = needs_clickhouse

TYPES = {
    "cohort_retention_select": "COHORT_RETENTION_TYPES",
    "enhanced_cohort_retention_select": "ENHANCED_RETENTION_TYPES",
    "days_to_second_purchase_select": "DAYS_TO_SECOND_TYPES",
    "cohort_ltv_select": "COHORT_LTV_TYPES",
    "at_risk_customers_select": "AT_RISK_TYPES",
}


async def _seed(orders=ORDERS):
    """`silver.orders` by the shipper's own DDL, so the column types are
    production's (`is_active_source Bool`, `LowCardinality` sales type)."""
    from core.ch_common import execute
    from core.ch_silver import _SILVER_DDL

    await execute("CREATE DATABASE IF NOT EXISTS silver")
    await execute("DROP TABLE IF EXISTS silver.orders")
    await execute(_SILVER_DDL.format(name="silver.orders"))
    values = ", ".join(
        f"({oid}, {buyer}, toDate('{day}'), false, 'retail', {total}, {source},"
        f" {'true' if active else 'false'})"
        for oid, buyer, day, source, active, total in orders)
    await execute("INSERT INTO silver.orders (id, buyer_id, order_date, is_return,"
                  " sales_type, grand_total, source_id, is_active_source)"
                  f" VALUES {values}")


async def _ask(name):
    from core import ch_cohorts

    sql = getattr(dialects, name)(CLICKHOUSE_ANALYTICS, sales_type_filter=RETAIL,
                                  months_back=24)
    rows = await ch_cohorts.fetch(sql, BODIES[name], getattr(dialects, TYPES[name]))
    return [tuple(round(v, 1) if isinstance(v, float) else v for v in r) for r in rows]


def _at_risk_today():
    """The at-risk row for the server's today, which is Kyiv's: buyer 11 last
    bought on an active source on 15 June (₴1 700 over two orders), buyer 12
    on 10 March (₴400, one order), buyer 13 never."""
    today = datetime.now(ZoneInfo("Europe/Kyiv")).date()
    last = {11: (date(2026, 6, 15), 1700.0, 2), 12: (date(2026, 3, 10), 400.0, 1)}
    days = {b: (today - d).days for b, (d, _r, _o) in last.items()}
    past_90 = [b for b, d in days.items() if d > 90]
    return [(
        "2026-03", 3,
        sum(1 for d in days.values() if 90 < d <= 180),
        round(100.0 * len(past_90) / 3, 1),
        sum(last[b][1] for b in past_90) if past_90 else None,
        round(sum(last[b][2] for b in past_90) / len(past_90), 1) if past_90 else None,
        sum(1 for d in days.values() if d > 180),
    )]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", sorted(BODIES))
async def test_clickhouse_answers_without_the_retired_source(name):
    """Mutation: drop `AND o.is_active_source` from the body — ClickHouse then
    counts the Opencart buyer in March and Opencart's money in its cells."""
    await _seed()
    rows = await _ask(name)
    expected = _at_risk_today() if name == "at_risk_customers_select" else EXPECTED[name]
    assert rows == expected
    for row in rows:
        assert not set(row) & set(RETIRED_MONEY), (name, row)


@pytest.mark.asyncio
async def test_clickhouse_takes_the_second_purchase_on_an_active_source():
    """Mutation: join the second purchase on `order_num = 2` again — the
    Opencart order ten days after the first is then the second purchase."""
    await _seed(SECOND_ON_A_RETIRED_SOURCE)
    assert await _ask("days_to_second_purchase_select") == SECOND_ON_A_RETIRED_SOURCE_EXPECTED
