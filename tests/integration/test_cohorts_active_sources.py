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

import pytest

from tests.helpers.cohort_fixture import (
    BODIES, EXPECTED, RETIRED_MONEY, SECOND_ON_A_RETIRED_SOURCE,
    SECOND_ON_A_RETIRED_SOURCE_EXPECTED,
)
from tests.helpers.cohort_fixture import _ask, _at_risk_today, _seed

needs_clickhouse = pytest.mark.skipif(
    not os.getenv("KS_CH_URL"), reason="needs a live ClickHouse at KS_CH_URL",
)
pytestmark = needs_clickhouse


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
