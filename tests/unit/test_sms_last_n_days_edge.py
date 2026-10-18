""""The last N days" is today − N to today, both included — on the server too.

The page says, under a period's number, which dates it covers: on 18 Oct 2026
"the last 91 days" is «= с 19 июля по 18 октября 2026, включительно» — 92
calendar days, because every relative time here is `today − date <= N`
(`web/frontend/src/utils/smsDates.ts::lastDays`). These run the server's own
SQL fragments on DuckDB for the same day and the same N and require the same
first day; `web/frontend/src/utils/__tests__/smsDates.test.ts` states the same
literals. If either side moves an edge, one of the two fails.

The built-in presets were written against this rule (a "last 270 days" preset
sends exactly `max_recency_days=270`), so it is the label that follows the
query, never the other way round.
"""
from __future__ import annotations

from datetime import date

import duckdb
import pytest

from core.repositories.customers import SmsAudienceFilters
from core.sql_dialect import TODAY_IN_KYIV

TODAY = date(2026, 10, 18)
N = 91
FIRST_DAY_IN = date(2026, 7, 19)    # lastDays(91, '2026-10-18').from
LAST_DAY_OUT = date(2026, 7, 18)

ORDERS = [LAST_DAY_OUT, FIRST_DAY_IN, TODAY]


def _run(expression: str, params: list) -> list:
    """`expression` over one row per order date, with today pinned."""
    sql = expression.replace(TODAY_IN_KYIV, f"DATE '{TODAY.isoformat()}'")
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE o (order_id INTEGER, order_date DATE)")
        conn.executemany("INSERT INTO o VALUES (?, ?)",
                         [(i, d) for i, d in enumerate(ORDERS)])
        return conn.execute(sql, params).fetchall()


def test_the_today_expression_is_what_is_replaced():
    """Guard for the helper: if the fragment stops spelling today this way,
    the replacements below would test nothing."""
    expr, _ = SmsAudienceFilters(period_days=N).period_count("n")
    assert TODAY_IN_KYIV in expr


def test_orders_in_the_last_n_days_start_today_minus_n():
    expr, params = SmsAudienceFilters(period_days=N).period_count("n")
    [(counted,)] = _run(f"SELECT {expr} FROM o", params)

    assert counted == 2, f"{FIRST_DAY_IN} and {TODAY} are in, {LAST_DAY_OUT} is out"


def test_last_order_within_n_days_is_the_same_edge():
    """Recency is `today − MAX(order_date)`; "within N days" is `recency <= N`."""
    rows = _run(
        f"SELECT order_date FROM o WHERE ({TODAY_IN_KYIV} - order_date) <= ? "
        f"ORDER BY order_date",
        [N],
    )
    assert [r[0] for r in rows] == [FIRST_DAY_IN, TODAY]


@pytest.mark.parametrize("n", [1, 30, 90, 91, 92, 180, 365])
def test_every_n_is_n_plus_one_calendar_days(n):
    expr, params = SmsAudienceFilters(period_days=n).period_count("n")
    sql = expr.replace(TODAY_IN_KYIV, f"DATE '{TODAY.isoformat()}'")
    with duckdb.connect(":memory:") as conn:
        [(counted,)] = conn.execute(
            f"SELECT {sql} FROM (SELECT i AS order_id, "
            f"DATE '{TODAY.isoformat()}' - i::INTEGER AS order_date "
            f"FROM range(0, 4000) t(i))",
            params,
        ).fetchall()
    assert counted == n + 1
