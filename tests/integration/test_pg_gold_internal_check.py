"""`pg_gold_internal_check` against a real Postgres (DN-28).

The one check that sees the fine rows of sources 3 and 5 used to be reachable
only through `reconcile_gold`, after a DuckDB read that step 13 retires. Here
it reads `gold.daily_revenue` by itself: an injected roll-up that its own fine
rows do not add up to is found, and one that they do is not.

Rows are written on dates no other test uses and removed afterwards; the
assertions look only at those dates, because the check reads the whole table.
"""
from __future__ import annotations

import os
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

GOOD = date(2099, 1, 1)
BAD = date(2099, 1, 2)
_INSERT = """
    INSERT INTO gold.daily_revenue (date, sales_type, source_id, revenue, orders_count,
        unique_customers, new_customers, returning_customers, returns_count,
        returns_revenue, avg_order_value)
    VALUES ($1, 'retail', $2, $3, $4, $5, 0, 0, 0, 0, 0)
"""


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4)

    async def clear():
        await p.execute("DELETE FROM gold.daily_revenue WHERE date >= $1", GOOD)

    await clear()
    with patch("core.pg.get_pool", new=AsyncMock(return_value=p)):
        yield p
    await clear()
    await p.close()


async def _mine():
    from core.mirror_reconciliation import pg_gold_internal_check

    issues = await pg_gold_internal_check(max_samples=10_000)
    return [i for i in issues if i.check_name == "gold_rollup_mismatch"]


@pytest.mark.asyncio
async def test_an_injected_roll_up_mismatch_is_found(pool):
    # A roll-up of 100 over fine rows of 60 (Instagram) and 30 (Виставка):
    # the ten that went nowhere is the rebuild writing from two reads.
    await pool.executemany(_INSERT, [
        (BAD, None, 100, 3, 3),
        (BAD, 1, 60, 2, 2),
        (BAD, 5, 30, 1, 1),
    ])
    (issue,) = await _mine()
    assert issue.severity.name == "CRITICAL"
    assert str(BAD) in issue.description and "revenue" in issue.description


@pytest.mark.asyncio
async def test_fine_rows_that_add_up_are_silent(pool):
    # The distinct count is 2 at the roll-up and 1 + 2 below it — one buyer on
    # two channels — and that is not a mismatch: it is why the roll-up exists.
    await pool.executemany(_INSERT, [
        (GOOD, None, 100, 3, 2),
        (GOOD, 1, 70, 2, 1),
        (GOOD, 5, 30, 1, 2),
    ])
    assert all(str(GOOD) not in i.description for i in await _mine())


@pytest.mark.asyncio
async def test_it_names_the_cell_and_leaves_the_good_one_out(pool):
    await pool.executemany(_INSERT, [
        (GOOD, None, 100, 3, 2),
        (GOOD, 1, 70, 2, 1),
        (GOOD, 5, 30, 1, 2),
        (BAD, None, 100, 3, 3),
        (BAD, 1, 60, 2, 2),
        (BAD, 5, 30, 2, 1),
    ])
    (issue,) = await _mine()
    assert str(BAD) in issue.description and str(GOOD) not in issue.description
    # Revenue and orders both went astray in the bad cell, and both are named.
    assert "orders_count" in issue.description
