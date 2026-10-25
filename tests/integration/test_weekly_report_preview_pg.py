"""`scripts/weekly_report_preview.py` against a real PostgreSQL.

The unit tests prove the preview reaches Postgres through the weekly report's
own readers with `pg_weekly_read.fetch` stood in. This seeds two weeks of
`gold.daily_revenue` — the roll-up rows and two channels' fine rows — on a
live database at head and builds the preview's report from them: the numbers
the preview would send are Postgres's, read through `build_report` and its
`fetch_*` exactly as the `weekly_report` job reads them, with a store that
refuses to connect standing where the job's would.

The traffic half does the same for `--report traffic`: `silver.orders` and
`silver.order_utm` seeded for a week of their own, and the week built by
`send_traffic` through the traffic tab's repository methods, Telegram alone
stood in.
"""
from __future__ import annotations

import importlib.util
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "weekly_report_preview_pg", ROOT / "scripts" / "weekly_report_preview.py")
preview = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(preview)

# A Monday nobody else's fixture writes, the week before it, and the Monday
# after the reported week — the `today` the report is built for.
MONDAY = date(2001, 4, 2)
PREVIOUS = MONDAY - timedelta(days=7)
TODAY = MONDAY + timedelta(days=7)
FIRST, LAST = PREVIOUS, MONDAY + timedelta(days=6)


def _row(day, source, revenue, orders):
    return (day, "retail", source, revenue, orders, orders, 0, orders, 0, 0,
            round(revenue / orders, 2) if orders else 0)


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM gold.daily_revenue WHERE date BETWEEN $1 AND $2", FIRST, LAST)


@pytest_asyncio.fixture
async def pool(monkeypatch):
    for name in ("KS_READ_WEEKLY", "KS_READ_TRAFFIC"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_READ_WEEKLY", "postgres")
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    rows = []
    for i in range(7):
        # This week: ₴100 a day on Instagram, ₴50 on Shopify; the week
        # before, ₴80 a day on Instagram alone.
        day = MONDAY + timedelta(days=i)
        rows += [_row(day, None, 150.0, 3), _row(day, 1, 100.0, 2), _row(day, 4, 50.0, 1)]
        before = PREVIOUS + timedelta(days=i)
        rows += [_row(before, None, 80.0, 1), _row(before, 1, 80.0, 1)]
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO gold.daily_revenue (date, sales_type, source_id, revenue,
                orders_count, unique_customers, new_customers,
                returning_customers, returns_count, returns_revenue,
                avg_order_value)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """, rows)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield pool
    finally:
        await _clean(pool)
        await pool.close()


@pytest.mark.asyncio
async def test_the_preview_builds_the_week_from_postgres(pool):
    assert preview.postgres_unready("sales") is None

    report = await preview.live_report("retail", TODAY)

    assert (report.start, report.end) == (MONDAY, MONDAY + timedelta(days=6))
    # The roll-up rows, never the roll-up and the fine rows together.
    assert (report.current.revenue, report.current.orders) == (1050.0, 21)
    assert report.previous is not None
    assert (report.previous.revenue, report.previous.orders) == (560.0, 7)
    assert [d.revenue for d in report.days] == [150.0] * 7
    assert [d.revenue for d in report.previous_days] == [80.0] * 7
    # The channels come from the fine rows.
    by_name = {c.name: c for c in report.channels}
    assert by_name["Instagram"].revenue == 700.0
    assert by_name["Instagram"].previous_revenue == 560.0
    assert by_name["Shopify"].revenue == 350.0


# ─── The traffic report ─────────────────────────────────────────────────────
#
# `--report traffic` builds the week through the traffic tab's own repository
# methods, which read `silver.orders LEFT JOIN silver.order_utm` once
# `KS_READ_TRAFFIC=postgres`. Seeded here for a week of its own, under ids no
# other fixture uses, and built by `send_traffic` itself with only the
# Telegram send stood in — so a read that went anywhere but Postgres would
# reach the store that refuses to connect and stop the run.

T_MONDAY = date(2001, 5, 7)
T_PREVIOUS = T_MONDAY - timedelta(days=7)
T_TODAY = T_MONDAY + timedelta(days=7)
T_IDS = range(990_000_001, 990_000_007)

# (id, source_id, grand_total, day, is_return, sales_type)
T_ORDERS = [
    (990_000_001, 4, 3000.0, T_MONDAY + timedelta(days=1), False, "retail"),
    (990_000_002, 1, 1000.0, T_MONDAY + timedelta(days=2), False, "retail"),
    (990_000_003, 4, 500.0, T_MONDAY + timedelta(days=3), True, "retail"),   # a return
    (990_000_004, 4, 9000.0, T_MONDAY + timedelta(days=4), False, "b2b"),    # not retail
    (990_000_005, 4, 800.0, T_PREVIOUS + timedelta(days=1), False, "retail"),
    (990_000_006, 1, 700.0, T_PREVIOUS + timedelta(days=2), False, "retail"),
]
# The two Shopify orders carry a paid Facebook campaign; the Instagram ones
# have no UTM row and fall through the COALESCE to organic / instagram.
T_UTM = [
    (990_000_001, "fbads", "paid", "spring", "paid_confirmed", "facebook"),
    (990_000_005, "fbads", "paid", "spring", "paid_confirmed", "facebook"),
]
_SOURCE = {1: "Instagram", 4: "Shopify"}


async def _clean_traffic(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM silver.order_utm WHERE order_id BETWEEN $1 AND $2",
            T_IDS[0], T_IDS[-1])
        await conn.execute(
            "DELETE FROM silver.orders WHERE id BETWEEN $1 AND $2",
            T_IDS[0], T_IDS[-1])


@pytest_asyncio.fixture
async def traffic_pool(monkeypatch):
    for name in ("KS_READ_WEEKLY", "KS_READ_TRAFFIC"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_READ_TRAFFIC", "postgres")
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean_traffic(pool)
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO silver.orders (id, source_id, status_id, grand_total,
                ordered_at, buyer_id, manager_id, order_date, is_return,
                sales_type, is_active_source, source_name, is_new_customer,
                buyer_first_order_date, promocode)
            VALUES ($1, $2, 1, $3, $4, $1, NULL, $5, $6, $7, TRUE, $8, FALSE,
                    NULL, NULL)
            """,
            [(oid, src, total,
              datetime.combine(day, time(12, 0), tzinfo=timezone.utc),
              day, is_return, sales_type, _SOURCE[src])
             for oid, src, total, day, is_return, sales_type in T_ORDERS])
        await conn.executemany(
            """
            INSERT INTO silver.order_utm (order_id, utm_source, utm_medium,
                utm_campaign, traffic_type, platform, parsed_at)
            VALUES ($1, $2, $3, $4, $5, $6, now())
            """, T_UTM)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield pool
    finally:
        await _clean_traffic(pool)
        await pool.close()


@pytest.mark.asyncio
async def test_the_preview_builds_the_traffic_week_from_postgres(traffic_pool, monkeypatch):
    from core import telegram_alerts, traffic_report

    assert preview.postgres_unready("traffic") is None

    built = []
    real = traffic_report.build_report

    async def spy(store, today, *args, **kwargs):
        report = await real(store, today, *args, **kwargs)
        built.append(report)
        return report

    monkeypatch.setattr(traffic_report, "build_report", spy)
    sent = AsyncMock(return_value=1)
    monkeypatch.setattr(telegram_alerts, "send_rich_message_http", sent)

    await preview.send_traffic(5, "en", None, today=T_TODAY)

    (report,) = built
    assert (report.start, report.end) == (T_MONDAY, T_MONDAY + timedelta(days=6))
    # Retail and not returned: the return and the b2b order never count.
    assert (report.revenue, report.orders) == (4000.0, 2)
    assert (report.previous_revenue, report.previous_orders) == (1500.0, 2)
    buckets = {b.name: b for b in report.buckets}
    assert (buckets["paid"].revenue, buckets["paid"].previous_revenue) == (3000.0, 800.0)
    assert (buckets["organic"].revenue, buckets["organic"].previous_revenue) == (1000.0, 700.0)
    platforms = {p.name: p for p in report.platforms}
    assert (platforms["facebook"].revenue, platforms["facebook"].previous_revenue) == (3000.0, 800.0)
    assert platforms["instagram"].revenue == 1000.0
    # ₴2 200 moved, over the ₴2 000 floor; the untagged row is never a mover.
    assert [(m.campaign, m.current, m.previous) for m in report.movers] == [
        ("spring", 3000.0, 800.0)]
    assert sent.await_count == 1 and sent.await_args.kwargs["chat_ids"] == [5]
