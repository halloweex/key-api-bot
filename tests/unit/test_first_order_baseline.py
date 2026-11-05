"""A purchase on a retired channel is still a purchase.

The first-order baseline used to be computed over active sources only, so a
buyer whose first order was placed on Opencart counted as brand new the next
time they bought on Instagram: 419 buyers, 422 orders, ₴1,081,979.59 of repeat
business booked as acquisition, overstating new customers by 3.9% across 2025.

Silver is Postgres's (`core.pg_silver`), derived here from Bronze by the
derivation's own rebuild. Until step 14's S14-18 these ran the same shared
projection on DuckDB's landing (`tests/silver_on_duckdb.py`, deleted with them); the answers they
assert are DuckDB's on the same fixture, and Postgres gives them (rule 9).
"""
from datetime import date, datetime, timedelta, timezone

import pytest

from tests.integration.pg_tab_seed import (
    DSN, derive, needs_postgres, seed, tab_in_postgres,
)

pytestmark = needs_postgres

KYIV = timezone(timedelta(hours=3))


def _order(oid, buyer, source, ordered, status=12, total=100.0):
    """`bronze.orders` as the landing writes it, at noon Kyiv."""
    when = datetime.fromisoformat(f"{ordered}T12:00:00").replace(tzinfo=KYIV)
    return (oid, source, status, total, when, buyer, None, None)


async def _flags(monkeypatch, *orders):
    """`{id: (is_new_customer, buyer_first_order_date)}` from Postgres Silver."""
    async def populate(conn, pool):
        await seed(conn, orders=orders)

    async with tab_in_postgres(DSN, monkeypatch, populate) as pool:
        await derive(pool)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, is_new_customer, buyer_first_order_date "
                "FROM silver.orders ORDER BY id")
    return {r["id"]: (r["is_new_customer"], r["buyer_first_order_date"]) for r in rows}


class TestFirstOrderBaseline:
    @pytest.mark.asyncio
    async def test_opencart_first_order_makes_the_next_one_returning(self, monkeypatch):
        flags = await _flags(monkeypatch,
                             _order(1, buyer=50, source=3, ordered="2024-03-01"),  # Opencart
                             _order(2, buyer=50, source=1, ordered="2026-01-15"))  # Instagram
        assert flags[2][0] is False, "buyer had already bought — not an acquisition"
        assert flags[2][1] == date(2024, 3, 1)

    @pytest.mark.asyncio
    async def test_a_genuinely_first_order_is_still_new(self, monkeypatch):
        flags = await _flags(monkeypatch,
                             _order(1, buyer=51, source=1, ordered="2026-01-15"),
                             _order(2, buyer=51, source=1, ordered="2026-02-20"))
        assert flags[1][0] is True
        assert flags[2][0] is False

    @pytest.mark.asyncio
    async def test_a_cancelled_first_order_does_not_consume_the_baseline(self, monkeypatch):
        """Returns stay excluded — a cancelled order is not a purchase."""
        flags = await _flags(monkeypatch,
                             _order(1, buyer=52, source=1, ordered="2025-06-01", status=19),
                             _order(2, buyer=52, source=1, ordered="2026-01-15"))
        assert flags[2][0] is True, "their first actual purchase"

    @pytest.mark.asyncio
    async def test_an_order_on_a_retired_channel_is_never_flagged_new_itself(self, monkeypatch):
        """Inactive sources stay out of the metrics; only the baseline uses them."""
        flags = await _flags(monkeypatch, _order(1, buyer=53, source=3, ordered="2024-03-01"))
        assert flags[1][0] is False

    @pytest.mark.asyncio
    async def test_same_day_first_orders_are_both_new(self, monkeypatch):
        flags = await _flags(monkeypatch,
                             _order(1, buyer=54, source=1, ordered="2026-01-15"),
                             _order(2, buyer=54, source=2, ordered="2026-01-15"))
        assert flags[1][0] is True and flags[2][0] is True


# `TestDeadTableIsGone` stood here until step 14's S14-18: DuckDB's schema
# created no `daily_stats` (empty, unwritten, advertised as revenue by
# source). The schema went with the store.
