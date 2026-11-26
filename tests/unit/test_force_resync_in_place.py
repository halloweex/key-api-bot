"""`force_resync` rewrites the window in place; it never empties the store.

It used to DELETE the orders, their line items and their expenses first and
refill them chunk by chunk, so a 429 storm halfway through a year-long walk
over the API — its ordinary failure — left months of orders missing, with
nothing to restore them but the hourly gap crawl at 200 ids an hour. In place,
with every order forced, it reaches the same end state and can simply be run
again.

Through the admin's door, `POST /api/sync/resync`, over the test Postgres with
chain 3's real writer and KeyCRM faked at its HTTP transport
(`tests.helpers.orders_sync`).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from core import pg_orders_write
from tests.helpers.app_client import signed_in
from tests.helpers.orders_sync import (
    answered, keycrm_serving_orders, listing_window, order_payload, orders_held,
    orders_in_postgres, versions_of)
from tests.integration.pg_tab_seed import needs_postgres

KYIV = ZoneInfo("Europe/Kyiv")
DAYS = 40
EARLY, LATE = 991_201, 991_202


def _kyiv_noon(days_ago: int) -> datetime:
    return (datetime.now(KYIV) - timedelta(days=days_ago)).replace(
        hour=12, minute=0, second=0, microsecond=0)


# Five weeks back and five days back: the walk over the window reaches the
# first well before the second.
EARLY_AT, LATE_AT = _kyiv_noon(35), _kyiv_noon(5)


async def _seed() -> None:
    """Both orders held at status 12, each with a line item and an expense."""
    await pg_orders_write.upsert_orders_with_expenses([
        order_payload(EARLY, created=EARLY_AT), order_payload(LATE, created=LATE_AT)])


@needs_postgres
class TestTheResyncRewritesInPlace:
    @pytest.mark.asyncio
    async def test_a_status_moved_without_updated_at_is_rewritten_and_nothing_is_deleted(
            self, monkeypatch):
        """KeyCRM serves EARLY cancelled under the `updated_at` Postgres holds,
        and no longer lists LATE. The resync writes the cancellation, which an
        unforced write skips as unchanged, and LATE stays with its basket and
        its cost: dropping what KeyCRM deleted is nobody's job here."""
        async with orders_in_postgres(), signed_in("admin") as admin:
            await _seed()
            keycrm_serving_orders(monkeypatch, [
                order_payload(EARLY, created=EARLY_AT, status=19, group=6)])

            r = await admin.post("/api/sync/resync", params={"days": DAYS})

            assert r.status_code == 200, r.text
            assert r.json()["stats"]["orders"] == 1
            held = await orders_held()
            assert held == {
                EARLY: {"status": 19, "lines": [EARLY * 1000], "expenses": [EARLY * 10]},
                LATE: {"status": 12, "lines": [LATE * 1000], "expenses": [LATE * 10]},
            }
            assert await versions_of(EARLY) == [("create", 12), ("change", 19)]
            assert await versions_of(LATE) == [("create", 12)]

    @pytest.mark.asyncio
    async def test_a_429_storm_partway_loses_no_order(self, monkeypatch):
        """KeyCRM answers the walk until it has served EARLY's window, then
        rate-limits every listing after it. The resync fails, the orders it had
        not reached are held as they were — basket and cost — and what it had
        written stays written, so running it again goes on from there."""
        def storm(request):
            window = listing_window(request)
            if window is not None and window[1] > EARLY_AT.date().isoformat():
                return answered(429)
            return None

        async with orders_in_postgres(), signed_in("admin") as admin:
            await _seed()
            keycrm_serving_orders(monkeypatch, [
                order_payload(EARLY, created=EARLY_AT, status=19, group=6),
                order_payload(LATE, created=LATE_AT, status=19, group=6),
            ], answer=storm)

            r = await admin.post("/api/sync/resync", params={"days": DAYS})

            assert r.status_code == 500, r.text
            assert await orders_held() == {
                EARLY: {"status": 19, "lines": [EARLY * 1000], "expenses": [EARLY * 10]},
                LATE: {"status": 12, "lines": [LATE * 1000], "expenses": [LATE * 10]},
            }
