"""A cached GA4 funnel answer never waits behind another period's fetch (L3-05).

`get_funnel` held one module-wide lock across the cache check and the whole
fetch — six GA4 requests of up to 20 s each — so a fresh cached answer for
October waited 2.97 s behind September's fetch. The cache is read outside any
lock now, and fetches are single-flight per period: two readers of one period
ask GA4 once, readers of different periods do not wait for each other. The
locks are pruned with the cache's bound. Only the locking changed.
"""
from __future__ import annotations

import asyncio
import time
from datetime import date, timedelta

import pytest

from web.services import ga4_funnel_service as g

A = (date(2026, 10, 1), date(2026, 10, 31))
B = (date(2026, 9, 1), date(2026, 9, 30))


@pytest.fixture
def slow(monkeypatch):
    g._reset_cache()
    asked: list = []

    async def ask(start, end, client, today):
        asked.append((start, end))
        await asyncio.sleep(0.5)
        return {"x": len(asked)}

    monkeypatch.setattr(g, "_ask", ask)
    yield asked
    g._reset_cache()


@pytest.mark.asyncio
async def test_a_cached_hit_does_not_wait_for_another_period(slow):
    """Mutation: one lock around the check and the fetch again — ~0.5 s."""
    await g.get_funnel(*A, client=object())
    other = asyncio.create_task(g.get_funnel(*B, client=object()))
    await asyncio.sleep(0.05)
    started = time.monotonic()
    hit = await g.get_funnel(*A, client=object())
    waited = time.monotonic() - started
    await other
    assert waited < 0.1, waited
    assert hit["stale"] is False


@pytest.mark.asyncio
async def test_two_readers_of_one_period_ask_once(slow):
    first, second = await asyncio.gather(g.get_funnel(*A, client=object()),
                                         g.get_funnel(*A, client=object()))
    assert slow == [A]
    assert first["x"] == second["x"] == 1


@pytest.mark.asyncio
async def test_different_periods_fetch_side_by_side(slow):
    started = time.monotonic()
    await asyncio.gather(g.get_funnel(*A, client=object()),
                         g.get_funnel(*B, client=object()))
    assert time.monotonic() - started < 0.9
    assert sorted(slow) == sorted([A, B])


@pytest.mark.asyncio
async def test_the_locks_keep_the_cache_s_bound(monkeypatch):
    g._reset_cache()

    async def ask(start, end, client, today):
        return {"x": 1}

    monkeypatch.setattr(g, "_ask", ask)
    first = date(2025, 1, 1)
    for n in range(g.MAX_ENTRIES + 5):
        day = first + timedelta(days=n)
        await g.get_funnel(day, day, client=object())
    assert len(g._cache) == g.MAX_ENTRIES
    assert len(g._locks) <= g.MAX_ENTRIES
    g._reset_cache()
