"""A cached GA4 funnel answer never waits behind another period's fetch (L3-05).

`get_funnel` held one module-wide lock across the cache check and the whole
fetch — six GA4 requests of up to 20 s each — so a fresh cached answer for
October waited 2.97 s behind September's fetch. The cache is read outside any
lock now, and fetches are single-flight per period: two readers of one period
ask GA4 once, readers of different periods do not wait for each other. A
lock goes with its last user, whether the fetch succeeded or not, and at most
`GA4_CONCURRENCY` GA4 requests are in flight across all periods.
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

    async def ask(start, end, client, today, countries=None):
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

    async def ask(start, end, client, today, countries=None):
        return {"x": 1}

    monkeypatch.setattr(g, "_ask", ask)
    first = date(2025, 1, 1)
    for n in range(g.MAX_ENTRIES + 5):
        day = first + timedelta(days=n)
        await g.get_funnel(day, day, client=object())
    assert len(g._cache) == g.MAX_ENTRIES
    assert len(g._locks) <= g.MAX_ENTRIES
    g._reset_cache()


# ─── The second review: a cap across periods, and no lock left behind ───────
#
# Per-period locks let different uncached periods fetch at once, each with
# its six requests: three periods were eighteen GA4 requests in flight, where
# the Data API admits ten concurrent requests per standard property. And a
# lock was pruned only after a fetch that succeeded, so every period GA4
# failed for left one behind for as long as GA4 kept failing.

TODAY = date(2026, 11, 5)
#: Three periods whose three windows all lie inside GA4's 14 months, so each
#: costs the full six requests.
PERIODS = [(date(2026, 10, 1), date(2026, 10, 31)),
           (date(2026, 10, 10), date(2026, 10, 20)),
           (date(2026, 10, 25), date(2026, 10, 31))]


@pytest.mark.asyncio
async def test_ga4_never_has_more_than_the_cap_in_flight(monkeypatch):
    """Mutation: drop the semaphore around `ga4.run_funnel` — 18 at once."""
    from core import ga4

    g._reset_cache()
    in_flight = peak = calls = 0

    async def token(client, **kwargs):
        return "tok"

    async def run_funnel(client, tok, start, end, breakdown=None, countries=None):
        nonlocal in_flight, peak, calls
        in_flight += 1
        calls += 1
        peak = max(peak, in_flight)
        try:
            await asyncio.sleep(0.05)
            return {}
        finally:
            in_flight -= 1

    monkeypatch.setattr(ga4, "access_token", token)
    monkeypatch.setattr(ga4, "run_funnel", run_funnel)
    try:
        await asyncio.gather(*(g.get_funnel(s, e, client=object(), today=TODAY)
                               for s, e in PERIODS))
    finally:
        g._reset_cache()
    assert calls == 18
    assert peak <= 6, peak
    # Capped, not serialised: one period's six still go side by side.
    assert peak == 6


@pytest.mark.asyncio
async def test_a_period_ga4_failed_for_leaves_no_lock(monkeypatch):
    """Mutation: prune only after a fetch that succeeded — one lock per
    failing period, for as long as GA4 keeps failing."""
    from core import ga4

    g._reset_cache()

    async def ask(start, end, client, today, countries=None):
        await asyncio.sleep(0)
        raise ga4.GA4Unavailable("GA4 answered 503")

    monkeypatch.setattr(g, "_ask", ask)
    first = date(2025, 1, 1)
    try:
        for n in range(3 * g.MAX_ENTRIES):
            day = first + timedelta(days=n)
            with pytest.raises(ga4.GA4Unavailable):
                await g.get_funnel(day, day, client=object())
        # And when they fail side by side, two readers of one period among them.
        days = [first + timedelta(days=n) for n in range(10)] + [first]
        answers = await asyncio.gather(
            *(g.get_funnel(d, d, client=object()) for d in days),
            return_exceptions=True)
        assert all(isinstance(a, ga4.GA4Unavailable) for a in answers)
        assert g._locks == {}
    finally:
        g._reset_cache()


@pytest.mark.asyncio
async def test_a_reader_waiting_on_a_failed_fetch_keeps_its_lock(monkeypatch):
    """The prune must not take a lock somebody still waits on: a third reader
    arriving while the second waits shares the second's fetch, so a period
    is still asked once per failure, not once per reader."""
    from core import ga4

    g._reset_cache()
    asked: list = []

    async def ask(start, end, client, today, countries=None):
        asked.append((start, end))
        await asyncio.sleep(0.05)
        if len(asked) == 1:
            raise ga4.GA4Unavailable("GA4 answered 503")
        return {"x": len(asked)}

    monkeypatch.setattr(g, "_ask", ask)
    try:
        first = asyncio.create_task(g.get_funnel(*A, client=object()))
        second = asyncio.create_task(g.get_funnel(*A, client=object()))
        await asyncio.sleep(0.07)       # the first has failed, the second asks
        third = asyncio.create_task(g.get_funnel(*A, client=object()))
        with pytest.raises(ga4.GA4Unavailable):
            await first
        assert (await second)["x"] == (await third)["x"] == 2
        assert asked == [A, A]
        assert g._locks == {}
    finally:
        g._reset_cache()
