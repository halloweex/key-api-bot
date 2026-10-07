"""Each incident of a condition is its own cycle — run against real SQL.

The ledger keeps one row per condition for good, and until 2026-09-25 a
condition that cleared and came back carried its first incident's clock into
every later one. The escalator took the last *resolution* as the start of the
cycle, so the healthy days between two incidents counted as firing:
freshness_orders resolved 09-21 10:00, returned 09-25 04:00, and was escalated
at 04:10 as "90h". Of every escalation ever sent, 8 of 11 came 0.0-0.2 h into
their incident against a six-hour bar.

The unit tests for these queries read their text and never ran them, which is
how that lived for a month. These run them.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

KEY = "freshness_orders"          # a registered condition
H = timedelta(hours=1)


@pytest_asyncio.fixture
async def pool(monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    async with p.acquire() as conn:
        await conn.execute("DELETE FROM app.alert_events")
        await conn.execute("DELETE FROM app.alert_series")
    with patch("core.pg.get_pool", AsyncMock(return_value=p)):
        yield p
    await p.close()


async def _now(conn) -> datetime:
    return await conn.fetchval("SELECT now()")


async def _series(conn, *, state, first, count, acked=False, suppressed=0):
    await conn.execute(
        """
        INSERT INTO app.alert_series
            (condition_key, kind, state, first_fired_at, last_fired_at,
             fired_count, suppressed_count, resolved_at, instance,
             acknowledged_by, acknowledged_at, acknowledged_reason)
        VALUES ($1, 'condition', $2, $3, $3, $4, $9, $5, 'test', $6, $7, $8)
        """,
        KEY, state, first, count,
        first if state == "resolved" else None,
        "someone" if acked else None,
        first if acked else None,
        "known" if acked else None,
        suppressed,
    )


async def _event(conn, kind, at):
    await conn.execute(
        "INSERT INTO app.alert_events (condition_key, event_type, instance, at) "
        "VALUES ($1, $2, 'test', $3)",
        KEY, kind, at,
    )


async def _due(conn):
    from core.alert_escalator import ESCALATE_AFTER_S, _DUE_QUERY

    return await conn.fetch(_DUE_QUERY, float(ESCALATE_AFTER_S))


class TestTheRowOpensANewCycle:
    @pytest.mark.asyncio
    async def test_a_return_after_a_resolve_starts_its_own_clock(self, pool):
        from core.alert_archive import _write_fired

        async with pool.acquire() as conn:
            t = await _now(conn)
            await _series(conn, state="resolved", first=t - 96 * H, count=5,
                          acked=True, suppressed=9)
        await _write_fired([KEY], "back", 1, 3)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM app.alert_series WHERE condition_key = $1", KEY)
            t = await _now(conn)
        assert row["state"] == "firing"
        assert abs((row["first_fired_at"] - t).total_seconds()) < 60
        assert row["fired_count"] == 1
        assert row["suppressed_count"] == 3        # this incident's, not 9 + 3
        # An acknowledgement belongs to the incident it was given for.
        assert row["acknowledged_at"] is None
        assert row["acknowledged_by"] is None
        assert row["acknowledged_reason"] is None

    @pytest.mark.asyncio
    async def test_a_standing_condition_keeps_its_cycle(self, pool):
        from core.alert_archive import _write_fired

        async with pool.acquire() as conn:
            t = await _now(conn)
            first = t - 5 * H
            await _series(conn, state="firing", first=first, count=1,
                          acked=True, suppressed=9)
        await _write_fired([KEY], "still", 1, 3)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM app.alert_series WHERE condition_key = $1", KEY)
        assert row["first_fired_at"] == first
        assert row["fired_count"] == 2
        assert row["suppressed_count"] == 12
        # A repeat inside the incident keeps the acknowledgement given for it.
        assert row["acknowledged_by"] == "someone"
        assert row["acknowledged_at"] == first
        assert row["acknowledged_reason"] == "known"


class TestTheEscalatorMeasuresTheIncident:
    @pytest.mark.asyncio
    async def test_the_25_09_case_waits_six_hours_from_the_return(self, pool):
        """Resolved 90 h ago, back now. Not due — this is ten minutes old."""
        from core.alert_archive import _write_fired

        async with pool.acquire() as conn:
            t = await _now(conn)
            await _series(conn, state="resolved", first=t - 96 * H, count=1)
            await _event(conn, "fired", t - 96 * H)
            await _event(conn, "resolved", t - 90 * H)
        await _write_fired([KEY], "back", 1, 0)
        async with pool.acquire() as conn:
            assert await _due(conn) == []

    @pytest.mark.asyncio
    async def test_a_row_written_before_the_fix_is_read_from_its_events(self, pool):
        """Production's freshness_orders row as it stands: firing, its first
        fire four days back, a resolve and a new fire since. The events say
        when this incident began, whatever the row says."""
        async with pool.acquire() as conn:
            t = await _now(conn)
            await _series(conn, state="firing", first=t - 200 * H, count=3)
            await _event(conn, "fired", t - 200 * H)
            await _event(conn, "resolved", t - 190 * H)
            await _event(conn, "fired", t - 96 * H)
            await _event(conn, "resolved", t - 90 * H)
            # Not a fire: a cycle that began here would be sixty hours old.
            await _event(conn, "action_requested", t - 60 * H)
            await _event(conn, "fired", t - timedelta(minutes=10))
            assert await _due(conn) == []

    @pytest.mark.asyncio
    async def test_a_condition_standing_six_hours_is_escalated_once(self, pool):
        """The other half: the fix must not make the escalator deaf."""
        async with pool.acquire() as conn:
            t = await _now(conn)
            await _series(conn, state="firing", first=t - 7 * H, count=1)
            await _event(conn, "fired", t - 7 * H)
            due = await _due(conn)
            assert [r["condition_key"] for r in due] == [KEY]

            await _event(conn, "escalated", t - 1 * H)
            assert await _due(conn) == []

    @pytest.mark.asyncio
    async def test_a_cycle_is_timed_from_its_first_fire_not_its_latest(self, pool):
        """An incident fires more than once — repeats in its loud half-hour,
        then a reminder a day. Its age runs from the first of them: timed
        from the latest, a condition would never stand six hours for as long
        as it kept reminding."""
        from core.alert_archive import fetch_digest_tail

        async with pool.acquire() as conn:
            t = await _now(conn)
            await _series(conn, state="firing", first=t - 100 * H, count=4)
            await _event(conn, "fired", t - 100 * H)
            await _event(conn, "resolved", t - 90 * H)
            await _event(conn, "fired", t - 7 * H)
            await _event(conn, "fired", t - 6.5 * H)
            await _event(conn, "fired", t - 1 * H)
            due = await _due(conn)
            assert [r["condition_key"] for r in due] == [KEY]
            assert abs((due[0]["cycle_start"] - (t - 7 * H)).total_seconds()) < 1

            await _event(conn, "escalated", t - 5 * H)
            assert await _due(conn) == []
        tail = await fetch_digest_tail()
        assert f"• {KEY} — 7h" in tail, tail
        assert "· escalated" in tail

    @pytest.mark.asyncio
    async def test_a_return_standing_six_hours_is_escalated_again(self, pool):
        """A new incident earns its own escalation — once it has stood."""
        async with pool.acquire() as conn:
            t = await _now(conn)
            await _series(conn, state="firing", first=t - 96 * H, count=2)
            await _event(conn, "fired", t - 96 * H)
            await _event(conn, "escalated", t - 90 * H)
            await _event(conn, "resolved", t - 80 * H)
            await _event(conn, "fired", t - 7 * H)
            due = await _due(conn)
        assert [r["condition_key"] for r in due] == [KEY]
        assert abs((due[0]["cycle_start"] - (t - 7 * H)).total_seconds()) < 1


class TestTheDigestAgesTheIncident:
    @pytest.mark.asyncio
    async def test_the_tail_says_minutes_not_days(self, pool):
        from core.alert_archive import fetch_digest_tail

        async with pool.acquire() as conn:
            t = await _now(conn)
            await _series(conn, state="firing", first=t - 96 * H, count=2)
            await _event(conn, "fired", t - 96 * H)
            await _event(conn, "escalated", t - 93 * H)
            await _event(conn, "resolved", t - 90 * H)
            await _event(conn, "fired", t - timedelta(minutes=10))
        tail = await fetch_digest_tail()
        assert tail is not None
        assert f"• {KEY} — 0h" in tail, tail
        assert "4d" not in tail
        # That escalation was the last incident's; this one has had none.
        assert "escalated" not in tail, tail
