"""The canary's watch row, written by the real statement on a real Postgres.

`core.alert_archive._WATCH_SQL` keeps one row, `watch:read_fallbacks` in
`app.alert_series`, that says since when every probe read `/api/health`'s
`read_fallbacks` empty (OD-07). Its arithmetic is all in the statement — a
continuing run, an unread tail that restarts it (from the last probe to the
new web process's start, read from `uptime_seconds`), a fallback that restarts
it at itself —
so it is held here by executing it, against rows seeded where the clock can be
set: `now()` is fixed inside a transaction, so the row is seeded relative to it
and the statement then runs once, in the same rolled-back transaction.

And the row must stay what the archive says it is: an event-kind record nothing
that reads the series ever reads — not the escalator, not the digest's tail.
"""
from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

from bot.canary import READ_FALLBACK_WATCH_GAP_S, READ_FALLBACK_WATCH_KEY  # noqa: E402
from core import alert_archive  # noqa: E402

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

KEY = READ_FALLBACK_WATCH_KEY
GAP = float(READ_FALLBACK_WATCH_GAP_S)


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    yield p
    await p.close()


@asynccontextmanager
async def tx(pool):
    async with pool.acquire() as conn:
        t = conn.transaction()
        await t.start()
        try:
            await conn.execute("DELETE FROM app.alert_series WHERE condition_key = $1", KEY)
            yield conn
        finally:
            await t.rollback()


async def seed(conn, *, since_min: float, last_min: float, probes: int) -> None:
    """The row as a run of probes left it: clean since `since_min` minutes
    before this transaction's now(), last probe `last_min` minutes before."""
    await conn.execute(
        """
        INSERT INTO app.alert_series (condition_key, kind, state, first_fired_at,
                                      last_fired_at, fired_count, suppressed_count,
                                      instance)
        VALUES ($1, 'event', 'event', now() - make_interval(secs => $2::float8 * 60),
                now() - make_interval(secs => $3::float8 * 60), $4, 0, 'seeded')
        """, KEY, float(since_min), float(last_min), probes)


async def probe(conn, clean: bool, uptime_min: "float | None" = None) -> dict:
    """One probe through the statement the bot runs. `uptime_min` is how long
    the web process it read had been running; None is a probe that read no
    uptime, judged by the gap alone."""
    uptime = None if uptime_min is None else float(uptime_min) * 60
    await conn.execute(alert_archive._WATCH_SQL, KEY, clean, GAP, "bot-host", uptime)
    row = await conn.fetchrow(
        """
        SELECT kind, state, fired_count, instance,
               EXTRACT(EPOCH FROM now() - first_fired_at)::int AS since_s,
               EXTRACT(EPOCH FROM now() - last_fired_at)::int AS last_s
        FROM app.alert_series WHERE condition_key = $1
        """, KEY)
    return dict(row)


class TestTheRun:
    @pytest.mark.asyncio
    async def test_a_first_clean_probe_opens_a_run(self, pool):
        async with tx(pool) as conn:
            row = await probe(conn, clean=True)
        assert row == {"kind": "event", "state": "event", "fired_count": 1,
                       "instance": "bot-host", "since_s": 0, "last_s": 0}

    @pytest.mark.asyncio
    async def test_a_first_dirty_probe_opens_nothing_clean(self, pool):
        async with tx(pool) as conn:
            row = await probe(conn, clean=False)
        assert (row["fired_count"], row["since_s"], row["last_s"]) == (0, 0, 0)

    @pytest.mark.asyncio
    async def test_a_clean_probe_inside_the_gap_extends_the_run(self, pool):
        async with tx(pool) as conn:
            await seed(conn, since_min=170 * 60, last_min=15, probes=680)
            row = await probe(conn, clean=True)
        assert row["fired_count"] == 681
        assert row["since_s"] == 170 * 3600, "the run keeps its start"
        assert row["last_s"] == 0

    @pytest.mark.asyncio
    async def test_a_probe_lost_to_a_deploy_does_not_break_it(self, pool):
        """One probe lost — web recreated, or the bot restarted and probing
        90 s after — leaves about 30 minutes."""
        async with tx(pool) as conn:
            await seed(conn, since_min=500, last_min=31, probes=30)
            row = await probe(conn, clean=True)
        assert row["fired_count"] == 31 and row["since_s"] == 500 * 60

    @pytest.mark.asyncio
    async def test_a_gap_past_the_limit_restarts_the_run_at_the_next_probe(self, pool):
        """Nobody read the block for longer than the gap, so a web process
        could have come and gone with its counters unread."""
        async with tx(pool) as conn:
            await seed(conn, since_min=170 * 60,
                       last_min=READ_FALLBACK_WATCH_GAP_S / 60 + 1, probes=680)
            row = await probe(conn, clean=True)
        assert (row["fired_count"], row["since_s"], row["last_s"]) == (1, 0, 0)

    @pytest.mark.asyncio
    async def test_the_same_process_after_any_gap_keeps_the_run(self, pool):
        """Web started long before the last probe, so this probe read the
        same counters, which cover everything since: the bot being away for
        two hours lost nothing."""
        async with tx(pool) as conn:
            await seed(conn, since_min=170 * 60, last_min=120, probes=680)
            row = await probe(conn, clean=True, uptime_min=48 * 60)
        assert (row["fired_count"], row["since_s"]) == (681, 170 * 3600)

    @pytest.mark.asyncio
    async def test_a_sunday_compaction_keeps_the_run(self, pool):
        """The review's arithmetic: last probe of the old web 15 min before
        the stop, 5 min of compaction, then web's startup and the bot's
        first probes failing until 16.5 min after the bot started — a 36.5
        min gap between probes, which used to restart the week every Sunday.
        The unread tail is 20 min: from the last probe to the new start."""
        async with tx(pool) as conn:
            await seed(conn, since_min=170 * 60, last_min=36.5, probes=680)
            row = await probe(conn, clean=True, uptime_min=16.5)
        assert (row["fired_count"], row["since_s"]) == (681, 170 * 3600)

    @pytest.mark.asyncio
    async def test_a_new_process_that_started_past_the_limit_restarts_it(self, pool):
        """Web replaced, and its predecessor unread for 40 min before that:
        what the old process counted in its tail was never read."""
        async with tx(pool) as conn:
            await seed(conn, since_min=170 * 60, last_min=50, probes=680)
            row = await probe(conn, clean=True, uptime_min=10)
        assert (row["fired_count"], row["since_s"], row["last_s"]) == (1, 0, 0)

    @pytest.mark.asyncio
    async def test_the_limit_is_the_tail_and_it_is_inclusive(self, pool):
        """A tail of exactly the gap keeps the run; one second more does not."""
        limit_min = READ_FALLBACK_WATCH_GAP_S / 60
        async with tx(pool) as conn:
            await seed(conn, since_min=500, last_min=limit_min + 5, probes=30)
            row = await probe(conn, clean=True, uptime_min=5)
        assert row["fired_count"] == 31
        async with tx(pool) as conn:
            await seed(conn, since_min=500, last_min=limit_min + 5, probes=30)
            row = await probe(conn, clean=True, uptime_min=5 - 1 / 60)
        assert row["fired_count"] == 1

    @pytest.mark.asyncio
    async def test_a_fallback_in_the_same_process_still_restarts_it(self, pool):
        async with tx(pool) as conn:
            await seed(conn, since_min=170 * 60, last_min=15, probes=680)
            row = await probe(conn, clean=False, uptime_min=48 * 60)
        assert (row["fired_count"], row["since_s"]) == (0, 0)

    @pytest.mark.asyncio
    async def test_a_fallback_restarts_the_run_at_itself(self, pool):
        async with tx(pool) as conn:
            await seed(conn, since_min=170 * 60, last_min=15, probes=680)
            row = await probe(conn, clean=False)
        assert (row["fired_count"], row["since_s"], row["last_s"]) == (0, 0, 0)

    @pytest.mark.asyncio
    async def test_the_first_clean_probe_after_a_fallback_counts_from_the_fallback(
        self, pool,
    ):
        """Nothing was seen after the dirty probe, so the run it restarted is
        clean from that instant — and only from it."""
        async with tx(pool) as conn:
            await seed(conn, since_min=15, last_min=15, probes=0)
            row = await probe(conn, clean=True)
        assert (row["fired_count"], row["since_s"]) == (1, 15 * 60)


class TestNothingElseReadsIt:
    @pytest.mark.asyncio
    async def test_the_escalator_and_the_digest_never_see_the_watch(self, pool):
        """An event-kind row: the escalator's due query and the digest's
        tail ask for standing conditions, and a watch standing for weeks
        must never read as one."""
        from core.alert_escalator import _DUE_QUERY

        async with tx(pool) as conn:
            await seed(conn, since_min=30 * 24 * 60, last_min=5, probes=2880)
            due = await conn.fetch(_DUE_QUERY, 0.0)
            firing = await conn.fetch(
                "SELECT condition_key FROM app.alert_series "
                "WHERE kind = 'condition' AND state = 'firing'")
        assert KEY not in {r["condition_key"] for r in due}
        assert KEY not in {r["condition_key"] for r in firing}


class TestThroughTheArchive:
    @pytest.mark.asyncio
    async def test_record_watch_lands_the_row(self, pool, monkeypatch):
        """The fire-and-forget path, end to end, under a key of its own so a
        committed row cannot meet the rolled-back scenarios above."""
        key = f"watch:test-{uuid.uuid4().hex[:8]}"
        monkeypatch.setenv("KS_PG_DSN", DSN)
        monkeypatch.setattr(alert_archive, "_standing_down", False)
        try:
            with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
                for clean in (True, True, False):
                    await alert_archive.record_watch(key, clean=clean, gap_s=GAP,
                                                     web_uptime_s=3868)
            row = await pool.fetchrow(
                "SELECT kind, fired_count FROM app.alert_series WHERE condition_key = $1",
                key)
            assert dict(row) == {"kind": "event", "fired_count": 0}
        finally:
            await pool.execute("DELETE FROM app.alert_series WHERE condition_key = $1", key)
