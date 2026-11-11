"""The digest remembers, in the store, when it last reached a human.

`build_digest` decides silence from `last_sent_at`; this is the half that
supplies it. Kept on a real store rather than a mock, because the value has to
survive a round trip — an ISO string written into `meta.chain_watermarks` and
read back into an aware datetime — and a mock would agree with whatever the
code did.

The marker lives in the database and not on the scheduler object for one
reason: deploys. An in-memory beat resets on every release, and this project
releases often enough that "restated weekly" would have meant "restated
whenever we shipped".

Since step 14's S14-10 the store is Postgres alone (chain 9): the journal in
`app.data_quality_*`, the beat in `meta.chain_watermarks`. Until then this
module routed the chain back to DuckDB and kept both in a DuckDB store. It
needs a live Postgres, which CI supplies.
"""
from __future__ import annotations

import os
import sys
import types
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import dq_journal, pg_dq_journal_write as chain
from core.data_quality import (
    WATCHED_LAYERS,
    IntegrityIssue,
    Severity,
)
from core.scheduler import DQ_DIGEST_LAST_SENT_KEY, BackgroundScheduler

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")


@pytest_asyncio.fixture
async def pg(monkeypatch):
    """The throwaway Postgres, its journal and beat cleared before and after,
    and `core.pg.get_pool` answering with it."""
    import asyncpg

    monkeypatch.setenv("KS_PG_DSN", DSN)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)

    async def clean():
        async with pool.acquire() as conn:
            for t in reversed(chain.CHAIN_TABLES):
                await conn.execute(f"DELETE FROM {t}")
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%' "
                               "OR key = $1", DQ_DIGEST_LAST_SENT_KEY)
            await conn.execute("DELETE FROM app.sync_metadata WHERE key = $1",
                               DQ_DIGEST_LAST_SENT_KEY)

    await clean()
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await clean()
    await pool.close()


def _issue(count: int, severity=Severity.WARN):
    return IntegrityIssue(
        check_name="pg_headline_vs_line_items", table_name="silver_orders",
        severity=severity, count=count, sample_ids=(1, 2),
        description=f"{count} order(s) have grand_total = 0",
    )


class _Outbox:
    """Stands in for bot.main.send_admin_message, which the job imports late."""

    def __init__(self, fail: bool = False):
        self.messages: list[str] = []
        self.fail = fail

    async def __call__(self, text, key=None, **kw):
        if self.fail:
            raise RuntimeError("telegram is down")
        self.messages.append(text)
        return 1


def _install_outbox(monkeypatch, *, fail: bool = False) -> _Outbox:
    box = _Outbox(fail=fail)
    module = types.ModuleType("bot.main")
    module.send_admin_message = box
    monkeypatch.setitem(sys.modules, "bot.main", module)
    return box


async def _seed_two_runs(first: int, second: int) -> None:
    """Two integrity runs so the digest has a previous one to diff against.

    Both dated within the hour: a stale layer is news in its own right and
    would send the digest for a reason these tests are not about. The clean
    run seeded for every *other* watched layer is there for the same reason —
    a layer that never ran is news, and every digest here would go out on that
    alone. Driven off WATCHED_LAYERS rather than a hand-written list, so
    adding a layer does not silently turn these tests into assertions about
    that layer instead of about the beat.
    """
    now = datetime.now(timezone.utc)
    for minutes_ago, count in ((45, first), (5, second)):
        at = now - timedelta(minutes=minutes_ago)
        await dq_journal.journal_run(
            started_at=at, ended_at=at, as_of=at,
            window_start=date(2026, 1, 1), window_end=date(2026, 8, 15),
            layer="integrity", issues=[_issue(count)], discrepancies=[],
        )
    at = now - timedelta(minutes=30)
    for layer in WATCHED_LAYERS:
        if layer == "integrity":
            continue
        await dq_journal.journal_run(
            started_at=at, ended_at=at, as_of=at,
            window_start=date(2026, 1, 1), window_end=date(2026, 8, 15),
            layer=layer, issues=[], discrepancies=[],
        )


async def _set_marker(pool, value: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
            "VALUES ($1, $2, now()) ON CONFLICT (key) DO UPDATE "
            "SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
            DQ_DIGEST_LAST_SENT_KEY, value)


async def _marker(pool):
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT value FROM meta.chain_watermarks WHERE key = $1",
            DQ_DIGEST_LAST_SENT_KEY)


@pytest.mark.asyncio
async def test_first_send_writes_a_parseable_marker(pg, monkeypatch):
    _install_outbox(monkeypatch)
    await _seed_two_runs(413, 414)
    result = await BackgroundScheduler()._run_dq_digest()

    assert result["sent"] is True
    written = await _marker(pg)
    assert written is not None
    parsed = datetime.fromisoformat(written)
    assert parsed.tzinfo is not None, "a naive marker would drift by the offset"
    assert abs((datetime.now(timezone.utc) - parsed).total_seconds()) < 60


@pytest.mark.asyncio
async def test_the_second_morning_is_quiet(pg, monkeypatch):
    """Same count, same day after: the reader already knows."""
    outbox = _install_outbox(monkeypatch)
    await _seed_two_runs(414, 414)
    first = await BackgroundScheduler()._run_dq_digest()
    assert first["sent"] is True
    marker_after_first = await _marker(pg)

    second = await BackgroundScheduler()._run_dq_digest()

    assert second["sent"] is False
    assert second["quiet"] is True
    assert len(outbox.messages) == 1
    assert await _marker(pg) == marker_after_first, \
        "a quiet day must not move the beat"


@pytest.mark.asyncio
async def test_a_week_of_silence_ends_in_a_restatement(pg, monkeypatch):
    outbox = _install_outbox(monkeypatch)
    await _seed_two_runs(414, 414)
    await _set_marker(pg, (datetime.now(timezone.utc) - timedelta(days=8)).isoformat())
    result = await BackgroundScheduler()._run_dq_digest()

    assert result["sent"] is True
    assert "weekly restatement" in outbox.messages[0]
    assert "headline_vs_line_items): 414" in outbox.messages[0]


@pytest.mark.asyncio
async def test_a_failed_send_does_not_start_the_week(pg, monkeypatch):
    """Muting seven days on the strength of a message nobody received is how
    a standing WARN becomes a silent one."""
    _install_outbox(monkeypatch, fail=True)
    await _seed_two_runs(413, 414)
    result = await BackgroundScheduler()._run_dq_digest()

    assert result["sent"] is False
    assert result["quiet"] is False, "there was a message; it did not arrive"
    assert await _marker(pg) is None


@pytest.mark.asyncio
async def test_an_unreadable_marker_does_not_mute_the_digest(pg, monkeypatch):
    _install_outbox(monkeypatch)
    await _seed_two_runs(414, 414)
    await _set_marker(pg, "last tuesday")
    result = await BackgroundScheduler()._run_dq_digest()

    assert result["sent"] is True
    assert datetime.fromisoformat(await _marker(pg))
