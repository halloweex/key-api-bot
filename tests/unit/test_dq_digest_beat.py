"""The digest remembers, in the warehouse, when it last reached a human.

`build_digest` decides silence from `last_sent_at`; this is the half that
supplies it. Kept on a real store rather than a mock, because the value has to
survive a round trip through `sync_metadata` — an ISO string written into a
VARCHAR and read back into an aware datetime — and a mock would agree with
whatever the code did.

The marker lives in the database and not on the scheduler object for one
reason: deploys. An in-memory beat resets on every release, and this project
releases often enough that "restated weekly" would have meant "restated
whenever we shipped".
"""
from __future__ import annotations

import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.data_quality import IntegrityIssue, Severity, persist_run
from core.duckdb_store import DuckDBStore
from core.scheduler import DQ_DIGEST_LAST_SENT_KEY, BackgroundScheduler


def _issue(count: int, severity=Severity.WARN):
    return IntegrityIssue(
        check_name="headline_vs_line_items", table_name="silver_orders",
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


async def _make_store(tmp_path: Path, monkeypatch) -> DuckDBStore:
    """A real warehouse, wired in where the job reaches for the global one."""
    store = DuckDBStore(db_path=tmp_path / "beat.duckdb")
    await store.connect()

    async def _get_store():
        return store

    monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
    return store


def _install_outbox(monkeypatch, *, fail: bool = False) -> _Outbox:
    box = _Outbox(fail=fail)
    module = types.ModuleType("bot.main")
    module.send_admin_message = box
    monkeypatch.setitem(sys.modules, "bot.main", module)
    return box


async def _seed_two_runs(store: DuckDBStore, first: int, second: int) -> None:
    """Two integrity runs so the digest has a previous one to diff against.

    Both dated within the hour: a stale layer is news in its own right and
    would send the digest for a reason these tests are not about. The clean
    reconciliation run is there for the same reason — a layer that never ran
    is news, and every digest here would go out on that alone.
    """
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        for minutes_ago, count in ((45, first), (5, second)):
            at = now - timedelta(minutes=minutes_ago)
            persist_run(
                conn, started_at=at, ended_at=at, as_of=at,
                window_start=date(2026, 1, 1), window_end=date(2026, 8, 15),
                layer="integrity", issues=[_issue(count)], discrepancies=[],
            )
        at = now - timedelta(minutes=30)
        persist_run(
            conn, started_at=at, ended_at=at, as_of=at,
            window_start=date(2026, 1, 1), window_end=date(2026, 8, 15),
            layer="reconciliation", issues=[], discrepancies=[],
        )


async def _set_marker(store: DuckDBStore, value: str) -> None:
    async with store.connection() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO sync_metadata (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, [DQ_DIGEST_LAST_SENT_KEY, value])


async def _marker(store: DuckDBStore):
    async with store.connection() as conn:
        row = conn.execute(
            "SELECT value FROM sync_metadata WHERE key = ?",
            [DQ_DIGEST_LAST_SENT_KEY],
        ).fetchone()
    return row[0] if row else None


@pytest.mark.asyncio
async def test_first_send_writes_a_parseable_marker(tmp_path, monkeypatch):
    store = await _make_store(tmp_path, monkeypatch)
    _install_outbox(monkeypatch)
    await _seed_two_runs(store, 413, 414)
    try:
        result = await BackgroundScheduler()._run_dq_digest()

        assert result["sent"] is True
        written = await _marker(store)
        assert written is not None
        parsed = datetime.fromisoformat(written)
        assert parsed.tzinfo is not None, "a naive marker would drift by the offset"
        assert abs((datetime.now(timezone.utc) - parsed).total_seconds()) < 60
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_the_second_morning_is_quiet(tmp_path, monkeypatch):
    """Same count, same day after: the reader already knows."""
    store = await _make_store(tmp_path, monkeypatch)
    outbox = _install_outbox(monkeypatch)
    await _seed_two_runs(store, 414, 414)
    try:
        first = await BackgroundScheduler()._run_dq_digest()
        assert first["sent"] is True
        marker_after_first = await _marker(store)

        second = await BackgroundScheduler()._run_dq_digest()

        assert second["sent"] is False
        assert second["quiet"] is True
        assert len(outbox.messages) == 1
        assert await _marker(store) == marker_after_first, \
            "a quiet day must not move the beat"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_week_of_silence_ends_in_a_restatement(tmp_path, monkeypatch):
    store = await _make_store(tmp_path, monkeypatch)
    outbox = _install_outbox(monkeypatch)
    await _seed_two_runs(store, 414, 414)
    await _set_marker(store, (datetime.now(timezone.utc) - timedelta(days=8)).isoformat())
    try:
        result = await BackgroundScheduler()._run_dq_digest()

        assert result["sent"] is True
        assert "Repeated weekly" in outbox.messages[0]
        assert "headline_vs_line_items: 414" in outbox.messages[0]
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_failed_send_does_not_start_the_week(tmp_path, monkeypatch):
    """Muting seven days on the strength of a message nobody received is how
    a standing WARN becomes a silent one."""
    store = await _make_store(tmp_path, monkeypatch)
    _install_outbox(monkeypatch, fail=True)
    await _seed_two_runs(store, 413, 414)
    try:
        result = await BackgroundScheduler()._run_dq_digest()

        assert result["sent"] is False
        assert result["quiet"] is False, "there was a message; it did not arrive"
        assert await _marker(store) is None
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_an_unreadable_marker_does_not_mute_the_digest(tmp_path, monkeypatch):
    store = await _make_store(tmp_path, monkeypatch)
    _install_outbox(monkeypatch)
    await _seed_two_runs(store, 414, 414)
    await _set_marker(store, "last tuesday")
    try:
        result = await BackgroundScheduler()._run_dq_digest()

        assert result["sent"] is True
        assert datetime.fromisoformat(await _marker(store))
    finally:
        await store.close()
