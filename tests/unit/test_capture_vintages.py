"""The forecast vintages' capture, on a connection that records what it is
asked (revision 0036; the server's half is
`tests/integration/test_capture_vintages_pg.py`).

`store_predictions` copies the rows it is about to delete into
`app.revenue_prediction_vintages` first. What is pinned here is the shape of
that: inside the writer's transaction and a savepoint of its own, after the
chain's advisory lock, before the DELETE and with the DELETE's own arguments;
and with the captures switched off, the writer's statements exactly as before.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core import analytics_captures as cap
from core import pg_forecast_write as fw

STAMP = datetime(2026, 11, 16, 0, 30, tzinfo=timezone.utc)
FORECAST = [{"date": date(2026, 11, 16) + timedelta(days=n), "predicted_revenue": 1000.0 + n}
            for n in range(5)]
METRICS = {"mae": 120.5, "mape": 31.2, "wape": 27.66}
DELETE = "DELETE FROM app.revenue_predictions"


class _Tx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        self._conn.depth += 1
        return self

    async def __aexit__(self, *exc):
        self._conn.depth -= 1
        return False


class _Conn:
    """Every statement, its arguments, and how many transactions deep it ran
    (1: the writer's; 2: a savepoint inside it)."""

    def __init__(self):
        self.depth = 0
        self.log = []

    def is_in_transaction(self):
        return self.depth > 0

    def transaction(self, **_kw):
        return _Tx(self)

    async def execute(self, sql, *args, timeout=None):
        # `timeout`: the capture's own bound (`CAPTURE_TIMEOUT_S`), asyncpg's
        # per-call argument.
        self.log.append((sql, args, self.depth))
        return "INSERT 0 3" if sql.lstrip().startswith("INSERT") else "OK"

    async def executemany(self, sql, rows):
        self.log.append((sql, tuple(rows), self.depth))


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False
        return _Ctx()


async def _store(monkeypatch, switch=None):
    if switch is None:
        monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    else:
        monkeypatch.setenv(cap.SWITCH_ENV, switch)
    conn = _Conn()
    with patch.object(fw, "_pool", AsyncMock(return_value=_Pool(conn))):
        assert await fw.store_predictions(FORECAST, "retail", METRICS, STAMP) == 5
    return conn.log


def _index(log, needle):
    (found,) = [i for i, (sql, _a, _d) in enumerate(log) if needle in sql]
    return found


class TestTheShapeOfTheCapture:
    def test_the_table_is_the_one_captures_names_this_module_for(self):
        """Kills: a constant renamed away from the spec — the helper refuses
        a table not in `CAPTURES` and the history is never written."""
        spec = cap.CAPTURES[fw.VINTAGES_TABLE]
        assert spec.writer == fw.__name__ and spec.kind == cap.IN_TRANSACTION
        assert fw._VINTAGES_CAPTURE_SQL.lstrip().startswith(
            f"INSERT INTO {fw.VINTAGES_TABLE} ")

    @pytest.mark.asyncio
    async def test_it_runs_in_a_savepoint_after_the_lock_and_before_the_delete(
            self, monkeypatch):
        """Kills: the copy after the DELETE (it would copy nothing), before
        the advisory lock (two trainings at once would both copy the same
        rows and lose the other's), outside the writer's transaction or
        without a savepoint, or over arguments other than the DELETE's."""
        log = await _store(monkeypatch)
        lock = _index(log, "pg_advisory_xact_lock")
        capture = _index(log, fw._VINTAGES_CAPTURE_SQL)
        delete = _index(log, DELETE)
        assert lock < capture < delete
        assert log[capture][1] == log[delete][1] == (
            "retail", date(2026, 11, 16), date(2026, 11, 20))
        assert log[delete][2] == 1, "the writer's own transaction"
        assert log[capture][2] == 2, "a savepoint inside it"
        # Its ok stamp, in the same savepoint.
        stamp = log[capture + 1]
        assert "meta.mirror_state" in stamp[0] and stamp[2] == 2
        assert stamp[1][0] == cap.state_row(fw.VINTAGES_TABLE)

    @pytest.mark.asyncio
    async def test_switched_off_the_writer_runs_exactly_as_before(self, monkeypatch):
        """Kills: a switch that skips more than the capture, or one that
        skips nothing."""
        on = await _store(monkeypatch)
        off = await _store(monkeypatch, "off")
        assert not [s for s, _a, _d in off
                    if fw.VINTAGES_TABLE in s or "meta.mirror_state" in s]
        capture = _index(on, fw._VINTAGES_CAPTURE_SQL)
        assert off == on[:capture] + on[capture + 2:]
