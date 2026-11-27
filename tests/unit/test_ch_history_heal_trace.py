"""The archive's heal, durably (`core.ch_history.HEAL_ROW`).

A heal — the hourly ids-diff re-shipping versions below the copy's own
high-water mark — used to be known only to the process that made it, in
`ch_history.last_heal`. The daily comparison and the alerts' machine note
read that memory, so a restart lost it, and a heal made by another process
— the Rust worker, once `executor:ch_sync` names it (the Rust plan, К0 and
К1) — would never be seen at all. Whoever heals now also stamps a row of
`meta.mirror_state`, and the daily comparison takes it back into memory.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest

from core import ch_history

NOW = datetime.now(timezone.utc)


class _Conn:
    """Postgres as `ship_history` and `reconcile_ch_history` ask it."""

    def __init__(self, pg_ids: List[int], heal: Optional[Dict[str, Any]] = None):
        self.pg_ids, self.heal = pg_ids, heal

    async def fetch(self, sql, *args):
        if "ANY($1" in sql:
            return [(i,) for i in args[0]]
        if "id / 10000" in sql:
            ceiling = args[0]
            out: Dict[int, int] = {}
            for i in self.pg_ids:
                if i <= ceiling:
                    out[i // 10000] = out.get(i // 10000, 0) + 1
            return list(out.items())
        return [(i,) for i in self.pg_ids]

    async def fetchrow(self, sql, arg):
        if arg == ch_history.HEAL_ROW:
            return self.heal
        if "order_versions" in sql:
            return {"n": 0, "oldest": None}
        return {"last_ok_at": NOW, "failures_since_ok": 0}


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        conn = self.conn

        class Acq:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False
        return Acq()


@pytest.fixture
def archive(monkeypatch):
    """ClickHouse holding `held` below `ceiling`, Postgres holding the ids a
    test gives, and every `mark_ok` recorded."""
    marks: List[tuple] = []
    state: Dict[str, Any] = {"held": [], "ceiling": 0, "fail_heal": False}

    async def execute(sql, body=None):
        if sql.startswith("SELECT id FROM"):
            return "".join(f"{i}\n" for i in state["held"])
        if "intDiv(id, 10000)" in sql:
            out: Dict[int, int] = {}
            for i in state["held"]:
                out[i // 10000] = out.get(i // 10000, 0) + 1
            return "".join(f"{b}\t{n}\n" for b, n in out.items())
        return ""

    async def mark_ok(row, rows):
        if row == ch_history.HEAL_ROW and state["fail_heal"]:
            raise RuntimeError("Postgres went away")
        marks.append((row, rows))

    def ceiling(*, ensure=False):
        return state["ceiling"]

    monkeypatch.setattr(ch_history, "configured", lambda: True)
    monkeypatch.setattr(ch_history, "_execute", execute)
    monkeypatch.setattr(ch_history, "_mark_ok", mark_ok)
    monkeypatch.setattr(ch_history, "render_tsv", lambda rows: b"")
    monkeypatch.setattr(ch_history, "last_heal", {})
    monkeypatch.setattr(ch_history, "_ch_max_id", AsyncMock(side_effect=ceiling))
    return state, marks


def _pool(monkeypatch, conn):
    monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=_Pool(conn)))


@pytest.mark.asyncio
async def test_a_heal_is_stamped_beside_the_ship(monkeypatch, archive):
    state, marks = archive
    state.update(held=[i for i in range(1, 11) if i not in (3, 7)], ceiling=10)
    _pool(monkeypatch, _Conn(list(range(1, 13))))

    assert await ch_history.ship_history() == {"appended": 4}
    assert marks == [(ch_history.STATE_ROW, 4), (ch_history.HEAL_ROW, 2)]
    assert ch_history.last_heal["shipped"] == 2


@pytest.mark.asyncio
async def test_an_append_is_not_a_heal(monkeypatch, archive):
    state, marks = archive
    state.update(held=list(range(1, 11)), ceiling=10)
    _pool(monkeypatch, _Conn(list(range(1, 13))))

    assert await ch_history.ship_history() == {"appended": 2}
    assert marks == [(ch_history.STATE_ROW, 2)]
    assert ch_history.last_heal == {}


@pytest.mark.asyncio
async def test_a_trace_that_cannot_be_written_costs_the_trace_not_the_ship(
        monkeypatch, archive, caplog):
    state, marks = archive
    state.update(held=[1, 2, 4], ceiling=4, fail_heal=True)
    _pool(monkeypatch, _Conn([1, 2, 3, 4]))

    assert await ch_history.ship_history() == {"appended": 1}
    assert marks == [(ch_history.STATE_ROW, 1)]
    assert ch_history.last_heal["shipped"] == 1
    assert "could not record the heal" in caplog.text


@pytest.mark.asyncio
async def test_the_daily_comparison_sees_a_heal_another_process_made(monkeypatch, archive):
    """This process healed nothing; the durable row says another did, an hour
    ago. The comparison reports it, and the alerts' note can say so."""
    from core.data_quality import machine_attempts_note

    state, _ = archive
    state.update(held=list(range(1, 11)), ceiling=10)
    heal = {"last_ok_at": NOW - timedelta(hours=1), "last_rows": 39}
    _pool(monkeypatch, _Conn(list(range(1, 11)), heal=heal))

    issues = await ch_history.reconcile_ch_history()
    [found] = [i for i in issues if i.check_name == "ch_history_selfhealed"]
    assert found.count == 39
    assert ch_history.last_heal == {"at": heal["last_ok_at"], "shipped": 39}
    note = machine_attempts_note(now=NOW)
    assert note is not None and "auto re-shipped 39 rows" in note


@pytest.mark.asyncio
async def test_a_heal_older_than_a_day_is_not_news(monkeypatch, archive):
    state, _ = archive
    state.update(held=list(range(1, 11)), ceiling=10)
    _pool(monkeypatch, _Conn(list(range(1, 11)),
                             heal={"last_ok_at": NOW - timedelta(hours=30), "last_rows": 5}))

    issues = await ch_history.reconcile_ch_history()
    assert not [i for i in issues if i.check_name == "ch_history_selfhealed"]


def test_an_older_row_never_overwrites_what_this_process_knows(monkeypatch):
    mine = {"at": NOW, "shipped": 7}
    monkeypatch.setattr(ch_history, "last_heal", dict(mine))
    ch_history._refresh_heal({"last_ok_at": NOW - timedelta(minutes=5), "last_rows": 99})
    assert ch_history.last_heal == mine
    ch_history._refresh_heal({"last_ok_at": NOW + timedelta(minutes=5), "last_rows": 3})
    assert ch_history.last_heal == {"at": NOW + timedelta(minutes=5), "shipped": 3}


def test_no_row_and_a_row_never_stamped_change_nothing(monkeypatch):
    monkeypatch.setattr(ch_history, "last_heal", {})
    ch_history._refresh_heal(None)
    ch_history._refresh_heal({"last_ok_at": None, "last_rows": None})
    assert ch_history.last_heal == {}


def test_the_row_is_the_archives_own():
    assert ch_history.HEAL_ROW == "clickhouse.order_versions.heal"
    assert ch_history.HEAL_ROW.startswith(ch_history.STATE_ROW)
