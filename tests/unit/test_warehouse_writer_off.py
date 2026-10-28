"""Stage 5, PR-6: the warehouse writer's record under `KS_DUCKDB=off`.

The record lives in DuckDB's `sync_metadata`, in the file web may not open
for the week of silence. The owner's answer (OD-S5-2 (a), 2026-10-10), pinned
here:

- under `off` every settle reads the record from its frozen copy in Postgres,
  `app.sync_metadata`, and writes nothing — it only closes the `warehouse`
  group when a page of it is open, which asks no store;
- a door that re-parses every UTM verdict in Postgres alone notes it in a
  local marker beside the DuckDB file instead of in the record, and parses
  in Postgres as it always did;
- the first start back under `on` merges the marker into DuckDB's record,
  so a later way back from step 13 still re-parses DuckDB's verdicts;
- and under `on` with no marker, nothing a start does has changed.

Since step 14's S14-00 web runs `off` whatever `KS_DUCKDB` says and step 13
is a constant, so a start of web under `on`, and a process where DuckDB still
derives, no longer exist: their tests went with them. The marker is still
written under `off` — an image from before step 14, the rollback target,
merges it on a start under `on` — and what web does under `off` is still held here.

The design is `.planning/DUCKDB_EXIT_STAGE5_DECOUPLING.md`, sections 2.6 and
PR-6.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from core import duckdb_switch, warehouse_cutover as wc
from tests.unit.test_warehouse_writer import (  # noqa: F401 — fixtures
    _other_modes_put_back, _restart, _store, _unwrap, met, met_env, postgres,
)

RESOLVED = {"writer": "postgres", "since": "2026-10-01T06:00:00+00:00",
            "resolved": True, "resolved_at": "2026-10-01T06:00:01+00:00"}


class _FrozenCopy:
    """Postgres' `app.sync_metadata` as the frozen read meets it: one value,
    and every statement it was handed — so a test can say it was only read."""

    def __init__(self, value):
        self.value = value
        self.statements = []

    async def fetchval(self, sql, *args):
        self.statements.append((sql, args))
        return self.value

    async def execute(self, sql, *args):  # pragma: no cover — must not be reached
        raise AssertionError(f"the frozen copy was written: {sql}")


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn


@pytest.fixture
def frozen(duckdb_off, monkeypatch):
    """`KS_DUCKDB=off`, and Postgres answering the frozen read with the
    record step 13's first start resolved. Returns the copy, the pool getter
    and the directory the DuckDB file would be in."""
    copy = _FrozenCopy(json.dumps(RESOLVED))
    get_pool = AsyncMock(return_value=_Pool(copy))
    monkeypatch.setattr("core.pg.get_pool", get_pool)
    monkeypatch.setattr("core.pg.require_revision", AsyncMock())
    return SimpleNamespace(copy=copy, get_pool=get_pool, data=duckdb_off)


def _unconnected():
    from core.duckdb_store import DuckDBStore

    return DuckDBStore()


def _write_marker(stamp: str) -> None:
    wc.UTM_MARKER.parent.mkdir(parents=True, exist_ok=True)
    wc.UTM_MARKER.write_text(json.dumps({wc.UTM_REPARSED: stamp}), encoding="utf-8")


def _settle(store):
    return asyncio.run(wc.settle_writer(store))


# ─── The settle under off ────────────────────────────────────────────────────


class TestTheSettleUnderOff:
    def test_it_reads_the_frozen_copy_and_opens_nothing(self, postgres, frozen):
        """The proof the plan asks for: settled, the record published as the
        copy holds it, the copy asked one SELECT and nothing else, and no
        reach for the file — not even its directory. Mutation: read the
        record through the store under off, as `on` does."""
        _settle(_unconnected())
        assert wc._settled and wc.status()["writer"] == RESOLVED
        assert frozen.copy.statements == [(wc._FROZEN_WRITER_SQL, (wc.WRITER_KEY,))]
        assert duckdb_switch.opened() == {}
        assert not frozen.data.exists()

    def test_it_needs_no_store(self, postgres, frozen):
        """The scheduler hands it none under off."""
        _settle(None)
        assert wc._settled and wc.status()["writer"] == RESOLVED

    def test_it_closes_an_open_warehouse_page(self, postgres, frozen):
        """Nothing else would under off: the group's only resolver is the
        DuckDB tick, and a way back that never validated can have left a page
        open under a record that says resolved."""
        from core.alerting import _gate, delivered_conditions

        _gate.note_delivered_conditions(["warehouse:validation_retrying"], "warehouse")
        with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=True)), \
             patch("bot.main.send_admin_message", AsyncMock(return_value=1)) as send:
            _settle(_unconnected())
        (text,) = send.await_args.args[:1]
        assert "warehouse:validation_retrying" in text and wc.RESOLVE_NOTE in text
        assert "warehouse" not in delivered_conditions().values()
        assert wc._settled and duckdb_switch.opened() == {}

    def test_a_close_the_ledger_refused_is_asked_again(self, postgres, frozen):
        from core.alerting import _gate

        _gate.note_delivered_conditions(["warehouse:validation_retrying"], "warehouse")
        with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=False)):
            _settle(_unconnected())
        assert not wc._settled
        with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=True)), \
             patch("bot.main.send_admin_message", AsyncMock(return_value=1)):
            _settle(_unconnected())
        assert wc._settled

    def test_a_resolved_copy_with_no_page_open_announces_nothing(self, postgres, frozen):
        resolve = AsyncMock(return_value=0)
        with patch("core.alerting.resolve_group", resolve):
            _settle(_unconnected())
        resolve.assert_not_awaited()

    @pytest.mark.parametrize("value", [
        None,
        json.dumps({"writer": "postgres", "since": "2026-10-01T06:00:00+00:00",
                    "resolved": False}),
        "postgres",
    ], ids=["absent", "unresolved", "unreadable"])
    def test_a_copy_that_is_not_resolved_is_left_as_it_is(
        self, postgres, frozen, value, caplog,
    ):
        """What `off`'s verdict refuses to start over (`writer_record`); if a
        process gets here anyway, it closes the group as a first start under
        postgres would, says what it found, and writes nothing — the copy
        cannot be written, and a write to DuckDB is the one thing off forbids.
        Mutation: drop the `is_off()` branch from `settle_writer`."""
        frozen.copy.value = value
        resolve = AsyncMock(return_value=0)
        with patch("core.alerting.resolve_group", resolve):
            _settle(_unconnected())
        resolve.assert_awaited_once_with("warehouse", note=wc.RESOLVE_NOTE)
        assert wc._settled and "not postgres resolved" in caplog.text
        assert len(frozen.copy.statements) == 1
        assert duckdb_switch.opened() == {}

    def test_a_read_that_failed_is_asked_again_by_the_next_caller(self, postgres, frozen):
        frozen.get_pool.side_effect = OSError("connection refused")
        with pytest.raises(OSError):
            _settle(_unconnected())
        assert not wc._settled and duckdb_switch.opened() == {}
        frozen.get_pool.side_effect = None
        _settle(_unconnected())
        assert wc._settled


    def test_the_scheduler_hands_no_store_under_off(self, postgres, frozen, monkeypatch):
        """The scheduler's settle asks no store under off (PR-6). Mutation:
        hand it `get_store()` there."""
        from core.scheduler import BackgroundScheduler

        monkeypatch.setattr("core.duckdb_store.get_store",
                            AsyncMock(side_effect=AssertionError("get_store under off")))
        settle = AsyncMock()
        monkeypatch.setattr(wc, "settle_writer", settle)
        asyncio.run(BackgroundScheduler._settle_warehouse_writer())
        settle.assert_awaited_once_with(None)


# ─── The doors under off ─────────────────────────────────────────────────────


def _traffic_door(name, store, router):
    from web.routes.api import traffic

    with patch.object(traffic, "get_store", AsyncMock(return_value=store)), \
         patch.object(traffic, "reparse_router", router):
        return asyncio.run(_unwrap(getattr(traffic, name))(request=None, user={}))


class TestTheDoorsUnderOff:
    def test_the_reclassify_notes_the_marker_and_parses_in_postgres(
        self, postgres, frozen,
    ):
        """The plan's proof: `/api/traffic/reclassify` under off writes the
        marker and parses in Postgres, and nothing reaches the file or the
        frozen copy. Mutation: drop the `is_off()` branch from
        `note_utm_reparsed_in_postgres`."""
        store = _unconnected()
        router = AsyncMock(return_value={"rows": 7, "replaced": 6})
        result = _traffic_door("reclassify_traffic", store, router)
        assert result == {"success": True, "utm_records": 7, "engine": "postgres"}
        router.assert_awaited_once_with(store, full=True)
        assert wc.utm_marker_stamp() is not None
        assert duckdb_switch.opened() == {} and frozen.copy.statements == []
        assert not frozen.data.exists()

    def test_the_refresh_leaves_no_marker(self, postgres, frozen):
        """Only the doors that re-parse everything note it: an incremental
        parse changes no verdict a rule change left behind."""
        store = _unconnected()
        router = AsyncMock(return_value={"parsed": 2, "rows": 9})
        _traffic_door("refresh_traffic_data", store, router)
        assert wc.utm_marker_stamp() is None and duckdb_switch.opened() == {}

    def test_the_cli_notes_the_marker_and_parses_in_postgres(self, postgres, frozen):
        import scripts.backfill_utm as cli

        store = _unconnected()
        router = AsyncMock(return_value={"rows": 2, "replaced": 1})
        with patch("core.pg_utm_parse.reparse_router", router):
            asyncio.run(cli._reparse(store, force_ship=False))
        router.assert_awaited_once_with(store, full=True, force=False)
        assert wc.utm_marker_stamp() is not None and duckdb_switch.opened() == {}


    def test_a_second_note_keeps_the_later(self, postgres, frozen, monkeypatch):
        _write_marker("2026-10-12T09:00:00+00:00")
        monkeypatch.setattr(wc, "_now_iso", lambda: "2026-10-11T09:00:00+00:00")
        asyncio.run(wc.note_utm_reparsed_in_postgres(_unconnected()))
        assert wc.utm_marker_stamp() == "2026-10-12T09:00:00+00:00"
        monkeypatch.setattr(wc, "_now_iso", lambda: "2026-10-13T09:00:00+00:00")
        asyncio.run(wc.note_utm_reparsed_in_postgres(_unconnected()))
        assert wc.utm_marker_stamp() == "2026-10-13T09:00:00+00:00"

    def test_the_marker_is_written_whole_or_not_at_all(self, postgres, frozen):
        """Through the latch markers' writer — temp file, fsync, rename,
        fsync the directory — and nothing beside it is left behind."""
        from core import chain_latch

        with patch("core.chain_latch._write_marker",
                   wraps=chain_latch._write_marker) as write:
            asyncio.run(wc.note_utm_reparsed_in_postgres(_unconnected()))
        path, payload = write.call_args.args
        assert path == wc.UTM_MARKER
        assert set(json.loads(payload)) == {wc.UTM_REPARSED}
        assert sorted(p.name for p in wc.UTM_MARKER.parent.iterdir()
                      if p.name.startswith(f".{wc.UTM_MARKER.name}")) == []

    def test_off_publishes_the_frozen_record_with_the_note_over_it(
        self, postgres, frozen, monkeypatch,
    ):
        monkeypatch.setattr(wc, "_now_iso", lambda: "2026-10-12T09:00:00+00:00")
        _settle(_unconnected())
        asyncio.run(wc.note_utm_reparsed_in_postgres(_unconnected()))
        assert wc.status()["writer"] == {**RESOLVED,
                                         wc.UTM_REPARSED: "2026-10-12T09:00:00+00:00"}
        # And a later process reads the same, from the copy and the marker.
        _restart()
        wc.configure_mode()
        _settle(None)
        assert wc.status()["writer"][wc.UTM_REPARSED] == "2026-10-12T09:00:00+00:00"


class TestTheMarkerItself:
    def test_absent_is_none(self):
        assert wc.utm_marker_stamp() is None

    @pytest.mark.parametrize("content", ["", "not json", json.dumps({"x": 1}),
                                         json.dumps({wc.UTM_REPARSED: "yesterday"})])
    def test_a_stamp_that_cannot_be_read_is_dated_by_its_mtime(self, content, caplog):
        """The file existing is the note; its content only says when."""
        wc.UTM_MARKER.write_text(content, encoding="utf-8")
        mtime = datetime(2026, 10, 11, 8, 0, tzinfo=timezone.utc).timestamp()
        os.utime(wc.UTM_MARKER, (mtime, mtime))
        assert wc.utm_marker_stamp() == "2026-10-11T08:00:00+00:00"

    @pytest.mark.parametrize("a, b, later", [
        (None, "2026-10-11T08:00:00+00:00", "2026-10-11T08:00:00+00:00"),
        ("2026-10-11T08:00:00+00:00", None, "2026-10-11T08:00:00+00:00"),
        ("2026-10-11T08:00:00+00:00", "2026-10-11T10:00:00+02:00",
         "2026-10-11T08:00:00+00:00"),
        ("2026-10-11T08:00:00+00:00", "2026-10-11T08:00:01+00:00",
         "2026-10-11T08:00:01+00:00"),
        ("garbage", "2026-10-11T08:00:00+00:00", "2026-10-11T08:00:00+00:00"),
    ])
    def test_the_later_stamp_by_the_instant_not_the_text(self, a, b, later):
        assert wc._later(a, b) == later


# ─── Back under on ───────────────────────────────────────────────────────────


def _record_under_postgres(tmp_path, record=None):
    """A store whose record says what step 13's first start wrote."""
    from tests.unit.test_warehouse_writer import _seed

    store = _store(tmp_path)
    _seed(store, **{wc.WRITER_KEY: record or RESOLVED})
    return store


def _sync_metadata(store):
    async def read():
        async with store.connection() as conn:
            return conn.execute(
                "SELECT key, value, updated_at FROM sync_metadata ORDER BY key").fetchall()
    return asyncio.run(read())


# ─── In the week's state ─────────────────────────────────────────────────────

from tests.unit.test_duckdb_off_sweep import target_state  # noqa: E402,F401 — fixture
from tests.unit.test_read_fallback_sites import fresh_read_fallback  # noqa: E402,F401


class _KeyCRM:
    """KeyCRM, returning a comment for two orders."""

    async def paginate(self, *_a, **_k):
        yield [{"id": 1, "manager_comment": "utm_source=fb"},
               {"id": 2, "manager_comment": "utm_source=tg"}]


class TestInTheWeeksState:
    """The sweep's target state, derived from the registries: every chain
    latched, step 13 in force, every read on Postgres, `KS_DUCKDB=off` — the
    state the two doors that note a re-parse will meet during the week."""

    def test_the_backfill_cli_runs_without_the_file(self, target_state):  # noqa: F811
        """`scripts/backfill_utm.py` end to end under chain 3: the comments to
        Postgres, the re-parse to Postgres, the note to the marker, and
        nothing to the file — the store it is handed stays unconnected."""
        import scripts.backfill_utm as cli
        from core import pg_orders_read, pg_orders_write

        router = AsyncMock(return_value={"rows": 2, "replaced": 2})
        restored = AsyncMock(side_effect=lambda comments: sorted(comments))
        with patch("core.keycrm.get_async_client", AsyncMock(return_value=_KeyCRM())), \
             patch.object(pg_orders_read, "null_comment_count", AsyncMock(return_value=2)), \
             patch.object(pg_orders_write, "restore_manager_comments", restored), \
             patch("core.pg_utm_parse.reparse_router", router):
            asyncio.run(cli.backfill_utm(days_back=1))
        restored.assert_awaited()
        (store,), kwargs = router.await_args
        assert kwargs == {"full": True, "force": False}
        assert store._connection is None
        assert wc.utm_marker_stamp() is not None
        assert duckdb_switch.opened() == {}

    def test_the_reclassify_runs_without_the_file(self, target_state):  # noqa: F811
        from core.duckdb_store import get_store

        store = asyncio.run(get_store())
        router = AsyncMock(return_value={"rows": 4, "replaced": 4})
        result = _traffic_door("reclassify_traffic", store, router)
        assert result["engine"] == "postgres" and result["utm_records"] == 4
        assert wc.utm_marker_stamp() is not None
        assert store._connection is None and duckdb_switch.opened() == {}
