"""The catalogue's history, without a server: where the capture sits beside
chain 6's write, and the mass-delete guard's arithmetic.

The behaviour on a real Postgres — seeding, versions, tombstones, the guard,
the self-check, the switch, two captures at once — is
`tests/integration/test_capture_catalogue_pg.py`.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from core import analytics_captures as cap
from core import pg_catalogue_write as chain
from core.landing_rows import product_rows
from tests.unit import test_chain_writers_sql_frozen as frozen


class _Conn(frozen._Recorder):
    """The frozen test's recording connection, answering the capture's reads
    as an empty history over an empty catalogue would."""

    def is_in_transaction(self):
        return False

    async def fetchval(self, sql, *args, **kw):
        value = await super().fetchval(sql, *args, **kw)
        return 0 if sql == chain._VERSIONS_LIVE_SQL else value

    async def fetchrow(self, sql, *args, **kw):
        await super().fetchrow(sql, *args, **kw)
        return {"differ": 0, "orphaned": 0, "mis_hashed": 0}


class _Pool:
    """One connection, and when each acquire happened and how many were held."""

    def __init__(self, conn):
        self.conn, self.held, self.acquired = conn, 0, []

    def acquire(self, timeout=None):
        pool = self

        @asynccontextmanager
        async def held():
            pool.acquired.append((len(pool.conn.calls), pool.held))
            pool.held += 1
            try:
                yield pool.conn
            finally:
                pool.held -= 1

        return held()


# The frozen test's products scenario, the same rows.
PRODUCTS = [{"id": 1, "name": "A", "category_id": 2, "sku": "S1", "price": 500,
             "custom_fields": []},
            {"id": 2, "name": "B", "category_id": 2, "sku": "S2", "price": 300,
             "custom_fields": []}]


async def _record(monkeypatch, *, switch=None):
    if switch is None:
        monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    else:
        monkeypatch.setenv(cap.SWITCH_ENV, switch)
    pool = _Pool(_Conn())
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
            patch("core.pg.require_revision", new=AsyncMock()):
        assert await chain.upsert_products(product_rows(PRODUCTS)) == 2
    return pool


def _golden():
    return json.loads(frozen.GOLDEN.read_text(encoding="utf-8"))["scenarios"]["products"]


class TestTheCaptureFollowsTheWrite:
    """Rule 12 of step 14 holds chain 6's statements byte for byte
    (`test_chain_writers_sql_frozen.py`). The capture is not one of them: it
    is a second unit of work, after the write's transaction has committed and
    its connection has gone back."""

    @pytest.mark.asyncio
    async def test_the_write_sends_what_it_sent_and_then_the_capture_runs_apart(
            self, monkeypatch):
        pool = await _record(monkeypatch)
        calls = pool.conn.calls

        (_, first), (at, held) = pool.acquired
        assert first == 0 and held == 0, (
            "the capture took its connection while the write still held one")
        assert calls[:at] == _golden()
        capture = calls[at:]
        assert capture[0] == ["begin"] and capture[-1] == ["end", True]
        sent = [c[1] for c in capture if len(c) == 3]
        assert sent[:3] == [chain._VERSIONS_ISOLATION_SQL,
                            f"SET LOCAL statement_timeout = '{chain.STATEMENT_TIMEOUT}'",
                            chain._VERSIONS_LOCK_SQL]
        assert chain._VERSIONS_SELF_CHECK_SQL in sent
        # Nothing owed over an empty catalogue: no insert, and the stamp.
        assert chain._VERSION_INSERT_SQL not in sent
        from core.pg_landing import WATERMARK_OK_SQL
        assert sent[-1] == WATERMARK_OK_SQL

    @pytest.mark.asyncio
    async def test_switched_off_the_write_is_all_there_is(self, monkeypatch):
        pool = await _record(monkeypatch, switch="off")
        assert pool.conn.calls == _golden()
        assert len(pool.acquired) == 1

    def test_it_is_the_capture_this_module_is_named_for(self):
        spec = cap.CAPTURES[chain.VERSIONS]
        assert spec.writer == chain.__name__ and spec.kind == cap.OWN_TRANSACTION
        assert spec.self_checked


class _GuardConn:
    """A capture's connection over `live` products held live, of which the
    diff says `tombstoned` turn into tombstones."""

    def __init__(self, live, tombstoned):
        self.live = live
        self.owed = [{"product_id": i, "name": f"P{i}", "brand": None,
                      "category_id": None, "row_hash": i, "is_deleted": True,
                      "tombstoned": True} for i in range(1, tombstoned + 1)]
        self.inserted = None

    async def execute(self, sql, *args):
        return "SET"

    async def fetchval(self, sql, *args):
        if sql == chain._VERSIONS_LIVE_SQL:
            return self.live
        return None

    async def fetch(self, sql, *args):
        return self.owed

    async def executemany(self, sql, rows):
        self.inserted = list(rows)

    async def fetchrow(self, sql, *args):
        return {"differ": 0, "orphaned": 0, "mis_hashed": 0}


class TestTheGuardsArithmetic:
    """max(50, 1 %) of the products held live, "more than" refused. The
    integration test holds the 50; this holds the 1 %, which needs a
    catalogue over 5 000."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("live,tombstoned,refused", [
        (120, 50, False), (120, 51, True),
        (10_000, 100, False), (10_000, 101, True),
    ])
    async def test_the_limit(self, live, tombstoned, refused):
        conn = _GuardConn(live, tombstoned)
        if refused:
            with pytest.raises(cap.CaptureRefused) as caught:
                await chain._capture_versions(conn)
            assert (caught.value.facts["tombstoned"], caught.value.facts["live"]) == (
                tombstoned, live)
            assert conn.inserted is None
        else:
            assert await chain._capture_versions(conn) == tombstoned
            assert len(conn.inserted) == tombstoned

    @pytest.mark.asyncio
    async def test_the_lever_lifts_it(self):
        conn = _GuardConn(120, 120)
        assert await chain._capture_versions(conn, guard=False) == 120
