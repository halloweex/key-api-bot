"""The catalogue's history, without a server: the capture's registration and
the mass-delete guard's arithmetic.

The behaviour on a real Postgres — seeding, versions, tombstones, the guard,
the self-check, the switch, two captures at once — is
`tests/integration/test_capture_catalogue_pg.py`.
"""
from __future__ import annotations

import pytest

from core import analytics_captures as cap
from core import pg_catalogue_write as chain


class TestTheCaptureFollowsTheWrite:
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
