"""Every buyer writer mirrors, in portions, and the long one does not hold the
warehouse while it waits on KeyCRM (chain 4, PR-1, step 3).

Three shapes this replaces, each measured:

* the mirror was a line at ONE caller, so the admin `sync-all-buyers` wrote
  DuckDB alone and every buyer it changed stayed a daily CRITICAL in the
  comparison with nothing able to repair it;
* one transaction carried the whole batch, and twenty thousand buyers run DuckDB
  out of memory under the production limit (8 000 already do);
* `sync-all-buyers` would have held the heavy-job lock through ~460 sequential
  KeyCRM pages, stopping the minute sync for as long.
"""
from __future__ import annotations

import ast
import asyncio
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.duckdb_store import DuckDBStore
from core.models import Buyer

ROOT = Path(__file__).resolve().parents[2]


def _buyers(n, *, base=1, name="Покупець"):
    return [Buyer.from_api({"id": base + i, "full_name": f"{name} {i}",
                            "phone": [f"+3805{i:08d}"]}) for i in range(n)]


class TestTheMirrorHasOneCallSite:
    def test_only_upsert_buyers_calls_mirror_buyers(self):
        """Walks the tree rather than naming files: a second call site added
        anywhere is the regression, wherever it is added."""
        sites = set()
        for top in ("core", "web", "bot", "scripts", "deploy"):
            for path in (ROOT / top).rglob("*.py"):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                owner = {}
                for fn in ast.walk(tree):
                    if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        for node in ast.walk(fn):
                            owner.setdefault(id(node), fn.name)
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    f = node.func
                    name = f.id if isinstance(f, ast.Name) else getattr(f, "attr", None)
                    if name == "mirror_buyers":
                        rel = path.relative_to(ROOT).as_posix()
                        sites.add((rel, owner.get(id(node))))
        assert sites == {("core/duckdb_store.py", "upsert_buyers")}, sites


class TestPortions:
    def test_the_production_portion_is_a_thousand(self):
        # Below the measured failure point with room to spare: 6 500 commit,
        # 8 000 run out of memory under the production 4 GB limit.
        assert DuckDBStore.BUYER_WRITE_PORTION == 1000

    @pytest.mark.asyncio
    async def test_a_batch_is_split_into_portions_each_mirrored(self, tmp_path):
        """25 buyers at a portion of 10 is the same arithmetic as 2 500 at 1 000
        — three transactions, three mirrors, the last one short — without ten
        seconds of real writes in every test run."""
        store = DuckDBStore(db_path=tmp_path / "p.duckdb")
        await store.connect()
        store.BUYER_WRITE_PORTION = 10
        mirrored = []

        async def spy(portion):
            mirrored.append(len(portion))
            return {"rows": len(portion)}

        try:
            with patch("core.pg_buyers.mirror_buyers", new=spy), \
                 patch.object(store, "_upsert_buyer_portion",
                              wraps=store._upsert_buyer_portion) as portion:
                assert await store.upsert_buyers(_buyers(25)) == 25
            assert portion.await_count == 3
            assert mirrored == [10, 10, 5]
            async with store.connection() as conn:
                assert conn.execute("SELECT COUNT(*) FROM buyers").fetchone()[0] == 25
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_portion_is_mirrored_only_after_it_committed(self, tmp_path):
        store = DuckDBStore(db_path=tmp_path / "p.duckdb")
        await store.connect()
        seen = []

        async def spy(portion):
            # The store lock is free and the rows are there: committed first.
            async with store.connection() as conn:
                seen.append(conn.execute("SELECT COUNT(*) FROM buyers").fetchone()[0])
            return {}

        try:
            with patch("core.pg_buyers.mirror_buyers", new=spy):
                await store.upsert_buyers(_buyers(3))
            assert seen == [3]
        finally:
            await store.close()


class _FakeLock:
    """A heavy-job lock that is always taken: acquire waits forever."""

    def locked(self):
        return True

    async def acquire(self):
        await asyncio.sleep(3600)

    def release(self):  # pragma: no cover — never acquired
        raise AssertionError("released a lock it never held")


def _client():
    from fastapi.testclient import TestClient

    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    cookie = session_serializer.dumps(create_session_data({
        "id": str(sorted(ADMIN_USER_IDS)[0]), "first_name": "T", "last_name": "U",
        "username": "t", "auth_date": str(int(time.time()))}, role="admin"))
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, cookie)
    return client


@pytest.fixture
def fresh_limits():
    from web.routes.api._deps import limiter
    limiter.reset()
    yield
    limiter.reset()


class TestSyncAll:
    def _wire(self, monkeypatch, store, fetch):
        from core import keycrm
        from web.routes.api import admin

        client = MagicMock()
        client.fetch_all_buyers = fetch
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(keycrm, "KeyCRMClient", MagicMock(return_value=client))
        monkeypatch.setattr(admin, "get_store", AsyncMock(return_value=store))

    def test_keycrm_is_fetched_with_the_lock_free_and_every_portion_written_under_it(
            self, monkeypatch, tmp_path, fresh_limits):
        from core.scheduler import get_scheduler

        store = MagicMock()
        store.BUYER_WRITE_PORTION = 1000
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (0,)
        store.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
        store.connection.return_value.__aexit__ = AsyncMock(return_value=False)
        held_while_writing = []

        async def upsert(portion):
            held_while_writing.append(get_scheduler()._heavy_job_lock.locked())
            return len(portion)

        store.upsert_buyers = upsert

        async def fetch():
            assert not get_scheduler()._heavy_job_lock.locked(), \
                "the KeyCRM pages were fetched under the heavy-job lock"
            return _buyers(2100)

        self._wire(monkeypatch, store, fetch)
        r = _client().post("/api/duckdb/sync-all-buyers")

        assert r.status_code == 200, r.text
        assert r.json()["buyers_written"] == 2100
        assert held_while_writing == [True, True, True]
        assert not get_scheduler()._heavy_job_lock.locked()

    def test_a_busy_warehouse_is_a_409_not_a_hang(self, monkeypatch, tmp_path, fresh_limits):
        from core.scheduler import get_scheduler
        from web.routes.api import admin

        store = MagicMock()
        store.BUYER_WRITE_PORTION = 1000
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (0,)
        store.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
        store.connection.return_value.__aexit__ = AsyncMock(return_value=False)
        store.upsert_buyers = AsyncMock(side_effect=AssertionError("wrote without the lock"))

        self._wire(monkeypatch, store, AsyncMock(return_value=_buyers(10)))
        monkeypatch.setattr(admin, "HEAVY_LOCK_WAIT_S", 0.05)
        monkeypatch.setattr(get_scheduler(), "_heavy_job_lock", _FakeLock())

        r = _client().post("/api/duckdb/sync-all-buyers")

        assert r.status_code == 409, r.text
        assert "0 of 10" in r.json()["detail"]

    def test_the_manual_sync_waits_for_the_same_lock(self, monkeypatch, fresh_limits):
        from core.scheduler import get_scheduler
        from web.routes.api import admin

        monkeypatch.setattr(admin, "HEAVY_LOCK_WAIT_S", 0.05)
        monkeypatch.setattr(get_scheduler(), "_heavy_job_lock", _FakeLock())

        r = _client().post("/api/duckdb/sync-buyers")

        assert r.status_code == 409, r.text
