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

Since step 14's S14-00 chain 4 writes the buyers, a constant, so DuckDB's
portions and their mirroring are no path production takes: those tests, and
the refusal of a flag nobody can read, went with it.
"""
from __future__ import annotations

import ast
import asyncio
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

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


def _store_for_sync_all(upsert):
    store = MagicMock()
    store.BUYER_WRITE_PORTION = 1000
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (0,)
    store.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    store.connection.return_value.__aexit__ = AsyncMock(return_value=False)
    store.upsert_buyers = upsert
    return store


def _keycrm(monkeypatch, fetch):
    from core import keycrm

    client = MagicMock()
    client.fetch_all_buyers = fetch
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(keycrm, "KeyCRMClient", MagicMock(return_value=client))


class TestUnderChain4:
    """The buyers land in Postgres alone, so sync-all counts them there, and a
    flag nobody can read is refused at both doors before anything starts."""

    READERS = ("KS_SMS_STORE", "KS_READ_SEARCH_INDEX", "KS_READ_DASHBOARD")

    @pytest.mark.asyncio
    async def test_sync_all_counts_the_buyers_where_they_are_written(self, monkeypatch):
        from core import pg_buyer_sync_read
        from web.routes.api import admin

        monkeypatch.setenv("KS_WRITE_BUYERS", "postgres")
        for reader in self.READERS:
            monkeypatch.setenv(reader, "postgres")
        counts = iter([100, 103])
        monkeypatch.setattr(pg_buyer_sync_read, "count_buyers",
                            AsyncMock(side_effect=lambda: next(counts)))
        _keycrm(monkeypatch, AsyncMock(return_value=_buyers(3)))
        store = _store_for_sync_all(AsyncMock(return_value=3))
        store.connection.side_effect = AssertionError("DuckDB counted under chain 4")

        result = await admin._sync_all_buyers(store)

        assert (result["before_count"], result["after_count"],
                result["new_buyers"]) == (100, 103, 3)


class TestTheRoutes:
    def test_sync_all_answers_at_once_and_runs_detached(self, monkeypatch, fresh_limits):
        from web.routes.api import admin

        body = AsyncMock(return_value={"status": "done"})
        monkeypatch.setattr(admin, "_sync_all_buyers", body)
        monkeypatch.setattr(admin, "get_store", AsyncMock(return_value=MagicMock()))

        r = _client().post("/api/duckdb/sync-all-buyers")

        assert r.status_code == 200, r.text
        assert r.json()["status"] == "started"

    def test_a_second_full_sync_while_one_runs_is_refused(self, monkeypatch, fresh_limits):
        from web.routes.api import admin

        running = MagicMock()
        running.get_name.return_value = "sync_all_buyers"
        running.done.return_value = False
        monkeypatch.setattr(admin, "_BACKGROUND_TASKS", {running})

        r = _client().post("/api/duckdb/sync-all-buyers")

        assert r.status_code == 409, r.text

    def test_the_manual_sync_waits_for_the_same_lock(self, monkeypatch, fresh_limits):
        from core.scheduler import get_scheduler
        from web.routes.api import admin

        monkeypatch.setattr(admin, "HEAVY_LOCK_WAIT_S", 0.05)
        monkeypatch.setattr(get_scheduler(), "_heavy_job_lock", _FakeLock())

        r = _client().post("/api/duckdb/sync-buyers")

        assert r.status_code == 409, r.text

    def test_the_manual_syncs_409_fits_inside_the_request_budget(self):
        """At 60 s the app's own 30 s RequestTimeoutMiddleware answered 504
        first, and the handler kept writing behind that answer."""
        from web import middleware
        from web.routes.api import admin

        assert "/api/duckdb/sync-buyers" not in middleware.SLOW_ENDPOINTS
        assert admin.HEAVY_LOCK_WAIT_S < middleware.DEFAULT_REQUEST_TIMEOUT
