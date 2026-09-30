"""The DuckDB-only doors the owner retired under OD-10 (2026-09-30) stay retired.

Step 13 stops DuckDB deriving Silver, Gold and the UTM verdicts. Every door
that read those tables with no read switch would, after the flip, answer from
a Silver as old as the switch and after the first Sunday compaction from none —
so each was ported or retired (`core.warehouse_cutover.OD10_DOORS`, which the
walk in `tests/unit/test_warehouse_writer.py` keeps equal to what is left).
The owner retired all of them, in one change. This file is what keeps them
retired: a route that comes back under the same path, a tool the model is
offered again, fails here — and the walk would put the door back on the list
the switch waits for.
"""
from __future__ import annotations

import ast
import asyncio
import pathlib
import re

import pytest

from tests.routes_helper import iter_endpoints

# (method, path) of every retired HTTP door.
RETIRED_ROUTES = (
    # Never answered anything but 404 for a real id: `dict()` of a DuckDB
    # tuple since 2026-02-06 (and `op.sku`, a column order_products lacks).
    ("GET", "/api/buyers/{buyer_id}"),
    ("GET", "/api/orders/{order_id}"),
    ("GET", "/api/products/{product_id}"),
    # DuckDB Bronze against DuckDB Silver, with no caller; the KeyCRM half of
    # the second raised on every stored order since the day it was written.
    ("GET", "/api/debug/stale-returns"),
    ("GET", "/api/debug/order-status/{order_id}"),
    # DuckDB buyer counters with no caller; "all synced" after a compaction.
    ("GET", "/api/buyers/stats"),
    # Deleted orders from DuckDB alone, for the April 2026 MVCC incident;
    # never reached the Postgres every tab reads.
    ("POST", "/api/duckdb/purge-orders"),
    # The legacy 06:00 comparator's two doors; dq_reconciliation is the check.
    ("GET", "/api/reconciliation"),
    ("POST", "/api/reconciliation/run"),
)

# The assistant's tools that rode the retired detail service.
RETIRED_TOOLS = ("get_buyer_details", "get_order_details")


@pytest.fixture(scope="module")
def registered():
    from web.main import app

    return {(method, endpoint.path)
            for endpoint in iter_endpoints(app)
            for method in (endpoint.methods or ())}


@pytest.mark.parametrize("method, path", RETIRED_ROUTES)
def test_the_route_is_not_registered(registered, method, path):
    assert (method, path) not in registered, f"{method} {path} is back"


def test_the_walk_sees_routes_at_all(registered):
    """A helper that found nothing would pass every assertion above."""
    assert ("GET", "/api/search/buyers") in registered
    assert ("GET", "/api/health") in registered


@pytest.fixture
def admin_client(monkeypatch):
    """An admin session, resolved without a store. No lifespan runs."""
    import time

    from fastapi.testclient import TestClient

    from web.main import app
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    async def _resolve(session):
        return {"user_id": 555_000_777, "role": "admin"}

    monkeypatch.setattr("web.routes.auth._resolve_session", _resolve)
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
        {"id": "555000777", "first_name": "T", "last_name": "U", "username": "t",
         "auth_date": str(int(time.time()))}, role="admin")))
    return client


def _concrete(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "42", path)


@pytest.mark.parametrize("method, path", RETIRED_ROUTES)
def test_a_retired_door_answers_as_one(admin_client, method, path):
    """Not 200 and the SPA shell. The pages router's catch-all owns every
    unmatched GET, and served an admin the shell with a 200 for each retired
    GET, so a saved curl or an operator script read "retired" as "worked". A
    GET is a JSON 404 now. A POST was always a JSON 405, because the
    catch-all takes GET alone."""
    response = admin_client.request(method, _concrete(path), follow_redirects=False)
    assert response.status_code == (404 if method == "GET" else 405), response.text[:80]
    assert response.headers["content-type"].startswith("application/json")
    assert "<html" not in response.text.lower()


def test_the_session_is_real_and_the_shell_still_serves_pages(admin_client):
    """The two halves the assertion above leans on: the session gets the SPA
    on a page path, and a live API route still answers as itself."""
    page = admin_client.get("/reports", follow_redirects=False)
    assert page.status_code == 200 and page.headers["content-type"].startswith("text/html")
    assert admin_client.get("/api/health").headers["content-type"].startswith(
        "application/json")


@pytest.mark.parametrize("path", ["/api", "/api/", "/api/no-such-door",
                                  "/api/admin/retired/thing.json"])
def test_no_unmatched_api_get_gets_the_shell_even_without_a_session(path):
    """Answered before the session check: a 404 says only that no such
    route exists, which `/openapi.json` says to anyone already. Before, an
    anonymous caller was redirected to /login and an admin got the shell."""
    from fastapi.testclient import TestClient

    from web.main import app

    response = TestClient(app).get(path, follow_redirects=False)
    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}


@pytest.mark.parametrize("name", RETIRED_TOOLS)
def test_the_assistant_is_not_offered_the_tool(name):
    from core.chat_tools import TOOLS

    assert name not in {tool["name"] for tool in TOOLS}


@pytest.mark.parametrize("name", RETIRED_TOOLS)
def test_the_assistant_cannot_call_it_either(name):
    """A model that remembers a tool from an older conversation is told it
    does not exist, not answered from a service that is gone."""
    from core.chat_tools import execute_tool

    assert asyncio.run(execute_tool(name, {"buyer_id": 1, "order_id": 1})) == {
        "error": f"Unknown tool: {name}"}


def test_the_search_tools_stay():
    """OD-10 retired the detail tools; the Meilisearch searches are not doors."""
    from core.chat_tools import TOOLS

    names = {tool["name"] for tool in TOOLS}
    assert {"search_buyer", "search_order", "search_product"} <= names


def test_the_search_service_holds_no_detail_readers():
    from web.services.search_service import SearchService

    for name in ("get_buyer_details", "get_order_details", "get_product_details"):
        assert not hasattr(SearchService, name), name


# ─── The legacy 06:00 reconciliation ─────────────────────────────────────────

def test_the_legacy_job_is_not_registered():
    """`reconciliation_check` counted DuckDB's orders per day against KeyCRM
    over 14 days and resynced what drifted; `dq_reconciliation` compares 90
    days per order against all three stores from one fetch."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from core.scheduler import SCHEDULER_TIMEZONE, BackgroundScheduler

    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    asyncio.run(s._register_jobs())
    ids = {job.id for job in s._scheduler.get_jobs()}
    assert len(ids) > 20, "the registry looks empty — has it moved?"
    assert "reconciliation_check" not in ids
    assert "dq_reconciliation" in ids


def test_the_legacy_comparator_is_gone():
    from core.duckdb_store import DuckDBStore
    from core.scheduler import BackgroundScheduler
    from core.sync_service import SyncService

    assert not hasattr(SyncService, "reconcile_with_api")
    assert not hasattr(BackgroundScheduler, "_run_reconciliation")
    for name in ("get_order_summaries_by_date", "log_reconciliation"):
        assert not hasattr(DuckDBStore, name), name


REPO = pathlib.Path(__file__).resolve().parents[2]
# DuckDB's table by its bare name. `app.reconciliation_log` is its copy in
# Postgres, written by the hourly replication of what DuckDB holds.
_WRITES_THE_LOG = re.compile(
    r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+(?<![.\w])reconciliation_log\b",
    re.IGNORECASE)


def test_nothing_writes_reconciliation_log_any_more():
    """The legacy job and POST /api/reconciliation/run were its two writers,
    through `log_reconciliation`. With both retired the table is history —
    OD-13's freeze — kept in DuckDB and in `app.reconciliation_log`, whose
    hourly copy and daily comparison go on over a table that no longer moves.
    A writer that comes back fails here. Read from every string constant, so
    a docstring or a comment naming the table does not count."""
    found = []
    for root in ("core", "web", "bot", "scripts", "deploy"):
        for path in sorted((REPO / root).rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                        and _WRITES_THE_LOG.search(node.value)):
                    found.append(f"{path.relative_to(REPO)}:{node.lineno}")
    assert found == [], f"something writes DuckDB's reconciliation_log again: {found}"


@pytest.mark.parametrize("sql, writes", [
    ("INSERT INTO reconciliation_log (check_date) VALUES (?)", True),
    ("insert into reconciliation_log values (1)", True),
    ("DELETE FROM reconciliation_log WHERE id = ?", True),
    ("INSERT INTO app.reconciliation_log (id) VALUES ($1)", False),
    ("SELECT * FROM reconciliation_log", False),
])
def test_the_pattern_reads_each_shape(sql, writes):
    assert bool(_WRITES_THE_LOG.search(sql)) is writes


def test_the_sqlite_to_duckdb_user_copy_is_gone():
    """Its source froze on 2026-08-27, its target has not been read in
    production since 2026-09-07, and it wrote roles from two hardcoded ids."""
    assert not (REPO / "scripts" / "migrate_sqlite_to_duckdb.py").exists()
