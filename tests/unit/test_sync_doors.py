"""The sync levers under `/api/sync/`, and the `/api/duckdb/` names they keep.

Step 14's S14-17 (the owner's decision 6 for step 14, OD-S5-8) renamed the
four doors that start a sync from KeyCRM by hand — the resync, the status
refresh and the two buyer syncs — from `/api/duckdb/…`, a name from the days
they wrote DuckDB, to `/api/sync/…`. The old paths stay until step 14's final
stage as aliases: the same function behind both, left out of the schema, its
rate limit shared. Each slow one keeps its long timeout by both names, in the
app (`SLOW_ENDPOINTS`, 300 s, else a 504 at 30 s while the handler runs on)
and in nginx (`proxy_read_timeout 300`, else a 504 at 60 s).

`GET /api/duckdb/stats`, the fifth door under the old prefix, was deleted
outright: DuckDB's row counts, `off` since S14-00, read by nothing.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.routes_helper import iter_endpoints

REPO = Path(__file__).resolve().parents[2]
NGINX = REPO / "nginx" / "nginx.conf"

NEW = {
    "/api/sync/resync": "/api/duckdb/resync",
    "/api/sync/refresh-statuses": "/api/duckdb/refresh-statuses",
    "/api/sync/sync-buyers": "/api/duckdb/sync-buyers",
    "/api/sync/sync-all-buyers": "/api/duckdb/sync-all-buyers",
}
SLOW = {"/api/sync/resync", "/api/sync/refresh-statuses"}


def _posts() -> dict:
    from web.main import app

    return {e.path: e for e in iter_endpoints(app) if "POST" in e.methods}


def test_the_table_in_the_module_is_this_one():
    from web.routes.api.admin import SYNC_DOORS

    assert {f"/api{k}": f"/api{v}" for k, v in SYNC_DOORS.items()} == NEW


@pytest.mark.parametrize("new, old", sorted(NEW.items()))
def test_both_paths_are_one_function(new, old):
    """Mutation: give the alias a handler of its own, or drop it."""
    posts = _posts()
    assert new in posts and old in posts, (new, old)
    assert posts[new].route.endpoint is posts[old].route.endpoint


def test_only_the_new_names_are_in_the_schema():
    from fastapi.testclient import TestClient

    from web.main import app

    paths = TestClient(app).get("/openapi.json").json()["paths"]
    for new, old in NEW.items():
        assert new in paths and old not in paths, (new, old)
    assert not [p for p in paths if p.startswith("/api/duckdb/")]


def test_the_stats_route_is_gone():
    from web.main import app

    assert not [e for e in iter_endpoints(app) if e.path == "/api/duckdb/stats"]


def test_both_names_of_each_slow_lever_get_the_long_timeout():
    """Mutation: drop either name from `SLOW_ENDPOINTS`."""
    from web.middleware import SLOW_ENDPOINTS

    for new in SLOW:
        assert {new, NEW[new]} <= set(SLOW_ENDPOINTS), new
    for new in set(NEW) - SLOW:
        assert new not in SLOW_ENDPOINTS and NEW[new] not in SLOW_ENDPOINTS, new


def _long_timeout_locations() -> list:
    text = NGINX.read_text(encoding="utf-8")
    return [m.group(1) for m in re.finditer(
        r"location ~ (\S+) \{(?:(?!location).)*?proxy_read_timeout 300;", text, re.S)]


@pytest.mark.parametrize("path", sorted(SLOW | {"/api/duckdb/resync"}))
def test_nginx_gives_the_slow_levers_its_long_timeout(path):
    """The regex the plan names: `sync/(resync|refresh-statuses)` beside the
    old `duckdb/resync`, which keeps its timeout. Mutation: drop either."""
    assert any(re.match(rx, path) for rx in _long_timeout_locations()), path


def test_nginx_names_no_other_sync_door_slow():
    for path in ("/api/sync/sync-buyers", "/api/sync/sync-all-buyers"):
        assert not any(re.match(rx, path) for rx in _long_timeout_locations()), path


@pytest.mark.parametrize("new, old", sorted(NEW.items()))
def test_every_doors_limit_is_shared_by_both_its_names(new, old):
    """slowapi keys a plain `@limiter.limit` by the URL, so each name would get
    a counter of its own and the full buyer fetch (1/hour) could run twice an
    hour by alternating them. `shared_limit` with the new path as its scope is
    what makes it one lever — held here for all four doors, not only the
    resync the request test below exercises. Mutation: give any door a plain
    `@limiter.limit` back."""
    import inspect

    from web.ratelimit import limiter

    posts = _posts()
    func = inspect.unwrap(posts[new].route.endpoint)
    assert inspect.unwrap(posts[old].route.endpoint) is func, (new, old)
    limits = limiter._route_limits.get(f"{func.__module__}.{func.__name__}")
    assert limits, f"{new}: no rate limit registered"
    assert {limit.scope for limit in limits} == {new.removeprefix("/api/")}, (
        new, [(str(limit.limit), limit.scope) for limit in limits])


def test_an_alias_shares_its_rate_limit():
    """One limit for the lever, not one per name: the resync allows one a
    minute, and the second through the other name is refused."""
    import time
    from unittest.mock import AsyncMock, patch

    from fastapi.testclient import TestClient

    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.ratelimit import limiter
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    limiter.reset()
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data({
        "id": str(sorted(ADMIN_USER_IDS)[0]), "first_name": "T", "last_name": "U",
        "username": "t", "auth_date": str(int(time.time()))}, role="admin")))
    try:
        with patch("core.sync_service.force_resync", AsyncMock(return_value={"orders": 0})):
            first = client.post("/api/sync/resync", params={"days": 1})
            second = client.post("/api/duckdb/resync", params={"days": 1})
    finally:
        limiter.reset()
    assert first.status_code == 200, first.text
    assert second.status_code == 429, second.text
