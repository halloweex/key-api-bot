"""The sync levers under `/api/sync/`, and the `/api/duckdb/` names they lost.

Step 14's S14-17 (the owner's decision 6 for step 14, OD-S5-8) renamed the
four doors that start a sync from KeyCRM by hand — the resync, the status
refresh and the two buyer syncs — from `/api/duckdb/…`, a name from the days
they wrote DuckDB, to `/api/sync/…`. The old paths stayed until step 14's
final stage as aliases, the same function behind both, out of the schema.
The final stage removed them (2026-11-12), with their two entries in
`SLOW_ENDPOINTS` and `duckdb/resync` in nginx's long-timeout regex. Each slow
lever keeps its long timeout by its one name, in the app (`SLOW_ENDPOINTS`,
300 s, else a 504 at 30 s while the handler runs on) and in nginx
(`proxy_read_timeout 300`, else a 504 at 60 s).

What an old name answers now is what every path no route serves answers: a
POST is a JSON 405, because the pages router's catch-all takes GET alone, and
a GET is the JSON 404 every unmatched `/api/` GET gets — never the SPA shell,
and never the lever.

`GET /api/duckdb/stats`, the fifth door under the old prefix, was deleted
outright in S14-17: DuckDB's row counts, `off` since S14-00, read by nothing.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.routes_helper import aliases, iter_endpoints
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture

REPO = Path(__file__).resolve().parents[2]
NGINX = REPO / "nginx" / "nginx.conf"

DOORS = ("/api/sync/resync", "/api/sync/refresh-statuses",
         "/api/sync/sync-buyers", "/api/sync/sync-all-buyers")
# The names the final stage removed, each beside the door it reached.
RETIRED = {
    "/api/duckdb/resync": "/api/sync/resync",
    "/api/duckdb/refresh-statuses": "/api/sync/refresh-statuses",
    "/api/duckdb/sync-buyers": "/api/sync/sync-buyers",
    "/api/duckdb/sync-all-buyers": "/api/sync/sync-all-buyers",
}
SLOW = {"/api/sync/resync", "/api/sync/refresh-statuses"}
# What each lever's handler calls first, patched so that a request reaching
# one would be seen and would do nothing — the full buyer fetch above all,
# which is KeyCRM's ~460 pages.
_HANDLERS = ("core.sync_service.force_resync", "core.sync_service.get_sync_service",
             "web.routes.api.admin._sync_all_buyers")


def _posts() -> dict:
    from web.main import app

    return {e.path: e for e in iter_endpoints(app) if "POST" in e.methods}


@pytest.mark.parametrize("door", DOORS)
def test_every_door_is_registered_and_in_the_schema(door):
    from fastapi.testclient import TestClient

    from web.main import app

    assert door in _posts(), door
    assert door in TestClient(app).get("/openapi.json").json()["paths"], door


def test_no_path_under_the_old_prefix_is_registered():
    """Mutation: put any of the four `@router.post("/duckdb/…")` back."""
    from web.main import app

    paths = {e.path for e in iter_endpoints(app)}
    assert not [p for p in paths if p.startswith("/api/duckdb/")], sorted(paths)


def test_no_route_is_an_alias_of_another():
    """The aliases were the app's only routes out of the schema serving a
    function a route in the schema serves at another path. Mutation: register
    any lever under a second path, in the schema or out of it."""
    from web.main import app

    assert aliases(app) == frozenset()
    functions = [e.route.endpoint for e in iter_endpoints(app)
                 if e.path in DOORS]
    assert len(functions) == len(set(functions)) == len(DOORS)


def test_the_slow_levers_get_the_long_timeout_by_their_one_name():
    """Mutation: drop a lever from `SLOW_ENDPOINTS`, or put an alias back."""
    from web.middleware import SLOW_ENDPOINTS

    assert SLOW <= set(SLOW_ENDPOINTS)
    for door in set(DOORS) - SLOW:
        assert door not in SLOW_ENDPOINTS, door
    assert not [p for p in SLOW_ENDPOINTS if p.startswith("/api/duckdb/")]


def _long_timeout_locations() -> list:
    text = NGINX.read_text(encoding="utf-8")
    return [m.group(1) for m in re.finditer(
        r"location ~ (\S+) \{(?:(?!location).)*?proxy_read_timeout 300;", text, re.S)]


@pytest.mark.parametrize("path", sorted(SLOW))
def test_nginx_gives_the_slow_levers_its_long_timeout(path):
    """The regex names `sync/(resync|refresh-statuses)`. Mutation: drop it."""
    assert any(re.match(rx, path) for rx in _long_timeout_locations()), path


@pytest.mark.parametrize("path", ["/api/sync/sync-buyers", "/api/sync/sync-all-buyers",
                                  *sorted(RETIRED)])
def test_nginx_names_no_other_sync_door_slow(path):
    """The buyer syncs never were slow; the old names are gone. Mutation: put
    `duckdb/resync` back in the regex."""
    assert not any(re.match(rx, path) for rx in _long_timeout_locations()), path


@pytest.mark.parametrize("door", DOORS)
def test_every_doors_limit_is_one_counter_scoped_by_its_path(door):
    """The `shared_limit` each door took while it had two names, scoped by its
    path: one counter per lever. Mutation: drop a door's limit, or scope it
    by another name."""
    import inspect

    from web.ratelimit import limiter

    func = inspect.unwrap(_posts()[door].route.endpoint)
    limits = limiter._route_limits.get(f"{func.__module__}.{func.__name__}")
    assert limits, f"{door}: no rate limit registered"
    assert {limit.scope for limit in limits} == {door.removeprefix("/api/")}, (
        door, [(str(limit.limit), limit.scope) for limit in limits])


def test_the_resync_still_allows_one_a_minute(admin_client):  # noqa: F811
    """The limit survives the alias: the second resync in a minute is
    refused, and the refused one never reaches the lever."""
    from unittest.mock import AsyncMock, patch

    with patch("core.sync_service.force_resync",
               AsyncMock(return_value={"orders": 0})) as resync:
        first = admin_client.post("/api/sync/resync", params={"days": 1})
        second = admin_client.post("/api/sync/resync", params={"days": 1})
    assert first.status_code == 200, first.text
    assert second.status_code == 429, second.text
    assert resync.await_count == 1


@pytest.mark.parametrize("old", sorted(RETIRED))
def test_an_old_name_answers_as_no_route_and_runs_nothing(admin_client, old):  # noqa: F811
    """An admin's POST to an old name is the JSON 405 of a path no route
    serves, and a GET is the JSON 404 of every unmatched `/api/` GET — not
    the SPA shell, and neither reaches a lever. Until the final stage the
    POST ran the lever. Mutation: put the alias back — the POST answers as
    the lever does."""
    from unittest.mock import AsyncMock, MagicMock, patch

    with patch(_HANDLERS[0], AsyncMock(return_value={"orders": 0})) as resync, \
            patch(_HANDLERS[1], AsyncMock(return_value=MagicMock())) as sync, \
            patch(_HANDLERS[2], AsyncMock(return_value={})) as fetch_all, \
            patch("web.routes.api.admin.get_store", AsyncMock(return_value=MagicMock())):
        post = admin_client.post(old, params={"days": 1}, follow_redirects=False)
        get = admin_client.get(old, follow_redirects=False)
    assert post.status_code == 405, (old, post.status_code, post.text[:120])
    assert post.headers["content-type"].startswith("application/json")
    assert get.status_code == 404, (old, get.status_code, get.text[:120])
    assert get.json() == {"detail": "Not Found"}
    resync.assert_not_awaited()
    sync.assert_not_awaited()
    fetch_all.assert_not_called()


@pytest.mark.parametrize("old", ["/api/duckdb/stats", *sorted(RETIRED)])
def test_an_old_get_is_the_json_404_even_without_a_session(old):
    """Answered before the session check, as every unmatched `/api/` GET is
    (`tests/unit/test_od10_retired_doors.py`)."""
    from fastapi.testclient import TestClient

    from web.main import app

    response = TestClient(app).get(old, follow_redirects=False)
    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}
