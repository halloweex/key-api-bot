"""The sync levers under `/api/sync/`, and the `/api/duckdb/` names they lost.

Step 14's S14-17 (the owner's decision 6 for step 14, OD-S5-8) renamed the
four doors that start a sync from KeyCRM by hand — the resync, the status
refresh and the two buyer syncs — from `/api/duckdb/…`, a name from the days
they wrote DuckDB, to `/api/sync/…`. The old paths stayed until step 14's
final stage as aliases, the same function behind both, out of the schema.
The final stage removed them (2026-11-12), with their two entries in
`SLOW_ENDPOINTS`. nginx keeps `duckdb/resync` in its long-timeout regex until
Z3: the rollback image serves its resync only there. Each slow
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

from tests.routes_helper import iter_endpoints

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


def _posts() -> dict:
    from web.main import app

    return {e.path: e for e in iter_endpoints(app) if "POST" in e.methods}


@pytest.mark.parametrize("door", DOORS)
def test_every_door_is_registered_and_in_the_schema(door):
    from fastapi.testclient import TestClient

    from web.main import app

    assert door in _posts(), door
    assert door in TestClient(app).get("/openapi.json").json()["paths"], door


def _long_timeout_locations() -> list:
    text = NGINX.read_text(encoding="utf-8")
    return [m.group(1) for m in re.finditer(
        r"location ~ (\S+) \{(?:(?!location).)*?proxy_read_timeout 300;", text, re.S)]


@pytest.mark.parametrize("path", sorted(SLOW))
def test_nginx_gives_the_slow_levers_its_long_timeout(path):
    """The regex names `sync/(resync|refresh-statuses)`. Mutation: drop it."""
    assert any(re.match(rx, path) for rx in _long_timeout_locations()), path


@pytest.mark.parametrize("path", ["/api/sync/sync-buyers", "/api/sync/sync-all-buyers",
                                  *sorted(set(RETIRED) - {"/api/duckdb/resync"})])
def test_nginx_names_no_other_sync_door_slow(path):
    """The buyer syncs never were slow; the old names are gone. Mutation: add
    `duckdb/refresh-statuses` to the regex."""
    assert not any(re.match(rx, path) for rx in _long_timeout_locations()), path


def test_nginx_keeps_the_rollback_images_resync_slow_until_z3():
    """rollback-step14, a rollback target until Z3, serves the resync only at
    `/api/duckdb/resync` and runs it inside the request: without the long
    timeout nginx would answer 504 at 60 s while the handler runs on.
    Mutation: drop `duckdb/resync` from the regex."""
    assert any(re.match(rx, "/api/duckdb/resync") for rx in _long_timeout_locations())


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
