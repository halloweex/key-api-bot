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
from datetime import datetime, timezone
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


# What each door is posted with: the short windows, and the status refresh
# inline, so its run is counted before the answer.
PARAMS = {
    "/api/sync/resync": {"days": 1},
    "/api/sync/refresh-statuses": {"days": 1, "background": "false"},
    "/api/sync/sync-buyers": {},
    "/api/sync/sync-all-buyers": {},
}
# More requests than any door's limit lets through in its window.
MAX_TRIES = 200


def _levers(monkeypatch) -> dict:
    """Each door's work stood in for where the door hands it over, counting
    the runs it was asked for; every door answers as on a good day. The full
    buyer sync runs detached, so its count is the request reaching the store
    it hands to the run."""
    from types import SimpleNamespace

    from web.routes.api import admin

    runs = dict.fromkeys(DOORS, 0)
    stamp = datetime.now(timezone.utc)

    async def force_resync(days_back):
        runs["/api/sync/resync"] += 1
        return {"orders": 0}

    async def refresh_order_statuses(days_back):
        runs["/api/sync/refresh-statuses"] += 1
        return {"orders_checked": 0}

    async def sync_missing_buyers(limit):
        runs["/api/sync/sync-buyers"] += 1
        return 0

    service = SimpleNamespace(
        refresh_order_statuses=refresh_order_statuses,
        sync_missing_buyers=sync_missing_buyers,
        buyer_sync_state=SimpleNamespace(last_ok_at=stamp, last_attempt_at=stamp))

    async def get_sync_service():
        return service

    async def get_store():
        runs["/api/sync/sync-all-buyers"] += 1
        return SimpleNamespace()

    async def every_buyer(_store):
        return {"status": "done"}

    monkeypatch.setattr("core.sync_service.force_resync", force_resync)
    monkeypatch.setattr("core.sync_service.get_sync_service", get_sync_service)
    monkeypatch.setattr(admin, "get_store", get_store)
    monkeypatch.setattr(admin, "_sync_all_buyers", every_buyer)
    return runs


@pytest.mark.parametrize("door", DOORS)
def test_a_spent_door_answers_429_and_starts_nothing(door, admin_client, monkeypatch):
    """Every door that starts a sync from KeyCRM by hand is rate limited: once
    its limit is spent it answers 429, and the refused request never reaches
    the work. Each door counts on its own, so the others still answer."""
    runs = _levers(monkeypatch)

    answers = []
    while len(answers) < MAX_TRIES and 429 not in answers:
        answers.append(admin_client.post(door, params=PARAMS[door]).status_code)

    assert answers[-1] == 429, f"{door}: no 429 in {len(answers)} requests"
    assert set(answers[:-1]) == {200}, answers
    assert runs[door] == len(answers) - 1, "the refused request ran the lever"
    for other in DOORS:
        if other != door:
            assert admin_client.post(other, params=PARAMS[other]).status_code == 200, other
