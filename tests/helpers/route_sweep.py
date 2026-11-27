"""Every GET route under /api asked as an admin, with the stores mocked to
refuse: the request each route needs and the fixtures the sweeps share.
"""
from __future__ import annotations

from typing import List
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
def stores(monkeypatch, fresh_read_fallback):
    """Every read switch unset, a DSN that nothing will reach, a pool that
    raises, and the expense history gate starting from "never asked"."""
    from core import pg_expenses_read

    for name in ("KS_PG_DSN", "KS_CH_URL", "KS_WRITE_GOALS", "KS_READ_FALLBACK"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
    monkeypatch.setattr("core.pg.get_pool",
                        AsyncMock(side_effect=OSError("connection refused")))
    monkeypatch.setattr(pg_expenses_read, "_backfilled", False)
    monkeypatch.setattr(pg_expenses_read, "_checked_at", 0.0)
    monkeypatch.setattr(pg_expenses_read, "_check_failed", None)
    return fresh_read_fallback


# The routes the sweep does not call, each for a reason about the network and
# never about what the route reads. Which routes are swept is read off the app.
NETWORK_ROUTES = {
    "/api/chat/stream": (
        "streams a conversation with the Anthropic API for as long as it "
        "lasts; what the assistant makes of a refusal is DN-20c's"),
}


# Example values for required parameters a route validates itself, where a
# generic value would end the request with a 400 before anything is read.
# Values, not subjects: a parameter missing here still gets one by its type.
EXAMPLE_VALUES = {"period_type": "monthly", "abc_class": "A", "type": "summary"}


def _example(param) -> object:
    if param.name in EXAMPLE_VALUES:
        return EXAMPLE_VALUES[param.name]
    info = param.field_info
    if info.annotation is int:
        floor = [m.ge for m in getattr(info, "metadata", ()) if hasattr(m, "ge")]
        return floor[0] if floor else 1
    return "ab1"


def _request_for(endpoint) -> tuple:
    """The path with every path parameter filled, and every required query
    parameter, by name where the route validates it and by type otherwise."""
    dependant = endpoint.route.dependant
    path = endpoint.path
    for param in dependant.path_params:
        path = path.replace("{" + param.name + "}", str(_example(param)))
    from tests.routes_helper import is_required

    params = {q.name: _example(q) for q in dependant.query_params if is_required(q)}
    return path, params


def swept_routes(app) -> List:
    from tests.routes_helper import iter_endpoints

    return sorted(
        (e for e in iter_endpoints(app)
         if "GET" in e.methods and e.path.startswith("/api/")
         and e.path not in NETWORK_ROUTES),
        key=lambda e: e.path)


@pytest.fixture
def sweep_client(admin_client):
    """`admin_client`, answering a server error as a 500 rather than raising
    it into the test — a route that reads its store with no refusal (the
    managers screen, /sms, the user list) answers 500, and that is not this
    sweep's question."""
    from fastapi.testclient import TestClient

    from web.main import app

    client = TestClient(app, raise_server_exceptions=False)
    client.cookies.update(admin_client.cookies)
    return client


@pytest.fixture
def no_network(monkeypatch):
    """Nothing in the sweep leaves the process: the app is called in-process
    and the stores are mocked, so a socket opened is somebody's KeyCRM, CH
    or Meilisearch client — refused at once instead of waiting on a timeout."""
    import socket

    def refuse(*args, **kwargs):
        raise OSError("the network is off in this test")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)
    monkeypatch.setattr(socket, "getaddrinfo", refuse)
