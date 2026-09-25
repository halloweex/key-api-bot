"""DN-20b: under `KS_READ_FALLBACK=off` a route refuses instead of reading DuckDB.

After step 13 every fallback to DuckDB serves frozen Silver and Gold, and after
the first Sunday compaction empty ones, behind a page that looks as if it
works. Under `off`, `core.read_fallback.fall_back` raises `ReadUnavailable`
instead of letting the caller read DuckDB, and one handler in `web/main.py`
turns that — from any route — into a 503 naming the surface.

What is proved here, through the real app and a Postgres pool that raises:

- under `off`, 503 naming the surface for each router DN-20b covers — the
  dashboard's summary and trend, goals, traffic, expenses (both through the
  history gate `backfilled()` and through the router), margin, products
  intel, inventory, and cohorts — and, because the raise is one, every other
  router an HTTP route reaches (reports, marketing, the dashboard's own
  router, the filter lookups, the managers screen); nothing is counted as a
  fallback, and the refusal is counted and published beside the mode instead;
- the trend's forecast overlay, whose reads already degrade to "no forecast",
  drops under a refusal while the chart answers — no DuckDB number reaches
  the page, and the refusal is counted — while the forecast's own endpoint,
  `/api/revenue/forecast`, answers 503 like any route rather than "Forecast
  not available yet";
- a switch naming Postgres with no `KS_PG_DSN` at all is refused under `off`
  exactly like a failed read, for every one of those routes — the gate is
  simply false there, and a lost DSN line would otherwise put the whole
  dashboard back on DuckDB with nothing failing — and under `duckdb` it
  still answers what DuckDB answers, counting nothing;
- cohorts under `off` are answered by a live ClickHouse or not at all: a
  switch naming no configured ClickHouse is refused too, and a live one
  answers 200;
- under `duckdb`, the default, each of those routes answers exactly what
  DuckDB answers with the switch unset — the fallback is unchanged;
- no handler on an HTTP path catches `ReadUnavailable` without raising it.

The walk over every fallback site is `tests/unit/test_read_fallback_sites.py`.
"""
from __future__ import annotations

import ast
import logging
import pathlib
from datetime import date, timedelta
from typing import Dict, List, Optional
from unittest.mock import AsyncMock

import pytest

from core import read_fallback
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client,
    fresh_read_fallback,
)

REPO = pathlib.Path(__file__).resolve().parents[2]

DAY = date.today() - timedelta(days=3)
WINDOW = {"start_date": (DAY - timedelta(days=6)).isoformat(),
          "end_date": DAY.isoformat()}


class Case:
    """One route, the switch that sends it to Postgres, and the surface a
    failure there is counted against."""

    def __init__(self, name: str, surface: str, env: Dict[str, str], path: str,
                 params: Optional[dict] = None):
        self.name, self.surface, self.env = name, surface, env
        self.path, self.params = path, dict(params or {})

    def __repr__(self) -> str:  # pytest ids
        return self.name


# The routers DN-20b names, one route each (two for the dashboard, which the
# plan names by its two primitives). Postgres-backed only; the cohorts, which
# are ClickHouse's, have their own class below.
CASES: List[Case] = [
    Case("summary", "dashboard", {"KS_READ_GOLD": "postgres"},
         "/api/summary", WINDOW),
    Case("trend", "dashboard", {"KS_READ_GOLD": "postgres"},
         "/api/revenue/trend", WINDOW),
    Case("goals", "goals", {"KS_READ_GOALS": "postgres"}, "/api/goals"),
    Case("traffic", "traffic", {"KS_READ_TRAFFIC": "postgres"},
         "/api/traffic/analytics", WINDOW),
    Case("expenses_history", "expenses", {"KS_READ_EXPENSES": "postgres"},
         "/api/expenses/summary", WINDOW),
    Case("margin", "margin", {"KS_READ_MARGIN": "postgres"},
         "/api/margin/overview", WINDOW),
    Case("products_intel", "products_intel",
         {"KS_READ_PRODUCTS_INTEL": "postgres"},
         "/api/products/intel/summary", WINDOW),
    Case("inventory", "inventory", {"KS_READ_INVENTORY": "postgres"},
         "/api/stocks/summary"),
    # The forecast's own endpoint, which the frontend calls for the last
    # forecast date. Its reads are the goals router's; a refusal there used
    # to come back as 200 "Forecast not available yet" — an outage read as
    # an untrained model. The trend's overlay is the other door to the same
    # reads, and deliberately drops instead (below).
    Case("forecast", "goals", {"KS_READ_GOALS": "postgres"},
         "/api/revenue/forecast"),
    # Beyond the plan's list, and not by choice: `fall_back` is one raise, so
    # every other router behind an HTTP route refuses too. Each is here to
    # prove that nothing between it and the handler turns the refusal into a
    # 500 or a quiet answer — the filter bar's lookups included, which under
    # `off` are refused on every tab at once, exactly like the tab they sit on.
    Case("reports", "reports", {"KS_READ_REPORTS": "postgres"},
         "/api/reports/summary", WINDOW),
    Case("marketing", "marketing", {"KS_READ_MARKETING": "postgres"},
         "/api/promocodes/analytics", WINDOW),
    Case("insights", "dashboard", {"KS_READ_DASHBOARD": "postgres"},
         "/api/customers/insights", WINDOW),
    Case("lookups", "lookups", {"KS_READ_LOOKUPS": "postgres"},
         "/api/categories"),
    Case("managers", "managers", {"KS_READ_SILVER": "postgres"},
         "/api/managers"),
]

COHORTS = "/api/customers/cohort-retention"
COHORT_PARAMS = {"include_revenue": "false"}


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


def _mode(monkeypatch, value: Optional[str]) -> None:
    """Set KS_READ_FALLBACK and read it the way web's startup does."""
    if value is None:
        monkeypatch.delenv("KS_READ_FALLBACK", raising=False)
    else:
        monkeypatch.setenv("KS_READ_FALLBACK", value)
    read_fallback.configure_mode()


def _switches(monkeypatch, env: Dict[str, str]) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)


# ─── Under off: a 503 naming the surface, and nothing served from DuckDB ───

class TestOffRefuses:
    @pytest.mark.parametrize("case", CASES, ids=repr)
    def test_a_failed_engine_is_a_503_naming_the_surface(
        self, case, admin_client, stores, monkeypatch, caplog,
    ):
        _mode(monkeypatch, "off")
        _switches(monkeypatch, case.env)

        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            response = admin_client.get(case.path, params=case.params)

        assert response.status_code == 503, response.text
        body = response.json()
        assert body["surface"] == case.surface
        assert body["error"] == "Service Unavailable"
        assert "connection refused" not in response.text, (
            "the cause stays in the log; the response is public to the page")
        # Refused, not served: no fallback counted, the refusal counted.
        assert read_fallback.counts() == {}
        assert read_fallback.refusals()[case.surface]["count"] >= 1
        # The soak greps for this phrase; a refusal must not say it.
        assert "falling back to DuckDB" not in caplog.text
        assert "read refused" in caplog.text

    def test_the_refusal_is_published_beside_the_mode(
        self, admin_client, stores, monkeypatch,
    ):
        _mode(monkeypatch, "off")
        _switches(monkeypatch, {"KS_READ_TRAFFIC": "postgres"})
        assert admin_client.get("/api/traffic/analytics",
                                params=WINDOW).status_code == 503

        health = admin_client.get("/api/health").json()
        assert health["read_fallbacks"] == {}
        block = health["read_fallback_mode"]
        assert block["mode"] == "off"
        assert block["refused"]["traffic"]["count"] == 1
        assert block["refused"]["traffic"]["last_at"]
        assert "connection refused" not in str(block)

    def test_the_history_gate_refuses_rather_than_answering_no(
        self, admin_client, stores, monkeypatch,
    ):
        """`backfilled()` is the gate that served DuckDB under words nobody
        grepped for. Under off, its unreadable watermark is a 503 — and so is
        every call in the minute after, which reuses that failure rather than
        asking Postgres again."""
        from core import pg_expenses_read

        _mode(monkeypatch, "off")
        _switches(monkeypatch, {"KS_READ_EXPENSES": "postgres"})
        for _ in range(2):
            response = admin_client.get("/api/expenses/summary", params=WINDOW)
            assert response.status_code == 503
            assert response.json()["surface"] == "expenses"
        assert pg_expenses_read._check_failed is not None
        assert read_fallback.refusals()["expenses"]["count"] >= 2
        assert read_fallback.counts() == {}

    def test_a_confirmed_history_still_refuses_at_the_router(
        self, admin_client, stores, monkeypatch,
    ):
        """History already confirmed, so the gate passes and the router's own
        read is what fails — refused there, not answered from DuckDB."""
        from core import pg_expenses_read

        monkeypatch.setattr(pg_expenses_read, "_backfilled", True)
        _mode(monkeypatch, "off")
        _switches(monkeypatch, {"KS_READ_EXPENSES": "postgres"})
        response = admin_client.get("/api/expenses/summary", params=WINDOW)
        assert response.status_code == 503
        assert response.json()["surface"] == "expenses"

    def test_a_refused_forecast_drops_the_overlay_not_the_chart(
        self, admin_client, stores, monkeypatch,
    ):
        """The trend's forecast is an overlay, and its reads already degrade
        to "no forecast" on any failure (`get_forecast_data`, and the handler
        around it in the route). A refusal there is one of those failures:
        the chart answers without the overlay, nothing is served from DuckDB,
        and the refusal is still counted — so a broken goals port stays
        visible under `read_fallback_mode.refused`. What `off` forbids is
        DuckDB's numbers, and none reach the page. The forecast's own
        endpoint is not an overlay and answers 503 (the `forecast` case)."""
        _mode(monkeypatch, "off")
        _switches(monkeypatch, {"KS_READ_GOALS": "postgres"})
        response = admin_client.get("/api/revenue/trend", params={
            "period": "month", "include_forecast": "true"})
        assert response.status_code == 200, response.text
        assert "forecast" not in response.json()
        assert read_fallback.refusals()["goals"]["count"] >= 1
        assert read_fallback.counts() == {}

    @pytest.mark.parametrize("dsn", [True, False], ids=["dsn", "no_dsn"])
    def test_a_switch_left_at_duckdb_is_not_a_fallback(
        self, dsn, admin_client, stores, monkeypatch,
    ):
        """`off` refuses a fallback, not DuckDB as the engine a switch names:
        a Postgres-backed tab whose switch says `duckdb` is a routing
        decision, published nowhere and refused nowhere — with a DSN or
        without one. The cohorts are the exception, below."""
        if not dsn:
            monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setenv("KS_READ_TRAFFIC", "duckdb")
        _mode(monkeypatch, "off")
        response = admin_client.get("/api/traffic/analytics", params=WINDOW)
        assert response.status_code == 200
        assert read_fallback.refusals() == {}


# ─── A switch with no address: refused under off, unchanged under duckdb ───

@pytest.fixture
def no_dsn(stores, monkeypatch):
    """`stores`, and then no `KS_PG_DSN` at all: every Postgres switch below
    names an engine this process cannot reach, and nothing will fail."""
    monkeypatch.delenv("KS_PG_DSN", raising=False)
    return stores


class TestNoAddress:
    @pytest.mark.parametrize("case", CASES, ids=repr)
    def test_under_off_it_is_a_503_naming_the_surface(
        self, case, admin_client, no_dsn, monkeypatch, caplog,
    ):
        _switches(monkeypatch, case.env)
        _mode(monkeypatch, "off")
        (switch, value), = case.env.items()
        assert read_fallback.misconfigured() == [
            f"{switch}={value} without KS_PG_DSN"]

        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            response = admin_client.get(case.path, params=case.params)

        assert response.status_code == 503, response.text
        assert response.json()["surface"] == case.surface
        assert read_fallback.counts() == {}
        assert read_fallback.refusals()[case.surface]["count"] >= 1
        assert f"{switch}=postgres without KS_PG_DSN" in caplog.text
        assert "falling back to DuckDB" not in caplog.text

    @pytest.mark.parametrize("case", CASES, ids=repr)
    def test_under_duckdb_it_answers_what_duckdb_answers(
        self, case, admin_client, no_dsn, monkeypatch,
    ):
        """The default is untouched: published once as misconfigured, served
        from DuckDB per request, nothing counted and nothing refused."""
        _mode(monkeypatch, "duckdb")
        direct = admin_client.get(case.path, params=case.params)
        assert direct.status_code == 200, direct.text

        _switches(monkeypatch, case.env)
        unaddressed = admin_client.get(case.path, params=case.params)
        assert unaddressed.status_code == 200, unaddressed.text
        assert unaddressed.json() == direct.json()
        assert read_fallback.counts() == {}
        assert read_fallback.refusals() == {}


# ─── Under duckdb: exactly what it was ─────────────────────────────────────

class TestDuckdbUnchanged:
    @pytest.mark.parametrize("unset", [True, False], ids=["unset", "duckdb"])
    @pytest.mark.parametrize("case", CASES, ids=repr)
    def test_the_fallback_answers_what_duckdb_answers(
        self, case, unset, admin_client, stores, monkeypatch,
    ):
        """The same route twice: once with its switch unset, DuckDB answering
        as the named engine, and once sent to a Postgres that fails, so
        DuckDB answers as the fallback. Byte for byte the same body, and one
        fallback counted — whether KS_READ_FALLBACK is unset or `duckdb`."""
        _mode(monkeypatch, None if unset else "duckdb")

        direct = admin_client.get(case.path, params=case.params)
        assert direct.status_code == 200, direct.text
        assert read_fallback.counts() == {}

        _switches(monkeypatch, case.env)
        fallen = admin_client.get(case.path, params=case.params)
        assert fallen.status_code == 200, fallen.text
        assert fallen.json() == direct.json()
        assert read_fallback.counts()[case.surface]["count"] >= 1
        assert read_fallback.refusals() == {}

        block = admin_client.get("/api/health").json()["read_fallback_mode"]
        assert "refused" not in block, (
            "under duckdb the block keeps the shape it always had")


# ─── Cohorts: a live ClickHouse or nothing ─────────────────────────────────

@pytest.fixture
def clickhouse(monkeypatch):
    """KS_READ_COHORTS=clickhouse with an address; `fetch` is what each test
    makes of it."""
    from core import ch_cohorts

    monkeypatch.setenv("KS_READ_COHORTS", "clickhouse")
    monkeypatch.setenv("KS_CH_URL", "http://nowhere.invalid:8123")
    fetch = AsyncMock(side_effect=OSError("clickhouse down"))
    monkeypatch.setattr(ch_cohorts, "fetch", fetch)
    return fetch


class TestCohorts:
    def test_under_off_a_failed_clickhouse_is_a_503(
        self, admin_client, stores, clickhouse, monkeypatch,
    ):
        _mode(monkeypatch, "off")
        response = admin_client.get(COHORTS, params=COHORT_PARAMS)
        assert response.status_code == 503
        assert response.json()["surface"] == "cohorts"
        assert read_fallback.counts() == {}
        assert read_fallback.refusals()["cohorts"]["count"] == 1

    @pytest.mark.parametrize("switch", [None, "duckdb", "clickhouse"],
                             ids=["unset", "duckdb", "clickhouse_no_url"])
    def test_under_off_no_live_clickhouse_is_a_503(
        self, switch, admin_client, stores, monkeypatch,
    ):
        """No Postgres body, so DuckDB is the only other engine — and under
        off it may not answer, whatever the switch names."""
        if switch is None:
            monkeypatch.delenv("KS_READ_COHORTS", raising=False)
        else:
            monkeypatch.setenv("KS_READ_COHORTS", switch)
        _mode(monkeypatch, "off")
        response = admin_client.get(COHORTS, params=COHORT_PARAMS)
        assert response.status_code == 503
        assert response.json()["surface"] == "cohorts"
        assert read_fallback.refusals()["cohorts"]["count"] == 1

    def test_under_off_a_live_clickhouse_answers(
        self, admin_client, stores, clickhouse, monkeypatch,
    ):
        clickhouse.side_effect = None
        clickhouse.return_value = []
        _mode(monkeypatch, "off")
        response = admin_client.get(COHORTS, params=COHORT_PARAMS)
        assert response.status_code == 200, response.text
        assert clickhouse.await_count >= 1
        assert read_fallback.refusals() == {}

    def test_under_duckdb_a_failed_clickhouse_falls_back_unchanged(
        self, admin_client, stores, clickhouse, monkeypatch,
    ):
        _mode(monkeypatch, "duckdb")
        fallen = admin_client.get(COHORTS, params=COHORT_PARAMS)
        assert fallen.status_code == 200, fallen.text
        assert read_fallback.counts()["cohorts"]["count"] >= 1

        monkeypatch.delenv("KS_READ_COHORTS")
        direct = admin_client.get(COHORTS, params=COHORT_PARAMS)
        assert direct.status_code == 200
        assert fallen.json() == direct.json()
        assert read_fallback.refusals() == {}, (
            "under duckdb, DuckDB as the named engine is not a refusal")


# ─── Every GET route: a refusal is a 503 naming its surface ────────────────

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
    it into the test — a route whose engine fails with no fallback (the
    forecast's training input) answers 500, and that is not this sweep's
    question."""
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


def _every_switch_on(monkeypatch) -> None:
    """Every read switch the fallback walk finds, set to the engine it names."""
    import importlib

    from tests.unit.test_read_fallback_sites import read_switch_modules

    for name in read_switch_modules():
        module = importlib.import_module(f"core.{name}")
        monkeypatch.setenv(module.ENV,
                           "clickhouse" if name.startswith("ch_") else "postgres")


class TestEveryRoute:
    """Every GET route under /api, read off the app rather than listed: under
    `off`, any request that leaves a refusal counted answered 503 naming a
    surface it refused. A handler anywhere between a route and a router that
    turns the refusal into an answer — `except Exception: return {}` in a
    service — is found here whether or not anybody thought to list the
    route. The two ways a gate refuses are swept apart: an engine that fails,
    and a switch with no address."""

    @pytest.mark.parametrize("state", ["engine_fails", "no_address"])
    def test_a_refusal_is_a_503_naming_its_surface(
        self, state, sweep_client, stores, no_network, monkeypatch,
    ):
        from core import ch_cohorts
        from web.main import app
        from web.ratelimit import limiter

        _every_switch_on(monkeypatch)
        if state == "engine_fails":
            monkeypatch.setenv("KS_CH_URL", "http://nowhere.invalid:8123")
            monkeypatch.setattr(ch_cohorts, "fetch",
                                AsyncMock(side_effect=OSError("clickhouse down")))
        else:
            monkeypatch.delenv("KS_PG_DSN", raising=False)
            monkeypatch.delenv("KS_CH_URL", raising=False)
        _mode(monkeypatch, "off")

        routes = swept_routes(app)
        refused_routes, violations = [], []
        for endpoint in routes:
            read_fallback.reset_counts()
            limiter.reset()
            path, params = _request_for(endpoint)
            response = sweep_client.get(path, params=params)
            refused = read_fallback.refusals()
            assert read_fallback.counts() == {}, (
                f"{endpoint.path}: a fallback counted under off")
            if not refused:
                continue
            refused_routes.append(endpoint.path)
            try:
                surface = response.json().get("surface")
            except Exception:  # noqa: BLE001 — a CSV, an HTML page
                surface = None
            if response.status_code != 503 or surface not in refused:
                violations.append(
                    f"{endpoint.path} answered {response.status_code} "
                    f"(surface={surface!r}) with {sorted(refused)} refused")

        assert not violations, (
            "a refusal that did not reach the 503 handler: " + "; ".join(violations))
        # Non-vacuity: the sweep reached the routers. Sixty-odd of the
        # ninety-eight GET routes refused when it was written.
        assert len(routes) >= 90, len(routes)
        assert len(refused_routes) >= 55, refused_routes

    def test_the_sweep_is_read_off_the_app(self):
        """Every GET route but the named network ones — and a route added
        tomorrow is swept the day it is registered."""
        from tests.routes_helper import iter_endpoints
        from web.main import app

        every = {e.path for e in iter_endpoints(app)
                 if "GET" in e.methods and e.path.startswith("/api/")}
        assert {e.path for e in swept_routes(app)} == every - set(NETWORK_ROUTES)
        assert set(NETWORK_ROUTES) <= every, "a named network route no longer exists"


# ─── No HTTP path swallows a refusal ───────────────────────────────────────

def _module_file(dotted: str) -> Optional[pathlib.Path]:
    rel = dotted.replace(".", "/")
    for candidate in (REPO / f"{rel}.py", REPO / rel / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _imported_modules(path: pathlib.Path) -> set:
    """Every repository module `path` imports, at the top or inside a
    function — most imports here are local, to dodge cycles."""
    # A module's package is its directory, for `x.py` and `__init__.py` alike.
    package = list(path.relative_to(REPO).parent.parts)
    found = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = package[:len(package) - (node.level - 1)] if node.level else []
            module = ".".join(base + ([node.module] if node.module else []))
            if not module:
                continue
            found.add(module)
            found.update(f"{module}.{alias.name}" for alias in node.names)
    return found


def http_files() -> List[pathlib.Path]:
    """Every repository module reachable by import from `web/` — the code an
    HTTP route can run. Computed, not listed: `core/prediction_service.py`
    and `core/duckdb_store.py` are on HTTP paths without being routers."""
    seen = set()
    queue = [p for p in sorted((REPO / "web").rglob("*.py"))
             if "frontend" not in p.parts and "node_modules" not in p.parts]
    while queue:
        path = queue.pop()
        if path in seen:
            continue
        seen.add(path)
        for dotted in _imported_modules(path):
            target = _module_file(dotted)
            if target is not None and target not in seen:
                queue.append(target)
    return sorted(seen)


def _names_read_unavailable(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return False
    return any(
        (isinstance(n, ast.Name) and n.id == "ReadUnavailable")
        or (isinstance(n, ast.Attribute) and n.attr == "ReadUnavailable")
        for n in ast.walk(handler.type))


def _returns_somewhere(handler: ast.ExceptHandler) -> bool:
    """A `return` anywhere in the handler's own statements — nested
    functions aside. A handler that can return has an exit that is not the
    raise, and `return x` before a final `raise` never reaches it (DN-20c)."""
    stack = list(handler.body)
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Return):
            return True
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.Lambda, ast.ClassDef)):
            continue
        stack.extend(ast.iter_child_nodes(node))
    return False


def _lets_it_through(handler: ast.ExceptHandler) -> bool:
    """The handler ends in a bare `raise`, or `raise <the name it bound>` —
    the refusal itself, not something raised in its place — and has no
    `return` on the way. `raise HTTPException(500)` would turn the 503
    naming the surface into a 500 naming nothing."""
    if not handler.body or not isinstance(handler.body[-1], ast.Raise):
        return False
    if _returns_somewhere(handler):
        return False
    last = handler.body[-1]
    if last.exc is None:
        return True
    return (last.cause is None and isinstance(last.exc, ast.Name)
            and handler.name is not None and last.exc.id == handler.name)


def swallowing_handlers(source: str) -> List[int]:
    """Lines of every handler naming `ReadUnavailable` that can end without
    raising that refusal. Pure, so it can be shown synthetic code."""
    return [node.lineno for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.ExceptHandler)
            and _names_read_unavailable(node) and not _lets_it_through(node)]


def enclosing_functions(source: str) -> Dict[int, str]:
    """Line → `Class.function` (or `function`) of the innermost function
    holding it — the form `test_read_fallback_consumers.py` keys functions
    by, so a handler can be matched to an answer written down there."""
    found: Dict[int, str] = {}

    def visit(node, cls, qual):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, child.name, qual)
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = f"{cls}.{child.name}" if cls else child.name
                for inner in ast.walk(child):
                    if hasattr(inner, "lineno"):
                        found[inner.lineno] = name
                visit(child, cls, name)
            else:
                visit(child, cls, qual)

    visit(ast.parse(source), None, None)
    return found


class TestNoHandlerSwallowsARefusal:
    """The static half of the guard; `TestEveryRoute` is the half that runs.
    This one sees a handler that *names* the refusal — anywhere an HTTP route
    can reach, which is every module `web/` imports, transitively. A broad
    `except Exception` that catches it without naming it is invisible here
    and is what the sweep is for.

    DN-20c's answers are the one exception, by function and nowhere else:
    the scheduler's jobs defer or skip, and the assistant's tools return a
    named "data unavailable" result. Those modules are importable from
    `web/`, but the functions holding the answers are reached only by the
    consumers no HTTP request waits for — which
    `test_read_fallback_consumers.py` proves by walking up from each."""

    def test_every_handler_naming_it_raises(self):
        """A handler that names the refusal and does not raise it has turned
        a 503 into an answer — the DuckDB one, or a quiet partial one. The
        walk in `test_read_fallback_sites.py` accepts naming the refusal as
        a decision; on an HTTP path the only decision is to let it through."""
        from tests.unit.test_read_fallback_consumers import named_answer_functions

        answers = named_answer_functions()
        bad, exempted = [], set()
        for path in http_files():
            rel = path.relative_to(REPO).as_posix()
            source = path.read_text(encoding="utf-8")
            where = enclosing_functions(source)
            for line in swallowing_handlers(source):
                key = f"{rel}:{where.get(line)}"
                if key in answers:
                    exempted.add(key)
                else:
                    bad.append(f"{rel}:{line} ({where.get(line)})")
        assert not bad, "a refusal swallowed on an HTTP path: " + ", ".join(bad)
        # Each exemption is used: an answer that moved leaves no stale one.
        assert exempted == answers, sorted(answers - exempted)

    def test_it_is_looking(self):
        """Non-vacuity: the handlers that name it today are found, and found
        raising; and the scope reaches past the routers into what the
        routes call."""
        files = http_files()
        for rel in ("core/repositories/goals.py", "web/services/dashboard_service.py"):
            tree = ast.parse((REPO / rel).read_text(encoding="utf-8"))
            assert any(isinstance(n, ast.ExceptHandler) and _names_read_unavailable(n)
                       for n in ast.walk(tree)), rel
            assert REPO / rel in files
        for rel in ("core/prediction_service.py", "core/duckdb_store.py",
                    "core/read_fallback.py", "core/pg_traffic_read.py"):
            assert REPO / rel in files, rel

    def test_the_rule_bites(self):
        src = (
            "async def f(self):\n"
            "    try:\n"
            "        return await self._traffic_run('x')\n"
            "    except read_fallback.ReadUnavailable:\n"
            "        return []\n"
        )
        assert swallowing_handlers(src) == [4]
        assert swallowing_handlers(src.replace("return []", "raise")) == []

    def test_a_return_before_the_raise_is_an_exit(self):
        """`return` first and the final `raise` never runs — the shape the
        rule's last-statement check alone passed (found by a DN-20c
        mutation)."""
        src = (
            "async def f(self):\n"
            "    try:\n"
            "        return await self._traffic_run('x')\n"
            "    except read_fallback.ReadUnavailable:\n"
            "        if self.quiet:\n"
            "            return None\n"
            "        raise\n"
        )
        assert swallowing_handlers(src) == [4]

    @pytest.mark.parametrize("ending,swallows", [
        ("raise", False),
        ("raise exc", False),
        ("raise HTTPException(status_code=500)", True),
        ("raise exc from None", True),
        ("raise RuntimeError('x') from exc", True),
        ("raise other", True),
        ("log(exc)", True),
    ])
    def test_only_the_refusal_itself_may_be_raised(self, ending, swallows):
        src = (
            "async def f(self):\n"
            "    try:\n"
            "        return await self._traffic_run('x')\n"
            "    except read_fallback.ReadUnavailable as exc:\n"
            f"        {ending}\n"
        )
        assert (swallowing_handlers(src) == [4]) is swallows
