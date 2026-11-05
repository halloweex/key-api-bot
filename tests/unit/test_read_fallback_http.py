"""DN-20b: when the engine behind a route fails, the route answers 503 naming
the surface.

Every router reads one engine and refuses its failure
(`core.read_fallback.refuse`), and one handler in `web/main.py` turns that —
from any route — into a 503 naming the surface. After step 13 a DuckDB
answer would have served frozen Silver and Gold, and after the first Sunday
compaction empty ones, behind a page that looks as if it works; a 503 is an
outage somebody can see.

What is proved here, through the real app and a Postgres pool that raises:

- a 503 naming the surface for each router — the dashboard's summary and
  trend, goals, traffic, expenses (both through the history gate
  `backfilled()` and through the router), margin, products intel,
  inventory, reports, marketing, the dashboard's customer block, the filter
  lookups and the cohorts — and the refusal counted and published under
  `read_fallback_mode.refused`;
- the trend's forecast overlay, whose reads already degrade to "no
  forecast", drops under a refusal while the chart answers, and the refusal
  is counted — while the forecast's own endpoint, `/api/revenue/forecast`,
  answers 503 like any route rather than "Forecast not available yet";
- no `KS_PG_DSN` at all is the same 503: the pool's own refusal to guess a
  database is the failure refused;
- the cohorts are answered by a live ClickHouse or not at all;
- no read switch is read: any value of one, or none, is the same 503;
- **every GET route under `/api/`, read off the app** (`TestEveryRoute`):
  with the engine failing, or with no address for it, any request that left
  a refusal counted answered 503 naming it — and the routes of every tab
  that reads a store are among the refused;
- no handler on an HTTP path catches `ReadUnavailable` without raising it.

Until step 14's S14-16 this also held what `KS_READ_FALLBACK` made of a
fallback — `off` refused it, `duckdb` answered from DuckDB — and that a read
switch with no address was refused (`no_address`). Since S14-07 no router
has a DuckDB tail and no switch routes a read, and S14-16 deleted the mode
and the address question with the counts of reads answered from DuckDB.

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
    # Beyond DN-20b's list: every other router behind an HTTP route refuses
    # too. Each is here to prove that nothing between it and the handler
    # turns the refusal into a 500 or a quiet answer — the filter bar's
    # lookups included, which are refused on every tab at once, exactly like
    # the tab they sit on.
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

# Routes whose first read is a write chain's table. Since step 14's S14-00
# every chain writes Postgres — production's state since those chains' flips —
# and a read of its table asks Postgres with no fallback and no refusal, so a
# failed engine is that route's 500, not a 503: chain 5's classification
# behind the managers screen. Chain 7a's goals were the other until S14-07,
# whose `_goals_run` refuses every failure, theirs included.
CHAIN_FIRST = frozenset({"managers"})
REFUSING = [case for case in CASES if case.name not in CHAIN_FIRST]
# The routers step 14's S14-05 to S14-07 left with no DuckDB tail: they read
# Postgres whatever their switch says (`case.env` is not read for them), and
# refuse a failure — a DSN that is not there included. Every router is one
# since S14-07: no route is switched (and S14-16 deleted `no_address`).
POSTGRES_ONLY = frozenset({"traffic", "margin", "products_intel", "reports",
                           "marketing", "lookups",
                           # The dashboard's, step 14's S14-06.
                           "summary", "trend", "insights",
                           # `/expenses`, `/inventory` and `/goals` with its
                           # forecast, step 14's S14-07: none is left.
                           "goals", "expenses_history", "inventory", "forecast"})
UNSWITCHED = [case for case in CASES if case.name in POSTGRES_ONLY]
# The managers screen's Silver read left `KS_READ_SILVER` in S14-06 as well,
# but its route reads chain 5's classification first, which raises rather
# than refuses (`CHAIN_FIRST`): it is asked only where that read answers
# (`TestUnderDuckdbOff`, whose fixture makes it).
MANAGERS = next(case for case in CASES if case.name == "managers")

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


def _switches(monkeypatch, env: Dict[str, str]) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)


# ─── A failed engine: a 503 naming the surface ─────────────────────────────

class TestOffRefuses:
    @pytest.mark.parametrize("case", REFUSING, ids=repr)
    def test_a_failed_engine_is_a_503_naming_the_surface(
        self, case, admin_client, stores, monkeypatch, caplog,
    ):
        _switches(monkeypatch, case.env)

        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            response = admin_client.get(case.path, params=case.params)

        assert response.status_code == 503, response.text
        body = response.json()
        assert body["surface"] == case.surface
        assert body["error"] == "Service Unavailable"
        assert "connection refused" not in response.text, (
            "the cause stays in the log; the response is public to the page")
        # Refused, and the refusal counted.
        assert read_fallback.refusals()[case.surface]["count"] >= 1
        # The soak greps for this phrase; a refusal must not say it.
        assert "falling back to DuckDB" not in caplog.text
        assert "read refused" in caplog.text

    def test_the_refusal_is_published_beside_the_mode(
        self, admin_client, stores, monkeypatch,
    ):
        """Under the block's old name, `read_fallback_mode`, which the
        canary judges; since step 14's S14-16 the refusals are all it
        holds."""
        _switches(monkeypatch, {"KS_READ_TRAFFIC": "postgres"})
        assert admin_client.get("/api/traffic/analytics",
                                params=WINDOW).status_code == 503

        health = admin_client.get("/api/health").json()
        assert "read_fallbacks" not in health
        block = health["read_fallback_mode"]
        assert set(block) == {"refused"}
        assert block["refused"]["traffic"]["count"] == 1
        assert block["refused"]["traffic"]["last_at"]
        assert "connection refused" not in str(block)

    def test_the_history_gate_refuses_rather_than_answering_no(
        self, admin_client, stores, monkeypatch,
    ):
        """`backfilled()` is the gate that served DuckDB under words nobody
        grepped for. Its unreadable watermark is a 503 — and so is every call
        in the minute after, which reuses that failure rather than asking
        Postgres again."""
        from core import pg_expenses_read

        _switches(monkeypatch, {"KS_READ_EXPENSES": "postgres"})
        for _ in range(2):
            response = admin_client.get("/api/expenses/summary", params=WINDOW)
            assert response.status_code == 503
            assert response.json()["surface"] == "expenses"
        assert pg_expenses_read._check_failed is not None
        assert read_fallback.refusals()["expenses"]["count"] >= 2

    def test_a_confirmed_history_still_refuses_at_the_router(
        self, admin_client, stores, monkeypatch,
    ):
        """History already confirmed, so the gate passes and the router's own
        read is what fails — refused there, not answered from DuckDB."""
        from core import pg_expenses_read

        monkeypatch.setattr(pg_expenses_read, "_backfilled", True)
        _switches(monkeypatch, {"KS_READ_EXPENSES": "postgres"})
        response = admin_client.get("/api/expenses/summary", params=WINDOW)
        assert response.status_code == 503
        assert response.json()["surface"] == "expenses"

    @pytest.mark.parametrize("params", [
        {"period": "month"},
        {"period": "week"},
        {"start_date": date.today().isoformat(),
         "end_date": (date.today() + timedelta(days=5)).isoformat()},
    ], ids=["month", "week", "future-range"])
    def test_a_refused_forecast_drops_the_overlay_not_the_chart(
        self, params, admin_client, stores, monkeypatch,
    ):
        """The trend's forecast is an overlay, and its reads already degrade
        to "no forecast" on any failure (`get_forecast_data`, and the handler
        around it in the route). A refusal there is one of those failures:
        the chart answers without the overlay, nothing is served from DuckDB,
        and the refusal is still counted — so a broken goals port stays
        visible under `read_fallback_mode.refused`. The forecast's own
        endpoint is not an overlay and answers 503 (the `forecast` case).

        The chart's own series is Gold's in Postgres since step 14's S14-06,
        so it is given an answer here — the pool still raises for the
        forecast's reads, which is what this asks about. The week and a
        future range read the stored forecast as the month does since
        2026-11-04, so the same refusal is theirs."""
        _switches(monkeypatch, {"KS_READ_GOALS": "postgres"})
        monkeypatch.setattr("core.pg_gold_read.fetch_series",
                            AsyncMock(return_value=[]))
        response = admin_client.get("/api/revenue/trend", params={
            **params, "include_forecast": "true"})
        assert response.status_code == 200, response.text
        assert "forecast" not in response.json()
        assert read_fallback.refusals()["goals"]["count"] >= 1

    @pytest.mark.parametrize("value", [None, "duckdb", "junk-s14-05"],
                             ids=["unset", "duckdb", "junk"])
    @pytest.mark.parametrize("case", UNSWITCHED, ids=repr)
    def test_a_switch_it_no_longer_reads_still_asks_postgres(
        self, case, value, admin_client, stores, monkeypatch,
    ):
        """Step 14's S14-05: the six read Postgres whatever their switch
        says, so here — where the pool raises — every value is the same 503
        naming the surface, never DuckDB's answer and never a 500."""
        (switch, _on), = case.env.items()
        if value is None:
            monkeypatch.delenv(switch, raising=False)
        else:
            monkeypatch.setenv(switch, value)
        response = admin_client.get(case.path, params=case.params)
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == case.surface
        assert read_fallback.refusals()[case.surface]["count"] >= 1


# ─── No address: the same refusal ──────────────────────────────────────────

@pytest.fixture
def no_dsn(stores, monkeypatch):
    """`stores`, and then no `KS_PG_DSN` at all: every Postgres switch below
    names an engine this process cannot reach, and nothing will fail."""
    monkeypatch.delenv("KS_PG_DSN", raising=False)
    return stores


class TestNoAddress:
    @pytest.mark.parametrize("case", UNSWITCHED, ids=repr)
    def test_with_no_dsn_the_pool_refuses_and_that_is_the_503(
        self, case, admin_client, monkeypatch, fresh_read_fallback, caplog,
    ):
        """No pool patched and no `KS_PG_DSN`: `core.pg.dsn()` refusing to
        guess a database is the failure the router refuses — the same 503
        naming the surface, with the DSN named in the log beside it. (Until
        step 14's S14-07 a switch asked `no_address` first, and S14-16
        deleted it.)"""
        from core import pg, pg_expenses_read

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setattr(pg, "_pool", None)
        # The expenses' history latch, as `stores` leaves it: never asked. A
        # test before this one may have left it within its recheck window,
        # where it answers without asking the pool (since S14-07 that is
        # the explicit "no history" refusal, not the DSN's).
        monkeypatch.setattr(pg_expenses_read, "_backfilled", False)
        monkeypatch.setattr(pg_expenses_read, "_checked_at", 0.0)
        monkeypatch.setattr(pg_expenses_read, "_check_failed", None)
        _switches(monkeypatch, case.env)

        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            response = admin_client.get(case.path, params=case.params)

        assert response.status_code == 503, response.text
        assert response.json()["surface"] == case.surface
        assert read_fallback.refusals()[case.surface]["count"] >= 1
        assert (f"read refused: {case.surface} — its engine failed") in caplog.text
        assert "KS_PG_DSN is not set" in caplog.text
        assert "falling back to DuckDB" not in caplog.text


# ─── Under KS_DUCKDB=off: DuckDB answers nothing (stage 5, PR-8) ───────────

class TestUnderDuckdbOff:
    """With the file switched off — web's state since step 14's S14-00 — a
    failed engine is a 503 naming the surface, counted as a refusal, and
    nothing reaches for the file: without that each was the tripwire's, a
    503 naming `duckdb` and a CRITICAL page that named no switch. Since step
    14's S14-07 no router reads its switch at all, so what is held is that
    the switch at `duckdb` is not read.

    The managers screen reads the classification before its read switch,
    and that list is chain 5's under off — a chain short of Postgres holds
    `off` back (design section 6) — so here it is answered from Postgres,
    as in the state off runs in, and the switch is what is asked."""

    @pytest.fixture(autouse=True)
    def _chain5_moved(self, monkeypatch):
        from core import pg_managers_read

        # Chain 5 reads Postgres asking nothing since step 14's S14-11
        # (`reads_postgres`, patched here until then, went with the flag).
        monkeypatch.setattr(pg_managers_read, "fetch_all_managers",
                            AsyncMock(return_value=[]))

    @pytest.mark.parametrize("case", UNSWITCHED + [MANAGERS], ids=repr)
    def test_a_switch_it_no_longer_reads_asks_postgres(
        self, case, admin_client, stores, monkeypatch, caplog,
    ):
        """S14-05's six and S14-06's dashboard and managers screen: the
        switch at `duckdb` is not read, the router asks Postgres, and
        Postgres failing is the refusal — nothing reaches for the file (the
        store's connection a trap: since step 14's S14-18 it would be a
        refusal naming `duckdb`, not this tab), and the log names no
        switch."""
        from core import duckdb_store

        def _no_duckdb(self):
            raise AssertionError("the route reached for the DuckDB connection")

        monkeypatch.setattr(duckdb_store.DuckDBStore, "connection", _no_duckdb)
        (switch, _value), = case.env.items()
        monkeypatch.setenv(switch, "duckdb")

        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            response = admin_client.get(case.path, params=case.params)

        assert response.status_code == 503, response.text
        assert response.json()["surface"] == case.surface
        assert read_fallback.refusals()[case.surface]["count"] >= 1
        assert f"{switch}=duckdb" not in caplog.text
        assert "its engine failed" in caplog.text
        assert "duckdb" not in read_fallback.refusals()


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
        response = admin_client.get(COHORTS, params=COHORT_PARAMS)
        assert response.status_code == 503
        assert response.json()["surface"] == "cohorts"
        assert read_fallback.refusals()["cohorts"]["count"] == 1

    @pytest.mark.parametrize("switch", [None, "duckdb", "clickhouse"],
                             ids=["unset", "duckdb", "clickhouse_no_url"])
    def test_under_off_no_live_clickhouse_is_a_503(
        self, switch, admin_client, stores, monkeypatch,
    ):
        """No Postgres body: ClickHouse answers the cohorts or nobody does,
        whatever the switch names."""
        if switch is None:
            monkeypatch.delenv("KS_READ_COHORTS", raising=False)
        else:
            monkeypatch.setenv("KS_READ_COHORTS", switch)
        response = admin_client.get(COHORTS, params=COHORT_PARAMS)
        assert response.status_code == 503
        assert response.json()["surface"] == "cohorts"
        assert read_fallback.refusals()["cohorts"]["count"] == 1

    def test_under_off_a_live_clickhouse_answers(
        self, admin_client, stores, clickhouse, monkeypatch,
    ):
        clickhouse.side_effect = None
        clickhouse.return_value = []
        response = admin_client.get(COHORTS, params=COHORT_PARAMS)
        assert response.status_code == 200, response.text
        assert clickhouse.await_count >= 1
        assert read_fallback.refusals() == {}


# ─── Every GET route: a refusal is a 503 naming its surface ────────────────

# The routes the sweep does not call, each for a reason about the network and
# never about what the route reads. Which routes are swept is read off the app.
NETWORK_ROUTES = {
    "/api/chat/stream": (
        "streams a conversation with the Anthropic API for as long as it "
        "lasts; what the assistant makes of a refusal is DN-20c's"),
}

# The /traffic GET routes that read no store: the backfill's status is the
# process's own dict, and the site's funnel asks Google Analytics
# (`core/ga4.py`). Every other route of the tab refuses when Postgres does.
TRAFFIC_READS_NOTHING = frozenset({"/api/traffic/backfill-utm/status", "/api/traffic/funnel"})

# The dashboard's GET routes (step 14, S14-06): every one reads Postgres
# alone through `core/repositories/revenue.py`'s readers, and refuses when it
# fails.
DASHBOARD_ROUTES = frozenset({
    "/api/summary", "/api/revenue/trend", "/api/sales/by-source",
    "/api/products/top", "/api/products/performance", "/api/brands/analytics",
    "/api/categories/breakdown", "/api/returns", "/api/customers/insights",
})

# The third set's GET routes (step 14, S14-07), beside the goal routes the
# sweep reads off the app by prefix: `/expenses` and `/inventory` by theirs,
# the cohorts and the forecast by name, since they share none. Every one
# reads Postgres alone (the cohorts ClickHouse alone) through its router,
# and refuses when it fails.
EXPENSES_PREFIX = "/api/expense"           # /api/expenses*, /api/expense-types
INVENTORY_PREFIX = "/api/stocks/"
COHORT_ROUTES = frozenset({
    "/api/customers/cohort-retention", "/api/customers/cohort-ltv",
    "/api/customers/purchase-timing", "/api/customers/at-risk",
})
FORECAST_ROUTES = frozenset({"/api/revenue/forecast"})
# The training frame's own route: its reader has no fallback, so a failure is
# its own error, a 500 counted nowhere (S14-07's deviation 7), never a
# refusal. Named, and held to exist and not to refuse.
TRAINING_FRAME_ROUTES = frozenset({"/api/revenue/forecast/evaluate"})

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


class TestEveryRoute:
    """Every GET route under /api, read off the app rather than listed: any
    request that leaves a refusal counted answered 503 naming a surface it
    refused. A handler anywhere between a route and a router that turns the
    refusal into an answer — `except Exception: return {}` in a service — is
    found here whether or not anybody thought to list the route. Swept three
    ways: an engine that fails, no address for it, and an engine that fails
    under `KS_DUCKDB=off` — web's state since step 14's S14-00 — with every
    switch unset. (Until S14-16 the first two set every read switch on its
    engine first; no switch is read since S14-07.)"""

    @pytest.mark.parametrize("state", ["engine_fails", "no_address",
                                       "switches_at_duckdb_off"])
    def test_a_refusal_is_a_503_naming_its_surface(
        self, state, sweep_client, stores, no_network, monkeypatch, request,
    ):
        from core import ch_cohorts
        from web.main import app
        from web.ratelimit import limiter

        if state == "switches_at_duckdb_off":
            # Every switch unset, as `stores` leaves them. KS_DUCKDB=off was
            # set here, the way web read it, until step 14's S14-18 deleted
            # the switch: nothing reads it since.
            monkeypatch.setenv("KS_DUCKDB", "off")
        elif state == "engine_fails":
            monkeypatch.setenv("KS_CH_URL", "http://nowhere.invalid:8123")
            monkeypatch.setattr(ch_cohorts, "fetch",
                                AsyncMock(side_effect=OSError("clickhouse down")))
        else:
            monkeypatch.delenv("KS_PG_DSN", raising=False)
            monkeypatch.delenv("KS_CH_URL", raising=False)

        routes = swept_routes(app)
        refused_routes, violations = [], []
        for endpoint in routes:
            read_fallback.reset_counts()
            limiter.reset()
            path, params = _request_for(endpoint)
            response = sweep_client.get(path, params=params)
            refused = read_fallback.refusals()
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
        # Every goal route refuses, the three calculator GETs included: their
        # history goes through the router — Silver, the only history since
        # chain 7b-4. Read off the app by prefix, never listed. Mutation M18:
        # read a calculator's history on the store's own connection again
        # (the deleted bridge's way), and its route answers from DuckDB —
        # named here.
        # `GET /api/goals` itself included since step 14's S14-07: it reads
        # chain 7a's table first, which asked Postgres with no refusal from
        # S14-00 (`CHAIN_FIRST`) until `_goals_run` refused it too.
        goal_routes = {e.path for e in routes if e.path.startswith("/api/goals")}
        assert {"/api/goals", "/api/goals/seasonality", "/api/goals/growth",
                "/api/goals/weekly-patterns", "/api/goals/smart",
                "/api/goals/forecast", "/api/goals/history"} <= goal_routes, goal_routes
        assert goal_routes <= set(refused_routes), (
            f"goal routes not refused: {sorted(goal_routes - set(refused_routes))}")
        # Every /traffic route that reads refuses too, in every state: since
        # S14-05 the tab reads Postgres alone, whatever its switch says, and
        # the pool raises here. Read off the app by prefix, so a route added
        # to the tab is held the day it lands — the pixel-only breakdown was
        # one. What this adds to the violations above: a reader calling
        # Postgres round the router counts no refusal, so its uncounted 500
        # was invisible there. Mutation: `await pg_traffic_read.fetch(...)`
        # in `get_traffic_pixel_only` with no `try`.
        traffic_routes = {e.path for e in routes if e.path.startswith("/api/traffic/")}
        assert TRAFFIC_READS_NOTHING <= traffic_routes, "a named route is gone"
        assert not TRAFFIC_READS_NOTHING & set(refused_routes), (
            "a route said to read nothing was refused")
        traffic_routes -= TRAFFIC_READS_NOTHING
        assert len(traffic_routes) >= 7, traffic_routes
        assert traffic_routes <= set(refused_routes), (
            f"traffic routes not refused: {sorted(traffic_routes - set(refused_routes))}")
        # The dashboard's routes the same way, since S14-06 put them on
        # Postgres alone. Listed, because the dashboard shares no prefix; each
        # must be on the app. `/api/managers` is not among them: it reads
        # chain 5's classification first, which raises rather than refuses
        # (`CHAIN_FIRST`). Mutation: `await pg_dashboard_read.fetch(...)` in
        # `get_sales_by_source` with no `try`.
        assert DASHBOARD_ROUTES <= {e.path for e in routes}, (
            f"a named route is gone: {sorted(DASHBOARD_ROUTES - {e.path for e in routes})}")
        assert DASHBOARD_ROUTES <= set(refused_routes), (
            f"dashboard routes not refused: {sorted(DASHBOARD_ROUTES - set(refused_routes))}")
        # The third set's the same way, since S14-07 left each with one
        # engine: `/expenses` and `/inventory` read off the app by prefix,
        # so a route added to either is held the day it lands; the cohorts
        # and the forecast by name. What this adds to the violations above,
        # as for /traffic: a reader calling its engine round the router
        # counts no refusal, so its uncounted 500 was invisible there.
        # Mutations: `await pg_inventory_read.fetch_many(...)` in
        # `get_stock_summary`, `await ch_cohorts.fetch(...)` in
        # `get_cohort_ltv`, each with no `try`. (One in
        # `get_cohort_retention` passes here: the route's default asks the
        # enhanced form. `TestCohorts` and the walk in
        # `test_tab_routers_postgres_only.py` see that one.)
        on_app = {e.path for e in routes}
        third = {p for p in on_app if p.startswith((EXPENSES_PREFIX, INVENTORY_PREFIX))}
        assert len({p for p in third if p.startswith(EXPENSES_PREFIX)}) >= 4, third
        assert len({p for p in third if p.startswith(INVENTORY_PREFIX)}) >= 10, third
        named = COHORT_ROUTES | FORECAST_ROUTES | TRAINING_FRAME_ROUTES
        assert named <= on_app, f"a named route is gone: {sorted(named - on_app)}"
        third |= COHORT_ROUTES | FORECAST_ROUTES
        assert third <= set(refused_routes), (
            f"S14-07's routes not refused: {sorted(third - set(refused_routes))}")
        assert not TRAINING_FRAME_ROUTES & set(refused_routes), (
            "the training frame's route was refused: its reader has no fallback")

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
    consumers no HTTP request waits for and by the assistant's two routes,
    which answer inside the conversation (`IN_BAND_ROUTES`) — which
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
