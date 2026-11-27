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
  lookups and the managers screen — and the refusal counted and published
  under `read_fallback_mode.refused`;
- the trend's forecast overlay, whose reads already degrade to "no
  forecast", drops under a refusal while the chart answers, and the refusal
  is counted — while the forecast's own endpoint, `/api/revenue/forecast`,
  answers 503 like any route rather than "Forecast not available yet";
- the cohorts are answered by a live ClickHouse or not at all;
- **every GET route under `/api/`, read off the app** (`TestEveryRoute`):
  with the engine failing, or with no address for it, any request that left
  a refusal counted answered 503 naming it, and every route of the tabs
  that read a store did so — never a 500, never an answer; and the goal
  page's three writes and the model's tuning, none of them a GET, the same
  way.

That sweep replaced the static walks of the code that stood beside it until
2026-11 (a handler naming the refusal must raise it; nothing in a tab's
module may reach an engine round its router): it finds the same defects by
what the routes answer, so a rewrite that answers the same passes it.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Dict, FrozenSet, List, Optional, Union
from unittest.mock import AsyncMock

import pytest

from core import read_fallback
from core.pg import get_pool as REAL_GET_POOL
from tests.helpers.route_sweep import (  # noqa: F401
    _request_for, no_network, stores, sweep_client, swept_routes,
)

DAY = date.today() - timedelta(days=3)
WINDOW = {"start_date": (DAY - timedelta(days=6)).isoformat(),
          "end_date": DAY.isoformat()}


class Case:
    """One route, and the surface a failure there is counted against."""

    def __init__(self, name: str, surface: str, path: str,
                 params: Optional[dict] = None):
        self.name, self.surface = name, surface
        self.path, self.params = path, dict(params or {})

    def __repr__(self) -> str:  # pytest ids
        return self.name


# The routers DN-20b names, one route each (two for the dashboard, which the
# plan names by its two primitives). Postgres-backed only; the cohorts, which
# are ClickHouse's, have their own class below.
CASES: List[Case] = [
    Case("summary", "dashboard",
         "/api/summary", WINDOW),
    Case("trend", "dashboard",
         "/api/revenue/trend", WINDOW),
    Case("goals", "goals", "/api/goals"),
    Case("traffic", "traffic",
         "/api/traffic/analytics", WINDOW),
    Case("expenses_history", "expenses",
         "/api/expenses/summary", WINDOW),
    Case("margin", "margin",
         "/api/margin/overview", WINDOW),
    Case("products_intel", "products_intel",
         "/api/products/intel/summary", WINDOW),
    Case("inventory", "inventory",
         "/api/stocks/summary"),
    # The forecast's own endpoint, which the frontend calls for the last
    # forecast date. Its reads are the goals router's; a refusal there used
    # to come back as 200 "Forecast not available yet" — an outage read as
    # an untrained model. The trend's overlay is the other door to the same
    # reads, and deliberately drops instead (below).
    Case("forecast", "goals",
         "/api/revenue/forecast"),
    # Beyond DN-20b's list: every other router behind an HTTP route refuses
    # too. Each is here to prove that nothing between it and the handler
    # turns the refusal into a 500 or a quiet answer — the filter bar's
    # lookups included, which are refused on every tab at once, exactly like
    # the tab they sit on.
    Case("reports", "reports",
         "/api/reports/summary", WINDOW),
    Case("marketing", "marketing",
         "/api/promocodes/analytics", WINDOW),
    Case("insights", "dashboard",
         "/api/customers/insights", WINDOW),
    Case("lookups", "lookups",
         "/api/categories"),
    Case("managers", "managers",
         "/api/managers"),
]

# Routes whose first read is a write chain's table. Since step 14's S14-00
# every chain writes Postgres — production's state since those chains' flips —
# and a read of its table asks Postgres with no fallback and no refusal, so a
# failed engine is that route's 500, not a 503: chain 5's classification
# behind the managers screen, which the refusal test answers so that the
# Silver read behind it is the one refused. Chain 7a's goals were the other
# until S14-07, whose `_goals_run` refuses every failure, theirs included.
CHAIN_FIRST = frozenset({"managers"})

COHORTS = "/api/customers/cohort-retention"
COHORT_PARAMS = {"include_revenue": "false"}


# ─── A failed engine: a 503 naming the surface ─────────────────────────────

class TestOffRefuses:
    @pytest.mark.parametrize("case", CASES, ids=repr)
    def test_a_failed_engine_is_a_503_naming_the_surface(
        self, case, admin_client, stores, monkeypatch, caplog,
    ):
        if case.name in CHAIN_FIRST:
            # The managers screen reads chain 5's classification first, which
            # raises rather than refuses: answered here, so the Silver read
            # behind it is the one asked.
            from core import pg_managers_read

            monkeypatch.setattr(pg_managers_read, "fetch_all_managers",
                                AsyncMock(return_value=[]))

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
        # `read_refused`'s lever greps the log for this phrase.
        assert "read refused" in caplog.text

    def test_the_refusal_is_published_beside_the_mode(
        self, admin_client, stores, monkeypatch,
    ):
        """Under the block's old name, `read_fallback_mode`, which the
        canary judges; since step 14's S14-16 the refusals are all it
        holds."""
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
        to "no forecast" on any failure (the handler around
        `get_range_forecast` in the route). A refusal there is one of those failures:
        the chart answers without the overlay, nothing is served from DuckDB,
        and the refusal is still counted — so a broken goals port stays
        visible under `read_fallback_mode.refused`. The forecast's own
        endpoint is not an overlay and answers 503 (the `forecast` case).

        The chart's own series is Gold's in Postgres since step 14's S14-06,
        so it is given an answer here — the pool still raises for the
        forecast's reads, which is what this asks about. The week and a
        future range read the stored forecast as the month does since
        2026-11-04, so the same refusal is theirs."""
        monkeypatch.setattr("core.pg_gold_read.fetch_series",
                            AsyncMock(return_value=[]))
        response = admin_client.get("/api/revenue/trend", params={
            **params, "include_forecast": "true"})
        assert response.status_code == 200, response.text
        assert "forecast" not in response.json()
        assert read_fallback.refusals()["goals"]["count"] >= 1


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

    def test_under_off_no_live_clickhouse_is_a_503(self, admin_client, stores):
        """No Postgres body: ClickHouse answers the cohorts or nobody does."""
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

# The tabs whose every read goes through a router that refuses, and the GET
# routes each owns: read off the app by prefix, or by name where the tab
# shares none. With the engine failing, each of these answers 503 naming what
# was refused — never a 500, never an answer. A reader that calls its engine
# round its router counts no refusal and answers 500; a handler that catches
# the refusal and answers anyway leaves it counted behind a 200. Both are
# found here by what the route answers, however the code is written.
DASHBOARD_ROUTES = frozenset({
    "/api/summary", "/api/revenue/trend", "/api/sales/by-source",
    "/api/products/top", "/api/products/performance", "/api/brands/analytics",
    "/api/categories/breakdown", "/api/returns", "/api/customers/insights",
})
COHORT_ROUTES = frozenset({
    "/api/customers/cohort-retention", "/api/customers/cohort-ltv",
    "/api/customers/purchase-timing", "/api/customers/at-risk",
})
# The header's dropdowns, on every page.
FILTER_ROUTES = frozenset({
    "/api/categories", "/api/categories/{parent_id}/children", "/api/brands",
    "/api/promocodes",
})
TABS: Dict[str, Union[str, FrozenSet[str]]] = {
    "dashboard": DASHBOARD_ROUTES,
    # The stored forecast, and the model's evaluation over its training frame.
    "forecast": frozenset({"/api/revenue/forecast", "/api/revenue/forecast/evaluate"}),
    "traffic": "/api/traffic/",
    "margin": "/api/margin/",
    "products intelligence": "/api/products/intel/",
    "reports": "/api/reports/",
    "marketing": "/api/promocodes/",
    "expenses": "/api/expense",           # /api/expenses*, /api/expense-types
    "inventory": "/api/stocks/",
    "goals": "/api/goals",
    "promotions calendar": "/api/promotions",
    "cohorts": COHORT_ROUTES,
    "filters": FILTER_ROUTES,
}
# The /traffic routes that read no store: the backfill's status is the
# process's own dict, and the site's funnel asks Google Analytics.
READS_NO_STORE = frozenset({"/api/traffic/backfill-utm/status", "/api/traffic/funnel"})


def _owned(spec: Union[str, FrozenSet[str]], paths) -> set:
    if isinstance(spec, str):
        return {p for p in paths if p.startswith(spec)}
    return {p for p in paths if p in spec}


def _surface(response) -> Optional[str]:
    try:
        body = response.json()
    except ValueError:  # a CSV, an HTML page
        return None
    return body.get("surface") if isinstance(body, dict) else None


@pytest.fixture
def engine_down(request, stores, no_network, monkeypatch):
    """Postgres failing, ClickHouse failing for the cohorts, nothing leaving
    the process — in one of two ways: the engine fails (the pool raises), or
    there is no address for it (no `KS_PG_DSN`, no `KS_CH_URL`, and the real
    pool refusing to guess a database)."""
    from core import ch_cohorts, pg

    if request.param == "engine_fails":
        monkeypatch.setenv("KS_CH_URL", "http://nowhere.invalid:8123")
        monkeypatch.setattr(ch_cohorts, "fetch",
                            AsyncMock(side_effect=OSError("clickhouse down")))
    else:
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.delenv("KS_CH_URL", raising=False)
        monkeypatch.setattr(pg, "get_pool", REAL_GET_POOL)
        monkeypatch.setattr(pg, "_pool", None)
    return request.param


ENGINE_DOWN = pytest.mark.parametrize(
    "engine_down", ["engine_fails", "no_address"], indirect=True)


class TestEveryRoute:
    """Every GET route under /api, read off the app rather than listed, so a
    route added tomorrow is swept the day it is registered."""

    @ENGINE_DOWN
    def test_a_refusal_is_a_503_naming_its_surface(self, engine_down, sweep_client):
        from web.main import app
        from web.ratelimit import limiter

        routes = swept_routes(app)
        on_app = {e.path for e in routes}
        must_refuse = set()
        for tab, spec in TABS.items():
            owned = _owned(spec, on_app) - READS_NO_STORE
            assert owned, f"no route of the {tab} tab is on the app"
            must_refuse |= owned

        violations = []
        for endpoint in routes:
            read_fallback.reset_counts()
            limiter.reset()
            path, params = _request_for(endpoint)
            response = sweep_client.get(path, params=params)
            refused = read_fallback.refusals()
            surface = _surface(response)
            if refused and (response.status_code != 503 or surface not in refused):
                violations.append(
                    f"{endpoint.path} answered {response.status_code} "
                    f"(surface={surface!r}) with {sorted(refused)} refused")
            elif not refused and endpoint.path in must_refuse:
                violations.append(
                    f"{endpoint.path} answered {response.status_code} and refused nothing")
        assert not violations, "; ".join(violations)

    @ENGINE_DOWN
    @pytest.mark.parametrize("method,path,params", [
        ("POST", "/api/goals", {"period_type": "monthly", "amount": "1000"}),
        ("POST", "/api/goals/recalculate", {}),
        ("DELETE", "/api/goals/monthly", {}),
    ], ids=["set", "recalculate", "reset"])
    def test_the_goal_writes_are_refused_too(
        self, engine_down, sweep_client, method, path, params,
    ):
        """The goal page's three writes, which the GET sweep does not make:
        each reads through the goals router before it writes, and a failed
        engine is the same 503 naming `goals` — never a 500."""
        from web.ratelimit import limiter

        read_fallback.reset_counts()
        limiter.reset()
        response = sweep_client.request(method, path, params=params)
        assert response.status_code == 503, response.text
        assert _surface(response) == "goals"
        assert read_fallback.refusals()["goals"]["count"] >= 1

    @ENGINE_DOWN
    def test_the_models_tuning_is_refused_too(self, engine_down, sweep_client):
        """`evaluate`'s twin, a POST the sweep does not make: it reads the
        same training frame, and a failed engine is the same 503 naming
        `forecast`, never a 500."""
        from web.ratelimit import limiter

        read_fallback.reset_counts()
        limiter.reset()
        response = sweep_client.post("/api/revenue/forecast/tune")
        assert response.status_code == 503, response.text
        assert _surface(response) == "forecast"
        assert read_fallback.refusals()["forecast"]["count"] == 1
