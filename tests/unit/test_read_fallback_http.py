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
  the page, and the refusal is counted;
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
        DuckDB's numbers, and none reach the page."""
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


# ─── No HTTP path swallows a refusal ───────────────────────────────────────

# Where a route's read runs: the routers, the Postgres readers, and web/. The
# non-HTTP consumers (the reports, the assistant, training, the sync) are
# DN-20c's and may turn a refusal into their own named answer.
HTTP_SCOPE = ("core/repositories", "web")


def _http_files() -> List[pathlib.Path]:
    files = []
    for top in HTTP_SCOPE:
        files.extend(p for p in sorted((REPO / top).rglob("*.py"))
                     if "frontend" not in p.parts)
    files.extend(sorted((REPO / "core").glob("pg_*read*.py")))
    return files


def _names_read_unavailable(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return False
    return any(
        (isinstance(n, ast.Name) and n.id == "ReadUnavailable")
        or (isinstance(n, ast.Attribute) and n.attr == "ReadUnavailable")
        for n in ast.walk(handler.type))


def swallowing_handlers(source: str) -> List[int]:
    """Lines of every handler naming `ReadUnavailable` that can end without
    raising. Pure, so it can be shown synthetic code."""
    bad = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ExceptHandler) and _names_read_unavailable(node):
            if not node.body or not isinstance(node.body[-1], ast.Raise):
                bad.append(node.lineno)
    return bad


class TestNoHandlerSwallowsARefusal:
    def test_every_handler_naming_it_raises(self):
        """A handler that names the refusal and does not raise it has turned
        a 503 into an answer — the DuckDB one, or a quiet partial one. The
        walk in `test_read_fallback_sites.py` accepts naming the refusal as
        a decision; on an HTTP path the only decision is to let it through."""
        bad = []
        for path in _http_files():
            for line in swallowing_handlers(path.read_text(encoding="utf-8")):
                bad.append(f"{path.relative_to(REPO).as_posix()}:{line}")
        assert not bad, "a refusal swallowed on an HTTP path: " + ", ".join(bad)

    def test_it_is_looking(self):
        """Non-vacuity: the one handler that names it today is found, and it
        is found raising."""
        goals = (REPO / "core/repositories/goals.py").read_text(encoding="utf-8")
        tree = ast.parse(goals)
        assert any(isinstance(n, ast.ExceptHandler) and _names_read_unavailable(n)
                   for n in ast.walk(tree))
        assert REPO / "core/repositories/goals.py" in _http_files()

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
