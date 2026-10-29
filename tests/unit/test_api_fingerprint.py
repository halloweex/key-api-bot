"""deploy/api_fingerprint.py, run against a fake dashboard (step 14, S14-02).

The script is how a read PR of step 14 shows production answered the same
before and after its deploy, and it carries an admin session that logout does
not revoke. So it is run here the way the owner runs it — a separate python3
process, over real HTTP — against a fake ASGI app served by uvicorn on a
loopback port, which records every request it receives. What is held:

* it asks GETs only, each for an absolute window that ended at least 40 days
  ago, of routes the real app serves with the query parameters it declares;
* the session goes in the cookie and nowhere else: never in a URL, never
  printed, never in the snapshot, never after a redirect, never over plain
  http to another machine;
* `compare` says which routes moved, and its exit code tells a past window
  from a live route;
* it refuses — asking nothing, writing nothing — whatever it cannot do safely.
"""
from __future__ import annotations

import ast
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import parse_qsl

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "deploy" / "api_fingerprint.py"
SECRET = "eyJ1c2VyIjoxfQ.ZzZz.never-print-this-token"
TODAY = "2026-10-21"


def _load():
    import importlib.util

    spec = importlib.util.spec_from_file_location("api_fingerprint_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fp = _load()


class FakeDashboard:
    """An ASGI app that answers every path with a body made of the path, the
    query and a per-path version a test can bump, and records each request.
    A request without the right cookie is a 401, as the real app's is."""

    def __init__(self):
        self.requests = []
        self.version = {}
        self.status = {}
        self.redirect = {}

    async def __call__(self, scope, receive, send):
        assert scope["type"] == "http"
        headers = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
        path, query = scope["path"], scope["query_string"].decode()
        self.requests.append({"method": scope["method"], "path": path, "query": query,
                              "headers": headers})
        if headers.get("cookie") != f"{fp.SESSION_COOKIE}={SECRET}":
            status, extra, body = 401, [], b'{"detail":"Not authenticated"}'
        elif path in self.redirect:
            status, extra, body = 302, [(b"location", self.redirect[path].encode())], b""
        else:
            status, extra = self.status.get(path, 200), []
            body = json.dumps({"path": path, "query": query,
                               "v": self.version.get(path, 0)}).encode()
        await send({"type": "http.response.start", "status": status,
                    "headers": [(b"content-type", b"application/json"), *extra]})
        await send({"type": "http.response.body", "body": body})


@pytest.fixture
def dashboard():
    uvicorn = pytest.importorskip("uvicorn")
    app = FakeDashboard()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, lifespan="off",
                                           log_level="error", access_log=False))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 15
    while not server.started:
        assert time.monotonic() < deadline, "the fake dashboard did not start"
        time.sleep(0.01)
    app.base = f"http://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"
    yield app
    server.should_exit = True
    thread.join(10)


def run(*args, session=SECRET, cwd=None):
    env = {k: v for k, v in os.environ.items() if k != fp.SESSION_ENV}
    if session is not None:
        env[fp.SESSION_ENV] = session
    done = subprocess.run([sys.executable, str(SCRIPT), *map(str, args)], env=env,
                          capture_output=True, text=True, timeout=120, cwd=cwd)
    return done.returncode, done.stdout + done.stderr


def snap(dashboard, out, *extra, session=SECRET):
    return run("snap", "--base", dashboard.base, "--out", out, "--today", TODAY,
               "--pace", "0", *extra, session=session)


# ── a snapshot ────────────────────────────────────────────────────────────────

class TestASnapshot:
    def test_it_asks_every_route_once_with_get_and_the_cookie(self, dashboard, tmp_path):
        code, out = snap(dashboard, tmp_path / "a.json")
        assert code == 0, out
        table = fp.routes(date(2026, 8, 1), date(2026, 8, 31))
        assert [r["path"] + ("?" + r["query"] if r["query"] else "")
                for r in dashboard.requests] == [r["url"] for r in table]
        for req in dashboard.requests:
            assert req["method"] == "GET"
            assert req["headers"]["cookie"] == f"dashboard_session={SECRET}"
            assert req["headers"]["user-agent"] == fp.USER_AGENT

    def test_the_default_window_is_the_last_whole_month_forty_days_back(self, dashboard,
                                                                         tmp_path):
        """21 October: 40 days back is 11 September, so August. Mutation:
        count 30 days, or end the window on the 40th day."""
        code, out = snap(dashboard, tmp_path / "a.json")
        assert code == 0, out
        shot = json.loads((tmp_path / "a.json").read_text())
        assert shot["window"] == {"start": "2026-08-01", "end": "2026-08-31", "today": TODAY}
        dated = 0
        for req in dashboard.requests:
            params = dict(parse_qsl(req["query"]))
            assert "period" not in params, req          # "this month" moves
            for key in ("start_date", "end_date", "period_from", "period_to"):
                if key in params:
                    dated += 1
                    assert "2026-08-01" <= params[key] <= "2026-08-31", req
        assert dated > 60

    def test_the_session_is_nowhere_but_the_cookie(self, dashboard, tmp_path):
        """Not in a URL, not in the output, not in the file. Mutation: print
        the headers, or write them into the snapshot."""
        code, out = snap(dashboard, tmp_path / "a.json")
        assert code == 0, out
        assert SECRET not in out
        assert SECRET not in (tmp_path / "a.json").read_text()
        assert not [r for r in dashboard.requests if SECRET in r["path"] + r["query"]]

    def test_it_keeps_hashes_and_sizes_never_bodies(self, dashboard, tmp_path):
        code, _ = snap(dashboard, tmp_path / "a.json")
        shot = json.loads((tmp_path / "a.json").read_text())
        row = shot["routes"][0]
        assert set(row) == {"name", "url", "kind", "status", "sha256", "bytes"}
        assert '"path"' not in (tmp_path / "a.json").read_text().replace('"url"', "")
        assert shot["base"] == dashboard.base

    def test_a_route_that_does_not_answer_200_is_named_and_the_file_written(self, dashboard,
                                                                             tmp_path):
        dashboard.status["/api/margin/trend"] = 503
        code, out = snap(dashboard, tmp_path / "a.json")
        assert code == 1, out
        assert "margin_trend: 503" in out
        assert (tmp_path / "a.json").exists()

    def test_a_wrong_session_reads_as_401_and_says_nothing_of_it(self, dashboard, tmp_path):
        code, out = snap(dashboard, tmp_path / "a.json", session="not-it")
        assert code == 1 and "summary: 401" in out, out
        assert "not-it" not in out and "not-it" not in (tmp_path / "a.json").read_text()

    def test_a_redirect_is_recorded_and_never_followed(self, dashboard, tmp_path):
        """A 3xx is an answer to record. Followed, it would carry the cookie
        to wherever the Location points. Mutation: drop `_NoRedirect`."""
        dashboard.redirect["/api/summary"] = dashboard.base + "/steal"
        code, out = snap(dashboard, tmp_path / "a.json")
        assert code == 1 and "summary: 302" in out, out
        assert not [r for r in dashboard.requests if r["path"] == "/steal"]


# ── refusals: nothing asked, nothing written ──────────────────────────────────

class TestRefusals:
    @pytest.mark.parametrize("args, session, words", [
        ((), None, "KS_FINGERPRINT_SESSION is not set"),
        ((), "two\nlines", "characters a cookie value cannot carry"),
        ((), "a;b=c", "characters a cookie value cannot carry"),
        (("--start", "2026-09-01", "--end", "2026-09-30"), SECRET, "under 40 days before"),
        (("--start", "2026-08-31", "--end", "2026-08-01"), SECRET, "starts after it ends"),
        (("--start", "2026-08-01"), SECRET, "--start and --end go together"),
    ], ids=["no-session", "newline", "separator", "recent-window", "backwards", "half-window"])
    def test_it_refuses_before_asking_anything(self, dashboard, tmp_path, args, session, words):
        code, out = snap(dashboard, tmp_path / "a.json", *args, session=session)
        assert code == 2 and words in out, out
        assert dashboard.requests == [] and not (tmp_path / "a.json").exists()
        if session:
            assert session not in out

    @pytest.mark.parametrize("base, words", [
        ("http://198.51.100.7", "plain http to a host that is not this machine"),
        ("https://user:pw@example.invalid", "carries credentials"),
        ("https://example.invalid/api", "the origin alone"),
        ("ftp://example.invalid", "must be http(s)"),
    ])
    def test_it_sends_the_session_only_where_it_is_safe(self, tmp_path, base, words):
        code, out = run("snap", "--base", base, "--out", tmp_path / "a.json", "--today", TODAY)
        assert code == 2 and words in out, out
        assert not (tmp_path / "a.json").exists() and SECRET not in out

    def test_today_in_the_future_licenses_no_recent_window(self, dashboard, tmp_path):
        """`--today` picks the default window and nothing else: a window that
        ends inside the last 40 days of the real calendar is refused whatever
        `--today` says. Mutation: judge the window against `--today` alone."""
        end = fp.kyiv_today() - timedelta(days=5)
        start = end - timedelta(days=5)
        code, out = run("snap", "--base", dashboard.base, "--out", tmp_path / "a.json",
                        "--today", "2099-01-01", "--start", start.isoformat(),
                        "--end", end.isoformat(), "--pace", "0")
        assert code == 2 and "under 40 days before" in out, out
        assert dashboard.requests == [] and not (tmp_path / "a.json").exists()

    def test_a_snapshot_is_never_written_over(self, dashboard, tmp_path):
        """A "before" overwritten by an "after" leaves nothing to compare.
        Mutation: open the file with "w"."""
        target = tmp_path / "before.json"
        target.write_text("the before snapshot")
        code, out = snap(dashboard, target)
        assert code == 2 and "never written over" in out, out
        assert target.read_text() == "the before snapshot" and dashboard.requests == []

    def test_list_asks_nothing(self, tmp_path):
        code, out = run("list", "--today", TODAY, session=None)
        assert code == 0, out
        assert "window 2026-08-01 .. 2026-08-31" in out and "/api/summary?" in out

    def test_it_runs_as_its_header_shows(self):
        """By its path, not through an interpreter: that is how the header,
        CLAUDE.md and the PR's host steps write every command, and the host
        gets the file's mode from git. Committed 100644 it answered
        `permission denied`. Mutation: `chmod -x deploy/api_fingerprint.py`."""
        if not shutil.which("python3"):
            pytest.skip("no python3 on PATH for the shebang")
        env = {k: v for k, v in os.environ.items() if k != fp.SESSION_ENV}
        done = subprocess.run([str(SCRIPT), "list", "--today", TODAY], env=env,
                              capture_output=True, text=True, timeout=60)
        assert done.returncode == 0, done.stdout + done.stderr
        assert "window 2026-08-01 .. 2026-08-31" in done.stdout


# ── compare ───────────────────────────────────────────────────────────────────

class TestCompare:
    def _pair(self, dashboard, tmp_path, change=None):
        assert snap(dashboard, tmp_path / "before.json")[0] == 0
        if change:
            change(dashboard)
        snap(dashboard, tmp_path / "after.json")
        return run("compare", tmp_path / "before.json", tmp_path / "after.json", session=None)

    def test_nothing_moved_is_zero(self, dashboard, tmp_path):
        code, out = self._pair(dashboard, tmp_path)
        assert code == 0 and "every route answered the same" in out, out

    def test_a_past_window_that_moved_is_a_finding(self, dashboard, tmp_path):
        code, out = self._pair(dashboard, tmp_path,
                               lambda d: d.version.__setitem__("/api/summary", 1))
        assert code == 1, out
        moved = out.split("MOVED")[1]
        assert "summary: answer moved" in moved and "summary_all: answer moved" in moved
        assert "revenue_trend" not in moved

    def test_a_live_route_alone_is_a_question(self, dashboard, tmp_path):
        """No window in the API: an order between the runs moves it. Exit 3,
        not 1. Mutation: count live routes as findings."""
        code, out = self._pair(dashboard, tmp_path,
                               lambda d: d.version.__setitem__("/api/customers/cohort-retention", 1))
        assert code == 3, out
        assert "MOVED" not in out and "cohort_retention: answer moved" in out

    def test_a_status_change_is_a_finding_even_on_a_live_route(self, dashboard, tmp_path):
        code, out = self._pair(dashboard, tmp_path,
                               lambda d: d.status.__setitem__("/api/stocks/summary", 503))
        assert code == 1 and "stocks_summary: status 200 -> 503" in out, out

    def test_two_windows_are_not_comparable(self, dashboard, tmp_path):
        assert snap(dashboard, tmp_path / "a.json")[0] == 0
        assert snap(dashboard, tmp_path / "b.json", "--start", "2026-07-01",
                    "--end", "2026-07-31")[0] == 0
        code, out = run("compare", tmp_path / "a.json", tmp_path / "b.json", session=None)
        assert code == 2 and "not comparable: windows" in out, out

    def test_a_file_that_is_not_a_snapshot_is_not_comparable(self, tmp_path):
        (tmp_path / "x.json").write_text("{}")
        code, out = run("compare", tmp_path / "x.json", tmp_path / "x.json", session=None)
        assert code == 2 and "not comparable" in out, out


# ── the table against the application ─────────────────────────────────────────

def test_every_route_is_a_get_the_app_serves_with_parameters_it_declares():
    """A 404 or a 422 fingerprints an error page: every route in the table is
    a GET route of the real app, and every query parameter one it reads —
    directly or through a dependency. Mutation: misspell a path or a
    parameter."""
    from tests.routes_helper import iter_endpoints, query_param_names
    from web.main import app

    # Through the helper, never `app.routes`: on fastapi 0.141 that list holds
    # the included routers, not their routes, and `get_flat_dependant` is not
    # exported (this test failed in CI and the gate on exactly that).
    served = {e.path: query_param_names(e) for e in iter_endpoints(app) if "GET" in e.methods}
    for row in fp.routes(date(2026, 8, 1), date(2026, 8, 31)):
        path, _, query = row["url"].partition("?")
        assert path in served, row
        assert {k for k, _ in parse_qsl(query)} <= served[path], (row, served[path])
        if row["kind"] == fp.ABSOLUTE:
            params = dict(parse_qsl(query))
            assert {"start_date", "end_date"} <= set(params) or {"year", "month"} <= set(params), row


MARGIN_ROUTES = ("margin_overview", "margin_by_brand", "margin_by_category", "margin_trend",
                 "margin_brand_category", "margin_alerts")


def test_a_route_that_counts_from_today_is_live():
    """The smart goals take a month, but one signal is the last three
    complete months counted from today, and another the stored ML forecast;
    the customer insights take a window and answer all-time figures beside
    it; the margin routes take one and cost it at today's purchase price;
    the stock pages and the cohorts take no window at all. A moved answer
    there is a question, not a finding. Mutation: mark the goals absolute
    because they carry `year` and `month`, or the insights or a margin route
    because they carry `start_date` and `end_date`."""
    kinds = {r["name"]: r["kind"] for r in fp.routes(date(2026, 8, 1), date(2026, 8, 31))}
    for name in ("goals_smart", "goals_forecast", "cohort_retention", "stocks_summary",
                 "sms_audience", "categories", "customers_insights", *MARGIN_ROUTES):
        assert kinds[name] == fp.LIVE, name
    assert kinds["summary"] == kinds["revenue_trend"] == fp.ABSOLUTE


def test_the_insights_are_live_because_they_answer_all_time_figures():
    """The reason `customers_insights` is live, read from the statements it
    runs: the lifetime-value and repeat-rate figures read every order in
    Silver with no date bound, so an order today moves an August answer.
    If they are ever bounded by the window, this fails — mark the route
    absolute again then. Mutation: give either statement a date predicate
    without revisiting the kind."""
    from core.repositories import customers

    for sql in (customers._INSIGHTS_CLV_SQL, customers._INSIGHTS_ALLTIME_SQL):
        body = sql.replace("{sales_where}", "")
        assert "{silver_orders}" in body
        assert "order_date" not in body and "?" not in body, body
    window = {"start_date", "end_date"}
    [row] = [r for r in fp.routes(date(2026, 8, 1), date(2026, 8, 31))
             if r["name"] == "customers_insights"]
    assert window <= {k for k, _ in parse_qsl(row["url"].partition("?")[2])}
    assert row["kind"] == fp.LIVE


def test_every_margin_route_is_live_while_margin_costs_at_todays_price():
    """Every /api/margin route the table asks joins the window's lines to
    `{offer_stocks}` by SKU — today's `purchased_price`, which chain 1's
    hourly stock step upserts — so a price change between two runs moves an
    August margin. Read from the repository, so a margin route added to the
    table is held too. Mutation: mark one absolute, or add a seventh margin
    route as absolute."""
    text = (REPO / "core" / "repositories" / "margin.py").read_text()
    assert "LEFT JOIN {{offer_stocks}} os ON l.sku = os.sku" in text
    upsert = (REPO / "core" / "pg_inventory_write.py").read_text()
    assert '"bronze.offer_stocks"' in upsert and '"purchased_price"' in upsert
    margin = [r for r in fp.routes(date(2026, 8, 1), date(2026, 8, 31))
              if r["url"].startswith("/api/margin/")]
    assert sorted(r["name"] for r in margin) == sorted(MARGIN_ROUTES)
    assert {r["kind"] for r in margin} == {fp.LIVE}


def test_the_cookie_is_the_one_the_app_reads():
    """Spelled in the script because the host cannot import the app."""
    tree = ast.parse((REPO / "web" / "routes" / "auth.py").read_text())
    [value] = [n.value.value for n in tree.body if isinstance(n, ast.Assign)
               and any(getattr(t, "id", None) == "SESSION_COOKIE" for t in n.targets)]
    assert fp.SESSION_COOKIE == value


def test_it_needs_nothing_but_the_standard_library():
    """It runs on the host's python3, which has no application packages."""
    tree = ast.parse(SCRIPT.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module]
        else:
            continue
        for name in names:
            top = name.split(".")[0]
            assert top == "__future__" or top in sys.stdlib_module_names, name


@pytest.mark.parametrize("today, window", [
    ("2026-10-21", ("2026-08-01", "2026-08-31")),
    ("2026-11-09", ("2026-09-01", "2026-09-30")),   # 40 days back is 30 Sep itself
    ("2026-11-10", ("2026-09-01", "2026-09-30")),
    ("2026-03-12", ("2026-01-01", "2026-01-31")),
    ("2027-01-15", ("2026-11-01", "2026-11-30")),
])
def test_the_default_window(today, window):
    start, end = fp.default_window(date.fromisoformat(today))
    assert (start.isoformat(), end.isoformat()) == window
    assert fp.check_window(start, end, date.fromisoformat(today)) is None
