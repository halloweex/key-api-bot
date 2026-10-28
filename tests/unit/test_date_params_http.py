"""Every dashboard route answers a malformed date, a date outside the window,
or a range over the cap, 400 — in the shape its other validators answer.

The routes are read off the app: every GET under /api/ that takes
`start_date` and `end_date`, 36 on 2026-10-21, so one added later is covered
without being listed. Each must answer

- a malformed `start_date` or `end_date` with 400 `{"detail": "<field>: ..."}`.
  It answered 500 `{"error": "Internal Server Error", ...}`: 32 of the 36
  called `parse_period` after their `try`, and its ValueError reached the
  catch-all in web/main.py.
- a well-formed date outside 2000-01-01..2099-12-31 the same way. Eight
  routes answered the calendar's own edges 500, from an OverflowError or a
  ValueError in their date arithmetic (measured 2026-10-22), and every route
  answered a date beyond the window with zeros.
- a range over MAX_RANGE_DAYS with 400 `{"detail": "date_range: ..."}`.

`{"detail": ...}` is what every validator on these routes answers, through
the route's own `except ValidationError`, so the parse moved inside it. The
app-wide ValidationError handler would have answered 400 as well, but with
`"error"` beside the detail — a second shape on the same routes, and the four
/api/reports routes that already wrapped the parse would have kept the first.

Valid input answers as it did: a form `strptime` forgives gives the body its
ISO spelling gives, a range of exactly the cap is answered, and so are the
window's own edges, under every comparison. So is 29 February, which the
marketing report and its CSV answered 500 until its year ago became the
trend's. `parse_period` itself is tests/unit/test_filters_parse_period.py.
"""
from datetime import date, timedelta

import pytest

from core.filters import MAX_RANGE_DAYS
from tests.routes_helper import iter_endpoints
from tests.unit.test_read_fallback_http import _request_for
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    _seed_gold,
    admin_client,
)
from web.main import app

@pytest.fixture(autouse=True)
def _chains_on_duckdb(monkeypatch):
    """Since step 14's S14-00 every write chain writes Postgres, a constant.
    This module's subject is how a route reads its dates, not where its rows
    live, and it runs against the per-test DuckDB file with no Postgres
    behind the client. `/api/returns` joins the managers, whose reads follow
    chain 5 to Postgres once it writes there, and the marketing report and
    its CSV read chain 7a's target line the same way; so this module keeps
    DuckDB as those chains' store (`tests.conftest.route_chains_to_duckdb`)
    until their DuckDB branches go."""
    from core import pg_goals_write, pg_managers_write
    from tests.conftest import route_chains_to_duckdb

    route_chains_to_duckdb(monkeypatch, pg_goals_write, pg_managers_write)


DATED = sorted(
    (e for e in iter_endpoints(app)
     if "GET" in e.methods and e.path.startswith("/api/")
     and {"start_date", "end_date"} <= {q.name for q in e.route.dependant.query_params}),
    key=lambda e: e.path,
)


def _get(client, endpoint, **dates):
    path, params = _request_for(endpoint)
    return client.get(path, params={**params, **dates})


def test_the_walk_finds_the_routes():
    """A walk that found nothing would pass every test below."""
    paths = {e.path for e in DATED}
    assert len(paths) >= 36
    assert {"/api/revenue/trend", "/api/expenses/summary", "/api/expenses/profit",
            "/api/customers/insights", "/api/reports/summary",
            "/api/traffic/transactions", "/api/margin/overview"} <= paths


@pytest.mark.parametrize("field", ["start_date", "end_date"])
@pytest.mark.parametrize("endpoint", DATED, ids=lambda e: e.path)
def test_a_malformed_date_is_a_400_naming_it(admin_client, endpoint, field):
    dates = {"start_date": "2026-01-01", "end_date": "2026-01-31", field: "2026-02-30"}
    r = _get(admin_client, endpoint, **dates)
    assert r.status_code == 400, r.text
    assert r.json() == {
        "detail": f"{field}: Invalid date format. Expected %Y-%m-%d (got: '2026-02-30')"}


@pytest.mark.parametrize("endpoint", DATED, ids=lambda e: e.path)
def test_a_range_over_the_cap_is_a_400(admin_client, endpoint):
    start = date(2000, 1, 1)
    end = start + timedelta(days=MAX_RANGE_DAYS)
    r = _get(admin_client, endpoint, start_date=start.isoformat(), end_date=end.isoformat())
    assert r.status_code == 400, r.text
    assert r.json() == {"detail": (
        f"date_range: Date range cannot exceed {MAX_RANGE_DAYS} days "
        f"(got: '{MAX_RANGE_DAYS + 1} days')")}


@pytest.mark.parametrize("raw", ["2026-02-30", "2026-13-01", "26-01-01", "2026-01-01T00:00"])
def test_the_pilots_four_on_the_trend(admin_client, raw):
    """The four the Rust pilot reported, each a 500 on 3.14 before."""
    r = admin_client.get("/api/revenue/trend",
                         params={"start_date": raw, "end_date": "2026-03-31"})
    assert r.status_code == 400
    assert r.json() == {
        "detail": f"start_date: Invalid date format. Expected %Y-%m-%d (got: {raw!r})"}


def test_the_pilots_range_on_the_trend(admin_client):
    """0002-01-01 to 9999-12-30 with a year-ago comparison: 6.8 s of web's
    event loop and 95 MB of JSON, measured, before the cap. Its start is
    outside the window, which refuses it before its days are counted."""
    r = admin_client.get("/api/revenue/trend", params={
        "start_date": "0002-01-01", "end_date": "9999-12-30", "compare_type": "year_ago"})
    assert r.status_code == 400
    assert r.json()["detail"].startswith("start_date: Date must be between ")


# A day either side of the window, which every route answered with zeros, and
# the calendar's last day as the end alone, named before the range is
# counted. Then the three that answered 500 on eight routes between them: the
# calendar's last day under a per-day loop, year 1 under a comparison, and a
# backwards range whose previous period lands past 9999-12-31. Each starts
# outside the window as well, and the start is the date named.
OUTSIDE = [
    ("start_date", "1999-12-31", "2000-01-31"),
    ("end_date", "2099-12-01", "2100-01-01"),
    ("end_date", "2099-12-31", "9999-12-31"),
    ("start_date", "9999-12-01", "9999-12-31"),
    ("start_date", "0001-01-01", "0001-01-31"),
    ("start_date", "9999-12-31", "0001-01-01"),
]
COMPARISONS = ["previous_period", "year_ago", "month_ago"]


@pytest.mark.parametrize("field, start, end", OUTSIDE)
@pytest.mark.parametrize("endpoint", DATED, ids=lambda e: e.path)
def test_a_date_outside_the_window_is_a_400_naming_it(admin_client, endpoint, field, start, end):
    r = _get(admin_client, endpoint, start_date=start, end_date=end)
    raw = start if field == "start_date" else end
    assert r.status_code == 400, r.text
    assert r.json() == {
        "detail": f"{field}: Date must be between 2000-01-01 and 2099-12-31 (got: {raw!r})"}


@pytest.mark.parametrize("compare_type", COMPARISONS)
@pytest.mark.parametrize("field, start, end", OUTSIDE[3:])
def test_the_trend_refuses_them_under_every_comparison(admin_client, compare_type, field, start, end):
    """All three answered 500 under every comparison, year 1 stepping off
    the calendar its own way under each: a year, a month or a period back."""
    r = admin_client.get("/api/revenue/trend", params={
        "start_date": start, "end_date": end, "compare_type": compare_type})
    assert r.status_code == 400
    assert r.json()["detail"].startswith(f"{field}: Date must be between ")


# The window's own edges, and a backwards range across all of it: every
# comparison and per-day loop they start stays on the calendar. Then 29
# February at either end of a range and alone, which the marketing report and
# its CSV answered 500: their year ago was `date.replace(year=...)`.
INSIDE = [
    ("2000-01-01", "2000-01-01"),
    ("2099-12-31", "2099-12-31"),
    ("2099-12-31", "2000-01-01"),
    ("2024-02-29", "2024-03-31"),
    ("2024-02-01", "2024-02-29"),
    ("2028-02-29", "2028-02-29"),
]


@pytest.mark.parametrize("start, end", INSIDE)
@pytest.mark.parametrize("endpoint", DATED, ids=lambda e: e.path)
def test_dates_inside_the_window_are_answered(admin_client, endpoint, start, end):
    r = _get(admin_client, endpoint, start_date=start, end_date=end)
    assert r.status_code == 200, r.text


@pytest.mark.parametrize("compare_type", COMPARISONS)
@pytest.mark.parametrize("start, end", INSIDE)
def test_the_trend_answers_them_under_every_comparison(admin_client, compare_type, start, end):
    r = admin_client.get("/api/revenue/trend", params={
        "start_date": start, "end_date": end, "compare_type": compare_type})
    assert r.status_code == 200, r.text


FEBRUARY_2024 = {"start_date": "2024-02-01", "end_date": "2024-02-29"}


def test_the_marketing_report_takes_29_february_a_year_back_to_the_28th(admin_client):
    """The same dates a year back, as the trend's year ago has always taken
    them; the previous period is the 29 days before, as it was."""
    r = admin_client.get("/api/reports/marketing-summary", params=FEBRUARY_2024)
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["yoy_start_date"], body["yoy_end_date"]) == ("2023-02-01", "2023-02-28")
    assert (body["prev_start_date"], body["prev_end_date"]) == ("2024-01-03", "2024-01-31")


def test_the_marketing_csv_labels_29_february_a_year_back_as_the_28th(admin_client):
    r = admin_client.get("/api/reports/marketing-summary/export/csv", params=FEBRUARY_2024)
    assert r.status_code == 200, r.text
    assert "2023-02-01 — 2023-02-28" in r.content.decode("utf-8-sig")


class TestValidInputAnswersAsBefore:
    def test_a_forgiven_form_answers_as_its_iso_spelling(self, admin_client):
        _seed_gold(date(2026, 1, 7), 1234.5, 3)
        forgiven = admin_client.get("/api/revenue/trend", params={
            "start_date": "2026-1-5", "end_date": "2026-01- 9"})
        iso = admin_client.get("/api/revenue/trend", params={
            "start_date": "2026-01-05", "end_date": "2026-01-09"})
        assert forgiven.status_code == iso.status_code == 200
        assert forgiven.json() == iso.json()
        assert forgiven.json()["labels"] == ["05.01", "06.01", "07.01", "08.01", "09.01"]
        assert forgiven.json()["revenue"][2] == 1234.5

    def test_a_range_of_exactly_the_cap_is_answered(self, admin_client):
        start = date(2016, 1, 1)
        end = start + timedelta(days=MAX_RANGE_DAYS - 1)
        r = admin_client.get("/api/revenue/trend", params={
            "start_date": start.isoformat(), "end_date": end.isoformat()})
        assert r.status_code == 200
        assert len(r.json()["labels"]) == MAX_RANGE_DAYS

    def test_traffic_still_checks_its_own_filters_first(self, admin_client):
        """Two traffic routes check `platform` and the rest between their
        `try` and the dates, so their parse is guarded where it stands: a
        request wrong twice is answered as it was, on the platform."""
        r = admin_client.get("/api/traffic/transactions", params={
            "platform": "myspace", "start_date": "2026-02-30", "end_date": "2026-03-01"})
        assert r.status_code == 400
        assert r.json() == {"detail": "Invalid platform: myspace"}
