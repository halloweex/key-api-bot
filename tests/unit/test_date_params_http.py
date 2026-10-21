"""Every dashboard route answers a malformed date, or a range over the cap,
400 — in the shape its other validators answer.

The routes are read off the app: every GET under /api/ that takes
`start_date` and `end_date`, 36 on 2026-10-21, so one added later is covered
without being listed. Each must answer

- a malformed `start_date` or `end_date` with 400 `{"detail": "<field>: ..."}`.
  It answered 500 `{"error": "Internal Server Error", ...}`: 32 of the 36
  called `parse_period` after their `try`, and its ValueError reached the
  catch-all in web/main.py.
- a range over MAX_RANGE_DAYS with 400 `{"detail": "date_range: ..."}`.

`{"detail": ...}` is what every validator on these routes answers, through
the route's own `except ValidationError`, so the parse moved inside it. The
app-wide ValidationError handler would have answered 400 as well, but with
`"error"` beside the detail — a second shape on the same routes, and the four
/api/reports routes that already wrapped the parse would have kept the first.

Valid input answers as it did: a form `strptime` forgives gives the body its
ISO spelling gives, and a range of exactly the cap is answered. `parse_period`
itself is tests/unit/test_filters_parse_period.py.
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
    event loop and 95 MB of JSON, measured, before the cap."""
    r = admin_client.get("/api/revenue/trend", params={
        "start_date": "0002-01-01", "end_date": "9999-12-30", "compare_type": "year_ago"})
    assert r.status_code == 400
    assert r.json()["detail"].startswith("date_range: ")


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
