"""What the UTM completeness counts mean, without a server (DN-16).

`tests/integration/test_order_utm_completeness.py` proves the SQL on a real
Postgres, including that it owes exactly what the parser parses. This
pins the other half: how one aggregate row becomes findings — which count
pages, which is only counted — and that each finding a page can carry names a
lever instead of the generic anchor.
"""
from __future__ import annotations

import ast
import inspect
import re
import textwrap
from datetime import datetime, timezone

import pytest

from core.data_quality import HUMAN_CHECK_NAMES, Severity
from core.mirror_reconciliation import (
    ORDER_UTM_PG_TABLE,
    order_utm_completeness_findings,
)

# The grace at the default floor (`pg_warehouse_dq.silver_grace_minutes()`,
# 600 s + the twins' ten minutes) — the number the Silver comparison fixed as
# `mirror_reconciliation.SILVER_GRACE_MINUTES` until step 14's S14-12 deleted
# it with that comparison.
SILVER_GRACE_MINUTES = 20

NAMES = ("pg_order_utm_missing", "pg_order_utm_stale", "pg_order_utm_in_flight")
SINCE = datetime(2030, 6, 5, 4, 9, tzinfo=timezone.utc)

# A route a lever names, with the method written before it if one is: the
# path ends on a word character or a `{placeholder}`, so the full stop of a
# sentence is not read as part of it.
_LEVER_ROUTE = re.compile(r"(?:\b(GET|POST|PUT|PATCH|DELETE) )?(/api/[\w{}/.-]*[\w}])")
_LEVER_JOB = re.compile(r"/api/jobs/(\w+)/trigger")


def _row(**counts):
    """The aggregate as asyncpg returns it on an empty table: zero counts,
    NULL arrays and NULL minimums — `array_agg` over no rows is NULL."""
    row = {
        "missing": 0, "missing_ids": None, "missing_since": None,
        "stale": 0, "stale_ids": None, "stale_since": None,
        "in_flight": 0, "in_flight_ids": None,
    }
    row.update(counts)
    return row


def _findings(**counts):
    return order_utm_completeness_findings(
        _row(**counts), grace_minutes=SILVER_GRACE_MINUTES)


def _names_called(function) -> set:
    """Every name a function calls, parsed from its source rather than
    grepped: a comment naming `ship_after_reparse` is not a call to it."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(inspect.unwrap(function))))
    return {
        node.func.id if isinstance(node.func, ast.Name) else node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, (ast.Name, ast.Attribute))
    }


class TestEachCountMeansOneThing:
    def test_nothing_owed_is_no_finding(self):
        assert _findings() == []

    def test_an_overdue_missing_verdict_pages(self):
        (issue,) = _findings(missing=3, missing_ids=[7, 8, 9], missing_since=SINCE)
        assert issue.check_name == "pg_order_utm_missing"
        assert issue.severity is Severity.CRITICAL
        assert (issue.count, issue.sample_ids) == (3, (7, 8, 9))
        assert issue.table_name == ORDER_UTM_PG_TABLE
        # The oldest arrival is what tells a stalled ship from one late tick.
        assert SINCE.isoformat() in issue.description
        assert f"{SILVER_GRACE_MINUTES} minutes" in issue.description

    def test_an_overdue_stale_verdict_pages(self):
        (issue,) = _findings(stale=1, stale_ids=[11], stale_since=SINCE)
        assert issue.check_name == "pg_order_utm_stale"
        assert issue.severity is Severity.CRITICAL
        assert issue.sample_ids == (11,)
        # Its own oldest arrival, not the missing count's: with nothing
        # missing, reading the wrong key would print "unknown" here.
        assert SINCE.isoformat() in issue.description
        assert f"{SILVER_GRACE_MINUTES} minutes" in issue.description

    def test_in_flight_is_counted_and_never_pages(self):
        (issue,) = _findings(in_flight=2, in_flight_ids=[5, 6])
        assert issue.check_name == "pg_order_utm_in_flight"
        assert issue.severity is Severity.INFO
        assert (issue.count, issue.sample_ids) == (2, (5, 6))

    def test_all_three_at_once_stay_three_findings(self):
        issues = _findings(
            missing=1, missing_ids=[1], missing_since=SINCE,
            stale=1, stale_ids=[2], stale_since=SINCE,
            in_flight=1, in_flight_ids=[3],
        )
        assert [(i.check_name, i.sample_ids) for i in issues] == [
            ("pg_order_utm_missing", (1,)),
            ("pg_order_utm_stale", (2,)),
            ("pg_order_utm_in_flight", (3,)),
        ]


class TestTheGraceFollowsTheFloor:
    """The job calls the check with no grace at all, so the default *is* the
    production grace. It is the ship's floor plus the twins' margin, read when
    the check runs: raise `KS_PG_SILVER_INTERVAL_S` and the grace moves with
    it, instead of paging on the old floor's width.

    The SQL half is proved on a server in the integration twin of this file;
    this pins which number reaches it, which needs no server.
    """

    NOW = datetime(2030, 6, 5, 4, 30, tzinfo=timezone.utc)

    async def _grace_used(self, monkeypatch, **kwargs):
        from unittest.mock import AsyncMock, MagicMock, patch

        from core import mirror_reconciliation as mr

        monkeypatch.delenv("KS_MIRROR_LANDING", raising=False)
        # A Postgres to compare against (`configured()`), never reached: the
        # pool is a mock. Without it the check stands down and the test read
        # the DSN of whatever environment it ran in.
        monkeypatch.setenv("KS_PG_DSN", "postgresql://unused@127.0.0.1:1/none")
        seen = {}

        async def _fake_row(conn, *, now, grace_minutes, max_samples):
            seen["grace"] = grace_minutes
            return _row(missing=1, missing_ids=[7], missing_since=SINCE)

        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=object())
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()), \
             patch.object(mr, "order_utm_completeness_row", new=_fake_row):
            (issue,) = await mr.reconcile_order_utm_completeness(now=self.NOW, **kwargs)
        # And the finding says the same number the query was given.
        assert f"more than {seen['grace']} minutes" in issue.description
        return seen["grace"]

    @pytest.mark.asyncio
    async def test_the_default_floor_gives_the_comparisons_twenty(self, monkeypatch):
        monkeypatch.delenv("KS_PG_SILVER_INTERVAL_S", raising=False)
        assert await self._grace_used(monkeypatch) == SILVER_GRACE_MINUTES == 20

    @pytest.mark.asyncio
    async def test_raising_the_floor_raises_the_grace(self, monkeypatch):
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "1200")
        assert await self._grace_used(monkeypatch) == 30

    @pytest.mark.asyncio
    async def test_lowering_the_floor_lowers_it(self, monkeypatch):
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "60")
        assert await self._grace_used(monkeypatch) == 11

    @pytest.mark.asyncio
    async def test_a_grace_given_explicitly_is_the_one_used(self, monkeypatch):
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "1200")
        assert await self._grace_used(monkeypatch, grace_minutes=5) == 5


class TestTheReaderIsToldWhatToDo:

    def test_every_route_a_lever_names_is_served(self, monkeypatch):
        """A lever that names a route nobody serves sends the reader to a 404
        at the moment they are acting on a page — any lever, this module's
        among them. Each route is asked anonymously, so nothing runs: a route
        that exists answers with its sign-in gate (or, for the open health
        check, an answer), one that does not with 404 or 405. A job a lever
        triggers must be one the scheduler registers, or the trigger is a 404
        too."""
        import asyncio
        from unittest.mock import AsyncMock

        from fastapi.testclient import TestClient

        from core.data_quality import REMEDIATION
        from core.scheduler import registered_job_ids
        from web.main import app

        # The open health check reads no store here: it is asked whether it
        # is served, not what it says.
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=OSError("no store in this test")))
        client = TestClient(app)

        def served(method, path):
            # A path written without a method is read or posted to by the
            # sentence around it ("POST /api/a or /api/b"): either will do.
            concrete = re.sub(r"\{\w+\}", "1", path)
            return {m: client.request(m, concrete).status_code
                    for m in ([method] if method else ["GET", "POST"])}

        jobs = set(asyncio.run(registered_job_ids()))
        asked = 0
        for prefix, lever in REMEDIATION:
            for method, path in _LEVER_ROUTE.findall(lever):
                asked += 1
                answers = served(method, path)
                assert set(answers.values()) - {404, 405}, (prefix, path, answers)
            for job in _LEVER_JOB.findall(lever):
                assert job in jobs, (prefix, job)
        assert asked, "no lever names a route: the pattern reads none of them"

    def test_the_refresh_the_line_names_parses_by_itself(self):
        """A lever that waited for the next derivation could wait the floor out;
        the route the line names must parse by itself, which is what
        `reparse_router` does (DN-19)."""
        from tests.routes_helper import find_endpoint
        from web.main import app
        from web.routes.api import traffic

        endpoint = find_endpoint(app, "/api/traffic/refresh", "POST").route.endpoint
        assert "_parse_in_postgres" in _names_called(endpoint)
        assert "reparse_router" in _names_called(traffic._parse_in_postgres)

    def test_each_name_reads_as_words_in_the_digest(self):
        for name in NAMES:
            assert name in HUMAN_CHECK_NAMES, name


class TestUnderThePostgresParse:
    """DN-19 moved the parse into Postgres behind `KS_UTM_PARSE=postgres`, and
    the review found the levers still written for DuckDB's: copying the
    comment into DuckDB does nothing for a parser that reads bronze.orders,
    and `POST /api/warehouse/refresh`'s derivation now ends with the parse.
    The mode picked the words until step 14's S14-15 removed it: these are
    the words, the ones production published under `postgres`.
    """

    def test_the_warehouse_refresh_reaches_the_parse(self):
        """Why the line may offer it: the route, under own, runs the whole
        derivation, and the derivation's last step is the parse. Parsed from
        each link rather than trusted; the same `_run_pg_derivation` is
        proved to parse on a real Postgres in
        `tests/integration/test_pg_own_derivation.py`."""
        from core import pg_utm_parse
        from core.scheduler import BackgroundScheduler
        from tests.routes_helper import find_endpoint
        from web.main import app
        from web.routes.api import admin

        endpoint = find_endpoint(app, "/api/warehouse/refresh", "POST").route.endpoint
        chain = [
            (endpoint, "_derive_postgres_now"),
            (admin._derive_postgres_now, "_run_pg_derivation"),
            (BackgroundScheduler._run_pg_derivation, "_derive_pg_layers"),
            (BackgroundScheduler._derive_pg_layers, "_parse_order_utm_in_postgres"),
            (BackgroundScheduler._parse_order_utm_in_postgres, "parse_incremental_locked"),
        ]
        for function, callee in chain:
            assert callee in _names_called(function), (function.__qualname__, callee)
        assert "parse_incremental" in _names_called(pg_utm_parse.parse_incremental_locked)
