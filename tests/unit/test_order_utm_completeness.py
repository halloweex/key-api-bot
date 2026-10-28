"""What the UTM completeness counts mean, without a server (DN-16).

`tests/integration/test_order_utm_completeness.py` proves the SQL on a real
Postgres, including that it owes exactly what the DuckDB parser parses. This
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

from core.data_quality import (
    DEFAULT_REMEDIATION,
    HUMAN_CHECK_NAMES,
    Severity,
    remediation_for,
)
from core.mirror_reconciliation import (
    ORDER_UTM_TABLE,
    SILVER_GRACE_MINUTES,
    order_utm_completeness_findings,
)

NAMES = ("pg_order_utm_missing", "pg_order_utm_stale", "pg_order_utm_in_flight")
SINCE = datetime(2030, 6, 5, 4, 9, tzinfo=timezone.utc)


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
        assert issue.table_name == ORDER_UTM_TABLE.pg_table
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
    def test_each_name_has_a_lever_of_its_own(self):
        """A CRITICAL names its lever; the anchor is the signal one is missing."""
        for name in NAMES:
            assert remediation_for([name]) != [DEFAULT_REMEDIATION], name

    def test_the_watermark_is_read_before_anything_ships(self):
        """A successful ship clears `last_error`, and with it the reason the
        last one failed — so the line sends the reader to the watermark
        before it offers the lever that ships."""
        (line,) = remediation_for(["pg_order_utm_missing"])
        assert "meta.mirror_state" in line and "silver.order_utm" in line
        assert line.index("meta.mirror_state") < line.index("/api/traffic/refresh")

    def test_every_route_the_lever_names_exists(self):
        """A lever that names a route nobody registered sends the reader to a
        404 at the moment they are acting on a page."""
        from tests.routes_helper import find_endpoint
        from web.main import app

        (line,) = remediation_for(["pg_order_utm_missing"])
        named = re.findall(r"POST (/api/[\w/-]+)", line)
        assert set(named) == {"/api/traffic/refresh"}
        for path in named:
            assert find_endpoint(app, path, "POST") is not None, path

    def test_every_route_the_reasons_name_exists(self):
        """The line has room for the levers alone, so why each one is or is
        not offered rides in both paging descriptions — including the backfill
        the line no longer offers. A route named there must exist as surely
        as one named in the line: it is read at the same moment."""
        from tests.routes_helper import find_endpoint
        from web.main import app

        paged = _findings(
            missing=1, missing_ids=[1], missing_since=SINCE,
            stale=1, stale_ids=[2], stale_since=SINCE,
        )
        for issue in paged:
            named = set(re.findall(r"POST (/api/[\w/-]+)", issue.description))
            assert named == {
                "/api/traffic/backfill-utm",
                "/api/traffic/refresh",
                "/api/warehouse/refresh",
            }, issue.check_name
            for path in named:
                assert find_endpoint(app, path, "POST") is not None, path

    def test_one_lever_ships_when_nothing_marked_the_warehouse_dirty(self):
        """The 05:15 status refresh, the weekly full sync and the sync of
        today parse in DuckDB without marking the warehouse dirty, and a
        restart forgets the tick's deferral — so a lever that waits for the
        next dirty tick can wait all morning. The route the line names must
        parse and reach Postgres by itself, which is what `reparse_router`
        does under either KS_UTM_PARSE: it ships DuckDB's parse, or parses in
        Postgres (DN-19)."""
        from tests.routes_helper import find_endpoint
        from web.main import app

        endpoint = find_endpoint(app, "/api/traffic/refresh", "POST").route.endpoint
        called = _names_called(endpoint)
        assert {"refresh_utm_silver_layer", "reparse_router"} <= called

    def test_each_page_tells_the_reader_the_comment_may_be_missing_in_duckdb(self):
        """The description is what the diagnostic agent and the digest read.
        It names the finding that shows the divergence, which must be a
        condition that exists, and says the parser reads DuckDB."""
        from core.alerting import REGISTRY

        paged = _findings(
            missing=1, missing_ids=[1], missing_since=SINCE,
            stale=1, stale_ids=[2], stale_since=SINCE,
        )
        assert [i.check_name for i in paged] == [
            "pg_order_utm_missing", "pg_order_utm_stale"]
        for issue in paged:
            assert "mirror_row_values" in issue.description, issue.check_name
            assert "reads DuckDB" in issue.description, issue.check_name
        assert "mirror_row_values" in REGISTRY

    def test_each_name_reads_as_words_in_the_digest(self):
        for name in NAMES:
            assert name in HUMAN_CHECK_NAMES, name


class TestUnderThePostgresParse:
    """DN-19 moved the parse into Postgres behind `KS_UTM_PARSE=postgres`, and
    the review found the levers still written for DuckDB's. The first one,
    copying the comment into DuckDB, does nothing for a parser that reads
    bronze.orders. And the reasons said `POST /api/warehouse/refresh` never
    ships this table, while under own its derivation now ends with the
    parse. Not a false page while the flag is off; a misleading "→" the day
    it is flipped. Under the default the words are unchanged (the tests
    above), which is why the mode picks them rather than a neutral rewrite.
    """

    PG_ROUTES = {"/api/traffic/refresh", "/api/warehouse/refresh"}

    @pytest.fixture
    def postgres_mode(self, monkeypatch):
        """KS_PG_DERIVE=own and KS_UTM_PARSE=postgres, configured the way
        `configure_modes` does it — the derivation first — with every cache
        put back afterwards."""
        from core import pg_derivation, pg_utm_parse

        before = (pg_derivation._mode, pg_derivation._mode_error,
                  pg_utm_parse._mode, pg_utm_parse._mode_error)
        monkeypatch.setenv(pg_derivation.ENV, "own")
        monkeypatch.setenv(pg_utm_parse.ENV, "postgres")
        pg_derivation.configure_mode()
        assert pg_utm_parse.configure_mode() == "postgres"
        yield
        (pg_derivation._mode, pg_derivation._mode_error,
         pg_utm_parse._mode, pg_utm_parse._mode_error) = before

    @staticmethod
    def _routes_exist(named):
        from tests.routes_helper import find_endpoint
        from web.main import app

        for path in named:
            assert find_endpoint(app, path, "POST") is not None, path

    def test_the_line_reads_the_watermark_then_offers_both_refreshes(self, postgres_mode):
        """Read through the cached mode, as every alert reads it."""
        for name in NAMES:
            (line,) = remediation_for([name])
            assert "DuckDB" not in line and "manager_comment" not in line, name
            assert "meta.mirror_state" in line and "silver.order_utm" in line
            named = re.findall(r"(/api/[\w/-]+)", line)
            assert set(named) == self.PG_ROUTES, named
            assert line.index("meta.mirror_state") < min(line.index(r) for r in named)
            self._routes_exist(named)

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

    def test_the_pages_give_the_reasons_for_this_parser(self):
        paged = order_utm_completeness_findings(
            _row(missing=1, missing_ids=[1], missing_since=SINCE,
                 stale=1, stale_ids=[2], stale_since=SINCE),
            grace_minutes=SILVER_GRACE_MINUTES, parsed_in_postgres=True)
        assert [i.check_name for i in paged] == [
            "pg_order_utm_missing", "pg_order_utm_stale"]
        for issue in paged:
            text = issue.description
            assert "reads bronze.orders itself" in text, issue.check_name
            assert "reads DuckDB" not in text and "mirror_row_values" not in text
            assert "never ships this table" not in text
            assert "backfill-utm" not in text
            named = set(re.findall(r"POST (/api/[\w/-]+)", text))
            assert named == self.PG_ROUTES, (issue.check_name, named)
            self._routes_exist(named)
            # The counts are the same finding's whatever the words.
            assert f"more than {SILVER_GRACE_MINUTES} minutes" in text
            assert SINCE.isoformat() in text

    def test_in_flight_waits_for_the_derivation_not_the_tick(self):
        (issue,) = order_utm_completeness_findings(
            _row(in_flight=2, in_flight_ids=[5, 6]),
            grace_minutes=SILVER_GRACE_MINUTES, parsed_in_postgres=True)
        assert issue.severity is Severity.INFO
        assert "DuckDB's tick" not in issue.description
        assert "next derivation" in issue.description

    @pytest.mark.asyncio
    async def test_the_job_passes_the_mode(self, postgres_mode, monkeypatch):
        """The findings are pure and take the mode as an argument; the job is
        what reads it. Without this the words above would be reachable only
        from a test."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from core import mirror_reconciliation as mr

        monkeypatch.delenv("KS_MIRROR_LANDING", raising=False)

        async def _fake_row(conn, *, now, grace_minutes, max_samples):
            return _row(missing=1, missing_ids=[7], missing_since=SINCE)

        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=object())
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()), \
             patch.object(mr, "order_utm_completeness_row", new=_fake_row):
            (issue,) = await mr.reconcile_order_utm_completeness(
                now=datetime(2030, 6, 5, 4, 30, tzinfo=timezone.utc))
        assert "reads bronze.orders itself" in issue.description
