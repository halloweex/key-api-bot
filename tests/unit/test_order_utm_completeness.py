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
        assert set(named) == {"/api/traffic/backfill-utm", "/api/traffic/refresh"}
        for path in named:
            assert find_endpoint(app, path, "POST") is not None, path

    def test_one_lever_ships_when_nothing_marked_the_warehouse_dirty(self):
        """The 05:15 status refresh, the weekly full sync and the sync of
        today parse in DuckDB without marking the warehouse dirty, and a
        restart forgets the tick's deferral — so a lever that waits for the
        next dirty tick can wait all morning. The route the line names must
        parse and ship by itself, which is what `ship_after_reparse` is."""
        from tests.routes_helper import find_endpoint
        from web.main import app

        endpoint = find_endpoint(app, "/api/traffic/refresh", "POST").route.endpoint
        called = _names_called(endpoint)
        assert {"refresh_utm_silver_layer", "ship_after_reparse"} <= called

    def test_the_warehouse_refresh_is_not_offered_because_it_does_not_ship_this(self):
        """The lever every Silver alert names rebuilds DuckDB, and under own
        derives Postgres — and never calls the UTM ship. Offering it here would
        be a lever that moves nothing on /traffic."""
        from tests.routes_helper import find_endpoint
        from web.main import app

        endpoint = find_endpoint(app, "/api/warehouse/refresh", "POST").route.endpoint
        assert not {"ship_order_utm", "ship_after_reparse"} & _names_called(endpoint)
        (line,) = remediation_for(["pg_order_utm_missing"])
        assert "/api/warehouse/refresh" not in line

    def test_the_cause_no_ship_can_reach_is_named_first(self):
        """A comment Postgres keeps and DuckDB lost: the parser reads DuckDB,
        so re-parsing and re-shipping cannot clear it, and a reader who tried
        them first would conclude the check is broken."""
        (line,) = remediation_for(["pg_order_utm_missing"])
        comment = line.index("manager_comment")
        assert comment < line.index("meta.mirror_state")
        assert comment < line.index("/api/traffic/refresh")

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
