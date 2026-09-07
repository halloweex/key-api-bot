"""The UTM shipper, and the four things the review of it found.

`tests/integration/test_traffic_two_engines.py` proves the two engines agree.
This is about the machinery that keeps them agreeing, and every case here is
one the port shipped wrong or left unguarded:

1. the routed reads lost the thread-pool offload and the query timeout;
2. a failing shipper left no trace in the watermark;
3. three admin endpoints rewrote the UTM in DuckDB and never shipped it;
4. `reconcile_order_utm` was written and nothing exercised it.
"""
from __future__ import annotations

import ast
import inspect
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_order_utm
from core.mirror_reconciliation import ORDER_UTM_TABLE, compare_table
from core.pg_order_utm import UTM_COLUMNS

REPO = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)




def _migration_columns() -> set:
    """The columns revision 0018 declares for `silver.order_utm`.

    Structural on both counts: `ast` finds the SQL rather than a line search,
    and the column list is delimited by counting parentheses rather than by
    the first `)` — which in this file falls inside a comment.
    """
    src = (REPO / "migrations" / "versions" / "0018_traffic.py").read_text()
    statements = [
        node.value
        for node in ast.walk(ast.parse(src))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        and "CREATE TABLE silver.order_utm" in node.value
    ]
    assert len(statements) == 1, "the CREATE TABLE moved or was duplicated"

    # Comments first: they carry parentheses of their own.
    sql = "\n".join(
        line.split("--", 1)[0] for line in statements[0].splitlines()
    )
    body, depth, started = [], 0, False
    for ch in sql[sql.index("silver.order_utm"):]:
        if ch == "(":
            depth += 1
            started = True
            if depth == 1:
                continue
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
        if started:
            body.append(ch)

    columns = set()
    for part in "".join(body).split(","):
        words = part.split()
        # A column definition is `name TYPE ...`; a trailing constraint clause
        # would have a keyword first, and this table declares none.
        if len(words) >= 2 and words[0].isidentifier():
            columns.add(words[0])
    return columns

def _row(order_id, campaign="spring", traffic="paid_confirmed", platform="facebook"):
    return (order_id, "fbads", "paid", campaign, None, None, None,
            None, None, None, "abc", traffic, platform,
            NOW - timedelta(hours=6))


class TestTheReadsKeepTheirTimeout:
    """Finding 1, and the sharpest of the four.

    `traffic.py` was the only repository that ever used `_fetch_one` /
    `_fetch_all`, which offload the query to a thread — so the event loop keeps
    answering — and bound it with `DEFAULT_QUERY_TIMEOUT`, calling
    `conn.interrupt()` on expiry and waiting for the thread to come off the
    connection before raising. The first cut of `_traffic_run` replaced them
    with a bare `conn.execute` under `self.connection()`, which blocks the
    whole application and can hold the global store lock without bound —
    worst exactly when the DuckDB path is reached at all, Postgres being down.
    """

    def _router(self):
        from core.repositories.traffic import TrafficMixin
        return inspect.getsource(TrafficMixin._traffic_run)

    def test_the_duckdb_branch_goes_through_the_offloading_helpers(self):
        src = self._router()
        assert "self._fetch_one(" in src and "self._fetch_all(" in src

    def test_it_does_not_take_the_store_lock_itself(self):
        """The helpers take it. Taking it here as well is a nested
        acquisition, and the store lock is not reentrant — it hangs rather
        than raising, which is the worst way to find out.

        Parsed, not searched: the first version of this asserted the substring
        `self.connection()` was absent, and failed on the *comment* explaining
        why it must be. Prose is not structure.
        """
        tree = ast.parse(textwrap.dedent(self._router()))
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncWith):
                continue
            head = ast.unparse(node.items[0].context_expr)
            assert "connection()" not in head, (
                f"the router opens the store itself: {head}"
            )

    def test_no_traffic_read_bypasses_the_router(self):
        src = (REPO / "core" / "repositories" / "traffic.py").read_text()
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.AsyncFunctionDef)
                    and node.name.startswith("get_traffic")):
                continue
            body = ast.get_source_segment(src, node) or ""
            assert "conn.execute" not in body, node.name


class TestAFailingShipperSaysSo:
    """Finding 2. Without the watermark a shipper that keeps failing leaves
    `last_ok_at` at the last success and `failures_since_ok` at zero, so the
    daily check reads it as healthy, compares anyway, and reports thousands of
    row differences — the symptom, with the cause hidden."""

    @pytest.mark.asyncio
    async def test_the_failure_is_recorded_before_it_is_raised(self):
        store = AsyncMock()
        recorded = []

        async def _fail(table, error):
            recorded.append((table, error))

        with patch("core.pg.get_pool", new=AsyncMock(side_effect=RuntimeError("pg is down"))), \
             patch("core.pg_landing._record_failure", new=_fail):
            with pytest.raises(RuntimeError):
                await pg_order_utm.ship_order_utm(store)

        assert recorded, "the shipper failed silently"
        table, error = recorded[0]
        assert table == pg_order_utm.UTM_TABLE
        assert "pg is down" in error

    def test_the_store_lock_hold_is_reported_separately(self):
        """The DuckDB read is what costs the *whole dashboard* — the lock is
        global — while the Postgres half costs only this table. One number for
        both cannot tell an operator which half grew."""
        src = inspect.getsource(pg_order_utm.ship_order_utm)
        assert "read_ms" in src


class TestAReparseReachesPostgres:
    """Finding 3, and a regression the port itself created: before it, the tab
    read the very table these endpoints write, so a reclassify was visible at
    once. Now the tab reads Postgres."""

    ROUTES = REPO / "web" / "routes" / "api" / "traffic.py"

    def test_every_reparse_site_ships(self):
        """Parsed, not counted. Comparing two `str.count`s would be satisfied
        by three ships in one handler and none in the other two, and would be
        thrown off by the words appearing in a comment. So each function that
        reparses is checked for a ship of its own.

        `scripts/` is outside `testpaths` and is checked here deliberately:
        `backfill_utm.py` rewrites the same table the same way, and a
        divergence there is just as invisible.
        """
        for path in (self.ROUTES, REPO / "scripts" / "backfill_utm.py"):
            src = path.read_text()
            tree = ast.parse(src)
            for fn in ast.walk(tree):
                if not isinstance(fn, (ast.AsyncFunctionDef, ast.FunctionDef)):
                    continue
                calls = {
                    node.func.attr if isinstance(node.func, ast.Attribute)
                    else getattr(node.func, "id", None)
                    for node in ast.walk(fn) if isinstance(node, ast.Call)
                }
                if "refresh_utm_silver_layer" not in calls:
                    continue
                assert "ship_after_reparse" in calls, (
                    f"{path.name}:{fn.name} reparses the UTM in DuckDB and "
                    f"never ships it — the tab reads Postgres"
                )

    def test_it_ships_under_the_layer_lock(self):
        """The scheduler ships this table too. Two TRUNCATE+INSERT
        transactions that interleave leave Postgres holding whichever
        committed last under a fresh OK watermark — including a copy read
        *before* the reparse."""
        assert "PG_LAYER_LOCK" in inspect.getsource(pg_order_utm.ship_after_reparse)

    @pytest.mark.asyncio
    async def test_it_never_raises(self):
        """The DuckDB work has already succeeded and the endpoint must report
        it; a Postgres fault costs freshness until the next tick."""
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_order_utm.ship_order_utm",
                   new=AsyncMock(side_effect=RuntimeError("pg is down"))):
            out = await pg_order_utm.ship_after_reparse(AsyncMock())
        assert "error" in out

    @pytest.mark.asyncio
    async def test_a_stuck_lock_does_not_park_the_request(self):
        """Three of the four callers are HTTP handlers, and `asyncio.Lock` has
        no timeout. The same lock is held by the ClickHouse shippers across
        network I/O, so a hung ClickHouse would otherwise hang an admin
        request for ever."""
        from core.pg_silver import PG_LAYER_LOCK

        with patch("core.pg_order_utm.LOCK_WAIT_S", 0.05), \
             patch("core.mirror_reconciliation.configured", return_value=True):
            async with PG_LAYER_LOCK:            # held by somebody else
                out = await pg_order_utm.ship_after_reparse(AsyncMock())
        assert "error" in out and "Timeout" in out["error"]

    @pytest.mark.asyncio
    async def test_it_stands_down_without_a_dsn(self):
        with patch("core.mirror_reconciliation.configured", return_value=False):
            out = await pg_order_utm.ship_after_reparse(AsyncMock())
        assert "skipped" in out


class TestTheComparisonCatchesWhatItMustCatch:
    """Finding 4. `compare_table` is pure and takes two already-read
    dictionaries, so the whole comparison can be driven without a database —
    which is the only reason a planted-defect test is cheap enough to keep."""

    WATERMARK = {"last_ok_at": NOW - timedelta(minutes=2),
                 "failures_since_ok": 0, "last_error": None}

    def _sides(self):
        dk = {i: _row(i) for i in (1, 2, 3)}
        return dk, {k: v for k, v in dk.items()}

    # `None` is a *meaningful* watermark here — it is what "never shipped"
    # looks like — so the default cannot be None. The first version of this
    # helper used None for both, and the never-shipped case was silently
    # handed a healthy watermark and asserted against the wrong branch.
    _DEFAULT = object()

    def _run(self, dk, pg, watermark=_DEFAULT):
        synced = {k: v[-1] for k, v in dk.items()}
        return compare_table(
            ORDER_UTM_TABLE, dk, synced, pg,
            self.WATERMARK if watermark is self._DEFAULT else watermark,
            now=NOW, grace_minutes=20,
        )

    def test_an_honest_copy_reports_nothing(self):
        dk, pg = self._sides()
        assert self._run(dk, pg) == []

    def test_a_row_postgres_lost_is_critical(self):
        """`full_replace=True` removes the retired category, and correctly:
        every ship writes the whole table, so a row Postgres is missing was
        lost rather than retired."""
        dk, pg = self._sides()
        del pg[2]
        issues = self._run(dk, pg)
        assert issues and any(2 in (i.sample_ids or ()) for i in issues)

    def test_a_renamed_campaign_is_caught(self):
        """The reason this table is read whole rather than fingerprinted: a
        bucket fingerprint sums numbers and text *lengths*, and `spring` and
        `autumn` are both six characters."""
        dk, pg = self._sides()
        pg[1] = _row(1, campaign="autumn")
        assert len("spring") == len("autumn")          # the point, stated
        issues = self._run(dk, pg)
        assert issues, "a same-length rename went unreported"

    def test_a_reclassified_order_is_caught(self):
        dk, pg = self._sides()
        pg[3] = _row(3, traffic="organic", platform="instagram")
        assert self._run(dk, pg)

    def test_a_row_only_postgres_has_is_caught(self):
        dk, pg = self._sides()
        pg[99] = _row(99)
        assert self._run(dk, pg)

    def test_a_table_never_shipped_says_that_and_stops(self):
        """One fact about the schedule, not 32,905 facts about rows."""
        dk, pg = self._sides()
        issues = self._run(dk, {}, watermark=None)
        assert len(issues) == 1
        assert issues[0].check_name == "mirror_never_shipped"

    def test_a_failing_shipper_is_named_as_such(self):
        dk, pg = self._sides()
        del pg[2]
        issues = self._run(dk, pg, watermark={
            "last_ok_at": NOW - timedelta(hours=3),
            "failures_since_ok": 7, "last_error": "connection refused",
        })
        assert any(i.check_name == "mirror_failing" for i in issues)


class TestTheSpecItself:
    def test_the_compared_columns_are_the_shipped_columns(self):
        """`ORDER_UTM_TABLE.columns` *is* `UTM_COLUMNS`, so asserting the two
        equal proves nothing — it compares an object with itself. The list
        that can actually drift is the migration's, a file and a language
        away, so that is the one read here: a column the table has and the
        shipper does not carry would be written NULL for ever and compared
        against DuckDB's real value.

        Parsed, not grepped. The first attempt split the DDL text on `")\n"`
        and stopped inside a *comment* — `VARCHAR(n)` wraps exactly there —
        which is the failure this repository has written down twice. So the
        SQL is lifted out with `ast`, its comments are stripped, and the
        column list is taken by matching parentheses.
        """
        assert _migration_columns() - {"mirrored_at"} == set(UTM_COLUMNS)

    def test_it_is_a_full_replace(self):
        assert ORDER_UTM_TABLE.full_replace is True

    def test_its_clock_is_the_parse_time(self):
        assert ORDER_UTM_TABLE.synced_column == "parsed_at"

    def test_its_origin_note_is_its_own(self):
        """The shared default is landing's story — the same parsed tuple in
        the same call — and that is false here: this is a copy."""
        from core.mirror_reconciliation import _COPIED_FROM_DUCKDB
        assert ORDER_UTM_TABLE.origin_note != _COPIED_FROM_DUCKDB
        assert "organic" in ORDER_UTM_TABLE.origin_note
