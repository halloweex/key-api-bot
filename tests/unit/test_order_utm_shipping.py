"""The UTM shipper, and the four things the review of it found.

`tests/integration/test_traffic_two_engines.py` proves the two engines agree.
This is about the machinery that keeps them agreeing, and every case here is
one the port shipped wrong or left unguarded:

1. the routed reads lost the thread-pool offload and the query timeout;
2. a failing shipper left no trace in the watermark;
3. three admin endpoints rewrote the UTM in DuckDB and never shipped it;
4. `reconcile_order_utm` was written and nothing exercised it;
5. a full replace copied whatever DuckDB held, a half-finished re-parse
   included (DN-04) — the row guard, from `TestTheShrinkRule` down, and
   against a real Postgres in `tests/integration/test_order_utm_row_guard.py`.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import textwrap
from contextlib import asynccontextmanager
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


# ── DN-04: the row guard ────────────────────────────────────────────────────


async def _store_with_utm_rows(tmp_path, n: int):
    """A real DuckDB store holding `n` UTM rows, ids 1..n.

    `range()` rather than an `executemany` of tuples: the guard's cases are
    about a thousand rows, and building them in Python would be the slow half
    of every test here.
    """
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "utm.duckdb")
    await store.connect()
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO silver_order_utm (order_id, utm_source, utm_medium, "
            "utm_campaign, traffic_type, platform, parsed_at) "
            "SELECT i, 'fbads', 'paid', 'spring', 'paid_confirmed', 'facebook', "
            "TIMESTAMPTZ '2026-09-07 06:00:00+00' FROM range(1, ?) t(i)",
            [n + 1],
        )
    return store


class _FakePostgres:
    """Just enough of an asyncpg pool to watch what the shipper sends.

    The count is what `SELECT count(*)` answers; every statement is kept, so a
    test can say a TRUNCATE never happened rather than inferring it.
    """

    def __init__(self, count: int):
        self.count = count
        self.statements: list = []

    @asynccontextmanager
    async def acquire(self):
        yield self

    @asynccontextmanager
    async def _no_transaction(self):
        yield

    def transaction(self):
        return self._no_transaction()

    async def fetchval(self, sql, *args):
        self.statements.append(sql)
        return self.count

    async def execute(self, sql, *args):
        self.statements.append(sql)

    async def executemany(self, sql, rows):
        self.statements.append(sql)


class TestTheShrinkRule:
    """`refusal` is the whole decision, and pure, so its edges are pinned here
    without either database."""

    def test_a_first_ship_into_an_empty_table_is_never_refused(self):
        assert pg_order_utm.refusal(0, 0, None) is None
        assert pg_order_utm.refusal(7, 0, None) is None

    def test_a_copy_within_the_floor_ships(self):
        assert pg_order_utm.refusal(950, 1000, None) is None
        assert pg_order_utm.refusal(1200, 1000, None) is None

    def test_the_floor_itself_ships_and_one_row_under_it_does_not(self):
        """Integer arithmetic, so the line is exactly where the constant says."""
        assert pg_order_utm.SHRINK_FLOOR_PCT == 90
        assert pg_order_utm.refusal(900, 1000, None) is None
        assert pg_order_utm.refusal(899, 1000, None) is not None

    def test_a_shrink_is_refused_and_names_both_counts(self):
        why = pg_order_utm.refusal(100, 1000, None)
        assert why.startswith("refused:")
        assert "100" in why and "1000" in why

    def test_a_failed_parse_refuses_even_an_equal_copy(self):
        why = pg_order_utm.refusal(1000, 1000, "RuntimeError: boom")
        assert why and "RuntimeError: boom" in why
        assert "1000" in why

    @pytest.mark.parametrize("args", [
        (100, 1000, None),
        (1000, 1000, "RuntimeError: " + "x" * 500),
    ])
    def test_the_lever_survives_the_findings_truncation(self, args):
        """`mirror_failing` quotes the first 300 characters of `last_error`. A
        refusal whose `→` fell past that would reach the digest as a count
        with no instruction beside it."""
        why = pg_order_utm.refusal(*args)
        assert "→" in why[:300]

    def test_a_mock_store_is_not_a_failed_parse(self):
        """An `AsyncMock` attribute is a truthy mock; reading it as an error
        would refuse every ship in the suite."""
        assert pg_order_utm._parse_error(AsyncMock()) is None

        class _Store:
            last_utm_parse_error = "ValueError: bad comment"

        assert pg_order_utm._parse_error(_Store()) == "ValueError: bad comment"


class TestAShrinkIsNotShipped:
    """The shipper around the rule: a refusal writes nothing, is recorded, and
    returns rather than raises."""

    async def _ship(self, store, count, **kwargs):
        recorded = []

        async def _fail(table, error):
            recorded.append((table, error))

        fake = _FakePostgres(count)
        with patch("core.pg.require_revision", new=AsyncMock()), \
             patch("core.pg_landing._record_failure", new=_fail):
            result = await pg_order_utm.ship_order_utm(store, pool=fake, **kwargs)
        return result, fake, recorded

    @pytest.mark.asyncio
    async def test_a_shrink_is_refused_without_a_write(self, tmp_path):
        store = await _store_with_utm_rows(tmp_path, 100)
        try:
            result, fake, recorded = await self._ship(store, 1000)
        finally:
            await store.close()

        assert result["duckdb_rows"] == 100 and result["postgres_rows"] == 1000
        assert result["refused"].startswith("refused:")
        assert not any("TRUNCATE" in sql for sql in fake.statements)
        assert not any("INSERT" in sql for sql in fake.statements)
        assert len(recorded) == 1
        table, error = recorded[0]
        assert table == pg_order_utm.UTM_TABLE
        assert "100" in error and "1000" in error

    @pytest.mark.asyncio
    async def test_a_copy_within_the_floor_ships(self, tmp_path):
        store = await _store_with_utm_rows(tmp_path, 950)
        try:
            result, fake, recorded = await self._ship(store, 1000)
        finally:
            await store.close()

        assert result["rows"] == 950 and "refused" not in result
        assert any(sql.startswith("TRUNCATE") for sql in fake.statements)
        assert any("meta.mirror_state" in sql for sql in fake.statements)
        assert recorded == []

    @pytest.mark.asyncio
    async def test_a_failed_parse_is_refused_and_force_ships_anyway(self, tmp_path):
        store = await _store_with_utm_rows(tmp_path, 1000)
        store.last_utm_parse_error = "RuntimeError: boom"
        try:
            refused, _fake, recorded = await self._ship(store, 1000)
            forced, fake, forced_recorded = await self._ship(store, 1000, force=True)
        finally:
            await store.close()

        assert "refused" in refused and len(recorded) == 1
        assert "boom" in recorded[0][1]
        assert forced["rows"] == 1000 and forced.get("forced") is True
        assert any(sql.startswith("TRUNCATE") for sql in fake.statements)
        assert forced_recorded == []

    @pytest.mark.asyncio
    async def test_force_ships_a_shrink(self, tmp_path):
        store = await _store_with_utm_rows(tmp_path, 100)
        try:
            result, fake, recorded = await self._ship(store, 1000, force=True)
        finally:
            await store.close()

        assert result["rows"] == 100
        assert any(sql.startswith("TRUNCATE") for sql in fake.statements)
        assert recorded == []

    def test_the_count_is_taken_before_the_truncate(self):
        """Parsed: the comparison has to read the table it would replace, so
        the count comes before the write in the function, not after it."""
        tree = ast.parse(textwrap.dedent(inspect.getsource(pg_order_utm.ship_order_utm)))
        counts, truncates = [], []
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
                continue
            text = ast.unparse(node)
            if node.func.attr == "fetchval" and "count(*)" in text:
                counts.append(node.lineno)
            if node.func.attr == "execute" and "TRUNCATE" in text:
                truncates.append(node.lineno)
        assert counts and truncates and max(counts) < min(truncates)


class TestTheParserRecordsItsOutcome:
    """`last_utm_parse_error` is set by the parser itself, so every one of its
    five callers leaves a verdict the shipper can read — not only the tick."""

    async def _store_with_a_comment(self, tmp_path):
        from core.duckdb_store import DuckDBStore

        stamp = datetime(2026, 9, 7, 9, 0, tzinfo=timezone.utc)
        store = DuckDBStore(db_path=tmp_path / "parse.duckdb")
        await store.connect()
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total, "
                "ordered_at, updated_at, buyer_id, manager_id, manager_comment) "
                "VALUES (1, 4, 1, 100, ?, ?, 10, NULL, ?)",
                [stamp, stamp,
                 "utm_source: fbads; utm_medium: paid; utm_campaign: spring"],
            )
        return store

    @pytest.mark.asyncio
    async def test_a_raising_parse_is_recorded_and_a_clean_one_clears_it(self, tmp_path):
        from core.duckdb_store import DuckDBStore

        store = await self._store_with_a_comment(tmp_path)
        try:
            assert store.last_utm_parse_error is None
            with patch.object(DuckDBStore, "_parse_utm_from_comment",
                              side_effect=RuntimeError("bad comment")):
                with pytest.raises(RuntimeError):
                    await store.refresh_utm_silver_layer()
            assert store.last_utm_parse_error == "RuntimeError: bad comment"

            assert len(await store.refresh_utm_silver_layer()) == 1
            assert store.last_utm_parse_error is None
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_cancelled_parse_counts_as_failed(self, tmp_path):
        """`CancelledError` is not an `Exception`, and a parse cancelled between
        two write batches leaves the same partial table an error does."""
        from core.duckdb_store import DuckDBStore

        store = await self._store_with_a_comment(tmp_path)
        try:
            with patch.object(DuckDBStore, "_parse_utm_from_comment",
                              side_effect=asyncio.CancelledError()):
                with pytest.raises(asyncio.CancelledError):
                    await store.refresh_utm_silver_layer()
            assert store.last_utm_parse_error.startswith("CancelledError")
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_tick_swallows_the_error_and_the_flag_survives(self, tmp_path):
        """The case the flag exists for: `refresh_warehouse_layers` logs a
        failed parse as a warning and reports success, and the scheduler ships
        straight after — or later, from the deferred path, with no parse of
        its own at all."""
        from core.duckdb_store import DuckDBStore

        store = await self._store_with_a_comment(tmp_path)
        try:
            with patch.object(DuckDBStore, "_parse_utm_from_comment",
                              side_effect=RuntimeError("bad comment")):
                result = await store.refresh_warehouse_layers(trigger="manual")
            assert result["status"] == "success"
            assert store.last_utm_parse_error == "RuntimeError: bad comment"
        finally:
            await store.close()


class TestOnlyTheCliForces:
    """`force=True` defeats the guard, so where it may come from is pinned by
    structure, not left to convention."""

    SHIPPERS = {"ship_order_utm", "ship_after_reparse"}

    @pytest.mark.asyncio
    async def test_ship_after_reparse_as_the_endpoints_call_it_does_not_force(
        self, monkeypatch,
    ):
        """A fresh lock: the module's own may already be bound to the loop of
        whichever test first waited on it."""
        import core.pg_silver

        monkeypatch.setattr(core.pg_silver, "PG_LAYER_LOCK", asyncio.Lock())
        ship = AsyncMock(return_value={"rows": 1})
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_order_utm.ship_order_utm", new=ship):
            await pg_order_utm.ship_after_reparse(AsyncMock())
        assert ship.await_count == 1
        assert ship.await_args.kwargs.get("force", False) is False

    def _calls_passing_force(self, path):
        """Every call to a shipper in `path` that passes `force`, and whether
        it merely forwards a `force` parameter of an enclosing function."""
        tree = ast.parse(path.read_text())
        parents = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parents[child] = node

        def _enclosing_params(node):
            names = set()
            while node in parents:
                node = parents[node]
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    a = node.args
                    names |= {x.arg for x in a.posonlyargs + a.args + a.kwonlyargs}
            return names

        found = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = (node.func.attr if isinstance(node.func, ast.Attribute)
                    else getattr(node.func, "id", None))
            if name not in self.SHIPPERS:
                continue
            for kw in node.keywords:
                if kw.arg == "force":
                    forwards = (isinstance(kw.value, ast.Name)
                                and kw.value.id == "force"
                                and "force" in _enclosing_params(node))
                    found.append((path, node.lineno, kw.value, forwards))
        return found

    def test_nothing_in_the_application_originates_a_force(self):
        """Forwarding the parameter — `ship_after_reparse` handing its own
        `force` on — is allowed; deciding to force is not. Walks the trees
        rather than naming files, so a fifth reparse site is covered the day
        it is written."""
        offenders = []
        for root in ("core", "web", "bot"):
            for path in (REPO / root).rglob("*.py"):
                offenders += [
                    (str(p.relative_to(REPO)), line)
                    for p, line, _value, forwards in self._calls_passing_force(path)
                    if not forwards
                ]
        assert offenders == [], f"force passed outside the CLI: {offenders}"

    def test_the_backfill_script_forces_only_behind_its_flag(self):
        path = REPO / "scripts" / "backfill_utm.py"
        tree = ast.parse(path.read_text())

        passes = self._calls_passing_force(path)
        assert len(passes) == 1, "the script should ship exactly once"
        value = passes[0][2]
        assert isinstance(value, ast.Name) and value.id == "force_ship", (
            "the script must forward its flag, never a literal"
        )

        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.AsyncFunctionDef) and n.name == "backfill_utm")
        names = [a.arg for a in fn.args.args]
        defaults = dict(zip(names[len(names) - len(fn.args.defaults):], fn.args.defaults))
        assert isinstance(defaults["force_ship"], ast.Constant)
        assert defaults["force_ship"].value is False

        flags = [
            n for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "add_argument" and n.args
            and isinstance(n.args[0], ast.Constant) and n.args[0].value == "--force-ship"
        ]
        assert len(flags) == 1
        action = next(k.value for k in flags[0].keywords if k.arg == "action")
        assert action.value == "store_true"
