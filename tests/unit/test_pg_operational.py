"""The five tables with no source to be rebuilt from — and the sixth that has one.

Everything else Postgres holds is recoverable: `bronze.*` can be re-fetched
from KeyCRM, Silver and Gold can be recomputed. Five of these cannot, so the
tests below are about the two ways a copy of an irreplaceable thing goes wrong —
shipping less than it should, and claiming to have shipped when it did not.

`bronze.offer_stocks` is the exception and rides here for a reason that is not
about irreplaceability at all; `TestOfferStocksSurvivesTheSmsSwitch` is where
that is pinned.

The append-only claim is load-bearing and is checked rather than trusted:
`TestTheyReallyAreAppendOnly` parses the repository for an `UPDATE` or `DELETE`
against either table, because the watermark shipping in `core/pg_operational.py`
is only correct while that holds.
"""
from __future__ import annotations

import ast
import inspect
import re
import textwrap
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import DuckDBStore
from core.mirror_reconciliation import (
    APPEND_ONLY_TABLES,
    OPERATIONAL_GRACE_MINUTES,
    OPERATIONAL_TABLES,
    _as_sample_id,
    _row_key,
    fingerprints,
)
from core.pg_operational import (
    GOAL_COLUMNS,
    INVENTORY_HISTORY_COLUMNS,
    MANUAL_EXPENSE_COLUMNS,
    MISS_COLUMNS,
    MOVEMENTS_TABLE,
    MOVEMENT_COLUMNS,
    OFFER_STOCKS_TABLE,
    OFFER_STOCK_COLUMNS,
    SKU_HISTORY_COLUMNS,
    SKU_HISTORY_TABLE,
    SKU_STATUS_COLUMNS,
    _insert,
    _upsert,
    read_appends,
    read_full_replace,
    replicate_operational,
)

REPO = Path(__file__).resolve().parents[2]
_MIGRATION = (REPO / "migrations" / "versions" / "0008_operational_history.py")


# ── fixtures ─────────────────────────────────────────────────────────────────


async def _store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "ops.duckdb")
    await store.connect()
    return store


async def _seed(store):
    """One row in each small table, three days of snapshots, four movements."""
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO offer_stocks (id, sku, price, purchased_price, "
            " quantity, reserve) VALUES (11, 'SKU-11', 100.00, 40.00, 5, 1)"
        )
        conn.execute(
            "INSERT INTO revenue_goals (period_type, goal_amount, is_custom,"
            " calculated_goal, growth_factor) VALUES"
            " ('monthly', 5000000.00, TRUE, 4800000.00, 1.10)"
        )
        conn.execute(
            "INSERT INTO order_backfill_misses (order_id, checked_at, reason) "
            "VALUES (7, TIMESTAMP '2026-08-01 10:00:00+00', 'not found')"
        )
        for n, day in enumerate(("2026-08-01", "2026-08-02", "2026-08-03")):
            conn.execute(
                "INSERT INTO inventory_history "
                "(date, total_quantity, total_value, total_reserve, sku_count) "
                f"VALUES (DATE '{day}', {100 + n}, {1000 + n}.50, 5, 2)"
            )
            for offer in (11, 12):
                conn.execute(
                    "INSERT INTO inventory_sku_history "
                    "(date, offer_id, quantity, reserve, price) "
                    f"VALUES (DATE '{day}', {offer}, {50 + n}, 1, 9.99)"
                )
        conn.execute(
            "INSERT INTO sku_inventory_status "
            "(offer_id, product_id, sku, name, brand, category_id, quantity, "
            " reserve, price, purchased_price, last_sale_date, first_seen_at, "
            " updated_at, last_stock_out_at) VALUES "
            "(11, 3, 'SKU-11', 'A', 'B', 1, 50, 1, 9.99, 4.00, "
            " DATE '2026-08-01', DATE '2026-01-01', "
            " TIMESTAMP '2026-08-03 01:00:00+00', NULL)"
        )
        for movement in range(1, 5):
            conn.execute(
                "INSERT INTO stock_movements "
                "(offer_id, product_id, movement_type, quantity_before, "
                " quantity_after, delta, reserve_before, reserve_after) "
                f"VALUES (11, 3, 'stock_out', {60 - movement}, {59 - movement}, "
                "-1, 0, 0)"
            )


def _code_only(obj) -> str:
    """`obj`'s source with comments and docstrings removed.

    A grep over source is satisfied by prose that merely mentions the thing —
    six defects in this repository were hidden or invented that way. `ast`
    parses, `ast.unparse` re-renders without comments, and the docstrings are
    popped explicitly because they are real string nodes.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(obj)))
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list) or not body:
            continue
        first = body[0]
        if (isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)):
            body.pop(0)
    return ast.unparse(tree)


def _migration_columns(table: str) -> list[str]:
    """Column names `CREATE TABLE app.<table>` declares, in order.

    Parsed from the DDL. A grep would be satisfied by a comment naming the
    column, which is the failure this repository has hit six times.
    """
    ddl = _MIGRATION.read_text(encoding="utf-8")
    body = ddl.split(f"CREATE TABLE app.{table} (", 1)[1]
    columns = []
    for raw in body.splitlines():
        line = raw.split("--")[0].strip()
        if not line:
            continue
        if line.startswith(")") or line.startswith("PRIMARY KEY"):
            break
        match = re.match(r"^([a-z_]+)\s+(DATE|TEXT|INTEGER|NUMERIC|BIGINT|TIMESTAMPTZ)", line)
        if match:
            columns.append(match.group(1))
    return columns


# ── the writer and the schema ────────────────────────────────────────────────


class TestTheWriterAndTheMigrationAgree:
    """They are written a file apart and neither imports the other."""

    @pytest.mark.parametrize("table,columns", [
        ("order_backfill_misses", MISS_COLUMNS),
        ("inventory_history", INVENTORY_HISTORY_COLUMNS),
        ("sku_inventory_status", SKU_STATUS_COLUMNS),
        ("inventory_sku_history", SKU_HISTORY_COLUMNS),
        ("stock_movements", MOVEMENT_COLUMNS),
    ])
    def test_every_shipped_column_exists_in_the_table(self, table, columns):
        declared = _migration_columns(table)
        assert declared, f"parsed no columns out of app.{table}"
        assert set(columns) <= set(declared), (
            f"{set(columns) - set(declared)} shipped into app.{table} and not declared"
        )

    def test_the_movement_id_is_carried_not_generated(self):
        """DuckDB assigns it from `seq_stock_movements_id`. A Postgres sequence
        would give the same movement two names and the daily comparison would
        be meaningless before it started."""
        ddl = _MIGRATION.read_text(encoding="utf-8")
        body = ddl.split("CREATE TABLE app.stock_movements (", 1)[1]
        assert "GENERATED" not in body
        assert "serial" not in body.lower()
        assert "id" in MOVEMENT_COLUMNS, "the id must be shipped, not defaulted"

    def test_insert_binds_every_column(self):
        sql = _insert("app.t", ("a", "b", "c"))
        assert sql == "INSERT INTO app.t (a, b, c) VALUES ($1, $2, $3)"

    def test_the_upsert_overwrites_the_values_and_not_the_key(self):
        sql = _upsert("app.t", ("date", "offer_id", "qty"), ("date", "offer_id"))
        assert "ON CONFLICT (date, offer_id) DO UPDATE SET qty = EXCLUDED.qty" in sql
        assert "date = EXCLUDED.date" not in sql


class TestTheyReallyAreAppendOnly:
    """The watermark shipping is only correct while this holds."""

    @pytest.mark.parametrize("table", ["inventory_sku_history", "stock_movements"])
    def test_nothing_in_the_repository_updates_or_deletes_them(self, table):
        offenders = []
        for path in (REPO / "core").rglob("*.py"):
            if path.name in ("pg_operational.py", "mirror_reconciliation.py"):
                continue
            text = path.read_text(encoding="utf-8")
            for statement in ("DELETE FROM", "UPDATE"):
                for match in re.finditer(
                    rf"{statement}\s+{table}\b", text, re.IGNORECASE,
                ):
                    line = text[:match.start()].count("\n") + 1
                    offenders.append(f"{path.name}:{line}")
        assert offenders == [], (
            f"{table} is written as append-only by core/pg_operational.py, and "
            f"these mutate it: {offenders}. Either the shipping is now wrong or "
            "the table needs a full replace."
        )


# ── reading the two shapes ───────────────────────────────────────────────────


class TestReadingTheDuckDBSide:
    @pytest.mark.asyncio
    async def test_the_five_small_tables_come_back_whole(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                out = read_full_replace(conn)
            assert len(out[OFFER_STOCKS_TABLE]) == 1
            assert len(out["app.revenue_goals"]) == 1
            assert len(out["app.order_backfill_misses"]) == 1
            assert len(out["app.inventory_history"]) == 3
            assert len(out["app.sku_inventory_status"]) == 1
            # In the column order Postgres will bind them in.
            assert out["app.order_backfill_misses"][0][0] == 7
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_empty_postgres_asks_for_everything(self, tmp_path):
        """The first run. `None` and `0` are the two ways to say "nothing here"."""
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                history, movements = read_appends(conn, None, 0)
            assert len(history) == 6      # three days, two offers
            assert len(movements) == 4
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_re_ships_the_day_it_resumes_from(self, tmp_path):
        """`>=`, not `>`. A day goes in one statement so a partial day should
        not happen — but `>=` plus an upsert costs one re-shipped day and makes
        it not matter, where `>` would leave a hole nothing ever fills."""
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                history, _ = read_appends(conn, date(2026, 8, 2), 0)
            days = {row[0] for row in history}
            assert days == {date(2026, 8, 2), date(2026, 8, 3)}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_movements_resume_strictly_above_the_last_id(self, tmp_path):
        """`>`, not `>=`: they go in one ordered transaction, so what Postgres
        holds is always a complete prefix."""
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _, movements = read_appends(conn, None, 2)
            assert [row[0] for row in movements] == [3, 4]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_nothing_new_ships_nothing(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                history, movements = read_appends(conn, date(2026, 8, 4), 4)
            assert history == []
            assert movements == []
        finally:
            await store.close()


class TestTheRepairPath:
    """A row lost below the watermark is invisible to `id > MAX(id)` and to
    `date >= MAX(date)` forever. `full=True` is what puts it back."""

    def test_full_ignores_both_watermarks(self):
        source = inspect.getsource(replicate_operational)
        block = source[source.index("if full:"):]
        assert "since_date, since_movement_id = None, 0" in block[:120]

    def test_full_upserts_the_movements_and_the_incremental_path_does_not(self):
        """On the incremental path a conflict means the watermark logic is
        wrong and should say so, not overwrite and look fine."""
        source = inspect.getsource(replicate_operational)
        assert "_upsert(MOVEMENTS_TABLE, MOVEMENT_COLUMNS, (\"id\",))" in source
        assert "if full else" in source

    @pytest.mark.asyncio
    async def test_it_is_never_taken_on_a_schedule(self):
        """Reconciliation A reports and does not repair. A check that fixes
        what it finds destroys the evidence that it found anything."""
        from core.scheduler import BackgroundScheduler

        source = inspect.getsource(BackgroundScheduler._run_replicate_operational)
        assert "full=True" not in source
        assert "full" not in source.split("replicate_operational(store")[1][:40]


class TestItNeverRaises:
    @pytest.mark.asyncio
    async def test_no_postgres_configured_is_a_skip_not_an_error(self):
        with patch("core.mirror_reconciliation.configured", return_value=False):
            assert "skipped" in await replicate_operational(object())

    @pytest.mark.asyncio
    async def test_a_postgres_fault_is_returned_not_thrown(self):
        """Rule 8: this is called from a scheduler job that must survive it."""
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg.get_pool",
                   new=AsyncMock(side_effect=RuntimeError("postgres is down"))):
            result = await replicate_operational(object())
        assert "postgres is down" in result["error"]

    @pytest.mark.asyncio
    async def test_it_reads_the_watermarks_before_reading_duckdb(self):
        """The other order loses rows: one written between the two reads would
        be above the watermark Postgres reports next time and below the one
        this run recorded."""
        source = inspect.getsource(replicate_operational)
        tree = ast.parse(source.lstrip())
        awaits = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Await)
        ]
        assert awaits, "expected the function to await something"
        # `MAX(date)` is fetched, then `store.connection()` is entered.
        assert source.index("SELECT MAX(date)") < source.index(
            "async with store.connection()"
        )


# ── the sixth table, and why it is here ──────────────────────────────────────


class TestOfferStocksSurvivesTheSmsSwitch:
    """`bronze.offer_stocks` is landing data on a list of irreplaceable ones.

    It shipped inside `replicate_sms` until 2026-09-06, which was reasonable
    while the SMS tab was its only Postgres reader. Then `KS_SMS_STORE=postgres`
    made DuckDB no longer the writer of the SMS *state*, `replicate_sms`
    correctly stood down as a whole — and took with it a table KeyCRM keeps
    filling. It froze at 08:00:53 that morning; `reconcile_sms` stands down on
    the same flag, so the drift would never have been reported. The tab would
    have gone on computing margin from an ever-older `purchased_price`, with
    new SKUs missing and their COGS therefore zero.

    These tests are the two halves of "it cannot happen here": the table is
    shipped and compared on this path, and this path has no writer-side switch
    to stand down on.
    """

    def test_it_is_shipped_by_the_operational_replicator(self):
        from core.pg_operational import _FULL_REPLACE

        entry = next(
            e for e in _FULL_REPLACE if e[0] == OFFER_STOCKS_TABLE
        )
        assert entry[1] == "offer_stocks"
        assert entry[2] == OFFER_STOCK_COLUMNS

    def test_it_is_compared_by_the_operational_reconciliation(self):
        spec = next(
            s for s in OPERATIONAL_TABLES if s.pg_table == OFFER_STOCKS_TABLE
        )
        assert spec.full_replace and spec.synced_column == "synced_at"
        # The money columns are DECIMAL on one side and NUMERIC on the other.
        assert set(spec.numeric) == {"price", "purchased_price"}

    def test_this_path_has_no_writer_switch_to_stand_down_on(self):
        """The actual defect was a guard with a wider blast radius than its
        premise, so what is asserted is the *absence* of that check from these
        two functions.

        Over the code, never over the text: the first version of this test read
        the raw source and failed on the paragraph above explaining the move —
        [[feedback_assert_on_structure_not_prose]] scoring against its own
        test. `ast.unparse` drops comments, and the docstrings are popped, so
        what is left is what runs.
        """
        from core import pg_operational
        from core.mirror_reconciliation import reconcile_operational

        for obj in (pg_operational, reconcile_operational):
            code = _code_only(obj)
            assert "KS_SMS_STORE" not in code
            assert "sms_store_is_postgres" not in code

    @pytest.mark.asyncio
    async def test_the_flag_does_not_stop_it_being_read(self, tmp_path, monkeypatch):
        """The behaviour, not the wiring: with the switch on, the rows still
        come back to be shipped."""
        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                out = read_full_replace(conn)
            assert out[OFFER_STOCKS_TABLE] == [(11, "SKU-11", 100.00, 40.00, 5, 1)]
        finally:
            await store.close()

    def test_the_migration_that_declared_it_still_declares_it(self):
        """It lives in 0013 with the SMS tables, not in 0008 with these — the
        code moved and the schema did not, which is correct and confusing
        enough to write down."""
        ddl = (REPO / "migrations" / "versions" / "0013_sms_state.py").read_text(
            encoding="utf-8",
        )
        body = ddl.split("CREATE TABLE bronze.offer_stocks (", 1)[1]
        declared = []
        for raw in body.splitlines():
            line = raw.split("--")[0].strip()
            if not line or line.startswith("PRIMARY KEY"):
                continue
            if line.startswith(")"):
                break
            match = re.match(
                r"^([a-z_]+)\s+(TEXT|INTEGER|NUMERIC|TIMESTAMPTZ)", line,
            )
            if match:
                declared.append(match.group(1))
        assert set(OFFER_STOCK_COLUMNS) <= set(declared), (
            set(OFFER_STOCK_COLUMNS) - set(declared)
        )


# ── the comparison ───────────────────────────────────────────────────────────


class TestTheComparisonSpecs:
    def test_all_five_whole_tables_are_full_replace(self):
        """The replicator writes every row it holds, so "in DuckDB and not in
        Postgres" has one meaning: lost. There is no retired category to excuse
        it with."""
        assert all(spec.full_replace for spec in OPERATIONAL_TABLES)

    def test_each_whole_table_has_a_clock(self):
        """Without one, `compare_table` cannot tell a row in flight from a row
        that was dropped, and every recently-written row is a CRITICAL."""
        assert all(spec.synced_column for spec in OPERATIONAL_TABLES)

    def test_a_finding_says_these_are_copies_not_a_shared_parse(self):
        """The default sentence is landing's and is true only of landing."""
        for spec in OPERATIONAL_TABLES:
            assert "copied out of DuckDB" in spec.origin_note
            assert "same parsed tuple" not in spec.origin_note

    def test_the_compared_columns_are_the_shipped_columns(self):
        """Two lists a file apart. If the shipper gains a column the comparison
        does not read, the column is copied and never checked."""
        shipped = {
            "bronze.offer_stocks": set(OFFER_STOCK_COLUMNS),
            "app.revenue_goals": set(GOAL_COLUMNS),
            "app.manual_expenses": set(MANUAL_EXPENSE_COLUMNS),
            "app.order_backfill_misses": set(MISS_COLUMNS),
            "app.inventory_history": set(INVENTORY_HISTORY_COLUMNS),
            "app.sku_inventory_status": set(SKU_STATUS_COLUMNS),
        }
        for spec in OPERATIONAL_TABLES:
            assert set(spec.columns) == shipped[spec.pg_table], spec.pg_table

    def test_the_fingerprinted_tables_check_what_they_ship(self):
        """`recorded_at` is the one deliberate exclusion: it is the clock the
        grace window reads, and a value that is also the clock can never be
        forgiven by itself."""
        compared = {s.pg_table: set(s.columns) for s in APPEND_ONLY_TABLES}
        assert compared[SKU_HISTORY_TABLE] == set(SKU_HISTORY_COLUMNS)
        assert compared[MOVEMENTS_TABLE] == set(MOVEMENT_COLUMNS) - {"recorded_at"}

    def test_the_grace_window_is_sized_to_the_replication_interval(self):
        """One hour plus half of it again — the rule SILVER_GRACE_MINUTES
        follows against KS_PG_SILVER_INTERVAL_S. Raise the job's interval and
        this must move with it."""
        from core.scheduler import BackgroundScheduler

        source = inspect.getsource(BackgroundScheduler._register_jobs)
        block = source[source.index('job_id="replicate_operational"'):]
        assert "IntervalTrigger(hours=1)" in block[:400]
        assert OPERATIONAL_GRACE_MINUTES == 90

    def test_the_day_bucket_means_the_same_number_in_both_engines(self):
        """20692 for 2026-08-27, checked against DuckDB here and against
        PostgreSQL 17.2 on the runtime. Two expressions that must agree, and
        nothing in the type system says so."""
        import duckdb

        from core.mirror_reconciliation import _DAY_BUCKET_DK, _DAY_BUCKET_PG

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t(date DATE)")
        conn.execute("INSERT INTO t VALUES (DATE '2026-08-27')")
        got = conn.execute(f"SELECT {_DAY_BUCKET_DK} FROM t").fetchone()[0]
        assert got == 20692
        # The Postgres half is plain date arithmetic and is asserted as text,
        # because there is no PostgreSQL in this suite to run it against.
        assert _DAY_BUCKET_PG == "(date - DATE '1970-01-01')"

    @pytest.mark.asyncio
    async def test_a_days_snapshot_is_one_bucket(self, tmp_path):
        """Bucketing `inventory_sku_history` by an id would put all 143,274
        rows in one bucket — there are only ~900 offers — and the drill-down
        would read the table to find one row."""
        store = await _store(tmp_path)
        try:
            await _seed(store)
            spec = next(
                s for s in APPEND_ONLY_TABLES if s.pg_table == SKU_HISTORY_TABLE
            )
            async with store.connection() as conn:
                prints = fingerprints(conn, spec)
            assert len(prints) == 3               # three days, three buckets
            assert all(int(v[0]) == 2 for v in prints.values())
        finally:
            await store.close()


class TestARowKeyThatIsNotANumber:
    """`app.inventory_history` is keyed by the day. `_row_key` coerced to int
    unconditionally, which was right for every table that existed when it was
    written and a TypeError for the first one that was not."""

    def test_a_date_key_survives(self):
        spec = next(
            s for s in OPERATIONAL_TABLES if s.pg_table == "app.inventory_history"
        )
        row = [date(2026, 8, 27), 1, 2, 3, 4, None]
        assert _row_key(spec, row) == date(2026, 8, 27)

    def test_a_date_is_reported_as_a_day_a_human_reads(self):
        assert _as_sample_id(date(2026, 8, 27)) == 20260827
        assert _as_sample_id(datetime(2026, 8, 27, 12, tzinfo=timezone.utc)) == 20260827

    def test_a_key_with_no_integer_form_is_dropped_rather_than_forced(self):
        """A sample id nobody can look anything up with is worse than none."""
        assert _as_sample_id("SKU-11") is None


class TestTheJob:
    def test_there_is_exactly_one_call_site(self):
        """Five code paths write these tables. Hooking each is how the sixth
        gets forgotten — which is what `update_manager_stats` did."""
        callers = []
        for path in (REPO / "core").rglob("*.py"):
            if path.name == "pg_operational.py":
                continue
            text = path.read_text(encoding="utf-8")
            callers += [
                f"{path.name}:{text[:m.start()].count(chr(10)) + 1}"
                for m in re.finditer(r"await replicate_operational\(", text)
            ]
        assert len(callers) == 1, callers

    def test_the_reconciliation_runs_after_gold(self):
        """A Silver or Gold finding is almost always the better explanation of
        an operational one, so it should be read first."""
        from core.scheduler import BackgroundScheduler

        source = inspect.getsource(BackgroundScheduler._run_dq_mirror_landing)
        assert source.index("await reconcile_gold(store)") < source.index(
            "await reconcile_operational(store)"
        )

    @pytest.mark.asyncio
    async def test_the_job_returns_both_copies(self):
        """`data/bot.db` rides the same job — same cadence, same grace window,
        one schedule to reason about."""
        from core.scheduler import BackgroundScheduler

        with patch("core.duckdb_store.get_store", new=AsyncMock()), \
             patch("core.pg_operational.replicate_operational",
                   new=AsyncMock(return_value={"movements_appended": 3})), \
             patch("core.pg_bot_state.replicate_bot_state",
                   new=AsyncMock(return_value={"rows": {"app.authorized_users": 24}})):
            result = await BackgroundScheduler()._run_replicate_operational()
        assert result["movements_appended"] == 3
        assert result["bot_state"]["rows"]["app.authorized_users"] == 24
