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
import dataclasses
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
from core.landing_rows import EXPENSE_TYPE_COLUMNS
from core.pg_operational import (
    BUYER_GENDER_COLUMNS,
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


def _no_watermarks():
    """What a first run — and `full=True` — hand to `read_appends`: every table
    at None, meaning "Postgres holds nothing, send the lot"."""
    from core.pg_operational import _APPEND_ABOVE

    return {spec.pg_table: None for spec in _APPEND_ABOVE}


def _watermarks(**marks):
    """`_no_watermarks()` with the named tables set. Keyword names are the
    Postgres table names, so callers pass them through `**{TABLE: value}`."""
    out = _no_watermarks()
    for table, value in marks.items():
        assert table in out, f"{table} is not an append table"
        out[table] = value
    return out


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
                out = read_appends(conn, _no_watermarks())
            assert len(out[SKU_HISTORY_TABLE]) == 6   # three days, two offers
            assert len(out[MOVEMENTS_TABLE]) == 4
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
                out = read_appends(
                    conn, _watermarks(**{SKU_HISTORY_TABLE: date(2026, 8, 2)}))
            days = {row[0] for row in out[SKU_HISTORY_TABLE]}
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
                out = read_appends(conn, _watermarks(**{MOVEMENTS_TABLE: 2}))
            assert [row[0] for row in out[MOVEMENTS_TABLE]] == [3, 4]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_nothing_new_ships_nothing(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                out = read_appends(conn, _watermarks(**{
                    SKU_HISTORY_TABLE: date(2026, 8, 4), MOVEMENTS_TABLE: 4,
                }))
            assert out[SKU_HISTORY_TABLE] == []
            assert out[MOVEMENTS_TABLE] == []
        finally:
            await store.close()


class TestTheRepairPath:
    """A row lost below the watermark is invisible to `id > MAX(id)` and to
    `date >= MAX(date)` forever. `full=True` is what puts it back."""

    def test_full_ignores_every_watermark(self):
        """`full` starts every table at None, which `read_appends` reads as
        "send the lot" — so the repair path cannot be narrowed by forgetting a
        table when the list grows."""
        source = inspect.getsource(replicate_operational)
        assert "spec.pg_table: None for spec in _APPEND_ABOVE" in source
        block = source[source.index("if not full:"):]
        assert "SELECT MAX({spec.watermark})" in block[:400]

    def test_full_upserts_and_the_incremental_path_does_not(self):
        """On the incremental path a conflict means the watermark logic is
        wrong and should say so, not overwrite and look fine.

        Read off the declaration now rather than off the source text: the
        branch is `spec.always_upsert or full`, so the behaviour lives in the
        list and asserting the list is asserting the behaviour."""
        from core.pg_operational import _APPEND_ABOVE

        source = inspect.getsource(replicate_operational)
        assert "upsert = spec.always_upsert or full" in source
        assert "if upsert else" in source

        by_table = {s.pg_table: s for s in _APPEND_ABOVE}
        assert not by_table[MOVEMENTS_TABLE].always_upsert
        assert by_table[SKU_HISTORY_TABLE].always_upsert

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
        # The watermarks are fetched, then `store.connection()` is entered.
        assert source.index("SELECT MAX({spec.watermark})") < source.index(
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
        does not read, the column is copied and never checked.

        Derived from `_FULL_REPLACE`, not restated. It used to carry its own
        dictionary of eight tables — a *third* list, which then had to be
        edited every time either of the other two grew, and which failed with
        a `KeyError` rather than a sentence when revision 0025 added four
        tables to both. A test that restates the thing it checks is a thing to
        maintain, not a guard.
        """
        from core.pg_operational import _FULL_REPLACE

        shipped = {entry[0]: set(entry[2]) for entry in _FULL_REPLACE}
        for spec in OPERATIONAL_TABLES:
            assert spec.pg_table in shipped, (
                f"{spec.pg_table} is compared and never shipped"
            )
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


class TestBuyerGenderIsShippedAndCompared:
    """The inferred-gender twin, wired the way the study's design requires.

    The value has no source but DuckDB — KeyCRM carries no gender field at all
    — so if the copy silently stopped, nothing downstream could re-derive it
    and the SMS audience would read a table frozen at whatever it last held.
    The same two halves as `offer_stocks` above: it is shipped on the hourly
    path, and it is compared on the daily one.
    """

    def test_it_is_shipped_by_the_operational_replicator(self):
        from core.pg_operational import (
            BUYER_GENDER_COLUMNS, BUYER_GENDER_TABLE, _FULL_REPLACE,
        )

        entry = next(e for e in _FULL_REPLACE if e[0] == BUYER_GENDER_TABLE)
        assert entry[1] == "buyer_gender"
        assert entry[2] == BUYER_GENDER_COLUMNS

    def test_it_is_compared_by_the_operational_reconciliation(self):
        from core.pg_operational import BUYER_GENDER_COLUMNS, BUYER_GENDER_TABLE

        spec = next(
            s for s in OPERATIONAL_TABLES if s.pg_table == BUYER_GENDER_TABLE
        )
        assert spec.dk_table == "buyer_gender"
        assert spec.columns == BUYER_GENDER_COLUMNS
        assert spec.key_columns == ("buyer_id",)
        # Full replace, so a row missing from Postgres was LOST and is reported
        # CRITICAL — there is no retired category for a table whose writer
        # rewrites it whole.
        assert spec.full_replace

    def test_the_stamp_is_shipped_and_never_compared(self):
        """A full re-derivation stamps all 20 145 rows in one pass.

        Comparing `decided_at` would ask the two copies to have been taken at
        the same instant, which they never are — `sku_inventory_status.
        updated_at` is the same situation and the same exclusion. The verdict
        itself is compared, which is what the check is for.
        """
        from core.pg_operational import BUYER_GENDER_TABLE

        spec = next(
            s for s in OPERATIONAL_TABLES if s.pg_table == BUYER_GENDER_TABLE
        )
        assert spec.synced_column == "decided_at"
        assert "decided_at" in spec.ignore_columns
        for column in ("gender", "method", "confidence", "override_by_human"):
            assert column not in spec.ignore_columns, column

    def test_the_schema_revision_carries_the_table(self):
        """migrate must be rebuilt alongside web, or every read raises."""
        from core.pg import REQUIRED_REVISION

        # Not a literal any more: this pinned the constant to the revision
        # that introduced `buyer_gender`, so every later revision broke a test
        # about a table it does not touch. What matters here is that the
        # revision carrying this table exists and is not ahead of what the
        # application demands.
        versions = Path(__file__).resolve().parents[2] / "migrations" / "versions"
        assert (versions / "0024_buyer_gender.py").exists()
        assert REQUIRED_REVISION >= "0024_buyer_gender"


class TestTheForecastGroup:
    """Four tables, 313 rows, and the reason they went first: they are what the
    last two dashboard reads are waiting on.

    The load-bearing assertion here is not that they replicate — it is that the
    *two lists* agree. `core/pg_operational.py` decides what is shipped and
    `core/mirror_reconciliation.py` decides what is compared, and they are
    separate declarations. A table on the first and not the second is copied
    and never checked, which is the shape of a mirror rotting quietly.
    """

    FORECAST = ("app.revenue_predictions", "app.seasonal_indices",
                "app.weekly_patterns", "app.growth_metrics")

    def test_all_four_are_shipped(self):
        from core.pg_operational import _FULL_REPLACE

        shipped = {entry[0] for entry in _FULL_REPLACE}
        for table in self.FORECAST:
            assert table in shipped, f"{table} is not replicated"

    def test_all_four_are_compared(self):
        from core.mirror_reconciliation import OPERATIONAL_TABLES

        compared = {spec.pg_table for spec in OPERATIONAL_TABLES}
        for table in self.FORECAST:
            assert table in compared, (
                f"{table} is shipped and never compared — a copy nobody checks"
            )

    def test_the_two_lists_agree_on_every_table(self):
        """Not just the four: the whole of both lists. Adding to one and
        forgetting the other is the failure this pins, and it does not care
        which table it happens to."""
        from core.pg_operational import _FULL_REPLACE
        from core.mirror_reconciliation import OPERATIONAL_TABLES

        shipped = {entry[0] for entry in _FULL_REPLACE}
        compared = {spec.pg_table for spec in OPERATIONAL_TABLES}
        assert shipped == compared, (
            f"only shipped: {sorted(shipped - compared)}\n"
            f"only compared: {sorted(compared - shipped)}"
        )

    def test_the_column_tuples_are_the_shipped_ones(self):
        """The reconciliation imports them rather than restating them, which is
        what stops the comparison quietly checking a different set of columns
        from the set being written. Asserted rather than trusted."""
        from core.pg_operational import _FULL_REPLACE
        from core.mirror_reconciliation import OPERATIONAL_TABLES

        by_table = {e[0]: e[2] for e in _FULL_REPLACE}
        for spec in OPERATIONAL_TABLES:
            if spec.pg_table in self.FORECAST:
                assert tuple(spec.columns) == tuple(by_table[spec.pg_table]), spec.pg_table

    def test_the_migration_creates_what_is_shipped(self):
        """The third list. A column in the tuple and not in the DDL fails at
        the first INSERT, in production, on the hourly job."""
        import re
        from pathlib import Path

        from core.pg_operational import _FULL_REPLACE

        ddl = (Path(__file__).resolve().parents[2] / "migrations" / "versions"
               / "0025_forecast_tables.py").read_text(encoding="utf-8")
        by_table = {e[0]: e[2] for e in _FULL_REPLACE}
        for table in self.FORECAST:
            bare = table.split(".", 1)[1]
            block = re.search(
                rf"CREATE TABLE IF NOT EXISTS app\.{bare} \((.*?)\n        \)",
                ddl, re.S)
            assert block, f"{table} has no DDL in revision 0025"
            for column in by_table[table]:
                assert re.search(rf"\b{column}\b", block.group(1)), (
                    f"{table}.{column} is shipped and not created"
                )

    # A `test_the_required_revision_moved` stood here, asserting the constant
    # equalled the newest revision file. It was wrong in kind: it derived the
    # answer from the directory, so it could never trip, and it sat on top of
    # `test_order_versions.py`'s deliberate literal — which exists precisely so
    # that moving the pin is something a human types. Two guards where the
    # weaker one silently covers for the stronger is worse than one.



class TestTheWatchdogsAndLedgers:
    """Five tables, 1 240 rows, and one shipping shape between them.

    They share no subject — a disk reading and a record that a report was sent
    have nothing to say to each other — and that is deliberate. The twelve
    tables still homeless in DuckDB do not divide usefully by what they mean,
    but they divide exactly by *mechanism*, and the mechanism is the part that
    can be wrong. These five are the ones `_FULL_REPLACE` already carries with
    no new machinery, so a defect found here is a defect in these five and not
    in a watermark, a parent's watermark, a key filter or a shared parse.

    The two assertions with teeth are the ones about `numeric` and about the
    clock, because both decide whether the daily comparison is a check or a
    siren. See revision 0026 for the arguments; these pin the conclusions.
    """

    WATCHDOGS = ("app.disk_samples", "app.data_dir_samples", "app.memory_samples")
    LEDGERS = ("app.weekly_report_sends", "app.traffic_report_sends")

    @property
    def all_five(self):
        return self.WATCHDOGS + self.LEDGERS

    def _spec(self, table):
        from core.mirror_reconciliation import OPERATIONAL_TABLES

        return next(s for s in OPERATIONAL_TABLES if s.pg_table == table)

    def test_all_five_are_shipped_and_compared(self):
        from core.mirror_reconciliation import OPERATIONAL_TABLES
        from core.pg_operational import _FULL_REPLACE

        shipped = {entry[0] for entry in _FULL_REPLACE}
        compared = {spec.pg_table for spec in OPERATIONAL_TABLES}
        for table in self.all_five:
            assert table in shipped, f"{table} is not replicated"
            assert table in compared, f"{table} is shipped and never compared"

    def test_no_watchdog_measure_is_declared_numeric(self):
        """The absence is the decision, so it is asserted rather than assumed.

        Every measure on these three is DOUBLE in DuckDB and DOUBLE PRECISION
        in Postgres, so both sides hand back a Python float. Listing one in
        `numeric=` coerces that side to `Decimal`, and `_normalise_row` then
        finds 1.0 != Decimal('1.0') on every row of every sample, for ever, at
        a tolerance of zero. It is an attractive mistake because the columns
        are measurements and measurements usually want NUMERIC.
        """
        for table in self.WATCHDOGS:
            assert self._spec(table).numeric == (), (
                f"{table} declares a numeric column; its measures are floats "
                f"on both sides and coercing one of them manufactures a "
                f"difference on every row"
            )

    def test_both_ledgers_declare_their_money(self):
        """The mirror image of the rule above, and the same rule: follow the
        source type. `revenue` is DECIMAL(14, 2) in DuckDB and NUMERIC(14, 2)
        here, a Decimal on both sides, so it is listed."""
        for table in self.LEDGERS:
            assert self._spec(table).numeric == ("revenue",), table

    def test_every_clock_here_is_per_row_and_stays_compared(self):
        """These are the first specs in a while with an empty `ignore_columns`,
        and that is the point rather than an oversight.

        The four forecast specs above all hide their stamp, because one
        re-derivation writes one value to every row and comparing it would ask
        the two copies to have been taken at the same instant. A sample is the
        opposite: one reading at one moment, and a ledger row is one delivery
        at one moment. Hiding those stamps would throw away the only column
        that can catch a sample copied with the wrong timestamp.
        """
        for table in self.all_five:
            spec = self._spec(table)
            assert spec.ignore_columns == (), (
                f"{table} hides a column; its clock dates the row it sits on"
            )
            assert spec.stamp_is_per_row, table
            assert spec.synced_column in spec.columns, (
                f"{table}'s clock is not among the shipped columns"
            )

    def test_the_seven_path_groups_need_the_composite_key(self):
        """One sweep writes one row per path group, all sharing a `sampled_at`
        — seven of them in production. Keyed on the timestamp alone, six would
        collide and a healthy sweep would read as six lost rows."""
        assert self._spec("app.data_dir_samples").key_columns == (
            "sampled_at", "path_group")

    def test_the_ledgers_are_two_tables_and_not_one(self):
        """`weekly_report_sends.sales_type` means a sales type, and "traffic"
        is not one — the reason DuckDB keeps them apart, carried across rather
        than quietly tidied up on the way."""
        from core.pg_operational import _FULL_REPLACE

        by_pg = {e[0]: e[1] for e in _FULL_REPLACE}
        assert by_pg["app.weekly_report_sends"] == "weekly_report_sends"
        assert by_pg["app.traffic_report_sends"] == "traffic_report_sends"

    def test_the_migration_creates_what_is_shipped(self):
        """The third list, for these five.

        It searches every revision rather than one named file, unlike the
        forecast group's version of this test. The invariant is that the DDL
        exists at all — which revision introduced a table is history, and a
        test that names one has to be edited whenever a column is added in a
        later revision. `warehouse_refreshes` will need exactly that: its
        fourteenth column arrived in a migration after its CREATE TABLE.
        """
        import re
        from pathlib import Path

        from core.pg_operational import _FULL_REPLACE

        versions = Path(__file__).resolve().parents[2] / "migrations" / "versions"
        ddl = "\n".join(
            f.read_text(encoding="utf-8") for f in sorted(versions.glob("0*.py"))
        )
        by_table = {e[0]: e[2] for e in _FULL_REPLACE}
        for table in self.all_five:
            bare = table.split(".", 1)[1]
            block = re.search(
                rf"CREATE TABLE IF NOT EXISTS app\.(?:\{{table\}}|{bare}) \((.*?)\n\s*\)",
                ddl, re.S)
            assert block, f"{table} has no DDL in any revision"
            for column in by_table[table]:
                assert re.search(rf"\b{column}\b", block.group(1)), (
                    f"{table}.{column} is shipped and not created"
                )

    def test_limit_mb_is_the_one_nullable_measure(self):
        """Zero NULLs in production today, and nullable anyway.

        `core/memory_monitor.py` writes None whenever the cgroup reports
        `memory.max` as the literal "max". Declaring NOT NULL on the strength
        of today's data means the first unlimited container aborts the whole
        hourly transaction and takes the other twelve tables with it.
        """
        import re
        from pathlib import Path

        ddl = (Path(__file__).resolve().parents[2] / "migrations" / "versions"
               / "0026_watchdogs_and_ledgers.py").read_text(encoding="utf-8")
        block = re.search(
            r"CREATE TABLE IF NOT EXISTS app\.memory_samples \((.*?)\n\s*\)",
            ddl, re.S).group(1)
        limit_line = next(l for l in block.splitlines() if "limit_mb" in l)
        assert "NOT NULL" not in limit_line, limit_line
        for other in ("working_set_mb", "page_cache_mb", "oom_kills"):
            line = next(l for l in block.splitlines() if other in l)
            assert "NOT NULL" in line, line


class TestRetentionIsNotLoss:
    """The one new idea in revision 0026, and the only thing in it that could
    make the daily comparison lie.

    `full_replace` ends every run with the two copies identical, so an orphan
    can only be a row that left DuckDB *after* the copy. On twelve of the
    fourteen tables that is a defect. On the three watchdog samples it is the
    retention sweep: `DELETE FROM <t> WHERE sampled_at < <cutoff>`, the memory
    one every thirty minutes, against an hourly replication and a 07:30 check.
    Left as plain orphans it would have been a WARN a day, for ever, invented
    entirely by the migration that created the copy.
    """

    from datetime import datetime, timedelta, timezone as _tz

    @staticmethod
    def _spec(table):
        from core.mirror_reconciliation import OPERATIONAL_TABLES

        return next(s for s in OPERATIONAL_TABLES if s.pg_table == table)

    @staticmethod
    def _sample_rows(stamps):
        """`{key: row}` and `{key: clock}` for `app.memory_samples`, keyed on
        `sampled_at` — which is both the key and the first column."""
        rows = {s: (s, 100.0, 10.0, 7000.0, 0) for s in stamps}
        return rows, {s: s for s in stamps}

    def test_the_three_watchdogs_declare_it_and_nothing_else_does(self):
        from core.mirror_reconciliation import OPERATIONAL_TABLES

        sweeping = {s.pg_table for s in OPERATIONAL_TABLES if s.prunes_by_age}
        assert sweeping == {
            "app.disk_samples", "app.data_dir_samples", "app.memory_samples",
        }, (
            "only the tables whose DuckDB writer deletes by age may claim this; "
            "on every other table an orphan below the floor is a lost row"
        )

    def test_a_row_older_than_everything_duckdb_holds_is_a_sweep(self):
        from core.mirror_reconciliation import compare_table

        now = self.datetime(2026, 9, 14, 7, 30, tzinfo=self._tz.utc)
        kept = [now - self.timedelta(hours=n) for n in (1, 2, 3)]
        aged = now - self.timedelta(days=14, hours=1)

        dk_rows, dk_synced = self._sample_rows(kept)
        pg_rows, _ = self._sample_rows(kept + [aged])

        issues = compare_table(
            self._spec("app.memory_samples"), dk_rows, dk_synced, pg_rows,
            {"last_ok_at": now - self.timedelta(minutes=20), "last_rows": 3},
            now=now,
        )
        names = {i.check_name for i in issues}
        assert "mirror_pruned_rows" in names, names
        assert "mirror_orphan_rows" not in names, (
            "the aged row was reported as an orphan; this is the daily WARN "
            "the mechanism exists to prevent"
        )
        pruned = next(i for i in issues if i.check_name == "mirror_pruned_rows")
        assert pruned.count == 1
        assert pruned.severity.name == "INFO"

    def test_a_row_inside_the_window_is_still_an_orphan(self):
        """The half that keeps it a check. A row Postgres holds that is *newer*
        than DuckDB's oldest cannot have aged out — nothing else deletes from
        these tables, so something is wrong."""
        from core.mirror_reconciliation import compare_table

        now = self.datetime(2026, 9, 14, 7, 30, tzinfo=self._tz.utc)
        kept = [now - self.timedelta(hours=n) for n in (1, 3, 5)]
        intruder = now - self.timedelta(hours=2)   # between the oldest and newest

        dk_rows, dk_synced = self._sample_rows(kept)
        pg_rows, _ = self._sample_rows(kept + [intruder])

        issues = compare_table(
            self._spec("app.memory_samples"), dk_rows, dk_synced, pg_rows,
            {"last_ok_at": now - self.timedelta(minutes=20), "last_rows": 3},
            now=now,
        )
        names = {i.check_name for i in issues}
        assert "mirror_orphan_rows" in names, names
        assert "mirror_pruned_rows" not in names, names

    def test_a_table_that_does_not_sweep_reports_the_same_row_as_an_orphan(self):
        """The flag is the whole difference, and it is asserted rather than
        described: an `app.revenue_goals` row older than anything DuckDB holds
        is a row DuckDB lost."""
        from core.mirror_reconciliation import compare_table

        now = self.datetime(2026, 9, 14, 7, 30, tzinfo=self._tz.utc)
        spec = self._spec("app.memory_samples")
        not_sweeping = dataclasses.replace(spec, prunes_by_age=False)

        kept = [now - self.timedelta(hours=n) for n in (1, 2, 3)]
        aged = now - self.timedelta(days=14, hours=1)
        dk_rows, dk_synced = self._sample_rows(kept)
        pg_rows, _ = self._sample_rows(kept + [aged])

        issues = compare_table(
            not_sweeping, dk_rows, dk_synced, pg_rows,
            {"last_ok_at": now - self.timedelta(minutes=20), "last_rows": 3},
            now=now,
        )
        assert "mirror_orphan_rows" in {i.check_name for i in issues}

    def test_an_empty_duckdb_side_forgives_nothing(self):
        """No floor exists, and an empty source is a far larger finding than a
        sweep. Every orphan stands."""
        from core.mirror_reconciliation import compare_table

        now = self.datetime(2026, 9, 14, 7, 30, tzinfo=self._tz.utc)
        aged = now - self.timedelta(days=14, hours=1)
        pg_rows, _ = self._sample_rows([aged])

        issues = compare_table(
            self._spec("app.memory_samples"), {}, {}, pg_rows,
            {"last_ok_at": now - self.timedelta(minutes=20), "last_rows": 0},
            now=now,
        )
        names = {i.check_name for i in issues}
        assert "mirror_pruned_rows" not in names, names

    def test_the_finding_is_registered_and_has_a_human_name(self):
        """An unregistered check name is a finding that cannot become an alert
        — `core/alerting.py`'s registry is exact-match."""
        from core.alerting import REGISTRY
        from core.data_quality import HUMAN_CHECK_NAMES

        assert "mirror_pruned_rows" in REGISTRY
        assert "mirror_pruned_rows" in HUMAN_CHECK_NAMES


class TestTheAppendListIsNowDeclared:
    """The append path used to be two tables written out longhand in five
    places: the watermark reads, the reader's signature, its two-tuple return,
    the write branch and the two row-count stamps. Revision 0027 needed two
    more and the quality journal needs three after that, so it became a list.

    These tests exist because that refactor touches the path that ships 193 000
    rows an hour. What they pin is that the two original tables kept their
    exact behaviour — the `>=` that `inventory_sku_history` needs and the `>`
    that everything id-keyed needs are not interchangeable, and neither is the
    upsert.
    """

    def test_the_two_original_tables_kept_their_operators(self):
        from core.pg_operational import _APPEND_ABOVE

        by_table = {s.pg_table: s for s in _APPEND_ABOVE}
        sku = by_table["app.inventory_sku_history"]
        assert sku.inclusive, (
            "`date >= MAX(date)` re-ships the day it resumes from on purpose; "
            "`>` would step over a half-written day and leave a hole nothing "
            "would ever fill"
        )
        assert sku.always_upsert, "it re-ships a day, so it must overwrite it"

        movements = by_table["app.stock_movements"]
        assert not movements.inclusive
        assert not movements.always_upsert, (
            "a plain INSERT on the incremental path is the point: a row above "
            "MAX(id) cannot already be there, so a conflict means the "
            "watermark logic is wrong and should say so"
        )

    def test_every_id_keyed_table_asks_for_strictly_greater(self):
        """`>=` on a monotone id re-ships the last row every run for nothing —
        and, on the incremental path where the write is a plain INSERT, it
        would conflict every single time."""
        from core.pg_operational import _APPEND_ABOVE

        for spec in _APPEND_ABOVE:
            if spec.watermark == "id":
                assert not spec.inclusive, spec.pg_table
                assert not spec.always_upsert, spec.pg_table

    def test_the_reader_asks_for_the_whole_table_when_there_is_no_watermark(self):
        """A watermark of None is what `full=True` sets and what a first run
        finds. It must mean "everything", not "nothing"."""
        from core.pg_operational import _APPEND_ABOVE, read_appends

        asked = []

        class Conn:
            def execute(self, sql, params=None):
                asked.append((" ".join(sql.split()), params))
                return self

            def fetchall(self):
                return []

        read_appends(Conn(), {s.pg_table: None for s in _APPEND_ABOVE})
        assert len(asked) == len(_APPEND_ABOVE)
        for sql, params in asked:
            assert "WHERE" not in sql, sql
            assert params is None

    def test_the_reader_bounds_each_table_by_its_own_watermark(self):
        from core.pg_operational import _APPEND_ABOVE, read_appends

        asked = []

        class Conn:
            def execute(self, sql, params=None):
                asked.append((" ".join(sql.split()), params))
                return self

            def fetchall(self):
                return []

        read_appends(Conn(), {s.pg_table: 7 for s in _APPEND_ABOVE})
        by_sql = {sql: params for sql, params in asked}
        assert any("WHERE date >= ?" in sql for sql in by_sql), by_sql.keys()
        assert sum("WHERE id > ?" in sql for sql in by_sql) == 3, (
            "stock_movements, warehouse_refreshes and reconciliation_log are "
            "all id-keyed and all ask for strictly greater"
        )

    def test_the_two_forensic_logs_joined_the_list(self):
        from core.pg_operational import _APPEND_ABOVE

        tables = {s.pg_table for s in _APPEND_ABOVE}
        assert "app.warehouse_refreshes" in tables
        assert "app.reconciliation_log" in tables

    def test_they_are_compared_and_the_columns_match_what_is_shipped(self):
        """The three-list invariant, for the append shape. A column shipped and
        not compared is a column copied and never checked."""
        from core.mirror_reconciliation import APPEND_ONLY_TABLES
        from core.pg_operational import _APPEND_ABOVE

        shipped = {s.pg_table: set(s.columns) for s in _APPEND_ABOVE}
        compared = {s.pg_table: set(s.columns) for s in APPEND_ONLY_TABLES}
        assert set(shipped) == set(compared), (
            f"only shipped: {sorted(set(shipped) - set(compared))}\n"
            f"only compared: {sorted(set(compared) - set(shipped))}"
        )
        for table in ("app.warehouse_refreshes", "app.reconciliation_log"):
            assert shipped[table] == compared[table], table

    def test_silver_mode_is_shipped_despite_not_being_in_the_create_table(self):
        """It arrives by ALTER TABLE in DuckDB migration 0005. A twin built by
        reading the CREATE TABLE alone is one column short, and the shipper
        would then never carry the column that says whether Silver was rebuilt
        whole or incrementally."""
        from core.pg_operational import REFRESH_COLUMNS

        assert "silver_mode" in REFRESH_COLUMNS

    def test_the_forensic_ids_are_carried_not_generated(self):
        """`app.stock_movements`' decision, for the same reason: a Postgres
        IDENTITY would give one warehouse tick two names and the bucketed
        comparison joins on the id."""
        import re
        from pathlib import Path

        ddl = (Path(__file__).resolve().parents[2] / "migrations" / "versions"
               / "0027_forensics.py").read_text(encoding="utf-8")
        for table in ("warehouse_refreshes", "reconciliation_log"):
            block = re.search(
                rf"CREATE TABLE IF NOT EXISTS app\.{table} \((.*?)\n\s*\)",
                ddl, re.S).group(1)
            id_line = next(l for l in block.splitlines() if l.strip().startswith("id "))
            assert "GENERATED" not in id_line.upper(), id_line
            assert "DEFAULT" not in id_line.upper(), id_line

    def test_the_nullable_columns_that_carry_meaning_stay_nullable(self):
        """Three columns where NULL is a fact and not an absence.

        `gold_products_rows` is NULL on every row since the products Gold was
        retired — a 0 would read as "built nothing" when the truth is that
        there is no such layer. `checksum_match` and `validation_passed` are
        NULL on the error path, which means "we never got far enough to
        compare" rather than FALSE. `silver_mode` is NULL both before its
        migration ran and on every error-path row.
        """
        import re
        from pathlib import Path

        ddl = (Path(__file__).resolve().parents[2] / "migrations" / "versions"
               / "0027_forensics.py").read_text(encoding="utf-8")
        block = re.search(
            r"CREATE TABLE IF NOT EXISTS app\.warehouse_refreshes \((.*?)\n\s*\)",
            ddl, re.S).group(1)
        for column in ("gold_products_rows", "checksum_match",
                       "validation_passed", "silver_mode", "error"):
            line = next(l for l in block.splitlines() if l.strip().startswith(column))
            assert "NOT NULL" not in line, line

    def test_error_is_unbounded_text(self):
        """`str(e)` of an arbitrary exception, and the column the self-heal
        budget keys on — a truncating type would change behaviour, not just
        storage."""
        import re
        from pathlib import Path

        ddl = (Path(__file__).resolve().parents[2] / "migrations" / "versions"
               / "0027_forensics.py").read_text(encoding="utf-8")
        assert re.search(r"error\s+TEXT", ddl), "error must not be VARCHAR(n)"


class TestTheQualityJournal:
    """A run and its two findings tables, and the one thing that makes them
    comparable at all: the children have no clock and borrow the run's.

    Neither child carries a timestamp — seven columns and nine columns, none of
    them a time. The daily comparison forgives a row written between the copy
    and the check, and it needs a clock to do that; without one, every finding
    the 07:00 integrity scan wrote is CRITICAL at the 07:30 comparison, most
    mornings.
    """

    JOURNAL = ("app.data_quality_runs", "app.data_quality_issues",
               "app.data_quality_diffs")

    @staticmethod
    def _spec(table):
        from core.mirror_reconciliation import OPERATIONAL_TABLES

        return next(s for s in OPERATIONAL_TABLES if s.pg_table == table)

    def test_all_three_are_shipped_and_compared(self):
        from core.mirror_reconciliation import OPERATIONAL_TABLES
        from core.pg_operational import _FULL_REPLACE

        shipped = {e[0] for e in _FULL_REPLACE}
        compared = {s.pg_table for s in OPERATIONAL_TABLES}
        for table in self.JOURNAL:
            assert table in shipped, table
            assert table in compared, table

    def test_the_parent_ships_before_its_children(self):
        """One transaction, and the order inside it still matters: it must
        never hold findings without the run they belong to, even for a
        statement."""
        from core.pg_operational import _FULL_REPLACE

        order = [e[0] for e in _FULL_REPLACE]
        parent = order.index("app.data_quality_runs")
        for child in ("app.data_quality_issues", "app.data_quality_diffs"):
            assert parent < order.index(child), child

    def test_each_child_borrows_the_runs_clock(self):
        """And borrows it from the run table by name, not from a column of its
        own that does not exist."""
        for child in ("app.data_quality_issues", "app.data_quality_diffs"):
            clock = self._spec(child).synced_column
            assert clock, child
            assert "data_quality_runs" in clock, clock
            assert "started_at" in clock, clock

    def test_the_borrowed_clock_is_not_a_compared_column(self):
        """It is an expression, not a column the table has — so it can never
        appear in `columns`, and `stamp_is_per_row` must still hold because
        each run has its own moment."""
        for child in ("app.data_quality_issues", "app.data_quality_diffs"):
            spec = self._spec(child)
            assert spec.synced_column not in spec.columns
            assert spec.ignore_columns == ()
            assert spec.stamp_is_per_row

    def test_the_diff_values_are_not_declared_numeric(self):
        """They look like money and are DOUBLE on both sides. Declaring them
        coerces one side to Decimal and reports every row as differing — the
        same trap as the watchdog measures, arrived at from the opposite
        direction because these ones really are quantities of hryvnia."""
        assert self._spec("app.data_quality_diffs").numeric == ()

    def test_the_natural_keys_are_the_measured_ones(self):
        assert self._spec("app.data_quality_issues").key_columns == (
            "run_id", "check_name", "table_name")
        assert self._spec("app.data_quality_diffs").key_columns == (
            "run_id", "month", "source_id", "diff_class", "field")

    def test_no_foreign_key_is_declared(self):
        """DuckDB has none, the three ship in one transaction in parent-first
        order, and an FK would turn a partial shipment into a failure of the
        whole hourly replication rather than a finding the next comparison
        reports."""
        from pathlib import Path

        ddl = (Path(__file__).resolve().parents[2] / "migrations" / "versions"
               / "0028_quality_journal.py").read_text(encoding="utf-8")
        assert "REFERENCES" not in ddl.upper()

    def test_the_text_columns_are_text_and_not_json(self):
        """`sample_ids` and `order_ids` hold JSON arrays as strings. A json or
        jsonb column re-renders them on the round trip and the zero-tolerance
        comparison then reports every row. `order_ids` NULL is meaningful too —
        it is None precisely when the discrepancy carries no ids — so no
        DEFAULT."""
        import re
        from pathlib import Path

        source = (Path(__file__).resolve().parents[2] / "migrations" / "versions"
                  / "0028_quality_journal.py").read_text(encoding="utf-8")
        # The CREATE TABLE bodies only. The module docstring explains why json
        # is the wrong type, and a search of the whole file would match that
        # explanation — which is the prose-versus-structure mistake this suite
        # keeps relearning. Caught by this very test on its first run.
        ddl = "\n".join(
            m.group(0) for m in re.finditer(
                r"CREATE TABLE IF NOT EXISTS app\.\w+ \(.*?\n\s*\)",
                source, re.S)
        )
        assert ddl.count("CREATE TABLE") == 3, "expected three statements"
        assert "JSON" not in ddl.upper()
        for column in ("sample_ids", "order_ids", "description",
                       "error_message"):
            # The DDL is inside a docstring as well as in the statements, so
            # take the declaration line: the one that names the column first
            # and then a type.
            line = next(
                l for l in ddl.splitlines()
                if l.strip().startswith(column + " ") and "TEXT" in l
            )
            assert "NOT NULL" not in line, line
            assert "DEFAULT" not in line, line


class TestOffersTravelWithTheirStocks:
    """`bronze.offers` is landing data that is replicated rather than mirrored,
    and that is the interesting half.

    The obvious home is `core/pg_landing.py` — parse the KeyCRM payload once
    and write the same tuple to both stores. It is the wrong answer for
    `bronze.offer_stocks`' reason, and the two now share it: they are halves of
    one inventory sync, `sku_inventory_status` is built by joining them, and
    `stock_movements` is a delta against `offer_stocks` computed on the same
    tick. Split across the mirror and the replicator they would have two clocks
    and two ways to stand down, which is what silently froze `offer_stocks` at
    08:00:53 on 2026-09-06 with nothing to report it.
    """

    def test_offers_and_offer_stocks_ship_in_the_same_call(self):
        from core.pg_operational import _FULL_REPLACE

        tables = [e[0] for e in _FULL_REPLACE]
        assert "bronze.offers" in tables
        assert "bronze.offer_stocks" in tables

    def test_offers_is_not_in_the_landing_mirror(self):
        """Two shippers for one table is two clocks for one table."""
        from core.mirror_reconciliation import MIRRORED_TABLES

        assert "bronze.offers" not in {s.pg_table for s in MIRRORED_TABLES}

    def test_the_stamp_is_hidden_because_one_sync_writes_it_to_every_row(self):
        from core.mirror_reconciliation import OPERATIONAL_TABLES

        spec = next(s for s in OPERATIONAL_TABLES if s.pg_table == "bronze.offers")
        assert spec.ignore_columns == ("synced_at",)
        assert not spec.stamp_is_per_row


class TestTheSyncWatermarksAreAProjection:
    """Ten of the eleven `sync_metadata` keys cross. One does not, and the
    exclusion is the whole content of this spec.

    `warehouse_catalog_dirty` is a coordination flag — set when a catalogue
    sync changes something, read every two minutes by the warehouse refresh,
    and deleted *conditionally on its own `updated_at`*. Postgres runs no
    warehouse rebuild, so a copy there asserts something that cannot be true;
    and it would be wrong in a recurring way, because the flag lives about two
    minutes against an hourly copy and a 07:30 check, so a window that catches
    it set at the copy and cleared at the check reports an orphan the copy
    itself created — with no grace, because `mirror_orphan_rows` has none.
    """

    def test_the_excluded_key_is_named_once(self):
        """Once, so the shipper and the comparison cannot come to disagree
        about what the Postgres copy is supposed to contain."""
        from core.pg_operational import (
            SYNC_METADATA_SOURCE, TRANSIENT_SYNC_KEY,
        )

        assert TRANSIENT_SYNC_KEY == "warehouse_catalog_dirty"
        assert TRANSIENT_SYNC_KEY in SYNC_METADATA_SOURCE
        assert "key <>" in SYNC_METADATA_SOURCE

    def test_the_shipper_and_the_comparison_read_the_same_source(self):
        from core.mirror_reconciliation import OPERATIONAL_TABLES
        from core.pg_operational import _FULL_REPLACE, SYNC_METADATA_SOURCE

        shipped = next(e for e in _FULL_REPLACE if e[0] == "app.sync_metadata")
        compared = next(s for s in OPERATIONAL_TABLES
                        if s.pg_table == "app.sync_metadata")
        assert shipped[1] == SYNC_METADATA_SOURCE
        assert compared.dk_table == SYNC_METADATA_SOURCE

    def test_the_derived_table_is_aliased(self):
        """DuckDB refuses an unnamed derived table, and this one is
        interpolated straight after `FROM`. Found by a real defect in the
        /traffic port, not by reading the manual."""
        from core.pg_operational import SYNC_METADATA_SOURCE

        assert SYNC_METADATA_SOURCE.rstrip().endswith("AS sync_metadata")

    def test_the_durable_keys_are_not_filtered_out(self):
        """The projection removes one key, not a class of them. A filter that
        also dropped `last_sync_orders` would leave a stage-4 writer with no
        idea where it left off."""
        from core.pg_operational import SYNC_METADATA_SOURCE

        for key in ("last_sync_orders", "last_sync_products",
                    "dq_digest_last_sent"):
            assert key not in SYNC_METADATA_SOURCE

    def test_value_is_unbounded_text(self):
        """DuckDB's VARCHAR has no length. A VARCHAR(n) here raises on the
        first value that outgrows it, inside the hourly transaction, taking
        sixteen other tables with it."""
        import re
        from pathlib import Path

        ddl = (Path(__file__).resolve().parents[2] / "migrations" / "versions"
               / "0029_offers_and_sync_metadata.py").read_text(encoding="utf-8")
        block = re.search(
            r"CREATE TABLE IF NOT EXISTS app\.sync_metadata \((.*?)\n\s*\)",
            ddl, re.S).group(1)
        line = next(l for l in block.splitlines() if l.strip().startswith("value "))
        assert "TEXT" in line, line


class TestStageThreeIsComplete:
    """Every table this migration set out to give a home now has one.

    Sixteen DuckDB tables had no Postgres counterpart when stage 3 began. Four
    crossed with revision 0025, five with 0026, two with 0027, three with 0028
    and two with 0029. This asserts the arithmetic rather than trusting a
    handoff note, because the whole point of the stage is that nothing is left
    behind when DuckDB goes.
    """

    STAGE_THREE = (
        # 0025 — the forecast group
        "app.revenue_predictions", "app.seasonal_indices",
        "app.weekly_patterns", "app.growth_metrics",
        # 0026 — the watchdogs and the ledgers
        "app.disk_samples", "app.data_dir_samples", "app.memory_samples",
        "app.weekly_report_sends", "app.traffic_report_sends",
        # 0027 — the forensic logs
        "app.warehouse_refreshes", "app.reconciliation_log",
        # 0028 — the quality journal
        "app.data_quality_runs", "app.data_quality_issues",
        "app.data_quality_diffs",
        # 0029 — the catalogue and the watermarks
        "bronze.offers", "app.sync_metadata",
    )

    def test_all_sixteen_are_shipped(self):
        from core.pg_operational import _APPEND_ABOVE, _FULL_REPLACE

        shipped = ({e[0] for e in _FULL_REPLACE}
                   | {s.pg_table for s in _APPEND_ABOVE})
        missing = sorted(set(self.STAGE_THREE) - shipped)
        assert missing == [], f"shipped by nothing: {missing}"

    def test_all_sixteen_are_compared(self):
        from core.mirror_reconciliation import (
            APPEND_ONLY_TABLES, OPERATIONAL_TABLES,
        )

        compared = ({s.pg_table for s in OPERATIONAL_TABLES}
                    | {s.pg_table for s in APPEND_ONLY_TABLES})
        missing = sorted(set(self.STAGE_THREE) - compared)
        assert missing == [], f"copied and never checked: {missing}"

    def test_every_one_has_ddl_in_some_revision(self):
        """The third list. A table shipped with no DDL fails at the first
        INSERT, in production, on the hourly job."""
        import re
        from pathlib import Path

        versions = Path(__file__).resolve().parents[2] / "migrations" / "versions"
        ddl = "\n".join(
            f.read_text(encoding="utf-8") for f in sorted(versions.glob("0*.py"))
        )
        created = set(re.findall(
            r"CREATE TABLE IF NOT EXISTS ((?:app|bronze|gold|silver)\.\w+)", ddl))
        # The two ledgers are created in a loop over an f-string, so their
        # names are not literals anywhere in the DDL.
        created |= {"app.weekly_report_sends", "app.traffic_report_sends"}
        missing = sorted(set(self.STAGE_THREE) - created)
        assert missing == [], f"shipped and never created: {missing}"
