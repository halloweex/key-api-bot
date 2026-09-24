"""The chain invariants without a database: who is watched, and where each
threshold sits.

DN-07. The real-Postgres half is `tests/integration/test_chain_invariants.py`
and it proves the SQL; this proves the things that have nothing to do with
SQL — that a system with no moved chain asks Postgres nothing at all, that
every threshold sits where the module says it does rather than one row either
side of it, and that the integrity job judges these beside the Postgres twins,
so a DuckDB half that raises loses none of them and holds back no page.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import textwrap
from contextlib import ExitStack
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import chain_latch, pg_chain_invariants as inv

UTC_NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)


def _latch(chain: str, stamp: datetime = UTC_NOW) -> None:
    chain_latch.MARKER_DIR.mkdir(parents=True, exist_ok=True)
    (chain_latch.MARKER_DIR / chain).write_text(
        f'{{"chain": "{chain}", "latched_at": "{stamp.isoformat()}"}}',
        encoding="utf-8")
    chain_latch.load()


class _RefusingPool:
    """A pool that fails the test if anything touches it."""

    def acquire(self, *a, **kw):  # pragma: no cover - the assertion is the point
        raise AssertionError("read_facts opened a connection with no chain watched")


class TestNothingIsReadWhileNothingHasMoved:
    """No chain flagged and none latched. The check must cost a dictionary
    lookup, not a connection — a check that opened one to find out it had
    nothing to look at would put a Postgres failure on the path of a system
    not using Postgres for these tables.

    This is NOT production's state, and the first draft of this class said it
    was: production has `KS_WRITE_EXPENSES=postgres`, which is the class
    below."""

    @pytest.mark.asyncio
    async def test_no_chain_watched_means_no_query_and_no_pool(self, monkeypatch):
        for env in ("KS_WRITE_EXPENSES", "KS_WRITE_INVENTORY", "KS_WRITE_GOALS"):
            monkeypatch.delenv(env, raising=False)
        get_pool = AsyncMock(side_effect=AssertionError("asked for a pool"))
        with patch("core.pg.get_pool", new=get_pool):
            facts = await inv.read_facts(pool=_RefusingPool())
        get_pool.assert_not_awaited()
        assert facts.watched == () and facts.whole is None
        assert inv.check_chain_invariants(facts) == []

    @pytest.mark.asyncio
    async def test_a_forgotten_pre_read_is_not_silence_either(self, monkeypatch):
        """`None` is the caller having brought nothing. With nothing watched it
        is the same as clean; with a chain watched it is blindness over the one
        set of tables nothing else checks."""
        monkeypatch.delenv("KS_WRITE_EXPENSES", raising=False)
        assert inv.check_chain_invariants(None) == []
        monkeypatch.setenv("KS_WRITE_EXPENSES", "postgres")
        issues = inv.check_chain_invariants(None)
        assert [i.check_name for i in issues] == [inv.UNWATCHED]
        assert "without a pre-read" in issues[0].description


class _Ctx:
    """An async context manager that yields what it was given."""

    def __init__(self, value=None, raises: BaseException = None):
        self.value, self.raises = value, raises

    async def __aenter__(self):
        if self.raises is not None:
            raise self.raises
        return self.value

    async def __aexit__(self, *exc):
        return False


class _Recorder:
    """A connection that answers the pre-read and remembers every statement.

    `fail_on` makes any statement containing that text raise, which is how a
    single unreadable table is simulated without a database. `max_ids` gives a
    table (matched by substring of the allocator's SQL) rows up to that id.

    It keeps Postgres' one rule that matters to the per-group savepoints: a
    statement that fails aborts the transaction it ran in, every statement
    after it fails too, and only leaving that transaction or savepoint clears
    it. Without the rule a fake connection cannot tell a savepoint from none —
    the real-Postgres case is `tests/integration/test_chain_invariants.py`.
    """

    def __init__(self, fail_on: str = None, max_ids: dict = None):
        self.sql = []
        self.fail_on = fail_on
        self.max_ids = max_ids or {}
        self.depth = 0
        self.aborted_at = None

    def _see(self, sql: str) -> None:
        if self.aborted_at is not None:
            raise RuntimeError("current transaction is aborted, commands "
                               "ignored until end of transaction block")
        self.sql.append(sql)
        if self.fail_on and self.fail_on in sql:
            self.aborted_at = self.depth
            raise RuntimeError(f"relation unreadable: {self.fail_on}")

    def transaction(self, **_kw):
        return _Tx(self)

    async def execute(self, sql, *_a):
        self._see(sql)

    async def fetchval(self, sql, *_a):
        self._see(sql)
        if "AT TIME ZONE" in sql:
            return UTC_NOW.date()
        if "now()" in sql:
            return UTC_NOW
        return 0

    async def fetchrow(self, sql, *_a):
        self._see(sql)
        if "last_value" in sql:
            max_id = next((n for table, n in self.max_ids.items() if table in sql),
                          None)
            return {"last_value": 1, "is_called": False, "max_id": max_id}
        if "revenue_goals" in sql:
            return {"updated_at": 0, "is_custom": 0}
        if "recorded_at" in sql:
            return {"recorded_at": 0, "source": 0}
        if "created_at" in sql:
            return {"created_at": 0}
        raise AssertionError(f"unexpected fetchrow: {sql}")

    async def fetch(self, sql, *_a):
        self._see(sql)
        return []


class _Tx:
    """A transaction or savepoint of `_Recorder`: leaving it rolls back an
    abort that happened inside it, and no other."""

    def __init__(self, conn: _Recorder):
        self.conn = conn

    async def __aenter__(self):
        self.conn.depth += 1
        self.level = self.conn.depth

    async def __aexit__(self, *exc):
        if self.conn.aborted_at is not None and self.conn.aborted_at >= self.level:
            self.conn.aborted_at = None
        self.conn.depth -= 1
        return False


class _Pool:
    def __init__(self, conn=None, raises: BaseException = None):
        self.conn, self.raises = conn, raises

    def acquire(self, timeout=None):
        return _Ctx(self.conn, self.raises)


class TestProductionToday:
    """`KS_WRITE_EXPENSES=postgres` live since 2026-09-17, no expense typed,
    so chain 8 is flagged and not latched; `KS_WRITE_INVENTORY` is off.

    The claim this pins is the corrected one: every integrity run takes a
    connection and reads chain 8's allocator and NULL count — and nothing of
    chain 1's — and a Postgres it cannot reach is one WARN."""

    @pytest.fixture
    def production(self, monkeypatch):
        monkeypatch.setenv("KS_WRITE_EXPENSES", "postgres")
        monkeypatch.delenv("KS_WRITE_INVENTORY", raising=False)
        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")

    @pytest.mark.asyncio
    async def test_chain_8_is_read_and_chain_1_is_not(self, production):
        conn = _Recorder()
        with patch("core.pg.require_revision", new=AsyncMock()) as revision:
            facts = await inv.read_facts(pool=_Pool(conn))
        revision.assert_awaited_once()
        assert facts.watched == ("pg_expenses_write",) and facts.whole is None
        assert isinstance(facts.expenses, inv.Expenses)
        assert facts.inventory is None
        read = "\n".join(conn.sql)
        assert "app.manual_expenses_id_seq" in read
        assert "FROM app.manual_expenses" in read
        for chain_1 in ("stock_movements", "inventory", "offers", "chain_watermarks"):
            assert chain_1 not in read, chain_1
        # Zero rows: an allocator with nothing to collide with, no NULLs.
        assert inv.check_chain_invariants(facts) == []

    @pytest.mark.asyncio
    async def test_an_unreachable_postgres_is_one_warn(self, production):
        from core.data_quality import Severity

        with patch("core.pg.require_revision",
                   new=AsyncMock(side_effect=RuntimeError("SchemaVersionError"))):
            facts = await inv.read_facts(pool=_Pool(_Recorder()))
        issues = inv.check_chain_invariants(facts)
        assert [(i.check_name, i.severity) for i in issues] == [
            (inv.UNWATCHED, Severity.WARN)]

    @pytest.mark.asyncio
    async def test_a_pool_that_times_out_is_blindness_not_a_raise(self, production):
        """`pool.acquire(timeout=)` raises TimeoutError. Let out, the scan got
        `None` and reported a job that forgot its pre-read — a code defect
        named for an exhausted pool."""
        with patch("core.pg.require_revision", new=AsyncMock()):
            facts = await inv.read_facts(pool=_Pool(raises=TimeoutError()))
        assert facts.whole is not None and "TimeoutError" in facts.whole.reason
        assert "without a pre-read" not in (
            inv.check_chain_invariants(facts)[0].description)


class TestTheAllocatorIsReadAsPostgresHandsItOut:
    """`is_called` is the only Postgres sequence semantics in the module. A
    fresh `setval(seq, n, false)` hands out n itself; after a `nextval` it
    hands out `last_value + 1`. The integration tests prove it on a real
    sequence; this pins the arithmetic where no database is needed."""

    class _Row:
        def __init__(self, last, called, max_id):
            self.row = {"last_value": last, "is_called": called, "max_id": max_id}

        async def fetchrow(self, *_a):
            return self.row

    @pytest.mark.asyncio
    @pytest.mark.parametrize("last, called, next_id", [
        (5, False, 5),     # setval(seq, 5, false): nextval returns 5
        (5, True, 6),      # after nextval returned 5
        (None, False, None),
    ])
    async def test_next_id(self, last, called, next_id):
        allocator = await inv._read_allocator(
            self._Row(last, called, 5), "app.manual_expenses", "seq")
        assert allocator.next_id == next_id


class TestAChainWithNoInvariants:
    """Stage 4 moves more chains. One registered in `WRITE_CHAINS` and flipped
    used to be listed as watched, read by nothing, and judged clean."""

    @pytest.fixture
    def third_chain(self, monkeypatch):
        import types

        from core import write_chains

        fake = types.ModuleType("core.pg_orders_write")
        fake.WRITE_ENV = "KS_WRITE_ORDERS"
        fake.CHAIN_TABLES = ("bronze.orders",)
        fake.env_writes_postgres = lambda: True
        monkeypatch.setattr(write_chains, "WRITE_CHAINS",
                            write_chains.WRITE_CHAINS + (fake,))
        for env in ("KS_WRITE_EXPENSES", "KS_WRITE_INVENTORY", "KS_WRITE_GOALS"):
            monkeypatch.delenv(env, raising=False)
        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        return "pg_orders_write"

    @pytest.mark.asyncio
    async def test_it_is_unwatched_not_clean(self, third_chain):
        with patch("core.pg.require_revision", new=AsyncMock()):
            facts = await inv.read_facts(pool=_Pool(_Recorder()))
        assert facts.unread == (third_chain,)
        issues = inv.check_chain_invariants(facts)
        assert [i.check_name for i in issues] == [inv.UNWATCHED]
        assert "no invariants are defined for this chain" in issues[0].description
        assert third_chain in issues[0].description

    def test_every_registered_chain_has_a_reader(self):
        """The CI half of the same rule: a chain added to `WRITE_CHAINS`
        without invariants fails here, before it can be flipped."""
        from core.write_chains import WRITE_CHAINS, chain_name

        readers = inv._reader_groups()
        missing = [chain_name(c) for c in WRITE_CHAINS
                   if chain_name(c) not in readers]
        assert not missing, f"no chain invariants for {missing}"
        assert set(readers.values()) <= {"expenses", "inventory", "goals"}
        # And each names a field `Facts` actually carries — a reader whose
        # group the verdict never looks at is read and then judged by nothing.
        import dataclasses

        fields = {f.name for f in dataclasses.fields(inv.Facts)}
        assert set(readers.values()) <= fields, set(readers.values()) - fields


class TestChain7aGoals:
    """DN-25. `app.revenue_goals` has no allocator and no sync watermark; what
    is true of it on its own is that its writer supplies the two columns
    Postgres does not default — `updated_at`, the copy-back's clock, and
    `is_custom`, without which `get_smart_goals` drops a typed goal for the
    suggestion."""

    @pytest.fixture
    def flagged(self, monkeypatch):
        monkeypatch.setenv("KS_WRITE_GOALS", "postgres")
        for env in ("KS_WRITE_EXPENSES", "KS_WRITE_INVENTORY"):
            monkeypatch.delenv(env, raising=False)
        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")

    def test_the_flag_alone_makes_it_watched(self, flagged):
        assert inv.watched_chains() == {"pg_goals_write": None}

    @pytest.mark.asyncio
    async def test_it_reads_the_goals_table_and_nothing_else(self, flagged):
        conn = _Recorder()
        with patch("core.pg.require_revision", new=AsyncMock()):
            facts = await inv.read_facts(pool=_Pool(conn))
        assert facts.watched == ("pg_goals_write",) and facts.whole is None
        assert isinstance(facts.goals, inv.Goals)
        assert facts.expenses is None and facts.inventory is None
        read = "\n".join(conn.sql)
        assert "FROM app.revenue_goals" in read
        for other in ("manual_expenses", "stock_movements", "chain_watermarks"):
            assert other not in read, other
        assert inv.check_chain_invariants(facts) == []

    def test_a_null_either_column_names_it(self):
        from core.data_quality import Severity

        facts = inv.Facts(
            watched=("pg_goals_write",), now=UTC_NOW,
            goals=inv.Goals(nulls=inv.Nulls(
                "app.revenue_goals", {"updated_at": 1, "is_custom": 2})))
        (issue,) = inv.check_chain_invariants(facts)
        assert issue.check_name == inv.COLUMN_NULL
        assert issue.table_name == "app.revenue_goals"
        assert issue.severity is Severity.CRITICAL and issue.count == 3
        assert "updated_at in 1 row(s)" in issue.description
        assert "is_custom in 2 row(s)" in issue.description
        assert "pg_goals_write" in issue.description

    def test_an_unreadable_goals_table_is_blindness_not_silence(self):
        facts = inv.Facts(watched=("pg_goals_write",), now=UTC_NOW,
                          goals=inv.Unwatched("relation does not exist"))
        issues = inv.check_chain_invariants(facts)
        assert [i.check_name for i in issues] == [inv.UNWATCHED]
        assert "pg_goals_write" in issues[0].description
        assert inv.unverified_conditions(issues) == sorted(inv.CONDITIONS)


class TestWhoIsWatched:
    def test_a_flagged_chain_is_watched(self, monkeypatch):
        monkeypatch.setenv("KS_WRITE_EXPENSES", "postgres")
        monkeypatch.delenv("KS_WRITE_INVENTORY", raising=False)
        assert inv.watched_chains() == {"pg_expenses_write": None}

    def test_a_latched_chain_is_watched_though_its_flag_says_duckdb(self, monkeypatch):
        """The whole point of the latch: the writes are in Postgres whatever
        the variable says, so the invariants over those rows must follow."""
        monkeypatch.setenv("KS_WRITE_INVENTORY", "duckdb")
        monkeypatch.delenv("KS_WRITE_EXPENSES", raising=False)
        _latch("pg_inventory_write")
        assert inv.watched_chains() == {
            "pg_inventory_write": UTC_NOW.isoformat()}

    def test_a_flag_nobody_can_read_is_watched_rather_than_dropped(self, monkeypatch):
        """DN-01 stands that chain down from the shipper and the comparison, so
        it is exactly the chain whose tables nothing else is looking at."""
        monkeypatch.setenv("KS_WRITE_EXPENSES", "postgre")
        monkeypatch.delenv("KS_WRITE_INVENTORY", raising=False)
        assert list(inv.watched_chains()) == ["pg_expenses_write"]

    def test_the_state_comes_from_write_chains_and_not_from_the_environment(self):
        """Reused, not re-derived — a second reading of `KS_WRITE_*` here is
        how a latched chain with a flipped-back variable would end up
        unwatched by the check that exists for it."""
        source = textwrap.dedent(inspect.getsource(inv.watched_chains))
        calls = {n.func.id for n in ast.walk(ast.parse(source))
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        assert "chain_modes" in calls
        assert "getenv" not in source


class TestBlindnessIsReported:
    def test_a_failed_read_holds_every_condition(self):
        facts = inv.Facts.blind(("pg_expenses_write",), "Postgres is unreachable")
        issues = inv.check_chain_invariants(facts)
        assert [i.check_name for i in issues] == [inv.UNWATCHED]
        for condition in inv.CONDITIONS:
            assert condition in issues[0].description

    def test_one_unreadable_chain_does_not_silence_the_other(self):
        facts = inv.Facts(
            watched=("pg_expenses_write", "pg_inventory_write"),
            now=UTC_NOW,
            expenses=inv.Unwatched("relation does not exist"),
            inventory=inv.Inventory(
                allocator=inv.Allocator("app.stock_movements", "seq", 3, 9),
                nulls=inv.Nulls("app.stock_movements", {})),
        )
        names = [i.check_name for i in inv.check_chain_invariants(facts)]
        assert names == [inv.UNWATCHED, inv.SEQUENCE_BEHIND]

    @pytest.mark.asyncio
    async def test_one_unreadable_table_does_not_take_the_other_chains_read(
        self, monkeypatch,
    ):
        """The same property through `read_facts`, which is where it can break:
        the per-group savepoint. Chain 8's read fails first (groups are read in
        name order), and chain 1's allocator — at MAX(id), a real collision —
        must still be read in the same transaction and judged. Without the
        savepoint the failure aborts the transaction and every read after it
        comes back unwatched; with the failure re-raised instead of recorded,
        the whole run goes blind. Both leave the CRITICAL unfiled."""
        monkeypatch.setenv("KS_WRITE_EXPENSES", "postgres")
        monkeypatch.setenv("KS_WRITE_INVENTORY", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        conn = _Recorder(fail_on="manual_expenses", max_ids={"stock_movements": 1})
        with patch("core.pg.require_revision", new=AsyncMock()):
            facts = await inv.read_facts(pool=_Pool(conn))
        assert facts.whole is None
        assert isinstance(facts.expenses, inv.Unwatched)
        assert "manual_expenses" in facts.expenses.reason
        assert isinstance(facts.inventory, inv.Inventory)
        assert facts.watermarks_unread is None
        issues = inv.check_chain_invariants(facts)
        assert [(i.check_name, i.table_name) for i in issues] == [
            (inv.UNWATCHED, "(write chains)"),
            (inv.SEQUENCE_BEHIND, "app.stock_movements")]

    def test_a_blind_run_holds_every_condition(self):
        from core.data_quality import IntegrityIssue, Severity

        issue = IntegrityIssue(check_name=inv.UNWATCHED, table_name="(write chains)",
                               severity=Severity.WARN, count=1)
        assert inv.unverified_conditions([issue]) == sorted(inv.CONDITIONS)

    def test_a_run_that_looked_holds_nothing(self):
        """Its CRITICALs stay firing because they are found again, not because
        they are held — held would keep one standing after it cleared."""
        issues = inv.check_chain_invariants(_expenses(next_id=2, max_id=2))
        assert [i.check_name for i in issues] == [inv.SEQUENCE_BEHIND]
        assert inv.unverified_conditions(issues) == []


class TestTheVerdictNeverRaises:
    """`judge` is what the job calls, outside any `guarded`. A raise there would
    escape `_run_dq_integrity` before the run is persisted."""

    def test_a_raising_verdict_is_blindness(self, monkeypatch):
        def boom(_facts):
            raise KeyError("a field the verdict expected")

        monkeypatch.setattr(inv, "check_chain_invariants", boom)
        issues = inv.judge(_expenses(next_id=2, max_id=2))
        assert [i.check_name for i in issues] == [inv.UNWATCHED]
        assert "KeyError" in issues[0].description
        assert "pg_expenses_write" in issues[0].description
        assert inv.unverified_conditions(issues) == sorted(inv.CONDITIONS)

    def test_it_does_not_ask_who_is_watched_to_name_the_blindness(self, monkeypatch):
        """With no facts, the verdict asks `watched_chains()` — and if that is
        what raised, asking again on the way out would raise past the job."""
        def boom(*_a):
            raise RuntimeError("chain_modes unreadable")

        monkeypatch.setattr(inv, "watched_chains", boom)
        issues = inv.judge(None)
        assert [i.check_name for i in issues] == [inv.UNWATCHED]
        assert "chain_modes unreadable" in issues[0].description

    def test_a_verdict_that_does_not_raise_is_passed_through(self):
        facts = _expenses(next_id=2, max_id=2)
        assert inv.judge(facts) == inv.check_chain_invariants(facts)


def _expenses(next_id=None, max_id=None, nulls=None) -> inv.Facts:
    return inv.Facts(
        watched=("pg_expenses_write",), now=UTC_NOW,
        expenses=inv.Expenses(
            allocator=inv.Allocator("app.manual_expenses", "app.manual_expenses_id_seq",
                                    next_id, max_id),
            nulls=inv.Nulls("app.manual_expenses", nulls or {"created_at": 0})))


class TestTheAllocator:
    def test_one_above_the_table_is_correct(self):
        assert inv.check_chain_invariants(_expenses(next_id=8, max_id=7)) == []

    def test_equal_to_the_table_is_the_next_collision(self):
        issues = inv.check_chain_invariants(_expenses(next_id=7, max_id=7))
        assert [i.check_name for i in issues] == [inv.SEQUENCE_BEHIND]
        assert issues[0].count == 1

    def test_an_empty_table_has_nothing_to_collide_with(self):
        assert inv.check_chain_invariants(_expenses(next_id=1, max_id=None)) == []


class TestTheInventoryThresholds:
    def _inventory(self, **kw) -> inv.Facts:
        base = dict(
            allocator=inv.Allocator("app.stock_movements", "seq", 100, 99),
            nulls=inv.Nulls("app.stock_movements", {"recorded_at": 0, "source": 0}),
            latched_at=UTC_NOW - timedelta(days=5),
            offers=1000, status_rows=900,
            initial_24h=0, first_seen_moved=0, first_seen_sample=(),
            rollup_missing=(), snapshot_days=(),
            window_start=(UTC_NOW - timedelta(days=5)).date(),
            window_end=(UTC_NOW - timedelta(days=1)).date())
        base.update(kw)
        return inv.Facts(watched=("pg_inventory_write",), now=UTC_NOW,
                         inventory=inv.Inventory(**base))

    def _names(self, **kw):
        return [i.check_name for i in inv.check_chain_invariants(self._inventory(**kw))]

    def test_a_handful_of_new_skus_is_not_a_burst(self):
        assert self._names(initial_24h=49) == []

    def test_five_percent_of_offers_is(self):
        assert self._names(initial_24h=50) == [inv.INITIAL_BURST]

    def test_one_offer_first_seen_after_it_was_seen_is_a_lost_date(self):
        """No threshold, because nothing legitimate produces it: the reader
        counts only offers dated later than a day they were photographed.
        What that SQL does over months of growth is the integration test's."""
        assert self._names(first_seen_moved=0) == []
        issues = inv.check_chain_invariants(self._inventory(
            first_seen_moved=1, first_seen_sample=(42,)))
        assert [i.check_name for i in issues] == [inv.FIRST_SEEN_RESET]
        assert issues[0].sample_ids == (42,)
        assert "offer 42" in issues[0].description

    def test_a_short_snapshot_day_is_half_the_tracked_offers(self):
        day = (UTC_NOW - timedelta(days=1)).date()
        assert self._names(snapshot_days=((day, 450),)) == []
        assert self._names(snapshot_days=((day, 449),)) == [inv.SNAPSHOT_SHORT]

    def test_the_since_the_handover_checks_stand_down_before_the_handover(self):
        """A chain that is flagged but has never written has no handover to
        date anything from; only the allocator and the NULL columns apply."""
        assert self._names(latched_at=None, initial_24h=900,
                           first_seen_moved=900,
                           rollup_missing=(date(2026, 9, 17),)) == []


class TestTheWatermarks:
    def _facts(self, age_s=None, raw="2026-09-18T11:00:00+00:00") -> inv.Facts:
        return inv.Facts(
            watched=("pg_inventory_write",), now=UTC_NOW,
            watermarks=(inv.WatermarkAge("pg_inventory_write", "last_sync_stocks",
                                         raw, age_s),))

    def test_a_healthy_night_time_peak_is_inside_the_limit(self):
        """The one-hour gate, a 300 s off-hours tick (the 01:00 run is inside
        23:00-07:00) and two minutes of lock wait and fetch: ~67 minutes on a
        watermark with nothing wrong behind it."""
        assert inv.check_chain_invariants(self._facts(age_s=3600 + 300 + 120)) == []

    def test_ninety_minutes_is_the_edge(self):
        assert inv.check_chain_invariants(self._facts(age_s=90 * 60)) == []
        issues = inv.check_chain_invariants(self._facts(age_s=90 * 60 + 1))
        assert [i.check_name for i in issues] == [inv.WATERMARK_STALE]

    def test_a_value_that_is_not_a_timestamp_is_reported_not_skipped(self):
        issues = inv.check_chain_invariants(self._facts(age_s=None, raw="yesterday"))
        assert [i.check_name for i in issues] == [inv.WATERMARK_STALE]
        assert "not a timestamp" in issues[0].description

    def test_an_absent_watermark_is_left_to_the_freshness_check(self):
        assert inv.check_chain_invariants(self._facts(age_s=None, raw=None)) == []

    @pytest.mark.asyncio
    async def test_an_unreadable_watermark_table_is_reported_not_silent(
        self, monkeypatch,
    ):
        """Chain 1 flagged, so its two keys are read. An empty tuple from a
        failed read looked exactly like a chain with no sync keys at all."""
        monkeypatch.setenv("KS_WRITE_INVENTORY", "postgres")
        monkeypatch.delenv("KS_WRITE_EXPENSES", raising=False)
        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        conn = _Recorder(fail_on="meta.chain_watermarks")
        with patch("core.pg.require_revision", new=AsyncMock()):
            facts = await inv.read_facts(pool=_Pool(conn))
        assert isinstance(facts.inventory, inv.Inventory)
        issues = inv.check_chain_invariants(facts)
        assert [i.check_name for i in issues] == [inv.UNWATCHED]
        assert "meta.chain_watermarks unreadable" in issues[0].description


class TestTheWiring:
    def test_every_condition_has_a_lever(self):
        from core.data_quality import DEFAULT_REMEDIATION, remediation_for

        for condition in inv.CONDITIONS + (inv.UNWATCHED,):
            assert remediation_for([condition]) != [DEFAULT_REMEDIATION], condition

    def test_the_two_findings_that_must_never_say_repair(self):
        """An initial burst has already recorded the wrong deltas and a reset
        `first_seen_at` has already lost the dates."""
        from core.data_quality import remediation_for

        for condition in (inv.INITIAL_BURST, inv.FIRST_SEEN_RESET):
            assert "do not repair" in remediation_for([condition])[0].lower()

    def test_the_duckdb_scan_does_not_judge_them(self):
        """Structural half of `TestTheJobWhenTheDuckDBHalfFails`: nothing about
        the chains goes into or comes out of the DuckDB scan."""
        from core.data_quality import check_internal_integrity

        assert not [p for p in inspect.signature(check_internal_integrity).parameters
                    if "chain_facts" in p or "invariant" in p]
        tree = ast.parse(textwrap.dedent(inspect.getsource(check_internal_integrity)))
        named = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {
            n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        assert not named & {"check_chain_invariants", "judge", "pg_chain_invariants"}

    def test_every_condition_carries_the_prefix_and_nothing_else_on_the_layer_does(self):
        """The job resolves these by `only_prefix=PREFIX` when its DuckDB half
        failed, so a DuckDB-half or twin condition under the same prefix would
        be announced cleared by a run that never looked at it."""
        import pathlib

        for name in inv.CONDITIONS + (inv.UNWATCHED,):
            assert name.startswith(inv.PREFIX), name
        root = pathlib.Path(__file__).resolve().parents[2]
        others = set()
        for path in ("core/data_quality.py", "core/pg_warehouse_dq.py"):
            for node in ast.walk(ast.parse((root / path).read_text())):
                if isinstance(node, ast.keyword) and node.arg == "check_name" \
                        and isinstance(node.value, ast.Constant):
                    others.add(node.value.value)
        from core import pg_warehouse_dq

        others |= set(pg_warehouse_dq.UNWATCHED_NAMES.values())
        others |= {pg_warehouse_dq.WHOLE_UNWATCHED, pg_warehouse_dq.FLAG_INVALID}
        assert others and not {n for n in others if n.startswith(inv.PREFIX)}

    def test_the_window_follows_the_continuity_check(self):
        from core.data_quality import INVENTORY_CONTINUITY_WINDOW_DAYS

        assert inv._window_days() == INVENTORY_CONTINUITY_WINDOW_DAYS


class TestTheJobWhenTheDuckDBHalfFails:
    """The integrity job end to end, on a real (empty) DuckDB, with the chain
    facts handed in and the DuckDB scan made to raise.

    These are Postgres facts about tables DuckDB no longer writes. Judged inside
    `check_internal_integrity` they would go down with it: the run persisted
    failed with no chain finding, and the page — gated on the DuckDB half —
    waiting for DuckDB or for the canary's 12-hour run-age limit. The twins in
    `core/pg_warehouse_dq.py` stand outside that half for exactly this, and the
    chain invariants stand beside them."""

    @pytest_asyncio.fixture
    async def job(self, tmp_path, monkeypatch):
        from core.duckdb_store import DuckDBStore
        from core.scheduler import BackgroundScheduler

        for env in ("KS_WRITE_EXPENSES", "KS_WRITE_INVENTORY", "KS_DQ_PG_WAREHOUSE"):
            monkeypatch.delenv(env, raising=False)
        store = DuckDBStore(db_path=tmp_path / "integrity.duckdb")
        await store.connect()
        scheduler = BackgroundScheduler()
        sent = AsyncMock(return_value=True)
        monkeypatch.setattr(scheduler, "_send_dq_alert_throttled", sent)
        resolve = AsyncMock(return_value=0)
        with patch("core.duckdb_store.get_store", new=AsyncMock(return_value=store)), \
             patch("core.alerting.resolve_group", resolve):
            yield scheduler, store, sent, resolve
        await store.close()

    @staticmethod
    async def _run(scheduler, facts, *, duckdb_raises: bool):
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(inv, "read_facts", AsyncMock(return_value=facts)))
            if duckdb_raises:
                stack.enter_context(patch(
                    "core.data_quality.check_internal_integrity",
                    side_effect=RuntimeError("DuckDB file will not open")))
            return await scheduler._run_dq_integrity()

    @staticmethod
    async def _persisted(store, run_id):
        from core.data_quality import fetch_run_issues

        async with store.connection() as conn:
            return {r["check_name"]: r for r in fetch_run_issues(conn, run_id)}

    @staticmethod
    def _chain_resolves(resolve):
        return [c for c in resolve.await_args_list
                if c.kwargs.get("only_prefix") == inv.PREFIX]

    @pytest.mark.asyncio
    async def test_a_chain_critical_is_persisted_and_pages(self, job):
        scheduler, store, sent, resolve = job
        result = await self._run(scheduler, _expenses(next_id=2, max_id=2),
                                 duckdb_raises=True)

        assert "DuckDB file will not open" in result["error"]
        persisted = await self._persisted(store, result["run_id"])
        assert persisted[inv.SEQUENCE_BEHIND]["severity"] == "CRITICAL"
        assert persisted[inv.SEQUENCE_BEHIND]["table_name"] == "app.manual_expenses"
        assert sent.await_count == 1
        assert sent.await_args.kwargs["conditions"] == [inv.SEQUENCE_BEHIND]
        (call,) = self._chain_resolves(resolve)
        assert inv.SEQUENCE_BEHIND in call.kwargs["still_firing"]

    @pytest.mark.asyncio
    async def test_a_chain_that_could_not_be_read_pages_nobody_and_clears_nothing(
        self, job,
    ):
        scheduler, store, sent, resolve = job
        blind = inv.Facts.blind(("pg_expenses_write",), "Postgres is unreachable")
        result = await self._run(scheduler, blind, duckdb_raises=True)

        assert inv.UNWATCHED in await self._persisted(store, result["run_id"])
        assert sent.await_count == 0
        (call,) = self._chain_resolves(resolve)
        assert set(inv.CONDITIONS) <= set(call.kwargs["still_firing"])

    @pytest.mark.asyncio
    async def test_with_no_chain_watched_nothing_of_theirs_is_announced(self, job):
        """Nothing looked at the chains this run; a page left over from one that
        has stood down clears on the next run whose DuckDB half finishes."""
        scheduler, _store, sent, resolve = job
        await self._run(scheduler, inv.Facts(), duckdb_raises=True)
        assert sent.await_count == 0
        assert not self._chain_resolves(resolve)

    @pytest.mark.asyncio
    async def test_with_the_duckdb_half_healthy_the_chain_critical_pages_and_is_not_held(
        self, job,
    ):
        scheduler, store, sent, resolve = job
        result = await self._run(scheduler, _expenses(next_id=2, max_id=2),
                                 duckdb_raises=False)

        assert result["error"] is None
        assert inv.SEQUENCE_BEHIND in await self._persisted(store, result["run_id"])
        assert sent.await_count == 1
        assert inv.SEQUENCE_BEHIND in sent.await_args.kwargs["conditions"]
        (layer,) = [c for c in resolve.await_args_list if not c.kwargs.get("only_prefix")]
        still = set(layer.kwargs["still_firing"])
        assert inv.SEQUENCE_BEHIND in still                     # found again, CRITICAL
        assert not (set(inv.CONDITIONS) - {inv.SEQUENCE_BEHIND}) & still   # not held

    @pytest.mark.asyncio
    async def test_with_the_duckdb_half_healthy_a_blind_chain_read_holds_everything(
        self, job,
    ):
        """The layer's own resolution must hold them too: `data_quality`'s
        `unverified_conditions` no longer knows these conditions exist."""
        scheduler, store, sent, resolve = job
        blind = inv.Facts.blind(("pg_expenses_write",), "Postgres is unreachable")
        result = await self._run(scheduler, blind, duckdb_raises=False)

        assert result["error"] is None
        (layer,) = [c for c in resolve.await_args_list if not c.kwargs.get("only_prefix")]
        assert set(inv.CONDITIONS) <= set(layer.kwargs["still_firing"])


class TestTheLatchStamp:
    def test_an_unreadable_stamp_is_read_as_no_handover(self):
        """Reading it as `now` would make every since-the-handover invariant
        measure from the moment of the read and report its whole table."""
        assert inv._stamp("not a date") is None
        assert inv._stamp(None) is None

    def test_a_naive_stamp_is_utc_because_that_is_what_the_marker_writes(self):
        assert inv._stamp("2026-09-18T12:00:00") == UTC_NOW


def test_read_facts_never_raises_on_an_unreachable_store(monkeypatch):
    """A blind run must say so; an exception here would take the whole
    integrity scan's persistence with it."""
    monkeypatch.setenv("KS_WRITE_EXPENSES", "postgres")
    monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
    with patch("core.pg.get_pool", new=AsyncMock(side_effect=OSError("refused"))):
        facts = asyncio.run(inv.read_facts())
    assert facts.whole is not None and "refused" in facts.whole.reason
    assert [i.check_name for i in inv.check_chain_invariants(facts)] == [inv.UNWATCHED]
