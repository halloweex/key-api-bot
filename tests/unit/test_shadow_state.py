"""The third registry state: copy stops, comparison continues (OD-02 (c)).

Chains 9, 10 and 11 SHADOW DuckDB: Postgres becomes the writer and DuckDB is
still handed each row, after the Postgres commit. So the one answer the
frozen chains share is split in two — the hourly copy stands down (its full
replace out of DuckDB would delete every row whose shadow failed), and the
daily comparison keeps comparing, facing Postgres→DuckDB.

Every guard here names the mutation it exists to catch, in its docstring.
"""
from __future__ import annotations

import ast
import pathlib
import types
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from core import chain_latch, write_chains
from core.data_quality import Severity
from core.mirror_reconciliation import (
    OPERATIONAL_TABLES,
    compare_shadow,
    pg_clock_expr,
)

ROOT = pathlib.Path(__file__).resolve().parents[2]
SENDS = "app.weekly_report_sends"
NOW = datetime(2026, 10, 1, 7, 30, tzinfo=timezone.utc)
OLD = NOW - timedelta(days=3)
FRESH = NOW - timedelta(minutes=5)

# The eight tables OD-02 (c) shadows: chain 9's journal, chain 10's samples,
# chain 11's two ledgers.
SHADOW_CANDIDATES = (
    "app.data_quality_runs", "app.data_quality_issues", "app.data_quality_diffs",
    "app.disk_samples", "app.data_dir_samples", "app.memory_samples",
    "app.weekly_report_sends", "app.traffic_report_sends",
)


@pytest.fixture
def flags(monkeypatch):
    for chain in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    return monkeypatch


@pytest.fixture
def shadow_chain(flags):
    """Register a fake chain; `shadow` picks its shape, `env()` its flag."""
    def register(tables=(SENDS,), env=lambda: True, shadow=True,
                 name="pg_fake_shadow_write"):
        fake = types.ModuleType(f"core.{name}")
        fake.WRITE_ENV = "KS_WRITE_FAKE_SHADOW"
        fake.CHAIN = name
        fake.CHAIN_TABLES = tuple(tables)
        fake.env_writes_postgres = env
        if shadow:
            fake.CHAIN_SHADOW = True
        flags.setattr(write_chains, "WRITE_CHAINS",
                      write_chains.WRITE_CHAINS + (fake,))
        return fake

    return register


def _typo():
    raise RuntimeError("KS_WRITE_FAKE_SHADOW='postgrse' is not understood")


def _spec(table: str):
    return next(s for s in OPERATIONAL_TABLES if s.pg_table == table)


# ─── The registry's answer ────────────────────────────────────────────────────


class TestTheRegistrySplitsTheAnswer:
    def test_a_shadow_chain_at_postgres_is_stood_down_and_compared(self, shadow_chain):
        """Mutation: `compared_in_shadow` returns the empty set — the table
        would be stood down by both, and the comparison would go blind on the
        one store the chain still feeds."""
        shadow_chain()
        assert SENDS in write_chains.stood_down_tables(), "the copy must stop"
        assert write_chains.compared_in_shadow() == frozenset({SENDS})
        assert write_chains.chain_modes()["pg_fake_shadow_write"]["shadow"] is True

    def test_the_copy_stops_whatever_the_shape(self, shadow_chain):
        """The shipper's answer is not split: a full replace out of DuckDB
        would delete every Postgres row whose shadow failed. Mutation: leave
        shadow chains out of `stood_down_tables`."""
        shadow_chain()
        tables, errors = write_chains.stood_down_tables_checked()
        assert SENDS in tables and not errors

    def test_at_duckdb_it_is_neither(self, shadow_chain):
        shadow_chain(env=lambda: False)
        assert SENDS not in write_chains.stood_down_tables()
        assert write_chains.compared_in_shadow() == frozenset()

    def test_a_typo_stands_down_and_is_not_compared(self, shadow_chain):
        """A flag nobody can read writes nowhere — the writers raise — so
        there is no shadow to compare, and the frozen chains' answer holds:
        stood down by both, `write_chain_flag_invalid` paging. Mutation:
        treat `mode is None` as shadow-compared."""
        shadow_chain(env=_typo)
        tables, errors = write_chains.stood_down_tables_checked()
        assert SENDS in tables and "pg_fake_shadow_write" in errors
        assert write_chains.compared_in_shadow() == frozenset()

    def test_a_frozen_chain_at_postgres_is_not_compared(self, shadow_chain):
        """Mutation: compare every moved chain — chain 8's DuckDB is frozen,
        and every row written since the flip would read as a defect."""
        shadow_chain(shadow=False)
        assert SENDS in write_chains.stood_down_tables()
        assert write_chains.compared_in_shadow() == frozenset()
        assert write_chains.chain_modes()["pg_fake_shadow_write"]["shadow"] is False

    def test_a_latched_shadow_chain_with_its_flag_back_is_compared(self, shadow_chain):
        """OD-19 (a) applies unchanged: the latch outranks the flag, the chain
        still writes Postgres and still shadows, so it is still compared."""
        shadow_chain(env=lambda: False)
        chain_latch.latch("pg_fake_shadow_write")
        assert write_chains.compared_in_shadow() == frozenset({SENDS})
        assert write_chains.chain_modes()["pg_fake_shadow_write"]["mismatch"] is True

    def test_the_health_block_answers_without_postgres(self):
        """`compared_in_shadow` is asked by the comparison, but the block that
        publishes `shadow` is read while Postgres is down. Mutation: read the
        owner rows there."""
        import inspect
        import textwrap

        for fn in (write_chains.compared_in_shadow, write_chains.is_shadow):
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            assert not [n for n in ast.walk(tree) if isinstance(n, ast.Await)]


# ─── The comparison, facing the other way (pure) ─────────────────────────────


def _rows(*rows):
    """`{key: row}` and `{key: clock}` for ledger rows (week, type, rev, n, at)."""
    spec = _spec(SENDS)
    values, clocks = {}, {}
    for week, kind, revenue, orders, at in rows:
        key = (week, kind)
        values[key] = (week, kind, Decimal(revenue), orders, at)
        clocks[key] = at
    assert spec.columns == ("week_start", "sales_type", "revenue", "orders", "sent_at")
    return values, clocks


W1, W2, W3 = date(2026, 9, 14), date(2026, 9, 21), date(2026, 9, 28)


def _names(issues):
    return {i.check_name: i for i in issues}


class TestCompareShadow:
    def test_equal_copies_file_nothing(self):
        dk, dkc = _rows((W1, "retail", "10.00", 3, OLD))
        pg, pgc = _rows((W1, "retail", "10.00", 3, OLD))
        assert compare_shadow(_spec(SENDS), dk, dkc, pg, pgc, now=NOW) == []

    def test_a_row_only_postgres_holds_is_a_failed_shadow(self):
        """WARN, not CRITICAL: Postgres is the writer of record and nothing is
        lost. Mutation: swap the severities of the two one-sided findings."""
        dk, dkc = _rows((W1, "retail", "10.00", 3, OLD))
        pg, pgc = _rows((W1, "retail", "10.00", 3, OLD), (W2, "retail", "9.00", 2, OLD))
        found = _names(compare_shadow(_spec(SENDS), dk, dkc, pg, pgc, now=NOW))
        assert set(found) == {"shadow_missing_in_duckdb"}
        assert found["shadow_missing_in_duckdb"].severity is Severity.WARN
        assert found["shadow_missing_in_duckdb"].count == 1

    def test_a_row_only_duckdb_holds_was_written_round_the_chain(self):
        """DuckDB is written only after a Postgres commit, so a row only it
        holds cannot have come from the chain. Mutation: forgive DuckDB-only
        rows as a copy's retired category."""
        dk, dkc = _rows((W1, "retail", "10.00", 3, OLD), (W3, "retail", "1.00", 1, OLD))
        pg, pgc = _rows((W1, "retail", "10.00", 3, OLD))
        found = _names(compare_shadow(_spec(SENDS), dk, dkc, pg, pgc, now=NOW))
        assert set(found) == {"shadow_duckdb_only_rows"}
        assert found["shadow_duckdb_only_rows"].severity is Severity.CRITICAL

    def test_a_differing_row_is_critical(self):
        dk, dkc = _rows((W1, "retail", "10.00", 3, OLD))
        pg, pgc = _rows((W1, "retail", "10.01", 3, OLD))
        (issue,) = compare_shadow(_spec(SENDS), dk, dkc, pg, pgc, now=NOW)
        assert issue.check_name == "shadow_row_values"
        assert issue.severity is Severity.CRITICAL
        assert "revenue (1)" in issue.description

    @pytest.mark.parametrize("side", ["duckdb", "postgres"])
    def test_rows_in_flight_are_forgiven_on_either_clock(self, side):
        """The window is between the Postgres commit and the shadow's, and
        between the two reads. Mutation: a grace of zero; or reading only
        DuckDB's clock, which a row Postgres alone holds does not have."""
        if side == "postgres":
            dk, dkc = _rows((W1, "retail", "10.00", 3, OLD))
            pg, pgc = _rows((W1, "retail", "10.00", 3, OLD),
                            (W2, "retail", "9.00", 2, FRESH))
        else:
            dk, dkc = _rows((W1, "retail", "10.00", 3, OLD),
                            (W2, "retail", "9.00", 2, FRESH))
            pg, pgc = _rows((W1, "retail", "10.00", 3, OLD))
        assert compare_shadow(_spec(SENDS), dk, dkc, pg, pgc, now=NOW) == []
        # And the same row past the grace is reported: the forgiveness is the
        # clock's, not the shape's.
        later = NOW + timedelta(hours=2)
        assert compare_shadow(_spec(SENDS), dk, dkc, pg, pgc, now=later)

    def test_a_differing_row_in_flight_on_the_postgres_clock_is_forgiven(self):
        spec = _spec("app.memory_samples")
        cols = spec.columns
        assert cols[0] == "sampled_at"
        dk = {FRESH: (FRESH, 100.0, 10.0, 512.0, 0)}
        pg = {FRESH: (FRESH, 101.0, 10.0, 512.0, 0)}
        assert compare_shadow(spec, dk, {FRESH: FRESH}, pg, {FRESH: FRESH}, now=NOW) == []
        assert compare_shadow(spec, dk, {FRESH: FRESH}, pg, {FRESH: FRESH},
                              now=NOW + timedelta(hours=3))

    def test_a_prune_that_lagged_is_info(self):
        """Postgres prunes in its writing transaction and DuckDB's shadow
        prune follows: a DuckDB row older than everything Postgres holds is
        retention, not a stranded row. Mutation: drop the prune rule — every
        lagging prune pages CRITICAL."""
        spec = _spec("app.disk_samples")
        old, older, kept = OLD - timedelta(days=15), OLD - timedelta(days=16), OLD
        dk = {k: (k, 1.0, 50.0, 10.0) for k in (older, old, kept)}
        pg = {kept: (kept, 1.0, 50.0, 10.0)}
        found = _names(compare_shadow(spec, dk, {k: k for k in dk}, pg, {kept: kept}, now=NOW))
        assert set(found) == {"shadow_pruned_rows"}
        assert found["shadow_pruned_rows"].severity is Severity.INFO
        assert found["shadow_pruned_rows"].count == 2

    def test_the_prune_rule_is_not_applied_to_a_table_that_keeps_its_rows(self):
        """A ledger never sweeps: an old DuckDB-only row there is a defect.
        Mutation: apply the prune rule whatever `prunes_by_age` says."""
        dk, dkc = _rows((W1, "retail", "10.00", 3, OLD - timedelta(days=30)),
                        (W2, "retail", "9.00", 2, OLD))
        pg, pgc = _rows((W2, "retail", "9.00", 2, OLD))
        found = _names(compare_shadow(_spec(SENDS), dk, dkc, pg, pgc, now=NOW))
        assert set(found) == {"shadow_duckdb_only_rows"}

    def test_the_watermark_is_not_read(self):
        """The shipper stood down; its frozen `last_ok_at` and a failure it
        stamped say nothing about the shadow. Mutation: gate on
        `_watermark_findings` — the signature has nowhere to take one, and
        the module-level helper must not be called from the function."""
        import inspect
        import textwrap

        tree = ast.parse(textwrap.dedent(inspect.getsource(compare_shadow)))
        called = {getattr(n.func, "id", getattr(n.func, "attr", ""))
                  for n in ast.walk(tree) if isinstance(n, ast.Call)}
        assert "_watermark_findings" not in called

    def test_every_finding_has_a_lever_a_label_and_a_registry_entry(self):
        from core.alerting import REGISTRY
        from core.data_quality import DEFAULT_REMEDIATION, HUMAN_CHECK_NAMES, remediation_for

        for name in ("shadow_duckdb_only_rows", "shadow_missing_in_duckdb",
                     "shadow_row_values", "shadow_pruned_rows"):
            assert name in REGISTRY, name
            assert name in HUMAN_CHECK_NAMES, name
            assert remediation_for([name]) != [DEFAULT_REMEDIATION], name


class TestEveryShadowTableHasAPostgresClock:
    def test_the_eight_candidates_resolve_one(self):
        """The shadow comparison's grace is per row and reads a Postgres clock;
        the journal's children borrow their run's, spelled for Postgres.
        Mutation: delete `pg_synced_column` from `app.data_quality_issues`."""
        for table in SHADOW_CANDIDATES:
            clock = pg_clock_expr(_spec(table))
            assert clock, f"{table} has no clock Postgres can read"
            assert "app." in clock or clock in _spec(table).columns, (table, clock)
            # Never DuckDB's own expression, which names DuckDB's tables.
            assert "FROM data_quality_runs" not in clock, (table, clock)

    def test_every_registered_shadow_chain_too(self):
        for chain in write_chains.shadow_chains():
            for table in chain.CHAIN_TABLES:
                assert pg_clock_expr(_spec(table)), (chain.__name__, table)


# ─── The comparison in the job: Postgres first, then DuckDB ─────────────────


class _Ctx:
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *exc):
        return False


class _Pool:
    """Answers the owner rows, a failing watermark on the ledger, and the
    ledger's rows with a clock; logs every read in order."""

    def __init__(self, log, rows=(), owners=None):
        self.log, self.rows, self.owners = log, list(rows), owners or {}

    def acquire(self):
        return _Ctx(self)

    async def fetch(self, sql, *args):
        if "meta.chain_watermarks" in sql:
            return [{"key": f"owner:{t}", "value": v} for t, v in self.owners.items()]
        if "meta.mirror_state" in sql:
            return [{"table_name": SENDS, "last_attempted_at": OLD,
                     "last_ok_at": OLD - timedelta(days=5), "failures_since_ok": 4,
                     "last_error": "stamped by a mismatch", "last_rows": 0,
                     "backfilled_at": None}]
        if f"FROM {SENDS}" in sql:
            self.log.append(("postgres", SENDS, "_shadow_clock" in sql))
            return [dict(zip(("week_start", "sales_type", "revenue", "orders",
                              "sent_at", "_shadow_clock"), r + (r[-1],)))
                    for r in self.rows]
        return []


async def _store(tmp_path, log, rows):
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "shadow.duckdb")
    await store.connect()
    async with store.connection() as conn:
        for row in rows:
            conn.execute(
                "INSERT INTO weekly_report_sends (week_start, sales_type, revenue, "
                "orders, sent_at) VALUES (?, ?, ?, ?, ?)", list(row))
    real = store.connection

    def connection():
        log.append(("duckdb",))
        return real()

    store.connection = connection
    return store


@pytest.fixture
def pg(flags):
    from unittest.mock import AsyncMock

    from core import pg_landing

    flags.delenv(pg_landing.MIRROR_ENV, raising=False)
    flags.setattr("core.pg.require_revision", AsyncMock())

    def install(pool):
        flags.setattr("core.pg.get_pool", AsyncMock(return_value=pool))
        return pool

    return install


ROW = (W1, "retail", Decimal("10.00"), 3, OLD)


class TestTheJobComparesTheShadow:
    @pytest.mark.asyncio
    async def test_it_compares_and_ignores_the_frozen_watermark(
            self, shadow_chain, pg, tmp_path):
        """Mutation: drop `- shadowed` in `reconcile_operational` — the ledger
        is never read and nothing would ever say a shadow failed. And a
        `mirror_failing` here would be the shipper's stamp read as the
        shadow's health."""
        from core.mirror_reconciliation import reconcile_operational

        shadow_chain()
        log = []
        pg(_Pool(log, rows=[ROW, (W2, "retail", Decimal("9.00"), 2, OLD)]))
        store = await _store(tmp_path, log, [ROW])
        issues = await reconcile_operational(store, now=NOW)

        on_sends = _names(i for i in issues if i.table_name == SENDS)
        assert set(on_sends) == {"shadow_missing_in_duckdb"}, issues
        assert ("postgres", SENDS, True) in log

    @pytest.mark.asyncio
    async def test_postgres_is_read_before_duckdb(self, shadow_chain, pg, tmp_path):
        """A write landing between the reads then shows DuckDB-only with a
        fresh DuckDB clock — forgiven on the side that always has one.
        Mutation: read DuckDB first."""
        from core.mirror_reconciliation import reconcile_operational

        shadow_chain()
        log = []
        pg(_Pool(log, rows=[ROW]))
        store = await _store(tmp_path, log, [ROW])
        log.clear()
        await reconcile_operational(store, now=NOW)
        first_pg = log.index(("postgres", SENDS, True))
        assert ("duckdb",) in log[first_pg:], log
        assert ("duckdb",) not in log[:first_pg], log

    @pytest.mark.asyncio
    async def test_a_frozen_chain_still_stands_down(self, shadow_chain, pg, tmp_path):
        from core.mirror_reconciliation import reconcile_operational

        shadow_chain(shadow=False)
        log = []
        pg(_Pool(log, rows=[ROW]))
        issues = await reconcile_operational(await _store(tmp_path, log, [ROW]), now=NOW)
        assert not [i for i in issues if i.table_name == SENDS]
        assert not [e for e in log if e[0] == "postgres"]

    @pytest.mark.asyncio
    async def test_an_owner_row_without_its_marker_stays_stood_down(
            self, shadow_chain, pg, tmp_path):
        """The marker lost, the flag at duckdb: the chain follows its flag
        again and its tables are owned in Postgres. Not compared — the latch
        that is wrong has its own page. Mutation: compare whenever owner rows
        exist."""
        from core.mirror_reconciliation import reconcile_operational

        shadow_chain(env=lambda: False)
        log = []
        pg(_Pool(log, rows=[ROW], owners={SENDS: "2026-09-30T10:00:00+00:00"}))
        issues = await reconcile_operational(await _store(tmp_path, log, [ROW]), now=NOW)
        assert "chain_latch_disagrees" in {i.check_name for i in issues}
        assert not [e for e in log if e[0] == "postgres"]
        assert not [i for i in issues if i.table_name == SENDS]


# ─── The copy-back: the prune rule and the carried keys ──────────────────────


class TestTheHandoverKnowsASweep:
    def _spec(self, table):
        from core import chain_transfer

        chain = types.SimpleNamespace(CHAIN="x", CHAIN_TABLES=(table,))
        (spec,) = chain_transfer.chain_specs(chain)
        return spec

    def test_a_lagging_prune_is_info_and_a_newer_row_still_refuses(self):
        """Mutation: drop the rule (every chain-10 flip refuses on a lagging
        prune), or apply it to a row newer than Postgres's oldest (a stranded
        sample deleted unasked)."""
        from core.chain_transfer import classify_handover

        spec = self._spec("app.memory_samples")
        assert spec.prune_clock == "sampled_at"
        kept = OLD
        aged, stranded = OLD - timedelta(days=15), OLD + timedelta(hours=1)
        row = lambda at: (at, 100.0, 10.0, 512.0, 0)  # noqa: E731
        dk = {aged: row(aged), kept: row(kept), stranded: row(stranded)}
        pg = {kept: row(kept)}
        issues = classify_handover(spec, dk, pg, moved_on=True)
        found = {(i.check_name, i.severity): i.count for i in issues}
        assert found[("handover_rows_pruned", Severity.INFO)] == 1
        assert found[("handover_rows_missing", Severity.CRITICAL)] == 1

    def test_a_table_that_never_sweeps_has_no_prune_clock(self):
        assert self._spec(SENDS).prune_clock is None
        assert self._spec("app.data_quality_runs").prune_clock is None


class TestTheMarkerKeysTravelWithTheSyncKeys:
    def test_carried_keys_are_both_lists(self):
        """Mutation: drop `CHAIN_MARKER_KEYS` from `_carried_keys` — the digest
        beat left in `meta.chain_watermarks` is inherited, stale, by the next
        flip."""
        from core.chain_transfer import _carried_keys

        chain = types.SimpleNamespace(CHAIN_SYNC_KEYS=("last_sync_a",),
                                      CHAIN_MARKER_KEYS=("dq_digest_last_sent",))
        assert _carried_keys(chain) == ("last_sync_a", "dq_digest_last_sent")
        assert _carried_keys(types.SimpleNamespace()) == ()

    @pytest.mark.asyncio
    async def test_the_release_deletes_the_marker_keys(self, flags):
        from core.chain_transfer import release_chain

        deleted = []

        class Conn:
            def transaction(self):
                return _Ctx()

            async def execute(self, sql, keys):
                deleted.extend(keys)

        class Pool:
            def acquire(self):
                return _Ctx(Conn())

        chain = types.ModuleType("core.pg_fake_marker_write")
        chain.CHAIN_TABLES = ("app.data_quality_runs",)
        chain.CHAIN_MARKER_KEYS = ("dq_digest_last_sent",)
        await release_chain(Pool(), chain)
        assert "dq_digest_last_sent" in deleted
        assert "owner:app.data_quality_runs" in deleted

    def test_every_site_reads_through_the_one_helper(self):
        """`_read_sync_keys` and `release_chain` both ask `_carried_keys`, so a
        key cannot be carried back and then left standing. Mutation: spell
        `CHAIN_SYNC_KEYS` at one site again."""
        import inspect

        from core import chain_transfer

        for fn in (chain_transfer._read_sync_keys, chain_transfer.release_chain):
            src = inspect.getsource(fn)
            assert "_carried_keys(" in src, fn.__name__
            assert '"CHAIN_SYNC_KEYS"' not in src, fn.__name__


# ─── The shadow write primitive ──────────────────────────────────────────────


class TestIntoDuckdb:
    @pytest.mark.asyncio
    async def test_a_failing_write_rolls_back_is_counted_and_never_raises(self, tmp_path):
        """Mutation: re-raise — a delivered weekly report whose shadow failed
        would read as a job that failed, and send again tomorrow."""
        from core import shadow_writes
        from core.duckdb_store import DuckDBStore

        shadow_writes.reset()
        store = DuckDBStore(db_path=tmp_path / "s.duckdb")
        await store.connect()

        def write(conn):
            conn.execute("INSERT INTO weekly_report_sends (week_start, sales_type, "
                         "revenue, orders) VALUES ('2026-09-14', 'retail', 1, 1)")
            raise ValueError("second statement refused")

        assert await shadow_writes.into_duckdb(store, "pg_x_write", write) is False
        async with store.connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM weekly_report_sends").fetchone()[0] == 0
            # The connection is not left inside an aborted transaction.
            conn.execute("SELECT 1").fetchone()
        entry = shadow_writes.failures_of("pg_x_write")
        assert entry["count"] == 1 and entry["last_error_class"] == "ValueError"

        def ok(conn):
            conn.execute("INSERT INTO weekly_report_sends (week_start, sales_type, "
                         "revenue, orders) VALUES ('2026-09-14', 'retail', 1, 1)")

        assert await shadow_writes.into_duckdb(store, "pg_x_write", ok) is True
        async with store.connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM weekly_report_sends").fetchone()[0] == 1
        shadow_writes.reset()
        await store.close()

    def test_the_failure_is_published_by_class_only(self):
        from core import shadow_writes

        shadow_writes.reset()
        shadow_writes._record("pg_y_write", RuntimeError("user=ks_app host=secret"))
        published = str(shadow_writes.failures())
        assert "secret" not in published and "RuntimeError" in published
        shadow_writes.reset()


def _py_files(*dirs):
    for d in dirs:
        yield from sorted((ROOT / d).rglob("*.py"))


def _shadow_calls_inside_a_connection(tree) -> list:
    """Line numbers of `into_duckdb(...)` calls in the body of an `async with
    <x>.connection()` — the store lock is not reentrant, so such a call waits
    on itself."""
    out = []
    for w in ast.walk(tree):
        if not isinstance(w, ast.AsyncWith):
            continue
        if not any(isinstance(i.context_expr, ast.Call)
                   and getattr(i.context_expr.func, "attr", "") == "connection"
                   for i in w.items):
            continue
        for stmt in w.body:
            for n in ast.walk(stmt):
                if isinstance(n, ast.Call) and getattr(
                        n.func, "attr", getattr(n.func, "id", "")) == "into_duckdb":
                    out.append(n.lineno)
    return out


class TestTheShadowIsNeverNestedInTheStoreLock:
    def test_no_caller_holds_the_connection(self):
        """Mutation: call `into_duckdb` inside `async with store.connection()`
        — the shadow would wait for the lock its caller holds, for ever."""
        nested = {}
        for path in _py_files("core", "web", "bot", "scripts"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            lines = _shadow_calls_inside_a_connection(tree)
            if lines:
                nested[str(path.relative_to(ROOT))] = lines
        assert not nested, nested

    def test_the_walk_sees_the_shape_it_forbids(self):
        tree = ast.parse(
            "async def f(store):\n"
            "    async with store.connection() as conn:\n"
            "        await shadow_writes.into_duckdb(store, 'c', lambda c: None)\n")
        assert _shadow_calls_inside_a_connection(tree) == [3]

    def test_no_shadow_module_says_falling_back(self):
        """A shadow is not a fallback, and the soak greps that phrase.
        Mutation: log it in `core/shadow_writes.py` or a chain router."""
        offenders = []
        for path in _py_files("core"):
            text = path.read_text(encoding="utf-8")
            if "shadow_writes" not in text and "CHAIN_SHADOW" not in text:
                continue
            for n in ast.walk(ast.parse(text)):
                if isinstance(n, ast.Constant) and isinstance(n.value, str) \
                        and "falling back to duckdb" in n.value.lower():
                    offenders.append(str(path.relative_to(ROOT)))
        assert not offenders, offenders


# ─── Every shadow module is registered, and has the shapes it needs ─────────


def _declares_shadow(tree) -> bool:
    for n in tree.body:
        if isinstance(n, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "CHAIN_SHADOW" for t in n.targets):
            return isinstance(n.value, ast.Constant) and n.value.value is True
    return False


class TestEveryShadowChainIsRegistered:
    def test_the_walk_finds_them_all_and_the_registry_holds_them_all(self):
        """Mutation: a module declaring `CHAIN_SHADOW = True` left out of
        `WRITE_CHAINS` — its tables would be copied over and never compared."""
        declared = {p.stem for p in _py_files("core")
                    if _declares_shadow(ast.parse(p.read_text(encoding="utf-8")))}
        registered = {write_chains.chain_name(c) for c in write_chains.shadow_chains()}
        assert declared == registered, (declared, registered)

    def test_the_walk_reads_the_declaration(self):
        assert _declares_shadow(ast.parse("CHAIN_SHADOW = True\n"))
        assert not _declares_shadow(ast.parse("CHAIN_SHADOW = False\n"))

    def test_every_shadow_table_is_one_the_copy_back_replaces_whole(self):
        """A shadow chain's way back is a full replace (`_FULL_REPLACE`) and
        its comparison is an `OPERATIONAL_TABLES` spec; `chain_specs` raises
        for a table with neither."""
        from core import chain_transfer
        from core.pg_operational import _FULL_REPLACE

        whole = {pg for pg, *_ in _FULL_REPLACE}
        for chain in write_chains.shadow_chains():
            assert set(chain.CHAIN_TABLES) <= whole, chain.__name__
            assert chain_transfer.chain_specs(chain)
        assert set(SHADOW_CANDIDATES) <= whole
