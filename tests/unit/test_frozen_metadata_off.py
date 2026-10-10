"""Stage 5, PR-5: under `KS_DUCKDB=off`, what was inherited from DuckDB's
`sync_metadata` is read from its Postgres copy, and the file is not opened.

Four places read DuckDB for a key Postgres does not hold yet (the design's
2.5): the sync window a chain inherits (`get_last_sync_time`), the freshness
check's stand-in and its bootstrap gate, the digest's beat, and the report
ledger's adoption of a week delivered before the flip. Under `off` the first
three read `app.sync_metadata` — the hourly copy of the same table
(`core.pg_frozen_metadata`) — and the fourth reads nothing.

Each class holds one consumer: under `off` the copy answers and DuckDB is
never asked (`duckdb_switch.opened() == {}`, and the store's directory never
made); under `on` the answer is DuckDB's, as before. The round trip against a
real `app.sync_metadata` is at the end, skipped without `KS_PG_DSN`.

Every guard names, in its docstring, the mutation it catches.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from core import (
    chain_latch, data_quality as dq, dq_journal, duckdb_switch,
    pg_chain_watermarks, pg_frozen_metadata, pg_traffic_ledger_write,
    pg_weekly_ledger_write, report_ledger, shadow_writes,
)
from core.duckdb_store import DuckDBStore

UTC = timezone.utc
NOW = datetime(2026, 10, 10, 9, 0, tzinfo=UTC)
WEEK = datetime(2026, 9, 28).date()
DSN = os.getenv("KS_PG_DSN")


@pytest.fixture
def flags(monkeypatch):
    from core import write_chains

    for chain in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    shadow_writes.reset()
    yield monkeypatch
    shadow_writes.reset()


class Copy:
    """`app.sync_metadata` as a dict: what the copy holds, and every key a
    reader asked it for."""

    def __init__(self, monkeypatch, rows=None):
        self.rows = dict(rows or {})
        self.asked = []
        self.refuse = None

        async def frozen_values(keys):
            keys = sorted(set(keys))
            self.asked.extend(keys)
            if self.refuse:
                raise self.refuse
            return {k: self.rows[k] for k in keys if k in self.rows}

        async def frozen_sync_rows():
            self.asked.append("last_sync_*")
            if self.refuse:
                raise self.refuse
            return {k: v for k, v in self.rows.items() if k.startswith("last_sync_")}

        async def values_on(conn, keys):
            return await frozen_values(keys)

        monkeypatch.setattr(pg_frozen_metadata, "frozen_values", frozen_values)
        monkeypatch.setattr(pg_frozen_metadata, "frozen_sync_rows", frozen_sync_rows)
        monkeypatch.setattr(pg_frozen_metadata, "values_on", values_on)


def _untouched(data_dir):
    """Nothing reached for the file: no refusal counted, no directory made."""
    assert duckdb_switch.opened() == {}
    assert not data_dir.exists()


# ─── 1. The sync window a chain inherits ─────────────────────────────────────


class TestTheInheritedWatermark:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("chain,key", [
        ("pg_orders_write", "orders"),
        ("pg_catalogue_write", "products"),
        ("pg_catalogue_write", "categories"),
        ("pg_expense_types_write", "expense_types"),
    ])
    async def test_under_off_the_copy_stands_in(self, flags, duckdb_off, chain, key):
        """Chains 3, 6 and 6a inherit while Postgres holds no key: under off
        the stand-in is DuckDB's stamp as Postgres copied it, read without the
        file. Mutation: drop the `is_off()` branch — the read goes to DuckDB,
        is refused, and the order step of every tick fails (the sweep's rows
        for `get_last_sync_time`)."""
        frozen = NOW - timedelta(hours=3)
        chain_latch.latch(chain)
        flags.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=None))
        copy = Copy(flags, {f"last_sync_{key}": frozen.isoformat()})
        assert await DuckDBStore().get_last_sync_time(key) == frozen
        assert copy.asked == [f"last_sync_{key}"]
        _untouched(duckdb_off)

    @pytest.mark.asyncio
    async def test_postgres_s_own_value_wins_and_the_copy_is_not_asked(
            self, flags, duckdb_off):
        live = NOW - timedelta(minutes=1)
        chain_latch.latch("pg_orders_write")
        flags.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=live))
        copy = Copy(flags, {"last_sync_orders": (NOW - timedelta(days=3)).isoformat()})
        assert await DuckDBStore().get_last_sync_time("orders") == live
        assert copy.asked == []
        _untouched(duckdb_off)

    @pytest.mark.asyncio
    async def test_absent_from_the_copy_too_it_is_absent(self, flags, duckdb_off):
        """The copy has no key: the answer is DuckDB's would-be answer, None,
        and still not the file. Mutation: fall through to DuckDB when the
        copy is empty."""
        chain_latch.latch("pg_orders_write")
        flags.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=None))
        Copy(flags)
        assert await DuckDBStore().get_last_sync_time("orders") is None
        _untouched(duckdb_off)

    @pytest.mark.asyncio
    async def test_a_chain_that_does_not_inherit_never_asks_the_copy(
            self, flags, duckdb_off):
        """Chain 1's absent key is "never synced" in either mode. Mutation:
        consult the copy for every moved key."""
        chain_latch.latch("pg_inventory_write")
        flags.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=None))
        copy = Copy(flags, {"last_sync_offers": NOW.isoformat()})
        assert await DuckDBStore().get_last_sync_time("offers") is None
        assert copy.asked == []
        _untouched(duckdb_off)

    @pytest.mark.asyncio
    async def test_a_copy_that_cannot_be_read_raises_and_the_file_stays_shut(
            self, flags, duckdb_off):
        """Postgres refusing is the step's failure, as Postgres refusing the
        chain's own key was — never DuckDB in its place."""
        chain_latch.latch("pg_orders_write")
        flags.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=None))
        copy = Copy(flags)
        copy.refuse = ConnectionRefusedError("down")
        with pytest.raises(ConnectionRefusedError):
            await DuckDBStore().get_last_sync_time("orders")
        _untouched(duckdb_off)

    @pytest.mark.asyncio
    async def test_under_on_it_is_duckdb_s_and_the_copy_is_not_asked(
            self, flags, tmp_path):
        """The control: under on the inherit reads DuckDB, as before."""
        store = DuckDBStore(db_path=tmp_path / "on.duckdb")
        await store.connect()
        try:
            frozen = NOW - timedelta(hours=5)
            await store.set_last_sync_time("orders", frozen)     # chain off: DuckDB's
            chain_latch.latch("pg_orders_write")
            flags.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=None))
            copy = Copy(flags, {"last_sync_orders": NOW.isoformat()})
            assert await store.get_last_sync_time("orders") == frozen
            assert copy.asked == []
        finally:
            await store.close()


# ─── 2. The freshness check ──────────────────────────────────────────────────


def _rows(**ages):
    return {f"last_sync_{k}": (NOW - v).isoformat() for k, v in ages.items()}


FRESH = timedelta(minutes=5)
ALL_FRESH = {e: FRESH for e in dq.FRESHNESS_THRESHOLDS}


def _latch_every_chain():
    from core.write_chains import WRITE_CHAINS, chain_name

    for chain in WRITE_CHAINS:
        chain_latch.latch(chain_name(chain))


class _NoConn:
    def execute(self, *_a, **_k):
        raise AssertionError("the freshness check read DuckDB under off")


def _names(issues):
    return sorted(i.check_name for i in issues)


class TestTheFreshnessCheck:
    def test_under_off_the_copy_is_the_rows_and_conn_is_never_read(
            self, flags, duckdb_off):
        """Every chain latched, Postgres holding all but the four inherited
        keys: the copy stands in for those, at the same thresholds. Mutation:
        read `conn` under off."""
        _latch_every_chain()
        inherited = ("orders", "products", "categories", "expense_types")
        watermarks = _rows(**{e: FRESH for e in dq.FRESHNESS_THRESHOLDS
                              if e not in inherited})
        frozen = _rows(**{**ALL_FRESH, "orders": timedelta(hours=7)})
        issues = dq._freshness_check(_NoConn(), NOW, chain_watermarks=watermarks,
                                     frozen_rows=frozen)
        assert _names(issues) == ["freshness_orders"]
        assert "as Postgres copied it" in issues[0].description
        _untouched(duckdb_off)

    def test_under_off_postgres_s_own_value_wins_over_the_copy(self, flags, duckdb_off):
        _latch_every_chain()
        watermarks = _rows(**ALL_FRESH)
        frozen = _rows(**{e: timedelta(days=30) for e in dq.FRESHNESS_THRESHOLDS})
        assert dq._freshness_check(_NoConn(), NOW, chain_watermarks=watermarks,
                                   frozen_rows=frozen) == []

    def test_under_off_the_bootstrap_gate_reads_postgres(self, flags, duckdb_off):
        """Nothing in the copy and nothing in `meta.chain_watermarks`: a fresh
        install, said of Postgres. With history in either, the check judges.
        Mutation: gate on the copy alone — a copy holding no `last_sync_*`
        would silence every watermark Postgres holds."""
        _latch_every_chain()
        assert dq._freshness_check(_NoConn(), NOW, chain_watermarks={},
                                   frozen_rows={}) == []
        stale = _rows(offers=timedelta(hours=60))
        issues = dq._freshness_check(_NoConn(), NOW, chain_watermarks=stale,
                                     frozen_rows={})
        assert "freshness_offers" in _names(issues)

    def test_under_off_an_unreadable_copy_is_unwatched_not_never_synced(
            self, flags, duckdb_off):
        """A copy that failed to read is neither a bootstrap nor "never
        synced": the inherited entities Postgres holds no key for are named
        unwatched, and the rest are judged. Mutation: read None as `{}` —
        the inherited entities read "never synced", and with no watermark in
        hand the gate would call a failed read a fresh install."""
        _latch_every_chain()
        watermarks = _rows(**{e: FRESH for e in dq.FRESHNESS_THRESHOLDS
                              if e not in ("orders", "categories")})
        watermarks["last_sync_offers"] = (NOW - timedelta(hours=60)).isoformat()
        issues = dq._freshness_check(_NoConn(), NOW, chain_watermarks=watermarks,
                                     frozen_rows=None)
        assert _names(issues) == ["freshness_offers", "sync_watermarks_unwatched"]
        (unwatched,) = [i for i in issues if i.check_name == "sync_watermarks_unwatched"]
        assert unwatched.count == 2
        assert "categories, orders" in unwatched.description
        assert "app.sync_metadata" in unwatched.description
        assert "freshness_orders" in dq.unverified_conditions([], issues)

    def test_under_off_nothing_read_at_all_is_unwatched_whole(self, flags, duckdb_off):
        _latch_every_chain()
        issues = dq._freshness_check(_NoConn(), NOW, chain_watermarks=None,
                                     frozen_rows=None)
        assert _names(issues) == ["sync_watermarks_unwatched"]
        assert issues[0].count == len(dq.FRESHNESS_THRESHOLDS)

    @pytest.mark.asyncio
    async def test_under_on_the_rows_are_duckdb_s_and_the_copy_is_ignored(
            self, flags, tmp_path):
        """The control: under on `frozen_rows` is not consulted. Mutation:
        prefer `frozen_rows` whenever given."""
        store = DuckDBStore(db_path=tmp_path / "on.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                for key, value in _rows(**{**ALL_FRESH, "offers": timedelta(hours=60)}).items():
                    conn.execute("INSERT OR REPLACE INTO sync_metadata (key, value, "
                                 "updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                                 [key, value])
                issues = dq._freshness_check(conn, NOW, frozen_rows=_rows(**ALL_FRESH))
                assert _names(issues) == ["freshness_offers"]
                assert dq._freshness_check(conn, NOW) == issues
        finally:
            await store.close()

    def test_the_judge_is_the_check_s_verdict_over_rows(self, flags):
        """The split is a move, not a rewrite: under on, the judge over the
        rows DuckDB returned is the check."""
        rows = _rows(**{**ALL_FRESH, "stocks": timedelta(hours=50)})
        assert _names(dq._judge_freshness(rows, NOW)) == ["freshness_stocks"]
        assert dq._judge_freshness({}, NOW) == []

    def test_the_scan_hands_the_copy_through(self, flags, duckdb_off):
        """`check_internal_integrity` passes `frozen_sync_rows` to the
        freshness check; the scheduler's job reads it under off. Mutation:
        drop either."""
        import ast
        import inspect
        import textwrap

        from core.scheduler import BackgroundScheduler

        tree = ast.parse(textwrap.dedent(
            inspect.getsource(BackgroundScheduler._run_dq_integrity)))
        scans = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                 and getattr(n.func, "id", None) == "check_internal_integrity"]
        assert scans and all(any(k.arg == "frozen_sync_rows" for k in c.keywords)
                             for c in scans)
        source = inspect.getsource(dq.check_internal_integrity)
        assert "frozen_rows=frozen_sync_rows" in source


# ─── 3. The digest's beat ────────────────────────────────────────────────────


@pytest.fixture
def pg_beat(monkeypatch):
    """The journal chain's Postgres readers, with a beat absent from
    `meta.chain_watermarks` unless a test sets one."""
    from core import pg_dq_journal_write as chain

    state = {"marker": None}

    @asynccontextmanager
    async def reading():
        yield object()

    async def digest_marker(conn):
        return state["marker"]

    async def latest_run(conn, layer=None):
        return None

    monkeypatch.setattr(chain, "reading", reading)
    monkeypatch.setattr(chain, "digest_marker", digest_marker)
    monkeypatch.setattr(chain, "latest_run", latest_run)
    monkeypatch.setenv(chain.WRITE_ENV, "postgres")
    return state


class _Refused:
    """A store whose file may not be reached."""

    @asynccontextmanager
    async def connection(self):
        raise AssertionError("the digest reached DuckDB")
        yield  # pragma: no cover


class TestTheDigestBeat:
    @pytest.mark.asyncio
    async def test_under_off_an_absent_beat_is_the_copy_s(self, flags, duckdb_off, pg_beat):
        """Mutation: drop the copy — the first digest under off would restate
        every standing finding as new, or the read would reach the file."""
        beat = "2026-10-09T06:00:00+00:00"
        copy = Copy(flags, {dq_journal.DIGEST_MARKER_KEY: beat})
        last, _ = await dq_journal.digest_inputs(_Refused(), ("integrity",), NOW)
        assert last == datetime.fromisoformat(beat)
        assert copy.asked == [dq_journal.DIGEST_MARKER_KEY]
        _untouched(duckdb_off)

    @pytest.mark.asyncio
    async def test_under_off_postgres_s_beat_wins(self, flags, duckdb_off, pg_beat):
        pg_beat["marker"] = "2026-10-10T06:00:00+00:00"
        copy = Copy(flags, {dq_journal.DIGEST_MARKER_KEY: "2026-01-01T06:00:00+00:00"})
        last, _ = await dq_journal.digest_inputs(_Refused(), ("integrity",), NOW)
        assert last == datetime.fromisoformat(pg_beat["marker"])
        assert copy.asked == []

    @pytest.mark.asyncio
    async def test_under_off_no_beat_anywhere_is_none(self, flags, duckdb_off, pg_beat):
        Copy(flags)
        last, _ = await dq_journal.digest_inputs(_Refused(), ("integrity",), NOW)
        assert last is None
        _untouched(duckdb_off)

    @pytest.mark.asyncio
    async def test_under_on_the_beat_is_inherited_from_duckdb(self, flags, tmp_path,
                                                              pg_beat):
        """The control."""
        store = DuckDBStore(db_path=tmp_path / "on.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                conn.execute("INSERT OR REPLACE INTO sync_metadata (key, value, "
                             "updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                             [dq_journal.DIGEST_MARKER_KEY, "2026-09-29T06:00:00+00:00"])
            copy = Copy(flags, {dq_journal.DIGEST_MARKER_KEY: "2026-01-01T06:00:00+00:00"})
            last, _ = await dq_journal.digest_inputs(store, ("integrity",), NOW)
            assert last == datetime(2026, 9, 29, 6, tzinfo=UTC)
            assert copy.asked == []
        finally:
            await store.close()


# ─── 4. The report ledgers' adoption ─────────────────────────────────────────


class TestTheLedgerAdoptsNothingUnderOff:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("ledger,chain", [
        (report_ledger.WEEKLY, pg_weekly_ledger_write),
        (report_ledger.TRAFFIC, pg_traffic_ledger_write),
    ])
    async def test_postgres_alone_answers(self, flags, duckdb_off, ledger, chain):
        """Mutation: keep the adopt under off — the gate reaches the file
        before every report (the sweep's `already_sent` rows)."""
        flags.setenv(chain.WRITE_ENV, "postgres")
        flags.setattr(chain, "find_sent", AsyncMock(return_value=None))
        assert await report_ledger.already_sent(_Refused(), ledger, WEEK, "retail") is False
        flags.setattr(chain, "find_sent", AsyncMock(return_value={"week_start": WEEK}))
        assert await report_ledger.already_sent(_Refused(), ledger, WEEK, "retail") is True
        _untouched(duckdb_off)

    @pytest.mark.asyncio
    async def test_under_on_a_row_only_duckdb_holds_is_still_adopted(self, flags, tmp_path):
        """The control (`test_report_ledger.py` holds the rest of it)."""
        from core.weekly_report import mark_sent as dk_mark

        chain = pg_weekly_ledger_write
        store = DuckDBStore(db_path=tmp_path / "on.duckdb")
        await store.connect()
        try:
            flags.setenv(chain.WRITE_ENV, "postgres")
            flags.setattr(chain, "find_sent", AsyncMock(return_value=None))
            adopted = AsyncMock()
            flags.setattr(chain, "mark_sent", adopted)
            async with store.connection() as conn:
                dk_mark(conn, WEEK, "retail", 12.5, 4)
            assert await report_ledger.already_sent(store, report_ledger.WEEKLY,
                                                    WEEK, "retail") is True
            assert adopted.await_count == 1
        finally:
            await store.close()


# ─── The module ──────────────────────────────────────────────────────────────


class TestTheModule:
    def test_one_spelling_of_the_table(self):
        from core.pg_operational import SYNC_METADATA_TABLE

        assert pg_frozen_metadata.TABLE == SYNC_METADATA_TABLE

    def test_it_reads_and_never_writes(self):
        """Nothing here may write the copy: its one writer stands down under
        off (PR-7), and a second one would make the copy a store."""
        import ast
        import inspect

        tree = ast.parse(inspect.getsource(pg_frozen_metadata))
        sql = [n.value for n in ast.walk(tree)
               if isinstance(n, ast.Constant) and isinstance(n.value, str)
               and n.value.lstrip().upper().startswith(("SELECT", "INSERT", "UPDATE",
                                                        "DELETE", "TRUNCATE"))]
        assert sql and all(s.lstrip().upper().startswith("SELECT") for s in sql)


# ─── Against a real `app.sync_metadata` ──────────────────────────────────────


@pytest_asyncio.fixture
async def pg_copy(monkeypatch):
    """A pool on the throwaway Postgres, `app.sync_metadata` and
    `meta.chain_watermarks` emptied for the test and put back after."""
    if not DSN:
        pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
    import asyncpg

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        held = {t: await conn.fetch(f"SELECT key, value, updated_at FROM {t}")
                for t in ("app.sync_metadata", "meta.chain_watermarks")}
        for table in held:
            await conn.execute(f"DELETE FROM {table}")
    monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=pool))
    monkeypatch.setattr("core.pg.require_revision", AsyncMock())
    try:
        yield pool
    finally:
        async with pool.acquire() as conn:
            for table, rows in held.items():
                await conn.execute(f"DELETE FROM {table}")
                if rows:
                    await conn.executemany(
                        f"INSERT INTO {table} (key, value, updated_at) "
                        f"VALUES ($1, $2, $3)", [tuple(r) for r in rows])
        await pool.close()


async def _copy(pool, rows):
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO app.sync_metadata (key, value, updated_at) "
            "VALUES ($1, $2, now())", list(rows.items()))


class TestAgainstARealCopy:
    @pytest.mark.asyncio
    async def test_the_readers_read_the_table(self, flags, pg_copy):
        rows = {"last_sync_orders": (NOW - timedelta(hours=2)).isoformat(),
                "last_sync_categories": (NOW - timedelta(days=3)).isoformat(),
                dq_journal.DIGEST_MARKER_KEY: "2026-10-09T06:00:00+00:00",
                "lastXsyncY": "not a watermark"}
        await _copy(pg_copy, rows)
        assert await pg_frozen_metadata.frozen_values(
            ["last_sync_orders", "absent"]) == {"last_sync_orders": rows["last_sync_orders"]}
        assert await pg_frozen_metadata.frozen_sync_rows() == {
            k: v for k, v in rows.items() if k.startswith("last_sync_")}
        assert await pg_frozen_metadata.frozen_stamp("last_sync_orders") == NOW - timedelta(hours=2)
        assert await pg_frozen_metadata.frozen_stamp("last_sync_absent") is None

    @pytest.mark.asyncio
    async def test_the_order_window_is_inherited_from_it_under_off(
            self, flags, duckdb_off, pg_copy):
        """End to end through the store's getter: chain 3 latched, Postgres's
        own key absent, the copy's stamp answers, and the file stays shut."""
        frozen = NOW - timedelta(hours=2)
        await _copy(pg_copy, {"last_sync_orders": frozen.isoformat()})
        chain_latch.latch("pg_orders_write")
        assert await DuckDBStore().get_last_sync_time("orders") == frozen
        await DuckDBStore().set_last_sync_time("orders", NOW)
        assert await DuckDBStore().get_last_sync_time("orders") == NOW
        _untouched(duckdb_off)
