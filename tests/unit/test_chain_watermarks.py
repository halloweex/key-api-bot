"""Chain 1's watermarks: one registry for the getter, the setter and the check.

The defect this pins (revision 0032): under `KS_WRITE_INVENTORY=postgres` the
setter wrote `last_sync_offers`/`last_sync_stocks` to Postgres, the getter read
them from DuckDB, where they had stopped moving, and the freshness check judged
DuckDB's frozen copy — so the sync would have run on every tick and the check
would have paged "offers stalled" two days after a switch that changed nothing.

These run without Postgres: the freshness check is handed its watermarks, which
is the point of its signature. The round trip against a real Postgres is in
`tests/integration/test_chain_watermarks.py`.
"""
from __future__ import annotations

import ast
import inspect
import pathlib
import textwrap
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from core.data_quality import _freshness_check, check_internal_integrity
from core.duckdb_store import DuckDBStore

CORE = pathlib.Path(__file__).resolve().parents[2] / "core"
ENTITIES = ("orders", "products", "buyers", "offers", "stocks",
            "managers", "categories", "expense_types")


@pytest.fixture
def flags(monkeypatch):
    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS"):
        monkeypatch.delenv(env, raising=False)
    return monkeypatch


@pytest_asyncio.fixture
async def frozen_duckdb(tmp_path):
    """Every entity freshly synced in DuckDB except the chain's two, which
    stopped 60 h ago — the state DuckDB is in two and a half days after the
    switch. The threshold for both is 48 h."""
    store = DuckDBStore(db_path=tmp_path / "wm.duckdb")
    await store.connect()
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        for entity in ENTITIES:
            stamp = now - (timedelta(hours=60) if entity in ("offers", "stocks")
                           else timedelta(minutes=5))
            conn.execute(
                "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
                "VALUES (?, ?, CURRENT_TIMESTAMP)",
                [f"last_sync_{entity}", stamp.isoformat()])
    yield store, now
    await store.close()


def _names(issues):
    return sorted(i.check_name for i in issues)


class TestWhichKeysHaveMoved:
    def test_none_while_every_chain_writes_duckdb(self, flags):
        from core.write_chains import stood_down_sync_keys
        assert stood_down_sync_keys() == frozenset()

    def test_the_inventory_chain_takes_exactly_its_two(self, flags):
        from core.write_chains import stood_down_sync_keys
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        assert stood_down_sync_keys() == frozenset({"last_sync_offers", "last_sync_stocks"})

    def test_a_chain_with_no_watermarks_moves_none(self, flags):
        from core.write_chains import stood_down_sync_keys
        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        assert stood_down_sync_keys() == frozenset()


class TestTheFreshnessCheckFollowsTheWatermarks:
    @pytest.mark.asyncio
    async def test_without_the_flag_a_stale_duckdb_stamp_is_still_a_stall(
        self, flags, frozen_duckdb,
    ):
        """The control: DuckDB still owns the keys, so 60 h is a real stall."""
        store, now = frozen_duckdb
        async with store.connection() as conn:
            issues = _freshness_check(conn, now)
        assert _names(issues) == ["freshness_offers", "freshness_stocks"]

    @pytest.mark.asyncio
    async def test_under_the_flag_the_frozen_copy_is_not_judged(
        self, flags, frozen_duckdb,
    ):
        store, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        fresh = (now - timedelta(minutes=20)).isoformat()
        async with store.connection() as conn:
            issues = _freshness_check(conn, now, chain_watermarks={
                "last_sync_offers": fresh, "last_sync_stocks": fresh})
        assert issues == []

    @pytest.mark.asyncio
    async def test_under_the_flag_a_stale_postgres_stamp_is_a_stall(
        self, flags, frozen_duckdb,
    ):
        """What makes the test above mean something: the values are read, not
        merely skipped. Only offers is stale here, and only offers is named."""
        store, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        async with store.connection() as conn:
            issues = _freshness_check(conn, now, chain_watermarks={
                "last_sync_offers": (now - timedelta(hours=50)).isoformat(),
                "last_sync_stocks": (now - timedelta(minutes=20)).isoformat()})
        assert _names(issues) == ["freshness_offers"]
        assert issues[0].count == 50

    @pytest.mark.asyncio
    async def test_a_key_postgres_never_received_is_named_where_it_is_missing(
        self, flags, frozen_duckdb,
    ):
        """DuckDB's frozen copy must not stand in for a watermark Postgres lacks."""
        store, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        async with store.connection() as conn:
            issues = _freshness_check(conn, now, chain_watermarks={
                "last_sync_offers": (now - timedelta(minutes=20)).isoformat()})
        assert _names(issues) == ["freshness_stocks"]
        assert "meta.chain_watermarks" in issues[0].description

    @pytest.mark.asyncio
    async def test_called_without_them_it_says_it_is_not_watching(
        self, flags, frozen_duckdb,
    ):
        """Neither judging the frozen copy nor going quiet: one WARN that names
        the unwatched syncs, and nothing about them either way."""
        store, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        async with store.connection() as conn:
            issues = _freshness_check(conn, now)
        assert _names(issues) == ["sync_watermarks_unwatched"]
        assert "offers" in issues[0].description and "stocks" in issues[0].description

    @pytest.mark.asyncio
    async def test_the_integrity_scan_hands_them_through(self, flags, frozen_duckdb):
        store, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        fresh = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        async with store.connection() as conn:
            handed = check_internal_integrity(conn, chain_watermarks={
                "last_sync_offers": fresh, "last_sync_stocks": fresh})
            not_handed = check_internal_integrity(conn)
        assert not {"freshness_offers", "freshness_stocks",
                    "sync_watermarks_unwatched"} & set(_names(handed))
        assert "sync_watermarks_unwatched" in _names(not_handed)


class TestTheWiringHasOneHome:
    def test_the_integrity_job_reads_them_and_passes_them(self):
        from core.scheduler import BackgroundScheduler

        tree = ast.parse(textwrap.dedent(
            inspect.getsource(BackgroundScheduler._run_dq_integrity)))
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
        called = {c.func.id for c in calls if isinstance(c.func, ast.Name)}
        assert {"stood_down_sync_keys", "read_values"} <= called
        scan = [c for c in calls if isinstance(c.func, ast.Name)
                and c.func.id == "check_internal_integrity"]
        assert scan and all(
            any(k.arg == "chain_watermarks" for k in c.keywords) for c in scan)

    def test_the_store_asks_the_registry_on_both_sides(self):
        """Both ask the registry which chain owns the key — the same question,
        so they cannot disagree — and never every chain, or a typo in an
        unrelated KS_WRITE_* stops the orders sync (DN-01)."""
        for fn in (DuckDBStore.get_last_sync_time, DuckDBStore.set_last_sync_time):
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            called = {n.func.id for n in ast.walk(tree)
                      if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
            assert "chain_for_sync_key" in called, fn.__name__
            assert not called & {"stood_down_sync_keys", "stood_down_tables"}, (
                f"{fn.__name__} evaluates every chain's flag for one key")

    def test_nothing_copies_or_compares_the_new_table(self):
        """`meta.chain_watermarks` is safe only because no copy out of DuckDB
        reaches it. A string naming it in the shipper or the comparison is the
        first step to the hourly rollback this table exists to escape."""
        for name in ("pg_operational.py", "mirror_reconciliation.py",
                     "pg_replication.py"):
            tree = ast.parse((CORE / name).read_text(encoding="utf-8"))
            docstrings = {id(n.value) for n in ast.walk(tree)
                          if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant)}
            named = [n.value for n in ast.walk(tree)
                     if isinstance(n, ast.Constant) and isinstance(n.value, str)
                     and id(n) not in docstrings and "chain_watermarks" in n.value]
            assert named == [], f"{name} names meta.chain_watermarks: {named}"
