"""Chain 10: the watchdogs' samples behind `KS_WRITE_WATCHDOGS`, shadowed in DuckDB.

The parts that need no database; the real-Postgres half is
`tests/integration/test_watchdog_samples_pg.py`. Every guard names, in its
docstring, the mutation it exists to catch.
"""
from __future__ import annotations

import ast
import pathlib
import re
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import pg_watchdog_write as chain, shadow_writes, watchdog_samples
from core.duckdb_store import DuckDBStore

ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def flag(monkeypatch):
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    shadow_writes.reset()

    def set_(value):
        if value is None:
            monkeypatch.delenv(chain.WRITE_ENV, raising=False)
        else:
            monkeypatch.setenv(chain.WRITE_ENV, value)
    yield set_
    shadow_writes.reset()


@pytest_asyncio.fixture
async def store(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "samples.duckdb")
    await s.connect()
    yield s
    await s.close()


def _sample(at=None, pct=40.0, free=100.0):
    return {"sampled_at": at or datetime.now(timezone.utc), "db_size_mb": 2_000.0,
            "disk_pct_used": pct, "disk_free_gb": free, "disk_used_bytes": 1}


MEM = {"working_set": 600 * 1024 * 1024, "page_cache": 100 * 1024 * 1024,
       "limit": 1024 * 1024 * 1024, "oom_kills": 0}


def _no_postgres(monkeypatch):
    boom = AsyncMock(side_effect=AssertionError("the duckdb branch asked for a pool"))
    monkeypatch.setattr("core.pg.get_pool", boom)
    monkeypatch.setattr("core.pg.require_revision", boom)
    return boom


def _record(store, events):
    real = store.connection

    def connection():
        events.append("duckdb")
        return real()

    store.connection = connection


async def _count(store, table):
    async with store.connection() as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ─── The one door ─────────────────────────────────────────────────────────────

_DOOR_FUNCS = {
    "insert_sample", "prune_old_samples", "insert_dir_samples",
    "prune_old_dir_samples", "fetch_sample_at_age", "fetch_dir_sample_at_age",
    "fetch_remainder_series", "fetch_last_sample", "fetch_peak_working_set_mb",
}
_DEFINERS = {"core/disk_monitor.py", "core/memory_monitor.py",
             "core/watchdog_samples.py", "core/pg_watchdog_write.py"}
# SQL over the three tables: their own modules, the router, and the stores'
# machinery (DDL, migrations, the hourly copy, its comparison, the copy-back,
# the standing watch).
_SQL_ALLOWED = _DEFINERS | {
    "core/migrations.py", "core/mirror_reconciliation.py", "core/pg_operational.py",
    "core/chain_transfer.py", "core/pg_chain_invariants.py",
    "core/snapshot_validation.py",
}
_SAMPLES_SQL = re.compile(
    r"\b(SELECT|INSERT|UPDATE|DELETE|FROM)\b[\s\S]*\b"
    r"(disk_samples|data_dir_samples|memory_samples)\b", re.IGNORECASE)


def _docstrings(tree) -> set:
    out = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                          ast.AsyncFunctionDef)) and n.body:
            first = n.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                out.add(id(first.value))
    return out


def _offenders_in(tree, rel):
    out = []
    docs = _docstrings(tree)
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and rel not in _DEFINERS:
            name = getattr(n.func, "id", getattr(n.func, "attr", ""))
            if name in _DOOR_FUNCS:
                out.append(f"{rel}:{n.lineno} calls {name}")
        if (isinstance(n, ast.Constant) and isinstance(n.value, str)
                and id(n) not in docs and rel not in _SQL_ALLOWED
                and _SAMPLES_SQL.search(n.value)):
            out.append(f"{rel}:{n.lineno} spells SQL over the samples")
    return out


class TestTheOneDoor:
    def test_nothing_goes_round_the_router(self):
        """Mutation: `insert_sample(conn, sample)` back in `_run_disk_watchdog`
        — under `postgres` the sample lands in DuckDB alone while the 24 h and
        168 h reads ask Postgres, and the watchdog reports growth of zero for
        ever (the chain map's warning)."""
        offenders = []
        for d in ("core", "web", "bot", "scripts"):
            for path in sorted((ROOT / d).rglob("*.py")):
                rel = str(path.relative_to(ROOT))
                offenders += _offenders_in(ast.parse(path.read_text(encoding="utf-8")), rel)
        assert offenders == []

    def test_the_walk_sees_the_shape(self):
        tree = ast.parse("def f(conn):\n    insert_sample(conn, s)\n"
                         "    conn.execute('DELETE FROM memory_samples')\n")
        assert len(_offenders_in(tree, "core/scheduler.py")) == 2


# ─── duckdb: the default, byte for byte ──────────────────────────────────────


class TestUnderDuckdbNothingChanged:
    @pytest.mark.asyncio
    async def test_the_disk_tick_is_one_connection_and_no_postgres(
            self, flag, store, monkeypatch):
        flag(None)
        pool = _no_postgres(monkeypatch)
        events = []
        _record(store, events)
        reads = await watchdog_samples.disk_tick(store, _sample(), {"duckdb": 5})
        assert events == ["duckdb"] and not pool.await_count
        assert reads["persisted"] is True and reads["history"] is None
        assert await _count(store, "disk_samples") == 1
        assert await _count(store, "data_dir_samples") == 1

    @pytest.mark.asyncio
    async def test_a_duckdb_failure_still_propagates_from_the_disk_tick(
            self, flag, store, monkeypatch):
        """Today's behaviour, byte for byte: the disk job never caught it.
        Mutation: catch it under duckdb too."""
        flag(None)
        monkeypatch.setattr("core.disk_monitor.insert_sample",
                            lambda conn, s: (_ for _ in ()).throw(RuntimeError("full")))
        with pytest.raises(RuntimeError, match="full"):
            await watchdog_samples.disk_tick(store, _sample(), {})

    @pytest.mark.asyncio
    async def test_the_memory_tick_is_one_connection_and_no_postgres(
            self, flag, store, monkeypatch):
        flag(None)
        pool = _no_postgres(monkeypatch)
        first = await watchdog_samples.memory_tick(store, MEM)
        second = await watchdog_samples.memory_tick(store, MEM)
        assert not pool.await_count
        assert first["last"] is None and second["last"]["oom_kills"] == 0
        assert second["peak_24h"] == pytest.approx(600.0)


# ─── postgres: the tick, then the shadow ─────────────────────────────────────


class TestUnderPostgres:
    @pytest.mark.asyncio
    async def test_postgres_then_duckdb_with_one_set_of_instants(
            self, flag, store, monkeypatch):
        """The sample's `sampled_at`, the directory set's and both cutoffs are
        computed once and handed to both stores. Mutation: compute any per
        store — the comparison then reports every sample as differing."""
        flag("postgres")
        seen = {}
        events = []

        async def disk_tick(sample, dir_now, **kw):
            events.append("postgres")
            seen.update(kw)
            return {"history": None, "dir_week_ago": None, "dir_six_ago": None,
                    "remainder": [], "deleted": 0}

        monkeypatch.setattr(chain, "disk_tick", disk_tick)
        _record(store, events)
        sample = _sample()
        reads = await watchdog_samples.disk_tick(store, sample, {"duckdb": 7})
        assert events == ["postgres", "duckdb"] and reads["persisted"] is True
        async with store.connection() as conn:
            (dir_at,) = conn.execute(
                "SELECT DISTINCT sampled_at FROM data_dir_samples").fetchone()
            (disk_at,) = conn.execute("SELECT sampled_at FROM disk_samples").fetchone()
        assert dir_at == seen["dir_sampled_at"]
        assert disk_at == sample["sampled_at"]
        assert seen["disk_cutoff"] == seen["now"] - timedelta(days=14)
        assert seen["dir_cutoff"] == seen["now"] - timedelta(days=21)

    @pytest.mark.asyncio
    async def test_the_shadow_prunes_at_the_cutoff_postgres_was_handed(
            self, flag, store, monkeypatch):
        flag("postgres")
        old = datetime.now(timezone.utc) - timedelta(days=15)
        async with store.connection() as conn:
            from core.disk_monitor import insert_sample

            insert_sample(conn, _sample(at=old))
        cutoffs = {}

        async def disk_tick(sample, dir_now, **kw):
            cutoffs.update(kw)
            return {"history": None, "dir_week_ago": None, "dir_six_ago": None,
                    "remainder": [], "deleted": 1}

        monkeypatch.setattr(chain, "disk_tick", disk_tick)
        await watchdog_samples.disk_tick(store, _sample(), {})
        async with store.connection() as conn:
            remaining = [r[0] for r in conn.execute(
                "SELECT sampled_at FROM disk_samples").fetchall()]
        assert all(at >= cutoffs["disk_cutoff"] for at in remaining) and len(remaining) == 1

    @pytest.mark.asyncio
    async def test_every_shadow_prune_uses_the_routers_instant(
            self, flag, store, monkeypatch):
        """The router's clock is put an hour behind the wall, and each table
        is seeded with a row half an hour inside its retention as the router
        sees it — half an hour outside it as the wall does. Mutation: let any
        DuckDB prune compute its own cutoff (`retention_days=` for `cutoff=`)
        — DuckDB would then drop a row Postgres kept, and the comparison would
        read every edge sample as `shadow_pruned_rows`. The test above cannot
        see it: the two instants it compares are microseconds apart."""
        flag("postgres")

        class _Behind(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(tz) - timedelta(hours=1)

        monkeypatch.setattr(watchdog_samples, "datetime", _Behind)
        seen = _Behind.now(timezone.utc)
        inside = timedelta(minutes=30)
        edge = {"disk_samples": seen - timedelta(days=14) + inside,
                "data_dir_samples": seen - timedelta(days=21) + inside,
                "memory_samples": seen - timedelta(days=14) + inside}
        async with store.connection() as conn:
            from core.disk_monitor import insert_dir_samples, insert_sample
            from core.memory_monitor import insert_sample as insert_memory

            insert_sample(conn, _sample(at=edge["disk_samples"]))
            insert_dir_samples(conn, {"duckdb": 5}, sampled_at=edge["data_dir_samples"])
            insert_memory(conn, MEM, sampled_at=edge["memory_samples"])

        async def disk_tick(sample, dir_now, **kw):
            return {"history": None, "dir_week_ago": None, "dir_six_ago": None,
                    "remainder": [], "deleted": 0}

        async def memory_tick(mem, **kw):
            return {"last": None, "peak_24h": None}

        monkeypatch.setattr(chain, "disk_tick", disk_tick)
        monkeypatch.setattr(chain, "memory_tick", memory_tick)
        await watchdog_samples.disk_tick(store, _sample(), {"duckdb": 6})
        await watchdog_samples.memory_tick(store, MEM)
        async with store.connection() as conn:
            for table, at in edge.items():
                kept = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE sampled_at = ?",
                                    [at]).fetchone()[0]
                assert kept >= 1, f"{table}: the shadow pruned a row Postgres was told to keep"

    @pytest.mark.asyncio
    async def test_a_refusing_postgres_returns_no_history_and_writes_nothing(
            self, flag, store, monkeypatch):
        """Mutation: let the failure propagate under postgres — the disk job
        would raise before judging capacity, on the day the disk is full."""
        flag("postgres")
        monkeypatch.setattr(chain, "disk_tick", AsyncMock(side_effect=OSError("no space")))
        monkeypatch.setattr(chain, "memory_tick", AsyncMock(side_effect=OSError("no space")))
        assert await watchdog_samples.disk_tick(store, _sample(), {"duckdb": 1}) == \
            watchdog_samples.EMPTY_DISK
        assert await watchdog_samples.memory_tick(store, MEM) == watchdog_samples.EMPTY_MEMORY
        assert await _count(store, "disk_samples") == 0
        assert await _count(store, "memory_samples") == 0

    @pytest.mark.asyncio
    async def test_a_typo_persists_nowhere_and_judges_anyway(self, flag, store, monkeypatch):
        flag("postgrse")
        pool = _no_postgres(monkeypatch)
        assert await watchdog_samples.disk_tick(store, _sample(), {}) == \
            watchdog_samples.EMPTY_DISK
        assert await watchdog_samples.memory_tick(store, MEM) == watchdog_samples.EMPTY_MEMORY
        assert not pool.await_count and await _count(store, "disk_samples") == 0

    @pytest.mark.asyncio
    async def test_the_capacity_page_goes_out_with_postgres_down(
            self, flag, store, monkeypatch):
        """The job end to end: a full disk and a Postgres that refuses. The
        alert is the one that must still be sent."""
        from core.scheduler import BackgroundScheduler

        flag("postgres")
        monkeypatch.setattr(chain, "disk_tick", AsyncMock(side_effect=OSError("no space")))
        with patch("core.disk_monitor.sample_disk_state",
                   return_value=_sample(pct=97.0, free=1.5)), \
             patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
             patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)):
            result = await BackgroundScheduler()._run_disk_watchdog()
        assert result["alert_fired"] is True, result
        assert result["db_24h_ago_mb"] is None

    @pytest.mark.asyncio
    async def test_the_oom_delta_reads_the_writers_previous_sample(
            self, flag, store, monkeypatch):
        """Mutation: read DuckDB's previous sample — under shadow a failed
        shadow leaves DuckDB a sample behind, and an OOM kill would be counted
        against the wrong one, or never."""
        from core.scheduler import BackgroundScheduler

        flag("postgres")
        async with store.connection() as conn:
            from core.memory_monitor import insert_sample

            insert_sample(conn, {**MEM, "oom_kills": 3})     # DuckDB's stale view

        async def memory_tick(mem, **kw):
            return {"last": {"oom_kills": 0}, "peak_24h": 600.0}

        monkeypatch.setattr(chain, "memory_tick", memory_tick)
        monkeypatch.setattr("core.memory_monitor.read_cgroup_memory",
                            lambda: {**MEM, "oom_kills": 1})
        judged = {}

        def evaluate_memory(**kw):
            judged.update(kw)
            return None

        # The evaluation's input is what this guards, so the evaluation itself
        # is replaced and the alert path is not reached.
        monkeypatch.setattr("core.memory_monitor.evaluate_memory", evaluate_memory)
        with patch("core.duckdb_store.get_store", AsyncMock(return_value=store)):
            result = await BackgroundScheduler()._run_memory_monitor()
        assert judged["previous_oom_kills"] == 0, judged       # Postgres's, not DuckDB's 3
        assert result["peak_working_set_mb_24h"] == 600


# ─── The standing watch ──────────────────────────────────────────────────────


class TestTheStandingWatch:
    NOW = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)

    def _judge(self, spans):
        from core import pg_chain_invariants as inv

        return inv.check_chain_invariants(inv.Facts(
            watched=(chain.CHAIN,), now=self.NOW, watchdogs=inv.Watchdogs(spans=spans)))

    def _healthy(self):
        return (("app.disk_samples", self.NOW - timedelta(hours=3), self.NOW - timedelta(days=13)),
                ("app.data_dir_samples", self.NOW - timedelta(hours=3), self.NOW - timedelta(days=20)),
                ("app.memory_samples", self.NOW - timedelta(minutes=20), self.NOW - timedelta(days=13)))

    def test_a_healthy_set_is_quiet(self):
        assert self._judge(self._healthy()) == []

    def test_a_watchdog_that_stopped_storing_is_stale(self):
        """Mutation: drop the staleness rule — with the copy and its
        comparison stood down, nothing else notices a watchdog that stopped."""
        from core import pg_chain_invariants as inv

        spans = list(self._healthy())
        spans[2] = ("app.memory_samples", self.NOW - timedelta(hours=2), spans[2][2])
        (issue,) = self._judge(tuple(spans))
        assert issue.check_name == inv.SAMPLES_STALE
        assert issue.table_name == "app.memory_samples"

    def test_an_empty_table_is_stale(self):
        spans = list(self._healthy())
        spans[0] = ("app.disk_samples", None, None)
        (issue,) = self._judge(tuple(spans))
        assert "holds no sample" in issue.description

    def test_a_prune_that_stopped_is_unbounded(self):
        """Mutation: drop the retention rule."""
        from core import pg_chain_invariants as inv

        spans = list(self._healthy())
        spans[1] = ("app.data_dir_samples", spans[1][1], self.NOW - timedelta(days=23))
        (issue,) = self._judge(tuple(spans))
        assert issue.check_name == inv.RETENTION_UNBOUNDED

    def test_the_limits_are_the_jobs_own(self):
        from core.disk_monitor import DIR_RETENTION_DAYS, DISK_RETENTION_DAYS
        from core.memory_monitor import MEMORY_RETENTION_DAYS

        assert chain.retention_days() == {
            "app.disk_samples": DISK_RETENTION_DAYS,
            "app.data_dir_samples": DIR_RETENTION_DAYS,
            "app.memory_samples": MEMORY_RETENTION_DAYS}
        assert set(chain.STALE_AFTER) == set(chain.CHAIN_TABLES)
