"""Chain 10: the watchdogs' samples, written to and read from Postgres alone.

The parts that need no database; the real-Postgres half is
`tests/integration/test_watchdog_samples_pg.py`. Until step 14's S14-10 the
samples had a DuckDB shadow and a `duckdb` branch behind `KS_WRITE_WATCHDOGS`;
their tests went with them. Every guard names, in its docstring, the mutation
it exists to catch.
"""
from __future__ import annotations

import ast
import pathlib
import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_watchdog_write as chain, watchdog_samples

ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def flag(monkeypatch):
    """No chain's flag. The flag routes nothing since S14-00; it is set only
    to show that."""
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)

    def set_(value):
        if value is None:
            monkeypatch.delenv(chain.WRITE_ENV, raising=False)
        else:
            monkeypatch.setenv(chain.WRITE_ENV, value)
    yield set_


@pytest.fixture
def no_duckdb(monkeypatch):
    """Any reach for DuckDB's store fails the test."""
    from core import duckdb_store

    def connection(self):
        raise AssertionError("the samples opened a DuckDB connection")

    monkeypatch.setattr(duckdb_store.DuckDBStore, "connection", connection)


def _sample(at=None, pct=40.0, free=100.0):
    return {"sampled_at": at or datetime.now(timezone.utc), "db_size_mb": 2_000.0,
            "disk_pct_used": pct, "disk_free_gb": free, "disk_used_bytes": 1}


MEM = {"working_set": 600 * 1024 * 1024, "page_cache": 100 * 1024 * 1024,
       "limit": 1024 * 1024 * 1024, "oom_kills": 0}


# ─── The one door ─────────────────────────────────────────────────────────────

# DuckDB's writers and readers of the samples. S14-10 deleted them; their
# names stay here so one coming back is caught.
_DOOR_FUNCS = {
    "insert_sample", "prune_old_samples", "insert_dir_samples",
    "prune_old_dir_samples", "fetch_sample_at_age", "fetch_dir_sample_at_age",
    "fetch_remainder_series", "fetch_last_sample", "fetch_peak_working_set_mb",
}
_DEFINERS = {"core/disk_monitor.py", "core/memory_monitor.py",
             "core/watchdog_samples.py", "core/pg_watchdog_write.py"}
# SQL over the three tables: their own modules, the router, and the stores'
# machinery (DDL, migrations, the hourly copy, its comparison, the standing
# watch; the copy-back went with step 14's S14-04).
_SQL_ALLOWED = _DEFINERS | {
    "core/migrations.py", "core/mirror_reconciliation.py",
    "core/pg_chain_invariants.py", "core/snapshot_validation.py",
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
        — the sample would land in DuckDB alone while the 24 h and 168 h reads
        ask Postgres, and the watchdog would report growth of zero for ever
        (the chain map's warning)."""
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

    def test_duckdbs_sample_statements_are_gone(self):
        """S14-10 deleted them, and the statements render for Postgres alone.
        Mutation: put `insert_sample(conn, ...)` back in `core/disk_monitor.py`,
        or the `duckdb` entry back in `SAMPLE_TABLES`."""
        from core import disk_monitor, memory_monitor

        for module in (disk_monitor, memory_monitor):
            assert not {n for n in _DOOR_FUNCS if hasattr(module, n)}, module.__name__
        assert set(disk_monitor.SAMPLE_TABLES) == {"postgres"}
        assert set(memory_monitor.MEMORY_TABLES) == {"postgres"}
        with pytest.raises(KeyError):
            disk_monitor.sample_sql(disk_monitor.DISK_INSERT_SQL, "duckdb")
        with pytest.raises(KeyError):
            memory_monitor.memory_sql(memory_monitor.MEMORY_INSERT_SQL, "duckdb")
        assert (disk_monitor.sample_sql(disk_monitor.DISK_INSERT_SQL)
                == "INSERT INTO app.disk_samples (sampled_at, db_size_mb, "
                   "disk_pct_used, disk_free_gb) VALUES ($1, $2, $3, $4)")
        assert (memory_monitor.memory_sql(memory_monitor.MEMORY_PRUNE_SQL)
                == "DELETE FROM app.memory_samples WHERE sampled_at < $1 "
                   "RETURNING sampled_at")


# ─── The tick: one Postgres transaction, nothing after it ─────────────────────


class TestTheTick:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [None, "duckdb", "postgres", "pgsql-typo"])
    async def test_one_set_of_instants_handed_to_postgres(
            self, flag, no_duckdb, monkeypatch, value):
        """The directory set's `sampled_at` and both cutoffs are computed once,
        from one `now`, whatever the flag says. Mutation: compute any of them
        twice, or route by the flag."""
        flag(value)
        seen = {}

        async def disk_tick(sample, dir_now, **kw):
            seen.update(kw, dir_now=dir_now)
            return {"history": None, "dir_week_ago": None, "dir_six_ago": None,
                    "remainder": [], "deleted": 0}

        monkeypatch.setattr(chain, "disk_tick", disk_tick)
        reads = await watchdog_samples.disk_tick(_sample(), {"duckdb": 7})
        assert reads["persisted"] is True
        assert seen["dir_now"] == {"duckdb": 7}
        assert seen["dir_sampled_at"] == seen["now"]
        assert seen["disk_cutoff"] == seen["now"] - timedelta(days=14)
        assert seen["dir_cutoff"] == seen["now"] - timedelta(days=21)

    @pytest.mark.asyncio
    async def test_the_memory_tick_samples_and_prunes_at_one_instant(
            self, flag, no_duckdb, monkeypatch):
        seen = {}

        async def memory_tick(mem, **kw):
            seen.update(kw)
            return {"last": None, "peak_24h": None}

        monkeypatch.setattr(chain, "memory_tick", memory_tick)
        reads = await watchdog_samples.memory_tick(MEM)
        assert reads == {"last": None, "peak_24h": None, "persisted": True}
        assert seen["sampled_at"] == seen["now"]
        assert seen["cutoff"] == seen["now"] - timedelta(days=14)

    @pytest.mark.asyncio
    async def test_a_refusing_postgres_returns_no_history(self, flag, no_duckdb,
                                                          monkeypatch):
        """Mutation: let the failure propagate — the disk job would raise
        before judging capacity, on the day the disk is full."""
        monkeypatch.setattr(chain, "disk_tick", AsyncMock(side_effect=OSError("no space")))
        monkeypatch.setattr(chain, "memory_tick", AsyncMock(side_effect=OSError("no space")))
        assert await watchdog_samples.disk_tick(_sample(), {"duckdb": 1}) == \
            watchdog_samples.EMPTY_DISK
        assert await watchdog_samples.memory_tick(MEM) == watchdog_samples.EMPTY_MEMORY

    @pytest.mark.asyncio
    async def test_the_capacity_page_goes_out_with_postgres_down(
            self, flag, no_duckdb, monkeypatch, tmp_path):
        """The job end to end: a full disk and a Postgres that refuses. The
        alert is the one that must still be sent. The store is the job's only
        for its file's path (the sample's `db_size_mb`)."""
        from core.scheduler import BackgroundScheduler

        monkeypatch.setattr(chain, "disk_tick", AsyncMock(side_effect=OSError("no space")))
        store = SimpleNamespace(db_path=tmp_path / "analytics.duckdb")
        with patch("core.disk_monitor.sample_disk_state",
                   return_value=_sample(pct=97.0, free=1.5)), \
             patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
             patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)):
            result = await BackgroundScheduler()._run_disk_watchdog()
        assert result["alert_fired"] is True, result
        assert result["db_24h_ago_mb"] is None

    @pytest.mark.asyncio
    async def test_the_oom_delta_reads_postgres_previous_sample(
            self, flag, no_duckdb, monkeypatch):
        """Mutation: read the previous sample anywhere else — an OOM kill
        would be counted against the wrong sample, or never. The memory job
        asks no store since S14-10."""
        from core.scheduler import BackgroundScheduler

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
        get_store = AsyncMock(side_effect=AssertionError("the memory job asked a store"))
        with patch("core.duckdb_store.get_store", get_store):
            result = await BackgroundScheduler()._run_memory_monitor()
        assert judged["previous_oom_kills"] == 0, judged
        assert result["peak_working_set_mb_24h"] == 600
        assert not get_store.await_count


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
