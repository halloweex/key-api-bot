"""The table-fate manifest, a frozen literal, held to the host check.

`core/duckdb_table_fates.py` declared what becomes of every table and view
the DuckDB file can hold. Until step 14's S14-18 nothing here trusted it: the
set of names was derived from the code — a fresh `DuckDBStore.connect()`, an
AST walk of every `CREATE TABLE` / `CREATE VIEW` / `RENAME TO` in DuckDB code
and the compaction's own `DERIVED_TABLES` — and each field was checked against
the lists that decided it: the compaction's exclusion, the real
`phase1_export`, the snapshot validator's tiers, the DuckDB→Postgres pairings
the shippers stated, the store switches' writers and `duckdb_written()`.

S14-18 deleted all of that: the store's schema and migrations, the
compaction, the snapshot and its validator, the store switches' readers and
`duckdb_written`. The code can put nothing into the file any more, so a walk
of it has nothing to find, and the manifest is what the file *is*: a literal
(plan, S14-18: "→ литерал против deploy/duckdb_table_fates_check.py на
фикстурном файле"). What is held here is that the literal is still the one
that was walked — OD-11's two sets pinned, every field consistent with the
rest — and that `deploy/duckdb_table_fates_check.py`, the one reader left,
judges a file built from it the way it judged production's backups: clean,
and every way a file can disagree reported.

OD-11 (owner, 01.10.2026): no DROP of any kind before the owner's week after
stage 4 completes. The two sets a DROP would have changed are pinned as
literals below, so the change that edits either says so in the diff.
"""
from __future__ import annotations

import asyncio
import ast
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

import duckdb
import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "deploy"))

import duckdb_table_fates_check as check  # noqa: E402

from core import duckdb_table_fates as fates_mod  # noqa: E402
from core.duckdb_table_fates import (  # noqa: E402
    ARCHIVE_ONLY, COMPUTED, DERIVED, EXPORTED, FATES, IRREPLACEABLE,
    NONE, NOT_A_TABLE, RETIRED, SCHEMA, SCRATCH, SKIPPED, TABLE, VIEW,
)

DSN = os.getenv("KS_PG_DSN", "").strip()
needs_pg = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

CHECK_SCRIPT = REPO / "deploy" / "duckdb_table_fates_check.py"

# ── OD-11: the two sets a DROP would change ─────────────────────────────────
# What the weekly compaction skipped (`DERIVED_TABLES`), and what had no DDL
# left. Both frozen as they stood when the compaction was deleted (S14-18).
PINNED_DERIVED_TABLES = frozenset({
    "silver_orders", "silver_order_utm", "gold_daily_revenue",
    "gold_product_pairs", "orders_v2", "gold_daily_traffic",
    "gold_daily_products", "bronze_order_events",
})
PINNED_NO_DDL = frozenset({
    "gold_product_pairs", "orders_v2", "gold_daily_traffic",
    "gold_daily_products", "bronze_order_events",
})

# The tables Postgres received above a watermark — a log of events DuckDB
# recorded as they happened, which no source re-issues — as the hourly copy
# named them (`pg_operational._APPEND_ABOVE`) until S14-13 deleted it.
APPENDED_ABOVE_A_WATERMARK = frozenset({"stock_movements", "inventory_sku_history"})

# The switches that were not a write chain's (`STORE_SWITCHES` until S14-18).
STORE_SWITCHES = frozenset({fates_mod.SMS_STORE, fates_mod.USER_STORE,
                            fates_mod.WAREHOUSE})


# ─── the fixture file ────────────────────────────────────────────────────────

def build_from_the_literal(path: Path) -> None:
    """A DuckDB file holding every name the literal says today's schema held,
    as its object type — what a backup of production held, up to columns,
    which the check does not read."""
    conn = duckdb.connect(str(path))
    try:
        for name, fate in sorted(FATES.items()):
            if fate.ddl != SCHEMA:
                continue
            if fate.object == VIEW:
                conn.execute(f'CREATE VIEW "{name}" AS SELECT 1 AS id')
            else:
                conn.execute(f'CREATE TABLE "{name}" (id INTEGER)')
    finally:
        conn.close()


@pytest.fixture(scope="module")
def fresh_file(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("fresh") / "fresh.duckdb"
    build_from_the_literal(path)
    return path


def _catalogue(path: Path) -> Dict[str, str]:
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return {name: kind for schema, name, kind in conn.execute(
            "SELECT table_schema, table_name, table_type "
            "FROM information_schema.tables "
            "WHERE table_catalog = current_database()").fetchall()
            if schema == "main"}
    finally:
        conn.close()


@pytest.fixture(scope="module")
def fresh(fresh_file) -> Dict[str, str]:
    return _catalogue(fresh_file)


# ─── 1. the literal ──────────────────────────────────────────────────────────

class TestTheLiteral:
    def test_it_is_a_literal_and_imports_nothing_of_ours(self):
        """The pinned image mounts this module beside the check and runs
        both there (`deploy/duckdb_table_fates_check.py`'s recipe), so it may
        import the standard library alone — and since S14-18 it holds no
        function that could ask the code anything. Mutation: put
        `duckdb_written` or `STORE_SWITCHES` back, or import a `core` module."""
        tree = ast.parse(Path(fates_mod.__file__).read_text(encoding="utf-8"))
        imported = {alias.name.split(".")[0] for node in ast.walk(tree)
                    if isinstance(node, ast.Import) for alias in node.names}
        imported |= {node.module.split(".")[0] for node in ast.walk(tree)
                     if isinstance(node, ast.ImportFrom) and node.module}
        assert imported <= {"__future__", "dataclasses", "typing"}, imported
        functions = sorted(n.name for n in tree.body
                           if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)))
        assert functions == ["_view"], functions
        for gone in ("duckdb_written", "STORE_SWITCHES", "tables_by_switch",
                     "_store_switch_writes_duckdb"):
            assert not hasattr(fates_mod, gone), gone

    def test_od11_nothing_joins_derived_tables(self):
        assert fates_mod.DROPS_ALLOWED is False
        skipped = frozenset(n for n, f in FATES.items() if f.compaction == SKIPPED)
        assert skipped == PINNED_DERIVED_TABLES, (
            "the set the compaction skipped changed. A name added there stops "
            "travelling off-site — a DROP, which OD-11 forbids.")

    def test_od11_nothing_leaves_the_schema(self):
        no_ddl = frozenset(n for n, f in FATES.items() if f.ddl == NONE)
        assert no_ddl == PINNED_NO_DDL, (
            "the set of tables with no DDL changed (OD-11).")

    def test_a_view_is_never_exported(self):
        for name, fate in FATES.items():
            if fate.object == VIEW:
                assert fate.compaction == NOT_A_TABLE, name
            else:
                assert fate.compaction in (EXPORTED, SKIPPED), name

    def test_a_skipped_table_in_the_schema_is_one_the_app_rebuilds(self):
        for name, fate in FATES.items():
            if fate.compaction == SKIPPED and fate.ddl == SCHEMA:
                assert fate.kind == DERIVED and fate.origin == COMPUTED, name

    def test_what_nothing_can_rebuild_travels_off_site(self):
        for name, fate in FATES.items():
            if fate.origin == IRREPLACEABLE or fate.kind == ARCHIVE_ONLY:
                assert fate.offsite, (
                    f"{name} has no source to be rebuilt from and is not in "
                    f"the off-site archive")

    def test_an_append_only_replica_is_irreplaceable(self):
        """Review mutation M14: stock_movements IRREPLACEABLE → KEYCRM."""
        loose = sorted(f"{n}: {FATES[n].origin}" for n in APPENDED_ABOVE_A_WATERMARK
                       if FATES[n].origin != IRREPLACEABLE)
        assert not loose, loose

    def test_a_table_without_ddl_is_retired_and_skipped(self):
        for name, fate in FATES.items():
            if fate.ddl == NONE:
                assert fate.kind == RETIRED and fate.compaction == SKIPPED, name

    def test_scratch_is_retired(self):
        for name, fate in FATES.items():
            if fate.ddl == SCRATCH:
                assert fate.kind == RETIRED and fate.object == TABLE, name

    def test_every_fate_but_retirement_names_where_the_rows_go(self):
        for name, fate in FATES.items():
            if fate.kind == RETIRED:
                assert not fate.successors, name
            else:
                assert fate.successors, f"{name}: {fate.kind} to where?"
            assert fate.reason.strip(), name

    def test_successors_are_spelled_as_places(self):
        for name, fate in FATES.items():
            for s in fate.successors:
                assert re.fullmatch(
                    r"(clickhouse:)?(bronze|silver|gold|app|meta|history)\.[a-z_0-9]+",
                    s), f"{name}: {s!r}"

    def test_clickhouse_successors_are_tables_the_shippers_write(self):
        from core import ch_gold, ch_history, ch_silver

        shipped = {v for m in (ch_gold, ch_history, ch_silver)
                   for k, v in vars(m).items()
                   if k.isupper() and k.endswith("TABLE") and isinstance(v, str)}
        for name, fate in FATES.items():
            for s in fate.successors:
                if s.startswith("clickhouse:"):
                    assert s[len("clickhouse:"):] in shipped, f"{name}: {s}"

    @needs_pg
    def test_every_postgres_successor_exists_in_the_migrated_schema(self):
        """The live schema, after `alembic upgrade head`: a successor that is
        not a relation there is a fate nobody can carry out."""
        import asyncpg

        async def relations():
            conn = await asyncpg.connect(DSN)
            try:
                rows = await conn.fetch(
                    "SELECT n.nspname || '.' || c.relname AS name "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relkind IN ('r', 'p', 'v', 'm')")
                return {r["name"] for r in rows}
            finally:
                await conn.close()

        present = asyncio.run(relations())
        missing = sorted(f"{n} → {s}" for n, f in FATES.items()
                         for s in f.successors
                         if not s.startswith("clickhouse:") and s not in present)
        assert not missing, f"no such relation in Postgres: {missing}"


# ─── 2. the write chains ─────────────────────────────────────────────────────

def _chain_switches():
    from core.write_chains import WRITE_CHAINS

    return {c.WRITE_ENV: c for c in WRITE_CHAINS}


class TestTheChains:
    """The chains' `WRITE_ENV` names stay, read by nothing (S14-11), because
    revisions 0034/0035 name them; the literal says which tables each moved."""

    def test_every_chain_table_carries_its_chains_switch(self):
        for env, chain in _chain_switches().items():
            for pg in chain.CHAIN_TABLES:
                holders = [n for n, f in FATES.items() if pg in f.successors]
                assert holders, f"{env} moved {pg}, and no DuckDB table names it"
                for name in holders:
                    assert FATES[name].switch == env, (name, pg, env)

    def test_a_chain_switch_is_claimed_only_by_that_chains_tables(self):
        chains = _chain_switches()
        for name, fate in FATES.items():
            if fate.switch in chains:
                owned = set(chains[fate.switch].CHAIN_TABLES)
                assert owned & set(fate.successors), (name, fate.switch)

    def test_one_chain_one_number(self):
        for env in _chain_switches():
            numbers = {f.chain for f in FATES.values() if f.switch == env}
            assert len(numbers) == 1 and None not in numbers, (env, numbers)

    def test_every_switch_is_a_chains_or_a_store_switch(self):
        known = set(_chain_switches()) | STORE_SWITCHES
        for name, fate in FATES.items():
            assert fate.switch is None or fate.switch in known, (name, fate.switch)


# ─── 3. the host check ───────────────────────────────────────────────────────

def _backup_name(hours_ago: float) -> str:
    """A name the backup job would have written `hours_ago` hours ago."""
    stamp = datetime.now(check.STAMP_TZ) - timedelta(hours=hours_ago)
    return f"analytics-{stamp:%Y%m%d-%H%M%S}.duckdb"


@pytest.fixture
def host(tmp_path, fresh_file):
    """A data directory as the host has it: a live file and two backups, the
    newer one from last night."""
    data = tmp_path / "data"
    (data / "backups").mkdir(parents=True)
    live = data / "analytics.duckdb"
    shutil.copyfile(fresh_file, live)
    shutil.copyfile(fresh_file, data / "backups" / _backup_name(25))
    shutil.copyfile(fresh_file, data / "backups" / _backup_name(1))
    return data


@pytest.fixture
def opened(monkeypatch):
    """Every `duckdb.connect` the check makes, with its arguments."""
    calls = []
    real = duckdb.connect

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", spy)
    return calls


def _run(data: Path, *extra: str) -> int:
    return check.main(["--backups", str(data / "backups"),
                       "--live", str(data / "analytics.duckdb"), *extra])


class TestTheHostCheck:
    def test_the_fixture_is_the_literal(self, fresh):
        """What the check is shown below is today's schema as the literal
        has it, every object of its own type."""
        expected = {n: f.object for n, f in FATES.items() if f.ddl == SCHEMA}
        assert fresh == expected

    def test_a_clean_copy_passes_read_only(self, host, opened, capsys):
        newest = max(p.name for p in (host / "backups").iterdir())
        assert _run(host) == check.EXIT_CLEAN
        out = capsys.readouterr().out
        assert newest in out and "clean" in out
        assert opened and all(kw.get("read_only") is True for _, kw in opened)

    # ── a file it can open is not yet a file it can judge ──

    def _only(self, host, build) -> Path:
        """Replace the newest backup with a file `build(conn)` makes."""
        newest = check.newest_backup(host / "backups")
        newest.unlink()
        conn = duckdb.connect(str(newest))
        try:
            build(conn)
        finally:
            conn.close()
        return newest

    def test_an_empty_file_is_refused_not_clean(self, host, capsys):
        """Zero unknown names in a file holding nothing read "clean", exit 0
        (review M-F1). Mutation: drop the `unseen` call from `main`."""
        self._only(host, lambda conn: None)
        assert _run(host) == check.EXIT_REFUSED
        captured = capsys.readouterr()
        assert "clean" not in captured.out
        assert "does not hold" in captured.err

    def test_another_database_is_refused(self, host, capsys):
        """A wrong `--file` — any database but the application's."""
        def build(conn):
            conn.execute("CREATE TABLE authorized_users (user_id BIGINT)")
            conn.execute("CREATE TABLE cache (key VARCHAR)")
        path = self._only(host, build)
        assert _run(host, "--file", str(path)) == check.EXIT_REFUSED
        assert "clean" not in capsys.readouterr().out

    def test_a_file_missing_one_anchor_is_refused(self, host, capsys):
        """Mutation: drop the anchor half of `unseen` — the rest of the schema
        is there, so only the anchors can refuse it."""
        newest = check.newest_backup(host / "backups")
        conn = duckdb.connect(str(newest))
        conn.execute("DROP TABLE schema_migrations")
        conn.close()
        assert _run(host) == check.EXIT_REFUSED
        assert "schema_migrations" in capsys.readouterr().err

    def test_a_file_holding_only_the_anchors_is_refused(self, host, capsys):
        """Mutation: drop the share half of `unseen`."""
        def build(conn):
            for name in check.ANCHORS:
                conn.execute(f'CREATE TABLE "{name}" (id INTEGER)')
        self._only(host, build)
        assert _run(host) == check.EXIT_REFUSED
        assert "tables today's schema creates" in capsys.readouterr().err

    def test_the_anchors_are_tables_every_file_holds(self, fresh):
        for name in check.ANCHORS:
            assert FATES[name].ddl == SCHEMA and FATES[name].object == TABLE, name
            assert fresh.get(name) == TABLE, name

    def test_a_stale_newest_backup_is_refused(self, host, fresh_file, opened, capsys):
        """A backup job that stopped a month ago answers last month's question.
        Mutation: drop the `too_old` call from `main`."""
        for old in (host / "backups").iterdir():
            old.unlink()
        stale = host / "backups" / _backup_name(24 * 30)
        shutil.copyfile(fresh_file, stale)
        assert _run(host) == check.EXIT_REFUSED
        assert "old" in capsys.readouterr().err
        assert not opened
        # Read on purpose, or with the bound moved, it is judged.
        assert _run(host, "--file", str(stale)) == check.EXIT_CLEAN
        assert _run(host, "--max-age-hours", str(24 * 31)) == check.EXIT_CLEAN

    def test_a_name_the_backup_job_never_writes_is_not_the_newest(self, host, fresh_file):
        newest = check.newest_backup(host / "backups")
        shutil.copyfile(fresh_file, host / "backups" / "analytics-zzz.duckdb")
        shutil.copyfile(fresh_file, host / "backups" / "analytics-99999999-999999.duckdb")
        assert check.newest_backup(host / "backups") == newest

    def test_the_stamp_is_read_in_the_application_timezone(self):
        from core.duckdb_constants import DB_PATH, DEFAULT_TZ

        assert check.STAMP_TZ.key == DEFAULT_TZ.key
        assert check.LIVE_DB.name == DB_PATH.name

    def test_a_mismatch_alone_asks_for_a_decision(self, host, capsys):
        """The one mismatch test also planted scratch, which kept the exit at 1
        on its own (review M7: `mismatch` dropped from the exit condition)."""
        newest = check.newest_backup(host / "backups")
        conn = duckdb.connect(str(newest))
        conn.execute("CREATE VIEW orders_v2 AS SELECT 1 AS id")
        conn.close()
        assert _run(host) == check.EXIT_DECIDE
        out = capsys.readouterr().out
        assert "MISMATCH: orders_v2" in out
        assert "SCRATCH" not in out and "UNKNOWN" not in out

    def test_an_unknown_table_is_reported(self, host, capsys):
        newest = check.newest_backup(host / "backups")
        conn = duckdb.connect(str(newest))
        conn.execute("CREATE TABLE mystery_ledger (id INTEGER)")
        conn.execute("CREATE SCHEMA side")
        conn.execute("CREATE TABLE side.orders (id INTEGER)")
        conn.close()
        assert _run(host) == check.EXIT_DECIDE
        out = capsys.readouterr().out
        assert "UNKNOWN: mystery_ledger" in out
        assert "UNKNOWN: side.orders" in out

    def test_a_retired_table_still_in_the_file_is_known(self, host, capsys):
        newest = check.newest_backup(host / "backups")
        conn = duckdb.connect(str(newest))
        for name in PINNED_NO_DDL:
            conn.execute(f'CREATE TABLE "{name}" (id INTEGER)')
        conn.close()
        assert _run(host) == check.EXIT_CLEAN
        assert "gold_daily_traffic" in capsys.readouterr().out

    def test_scratch_and_a_wrong_object_type_are_reported(self, host, capsys):
        newest = check.newest_backup(host / "backups")
        conn = duckdb.connect(str(newest))
        conn.execute("CREATE TABLE order_products_backup (id INTEGER)")
        conn.execute("CREATE VIEW orders_v2 AS SELECT 1 AS id")
        conn.close()
        assert _run(host) == check.EXIT_DECIDE
        out = capsys.readouterr().out
        assert "SCRATCH: order_products_backup" in out
        assert "MISMATCH: orders_v2" in out

    def test_the_live_file_is_refused_by_name(self, host, opened):
        assert _run(host, "--file", str(host / "analytics.duckdb")) == check.EXIT_REFUSED
        assert not opened

    def test_any_file_named_like_the_live_one_is_refused(self, host, opened):
        """Not the configured live path and not the same inode — a separate
        file under `/app/data` mounted somewhere `--live` does not point at is
        still somebody's live database, and the name is all that says so."""
        elsewhere = host / "elsewhere"
        elsewhere.mkdir()
        shutil.copyfile(host / "analytics.duckdb", elsewhere / "analytics.duckdb")
        assert _run(host, "--file", str(elsewhere / "analytics.duckdb")) \
            == check.EXIT_REFUSED
        assert not opened

    def test_the_live_file_is_refused_under_another_name(self, host, opened):
        link = host / "backups" / "copy.duckdb"
        os.link(host / "analytics.duckdb", link)
        assert _run(host, "--file", str(link)) == check.EXIT_REFUSED
        assert not opened

    def test_a_file_with_a_wal_is_refused(self, host, opened):
        newest = check.newest_backup(host / "backups")
        newest.with_name(newest.name + ".wal").write_bytes(b"")
        assert _run(host) == check.EXIT_REFUSED
        assert not opened

    def test_no_backup_is_refused(self, tmp_path, opened):
        (tmp_path / "backups").mkdir()
        assert _run(tmp_path) == check.EXIT_REFUSED
        assert not opened

    def test_it_runs_by_path_as_the_container_runs_it(self, host):
        """By path from another directory, the way `docker run … python
        /app/deploy/duckdb_table_fates_check.py` reaches it: its own imports,
        not the suite's sys.path."""
        env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
        done = subprocess.run(
            [sys.executable, str(CHECK_SCRIPT), "--backups", str(host / "backups"),
             "--live", str(host / "analytics.duckdb")],
            cwd=str(host), env=env, capture_output=True, text=True, timeout=120)
        assert done.returncode == check.EXIT_CLEAN, done.stdout + done.stderr
        assert "clean" in done.stdout
