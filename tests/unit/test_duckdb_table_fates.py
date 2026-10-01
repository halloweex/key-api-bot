"""The table-fate manifest, held to the code that decides each fate.

`core/duckdb_table_fates.py` is a declaration, and a declaration nobody checks
is the "21 methods" figure again. So nothing here trusts it: the set of names
is derived from the code — a fresh `DuckDBStore.connect()`, an AST walk of
every `CREATE TABLE` / `CREATE VIEW` / `RENAME TO` in DuckDB code, and the
compaction's own `DERIVED_TABLES` — and every field of every entry is checked
against the list that already decides it: the compaction's exclusion, the
real `phase1_export` (which *is* the off-site archive), the snapshot
validator's tiers, the DuckDB→Postgres pairings the shippers and comparisons
already state, the live Postgres schema, and the registered write chains.

OD-11 (owner, 01.10.2026): no DROP of any kind before the owner's week after
stage 4 completes — and removing a table from `_init_schema` or adding one to
`DERIVED_TABLES` is a DROP at the next Sunday compaction. Both sets are pinned
here as literals, so the change that does either says so in the diff.
"""
from __future__ import annotations

import ast
import asyncio
import dataclasses
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterator, List, Set, Tuple

import duckdb
import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO / "deploy"))

import compact_duckdb as compact  # noqa: E402
import duckdb_table_fates_check as check  # noqa: E402

from core import duckdb_table_fates as fates_mod  # noqa: E402
from core.duckdb_table_fates import (  # noqa: E402
    ARCHIVE_ONLY, COMPUTED, DERIVED, EXPORTED, FATES, IRREPLACEABLE, MOVED,
    NONE, NOT_A_TABLE, RETIRED, SCHEMA, SCRATCH, SKIPPED, STORE_SWITCHES,
    TABLE, VIEW,
)

DSN = os.getenv("KS_PG_DSN", "").strip()
needs_pg = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

CHECK_SCRIPT = REPO / "deploy" / "duckdb_table_fates_check.py"

# Where the DuckDB file's DDL can be written. `bot/` is not here because it
# never opens DuckDB — `test_the_bot_never_opens_duckdb` holds that — and its
# CREATE TABLEs are bot.db's.
DUCKDB_TREES = ("core", "web", "scripts", "deploy")

# DDL whose table name is only known at run time. Each says where the names
# come from. A site that is not listed fails, and so does a listed site that
# no longer exists — an exemption outliving its reason is how a walk rots.
RENDERED_DDL = {
    ("core/duckdb_store.py", "DuckDBStore._create_inventory_views"):
        "inventory_view_selects(DUCKDB) — every view it renders is in the "
        "fresh file, which the runtime walk reads",
    ("core/ch_gold.py", "_TABLE_DDL"): "ClickHouse, not DuckDB",
    ("core/ch_history.py", "_DDL"): "ClickHouse, not DuckDB",
    ("core/ch_silver.py", "_SILVER_DDL"): "ClickHouse, not DuckDB",
    ("deploy/ark_freeze.py", "dump_schema"):
        "writes the frozen file's own tables out as text; executes nothing",
}

# Switch names in the code that do not decide a DuckDB table's writer.
NOT_A_DUCKDB_SWITCH = {
    "KS_BOT_STORE": "decides where bot.db's rows live — SQLite, never DuckDB",
}

# ── OD-11: the two sets a DROP would change ─────────────────────────────────
# Removing a name from the schema, or adding one to DERIVED_TABLES, deletes
# data at the next Sunday compaction. Change these only with the owner's week
# after stage 4 behind you, and say so in the commit.
PINNED_DERIVED_TABLES = frozenset({
    "silver_orders", "silver_order_utm", "gold_daily_revenue",
    "gold_product_pairs", "orders_v2", "gold_daily_traffic",
    "gold_daily_products", "bronze_order_events",
})
PINNED_NO_DDL = frozenset({
    "gold_product_pairs", "orders_v2", "gold_daily_traffic",
    "gold_daily_products", "bronze_order_events",
})


# ─── the walks ───────────────────────────────────────────────────────────────

_DDL = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?P<temp>(?:TEMP|TEMPORARY)\s+)?"
    r"(?P<kind>TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>[\"\w.{}]+)",
    re.IGNORECASE,
)
_RENAME = re.compile(r"\bRENAME\s+TO\s+(?P<name>[\"\w.{}]+)", re.IGNORECASE)


def _py_files(trees=DUCKDB_TREES) -> Iterator[Path]:
    for tree in trees:
        for path in sorted((REPO / tree).rglob("*.py")):
            if "__pycache__" not in path.parts:
                yield path


def _docstrings(tree: ast.AST) -> Set[int]:
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            body = node.body
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                out.add(id(body[0].value))
    return out


def _scopes(tree: ast.AST) -> Dict[int, str]:
    """Each node's enclosing scope: a function's qualname, or the name a
    module-level assignment binds."""
    scope: Dict[int, str] = {}

    def visit(node, name):
        for child in ast.iter_child_nodes(node):
            inner = name
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                                  ast.ClassDef)):
                inner = f"{name}.{child.name}" if name else child.name
            elif (not name and isinstance(child, (ast.Assign, ast.AnnAssign))):
                targets = (child.targets if isinstance(child, ast.Assign)
                           else [child.target])
                inner = next((t.id for t in targets if isinstance(t, ast.Name)),
                             "<module>")
            scope[id(child)] = inner or "<module>"
            visit(child, inner)

    visit(tree, "")
    return scope


def _strings(tree: ast.AST) -> Iterator[Tuple[ast.AST, str]]:
    """Every string the module can execute: constants that are not docstrings
    and not pieces of an f-string, and f-strings with `{}` for each hole."""
    docs = _docstrings(tree)
    inside = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            for part in node.values:
                inside.add(id(part))
            yield node, "".join(
                p.value if isinstance(p, ast.Constant) else "{}"
                for p in node.values)
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in docs and id(node) not in inside):
            yield node, node.value


@dataclasses.dataclass(frozen=True)
class DdlSite:
    path: str
    scope: str
    kind: str          # TABLE | VIEW
    name: str          # unquoted; `{` when rendered at run time
    temp: bool


def walk_ddl(trees=DUCKDB_TREES) -> List[DdlSite]:
    sites = []
    for path in _py_files(trees):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        scopes = _scopes(tree)
        rel = path.relative_to(REPO).as_posix()
        for node, text in _strings(tree):
            found = [(m.group("kind").upper(), m.group("name"), bool(m.group("temp")))
                     for m in _DDL.finditer(text)]
            found += [("TABLE", m.group("name"), False) for m in _RENAME.finditer(text)]
            for kind, name, temp in found:
                sites.append(DdlSite(rel, scopes.get(id(node), "<module>"),
                                     kind, name.strip('"'), temp))
    return sites


def duckdb_names(sites: List[DdlSite]) -> Dict[str, str]:
    """`{name: TABLE|VIEW}` the walk resolves to the DuckDB file: not TEMP
    (DuckDB keeps those in its temp catalogue, never the file), not rendered,
    and either unqualified or in `main` — a dotted name is another engine's."""
    out = {}
    for s in sites:
        if s.temp or "{" in s.name:
            continue
        name = s.name[5:] if s.name.lower().startswith("main.") else s.name
        if "." in name:
            continue
        out[name] = TABLE if s.kind == "TABLE" else VIEW
    return out


@pytest.fixture(scope="module")
def fresh_file(tmp_path_factory) -> Path:
    """A database built by today's code, exactly as web builds one: the schema,
    every migration, every view. Closed cleanly, so copies of it are whole."""
    from core.duckdb_store import DuckDBStore

    path = tmp_path_factory.mktemp("fresh") / "fresh.duckdb"

    async def build():
        store = DuckDBStore(db_path=path)
        await store.connect()
        await store.close()

    asyncio.run(build())
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


@pytest.fixture(scope="module")
def sites() -> List[DdlSite]:
    return walk_ddl()


def _validator_tiers() -> Dict[str, Set[str]]:
    """Every table tier `core.snapshot_validation` declares, read off the
    module rather than named here: each upper-case module constant that is a
    set of strings, or a mapping keyed by them (`BOUNDED_SHRINK` arrived as a
    dict with chain 4, where a hand list of the frozensets would not look)."""
    from core import snapshot_validation as sv

    tiers = {}
    for name, value in vars(sv).items():
        if not name.isupper():
            continue
        if isinstance(value, (set, frozenset, dict)) and value and all(
                isinstance(k, str) for k in value):
            tiers[name] = set(value)
    return tiers


# ─── 1. the names ────────────────────────────────────────────────────────────

class TestEveryNameHasAFate:
    def test_the_walks_find_the_family(self, fresh, sites):
        """A walk that found nothing passes everything below. Production's
        anchors: the order headers, the Silver view, a migration's table, and a
        migration's scratch."""
        walked = duckdb_names(sites)
        assert {"orders", "silver_order_lines", "data_quality_runs"} <= set(fresh)
        assert {"orders", "silver_order_lines", "order_products_backup",
                "_offer_stocks_old", "schema_migrations"} <= set(walked)
        assert len(fresh) > 50

    def test_the_manifest_names_exactly_what_the_code_can_put_in_the_file(
            self, fresh, sites):
        derived = (set(fresh) | set(duckdb_names(sites))
                   | set(compact.DERIVED_TABLES))
        missing = sorted(derived - set(FATES))
        stale = sorted(set(FATES) - derived)
        assert not missing, (
            f"no fate declared for {missing} — add each to "
            f"core/duckdb_table_fates.py: which chain moves it and where, or "
            f"why nothing succeeds it")
        assert not stale, (
            f"{stale} are declared but nothing in the code creates or names "
            f"them any more")

    def test_ddl_says_which_walk_found_it(self, fresh, sites):
        walked = duckdb_names(sites)
        for name, fate in FATES.items():
            if name in fresh:
                expected = SCHEMA
            elif name in walked:
                expected = SCRATCH
            else:
                expected = NONE
            assert fate.ddl == expected, (
                f"{name}: ddl={fate.ddl!r}, but the code says {expected!r}"
                + (" — a table the schema no longer creates is a DROP at the "
                   "next Sunday compaction (OD-11)"
                   if fate.ddl == SCHEMA else ""))

    def test_the_object_type_is_the_files(self, fresh, sites):
        walked = duckdb_names(sites)
        for name, fate in FATES.items():
            actual = fresh.get(name) or walked.get(name) or TABLE
            assert fate.object == actual, f"{name}: declared {fate.object}, is {actual}"

    def test_every_rendered_ddl_site_is_accounted_for(self, sites):
        rendered = {(s.path, s.scope) for s in sites if "{" in s.name and not s.temp}
        unlisted = sorted(rendered - set(RENDERED_DDL))
        gone = sorted(set(RENDERED_DDL) - rendered)
        assert not unlisted, (
            f"DDL naming its table at run time at {unlisted}: the walk cannot "
            f"read the name, so say where it comes from in RENDERED_DDL")
        assert not gone, f"RENDERED_DDL lists sites that no longer exist: {gone}"

    def test_the_bot_never_opens_duckdb(self):
        """What lets `bot/` stay out of the walk."""
        offenders = []
        for path in _py_files(("bot",)):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    names = [node.module]
                if any(n == "duckdb" or n.startswith("core.duckdb_store")
                       for n in names):
                    offenders.append(path.relative_to(REPO).as_posix())
        assert not offenders, f"bot/ opens DuckDB in {offenders}; walk it too"

    def test_every_table_a_validator_names_has_a_fate(self):
        tiers = _validator_tiers()
        # The family: a walk that found none of them would pass everything.
        assert {"MUST_BE_NONEMPTY", "MONOTONE", "MAY_BE_EMPTY",
                "DERIVED"} <= set(tiers)
        stray = sorted(f"{tier}: {name}" for tier, names in tiers.items()
                       for name in names if name not in FATES)
        assert not stray, stray


# ─── 2. compaction and the off-site archive ─────────────────────────────────

class TestTheCompactionAgrees:
    def test_skipped_is_derived_tables(self):
        for name, fate in FATES.items():
            if fate.object == VIEW:
                assert fate.compaction == NOT_A_TABLE, name
            else:
                expected = SKIPPED if name in compact.DERIVED_TABLES else EXPORTED
                assert fate.compaction == expected, (
                    f"{name}: compaction={fate.compaction!r}, but "
                    f"scripts/compact_duckdb.py does {expected!r}")

    def test_od11_nothing_joins_derived_tables(self):
        assert fates_mod.DROPS_ALLOWED is False
        assert compact.DERIVED_TABLES == PINNED_DERIVED_TABLES, (
            "DERIVED_TABLES changed. A name added there stops travelling "
            "off-site and leaves the file at the next Sunday compaction — a "
            "DROP, which OD-11 forbids before the owner's week after stage 4. "
            "A name removed brings a retired table back on Sunday.")

    def test_od11_nothing_leaves_the_schema(self):
        no_ddl = frozenset(n for n, f in FATES.items() if f.ddl == NONE)
        assert no_ddl == PINNED_NO_DDL, (
            "the set of tables with no DDL changed. Removing a table from "
            "_init_schema drops it at the next Sunday compaction (OD-11).")

    def test_a_skipped_table_in_the_schema_is_one_the_app_rebuilds(self):
        for name, fate in FATES.items():
            if fate.compaction == SKIPPED and fate.ddl == SCHEMA:
                assert fate.kind == DERIVED and fate.origin == COMPUTED, (
                    f"{name} is skipped by the compaction, so it is rebuilt "
                    f"from nothing on Sunday and never travels off-site")

    def test_what_nothing_can_rebuild_travels_off_site(self):
        for name, fate in FATES.items():
            if fate.origin == IRREPLACEABLE or fate.kind == ARCHIVE_ONLY:
                assert fate.offsite, (
                    f"{name} has no source to be rebuilt from and is not in "
                    f"the off-site archive — chain 12's blast radius")

    def test_a_table_without_ddl_is_retired_and_skipped(self):
        for name, fate in FATES.items():
            if fate.ddl == NONE:
                assert fate.kind == RETIRED and fate.compaction == SKIPPED, (
                    f"{name}: with no DDL an export cannot be imported back — "
                    f"phase 2 would abort the compaction and the off-site "
                    f"export behind it")

    def test_the_snapshot_validator_agrees(self):
        """Every tier the nightly validator judges an export by — whatever it
        is called and however many there are — expects its tables in the
        export, except `DERIVED`, which names what the export leaves out."""
        offsite = {n for n, f in FATES.items() if f.offsite}
        skipped = {n for n, f in FATES.items() if f.compaction == SKIPPED}
        for tier, names in _validator_tiers().items():
            if tier == "DERIVED":
                assert names <= skipped, sorted(names - skipped)
                continue
            stray = sorted(names - offsite)
            assert not stray, f"snapshot_validation.{tier} expects {stray} off-site"

    def test_the_real_export_ships_exactly_the_offsite_tables(
            self, fresh_file, tmp_path, monkeypatch):
        """`phase1_export`'s manifest is the off-site archive's table list. Run
        it on today's schema plus every table today's schema no longer makes,
        and it must name exactly the tables the manifest marks off-site."""
        d = tmp_path / "data"
        d.mkdir()
        src = d / "analytics.duckdb"
        shutil.copyfile(fresh_file, src)
        conn = duckdb.connect(str(src))
        try:
            for name, fate in FATES.items():
                if fate.ddl != SCHEMA:
                    conn.execute(f'CREATE TABLE "{name}" (id INTEGER)')
                    conn.execute(f'INSERT INTO "{name}" VALUES (1)')
        finally:
            conn.close()
        monkeypatch.setattr(compact, "DATA_DIR", d)
        monkeypatch.setattr(compact, "SOURCE_DB", src)
        monkeypatch.setattr(compact, "EXPORT_DIR", d / "export_parquet")
        monkeypatch.setattr(compact, "MANIFEST_PATH",
                            d / "export_parquet" / "_manifest.json")

        manifest = compact.phase1_export()

        assert manifest["tables"] == sorted(
            n for n, f in FATES.items() if f.object == TABLE and f.offsite)
        assert set(manifest["counts"]) == {
            n for n, f in FATES.items() if f.object == TABLE}


# ─── 3. successors ───────────────────────────────────────────────────────────

def _from_table(source: str) -> str:
    """A shipper's DuckDB source is a table, or a subquery over one
    (`sync_metadata` ships without its transient keys)."""
    if re.fullmatch(r"[a-z_]+", source):
        return source
    match = re.search(r"\bFROM\s+([a-z_]+)", source, re.IGNORECASE)
    assert match, f"cannot read the table a shipper reads from: {source!r}"
    return match.group(1)


def _bot_db_tables() -> Set[str]:
    """bot.db's tables, from its own DDL. The bot-state comparison pairs them
    with Postgres in the same spec shape the DuckDB tables use."""
    return set(duckdb_names(walk_ddl(("bot",))))


def _stated_pairings() -> Set[Tuple[str, str]]:
    """Every (DuckDB table, Postgres table) the code already states."""
    from core import pg_operational
    from core.sql_dialect import DUCKDB, POSTGRES, Dialect, inventory_view_selects

    pairs = set()
    for field in dataclasses.fields(Dialect):
        dk, pg = getattr(DUCKDB, field.name), getattr(POSTGRES, field.name)
        if (isinstance(dk, str) and re.fullmatch(r"[a-z_]+", dk)
                and isinstance(pg, str) and re.fullmatch(r"[a-z_]+\.[a-z_]+", pg)):
            pairs.add((dk, pg))
    pairs.update(zip((n for n, _ in inventory_view_selects(DUCKDB)),
                     (n for n, _ in inventory_view_selects(POSTGRES))))
    pairs.update((_from_table(dk), pg) for pg, dk, *_ in pg_operational._FULL_REPLACE)
    pairs.update((a.dk_table, a.pg_table) for a in pg_operational._APPEND_ABOVE)
    for path in _py_files(("core",)):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue
            kw = {k.arg: k.value for k in node.keywords if k.arg}
            dk, pg = kw.get("dk_table"), kw.get("pg_table")
            if (isinstance(dk, ast.Constant) and isinstance(dk.value, str)
                    and isinstance(pg, ast.Constant) and isinstance(pg.value, str)):
                pairs.add((dk.value, pg.value))
    return pairs


class TestSuccessors:
    def test_the_pairings_walk_finds_the_family(self):
        pairs = _stated_pairings()
        assert ("orders", "bronze.orders") in pairs
        assert ("stock_movements", "app.stock_movements") in pairs
        assert ("v_sku_analysis", "gold.v_sku_analysis") in pairs
        assert len(pairs) > 30

    def test_every_stated_pairing_is_a_declared_successor(self):
        bot_db = _bot_db_tables()
        assert {"authorized_users", "user_preferences"} <= bot_db
        wrong = sorted(f"{dk} → {pg}" for dk, pg in _stated_pairings()
                       if not (dk in bot_db and dk not in FATES)
                       and (dk not in FATES or pg not in FATES[dk].successors))
        assert not wrong, (
            f"the code pairs these, the manifest does not: {wrong}")

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


# ─── 4. the write chains ─────────────────────────────────────────────────────

def _chain_switches() -> Dict[str, object]:
    from core.write_chains import WRITE_CHAINS

    return {c.WRITE_ENV: c for c in WRITE_CHAINS}


class TestTheChains:
    def test_every_chain_table_carries_its_chains_switch(self):
        for env, chain in _chain_switches().items():
            for pg in chain.CHAIN_TABLES:
                holders = [n for n, f in FATES.items() if pg in f.successors]
                assert holders, (
                    f"{env} moves {pg}, and no DuckDB table names it as a "
                    f"successor")
                for name in holders:
                    assert FATES[name].switch == env, (
                        f"{name} → {pg} is moved by {env}; declare "
                        f"switch={env!r} so duckdb_written() can see it")

    def test_a_chain_switch_is_claimed_only_by_that_chains_tables(self):
        chains = _chain_switches()
        for name, fate in FATES.items():
            if fate.switch in chains:
                owned = set(chains[fate.switch].CHAIN_TABLES)
                assert owned & set(fate.successors), (
                    f"{name} says {fate.switch} moves it, but that chain's "
                    f"CHAIN_TABLES name none of {fate.successors}")

    def test_one_chain_one_number(self):
        for env in _chain_switches():
            numbers = {f.chain for f in FATES.values() if f.switch == env}
            assert len(numbers) == 1 and None not in numbers, (env, numbers)

    def test_every_switch_is_one_somebody_reads(self):
        known = set(_chain_switches()) | set(STORE_SWITCHES)
        for name, fate in FATES.items():
            assert fate.switch is None or fate.switch in known, (name, fate.switch)

    def test_every_switch_in_the_code_is_known_here(self):
        """A `KS_WRITE_*` or `KS_*_STORE` the manifest has never heard of is a
        writer moving without this file saying which tables went with it."""
        pattern = re.compile(r"\bKS_(?:WRITE_[A-Z_]+|[A-Z_]+_STORE)\b")
        known = (set(_chain_switches()) | set(STORE_SWITCHES)
                 | set(NOT_A_DUCKDB_SWITCH))
        seen: Dict[str, str] = {}
        for path in _py_files(DUCKDB_TREES + ("bot",)):
            for _, text in _strings(ast.parse(path.read_text(encoding="utf-8"))):
                for m in pattern.finditer(text):
                    seen.setdefault(m.group(0), path.relative_to(REPO).as_posix())
        unknown = sorted(f"{k} ({v})" for k, v in seen.items() if k not in known)
        assert not unknown, unknown
        assert set(STORE_SWITCHES) <= set(seen), "a store switch nobody reads"

    def test_every_store_switch_has_a_reader(self, monkeypatch):
        for switch in STORE_SWITCHES:
            assert fates_mod._store_switch_writes_duckdb(switch) is True, switch


# ─── 5. "which tables are still DuckDB-written today" ──────────────────────

class TestDuckdbWritten:
    def test_it_answers_for_every_entry(self):
        assert set(fates_mod.duckdb_written()) == set(FATES)

    def test_with_every_switch_off(self):
        written = fates_mod.duckdb_written()
        for name, fate in FATES.items():
            if fate.object == VIEW or fate.kind == RETIRED:
                assert written[name] is False, name
            elif fate.kind == ARCHIVE_ONLY and fate.switch is None:
                assert written[name] is False, name
            else:
                assert written[name] is True, name

    @pytest.mark.parametrize("switch", ["KS_SMS_STORE", "KS_USER_STORE"])
    def test_a_store_switch_moves_its_tables_and_no_others(self, switch, monkeypatch):
        before = fates_mod.duckdb_written()
        monkeypatch.setenv(switch, "postgres")
        after = fates_mod.duckdb_written()
        moved = set(fates_mod.tables_by_switch(switch))
        assert moved
        for name in FATES:
            assert after[name] == (False if name in moved else before[name]), name
        monkeypatch.setenv(switch, "postgress")
        unread = fates_mod.duckdb_written()
        assert {unread[n] for n in moved} == {None}

    def test_the_warehouse_switch(self, monkeypatch):
        from core import warehouse_cutover as wc

        monkeypatch.setattr(wc, "_mode", wc.POSTGRES)
        written = fates_mod.duckdb_written()
        moved = set(fates_mod.tables_by_switch(fates_mod.WAREHOUSE))
        assert {"silver_orders", "gold_daily_revenue", "warehouse_refreshes"} <= moved
        assert {written[n] for n in moved} == {False}

    def test_a_latched_chain_moves_its_tables(self, monkeypatch):
        from core import chain_latch
        from core.write_chains import chain_name

        for env, chain in _chain_switches().items():
            chain_latch.latch(chain_name(chain), env)
            written = fates_mod.duckdb_written()
            for name in fates_mod.tables_by_switch(env):
                assert written[name] is False, (env, name)

    def test_a_chain_flag_nobody_can_read_is_unknown(self, monkeypatch):
        for env in _chain_switches():
            monkeypatch.setenv(env, "postgress")
            written = fates_mod.duckdb_written()
            assert {written[n] for n in fates_mod.tables_by_switch(env)} == {None}, env
            monkeypatch.delenv(env)


# ─── 6. the host check ───────────────────────────────────────────────────────

@pytest.fixture
def host(tmp_path, fresh_file):
    """A data directory as the host has it: a live file and two backups."""
    data = tmp_path / "data"
    (data / "backups").mkdir(parents=True)
    live = data / "analytics.duckdb"
    shutil.copyfile(fresh_file, live)
    older = data / "backups" / "analytics-20260929-013000.duckdb"
    newer = data / "backups" / "analytics-20260930-013000.duckdb"
    shutil.copyfile(fresh_file, older)
    shutil.copyfile(fresh_file, newer)
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
    def test_a_clean_copy_passes_read_only(self, host, opened, capsys):
        assert _run(host) == check.EXIT_CLEAN
        out = capsys.readouterr().out
        assert "analytics-20260930-013000.duckdb" in out  # the newest
        assert opened and all(kw.get("read_only") is True for _, kw in opened)

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
