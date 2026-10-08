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
import contextlib
import dataclasses
import importlib
import inspect
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Set, Tuple

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

# Where the DuckDB file's DDL can be written. Nothing outside these opens
# DuckDB — `test_nothing_outside_the_walked_trees_opens_duckdb` walks every
# other Python file in the repository for a way in — so `bot/`'s CREATE TABLEs
# are bot.db's and `migrations/` is Postgres's.
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
    ("scripts/goals_semantics_dryrun.py", "copy_backup"):
        "copies the tables its TABLES names out of a backup ATTACHed "
        "READ_ONLY into an in-memory database (`:memory:`) — never the file",
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

_IDENT = r'(?:"[^"]+"|[A-Za-z_]\w*)'
_NAME = rf"{_IDENT}(?:\s*\.\s*{_IDENT})*"
_DDL = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?P<temp>(?:TEMP|TEMPORARY)\s+)?"
    r"(?P<kind>TABLE|VIEW)\b(?:\s+IF\s+NOT\s+EXISTS\b)?",
    re.IGNORECASE,
)
_RENAME = re.compile(r"\bRENAME\s+TO\b", re.IGNORECASE)
# The name after a DDL keyword, when the walk can read it: an identifier,
# qualified or not, ending where SQL ends one. Anything else — `%s`, a `{}`
# hole, a string that stops there because a variable is added to it — is a
# name known only at run time, and is recorded as `UNREAD`, never skipped.
_READABLE = re.compile(rf"\s+(?P<name>{_NAME})(?=[\s(;,)]|$)")
UNREAD = "{}"
# What an operand the walk cannot read becomes in the text it joins.
HOLE = "{}"
# DuckDB's relational API persists what these name — checked on 1.5.5:
# `create`/`to_table` make a table in the file, `create_view`/`to_view` a view.
_RELATIONAL = {"create": "TABLE", "to_table": "TABLE",
               "create_view": "VIEW", "to_view": "VIEW"}
_RELATIONAL_NAME_KW = ("table_name", "view_name")


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


def _is_text(node: ast.AST) -> bool:
    return (isinstance(node, ast.JoinedStr)
            or (isinstance(node, ast.Constant) and isinstance(node.value, str)))


def _add_chain(node: ast.AST, links: List[ast.AST]) -> List[ast.AST]:
    """`a + b + c`, flattened — Python nests it as `(a + b) + c`. `links`
    collects the `+` nodes themselves."""
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        links.append(node)
        return _add_chain(node.left, links) + _add_chain(node.right, links)
    return [node]


def _joined(node: ast.AST) -> str:
    """The text an f-string or a constant contributes, `HOLE` for the rest."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(p.value if isinstance(p, ast.Constant) else HOLE
                       for p in node.values)
    return HOLE


def _strings(tree: ast.AST) -> Iterator[Tuple[ast.AST, str]]:
    """Every string the module can execute: constants that are not docstrings,
    f-strings with `HOLE` for each hole, and a concatenation with `+` as one
    text — `"CREATE TABLE " + name` is a statement whose name is a hole, not a
    statement with no name. A piece of either is never read on its own."""
    docs = _docstrings(tree)
    inside: Set[int] = set()
    for node in ast.walk(tree):
        if id(node) in inside:
            continue
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            links: List[ast.AST] = []
            operands = _add_chain(node, links)
            if not any(_is_text(o) for o in operands):
                continue
            inside.update(id(n) for n in links)
            for o in operands:
                if _is_text(o):
                    inside.add(id(o))
                if isinstance(o, ast.JoinedStr):
                    inside.update(id(p) for p in o.values)
            yield node, "".join(_joined(o) for o in operands)
        elif isinstance(node, ast.JoinedStr):
            inside.update(id(part) for part in node.values)
            yield node, _joined(node)
        elif (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in docs):
            yield node, node.value


def _unquote(name: str) -> str:
    return ".".join(p.strip().strip('"') for p in re.split(r"\s*\.\s*", name))


def _named(text: str, at: int) -> str:
    """The name that follows position `at`, or `UNREAD`."""
    m = _READABLE.match(text, at)
    return _unquote(m.group("name")) if m else UNREAD


@dataclasses.dataclass(frozen=True)
class DdlSite:
    path: str
    scope: str
    kind: str          # TABLE | VIEW
    name: str          # unquoted, dotted parts joined; `UNREAD` at run time
    temp: bool


def _catalog() -> str:
    """The DuckDB file's own catalog name: the stem of the file web opens."""
    from core.duckdb_constants import DB_PATH

    return DB_PATH.stem


def place(name: str) -> "str | None":
    """The table in the DuckDB file a DDL name creates, or None when the walk
    cannot place it there: `main.x`, `<catalog>.x` and `<catalog>.main.x` are
    the file's own `x`; any other dotted name belongs to another catalog or
    another engine, and is listed like a rendered one rather than dropped."""
    if "{" in name:
        return None
    parts = name.split(".")
    catalog = _catalog().lower()
    lowered = [p.lower() for p in parts]
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2 and lowered[0] in ("main", catalog):
        return parts[1]
    if len(parts) == 3 and lowered[0] == catalog and lowered[1] == "main":
        return parts[2]
    return None


def walk_source(rel: str, source: str) -> List[DdlSite]:
    """Every DDL site in one module: SQL text, and DuckDB's relational API."""
    tree = ast.parse(source)
    scopes = _scopes(tree)
    sites = []
    for node, text in _strings(tree):
        found = [(m.group("kind").upper(), _named(text, m.end()), bool(m.group("temp")))
                 for m in _DDL.finditer(text)]
        found += [("TABLE", _named(text, m.end()), False) for m in _RENAME.finditer(text)]
        for kind, name, temp in found:
            sites.append(DdlSite(rel, scopes.get(id(node), "<module>"), kind, name, temp))
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in _RELATIONAL):
            continue
        arg = node.args[0] if node.args else next(
            (k.value for k in node.keywords if k.arg in _RELATIONAL_NAME_KW), None)
        if arg is None:
            continue  # `client.messages.create(**kwargs)` names no table
        name = (arg.value if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
                else UNREAD)
        sites.append(DdlSite(rel, scopes.get(id(node), "<module>"),
                             _RELATIONAL[node.func.attr], name, False))
    return sites


def walk_ddl(trees=DUCKDB_TREES) -> List[DdlSite]:
    sites = []
    for path in _py_files(trees):
        sites += walk_source(path.relative_to(REPO).as_posix(),
                             path.read_text(encoding="utf-8"))
    return sites


def needs_listing(site: DdlSite) -> bool:
    """A site whose table the walk cannot name in the file: rendered at run
    time, or qualified with a catalog or schema that is not the file's."""
    return not site.temp and place(site.name) is None


def duckdb_names(sites: List[DdlSite]) -> Dict[str, str]:
    """`{name: TABLE|VIEW}` the walk resolves to the DuckDB file: not TEMP
    (DuckDB keeps those in its temp catalogue, never the file), and placed —
    every other site is in `RENDERED_DDL` or fails."""
    out = {}
    for s in sites:
        name = None if s.temp else place(s.name)
        if name is not None:
            out[name] = TABLE if s.kind == "TABLE" else VIEW
    return out


# ── who can open DuckDB at all ──

# The application's one opener — `core.duckdb_switch.open_file` since the
# stage-4 integration made the kill guard's and the week of silence's one
# function — read from the guard's walk rather than spelled here: this walk
# predated it, knew only `duckdb` and the store, and passed a `bot/` module
# that opened the file through `open_file` and created a table in it, which
# the DDL walk then never read (batch-E review).
from tests.unit.test_duckdb_open_guard import OPENER as _GUARD_OPENER  # noqa: E402

_OPENER_MODULE = _GUARD_OPENER[0][:-len(".py")].replace("/", ".")
_OPENER_MODULES = ("duckdb", "core.duckdb_store", _OPENER_MODULE)
_OPENER_NAMES = frozenset({"DuckDBStore", "get_store", _GUARD_OPENER[1]})


def _names_opener(module: str) -> bool:
    return any(module == m or module.startswith(m + ".") for m in _OPENER_MODULES)


def duckdb_openers(rel: str, source: str) -> List[str]:
    """Every way one module can reach DuckDB, as `line: what`.

    Any import of `duckdb`, `core.duckdb_store` or the opener's module,
    `core.duckdb_switch` — `from core import duckdb_store` included, which
    the first walk missed by reading only the module of an ImportFrom and
    never its names — a relative import resolved against the file's package,
    `importlib.import_module`/`__import__` of any of them, and of a name
    computed at run time (it could be any), and the three doors —
    `DuckDBStore`, `get_store` and `open_file` — by name wherever they were
    imported from."""
    tree = ast.parse(source)
    package = rel.split("/")[:-1]
    found = []
    for node in ast.walk(tree):
        line = getattr(node, "lineno", 0)
        if isinstance(node, ast.Import):
            found += [f"{line}: import {a.name}" for a in node.names
                      if _names_opener(a.name)]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                parts = package[:len(package) - node.level + 1]
                base = ".".join(parts + ([node.module] if node.module else []))
            else:
                base = node.module or ""
            for a in node.names:
                full = f"{base}.{a.name}" if base else a.name
                if _names_opener(base) or _names_opener(full) or a.name in _OPENER_NAMES:
                    found.append(f"{line}: from {base or '.'} import {a.name}")
        elif isinstance(node, ast.Call):
            fn = node.func
            called = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", None)
            if called in ("import_module", "__import__"):
                arg = node.args[0] if node.args else None
                if not (isinstance(arg, ast.Constant) and isinstance(arg.value, str)):
                    found.append(f"{line}: {called}() of a name computed at run time")
                elif _names_opener(arg.value):
                    found.append(f"{line}: {called}({arg.value!r})")
        elif isinstance(node, ast.Name) and node.id in _OPENER_NAMES:
            found.append(f"{line}: {node.id}")
        elif (isinstance(node, ast.Attribute)
                and node.attr in _OPENER_NAMES | {m.rsplit(".", 1)[-1]
                                                  for m in _OPENER_MODULES[1:]}):
            found.append(f"{line}: .{node.attr}")
    return found


def openers_outside(files: List[Tuple[str, str]]) -> Dict[str, List[str]]:
    """`{rel: openers}` for each `(rel, source)` outside `DUCKDB_TREES` that
    reaches DuckDB."""
    out = {}
    for rel, source in files:
        if rel.split("/")[0] in DUCKDB_TREES:
            continue
        found = duckdb_openers(rel, source)
        if found:
            out[rel] = found
    return out


def _gitignored_dirs() -> Set[str]:
    """Directory names `.gitignore` excludes outright (`data/`, `venv/`)."""
    names = {"__pycache__", "node_modules"}
    for line in (REPO / ".gitignore").read_text(encoding="utf-8").splitlines():
        m = re.fullmatch(r"/?([\w.-]+)/?", line.strip())
        if m:
            names.add(m.group(1))
    return names


def repo_python() -> List[Tuple[str, Path]]:
    """Every Python file of the repository outside `tests/`: tracked, or new
    and not ignored — a fresh module or a probe is exactly what this is for.
    From `git ls-files` where git answers; without it (the py3.14 container
    has none), every `.py` under no hidden directory and no directory
    `.gitignore` names, which keeps out the untracked `data/` scripts."""
    try:
        out = subprocess.run(
            ["git", "-C", str(REPO), "ls-files", "-z", "--cached", "--others",
             "--exclude-standard", "--", "*.py"],
            capture_output=True, text=True, timeout=60, check=True).stdout
        rels = [r for r in out.split("\0") if r]
    except (OSError, subprocess.SubprocessError):
        ignored = _gitignored_dirs()
        rels = [p.relative_to(REPO).as_posix() for p in REPO.rglob("*.py")
                if not any(part.startswith(".") or part in ignored
                           for part in p.relative_to(REPO).parts[:-1])]
    return sorted((r, REPO / r) for r in set(rels)
                  if not r.startswith("tests/") and (REPO / r).is_file())


# ── who writes each table, and behind which switch ──
#
# A store switch moves the tables whose every DuckDB writer stops when it
# flips. That is read off the code, never declared: the statements each
# switch's routers run (rendered by the router itself, for DuckDB), every
# literal DuckDB write, and for each writer whether it runs only on the branch
# where the switch's reader says DuckDB — under `if duckdb_derives():`, after
# `if not duckdb_derives(): return`, or reached only from such a branch. A
# function that merely calls the reader somewhere is not guarded: `full_sync`
# asks `duckdb_derives()` about the warehouse and writes `categories` whatever
# it says, and reading "asks anywhere" claimed eight such tables.
#
# Limits, named: a write whose target is rendered at run time (`f"DELETE FROM
# {table}"` — the Postgres and ClickHouse shippers, the compaction's import,
# the copy-back) names no table and is not counted; a `{name}` template, one
# text for two engines, is rendered for DuckDB by its module's own function
# or says what fills its hole (`TEMPLATED_ELSEWHERE`); a guard spelled other
# than a call of the reader, `not` of one, `and`/`or` of those, or an early
# `return`/`raise`/`continue` is not seen, which errs toward "unguarded".

# The SQL keywords that write a table. Upper case only: the repository's SQL
# is, and "update orders" in a log line is not a statement.
_WRITE = re.compile(
    r"\b(?P<kw>INSERT\s+(?:OR\s+(?:REPLACE|IGNORE)\s+)?INTO|UPDATE|DELETE\s+FROM"
    r"|TRUNCATE(?:\s+TABLE)?|MERGE\s+INTO|COPY)(?=\s)")
# Words that follow a write keyword without being its table:
# `DO UPDATE SET`, `FOR UPDATE OF`, `... UPDATE RETURNING`.
_NOT_A_TABLE = frozenset({"set", "of", "on", "returning", "where", "from",
                          "select", "values", "default", "nowait", "skip"})

# Each store switch: the reader that decides it, whether the reader's True
# means DuckDB writes, and the routers — functions that pick the engine by the
# reader and run a statement handed to them. Held to the code below.
STORE_ROUTERS: Dict[str, Tuple[str, bool, Tuple[str, ...]]] = {
    fates_mod.SMS_STORE: ("sms_store_is_postgres", False, ("_sms_run", "_sms_tx")),
    fates_mod.USER_STORE: ("user_store_is_postgres", False, ("_users_run", "_perms_run")),
    fates_mod.WAREHOUSE: ("duckdb_derives", True, ()),
}
# `_sms_tx` yields a transaction; these are the methods that run a statement.
_TX_METHODS = ("all", "one", "none", "many")

# DuckDB writes that never reach the analytics file. Each says why; a listed
# site that no longer exists fails, as in RENDERED_DDL.
NOT_THE_FILE = {
    ("deploy/measure_dlr_throughput.py", "seed_duckdb"):
        "a benchmark's own DuckDB, created in a temp directory",
    ("deploy/measure_dlr_throughput.py", "main"):
        "a benchmark's own DuckDB, created in a temp directory",
}

# Write statements that name their table as a `{hole}` the module fills per
# engine — one text for DuckDB and Postgres. The literal pass reads a name
# only where it can see one, so each is rendered for DuckDB by the module's
# own function, as a router's statement is, and read like a literal: nothing
# restated here. Chain 10 wrote the watchdogs' statements so, and every DuckDB
# write of the three sample tables went dark to the walk until this read them.
#
# `{module: renderer}` — `renderer(text)` renders any of the module's texts
# for DuckDB, its default engine.
HOLE_RENDERERS = {
    "core/disk_monitor.py": "sample_sql",
    "core/memory_monitor.py": "memory_sql",
}
# `{(module, constant): renderer}` — a `Dialect` body, rendered by the one
# function that renders it, as `renderer(sql_dialect.DUCKDB)`.
DIALECT_BODIES = {
    ("core/sql_dialect.py", "_SKU_STATUS_REBUILD_BODY"): "sku_status_rebuild_select",
    ("core/sql_dialect.py", "_MANAGER_STATS_BODY"): "manager_stats_sql",
}
# Templated writes nothing above renders, each with what fills its hole. One
# that is in none of the three fails, and so does a listed one that no longer
# exists: the next writer of one text for two engines is told, not lost.
TEMPLATED_ELSEWHERE = {
    ("core/dashboard_access.py", "GRANT_ACCESS"):
        "handed to `_users_run`, which renders it; the router pass reads it there",
    ("core/dashboard_access.py", "SET_FEATURES"):
        "handed to `_users_run`, which renders it; the router pass reads it there",
    ("core/pg_landing.py", "_UPSERT"): "Postgres: the landing mirror's tables",
    ("core/report_ledger.py", "UPSERT_SQL"):
        "Postgres: the two send ledgers' chain writers (`$n` placeholders); "
        "DuckDB's half is each report's own literal",
    ("deploy/step13_rehearsal/probe.py", "_DELETE_ONE"):
        "the step-13 rehearsal's copy of the file, one `python -c` per table",
}
# A write keyword whose table is a named `{hole}`. An f-string's own holes
# read as `HOLE`, which has no name; `{{name}}` in one reads as `{name}`.
_TEMPLATED_WRITE = re.compile(_WRITE.pattern + r"\s+\{\w+\}")


def _render_template(module: "_Module", owner: Tuple[str, str],
                     text: str) -> "str | None":
    """The DuckDB text of a templated write, by the module's own renderer, or
    None when nothing listed renders it."""
    if module.rel in HOLE_RENDERERS:
        mod = importlib.import_module(_module_name(module.rel))
        return getattr(mod, HOLE_RENDERERS[module.rel])(text)
    kind, name = owner
    renderer = DIALECT_BODIES.get((module.rel, name)) if kind == "const" else None
    if renderer is None:
        return None
    from core.sql_dialect import DUCKDB

    mod = importlib.import_module(_module_name(module.rel))
    return getattr(mod, renderer)(DUCKDB)


@dataclasses.dataclass(frozen=True)
class WriteSite:
    path: str
    owner: str                 # the function's qualname
    table: str
    router: "str | None"       # the router the statement went through
    node: ast.AST = dataclasses.field(compare=False, hash=False, repr=False)


def _write_targets(text: str) -> List[str]:
    """The DuckDB tables a SQL text writes. A target the walk cannot read, or
    another engine's qualified name, is not one."""
    out = []
    for m in _WRITE.finditer(text):
        name = _named(text, m.end())
        if name == UNREAD or name.lower() in _NOT_A_TABLE:
            continue
        if m.group("kw") == "COPY" and not re.match(
                rf"\s+{_NAME}\s+FROM\b", text[m.end():]):
            continue  # `COPY t TO` exports; only `COPY t FROM` loads
        placed = place(name)
        if placed is not None:
            out.append(placed)
    return out


def _owners(tree: ast.AST) -> Dict[int, Tuple[str, str]]:
    """Each node's owner: `("fn", qualname)` inside a function, `("const",
    name)` in a module- or class-level assignment, else `("module", "")`."""
    out: Dict[int, Tuple[str, str]] = {}

    def visit(node, path, in_fn, const):
        for child in ast.iter_child_nodes(node):
            c_path, c_in_fn, c_const = path, in_fn, const
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                c_path, c_in_fn, c_const = path + [child.name], True, None
            elif isinstance(child, ast.ClassDef):
                c_path = path + [child.name]
            elif not in_fn and isinstance(child, (ast.Assign, ast.AnnAssign)):
                targets = child.targets if isinstance(child, ast.Assign) else [child.target]
                c_const = next((t.id for t in targets if isinstance(t, ast.Name)), None)
            out[id(child)] = (("fn", ".".join(c_path)) if c_in_fn
                              else ("const", c_const) if c_const else ("module", ""))
            visit(child, c_path, c_in_fn, c_const)

    visit(tree, [], False, None)
    return out


def _own_nodes(fn: ast.AST) -> Iterator[ast.AST]:
    """A function's body, without the bodies of functions defined in it."""
    stack = list(ast.iter_child_nodes(fn))
    while stack:
        node = stack.pop()
        yield node
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            stack.extend(ast.iter_child_nodes(node))


@dataclasses.dataclass
class _Fn:
    path: str
    qualname: str
    node: ast.AST
    # Every name the function reads, bare or as an attribute, with the nodes
    # that read it — a call, or a function handed on as a callback.
    refs: Dict[str, List[ast.AST]]


@dataclasses.dataclass
class _Module:
    rel: str
    tree: ast.AST
    owners: Dict[int, Tuple[str, str]]
    parents: Dict[int, ast.AST]
    functions: Dict[str, _Fn]


def _module_name(rel: str) -> str:
    return rel[:-3].replace("/", ".")


def _sources(trees=DUCKDB_TREES) -> List[Tuple[str, str]]:
    return [(p.relative_to(REPO).as_posix(), p.read_text(encoding="utf-8"))
            for p in _py_files(trees)]


def _modules(sources: List[Tuple[str, str]]) -> List[_Module]:
    out = []
    for rel, source in sources:
        tree = ast.parse(source)
        owners = _owners(tree)
        parents = {id(c): p for p in ast.walk(tree) for c in ast.iter_child_nodes(p)}
        functions = {}
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                refs: Dict[str, List[ast.AST]] = {}
                for sub in _own_nodes(node):
                    name = (sub.id if isinstance(sub, ast.Name)
                            else sub.attr if isinstance(sub, ast.Attribute) else None)
                    if name:
                        refs.setdefault(name, []).append(sub)
                qual = owners[id(node)][1]
                functions[qual] = _Fn(rel, qual, node, refs)
        out.append(_Module(rel, tree, owners, parents, functions))
    return out


def _statement_text(arg: ast.AST, module: _Module, fn: "_Fn | None") -> "str | None":
    """The text of a statement handed to a router, or None when the walk
    cannot know it: a constant, an f-string (holes as `__HOLE__`), a `+`
    chain, a local assigned one of those, or a name imported or defined at
    module level, read off the module it comes from."""
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value
    if isinstance(arg, ast.JoinedStr):
        return "".join(p.value if isinstance(p, ast.Constant) else "__HOLE__"
                       for p in arg.values)
    if isinstance(arg, ast.BinOp) and isinstance(arg.op, ast.Add):
        left = _statement_text(arg.left, module, fn)
        right = _statement_text(arg.right, module, fn)
        return None if left is None or right is None else left + right
    if not isinstance(arg, ast.Name):
        return None
    local = list(_own_nodes(fn.node)) if fn else []
    for node in local:
        if (isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == arg.id for t in node.targets)):
            return _statement_text(node.value, module, fn)
    for node in local + list(module.tree.body):
        if isinstance(node, ast.ImportFrom) and node.module and not node.level:
            for a in node.names:
                if (a.asname or a.name) == arg.id:
                    value = getattr(importlib.import_module(node.module), a.name, None)
                    return value if isinstance(value, str) else None
    value = getattr(importlib.import_module(_module_name(module.rel)), arg.id, None)
    return value if isinstance(value, str) else None


class _Captured:
    """A DuckDB connection that records what it is asked to run."""

    def __init__(self):
        self.sql: List[str] = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        return self

    def executemany(self, sql, rows=None):
        self.sql.append(sql)
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return None


class _CapturingStore:
    def __init__(self):
        self.conn = _Captured()

    @contextlib.asynccontextmanager
    async def connection(self):
        yield self.conn


def _render_for_duckdb(router: str, sql: str) -> List[str]:
    """What `router` runs on DuckDB for `sql`, by running the router itself
    against a connection that records — its own placeholders and its own
    engine choice, nothing restated here."""
    from core.duckdb_store import DuckDBStore

    store = _CapturingStore()

    async def run():
        if router == "_sms_tx":
            async with DuckDBStore._sms_tx(store) as tx:
                await tx.none(sql)
        else:
            await getattr(DuckDBStore, router)(store, sql, [], mode="none")

    saved = {k: os.environ.get(k) for k in (fates_mod.SMS_STORE, fates_mod.USER_STORE)}
    os.environ.update({fates_mod.SMS_STORE: "duckdb", fates_mod.USER_STORE: "duckdb"})
    try:
        asyncio.run(run())
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return [s for s in store.conn.sql
            if s.strip().upper() not in ("BEGIN TRANSACTION", "COMMIT", "ROLLBACK")]


@dataclasses.dataclass
class WriteWalk:
    sites: List[WriteSite]
    modules: Dict[str, _Module]
    unread_statements: List[str]   # routed statements the walk could not read

    def function(self, path: str, qualname: str) -> "_Fn | None":
        module = self.modules.get(path)
        return module.functions.get(qualname) if module else None

    def writers(self, table: str) -> List[WriteSite]:
        return [s for s in self.sites if s.table == table]

    def written(self, exempt=True) -> Set[str]:
        return {s.table for s in self.sites
                if not (exempt and (s.path, s.owner) in NOT_THE_FILE)}


def _handed_statements(module: _Module, routers) -> Iterator[Tuple[str, ast.AST, ast.AST]]:
    """`(router, statement, call)` for every statement handed to a router:
    its first argument, or for `_sms_tx` the first argument of each
    transaction method called on the name it binds."""
    for node in ast.walk(module.tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in routers and node.args):
            yield node.func.attr, node.args[0], node
        if isinstance(node, ast.AsyncWith):
            for item in node.items:
                ctx = item.context_expr
                if not (isinstance(ctx, ast.Call) and isinstance(ctx.func, ast.Attribute)
                        and ctx.func.attr in routers
                        and isinstance(item.optional_vars, ast.Name)):
                    continue
                var = item.optional_vars.id
                for sub in ast.walk(node):
                    if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)
                            and isinstance(sub.func.value, ast.Name)
                            and sub.func.value.id == var
                            and sub.func.attr in _TX_METHODS and sub.args):
                        yield ctx.func.attr, sub.args[0], sub


def walk_writes(sources: "List[Tuple[str, str]] | None" = None) -> WriteWalk:
    """Every DuckDB write in `DUCKDB_TREES`, or in the `(rel, source)`
    modules given."""
    routers = {r for _, _, rs in STORE_ROUTERS.values() for r in rs}
    modules = {m.rel: m for m in _modules(_sources() if sources is None else sources)}
    loaders: Dict[str, List[Tuple[_Fn, ast.AST]]] = {}
    for module in modules.values():
        for fn in module.functions.values():
            for name, nodes in fn.refs.items():
                loaders.setdefault(name, []).extend((fn, n) for n in nodes)
    sites: List[WriteSite] = []
    unread: List[str] = []

    for module in modules.values():
        handed = list(_handed_statements(module, routers))
        handed_ids = {id(arg) for _, arg, _ in handed}

        # Literal statements, attributed to the function they are in, or to
        # every function that reads the constant they are assigned to.
        for node, text in _strings(module.tree):
            if id(node) in handed_ids:
                continue
            kind, owner = module.owners.get(id(node), ("module", ""))
            rendered = (_render_template(module, (kind, owner), text)
                        if _TEMPLATED_WRITE.search(text) else None)
            targets = _write_targets(rendered if rendered is not None else text)
            if not targets:
                continue
            if kind == "fn":
                at = [(module.rel, owner, node)]
            else:
                at = [(fn.path, fn.qualname, ref) for fn, ref in loaders.get(owner, [])]
                at = at or [(module.rel, f"<{owner or 'module'}>", node)]
            sites += [WriteSite(rel, qual, t, None, where)
                      for rel, qual, where in at for t in targets]

        # Routed statements, each rendered for DuckDB by its router.
        for router, arg, call in handed:
            kind, owner = module.owners.get(id(call), ("module", ""))
            fn = module.functions.get(owner) if kind == "fn" else None
            text = _statement_text(arg, module, fn)
            if text is None:
                unread.append(f"{module.rel}:{arg.lineno} ({router})")
                continue
            for rendered in _render_for_duckdb(router, text):
                if re.search(r"\b(?:INTO|UPDATE|FROM|TABLE)\s+__HOLE__", rendered):
                    unread.append(f"{module.rel}:{arg.lineno} ({router})")
                sites += [WriteSite(module.rel, owner if kind == "fn" else f"<{owner}>",
                                    t, router, call)
                          for t in _write_targets(rendered)]
    return WriteWalk(sites, modules, unread)


def templated_writes(sources: "List[Tuple[str, str]] | None" = None) -> Set[Tuple[str, str]]:
    """`(module, owner)` of every write statement naming its table as a
    `{hole}` — the owner a constant's name or a function's qualname — except
    a statement handed straight to a router, which renders it."""
    routers = {r for _, _, rs in STORE_ROUTERS.values() for r in rs}
    out = set()
    for module in _modules(_sources() if sources is None else sources):
        handed = {id(arg) for _, arg, _ in _handed_statements(module, routers)}
        for node, text in _strings(module.tree):
            if id(node) not in handed and _TEMPLATED_WRITE.search(text):
                out.add((module.rel, module.owners.get(id(node), ("module", ""))[1]))
    return out


def _reader_says(test: ast.AST, reader: str, true_means_duckdb: bool) -> "bool | None":
    """True when `test` is equivalent to "DuckDB writes", False when it is
    equivalent to "DuckDB does not", None when it is neither: a call of the
    reader, or `not` of one."""
    if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
        inner = _reader_says(test.operand, reader, true_means_duckdb)
        return None if inner is None else not inner
    if isinstance(test, ast.Await):
        return _reader_says(test.value, reader, true_means_duckdb)
    if isinstance(test, ast.Call) and not test.args:
        f = test.func
        if (f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)) == reader:
            return true_means_duckdb
    return None


def _implies_duckdb(test, reader, polarity) -> bool:
    if isinstance(test, ast.BoolOp) and isinstance(test.op, ast.And):
        return any(_implies_duckdb(v, reader, polarity) for v in test.values)
    return _reader_says(test, reader, polarity) is True


def _implied_by_not_duckdb(test, reader, polarity) -> bool:
    if isinstance(test, ast.BoolOp) and isinstance(test.op, ast.Or):
        return any(_implied_by_not_duckdb(v, reader, polarity) for v in test.values)
    return _reader_says(test, reader, polarity) is False


def _exits(body: List[ast.stmt]) -> bool:
    return bool(body) and isinstance(body[-1], (ast.Return, ast.Raise, ast.Continue))


def on_duckdb_branch(module: _Module, fn: ast.AST, node: ast.AST,
                     reader: str, polarity: bool) -> bool:
    """Whether `node`, inside `fn`, runs only where `reader` says DuckDB:
    in the body of an `if` the reader makes true, the `else` of one it makes
    false, or after `if <not DuckDB>: return` earlier in an enclosing block."""
    child = node
    while child is not fn:
        parent = module.parents.get(id(child))
        if parent is None:
            return False
        if isinstance(parent, ast.If):
            if any(s is child for s in parent.body) and _implies_duckdb(
                    parent.test, reader, polarity):
                return True
            if any(s is child for s in parent.orelse) and _implied_by_not_duckdb(
                    parent.test, reader, polarity):
                return True
        for field in ("body", "orelse", "finalbody"):
            block = getattr(parent, field, None)
            if not isinstance(block, list):
                continue
            for i, stmt in enumerate(block):
                if stmt is child:
                    if any(isinstance(prev, ast.If) and not prev.orelse
                           and _implied_by_not_duckdb(prev.test, reader, polarity)
                           and _exits(prev.body) for prev in block[:i]):
                        return True
                    break
        child = parent
    return False


def guarded_by(walk: WriteWalk, switch: str) -> "Callable[[WriteSite], bool]":
    """Whether a write runs only where the switch says DuckDB: through one
    of its routers, on the DuckDB branch of its reader in its own function, or
    in a function every reference to which — a call, or a callback handed on
    — is itself on that branch or in such a function. A function nothing
    references (a route, a job, a script's main) must guard itself; a cycle
    is not an answer."""
    reader, polarity, routers = STORE_ROUTERS[switch]
    refs: Dict[str, List[Tuple[_Fn, ast.AST]]] = {}
    for module in walk.modules.values():
        for fn in module.functions.values():
            for name, nodes in fn.refs.items():
                refs.setdefault(name, []).extend((fn, n) for n in nodes)
    memo: Dict[Tuple[str, str], bool] = {}
    busy: Set[Tuple[str, str]] = set()

    def at(fn: _Fn, node: ast.AST) -> bool:
        module = walk.modules[fn.path]
        return (on_duckdb_branch(module, fn.node, node, reader, polarity)
                or whole(fn))

    def whole(fn: _Fn) -> bool:
        key = (fn.path, fn.qualname)
        if key in memo:
            return memo[key]
        if key in busy:
            return False
        busy.add(key)
        reaching = [(g, n) for g, n in refs.get(fn.qualname.rsplit(".", 1)[-1], [])
                    if g is not fn]
        result = bool(reaching) and all(at(g, n) for g, n in reaching)
        busy.discard(key)
        memo[key] = result
        return result

    def site(s: WriteSite) -> bool:
        if s.router is not None:
            return s.router in routers
        fn = walk.function(s.path, s.owner)
        return fn is not None and at(fn, s.node)

    return site


def switch_tables(walk: WriteWalk, switch: str) -> Set[str]:
    """The tables `switch` stops DuckDB writing: written somewhere, and by no
    writer that runs whatever the switch says."""
    check = guarded_by(walk, switch)
    out = set()
    for table in walk.written():
        sites = [s for s in walk.writers(table)
                 if (s.path, s.owner) not in NOT_THE_FILE]
        if sites and all(check(s) for s in sites):
            out.add(table)
    return out


@pytest.fixture(scope="module")
def writes() -> WriteWalk:
    return walk_writes()


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
        rendered = {(s.path, s.scope) for s in sites if needs_listing(s)}
        unlisted = sorted(rendered - set(RENDERED_DDL))
        gone = sorted(set(RENDERED_DDL) - rendered)
        assert not unlisted, (
            f"DDL naming its table at run time, or in a catalog the walk cannot "
            f"place, at {unlisted}: say where the name comes from in RENDERED_DDL")
        assert not gone, f"RENDERED_DDL lists sites that no longer exist: {gone}"

    @pytest.mark.parametrize("source, expected", [
        # The four shapes that passed the whole file green (review, finding 2).
        ('def f(c, name): c.execute("CREATE TABLE " + name + " (id INTEGER)")',
         ("f", "TABLE", UNREAD)),
        ('def f(c, name): c.execute("CREATE TABLE %s (id INTEGER)" % name)',
         ("f", "TABLE", UNREAD)),
        ('def f(c): c.sql("SELECT 1 AS id").create("mystery_relational")',
         ("f", "TABLE", "mystery_relational")),
        ('def f(c): c.execute("CREATE TABLE IF NOT EXISTS '
         'analytics.main.mystery_catalog (id INTEGER)")',
         ("f", "TABLE", "analytics.main.mystery_catalog")),
        # And their neighbours.
        ('def f(c, n): c.sql("SELECT 1").to_table(n)', ("f", "TABLE", UNREAD)),
        ('def f(c): c.sql("SELECT 1").create_view(view_name="mystery_v")',
         ("f", "VIEW", "mystery_v")),
        ('def f(c): c.sql("SELECT 1").to_view("mystery_v2", replace=True)',
         ("f", "VIEW", "mystery_v2")),
        ('def f(c, n): c.execute("CREATE TABLE stg_{} (id INTEGER)".format(n))',
         ("f", "TABLE", UNREAD)),
        ('def f(c, n): c.execute(f"CREATE TABLE stg_{n} (id INTEGER)")',
         ("f", "TABLE", UNREAD)),
        ('def f(c): c.execute("CREATE TABLE "\n "mystery_implicit (id INTEGER)")',
         ("f", "TABLE", "mystery_implicit")),
        ('def f(c): c.execute("CREATE VIEW " + "mystery_folded AS SELECT 1")',
         ("f", "VIEW", "mystery_folded")),
        ('def f(c): c.execute(\'CREATE TABLE "main"."mystery_quoted" (id INT)\')',
         ("f", "TABLE", "main.mystery_quoted")),
        ('def f(c, old): c.execute("ALTER TABLE " + old + " RENAME TO mystery_rn")',
         ("f", "TABLE", "mystery_rn")),
    ])
    def test_the_walk_reads_every_shape_of_ddl(self, source, expected):
        """Each shape yields one site, read or recorded as unread — never
        nothing. Mutations: the `+` folding in `_strings`, the `_READABLE`
        fallback to `UNREAD`, the relational walk in `walk_source`."""
        sites = walk_source("core/_probe.py", source)
        assert [(s.scope, s.kind, s.name) for s in sites if not s.temp] == [expected]

    @pytest.mark.parametrize("name, placed", [
        ("orders", "orders"), ("main.orders", "orders"),
        ("analytics.orders", "orders"), ("analytics.main.orders", "orders"),
        ("ANALYTICS.MAIN.orders", "orders"),
        ("bronze.orders", None), ("memory.main.orders", None),
        ("analytics.other.orders", None), ("temp.orders", None), (UNREAD, None),
    ])
    def test_a_qualified_name_is_placed_or_listed(self, name, placed):
        """Mutation: `place` dropping every dotted name as another engine's,
        which is what let `analytics.main.mystery_catalog` through."""
        assert place(name) == placed

    @pytest.mark.parametrize("name, listed", [
        ("orders", False), ("main.orders", False), ("analytics.main.orders", False),
        ("bronze.orders", True), ("memory.main.orders", True), (UNREAD, True),
    ])
    def test_an_unplaced_name_must_be_listed(self, name, listed):
        """Mutation: `needs_listing` reading only `{` holes, which let every
        dotted name through unlisted."""
        assert needs_listing(DdlSite("core/x.py", "f", "TABLE", name, False)) is listed
        assert needs_listing(DdlSite("core/x.py", "f", "TABLE", name, True)) is False

    def test_the_catalog_is_the_files_stem(self):
        from core.duckdb_constants import DB_PATH

        assert _catalog() == "analytics" == DB_PATH.stem

    def test_nothing_outside_the_walked_trees_opens_duckdb(self):
        """What lets the DDL walk read `DUCKDB_TREES` alone. It used to be a
        check of `bot/` only, reading only `import duckdb` and the module of a
        `from ... import`, so `from core import duckdb_store` in bot/, an
        `importlib.import_module("duckdb")` there, and a `tools/` directory
        nobody walked all passed (review, finding 3). Mutation: keep
        `DUCKDB_TREES` files in the scan — every opener there then fails."""
        files = repo_python()
        tops = {rel.split("/")[0] for rel, _ in files}
        # The family: a scan that listed nothing outside the trees passes.
        assert {"bot", "migrations"} <= tops and set(DUCKDB_TREES) <= tops
        offenders = openers_outside(
            [(rel, path.read_text(encoding="utf-8")) for rel, path in files])
        assert not offenders, (
            f"DuckDB is reached from outside {DUCKDB_TREES}: {offenders}. Move "
            f"the code into one of them, or add its directory to DUCKDB_TREES "
            f"so the DDL walk reads it")

    def test_only_the_walked_trees_may_open_duckdb(self):
        """Any top-level directory but the four, `tools/` as much as `bot/`.
        Mutation: narrow `openers_outside` to `bot/`, as the first test was."""
        files = [("tools/ledger.py", "import duckdb"), ("bot/x.py", "import duckdb"),
                 ("migrations/env.py", "from core import duckdb_store"),
                 ("manage.py", "import duckdb"),
                 *[(f"{tree}/x.py", "import duckdb") for tree in DUCKDB_TREES]]
        assert set(openers_outside(files)) == {
            "tools/ledger.py", "bot/x.py", "migrations/env.py", "manage.py"}

    def test_the_opener_walk_sees_every_tree_open_duckdb(self):
        """The scan above is only as good as `duckdb_openers`: each walked
        tree opens DuckDB somewhere, and the walk must see it there."""
        seen = {rel.split("/")[0] for rel, path in repo_python()
                if duckdb_openers(rel, path.read_text(encoding="utf-8"))}
        assert set(DUCKDB_TREES) <= seen, set(DUCKDB_TREES) - seen

    @pytest.mark.parametrize("rel, source", [
        ("bot/x.py", "import duckdb"),
        ("bot/x.py", "import duckdb as dk"),
        ("bot/x.py", "from duckdb import connect"),
        ("bot/x.py", "import core.duckdb_store"),
        ("bot/x.py", "from core.duckdb_store import get_store"),
        # The three reproductions of finding 3.
        ("bot/x.py", "from core import duckdb_store\n"
                     "async def f():\n    s = duckdb_store.DuckDBStore()"),
        ("bot/x.py", 'import importlib\nimportlib.import_module("duckdb").connect("x")'),
        ("tools/ledger.py", 'import duckdb\nduckdb.connect("data/analytics.duckdb")'),
        ("bot/x.py", '__import__("core.duckdb_store")'),
        ("bot/x.py", "import importlib\ndef f(n): return importlib.import_module(n)"),
        ("bot/x.py", "from core.sync_service import get_store"),
        ("bot/x.py", "import core\ncore.duckdb_store"),
        ("core/x.py", "from . import duckdb_store"),
        ("core/repositories/x.py", "from ..duckdb_store import DuckDBStore"),
        # The one opener, every way the review's probes reached it.
        ("bot/x.py", "from core import duckdb_switch\n"
                     "def peek(p):\n    return duckdb_switch.open_file(p)"),
        ("migrations/x.py", "from core.duckdb_switch import open_file"),
        ("bot/x.py", "import core.duckdb_switch"),
        ("bot/x.py", "import core\ncore.duckdb_switch.open_file('x')"),
        ("bot/x.py", 'import importlib\nimportlib.import_module("core.duckdb_switch")'),
        ("bot/x.py", "from core.somewhere import open_file\nopen_file('x')"),
    ])
    def test_the_opener_walk_reads_every_way_in(self, rel, source):
        assert duckdb_openers(rel, source)

    def test_the_opener_is_the_guards_own(self):
        """Read off the kill guard, so the two walks cannot name two openers.
        Mutation: spell this walk's opener as a literal and move the guard's."""
        from core import duckdb_switch

        assert _GUARD_OPENER == ("core/duckdb_switch.py", "open_file")
        assert _OPENER_MODULE == duckdb_switch.__name__
        assert callable(getattr(duckdb_switch, _GUARD_OPENER[1]))
        assert _GUARD_OPENER[1] in _OPENER_NAMES

    @pytest.mark.parametrize("source", [
        "from core import duckdb_constants",
        "# import duckdb",
        'MODE = "duckdb"',
        "from core.pg_sms import sms_store_is_postgres",
    ])
    def test_the_opener_walk_does_not_read_prose(self, source):
        assert duckdb_openers("bot/x.py", source) == []

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

    def test_an_append_only_replica_is_irreplaceable(self):
        """A table Postgres receives above a watermark (`_APPEND_ABOVE`) is a
        log of events DuckDB recorded as they happened, and no source re-issues
        an event. Marking one KEYCRM or COMPUTED takes it out of the
        must-travel-off-site rule above (review, mutation M14: stock_movements
        IRREPLACEABLE → KEYCRM survived the whole file)."""
        from core import pg_operational

        appended = {a.dk_table for a in pg_operational._APPEND_ABOVE}
        assert {"stock_movements", "inventory_sku_history"} <= appended
        loose = sorted(f"{n}: {FATES[n].origin}" for n in appended
                       if FATES[n].origin != IRREPLACEABLE)
        assert not loose, loose

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


@pytest.fixture
def every_switch_at_its_default(monkeypatch):
    """Every switch as production reads it with no line in `.env` — whatever
    this machine's `.env` says. `core.config` loads `.env` at import, and
    production's sets `KS_USER_STORE` and `KS_WRITE_EXPENSES`: a laptop
    holding a copy would otherwise read "every switch off" as a failure."""
    from core import warehouse_cutover as wc

    for env in [*_chain_switches(), *STORE_SWITCHES]:
        if env != fates_mod.WAREHOUSE:
            monkeypatch.setenv(env, "duckdb")
    # As `configure_mode()` leaves it for an unset variable, in web.
    monkeypatch.setattr(wc, "_value", wc.DUCKDB)
    monkeypatch.setattr(wc, "_mode", wc.DUCKDB)


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

    @pytest.mark.usefixtures("every_switch_at_its_default")
    def test_every_store_switch_has_a_reader(self):
        for switch in STORE_SWITCHES:
            assert fates_mod._store_switch_writes_duckdb(switch) is True, switch


# ─── 4b. the store switches, read off the code ───────────────────────────────

def _guarded(sources: List[Tuple[str, str]], switch: str = fates_mod.WAREHOUSE,
             table: str = "silver_orders") -> List[bool]:
    walk = walk_writes(sources)
    check = guarded_by(walk, switch)
    return [check(s) for s in walk.writers(table)]


class TestTheStoreSwitches:
    """Which tables KS_SMS_STORE, KS_USER_STORE and KS_WRITE_WAREHOUSE move
    was declared by hand and checked against nothing, so a table marked with
    the wrong switch — the unsafe direction, `duckdb_written()` answering
    False for a table DuckDB still writes — passed (review, finding 4:
    M11, M12, M13, M18, M19)."""

    @pytest.mark.parametrize("switch", STORE_SWITCHES)
    def test_a_store_switch_moves_exactly_what_it_stops_duckdb_writing(
            self, writes, switch):
        derived = switch_tables(writes, switch)
        declared = set(fates_mod.tables_by_switch(switch))
        assert derived, f"the walk found nothing {switch} moves"
        assert declared == derived, (
            f"{switch}: declared and not stopped by it {sorted(declared - derived)} "
            f"— DuckDB still writes these whatever {switch} says, so "
            f"duckdb_written() would lie about them; stopped by it and not "
            f"declared {sorted(derived - declared)}")

    def test_the_walk_reads_every_routed_statement(self, writes):
        assert not writes.unread_statements, writes.unread_statements
        routed = {(s.table, s.router) for s in writes.sites if s.router}
        # The family: each router, and the transaction.
        assert {("users", "_users_run"), ("role_permissions", "_perms_run"),
                ("sms_audience_presets", "_sms_run"),
                ("sms_campaign_members", "_sms_tx")} <= routed

    def test_the_routers_are_the_ones_that_route(self):
        """Each router asks its switch's reader, and the manifest reads the
        switch through the same reader."""
        from core.duckdb_store import DuckDBStore

        manifest = inspect.getsource(fates_mod._store_switch_writes_duckdb)
        for switch, (reader, _, routers) in STORE_ROUTERS.items():
            assert reader in manifest, (switch, reader)
            for router in routers:
                source = inspect.getsource(getattr(DuckDBStore, router))
                assert re.search(rf"\b{reader}\(\)", source), (router, reader)

    def test_every_router_in_the_code_is_listed(self):
        """A function that asks a store reader and runs a `sql` it is handed
        is a router; one not listed would route statements the walk never
        renders."""
        readers = {r for r, _, _ in STORE_ROUTERS.values()}
        listed = {r for _, _, rs in STORE_ROUTERS.values() for r in rs}
        found = set()
        for module in _modules(_sources(("core",))):
            for fn in module.functions.values():
                args = {a.arg for a in fn.node.args.args + fn.node.args.kwonlyargs}
                if "sql" in args and readers & set(fn.refs):
                    found.add(fn.qualname.rsplit(".", 1)[-1])
        assert found == listed - {"_sms_tx"}, found

    def test_not_the_file_lists_only_sites_that_exist(self, writes):
        present = {(s.path, s.owner) for s in writes.sites}
        assert set(NOT_THE_FILE) <= present, set(NOT_THE_FILE) - present

    # ── one text for two engines ──

    def test_the_watchdogs_templates_are_read_as_writes(self, writes):
        """Chain 10 made each sample statement one `{table}` text for both
        stores, and the literal pass lost all three tables: `duckdb_written()`
        said True and the walk said nothing writes them. Mutation: empty
        `HOLE_RENDERERS`."""
        expected = {
            "disk_samples": {("core/disk_monitor.py", "insert_sample"),
                             ("core/disk_monitor.py", "prune_old_samples")},
            "data_dir_samples": {("core/disk_monitor.py", "insert_dir_samples"),
                                 ("core/disk_monitor.py", "prune_old_dir_samples")},
            "memory_samples": {("core/memory_monitor.py", "insert_sample"),
                               ("core/memory_monitor.py", "prune_old_samples")},
        }
        for table, functions in expected.items():
            owners = {(s.path, s.owner) for s in writes.writers(table)}
            assert functions <= owners, (table, owners)

    def test_a_dialect_body_is_read_for_duckdb(self, writes):
        """Mutation: empty `DIALECT_BODIES`."""
        for (path, _), renderer in DIALECT_BODIES.items():
            assert any(s.path == path and s.owner == renderer for s in writes.sites), (
                path, renderer)

    def test_every_templated_write_is_rendered_or_listed(self):
        """Mutation: add `"DELETE FROM {t} WHERE x = ?"` to any module
        without listing it — the walk would not count the write."""
        found = templated_writes()
        unlisted = sorted(site for site in found
                          if site[0] not in HOLE_RENDERERS
                          and site not in DIALECT_BODIES
                          and site not in TEMPLATED_ELSEWHERE)
        assert not unlisted, (
            f"a write naming its table as a {{hole}} at {unlisted}: list the "
            f"module's DuckDB renderer, or say in TEMPLATED_ELSEWHERE what "
            f"fills it")
        gone = sorted((set(DIALECT_BODIES) | set(TEMPLATED_ELSEWHERE)) - found)
        assert not gone, f"listed and no longer there: {gone}"
        for module, renderer in HOLE_RENDERERS.items():
            assert any(rel == module for rel, _ in found), module
            assert callable(getattr(importlib.import_module(_module_name(module)),
                                    renderer)), (module, renderer)

    @pytest.mark.parametrize("source, found", [
        ('X = "INSERT INTO {t} VALUES (?)"', {("core/_probe.py", "X")}),
        ('def f(c):\n    c.execute("DELETE FROM {t} WHERE a = ?")',
         {("core/_probe.py", "f")}),
        ('X = f"UPDATE {{t}} SET a = {v}"', {("core/_probe.py", "X")}),
        ('X = f"UPDATE {t} SET a = 1"', set()),        # an f-string's hole
        ('X = "SELECT * FROM {t}"', set()),            # a read
        ('async def f(s):\n    await s._sms_run("DELETE FROM {presets}", [])',
         set()),                                       # the router renders it
    ])
    def test_the_template_walk_reads_its_shapes(self, source, found):
        assert templated_writes([("core/_probe.py", source)]) == found

    # ── the guard, shape by shape ──

    _W = 'c.execute("DELETE FROM silver_orders")'

    @pytest.mark.parametrize("source", [
        f"def f(c):\n    if duckdb_derives():\n        {_W}",
        f"def f(c):\n    if not wc.duckdb_derives():\n        return\n    {_W}",
        f"def f(c):\n    if not duckdb_derives():\n        x()\n    else:\n        {_W}",
        f"def f(c, x):\n    if x and duckdb_derives():\n        {_W}",
        f"def f(c, x):\n    if not duckdb_derives() or x:\n        raise E\n    {_W}",
        f"def f(c):\n    for i in y:\n        if not duckdb_derives():\n"
        f"            continue\n        {_W}",
        f"def w(c):\n    {_W}\n\ndef route(c):\n    if duckdb_derives():\n        w(c)",
        f"def w(c):\n    {_W}\n\ndef route(loop):\n    if not duckdb_derives():\n"
        f"        return\n    loop.run(w)",
    ])
    def test_a_write_on_the_duckdb_branch_is_guarded(self, source):
        assert _guarded([("core/_probe.py", source)]) == [True]

    @pytest.mark.parametrize("source", [
        # Asks, and writes whatever the answer: `full_sync`'s shape. Reading
        # "asks anywhere" as guarded claimed eight tables for the warehouse.
        f"def f(c):\n    derives = duckdb_derives()\n    {_W}",
        f"def f(c):\n    if duckdb_derives():\n        pass\n    {_W}",
        f"def f(c):\n    if not duckdb_derives():\n        log()\n    {_W}",
        f"def f(c, x):\n    if x or duckdb_derives():\n        {_W}",
        f"def f(c):\n    {_W}",
        f"def w(c):\n    {_W}\n\ndef a(c):\n    if duckdb_derives():\n        w(c)\n\n"
        f"def b(c):\n    w(c)",
        f"def w(c):\n    {_W}\n\ndef route(c):\n    if writes_postgres():\n        w(c)",
    ])
    def test_a_write_off_the_duckdb_branch_is_not(self, source):
        assert _guarded([("core/_probe.py", source)]) == [False]

    def test_the_polarity_follows_the_reader(self):
        """`sms_store_is_postgres()` true means Postgres writes."""
        w = 'c.execute("DELETE FROM marketing_optouts")'
        on = f"def f(c):\n    if not sms_store_is_postgres():\n        {w}"
        off = f"def f(c):\n    if sms_store_is_postgres():\n        {w}"
        after = f"def f(c):\n    if sms_store_is_postgres():\n        return\n    {w}"
        for source, expected in ((on, True), (off, False), (after, True)):
            assert _guarded([("core/_probe.py", source)], fates_mod.SMS_STORE,
                            "marketing_optouts") == [expected], source


# ─── 5. "which tables are still DuckDB-written today" ──────────────────────

@pytest.mark.usefixtures("every_switch_at_its_default")
class TestDuckdbWritten:
    def test_it_answers_for_every_entry(self):
        assert set(fates_mod.duckdb_written()) == set(FATES)

    def test_with_every_switch_off(self, writes):
        """With every switch at duckdb, DuckDB writes exactly the schema
        tables some DuckDB statement in the code writes — read off the code,
        not off `kind`. The first form pinned `schema_migrations` as not
        written because its fate is "retired", while `_record_migration`
        writes it on every connect that applies a step (review, finding 6).
        Mutation: `duckdb_written` answering False for RETIRED again."""
        written = fates_mod.duckdb_written()
        by_code = writes.written()
        assert "schema_migrations" in by_code  # the family
        wrong = {}
        for name, fate in FATES.items():
            expected = (fate.object == TABLE and fate.ddl == SCHEMA
                        and name in by_code)
            if written[name] is not expected:
                wrong[name] = (written[name], expected)
        assert not wrong, f"duckdb_written() against the code's writers: {wrong}"

    def test_the_warehouse_is_unknown_where_its_mode_was_never_configured(
            self, monkeypatch):
        """Outside web, `configure_modes()` never ran, and the warehouse
        tables read True whatever KS_WRITE_WAREHOUSE said (review, finding 5).
        Mutation: drop the `value() is None` answer."""
        from core import warehouse_cutover as wc

        monkeypatch.setattr(wc, "_value", None)
        monkeypatch.setattr(wc, "_mode", None)
        monkeypatch.setenv(fates_mod.WAREHOUSE, "postgres")
        written = fates_mod.duckdb_written()
        moved = set(fates_mod.tables_by_switch(fates_mod.WAREHOUSE))
        assert moved and {written[n] for n in moved} == {None}
        assert all(written[n] is not None for n in FATES if n not in moved
                   and FATES[n].switch not in _chain_switches())

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

        monkeypatch.setattr(wc, "_value", wc.POSTGRES)
        monkeypatch.setattr(wc, "_mode", wc.POSTGRES)
        written = fates_mod.duckdb_written()
        moved = set(fates_mod.tables_by_switch(fates_mod.WAREHOUSE))
        assert {"silver_orders", "gold_daily_revenue", "warehouse_refreshes"} <= moved
        assert {written[n] for n in moved} == {False}

    def test_a_latched_chain_moves_its_tables(self, monkeypatch):
        """A frozen chain's tables leave DuckDB with its latch. A shadow
        chain's do not (OD-02 (c)): Postgres writes first and DuckDB is still
        handed every row (`core/shadow_writes.py`), so the file is written as
        long as the shadow stands. Mutations: answer `mode == "duckdb"` for
        every chain (the shadow tables read False), or True for every one."""
        from core import chain_latch
        from core.write_chains import chain_name, is_shadow

        shapes = set()
        for env, chain in _chain_switches().items():
            chain_latch.latch(chain_name(chain), env)
            written = fates_mod.duckdb_written()
            shadow = is_shadow(chain)
            shapes.add(shadow)
            for name in fates_mod.tables_by_switch(env):
                assert written[name] is shadow, (env, name, shadow)
        assert shapes == {True, False}, "the family: both shapes are registered"

    def test_a_shadow_chains_tables_are_written_by_its_shadow(self, writes):
        """What makes True the honest answer for a latched shadow chain: the
        DuckDB statements the shadow runs are the walk's writes of the very
        tables the chain declares."""
        from core.write_chains import WRITE_CHAINS, is_shadow

        shadow = [c for c in WRITE_CHAINS if is_shadow(c)]
        assert shadow
        by_code = writes.written()
        for chain in shadow:
            tables = fates_mod.tables_by_switch(chain.WRITE_ENV)
            assert tables and set(tables) <= by_code, (chain.WRITE_ENV, tables)

    def test_a_chain_flag_nobody_can_read_is_unknown(self, monkeypatch):
        for env in _chain_switches():
            monkeypatch.setenv(env, "postgress")
            written = fates_mod.duckdb_written()
            assert {written[n] for n in fates_mod.tables_by_switch(env)} == {None}, env
            monkeypatch.setenv(env, "duckdb")


# ─── 6. the host check ───────────────────────────────────────────────────────

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
