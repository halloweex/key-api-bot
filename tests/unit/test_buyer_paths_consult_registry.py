"""Every DuckDB-side writer of the buyers is reached only through chain 4's answer.

Once `KS_WRITE_BUYERS` has moved the buyers, DuckDB's `buyers`, `buyer_contacts`
and `buyer_gender` have stopped: a write there is a second writer beside the
chain's, and the copy-back would later refuse or overwrite what it wrote. The
DN-22b walk in `test_write_chains.py` guards the Postgres side and sees only
awaited asyncpg writes; this is the DuckDB half it cannot see.

The rule: walk `core/`, `web/`, `scripts/`, `bot/` and `deploy/` for every
function carrying a DuckDB statement that writes one of the three tables, and
require every path into it to pass through a function that evaluates
`pg_buyers_write.mode()` or `pg_buyers_write.writes_postgres()` — or to be named
below with the reason it may not. Found, not listed: a writer added tomorrow is
held to it the day it lands.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Dict, Set, Tuple

import pytest

ROOT = Path(__file__).resolve().parents[2]
TREES = ("core", "web", "scripts", "bot", "deploy")

# A DuckDB statement writing a buyer table: no schema, which is what tells it
# from Postgres's `bronze.buyers` / `app.buyer_gender`.
_WRITE = re.compile(
    r"(?<![\w.])(?:INSERT(?:\s+OR\s+REPLACE)?\s+INTO|DELETE\s+FROM|UPDATE)\s+"
    r"(buyers|buyer_contacts|buyer_gender)\b", re.IGNORECASE)

# The chain's own questions, as attribute calls on the module.
_QUESTIONS = {"mode", "writes_postgres"}
_CHAIN_MODULE = "pg_buyers_write"

Key = Tuple[str, str]  # (repo-relative path, function name)

# Functions that write DuckDB's buyer tables, or are reached only by what
# does, without asking — each with the reason that is right. Empty today:
# `gender_backfill.derive_gender` does not ask either, and needs no entry,
# because the walk climbs to its two callers (the rider and the CLI) and both
# ask — it is the DuckDB branch they choose.
EXEMPT: Dict[Key, str] = {}


def _functions():
    """`{key: (node, module_imports)}` over every function in the trees."""
    out = {}
    for tree_name in TREES:
        for path in sorted((ROOT / tree_name).rglob("*.py")):
            rel = str(path.relative_to(ROOT))
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except SyntaxError:  # pragma: no cover - not ours to parse
                continue
            consts = {}
            imported = {}
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    for alias in node.names:
                        imported[alias.asname or alias.name] = (node.module, alias.name)
            for node in tree.body:
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            consts[target.id] = [
                                c.value for c in ast.walk(node.value)
                                if isinstance(c, ast.Constant) and isinstance(c.value, str)]
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    out[(rel, node.name)] = (node, consts, imported)
    return out


def _strings(node, consts):
    found = []
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            found.append(n.value)
        elif isinstance(n, ast.Name):
            found.extend(consts.get(n.id, ()))
    return found


def _asks(node) -> bool:
    return any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
               and n.func.attr in _QUESTIONS
               and isinstance(n.func.value, ast.Name)
               and n.func.value.id == _CHAIN_MODULE
               for n in ast.walk(node))


def _module_of(rel: str) -> str:
    return rel[:-3].replace("/", ".")


def _calls_to(target: Key, functions) -> Set[Key]:
    """Functions that call `target`: a bare call in its own module or through
    a `from <module> import <name>`, or an attribute call — `self.x`, or
    `<module>.x` where the module is the target's."""
    rel, name = target
    module = _module_of(rel)
    short = module.rsplit(".", 1)[-1]
    callers = set()
    for key, (node, _consts, imported) in functions.items():
        if key == target:
            continue
        for n in ast.walk(node):
            if not isinstance(n, ast.Call):
                continue
            f = n.func
            if isinstance(f, ast.Name) and f.id == name:
                if key[0] == rel or imported.get(name) == (module, name):
                    callers.add(key)
            elif isinstance(f, ast.Attribute) and f.attr == name:
                base = f.value
                if isinstance(base, ast.Name) and (
                        (base.id == "self" and key[0] == rel) or base.id == short):
                    callers.add(key)
                elif name.startswith("_") and key[0] == rel:
                    callers.add(key)
                elif not name.startswith("_") and isinstance(base, (ast.Name, ast.Attribute)) \
                        and name in _STORE_METHODS:
                    callers.add(key)
    return callers


# Store methods reached as `store.<name>(...)` from anywhere — the receiver is
# not a name the walk can resolve, so the method name is the key. Only the
# DuckDB buyer writers' own names belong here.
_STORE_METHODS = {"upsert_buyers", "_upsert_buyer_portion"}


@pytest.fixture(scope="module")
def functions():
    return _functions()


@pytest.fixture(scope="module")
def writers(functions):
    return {key for key, (node, consts, _imp) in functions.items()
            if any(_WRITE.search(s) for s in _strings(node, consts))}


def _covered(key, functions, seen=frozenset()) -> bool:
    node = functions[key][0]
    if _asks(node) or key in EXEMPT:
        return True
    if key in seen:
        return False
    callers = _calls_to(key, functions)
    return bool(callers) and all(_covered(c, functions, seen | {key}) for c in callers)


class TestTheWalk:
    def test_it_finds_the_writers_that_exist(self, writers):
        """Not vacuous: DuckDB's buyer rows and its gender verdicts."""
        assert {("core/duckdb_store.py", "_upsert_buyer_portion"),
                ("core/gender_backfill.py", "write")} <= writers, sorted(writers)

    def test_it_reads_a_statement_held_in_a_module_constant(self):
        """`gender_backfill.write` runs `_INSERT`, a module constant."""
        functions = _functions()
        node, consts, _ = functions[("core/gender_backfill.py", "write")]
        assert any(_WRITE.search(s) for s in consts["_INSERT"])

    def test_a_postgres_statement_is_not_one(self):
        assert not _WRITE.search("INSERT INTO bronze.buyers (id) VALUES ($1)")
        assert not _WRITE.search("INSERT INTO app.buyer_gender AS g")
        assert _WRITE.search("INSERT OR REPLACE INTO buyers (id) VALUES (?)")
        assert _WRITE.search("DELETE FROM buyer_contacts WHERE buyer_id = ?")


class TestEveryPathAsksChain4:
    def test_every_writer_is_reached_only_through_the_chains_answer(
            self, functions, writers):
        uncovered = sorted(w for w in writers if not _covered(w, functions))
        assert not uncovered, (
            "writes DuckDB's buyers without chain 4's answer on every path "
            f"into it: {uncovered}. Branch on pg_buyers_write.mode() or "
            "writes_postgres() in the caller, or name it in EXEMPT with why.")

    def test_the_routers_that_choose_are_the_ones_expected(self, functions):
        """Where the question is asked today — the store method, the hourly
        rider and the CLI. A router that stopped asking would make the walk
        climb one level and find a caller that does not."""
        for key in (("core/duckdb_store.py", "upsert_buyers"),
                    ("core/scheduler.py", "_run_replicate_operational"),
                    ("scripts/backfill_gender.py", "run")):
            assert _asks(functions[key][0]), key

    def test_every_exemption_is_still_a_real_function(self, functions):
        for key in EXEMPT:
            assert key in functions, f"{key} is exempt and no longer exists"
