"""Every writer of the buyers is reached only through chain 4's answer, on the
branch that answer sends it down.

Once `KS_WRITE_BUYERS` has moved the buyers, DuckDB's `buyers`,
`buyer_contacts` and `buyer_gender` have stopped: a write there is a second
writer beside the chain's, and the copy-back would later refuse or overwrite
what it wrote. And the chain's own writers write Postgres unconditionally and
latch the chain on their first write, so a caller that reaches one without
asking moves the chain whatever its flag says. The DN-22b walk in
`test_write_chains.py` guards the shippers and sees only awaited asyncpg
writes, and it takes a registered chain for the destination; these are the two
halves it cannot see.

The rule, both halves: walk `core/`, `web/`, `scripts/`, `bot/` and `deploy/`,
and require every call into a writer to sit on the branch of chain 4's answer
that leads to its store — or to be reached only from calls that do. "Asks
somewhere in the function" is not enough (review of PR-3): the buyers step
asks `mode() is None` to refuse a typo, and a DuckDB write added below that
check would still be a second writer. What counts:

- DuckDB side: in the body of `if <mode> == "duckdb"` or `if not
  writes_postgres()`; after `if writes_postgres(): return`, or after
  `if <mode> != "duckdb": return`; or in the `else` of `if <mode> ==
  "postgres"` when the function has already left on `<mode> is None`.
- Postgres side: in the body of `if <mode> == "postgres"` or `if
  writes_postgres()`; or after `if <mode> != "postgres": return` or `if not
  writes_postgres(): return`.

`<mode>` is `pg_buyers_write.mode()` itself or a name the function bound to
it. Found, not listed: a writer or a caller added tomorrow is held to it the
day it lands.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pytest

ROOT = Path(__file__).resolve().parents[2]
TREES = ("core", "web", "scripts", "bot", "deploy")
CHAIN_MODULE = "core.pg_buyers_write"

# A statement writing a buyer table in DuckDB: no schema, or DuckDB's own
# `main.`, a quoted name, or a `{table}` hole for `sql_dialect.render_tables`
# — which may render either engine, and is counted: a write whose target the
# walk cannot resolve is a write (the DN-22b walk's rule).
_WRITE = re.compile(
    r"(?<![\w.])(?:INSERT(?:\s+OR\s+(?:REPLACE|IGNORE))?\s+INTO|DELETE\s+FROM|"
    r"UPDATE|TRUNCATE(?:\s+TABLE)?)\s+(?:main\.)?[\"{]?"
    r"(buyers|buyer_contacts|buyer_gender)[\"}]?(?![\w.])", re.IGNORECASE)

Key = Tuple[str, str]  # (repo-relative path, function name)

# Functions exempt from the rule, each with its reason. Empty today.
EXEMPT: Dict[Key, str] = {}


class _Fn:
    def __init__(self, rel, node, consts, imported, aliases):
        self.rel, self.node = rel, node
        self.consts, self.imported, self.aliases = consts, imported, aliases

    @property
    def module(self) -> str:
        return self.rel[:-3].replace("/", ".")


def _functions() -> Dict[Key, _Fn]:
    out = {}
    for tree_name in TREES:
        for path in sorted((ROOT / tree_name).rglob("*.py")):
            rel = str(path.relative_to(ROOT))
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except SyntaxError:  # pragma: no cover - not ours to parse
                continue
            consts, imported, aliases = {}, {}, {}
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    for alias in node.names:
                        bound = alias.asname or alias.name
                        imported[bound] = (node.module, alias.name)
                        # `from core import gender_backfill [as gb]` binds a module.
                        aliases[bound] = f"{node.module}.{alias.name}"
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.asname:
                            aliases[alias.asname] = alias.name
            for node in tree.body:
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            consts[target.id] = [
                                c.value for c in ast.walk(node.value)
                                if isinstance(c, ast.Constant) and isinstance(c.value, str)]
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    out[(rel, node.name)] = _Fn(rel, node, consts, imported, aliases)
    return out


def _strings(fn: _Fn) -> List[str]:
    found = []
    for n in ast.walk(fn.node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            found.append(n.value)
        elif isinstance(n, ast.Name):
            found.extend(fn.consts.get(n.id, ()))
    return found


def _dotted(node) -> Optional[str]:
    """`a.b.c` for a Name/Attribute chain, else None."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    parts.append(node.id)
    return ".".join(reversed(parts))


def _calls_into(target_module: str, name: str, caller: _Fn,
                methods: bool = False) -> List[ast.Call]:
    """The calls in `caller` that reach `target_module.name`: a bare call in
    its own module or through `from <module> import <name> [as x]`, an
    attribute call on the module however it was bound (`import M as A`, `from
    pkg import M as A`, `core.M`), and — for store methods, whose receiver the
    walk cannot type — `self.name` in the same module or `<anything>.name`."""
    out = []
    for n in ast.walk(caller.node):
        if not isinstance(n, ast.Call):
            continue
        f = n.func
        if isinstance(f, ast.Name):
            if caller.module == target_module and f.id == name:
                out.append(n)
            elif caller.imported.get(f.id) == (target_module, name):
                out.append(n)
        elif isinstance(f, ast.Attribute) and f.attr == name:
            base = _dotted(f.value)
            resolved = caller.aliases.get(base, base) if base else None
            if resolved == target_module or (
                    base and base.split(".")[-1] == target_module.split(".")[-1]
                    and caller.aliases.get(base.split(".")[0], "").startswith("core")):
                out.append(n)
            elif methods and (base == "self" and caller.module == target_module
                              or base not in (None, "self")):
                out.append(n)
    return out


# ─── Which branch of the chain's answer a call sits on ───────────────────────

def _is_question(node, names: Set[str], attr: str) -> bool:
    """`pg_buyers_write.<attr>()` (however bound), or a name bound to it."""
    if isinstance(node, ast.Name):
        return node.id in names
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == attr and _dotted(node.func.value) is not None
            and _dotted(node.func.value).split(".")[-1] == "pg_buyers_write")


def _mode_names(fn: _Fn) -> Set[str]:
    names = set()
    for n in ast.walk(fn.node):
        if isinstance(n, ast.Assign) and _is_question(n.value, set(), "mode"):
            names |= {t.id for t in n.targets if isinstance(t, ast.Name)}
    return names


def _compares(test, names, value: str, op) -> bool:
    return (isinstance(test, ast.Compare) and len(test.ops) == 1
            and isinstance(test.ops[0], op)
            and _is_question(test.left, names, "mode")
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == value)


def _is_none_check(test, names) -> bool:
    return (isinstance(test, ast.Compare) and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Is)
            and _is_question(test.left, names, "mode")
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value is None)


def _writes_pg(test) -> bool:
    return _is_question(test, set(), "writes_postgres")


def _not(test):
    return test.operand if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not) else None


def _exits(body) -> bool:
    return bool(body) and isinstance(body[-1], (ast.Return, ast.Raise))


def _leads_to(test, names, store: str, where: str) -> bool:
    """Whether code in the `body`/`orelse` of `if test` runs only on `store`."""
    other = "postgres" if store == "duckdb" else "duckdb"
    negated = _not(test)
    if where == "body":
        return (_compares(test, names, store, ast.Eq)
                or (store == "postgres" and _writes_pg(test))
                or (store == "duckdb" and negated is not None and _writes_pg(negated)))
    return (_compares(test, names, other, ast.NotEq)
            or (store == "duckdb" and _writes_pg(test))
            or (store == "postgres" and negated is not None and _writes_pg(negated)))


def _on_branch(fn: _Fn, call: ast.Call, store: str) -> bool:
    """Whether `call` in `fn` runs only when chain 4 writes `store`."""
    names = _mode_names(fn)
    left_on_none = False

    def visit(stmts) -> Optional[bool]:
        nonlocal left_on_none
        exited_to_store = False
        for stmt in stmts:
            contains = any(n is call for n in ast.walk(stmt))
            if isinstance(stmt, ast.If):
                if contains:
                    in_body = any(n is call for s in stmt.body for n in ast.walk(s))
                    branch = stmt.body if in_body else stmt.orelse
                    if _leads_to(stmt.test, names, store, "body" if in_body else "orelse"):
                        return True
                    # `else` of `== "postgres"` is DuckDB once None has left.
                    if (not in_body and store == "duckdb" and left_on_none
                            and _compares(stmt.test, names, "postgres", ast.Eq)):
                        return True
                    inner = visit(branch)
                    return bool(exited_to_store or inner)
                if _exits(stmt.body):
                    if _is_none_check(stmt.test, names):
                        left_on_none = True
                    # An early exit on the other store leaves only this one.
                    if _leads_to(stmt.test, names, store, "orelse"):
                        exited_to_store = True
                continue
            if contains:
                for field in ("body", "orelse", "finalbody", "handlers"):
                    block = getattr(stmt, field, None)
                    if isinstance(block, list) and any(
                            n is call for s in block for n in ast.walk(s)):
                        stmts_in = ([s for h in block for s in h.body]
                                    if field == "handlers" else block)
                        inner = visit(stmts_in)
                        return bool(exited_to_store or inner)
                return exited_to_store
        return None

    return bool(visit(fn.node.body))


# ─── The walk ────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def functions():
    return _functions()


@pytest.fixture(scope="module")
def writers(functions):
    return {key for key, fn in functions.items()
            if any(_WRITE.search(s) for s in _strings(fn))}


def _callers(target: Key, functions, methods: bool) -> List[Tuple[Key, ast.Call]]:
    module = target[0][:-3].replace("/", ".")
    out = []
    for key, fn in functions.items():
        if key == target:
            continue
        for call in _calls_into(module, target[1], fn, methods=methods):
            out.append((key, call))
    return out


def _covered(key: Key, functions, store: str, seen=frozenset()) -> bool:
    """Every call into `key` is on `store`'s branch, or sits in a function
    whose own callers all are."""
    if key in EXEMPT:
        return True
    if key in seen:
        return False
    # Store methods are reached as `store.<name>(...)`, a receiver the walk
    # cannot type — so for those, and only those, the method name is the key.
    # The chain's `upsert_buyers` shares the name and is reached by its module.
    callers = _callers(key, functions, methods=key[0] == "core/duckdb_store.py")
    if not callers:
        return False
    for caller, call in callers:
        if _on_branch(functions[caller], call, store):
            continue
        if not _covered(caller, functions, store, seen | {key}):
            return False
    return True


def _chain_writers(functions) -> List[Key]:
    from tests.unit.test_chain_latch import _writers
    from core import pg_buyers_write

    rel = "core/pg_buyers_write.py"
    return [(rel, name) for name in _writers(pg_buyers_write)]


class TestTheWalk:
    def test_it_finds_the_duckdb_writers_that_exist(self, writers):
        assert {("core/duckdb_store.py", "_upsert_buyer_portion"),
                ("core/gender_backfill.py", "write")} <= writers, sorted(writers)

    def test_it_reads_a_statement_held_in_a_module_constant(self, functions):
        """`gender_backfill.write` runs `_INSERT`, a module constant."""
        fn = functions[("core/gender_backfill.py", "write")]
        assert any(_WRITE.search(s) for s in fn.consts["_INSERT"])

    @pytest.mark.parametrize("sql,is_write", [
        ("INSERT INTO bronze.buyers (id) VALUES ($1)", False),
        ("INSERT INTO app.buyer_gender AS g", False),
        ("SELECT * FROM buyers", False),
        ("INSERT OR REPLACE INTO buyers (id) VALUES (?)", True),
        ("INSERT OR IGNORE INTO buyer_gender VALUES (?)", True),
        ("DELETE FROM buyer_contacts WHERE buyer_id = ?", True),
        ("DELETE FROM {buyer_gender} WHERE buyer_id = ?", True),
        ("TRUNCATE buyer_gender", True),
        ('DELETE FROM "buyer_gender"', True),
        ("INSERT INTO main.buyer_gender VALUES (?)", True),
        ("UPDATE buyer_gender SET gender = 'f'", True),
    ])
    def test_what_counts_as_a_duckdb_write(self, sql, is_write):
        assert bool(_WRITE.search(sql)) is is_write, sql

    def test_an_aliased_module_is_followed(self):
        src = ("import core.gender_backfill as gb\n"
               "from core import gender_backfill as g2\n"
               "from core.gender_backfill import derive_gender as dg\n"
               "async def f(store):\n"
               "    await gb.derive_gender(store)\n"
               "    await g2.derive_gender(store)\n"
               "    await dg(store)\n")
        tree = ast.parse(src)
        imported, aliases = {}, {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for a in node.names:
                    imported[a.asname or a.name] = (node.module, a.name)
                    aliases[a.asname or a.name] = f"{node.module}.{a.name}"
            elif isinstance(node, ast.Import):
                for a in node.names:
                    if a.asname:
                        aliases[a.asname] = a.name
        fn = _Fn("scripts/x.py", tree.body[-1], {}, imported, aliases)
        assert len(_calls_into("core.gender_backfill", "derive_gender", fn)) == 3


def _fn_of(src: str) -> Tuple[_Fn, ast.Call]:
    tree = ast.parse(src)
    fn = _Fn("core/x.py", tree.body[-1], {}, {}, {})
    calls = [n for n in ast.walk(fn.node) if isinstance(n, ast.Call)
             and getattr(n.func, "attr", getattr(n.func, "id", "")) == "target"]
    return fn, calls[0]


class TestWhatCountsAsOnTheBranch:
    @pytest.mark.parametrize("src,store,on", [
        # The store method's shape.
        ("async def f():\n    if pg_buyers_write.writes_postgres():\n"
         "        return await other()\n    await target()\n", "duckdb", True),
        # The rider's.
        ("async def f():\n    m = pg_buyers_write.mode()\n    if m == 'postgres':\n"
         "        await other()\n    elif m == 'duckdb':\n        await target()\n",
         "duckdb", True),
        ("async def f():\n    m = pg_buyers_write.mode()\n    if m == 'postgres':\n"
         "        await target()\n", "postgres", True),
        # The CLI's: None has left, so the else of == 'postgres' is DuckDB.
        ("async def f():\n    m = pg_buyers_write.mode()\n    if m is None:\n"
         "        return 4\n    if m == 'postgres':\n        await other()\n"
         "    else:\n        await target()\n", "duckdb", True),
        # Asking without routing: the step's typo refusal.
        ("async def f():\n    if pg_buyers_write.mode() is None:\n        return 0\n"
         "    await target()\n", "duckdb", False),
        # The else of == 'postgres' without the None exit: a typo lands here.
        ("async def f():\n    m = pg_buyers_write.mode()\n    if m == 'postgres':\n"
         "        await other()\n    else:\n        await target()\n", "duckdb", False),
        ("async def f():\n    await target()\n", "duckdb", False),
        # The wrong branch.
        ("async def f():\n    if pg_buyers_write.writes_postgres():\n"
         "        await target()\n", "duckdb", False),
    ])
    def test_the_branch_decides_not_the_question(self, src, store, on):
        fn, call = _fn_of(src)
        assert _on_branch(fn, call, store) is on


class TestEveryPathTakesTheRightBranch:
    def test_every_duckdb_writer_is_reached_only_on_the_duckdb_branch(
            self, functions, writers):
        uncovered = sorted(w for w in writers
                           if not _covered(w, functions, "duckdb"))
        assert not uncovered, (
            "writes DuckDB's buyers without being on chain 4's duckdb branch on "
            f"every path into it: {uncovered}. Branch on pg_buyers_write.mode() "
            "or writes_postgres() at the call, or name it in EXEMPT with why.")

    def test_every_chain_writer_is_reached_only_on_the_postgres_branch(self, functions):
        """The chain's writers latch on their first write whatever the flag
        says, so a caller that reached one without asking would move the
        chain (review of PR-3)."""
        writers = _chain_writers(functions)
        assert {k[1] for k in writers} == {"upsert_buyers", "derive_gender_pg"}
        uncovered = sorted(w for w in writers
                           if not _covered(w, functions, "postgres"))
        assert not uncovered, (
            f"reaches chain 4's writer without being on its postgres branch: {uncovered}")

    def test_the_routers_that_choose_are_the_ones_expected(self, functions):
        """Where the branch is taken today. A router that stopped taking it
        would make the walk climb one level and find a caller that does not."""
        cases = {
            ("core/duckdb_store.py", "upsert_buyers"): "_upsert_buyer_portion",
            ("core/scheduler.py", "_run_replicate_operational"): "derive_gender",
            ("scripts/backfill_gender.py", "run"): "derive_gender",
        }
        for key, callee in cases.items():
            fn = functions[key]
            calls = [n for n in ast.walk(fn.node) if isinstance(n, ast.Call)
                     and getattr(n.func, "attr", getattr(n.func, "id", "")) == callee]
            assert calls and all(_on_branch(fn, c, "duckdb") for c in calls), key

    def test_every_exemption_is_still_a_real_function(self, functions):
        for key in EXEMPT:
            assert key in functions, f"{key} is exempt and no longer exists"
