"""Chain 7a's reads follow the chain, not the page's read flag (DN-25 review).

The first version of the goals writer moved the write and left every read of
`app.revenue_goals` on `KS_READ_GOALS` and `KS_READ_MARKETING`, with "both at
`postgres`" as a precondition of the flip that nothing enforced. Putting a
read flag back is every read port's documented rollback, so one routine
rollback after the latch would have shown the pre-flip goal from a DuckDB
nothing writes any more — silently, and for good. And with the flags at
`postgres`, a Postgres error fell back to that same frozen copy.

Two things are pinned here, without a database:

- `reads_postgres` is `writes_postgres` applied to reads — latch first, then
  the flag — except that an unreadable flag on a chain that never wrote here
  hands the choice back to the page instead of raising;
- **the walk**: every shared statement under `core/` and `web/` that carries
  the `{revenue_goals}` hole is handed to a router that asks
  `reads_the_chain`. Found by reading the code, never listed, so a goal read
  added tomorrow through a router that does not ask fails here rather than
  in front of the owner. The behaviour itself — the page, the target line,
  the refusal to fall back — is proven against a real Postgres in
  `tests/integration/test_goals_writer.py::TestTheReadsFollowTheChain`.
"""
from __future__ import annotations

import ast
import pathlib
from typing import Dict, Iterable, List, Set, Tuple

import pytest

from core import chain_latch, pg_goals_write

REPO = pathlib.Path(__file__).resolve().parents[2]
WALKED = ("core", "web")
HOLE = pg_goals_write.TABLE_HOLE
ASKS = "reads_the_chain"
HOLE_HOME = "core/pg_goals_write.py"
# String methods a statement passes through on its way to a router; the
# statement is the receiver, so the router is still the call it lands in.
TRANSFORMS = {"replace", "format", "strip", "lstrip", "rstrip"}


@pytest.fixture
def flag(monkeypatch):
    monkeypatch.delenv(pg_goals_write.WRITE_ENV, raising=False)
    return monkeypatch


class TestReadsPostgres:
    def test_off_by_default(self, flag):
        assert pg_goals_write.reads_postgres() is False

    def test_the_flag_turns_it_on(self, flag):
        flag.setenv(pg_goals_write.WRITE_ENV, "postgres")
        assert pg_goals_write.reads_postgres() is True

    @pytest.mark.parametrize("value", ["duckdb", "postgre", None])
    def test_the_latch_outranks_any_flag(self, flag, value):
        if value is not None:
            flag.setenv(pg_goals_write.WRITE_ENV, value)
        chain_latch.latch(pg_goals_write.CHAIN, pg_goals_write.WRITE_ENV)
        assert pg_goals_write.reads_postgres() is True

    def test_an_unreadable_flag_never_written_through_hands_back_the_choice(self, flag):
        """The writer refuses in both stores and the replace stands down, so
        neither copy moves; raising would take the page down over a variable
        about writes. The writer itself still raises."""
        flag.setenv(pg_goals_write.WRITE_ENV, "postgre")
        assert pg_goals_write.reads_postgres() is False
        with pytest.raises(RuntimeError, match=pg_goals_write.WRITE_ENV):
            pg_goals_write.writes_postgres()

    def test_only_a_statement_carrying_the_hole_follows(self, flag):
        chain_latch.latch(pg_goals_write.CHAIN, pg_goals_write.WRITE_ENV)
        assert pg_goals_write.reads_the_chain(
            "SELECT goal_amount FROM {revenue_goals}") is True
        assert pg_goals_write.reads_the_chain(
            "SELECT SUM(revenue) FROM {gold_daily_revenue}") is False

    def test_a_goal_read_keeps_the_page_flag_while_the_chain_writes_duckdb(self, flag):
        assert pg_goals_write.reads_the_chain(
            "SELECT goal_amount FROM {revenue_goals}") is False


# ─── The walk ──────────────────────────────────────────────────────────────


def _modules() -> Iterable[Tuple[str, ast.Module]]:
    for top in WALKED:
        for path in sorted((REPO / top).rglob("*.py")):
            rel = str(path.relative_to(REPO))
            yield rel, ast.parse(path.read_text(encoding="utf-8"), filename=rel)


def _docstrings(tree: ast.Module) -> Set[int]:
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if (isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str)):
                out.add(id(first.value))
    return out


def _call_name(call: ast.Call) -> str:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return ""


def _own_calls(fn: ast.AST) -> Set[str]:
    """Names of the calls `fn` makes itself, not those of nested functions."""
    out, stack = set(), list(ast.iter_child_nodes(fn))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda,
                             ast.ClassDef)):
            continue
        if isinstance(node, ast.Call):
            out.add(_call_name(node))
        stack.extend(ast.iter_child_nodes(node))
    return out


def _holding_literals(tree: ast.Module, docs: Set[int]) -> List[ast.Constant]:
    return [n for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
            and HOLE in n.value and id(n) not in docs]


def _constants(tree: ast.Module) -> Dict[str, ast.Constant]:
    """Module-level names bound to a statement carrying the hole."""
    out = {}
    for node in tree.body:
        targets, value = [], None
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        if (isinstance(value, ast.Constant) and isinstance(value.value, str)
                and HOLE in value.value):
            for t in targets:
                if isinstance(t, ast.Name):
                    out[t.id] = value
    return out


def _carries(node: ast.AST, constants: Dict[str, ast.Constant]) -> bool:
    """The expression is a statement with the hole: a literal, a module
    constant, an f-string with the hole in it, or one of those through a
    string transform or a `%`."""
    if isinstance(node, ast.Constant):
        return isinstance(node.value, str) and HOLE in node.value
    if isinstance(node, ast.Name):
        return node.id in constants
    if isinstance(node, ast.JoinedStr):
        return any(_carries(v, constants) for v in node.values)
    if isinstance(node, ast.BinOp):
        return _carries(node.left, constants) or _carries(node.right, constants)
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr in TRANSFORMS):
        return _carries(node.func.value, constants)
    return False


def _router_calls(tree: ast.Module, constants):
    """`(call, carrying arg)` for every call a statement with the hole is
    handed to, string transforms looked through."""
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or (
                isinstance(node.func, ast.Attribute)
                and node.func.attr in TRANSFORMS):
            continue
        for arg in list(node.args) + [k.value for k in node.keywords]:
            if _carries(arg, constants):
                out.append((node, arg))
    return out


def _walk():
    trees = list(_modules())
    routers = {fn.name for _rel, tree in trees for fn in ast.walk(tree)
               if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef))
               and ASKS in _own_calls(fn)}
    sites, stray = [], []
    for rel, tree in trees:
        docs = _docstrings(tree)
        constants = _constants(tree)
        if rel == HOLE_HOME:
            # The hole's own definition, which `reads_the_chain` tests with.
            constants.pop("TABLE_HOLE", None)
        handed = _router_calls(tree, constants)
        covered: Set[int] = set()
        used_names: Set[str] = set()
        for call, arg in handed:
            sites.append((rel, call.lineno, _call_name(call)))
            for n in ast.walk(arg):
                covered.add(id(n))
                if isinstance(n, ast.Name):
                    used_names.add(n.id)
        bound = {id(v) for v in constants.values()}
        for lit in _holding_literals(tree, docs):
            if id(lit) in covered or id(lit) in bound:
                continue
            if rel == HOLE_HOME and lit.value == HOLE:
                continue                     # TABLE_HOLE itself
            stray.append((rel, lit.lineno))
        for name in constants:
            loads = [n for n in ast.walk(tree) if isinstance(n, ast.Name)
                     and n.id == name and isinstance(n.ctx, ast.Load)]
            for n in loads:
                if id(n) not in covered:
                    stray.append((rel, n.lineno))
            if not loads:
                stray.append((rel, constants[name].lineno))
    return routers, sites, stray


class TestEveryGoalReadIsHandedToARouterThatAsks:
    def test_the_walk_finds_the_goal_reads_there_are(self):
        """Non-vacuity: the stored goals — `get_goals` and `get_smart_goals`
        — and the /marketing target line."""
        routers, sites, _stray = _walk()
        assert {"_goals_run", "_marketing_run"} <= routers
        by_router = {}
        for rel, _line, name in sites:
            by_router.setdefault((rel, name), 0)
            by_router[(rel, name)] += 1
        assert by_router.get(("core/repositories/goals.py", "_goals_run"), 0) >= 2
        assert by_router.get(("core/repositories/revenue.py", "_marketing_run"), 0) >= 1

    def test_each_is_handed_to_a_router_that_asks_the_chain(self):
        routers, sites, _stray = _walk()
        wrong = [(rel, line, name) for rel, line, name in sites
                 if name not in routers]
        assert not wrong, (
            f"a statement reading {HOLE} goes through a router that never asks "
            f"pg_goals_write.{ASKS}(), so under chain 7a it reads whatever the "
            f"page's own flag names — DuckDB's frozen copy included: {wrong}")

    def test_no_goal_statement_is_left_where_the_walk_cannot_follow_it(self):
        """A statement the walk cannot trace to a router — bound to a local,
        built in a helper, passed through something it does not know — is a
        read it cannot vouch for. Rewrite it into a shape the walk follows."""
        _routers, _sites, stray = _walk()
        assert not stray, f"{HOLE} used where the walk cannot follow it: {stray}"
