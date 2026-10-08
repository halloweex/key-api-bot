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
  `pg_goals_write.reads_the_chain`. Found by reading the code, never listed,
  so a goal read added tomorrow through a router that does not ask fails here
  rather than in front of the owner. The behaviour itself — the page, the
  target line, the refusal to fall back — is proven against a real Postgres
  in `tests/integration/test_goals_writer.py::TestTheReadsFollowTheChain`.

Chain 7b-3 (`KS_WRITE_FORECAST`) has the same rule for its four tables, and
the walk runs once per chain: a statement carrying `{revenue_predictions}`,
`{seasonal_indices}`, `{growth_metrics}` or `{weekly_patterns}` must be
handed to a router that asks `pg_forecast_write.reads_the_chain` — by that
name, so a router asking only 7a's question does not pass for 7b-3's tables.
Two more rules for 7b-3 alone: the three goal tables are read in ONE
statement (their writer stores them in one transaction), and no bare DuckDB
read of the four tables is left anywhere but in the DuckDB writer branches.
"""
from __future__ import annotations

import ast
import pathlib
import re
from typing import Dict, Iterable, List, Set, Tuple

import pytest

from core import chain_latch, pg_forecast_write, pg_goals_write

REPO = pathlib.Path(__file__).resolve().parents[2]
WALKED = ("core", "web")
HOLE = pg_goals_write.TABLE_HOLE
ASKS = "reads_the_chain"
HOLE_HOME = "core/pg_goals_write.py"

# `(chain name, holes, the module that defines them, the constant they live
# in)`. The router must ask `<chain name>.reads_the_chain`. Chain 7b-3's holes
# are derived from the tables it owns, not read from `TABLE_HOLES`: the walk
# taking its holes from the module under test would shrink with it, and a
# hole dropped there is exactly a table whose reads stop following the chain
# (`test_forecast_chain.py::TestReadsTheChain` pins the two equal).
CHAINS = {
    "pg_goals_write": ((pg_goals_write.TABLE_HOLE,), HOLE_HOME, "TABLE_HOLE"),
    "pg_forecast_write": (tuple("{%s}" % t.split(".", 1)[1]
                                for t in pg_forecast_write.CHAIN_TABLES),
                          "core/pg_forecast_write.py", "TABLE_HOLES"),
}
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


def _qualified(call: ast.Call) -> str:
    """`module.attr` for a call on a bare name, else the bare call name."""
    func = call.func
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        return f"{func.value.id}.{func.attr}"
    return _call_name(call)


def _own_calls(fn: ast.AST) -> Set[str]:
    """Qualified names of the calls `fn` makes itself, not those of nested
    functions."""
    out, stack = set(), list(ast.iter_child_nodes(fn))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda,
                             ast.ClassDef)):
            continue
        if isinstance(node, ast.Call):
            out.add(_qualified(node))
        stack.extend(ast.iter_child_nodes(node))
    return out


def _has_hole(value: str, holes: Tuple[str, ...]) -> bool:
    return any(h in value for h in holes)


def _holding_literals(tree: ast.Module, docs: Set[int],
                      holes: Tuple[str, ...]) -> List[ast.Constant]:
    return [n for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
            and _has_hole(n.value, holes) and id(n) not in docs]


def _constants(tree: ast.Module, holes: Tuple[str, ...]) -> Dict[str, ast.Constant]:
    """Module-level names bound to a statement carrying a hole."""
    out = {}
    for node in tree.body:
        targets, value = [], None
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        if (isinstance(value, ast.Constant) and isinstance(value.value, str)
                and _has_hole(value.value, holes)):
            for t in targets:
                if isinstance(t, ast.Name):
                    out[t.id] = value
    return out


def _carries(node: ast.AST, constants: Dict[str, ast.Constant],
             holes: Tuple[str, ...]) -> bool:
    """The expression is a statement with a hole: a literal, a module
    constant, an f-string with the hole in it, or one of those through a
    string transform or a `%`."""
    if isinstance(node, ast.Constant):
        return isinstance(node.value, str) and _has_hole(node.value, holes)
    if isinstance(node, ast.Name):
        return node.id in constants
    if isinstance(node, ast.JoinedStr):
        return any(_carries(v, constants, holes) for v in node.values)
    if isinstance(node, ast.BinOp):
        return (_carries(node.left, constants, holes)
                or _carries(node.right, constants, holes))
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr in TRANSFORMS):
        return _carries(node.func.value, constants, holes)
    return False


def _router_calls(tree: ast.Module, constants, holes: Tuple[str, ...]):
    """`(call, carrying arg)` for every call a statement with a hole is
    handed to, string transforms looked through."""
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or (
                isinstance(node.func, ast.Attribute)
                and node.func.attr in TRANSFORMS):
            continue
        for arg in list(node.args) + [k.value for k in node.keywords]:
            if _carries(arg, constants, holes):
                out.append((node, arg))
    return out


def _walk(chain: str = "pg_goals_write"):
    holes, home, home_constant = CHAINS[chain]
    trees = list(_modules())
    routers = {fn.name for _rel, tree in trees for fn in ast.walk(tree)
               if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef))
               and f"{chain}.{ASKS}" in _own_calls(fn)}
    sites, stray = [], []
    for rel, tree in trees:
        docs = _docstrings(tree)
        constants = _constants(tree, holes)
        if rel == home:
            # The holes' own definition, which `reads_the_chain` tests with.
            constants.pop(home_constant, None)
        handed = _router_calls(tree, constants, holes)
        covered: Set[int] = set()
        for call, arg in handed:
            sites.append((rel, call.lineno, _call_name(call)))
            for n in ast.walk(arg):
                covered.add(id(n))
        bound = {id(v) for v in constants.values()}
        for lit in _holding_literals(tree, docs, holes):
            if id(lit) in covered or id(lit) in bound:
                continue
            if rel == home and lit.value in holes:
                continue                     # TABLE_HOLE(S) itself
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

    def test_the_walk_finds_the_forecast_reads_there_are(self):
        """Non-vacuity for chain 7b-3: `_PREDICTIONS_SQL` and
        `_FORECAST_PREDICTED_SQL` through `_goals_run`, the three goal tables
        through `_goal_tables_run`."""
        routers, sites, _stray = _walk("pg_forecast_write")
        assert {"_goals_run", "_goal_tables_run"} <= routers
        assert "_marketing_run" not in routers
        in_goals = [name for rel, _l, name in sites
                    if rel == "core/repositories/goals.py"]
        assert in_goals.count("_goals_run") >= 2
        assert in_goals.count("_goal_tables_run") >= 1

    @pytest.mark.parametrize("chain", sorted(CHAINS))
    def test_each_is_handed_to_a_router_that_asks_the_chain(self, chain):
        routers, sites, _stray = _walk(chain)
        wrong = [(rel, line, name) for rel, line, name in sites
                 if name not in routers]
        assert not wrong, (
            f"a statement reading {CHAINS[chain][0]} goes through a router "
            f"that never asks {chain}.{ASKS}(), so once that chain writes "
            f"Postgres it reads whatever the page's own flag names — DuckDB's "
            f"frozen copy included: {wrong}")

    @pytest.mark.parametrize("chain", sorted(CHAINS))
    def test_no_goal_statement_is_left_where_the_walk_cannot_follow_it(self, chain):
        """A statement the walk cannot trace to a router — bound to a local,
        built in a helper, passed through something it does not know — is a
        read it cannot vouch for. Rewrite it into a shape the walk follows."""
        _routers, _sites, stray = _walk(chain)
        assert not stray, f"{CHAINS[chain][0]} used where the walk cannot follow it: {stray}"

    def test_a_router_asking_only_the_other_chain_does_not_pass(self):
        """`_marketing_run` asks 7a's question. Were a forecast read handed to
        it, 7b-3's walk must name it — the qualification is the point."""
        routers, _sites, _stray = _walk("pg_forecast_write")
        assert "_marketing_run" not in routers
        routers, _sites, _stray = _walk("pg_goals_write")
        assert "_goal_tables_run" not in routers


# ─── Chain 7b-3's two rules of its own ──────────────────────────────────────

GOAL_TABLE_HOLES = ("{seasonal_indices}", "{growth_metrics}", "{weekly_patterns}")


class TestTheGoalTablesAreReadAsOneSet:
    def test_every_statement_reading_one_reads_all_three(self):
        """Their writer stores them in one transaction; three statements
        would read two sets on a Postgres that commits between them."""
        found = 0
        for rel, tree in _modules():
            docs = _docstrings(tree)
            for lit in _holding_literals(tree, docs, GOAL_TABLE_HOLES):
                if lit.value in GOAL_TABLE_HOLES:
                    continue                 # a hole's own definition
                found += 1
                missing = [h for h in GOAL_TABLE_HOLES if h not in lit.value]
                assert not missing, (rel, lit.lineno, missing)
        assert found >= 1


# A DuckDB statement naming one of the four tables bare, as a table.
_BARE = re.compile(
    r"\b(FROM|JOIN)\s+(seasonal_indices|growth_metrics|weekly_patterns"
    r"|revenue_predictions)\b", re.IGNORECASE)
# The DuckDB writer branches, where the bare names belong: chain 7b-3 routes
# each of these to Postgres before it reaches them.
WRITER_BRANCHES = {"_persist_goal_tables", "store_predictions"}


class TestNoBareReadIsLeft:
    def test_only_the_duckdb_writers_name_the_tables_bare(self):
        """The smart goal read the three goal tables by their bare DuckDB
        names, on its own connection, until this chain. A bare read is one no
        router can send to Postgres."""
        stray = []
        for rel, tree in _modules():
            docs = _docstrings(tree)
            owners = {}
            for fn in ast.walk(tree):
                if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    for n in ast.walk(fn):
                        owners.setdefault(id(n), set()).add(fn.name)
            module_names = {}
            for node in tree.body:
                if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
                        and isinstance(node.value.value, str)):
                    for t in node.targets:
                        if isinstance(t, ast.Name):
                            module_names[id(node.value)] = t.id
            for lit in (n for n in ast.walk(tree)
                        if isinstance(n, ast.Constant) and isinstance(n.value, str)):
                if id(lit) in docs or not _BARE.search(lit.value):
                    continue
                if owners.get(id(lit), set()) & WRITER_BRANCHES:
                    continue
                name = module_names.get(id(lit))
                if name is not None:
                    users = {f for f in _users(tree, name)}
                    if users and users <= WRITER_BRANCHES:
                        continue
                stray.append((rel, lit.lineno))
        assert not stray, f"a bare read of a 7b-3 table: {stray}"


def _users(tree: ast.Module, name: str) -> Set[str]:
    out = set()
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if any(isinstance(n, ast.Name) and n.id == name
                   and isinstance(n.ctx, ast.Load) for n in ast.walk(fn)):
                out.add(fn.name)
    return out
