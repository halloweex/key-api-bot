"""No read falls back to DuckDB, and every router refuses — found by a walk.

Every router reads one engine and refuses its failure through
`core.read_fallback.refuse`, which counts the refusal and hands back the
`ReadUnavailable` an HTTP route answers 503 naming the surface. What makes
that hold everywhere is the walk below, which reads `core/` and `web/` for the
shapes a read reaching DuckDB took rather than trusting a list of routers — a
list only ever guards the routers somebody was already thinking about, and
`pg_expenses_read.backfilled()` is the proof: it served DuckDB for weeks under
log text no grep for "falling back to DuckDB" could find, because it was
neither a router nor a gate.

THE SHAPES, AND WHAT EACH MUST DO

1. **A log call saying "falling back to DuckDB"** (docstrings are not calls,
   so they do not count). None may stand where a failure is handled without
   being raised again: there is no DuckDB left to fall back to.
2. **An `X.enabled() and X.available()` gate** — the shape every read switch
   used to choose an engine. None is left since step 14's S14-07, and none
   may come back: a gate anywhere is a violation.
3. **Any except handler in a function that reaches another engine's read** —
   `get_pool`, a `*_run` router, or a function of a read-switch module — under
   `core/repositories/` or `core/pg_*_read*.py`. A handler that returns is
   judged too: `backfilled()` returned False and `_pg_gold_summary` None, and
   it was the *caller* that then read DuckDB — invisible from the handler, so
   every handler in that scope has to decide: re-raise, or say in the same
   `try` what `ReadUnavailable` means.

5. **A router refuses every engine failure** (step 14, S14-05). A `_*_run`
   router under `core/repositories/` — `_traffic_run`, `_margin_run`,
   `_intel_run`, `_reports_run`, `_marketing_run` and `_lookups_run` first,
   the dashboard's `_dashboard_run` since S14-06 — or any function there that
   calls `refuse` at all (the dashboard's `_pg_gold_summary`,
   `_pg_gold_series`, `_pg_silver` and `get_manager_sales_365d`), must run
   every engine call
   (a function of a `core.pg_*` module it imports — or, since S14-07, of a
   `core.ch_*` one — however it is spelled, or `get_pool`) inside a
   `try` whose handler for `Exception` is exactly `raise
   read_fallback.refuse("<surface>", <the caught exception>)`, and reach
   DuckDB through nothing at all. One bare `await pg_x.fetch()` outside the
   `try` would turn the counted 503 naming the surface into an uncounted
   500; a handler that falls back, returns or raises something else would
   lose the surface or the count.

Rule 4 asked every gate whether its switch had an address
(`read_fallback.no_address`), until step 14's S14-16 deleted it: no gate is
left to ask. Rules 1 to 3 counted a fallback through `read_fallback.fall_back`
until then; it is deleted too, so re-raising is the only decision left.

A read-switch module is one whose `ENV` names a `KS_READ_*` variable, found by
walking `core/` — not listed either. None is left since S14-07.
"""
from __future__ import annotations

import ast
import logging
import pathlib
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Iterable, List, Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core import read_fallback

REPO = pathlib.Path(__file__).resolve().parents[2]
WALKED = ("core", "web")
FALLBACK_TEXT = re.compile(r"falling back to DuckDB", re.IGNORECASE)
# What a handler that carries on toward DuckDB looks like when it reads it
# itself. Deliberately loose: a false positive costs a decision somebody has
# to write, a false negative costs a silent fallback.
DUCKDB_READS = {"connection", "get_store", "_fetch_one", "_fetch_all"}


# ─── The walk ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Site:
    rule: str          # "log" | "gate" | "handler" | "refuse"
    path: str
    line: int
    function: str
    ok: bool


def _own_nodes(node: ast.AST) -> Iterable[ast.AST]:
    """Every node under `node`, not descending into nested functions,
    lambdas or classes — those are judged as functions of their own."""
    stack = list(ast.iter_child_nodes(node))
    while stack:
        child = stack.pop()
        yield child
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                              ast.Lambda, ast.ClassDef)):
            continue
        stack.extend(ast.iter_child_nodes(child))


def _call_name(call: ast.Call) -> Optional[str]:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _functions(tree: ast.AST) -> List[ast.AST]:
    return [n for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]


def _mentions_read_unavailable(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return False
    for n in ast.walk(handler.type):
        if isinstance(n, ast.Name) and n.id == "ReadUnavailable":
            return True
        if isinstance(n, ast.Attribute) and n.attr == "ReadUnavailable":
            return True
    return False


def _decides(handler: ast.ExceptHandler, try_node: ast.Try) -> bool:
    """The handler re-raises, or the `try` it belongs to has already said
    what `ReadUnavailable` means. (Counting a fallback through
    `read_fallback.fall_back` was the other answer until step 14's S14-16
    deleted it.)"""
    for n in _own_nodes(handler):
        if isinstance(n, ast.Raise):
            return True
    for earlier in try_node.handlers:
        if _mentions_read_unavailable(earlier):
            return True
        if earlier is handler:
            break
    return False


def _handlers(fn: ast.AST) -> List[tuple]:
    """(handler, its try) for every handler owned by `fn`."""
    out = []
    for n in _own_nodes(fn):
        if isinstance(n, (ast.Try, getattr(ast, "TryStar", ast.Try))):
            for h in n.handlers:
                out.append((h, n))
    return out


def _is_gate(node: ast.AST) -> bool:
    if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.And)):
        return False
    called = {_call_name(n) for v in node.values for n in ast.walk(v)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    return {"enabled", "available"} <= called


def _string_parts(node: ast.AST) -> List[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.JoinedStr):
        return [v.value for v in node.values
                if isinstance(v, ast.Constant) and isinstance(v.value, str)]
    return []


def read_switch_modules() -> Set[str]:
    """Modules in `core/` whose `ENV` names a `KS_READ_*` variable and which
    answer `enabled()` — a switch choosing an engine. `core/read_fallback.py`
    has the prefix and no `enabled`: it chooses how a failure degrades, not
    where a read goes."""
    found = set()
    for path in sorted((REPO / "core").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names_env = any(
            isinstance(stmt, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "ENV" for t in stmt.targets)
            and isinstance(stmt.value, ast.Constant)
            and str(stmt.value.value).startswith("KS_READ_")
            for stmt in tree.body)
        answers = any(isinstance(stmt, ast.FunctionDef) and stmt.name == "enabled"
                      for stmt in tree.body)
        if names_env and answers:
            found.add(path.stem)
    return found


def _switch_names(tree: ast.AST, switches: Set[str]) -> tuple:
    """(module aliases, function names) this file binds to a read switch."""
    modules, functions = set(), set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            for a in n.names:
                if a.name in switches:
                    modules.add(a.asname or a.name)
        elif (isinstance(n, ast.ImportFrom) and n.module
              and n.module.startswith("core.")
              and n.module.split(".", 1)[1] in switches):
            for a in n.names:
                functions.add(a.asname or a.name)
        elif isinstance(n, ast.Import):
            for a in n.names:
                if a.name.startswith("core.") and a.name.split(".", 1)[1] in switches:
                    modules.add(a.asname or a.name)
    return modules, functions


def _reaches_another_engine(fn: ast.AST, modules: Set[str], functions: Set[str]) -> bool:
    for n in ast.walk(fn):
        if not isinstance(n, ast.Call):
            continue
        name = _call_name(n)
        if name == "get_pool":
            return True
        if name and name.endswith("_run") and name != "_run":
            return True
        if (isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)
                and n.func.value.id in modules
                and name not in ("enabled", "available")):
            return True
        if isinstance(n.func, ast.Name) and n.func.id in functions:
            return True
    return False


def _in_handler_scope(relpath: str) -> bool:
    return (relpath.startswith("core/repositories/")
            or re.fullmatch(r"core/pg_[^/]*read[^/]*\.py", relpath) is not None)


# Rule 5. What a router with no tail may not reach: DuckDB through the store,
# and the gate's or the fallback's machinery, which belong to the old shape.
TAIL_READS = {"connection", "_fetch_one", "_fetch_all", "execute"}
# The old shape's names: the gate's, and `read_fallback`'s fallback machinery,
# deleted in step 14's S14-16 — a call of one there would not even resolve.
OLD_SHAPE = {"fall_back", "no_address", "no_engine", "enabled", "available"}
BROAD = {"Exception", "BaseException"}


# The engines a router reaches: Postgres' modules, and since step 14's
# S14-07 ClickHouse's, whose cohort reader is the cohorts' one engine.
ENGINE_PREFIXES = ("pg_", "ch_")


def _engine_aliases(tree: ast.AST) -> Set[str]:
    """Every name the file binds to a `core.pg_*` or `core.ch_*` module, at
    the top or inside a function: `from core import pg_traffic_read`,
    `import core.pg_x as y`."""
    found = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            for a in n.names:
                if a.name.startswith(ENGINE_PREFIXES):
                    found.add(a.asname or a.name)
        elif isinstance(n, ast.Import):
            for a in n.names:
                if (a.name.startswith(tuple(f"core.{p}" for p in ENGINE_PREFIXES))
                        and a.asname):
                    found.add(a.asname)
    return found


def _is_engine_module(dotted: str) -> bool:
    """`core.pg`, or a `core.pg_*` or `core.ch_*` module (`ENGINE_PREFIXES`)."""
    return dotted == "core.pg" or dotted.startswith(
        tuple(f"core.{p}" for p in ENGINE_PREFIXES))


def _engine_names(tree: ast.AST) -> Set[str]:
    """Every name the file imports from `core.pg` or a `core.pg_*` or
    `core.ch_*` module, at the top or inside a function — only a call of one
    counts, so a constant among them changes nothing: `from
    core.pg_gold_read import fetch_series`, `from core.pg import get_pool as
    gp`, `from core.ch_cohorts import fetch`. Until the review of step 14's
    S14-06 merge the rule judged `<module>.<fn>(...)` and a bare
    `get_pool(...)` alone, so a call by such a name outside every `try` was
    an uncounted 500 it never saw; ClickHouse's modules, the cohorts' one
    engine since S14-07, are read in every spelling since S14-07's merge of
    that review."""
    found = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module and _is_engine_module(n.module):
            found |= {a.asname or a.name for a in n.names}
    return found


def _names_an_engine_module(node: ast.AST) -> bool:
    """`core.pg`, `core.pg_*` or `core.ch_*` spelled out: `import
    core.pg_x` binds no alias, so its calls are `core.pg_x.fetch(...)`."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not (isinstance(node, ast.Name) and node.id == "core" and parts):
        return False
    return _is_engine_module(f"core.{parts[-1]}")


def _engine_calls(fn: ast.AST, aliases: Set[str], names: Set[str]) -> List[ast.Call]:
    """The engine calls in `fn`'s own nodes: a function of an engine module
    by the module's alias or spelled out, a function imported by its name
    (`names`, `_engine_names` — required, so a caller cannot forget it), and
    `get_pool` however it is reached."""
    out = []
    for n in _own_nodes(fn):
        if not isinstance(n, ast.Call):
            continue
        if _call_name(n) == "get_pool":
            out.append(n)
        elif (isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)
              and n.func.value.id in aliases):
            out.append(n)
        elif isinstance(n.func, ast.Name) and n.func.id in names:
            out.append(n)
        elif isinstance(n.func, ast.Attribute) and _names_an_engine_module(n.func.value):
            out.append(n)
    return out


def _is_read_fallback_refuse(call: ast.Call) -> bool:
    """`read_fallback.refuse(...)` — qualified, because a route's validation
    helper is called `refuse` too (`web/routes/api/customers.py`)."""
    f = call.func
    return (isinstance(f, ast.Attribute) and f.attr == "refuse"
            and isinstance(f.value, ast.Name) and f.value.id == "read_fallback")


def _refuses_it(handler: ast.ExceptHandler) -> bool:
    """`except Exception as exc: raise read_fallback.refuse("<surface>", exc)`
    and nothing else."""
    caught = handler.type
    if caught is not None and not (
            isinstance(caught, ast.Name) and caught.id in BROAD):
        return False
    if handler.name is None or len(handler.body) != 1:
        return False
    stmt = handler.body[0]
    if not (isinstance(stmt, ast.Raise) and stmt.cause is None
            and isinstance(stmt.exc, ast.Call)):
        return False
    call = stmt.exc
    if not _is_read_fallback_refuse(call) or len(call.args) != 2 or call.keywords:
        return False
    surface, cause = call.args
    return (isinstance(surface, ast.Constant) and isinstance(surface.value, str)
            and re.fullmatch(r"[a-z_]+", surface.value) is not None
            and isinstance(cause, ast.Name) and cause.id == handler.name)


def _refused_where_it_fails(call: ast.Call, fn: ast.AST,
                            parent: Dict[ast.AST, ast.AST]) -> bool:
    """`call` sits in the body of a `try`, inside `fn`, one of whose handlers
    refuses it."""
    node = call
    while node in parent and node is not fn:
        up = parent[node]
        if isinstance(up, (ast.Try, getattr(ast, "TryStar", ast.Try))):
            # `node` is the child of the `try` the walk came up through.
            in_body = any(node is stmt for stmt in up.body)
            if in_body and any(_refuses_it(h) for h in up.handlers):
                return True
        node = up
    return False


def walk_source(source: str, relpath: str, switches: Set[str]) -> List[Site]:
    """The shapes in one file. Pure, so it can be shown synthetic code."""
    tree = ast.parse(source)
    parent: Dict[ast.AST, ast.AST] = {}
    for p in ast.walk(tree):
        for c in ast.iter_child_nodes(p):
            parent[c] = p

    def enclosing(node, kinds):
        while node in parent:
            node = parent[node]
            if isinstance(node, kinds):
                return node
        return None

    def try_of(handler):
        return parent[handler]

    sites: List[Site] = []

    # 1. A log call saying "falling back to DuckDB".
    for call in (n for n in ast.walk(tree) if isinstance(n, ast.Call)):
        texts = [t for a in call.args for t in _string_parts(a)]
        if not any(FALLBACK_TEXT.search(t) for t in texts):
            continue
        fn = enclosing(call, (ast.FunctionDef, ast.AsyncFunctionDef))
        name = getattr(fn, "name", "<module>")
        handler = enclosing(call, (ast.ExceptHandler,))
        if handler is not None and (fn is None or enclosing(handler, (
                ast.FunctionDef, ast.AsyncFunctionDef)) is fn):
            ok = _decides(handler, try_of(handler))
        else:
            ok = False
        sites.append(Site("log", relpath, call.lineno, name, ok))

    # 2. An enabled()-and-available() gate: none may stand (S14-07). (Rule
    # 4, its address, went with `no_address` in S14-16.)
    for gate in (n for n in ast.walk(tree) if _is_gate(n)):
        fn = enclosing(gate, (ast.FunctionDef, ast.AsyncFunctionDef))
        sites.append(Site("gate", relpath, gate.lineno,
                          getattr(fn, "name", "<module>"), False))

    # 3. Every handler in a function that reaches another engine's read.
    if _in_handler_scope(relpath):
        modules, functions = _switch_names(tree, switches)
        for fn in _functions(tree):
            if not _reaches_another_engine(fn, modules, functions):
                continue
            for h, t in _handlers(fn):
                sites.append(Site("handler", relpath, h.lineno, fn.name,
                                  _decides(h, t)))

    # 5. A router with no gate left refuses every engine failure.
    if relpath.startswith("core/repositories/"):
        aliases, names = _engine_aliases(tree), _engine_names(tree)
        for fn in _functions(tree):
            name = getattr(fn, "name", "")
            refuses = any(isinstance(n, ast.Call) and _is_read_fallback_refuse(n)
                          for n in _own_nodes(fn))
            # A `_*_run` router, or anything that refuses at all: the
            # dashboard's Gold and Silver helpers are not called `_*_run`
            # (step 14, S14-06), and a refusal is a claim this rule checks.
            if not ((name.startswith("_") and name.endswith("_run")) or refuses):
                continue
            if any(_is_gate(n) for n in _own_nodes(fn)):
                continue                      # the old shape; rules 2-4
            calls = _engine_calls(fn, aliases, names)
            if not calls:
                continue
            reached = {_call_name(n) for n in _own_nodes(fn)
                       if isinstance(n, ast.Call)}
            if reached & TAIL_READS and not refuses:
                # A tail routed by a chain's own question — `_sms_run` — is a
                # shape of its own until its PR (`_goal_tables_run` was the
                # other until step 14's S14-07, and refuses now).
                continue
            ok = (all(_refused_where_it_fails(c, fn, parent) for c in calls)
                  and not reached & TAIL_READS and not reached & OLD_SHAPE)
            sites.append(Site("refuse", relpath, fn.lineno, name, ok))
    return sites


def walk_tree() -> List[Site]:
    switches = read_switch_modules()
    sites: List[Site] = []
    for top in WALKED:
        for path in sorted((REPO / top).rglob("*.py")):
            if "frontend" in path.parts or "node_modules" in path.parts:
                continue
            relpath = path.relative_to(REPO).as_posix()
            sites.extend(walk_source(path.read_text(encoding="utf-8"),
                                     relpath, switches))
    return sites


@pytest.fixture(scope="module")
def sites() -> List[Site]:
    return walk_tree()


class TestTheWalk:
    def test_every_fallback_is_counted_or_re_raised(self, sites):
        bad = sorted(f"{s.rule} {s.path}:{s.line} in {s.function}"
                     for s in sites if not s.ok)
        assert not bad, (
            "a read that can fall back to DuckDB, or a router that does not "
            "refuse its engine's failure: " + "; ".join(bad))

    def test_the_routers_with_no_tail_are_found_and_refuse(self, sites):
        """Rule 5's non-vacuity: the six routers step 14's S14-05 took the
        DuckDB tail from are found by the walk — none is listed for it — and
        every one of them refuses. A router that lost its gate without
        learning to refuse would be found here and fail the walk above."""
        collapsed = {(s.path, s.function) for s in sites if s.rule == "refuse"}
        assert {
            ("core/repositories/traffic.py", "_traffic_run"),
            ("core/repositories/margin.py", "_margin_run"),
            ("core/repositories/products_intel.py", "_intel_run"),
            ("core/repositories/revenue.py", "_reports_run"),
            ("core/repositories/revenue.py", "_marketing_run"),
            ("core/repositories/revenue.py", "_lookups_run"),
            # The dashboard's, step 14's S14-06: its router, its Gold and
            # Silver helpers and the managers screen's one read — the last
            # four found because they refuse, not because of their names.
            ("core/repositories/revenue.py", "_dashboard_run"),
            ("core/repositories/revenue.py", "_pg_gold_summary"),
            ("core/repositories/revenue.py", "_pg_gold_series"),
            ("core/repositories/revenue.py", "_pg_silver"),
            ("core/repositories/revenue.py", "get_manager_sales_365d"),
            # The third set, step 14's S14-07: `/expenses`, `/inventory`,
            # `/goals` and the cohorts, whose engine is ClickHouse.
            ("core/repositories/expenses.py", "_expenses_run"),
            ("core/repositories/inventory.py", "_inventory_batch"),
            ("core/repositories/goals.py", "_goals_run"),
            ("core/repositories/goals.py", "_goal_tables_run"),
            ("core/repositories/customers.py", "_analytics_rows"),
        } <= collapsed, sorted(collapsed)

    def test_the_dashboard_file_holds_no_gate_and_no_handler(self, sites):
        """Since S14-06 nothing in `core/repositories/revenue.py` routes by a
        switch or catches a failure except to refuse it: the file appears in
        rule 5 alone."""
        rules = {s.rule for s in sites if s.path == "core/repositories/revenue.py"}
        assert rules == {"refuse"}, rules

    def test_it_finds_backfilled(self, sites):
        """The site critique 7 found by hand: neither a gate nor a log line
        with the phrase — the walk must reach it by the third rule alone."""
        assert any(s.rule == "handler" and s.path == "core/pg_expenses_read.py"
                   and s.function == "backfilled" for s in sites)

    def test_it_is_looking(self, sites):
        """Floors, not a list. Twenty gates and seventeen handlers when it
        was written; the six routers of step 14's S14-05 and the dashboard's
        six gates (S14-06) left rules 2 to 4 for rule 5 with their switches,
        and the last — `/expenses`, `/inventory`, `/goals`, the cohorts, the
        training frame, the weekly report, the assistant, the buyer-sync
        selection and the search index — with S14-07. No gate is left, and
        none may come back (rule 4, its address question, went with
        `no_address` in S14-16). What rule 3 still finds is the handlers
        around the engines in the reader modules (`backfilled()` among
        them), and rule 5 holds every router."""
        gates = {(s.path, s.function) for s in sites if s.rule == "gate"}
        assert gates == set(), sorted(gates)
        handlers = [s for s in sites if s.rule == "handler"]
        assert len(handlers) >= 1, handlers
        refusing = {(s.path, s.function) for s in sites if s.rule == "refuse"}
        assert len(refusing) >= 16, sorted(refusing)

    def test_the_read_switches_are_found_not_listed(self):
        """No module of `core/` is a read switch any more: the last
        (`pg_expenses_read`, `pg_inventory_read`, `ch_cohorts` and the five
        readers with no fallback) lost theirs in step 14's S14-07. Mutation:
        put `ENV` and `enabled` back on any of them."""
        switches = read_switch_modules()
        assert switches == set(), switches
        assert "read_fallback" not in switches
        assert "pg_sms_read" not in switches, (
            "the SMS tab fails closed on its own switch; it has no ENV here")

    def test_every_counted_surface_is_a_literal(self, sites):
        """A surface is a key in /api/health, so it must be readable from the
        code — never computed from a flag or an exception. Since step 14's
        S14-16 `refuse` is the one call that counts one."""
        surfaces = set()
        for top in WALKED:
            for path in (REPO / top).rglob("*.py"):
                for n in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                    if (isinstance(n, ast.Call) and n.args
                            and _is_read_fallback_refuse(n)):
                        arg = n.args[0]
                        if isinstance(arg, ast.Constant):
                            surfaces.add(arg.value)
                        else:
                            surfaces.add(ast.unparse(arg))
        # Every one a literal since step 14's S14-06: `_pg_silver` passed its
        # `surface` keyword on until then, and the managers screen names its
        # own now.
        literal = surfaces
        assert all(re.fullmatch(r"[a-z_]+", s) for s in literal), literal
        assert {"dashboard", "managers", "expenses", "goals", "cohorts"} <= literal
        assert {"traffic", "margin", "products_intel", "reports", "marketing",
                "lookups"} <= literal


# ─── The walk shown synthetic code, so each rule is seen to bite ───────────

class TestTheRules:
    SWITCHES = {"pg_x_read"}

    def _ok(self, source, relpath="core/repositories/x.py",
            rules=("log", "gate", "handler")):
        return [(s.rule, s.ok) for s in walk_source(source, relpath, self.SWITCHES)
                if s.rule in rules]

    def test_a_log_line_alone_is_not_counting(self):
        src = (
            "async def f(self):\n"
            "    try:\n"
            "        return await pg_x_read.fetch()\n"
            "    except Exception as exc:\n"
            "        logger.error('x: Postgres failed, falling back to DuckDB: %s', exc)\n"
            "    async with self.connection() as conn:\n"
            "        return conn.execute('x').fetchall()\n"
        )
        assert ("log", False) in self._ok(src, "core/somewhere.py")

    def test_any_gate_is_found(self):
        """Rule 2 since step 14's S14-16: a gate is a read switch choosing an
        engine, and none is left — whatever its handlers do. (Until then a
        gate whose handler counted its fallback through `fall_back`, or
        ended the step, passed.)"""
        src = (
            "async def f(self):\n"
            "    if pg_x_read.enabled() and pg_x_read.available():\n"
            "        try:\n"
            "            return await pg_x_read.fetch()\n"
            "        except Exception:\n"
            "            raise\n"
            "    return None\n"
        )
        assert self._ok(src, "core/somewhere.py") == [("gate", False)]

    def test_a_handler_returning_a_sentinel_is_judged_in_scope(self):
        """`backfilled()`'s shape: False from the handler, DuckDB in the caller."""
        src = (
            "async def backfilled():\n"
            "    try:\n"
            "        pool = await get_pool()\n"
            "    except Exception as exc:\n"
            "        logger.warning('cannot read the watermark: %s', exc)\n"
            "        return False\n"
            "    return True\n"
        )
        assert self._ok(src, "core/pg_x_read.py") == [("handler", False)]
        assert self._ok(src, "core/elsewhere.py") == [], (
            "outside core/repositories and core/pg_*_read* the third rule "
            "does not apply")

    def test_letting_read_unavailable_through_is_a_decision(self):
        src = (
            "async def f(self):\n"
            "    try:\n"
            "        return await self._goals_run('x')\n"
            "    except read_fallback.ReadUnavailable:\n"
            "        raise\n"
            "    except Exception:\n"
            "        return 0.0\n"
        )
        assert set(self._ok(src)) == {("handler", True)}
        without = src.replace("    except read_fallback.ReadUnavailable:\n"
                              "        raise\n", "")
        assert self._ok(without) == [("handler", False)]

    # Rule 5: a router with no tail.
    REFUSING = (
        "from core import read_fallback\n"
        "class M:\n"
        "    async def _x_run(self, sql):\n"
        "        from core import pg_x_read\n"
        "        try:\n"
        "            return await pg_x_read.fetch(sql)\n"
        "        except Exception as exc:  # noqa: BLE001\n"
        "            raise read_fallback.refuse('x', exc)\n"
    )

    def _refuse_ok(self, source):
        return [s.ok for s in walk_source(source, "core/repositories/x.py",
                                          self.SWITCHES) if s.rule == "refuse"]

    def test_a_router_that_refuses_passes(self):
        assert self._refuse_ok(self.REFUSING) == [True]

    def test_an_engine_call_outside_the_try_is_found(self):
        """The bare `await` the plan names: an uncounted 500, not a 503."""
        src = self.REFUSING.replace(
            "        try:\n",
            "        rows = await pg_x_read.fetch('SELECT 1')\n        try:\n")
        assert self._refuse_ok(src) == [False]

    # Every spelling of an engine call but `<alias>.<fn>(...)` and a bare
    # `get_pool(...)`, which the rule alone judged until the review of step
    # 14's S14-06 merge: the first passed a `fetch_series()` imported by its
    # name, outside the `try`, in a dashboard helper.
    SPELLINGS = [
        ("        from core.pg_x_read import fetch_y\n", "fetch_y('SELECT 1')"),
        ("        from core.pg_x_read import fetch_y as f\n", "f('SELECT 1')"),
        ("        from core.pg import get_pool as gp\n", "gp()"),
        ("        from core import pg\n", "pg.get_pool()"),
        ("        import core.pg_x_read\n", "core.pg_x_read.fetch('SELECT 1')"),
        # ClickHouse's modules, the cohorts' one engine since step 14's
        # S14-07, in each spelling: S14-07 taught the rule the module's
        # alias, and the merge of S14-06's review the rest.
        ("        from core import ch_x as c\n", "c.fetch('SELECT 1')"),
        ("        from core.ch_x import fetch\n", "fetch('SELECT 1')"),
        ("        import core.ch_x\n", "core.ch_x.fetch('SELECT 1')"),
    ]
    SPELLING_IDS = ["imported_name", "imported_alias", "get_pool_alias",
                    "get_pool_attribute", "spelled_out", "clickhouse_alias",
                    "clickhouse_imported_name", "clickhouse_spelled_out"]

    @pytest.mark.parametrize("binding,call", SPELLINGS, ids=SPELLING_IDS)
    def test_every_spelling_outside_the_try_is_found(self, binding, call):
        src = self.REFUSING.replace(
            "        try:\n", f"{binding}        rows = await {call}\n        try:\n")
        assert self._refuse_ok(src) == [False]

    @pytest.mark.parametrize("binding,call", SPELLINGS, ids=SPELLING_IDS)
    def test_every_spelling_inside_the_try_is_refused(self, binding, call):
        src = self.REFUSING.replace(
            "        try:\n", f"{binding}        try:\n            await {call}\n")
        assert self._refuse_ok(src) == [True]

    def test_a_name_imported_at_the_top_is_read_too(self):
        src = ("from core.pg_x_read import fetch_y\n"
               + self.REFUSING.replace(
                   "        try:\n", "        rows = await fetch_y(sql)\n        try:\n"))
        assert self._refuse_ok(src) == [False]

    @pytest.mark.parametrize("handler", [
        "            return []\n",
        "            read_fallback.fall_back('x', exc)\n",
        "            raise\n",
        "            raise read_fallback.ReadUnavailable('x')\n",
        "            raise read_fallback.refuse(surface, exc)\n",
        "            raise read_fallback.refuse('x', None)\n",
        "            raise read_fallback.refuse('x', exc) from exc\n",
    ], ids=["returns", "falls_back", "re_raises", "no_count", "computed_surface",
            "no_cause", "from"])
    def test_any_other_handler_is_found(self, handler):
        src = self.REFUSING.replace(
            "            raise read_fallback.refuse('x', exc)\n", handler)
        assert self._refuse_ok(src) == [False]

    def test_a_narrow_handler_does_not_refuse_everything(self):
        src = self.REFUSING.replace("except Exception as exc", "except OSError as exc")
        assert self._refuse_ok(src) == [False]

    def test_a_duckdb_tail_after_the_refusal_is_found(self):
        src = self.REFUSING + (
            "        async with self.connection() as conn:\n"
            "            return conn.execute(sql).fetchall()\n")
        assert self._refuse_ok(src) == [False]

    def test_a_chain_routed_tail_is_left_to_its_own_pr(self):
        """`_sms_run`'s shape: no gate, a branch on the chain's own question
        and a DuckDB tail — no refusal anywhere, so not this rule's yet."""
        src = (
            "class M:\n"
            "    async def _x_run(self, sql):\n"
            "        from core import pg_x_read\n"
            "        if store_is_postgres():\n"
            "            return await pg_x_read.fetch(sql)\n"
            "        async with self.connection() as conn:\n"
            "            return conn.execute(sql).fetchall()\n"
        )
        assert self._refuse_ok(src) == []

    def test_a_gated_router_is_left_to_the_old_rules(self):
        src = (
            "class M:\n"
            "    async def _x_run(self, sql):\n"
            "        from core import pg_x_read\n"
            "        if pg_x_read.enabled() and pg_x_read.available():\n"
            "            return await pg_x_read.fetch(sql)\n"
            "        return []\n"
        )
        assert self._refuse_ok(src) == []

    def test_any_function_that_refuses_is_held_to_it_whatever_its_name(self):
        """The dashboard's `_pg_gold_summary` and `get_manager_sales_365d` are
        not `_*_run` (step 14, S14-06). A refusal anywhere makes the function
        this rule's: the clean shape passes, a second engine call outside
        the `try` is found, and a name that neither ends in `_run` nor
        refuses is not this rule's at all."""
        clean = self.REFUSING.replace("_x_run", "get_x")
        assert self._refuse_ok(clean) == [True]
        bare = clean.replace(
            "        try:\n",
            "        rows = await pg_x_read.fetch('SELECT 1')\n        try:\n")
        assert self._refuse_ok(bare) == [False]
        silent = clean.replace(
            "            raise read_fallback.refuse('x', exc)\n", "            return []\n")
        assert self._refuse_ok(silent) == []

    def test_a_switch_function_imported_by_name_is_reaching(self):
        src = (
            "from core.pg_x_read import fetch\n"
            "async def f():\n"
            "    try:\n"
            "        return await fetch()\n"
            "    except Exception:\n"
            "        return None\n"
        )
        assert self._ok(src) == [("handler", False)]


# ─── The mode, the counter, the startup check ──────────────────────────────

@pytest.fixture
def fresh_read_fallback(monkeypatch):
    """The refusal counts start from zero and are forgotten after the test,
    and every `KS_READ_*` is unset — none is read since step 14's S14-07, so
    a test that sets one asks nothing of it. (The fixture saved and restored
    the mode's caches until S14-16 deleted them.)"""
    for name in [n for n in __import__("os").environ if n.startswith("KS_READ_")]:
        monkeypatch.delenv(name, raising=False)
    read_fallback.reset_counts()
    yield read_fallback
    read_fallback.reset_counts()


# ─── backfilled(): the site nobody's grep found ────────────────────────────

class TestBackfilledCounts:
    @pytest.fixture(autouse=True)
    def _fresh(self, fresh_read_fallback):
        from core import pg_expenses_read as r

        before = (r._backfilled, r._checked_at, r._check_failed)
        r._backfilled, r._checked_at, r._check_failed = False, 0.0, None
        yield r
        r._backfilled, r._checked_at, r._check_failed = before


    @pytest.mark.asyncio
    async def test_no_history_yet_is_routing_not_a_fallback(self, _fresh):
        conn = MagicMock()
        conn.fetchval = AsyncMock(return_value=False)
        acquire = MagicMock()
        acquire.__aenter__ = AsyncMock(return_value=conn)
        acquire.__aexit__ = AsyncMock(return_value=False)
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=acquire)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            assert await _fresh.backfilled() is False
            assert await _fresh.backfilled() is False
        assert "expenses" not in read_fallback.refusals()


# ─── The one handler around a router: ReadUnavailable passes through ───────

class TestTheForecastSignalLetsARefusalThrough:
    @pytest.mark.asyncio
    async def test_read_unavailable_is_not_a_missing_signal(self):
        from core.repositories.goals import GoalsMixin

        store = GoalsMixin.__new__(GoalsMixin)
        with patch.object(GoalsMixin, "_goals_run",
                          new=AsyncMock(side_effect=read_fallback.ReadUnavailable("goals"))), \
             patch.object(GoalsMixin, "_forecast_actual_revenue",
                          new=AsyncMock(return_value=0.0)):
            with pytest.raises(read_fallback.ReadUnavailable):
                await store._get_ml_forecast_total(2025, 3, "retail")

    @pytest.mark.asyncio
    async def test_any_other_failure_is_still_left_out(self):
        from core.repositories.goals import GoalsMixin

        store = GoalsMixin.__new__(GoalsMixin)
        with patch.object(GoalsMixin, "_goals_run",
                          new=AsyncMock(side_effect=RuntimeError("duckdb gone"))), \
             patch.object(GoalsMixin, "_forecast_actual_revenue",
                          new=AsyncMock(return_value=0.0)):
            assert await store._get_ml_forecast_total(2025, 3, "retail") == 0.0


# ─── Through the app ───────────────────────────────────────────────────────

@pytest.fixture
def admin_client(monkeypatch):
    import time

    from fastapi.testclient import TestClient

    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.ratelimit import limiter
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    admin = sorted(ADMIN_USER_IDS)[0]

    async def _resolve(session):
        return {"user_id": admin, "role": "admin"}

    monkeypatch.setattr("web.routes.auth._resolve_session", _resolve)
    limiter.reset()
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
        {"id": str(admin), "first_name": "T", "last_name": "U", "username": "t",
         "auth_date": str(int(time.time()))}, role="admin")))
    yield client
    limiter.reset()


class TestThroughTheApp:

    def test_nothing_fell_back_is_an_empty_block(self, admin_client, fresh_read_fallback):
        """Since step 14's S14-16 the block is the refusals alone, and
        `read_fallbacks` — the reads answered from DuckDB, none since S14-00
        — is gone. Mutation: publish `mode`, `misconfigured` or the old
        block again."""
        health = admin_client.get("/api/health").json()
        assert "read_fallbacks" not in health
        assert health["read_fallback_mode"] == {"refused": {}}

