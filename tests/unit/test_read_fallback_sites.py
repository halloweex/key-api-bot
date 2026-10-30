"""DN-20a: every read that falls back to DuckDB is counted, and found by a walk.

`core.read_fallback.fall_back` is the one place a fallback is counted and
logged. What makes it the *one* place is the walk below, which reads `core/`
and `web/` for the three shapes a fallback to DuckDB takes rather than
trusting a list of routers — a list only ever guards the routers somebody was
already thinking about, and `pg_expenses_read.backfilled()` is the proof: it
served DuckDB for weeks under log text no grep for "falling back to DuckDB"
could find, because it was neither a router nor a gate.

THE THREE SHAPES, AND WHAT EACH MUST DO

1. **A log call saying "falling back to DuckDB"** (docstrings are not calls,
   so they do not count). The handler it sits in must call `fall_back` or
   re-raise. After this change the only such call left is `fall_back`'s own.
2. **An `X.enabled() and X.available()` gate** — the shape every read switch
   uses to choose an engine. In the gate's function, every except handler that
   carries on toward DuckDB — it falls through past its `try`, or reads DuckDB
   itself — must call `fall_back` or re-raise. A handler that *returns* ends
   the function; in `sync_to_meilisearch` that is "skip the step", not DuckDB.
3. **Any except handler in a function that reaches another engine's read** —
   `get_pool`, a `*_run` router, or a function of a read-switch module — under
   `core/repositories/` or `core/pg_*_read*.py`. Here a handler that returns
   is judged too: `backfilled()` returns False and `_pg_gold_summary` returns
   None, and it is the *caller* that then reads DuckDB — invisible from the
   handler, so every handler in that scope has to decide.

"Re-raise" includes deciding for `ReadUnavailable` in the same `try`: that is
what DN-20b and DN-20c raise from `fall_back`, and a broad handler around a
router must not turn the refusal into a quiet answer.

4. **Every gate asks whether its switch has an address** (DN-20b). The
   function holding an `X.enabled() and X.available()` gate must call
   `read_fallback.no_address(surface, X)` with that same `X` — or
   `no_engine`, the cohorts' stronger form. That is the one route to DuckDB
   no exception marks: the switch names Postgres, no DSN is set, and the gate
   is simply false. Under `duckdb` the call does nothing; under `off` it
   refuses, so a lost DSN line is a 503 and not frozen numbers.

5. **A router with no DuckDB tail refuses every engine failure** (step 14,
   S14-05). A `_*_run` router under `core/repositories/` with no gate and
   no DuckDB tail left — `_traffic_run`, `_margin_run`, `_intel_run`,
   `_reports_run`, `_marketing_run` and `_lookups_run` first, the
   dashboard's `_dashboard_run` since S14-06 — or any function there that
   calls `refuse` at all (the dashboard's `_pg_gold_summary`,
   `_pg_gold_series`, `_pg_silver` and `get_manager_sales_365d`), must run
   every engine call
   (a function of a `core.pg_*` module it imports, or `get_pool`) inside a
   `try` whose handler for `Exception` is exactly `raise
   read_fallback.refuse("<surface>", <the caught exception>)`, and reach
   DuckDB through nothing at all. One bare `await pg_x.fetch()` outside the
   `try` would turn the counted 503 naming the surface into an uncounted
   500; a handler that falls back, returns or raises something else would
   lose the surface or the count.

A read-switch module is one whose `ENV` names a `KS_READ_*` variable, found by
walking `core/` — not listed either.

Two fallbacks are deliberately not counted per request, and each has another
answer: a gate whose switch is on and whose engine has no address
(`KS_READ_GOLD=postgres` without `KS_PG_DSN`) is a configuration, published at
startup under `read_fallback_mode.misconfigured` — and, under `off`, refused
by rule 4; and `backfilled()` answering "no history yet" is a correct routing
decision, not a failure.
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
# itself. Deliberately loose: a false positive costs a `fall_back` decision
# somebody has to write, a false negative costs a silent fallback.
DUCKDB_READS = {"connection", "get_store", "_fetch_one", "_fetch_all"}
EXITS = (ast.Return, ast.Raise, ast.Continue, ast.Break)


# ─── The walk ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Site:
    rule: str          # "log" | "gate" | "handler" | "address"
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
    """The handler counts its fallback, re-raises, or the `try` it belongs to
    has already said what `ReadUnavailable` means."""
    for n in _own_nodes(handler):
        if isinstance(n, ast.Call) and _call_name(n) == "fall_back":
            return True
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


def _carries_on_to_duckdb(handler: ast.ExceptHandler) -> bool:
    if not handler.body or not isinstance(handler.body[-1], EXITS):
        return True
    return any(isinstance(n, ast.Call) and _call_name(n) in DUCKDB_READS
               for n in _own_nodes(handler))


def _is_gate(node: ast.AST) -> bool:
    if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.And)):
        return False
    called = {_call_name(n) for v in node.values for n in ast.walk(v)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    return {"enabled", "available"} <= called


def _gate_switches(gate: ast.AST) -> Set[str]:
    """The names a gate calls `enabled()` on — `pg_traffic_read`, `pg_index`."""
    return {n.func.value.id for n in ast.walk(gate)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "enabled" and isinstance(n.func.value, ast.Name)}


def _asks_for_an_address(fn: ast.AST, switches: Set[str]) -> bool:
    """`fn` calls `no_address(<surface>, X)` for a switch X its gate consults,
    or `no_engine`, which refuses whatever the switch says."""
    for n in _own_nodes(fn):
        if not isinstance(n, ast.Call):
            continue
        name = _call_name(n)
        if name == "no_engine":
            return True
        if (name == "no_address" and len(n.args) >= 2
                and isinstance(n.args[1], ast.Name) and n.args[1].id in switches):
            return True
    return False


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
OLD_SHAPE = {"fall_back", "no_address", "no_engine", "enabled", "available"}
BROAD = {"Exception", "BaseException"}


def _engine_aliases(tree: ast.AST) -> Set[str]:
    """Every name the file binds to a `core.pg_*` module, at the top or
    inside a function: `from core import pg_traffic_read`, `import
    core.pg_x as y`."""
    found = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            for a in n.names:
                if a.name.startswith("pg_"):
                    found.add(a.asname or a.name)
        elif isinstance(n, ast.Import):
            for a in n.names:
                if a.name.startswith("core.pg_") and a.asname:
                    found.add(a.asname)
    return found


def _engine_calls(fn: ast.AST, aliases: Set[str]) -> List[ast.Call]:
    out = []
    for n in _own_nodes(fn):
        if not isinstance(n, ast.Call):
            continue
        if isinstance(n.func, ast.Name) and n.func.id == "get_pool":
            out.append(n)
        elif (isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)
              and n.func.value.id in aliases):
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
        if relpath == "core/read_fallback.py" and name == "fall_back":
            continue  # the one uniform line
        handler = enclosing(call, (ast.ExceptHandler,))
        if handler is not None and (fn is None or enclosing(handler, (
                ast.FunctionDef, ast.AsyncFunctionDef)) is fn):
            ok = _decides(handler, try_of(handler))
        else:
            ok = fn is not None and any(
                isinstance(n, ast.Call) and _call_name(n) == "fall_back"
                for n in _own_nodes(fn))
        sites.append(Site("log", relpath, call.lineno, name, ok))

    # 2. An enabled()-and-available() gate.
    for gate in (n for n in ast.walk(tree) if _is_gate(n)):
        fn = enclosing(gate, (ast.FunctionDef, ast.AsyncFunctionDef))
        if fn is None:
            sites.append(Site("gate", relpath, gate.lineno, "<module>", False))
            continue
        judged = [(h, t) for h, t in _handlers(fn) if _carries_on_to_duckdb(h)]
        ok = all(_decides(h, t) for h, t in judged)
        sites.append(Site("gate", relpath, gate.lineno, fn.name, ok))
        # 4. The same gate, asked whether its switch has an address.
        sites.append(Site("address", relpath, gate.lineno, fn.name,
                          _asks_for_an_address(fn, _gate_switches(gate))))

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
        aliases = _engine_aliases(tree)
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
            calls = _engine_calls(fn, aliases)
            if not calls:
                continue
            reached = {_call_name(n) for n in _own_nodes(fn)
                       if isinstance(n, ast.Call)}
            if reached & TAIL_READS and not refuses:
                # A tail routed by a chain's own question — `_sms_run`,
                # `_goal_tables_run` — is a shape of its own until its PR.
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
            "a read that can fall back to DuckDB without core.read_fallback."
            "fall_back counting it, and without re-raising: " + "; ".join(bad))

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
        """Floors, not a list: the walk found the gates and the handlers it
        exists for. Twenty gates and seventeen handlers when it was written;
        twenty-three gates once the two Gold readers took the shape (DN-20b),
        and every gate is also an address site. The dashboard's six gates —
        its router, the two Gold readers, the Silver helper and the two
        inline in `get_summary_stats` and `get_revenue_trend` — left with
        their switches in step 14's S14-06 (rule 5 holds them now)."""
        gates = {(s.path, s.function) for s in sites if s.rule == "gate"}
        assert gates == {(s.path, s.function) for s in sites
                         if s.rule == "address"}
        handlers = [s for s in sites if s.rule == "handler"]
        assert len(gates) >= 10, sorted(gates)
        # Fourteen since step 14's S14-05, whose six routers left rule 3 for
        # rule 5 with their switches (`test_the_routers_with_no_tail_...`);
        # ten since S14-06, whose dashboard handlers did the same.
        assert len(handlers) >= 9, handlers
        assert {"core/repositories", "core/chat_tools.py",
                "core/sync_service.py"} <= {
            p if not p.startswith("core/repositories/") else "core/repositories"
            for p, _ in gates}

    def test_the_read_switches_are_found_not_listed(self):
        switches = read_switch_modules()
        # `pg_gold_read` was the example until step 14's S14-06 took its
        # switch; `pg_inventory_read` still reads one.
        assert {"pg_expenses_read", "pg_inventory_read", "ch_cohorts"} <= switches
        assert not switches & {"pg_gold_read", "pg_silver_read", "pg_dashboard_read"}
        assert "read_fallback" not in switches
        assert "pg_sms_read" not in switches, (
            "the SMS tab fails closed on its own switch; it has no ENV here")

    def test_every_counted_surface_is_a_literal(self, sites):
        """A surface is a key in /api/health, so it must be readable from the
        code — never computed from a flag or an exception. The refusals
        (`no_address`, `no_engine`) are keys there too, under `off`."""
        surfaces = set()
        for top in WALKED:
            for path in (REPO / top).rglob("*.py"):
                for n in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                    if (isinstance(n, ast.Call) and n.args and (
                            _call_name(n) in (
                                "fall_back", "no_address", "no_engine",
                                "chain_refusal")
                            or _is_read_fallback_refuse(n))):
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

    def test_a_gate_whose_handler_falls_through_must_count(self):
        src = (
            "async def f(self):\n"
            "    if pg_x_read.enabled() and pg_x_read.available():\n"
            "        try:\n"
            "            return await pg_x_read.fetch()\n"
            "        except Exception:\n"
            "            pass\n"
            "    async with self.connection() as conn:\n"
            "        return conn.execute('x').fetchall()\n"
        )
        assert self._ok(src, "core/somewhere.py") == [("gate", False)]
        counted = src.replace("            pass\n",
                              "            read_fallback.fall_back('x', None)\n")
        assert self._ok(counted, "core/somewhere.py") == [("gate", True)]

    def test_a_gate_whose_handler_ends_the_step_is_not_a_fallback(self):
        """`sync_to_meilisearch`'s shape: the handler returns, nothing after it
        reads DuckDB, and outside the handler scope that is the end of it."""
        src = (
            "async def f(self):\n"
            "    try:\n"
            "        use_pg = pg_x_read.enabled() and pg_x_read.available()\n"
            "        return await (pg_x_read.fetch() if use_pg else g())\n"
            "    except Exception as e:\n"
            "        logger.error('sync error: %s', e)\n"
            "        return {}\n"
        )
        assert self._ok(src, "core/sync_service.py") == [("gate", True)]

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

    def test_a_gate_must_ask_whether_its_switch_has_an_address(self):
        """Rule 4 (DN-20b): the gate is false when the switch names Postgres
        and no DSN is set, and DuckDB answers with nothing to count."""
        src = (
            "async def f(self):\n"
            "    if pg_x_read.enabled() and pg_x_read.available():\n"
            "        return await pg_x_read.fetch()\n"
            "    async with self.connection() as conn:\n"
            "        return conn.execute('x').fetchall()\n"
        )
        assert self._ok(src, "core/somewhere.py", rules=("address",)) == [
            ("address", False)]
        asked = src.replace(
            "    if pg_x_read", "    read_fallback.no_address('x', pg_x_read)\n"
                              "    if pg_x_read")
        assert self._ok(asked, "core/somewhere.py", rules=("address",)) == [
            ("address", True)]

    def test_the_address_asked_for_is_the_gates_own_switch(self):
        """A copied line naming another tab's switch asks the wrong question:
        this gate's switch could be the one without an address."""
        src = (
            "async def f(self):\n"
            "    read_fallback.no_address('x', pg_y_read)\n"
            "    if pg_x_read.enabled() and pg_x_read.available():\n"
            "        return await pg_x_read.fetch()\n"
            "    return None\n"
        )
        assert self._ok(src, "core/somewhere.py", rules=("address",)) == [
            ("address", False)]

    def test_no_engine_answers_rule_four(self):
        """The cohorts' form: refused whatever the switch says."""
        src = (
            "async def f(self):\n"
            "    if ch_x.enabled() and ch_x.available():\n"
            "        return await ch_x.fetch()\n"
            "    else:\n"
            "        read_fallback.no_engine('cohorts', 'no clickhouse')\n"
            "    return None\n"
        )
        assert self._ok(src, "core/somewhere.py", rules=("address",)) == [
            ("address", True)]

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
    """The module's caches, restored after the test; counts start from zero.
    `configure_modes` sets the derivation mode too, so that is kept as well."""
    from core import pg_derivation

    before = (read_fallback._mode, read_fallback._mode_error,
              list(read_fallback._misconfigured), list(read_fallback._unreported),
              read_fallback._configured)
    derivation = (pg_derivation._mode, pg_derivation._mode_error)
    for name in [n for n in __import__("os").environ if n.startswith("KS_READ_")]:
        monkeypatch.delenv(name, raising=False)
    read_fallback._mode, read_fallback._mode_error = None, None
    read_fallback._misconfigured = []
    read_fallback._unreported = []
    read_fallback._configured = False
    read_fallback.reset_counts()
    yield read_fallback
    pg_derivation._mode, pg_derivation._mode_error = derivation
    read_fallback._mode, read_fallback._mode_error = before[0], before[1]
    read_fallback._misconfigured, read_fallback._unreported = before[2], before[3]
    read_fallback._configured = before[4]
    read_fallback.reset_counts()


class TestMisconfiguredReads:
    def test_a_switch_naming_postgres_without_a_dsn_is_published(
        self, fresh_read_fallback, monkeypatch, caplog,
    ):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        monkeypatch.setenv("KS_READ_INVENTORY", "duckdb")
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            fresh_read_fallback.configure_mode()
        assert fresh_read_fallback.misconfigured() == [
            "KS_READ_GOLD=postgres without KS_PG_DSN"]
        assert "KS_READ_GOLD=postgres without KS_PG_DSN" in caplog.text

    def test_a_configure_that_does_not_report_leaves_the_line_to_the_report(
        self, fresh_read_fallback, monkeypatch, caplog,
    ):
        """`configure_modes()` configures without reporting and reports after
        stage 5's verdict, which decides what the line says. The cause is
        logged once, by the report, and a later configure does not log it
        again. Mutation: drop a cause the second unreported configure no
        longer finds new."""
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            fresh_read_fallback.configure_mode(report=False)
            fresh_read_fallback.configure_mode(report=False)
            assert "KS_READ_GOLD" not in caplog.text
            fresh_read_fallback.report_misconfigured()
            fresh_read_fallback.configure_mode()
            fresh_read_fallback.report_misconfigured()
        lines = [r.getMessage() for r in caplog.records if "KS_READ_GOLD" in r.getMessage()]
        # Refused: since step 14's S14-00 the mode is `off` whatever
        # KS_READ_FALLBACK says, and the line says what this process does.
        assert lines == ["KS_READ_GOLD=postgres without KS_PG_DSN — every read "
                         "behind it is refused (KS_READ_FALLBACK=off). Set the "
                         "address or unset the flag."], lines

    def test_clickhouse_needs_its_url(self, fresh_read_fallback, monkeypatch):
        monkeypatch.delenv("KS_CH_URL", raising=False)
        monkeypatch.setenv("KS_READ_COHORTS", "clickhouse")
        fresh_read_fallback.configure_mode()
        assert fresh_read_fallback.misconfigured() == [
            "KS_READ_COHORTS=clickhouse without KS_CH_URL"]

    def test_with_the_address_nothing_is_published(self, fresh_read_fallback, monkeypatch):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        monkeypatch.setenv("KS_CH_URL", "http://ch:8123")
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        monkeypatch.setenv("KS_READ_COHORTS", "clickhouse")
        fresh_read_fallback.configure_mode()
        assert fresh_read_fallback.misconfigured() == []

    def test_every_read_switch_in_the_code_is_covered(self, fresh_read_fallback, monkeypatch):
        """The check scans the environment by prefix, so a switch added later
        is covered the day it is set. Shown against every switch the walk
        finds, so the prefix and the modules cannot drift apart."""
        import importlib

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.delenv("KS_CH_URL", raising=False)
        envs = {}
        for name in read_switch_modules():
            module = importlib.import_module(f"core.{name}")
            engine = "clickhouse" if name.startswith("ch_") else "postgres"
            envs[module.ENV] = engine
            monkeypatch.setenv(module.ENV, engine)
        fresh_read_fallback.configure_mode()
        published = {line.split("=", 1)[0] for line in fresh_read_fallback.misconfigured()}
        assert published == set(envs)

    def test_every_published_switch_is_refused_under_off(
        self, fresh_read_fallback, monkeypatch, caplog,
    ):
        """What is published under `misconfigured` and what `no_address`
        refuses are one rule, so under `off` the two lists are the same list
        — shown for every switch the walk finds, Postgres and ClickHouse."""
        import importlib

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.delenv("KS_CH_URL", raising=False)
        modules = []
        for name in sorted(read_switch_modules()):
            module = importlib.import_module(f"core.{name}")
            monkeypatch.setenv(module.ENV, "clickhouse" if name.startswith("ch_")
                               else "postgres")
            modules.append(module)
        monkeypatch.setenv("KS_READ_FALLBACK", "off")
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            fresh_read_fallback.configure_mode()
        assert "every read behind it is refused" in caplog.text
        for module in modules:
            with pytest.raises(fresh_read_fallback.ReadUnavailable):
                fresh_read_fallback.no_address(module.__name__, module)
        assert set(fresh_read_fallback.refusals()) == {m.__name__ for m in modules}
        assert len(fresh_read_fallback.misconfigured()) == len(modules)


class TestNoAddress:
    """`no_address`: refused under `off` only, and only for a switch naming an
    engine this process has no address for (DN-20b)."""


    def test_under_off_only_the_unaddressed_switch_is_refused(
        self, fresh_read_fallback, monkeypatch, caplog,
    ):
        # A switch still read: `KS_READ_TRAFFIC` is not, since step 14's
        # S14-05, and its router refuses a failure without asking this.
        from core import ch_cohorts, pg_inventory_read

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.delenv("KS_CH_URL", raising=False)
        monkeypatch.setenv("KS_READ_FALLBACK", "off")
        fresh_read_fallback.configure_mode()

        # Unset, or naming DuckDB: routing, never refused here.
        fresh_read_fallback.no_address("inventory", pg_inventory_read)
        monkeypatch.setenv("KS_READ_INVENTORY", "duckdb")
        fresh_read_fallback.no_address("inventory", pg_inventory_read)
        assert fresh_read_fallback.refusals() == {}

        # Naming Postgres with no DSN: refused, and the log says which switch.
        monkeypatch.setenv("KS_READ_INVENTORY", "postgres")
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            with pytest.raises(fresh_read_fallback.ReadUnavailable) as raised:
                fresh_read_fallback.no_address("inventory", pg_inventory_read)
        assert raised.value.surface == "inventory"
        assert "KS_READ_INVENTORY=postgres without KS_PG_DSN" in caplog.text
        assert "falling back to DuckDB" not in caplog.text
        assert ": None" not in caplog.text
        assert fresh_read_fallback.refusals()["inventory"]["count"] == 1

        # With the address the gate will ask Postgres: nothing to refuse.
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        fresh_read_fallback.no_address("inventory", pg_inventory_read)
        assert fresh_read_fallback.refusals()["inventory"]["count"] == 1

        # ClickHouse by its own address.
        monkeypatch.setenv("KS_READ_COHORTS", "clickhouse")
        with pytest.raises(fresh_read_fallback.ReadUnavailable):
            fresh_read_fallback.no_address("cohorts", ch_cohorts)
        monkeypatch.setenv("KS_CH_URL", "http://ch:8123")
        fresh_read_fallback.no_address("cohorts", ch_cohorts)
        assert fresh_read_fallback.refusals()["cohorts"]["count"] == 1


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
        assert "expenses" not in read_fallback.counts()


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


# ─── The canary ────────────────────────────────────────────────────────────

class TestTheCanary:
    def test_an_error_is_a_failure_key(self):
        from bot.canary import check_read_fallback_mode

        payload = {"read_fallback_mode": {
            "mode": "duckdb", "error": "KS_READ_FALLBACK='of' is not one of ('duckdb', 'off')",
            "misconfigured": []}}
        assert [k for k, _ in check_read_fallback_mode(payload)] == [
            "read_fallback_mode_invalid"]

    def test_absent_or_clean_is_quiet(self):
        from bot.canary import check_read_fallback_mode

        assert check_read_fallback_mode({}) == []
        assert check_read_fallback_mode({"read_fallback_mode": {
            "mode": "off", "error": None, "misconfigured": []}}) == []

    @pytest.mark.asyncio
    async def test_the_wiring_warns(self):
        """Not only the judge: a typo reaches a failure key and a warn, and
        names its lever."""
        from datetime import datetime, timezone

        import httpx

        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

        payload = _healthy_payload()
        payload["read_fallback_mode"] = {
            "mode": "duckdb", "error": "KS_READ_FALLBACK='of' is not one of ('duckdb', 'off')",
            "misconfigured": []}

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(handler) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.severity == "warn"
        assert result.failure_keys == ["read_fallback_mode_invalid"]
        assert "KS_READ_FALLBACK" in canary._what_to_do(result)


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
        health = admin_client.get("/api/health").json()
        assert health["read_fallbacks"] == {}
        # `fresh_read_fallback` unsets every KS_READ_*, so the cohorts are on
        # DuckDB here — the route `off` refuses, published as such. Since step
        # 14's S14-00 the mode is `off` whatever KS_READ_FALLBACK says, so the
        # block carries its refusals too.
        assert health["read_fallback_mode"] == {
            "mode": "off", "error": None, "misconfigured": [], "refused": {},
            "no_engine": ["cohorts: KS_READ_COHORTS=duckdb, and only clickhouse "
                          "may answer it under KS_READ_FALLBACK=off"]}

