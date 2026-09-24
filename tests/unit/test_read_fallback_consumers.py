"""Where a refusal goes when no swept HTTP request is waiting for it (DN-20b).

Under `KS_READ_FALLBACK=off` a router raises `ReadUnavailable`, and one place
raises for every caller. An HTTP GET route turns it into a 503 naming the
surface, and `tests/unit/test_read_fallback_http.py` proves that for every
such route by running it. Everything else that can reach a refusing router is
DN-20c's to answer, and until DN-20c lands a refusal arriving there is an
exception like any other. That list used to be remembered — "the weekly
reports, the assistant, training, the sync" — and it missed the Monday goals
job: `seasonality_calc` reaches `_goals_run` through
`calculate_suggested_goals`, and under `off` with Postgres down it wrote
`seasonal_indices`, then raised before `calculate_yoy_growth` ran, leaving
the shared goal tables half updated.

So the list is derived here. The walk starts at every function that refuses —
a call to `fall_back`, `no_address` or `no_engine` — and follows callers up
through the repository until it reaches an **entry point**: a function nothing
in the repository calls, because a scheduler, an executor, an HTTP route or a
script's `__main__` invokes it by reference. Each entry point is then one of:

- a GET route under `/api/` that the sweep runs — proved there, not here;
- another HTTP route (`POST`, `DELETE`) — answered by the same 503 handler,
  but not swept, because a sweep that writes is not a probe;
- an entry that is no HTTP request's at all — a scheduler job, the boot sync,
  the assistant, a script. These are DN-20c's, pinned below.

Both pins are **derived and then compared**: a new consumer fails this test
until somebody writes it down, which is the moment to decide what it answers.

WHAT THE WALK RESOLVES, AND WHAT IT CANNOT

Calls are resolved, not matched by name alone: a name like `train` or `_get`
means five things in this repository. `self.x()` inside the store's mixins is
any mixin's `x` — they compose one class; elsewhere it is the class's own
method. `x()` is the module's function, or the one a `from core.m import x`
names — function-local imports included, which is where most of them are
here. `m.x()` follows an imported repository module. A store method is
reached through a receiver called `store` (`store.x()`, `self.store.x()`,
`get_store().x()`). Any other `obj.x()` resolves only when exactly one class
in the repository defines `x` and `obj` is not an imported module
(`lgb.train` is not `PredictionService.train`). A method nothing calls in
the router layer itself is not an entry point but dead code, and is not
listed.

What it cannot see is a call by `getattr` or through a dispatch table.
`chat_tools.execute_tool` dispatches its tools by name and is itself resolved;
if a consumer ever hides behind a table the walk cannot read, it shows up as
the function that owns the table, which is still the right place to answer.

ONLY WEB, THE SCHEDULER AND THREE SCRIPTS CAN REFUSE AT ALL

A refusal happens only in a process that read `KS_READ_FALLBACK`, and that is
whoever calls `core.runtime_modes.configure_modes()`: web's startup, the
scheduler's start, and the scripts that call it themselves. The bot never
does — it has no routers to refuse — so `bot/` is outside the walk, and a test
below fails the day it starts to.
"""
from __future__ import annotations

import ast
import inspect
import pathlib
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
WALKED = ("core", "web", "scripts")
REFUSERS = {"fall_back", "no_address", "no_engine"}
# A receiver the store is reached through: `store`, `self.store`,
# `self._store`, `get_store()`, `(await get_store())`.
STORE_RECEIVER = re.compile(r"(^|[._])store$|get_store\(\)")


# ─── The pins ─────────────────────────────────────────────────────────────

# DN-20c's list: every entry point a refusal can reach that is no HTTP
# request's. Each must get an answer of its own — defer, keep the previous
# model, skip the step for the tick — and none may half-write on the way.
NON_HTTP_CONSUMERS = {
    # The sync, every 2 minutes, and its search index.
    "core/scheduler.py:BackgroundScheduler._run_incremental_sync",
    "core/scheduler.py:BackgroundScheduler._run_meilisearch_sync",
    # Training, and the Monday goals job that writes seasonal_indices before
    # the read that refuses.
    "core/scheduler.py:BackgroundScheduler._run_revenue_prediction",
    "core/scheduler.py:BackgroundScheduler._run_seasonality_calc",
    # The two weekly messages.
    "core/scheduler.py:BackgroundScheduler._run_traffic_report",
    "core/scheduler.py:BackgroundScheduler._run_weekly_report",
    # The boot sync, before web serves anything.
    "web/main.py:startup_event",
    # The assistant: its tools run inside a conversation, where a 503 cannot
    # be sent mid-stream (and /api/chat/stream is not swept, for the network).
    "web/services/chat_service.py:ChatService.chat",
    "web/services/chat_service.py:ChatService.chat_stream",
}

# HTTP, so the 503 handler answers them — but each writes, so the sweep does
# not run them, and a broad handler on their path is not proved absent.
UNSWEPT_ROUTES = {
    "DELETE /api/goals/{period_type}",
    "POST /api/duckdb/sync-buyers",
    "POST /api/goals",
    "POST /api/revenue/forecast/train",
    "POST /api/revenue/forecast/tune",
}


# ─── The walk ─────────────────────────────────────────────────────────────

@dataclass(eq=False)
class Fn:
    rel: str
    cls: Optional[str]
    node: ast.AST
    outer: Optional["Fn"]
    name: str = field(init=False)

    def __post_init__(self):
        self.name = self.node.name

    @property
    def qual(self) -> str:
        return f"{self.cls}.{self.name}" if self.cls else self.name

    @property
    def key(self) -> str:
        return f"{self.rel}:{self.qual}"

    def __repr__(self) -> str:
        return self.key


def _own(node: ast.AST):
    """Nodes under `node`, not descending into nested functions or classes."""
    stack = list(ast.iter_child_nodes(node))
    while stack:
        child = stack.pop()
        yield child
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                              ast.Lambda, ast.ClassDef)):
            continue
        stack.extend(ast.iter_child_nodes(child))


@dataclass
class Graph:
    functions: List[Fn]
    calls: Dict[Fn, Set[Fn]]
    callers: Dict[Fn, Set[Fn]]
    reaching: Set[Fn]
    store: Set[Tuple[str, str]]
    trees: Dict[str, ast.Module]


def _parse(tops) -> Dict[str, ast.Module]:
    trees = {}
    for top in tops:
        for path in sorted((REPO / top).rglob("*.py")):
            if "frontend" in path.parts or "node_modules" in path.parts:
                continue
            trees[path.relative_to(REPO).as_posix()] = ast.parse(
                path.read_text(encoding="utf-8"))
    return trees


def build_graph(tops=WALKED) -> Graph:
    trees = _parse(tops)
    functions: List[Fn] = []
    classes: Dict[Tuple[str, str], ast.ClassDef] = {}

    for rel, tree in trees.items():
        def visit(node, cls, outer):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, ast.ClassDef):
                    classes[(rel, child.name)] = child
                    visit(child, child.name, outer)
                elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    fn = Fn(rel, cls, child, outer)
                    functions.append(fn)
                    visit(child, cls, fn)
                else:
                    visit(child, cls, outer)
        visit(tree, None, None)

    # The store: DuckDBStore and the mixins it is composed of.
    store_def = classes[("core/duckdb_store.py", "DuckDBStore")]
    composed = {b.id for b in store_def.bases if isinstance(b, ast.Name)}
    composed.add("DuckDBStore")
    store = {k for k in classes if k[1] in composed and (
        k[0].startswith("core/repositories/") or k[0] == "core/duckdb_store.py")}

    store_methods: Dict[str, List[Fn]] = defaultdict(list)
    by_module: Dict[str, Dict[str, Fn]] = defaultdict(dict)
    by_class: Dict[Tuple[str, str], Dict[str, Fn]] = defaultdict(dict)
    other_methods: Dict[str, List[Fn]] = defaultdict(list)
    for fn in functions:
        if (fn.rel, fn.cls) in store:
            store_methods[fn.name].append(fn)
        elif fn.cls is not None and fn.outer is None:
            by_class[(fn.rel, fn.cls)][fn.name] = fn
            other_methods[fn.name].append(fn)
        if fn.cls is None or fn.outer is not None:
            by_module[fn.rel].setdefault(fn.name, fn)

    def module_file(dotted: str) -> Optional[str]:
        rel = dotted.replace(".", "/")
        for candidate in (f"{rel}.py", f"{rel}/__init__.py"):
            if candidate in trees:
                return candidate
        return None

    def bindings(nodes):
        """(repository modules by alias, imported names, every module alias)."""
        modules, names, aliases = {}, {}, set()
        for node in nodes:
            if isinstance(node, ast.Import):
                for a in node.names:
                    bound = a.asname or a.name.split(".")[0]
                    aliases.add(bound)
                    target = module_file(a.name)
                    if target:
                        modules[a.asname or a.name] = target
            elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
                for a in node.names:
                    bound = a.asname or a.name
                    sub = module_file(f"{node.module}.{a.name}")
                    if sub:
                        modules[bound] = sub
                        aliases.add(bound)
                    elif module_file(node.module):
                        names[bound] = (module_file(node.module), a.name)
        return modules, names, aliases

    top_level = {rel: bindings(tree.body) for rel, tree in trees.items()}

    def scope(fn: Fn):
        modules, names, aliases = (dict(top_level[fn.rel][0]),
                                   dict(top_level[fn.rel][1]),
                                   set(top_level[fn.rel][2]))
        chain, g = [], fn
        while g is not None:
            chain.append(g)
            g = g.outer
        for g in reversed(chain):
            m, n, a = bindings(list(_own(g.node)))
            modules.update(m)
            names.update(n)
            aliases |= a
        return modules, names, aliases

    def resolve(fn: Fn) -> Set[Fn]:
        modules, names, aliases = scope(fn)
        out: Set[Fn] = set()
        for call in (n for n in _own(fn.node) if isinstance(n, ast.Call)):
            f = call.func
            if isinstance(f, ast.Name):
                if f.id in names:
                    rel, name = names[f.id]
                    target = by_module[rel].get(name)
                    if target:
                        out.add(target)
                elif f.id in by_module[fn.rel]:
                    out.add(by_module[fn.rel][f.id])
                continue
            if not isinstance(f, ast.Attribute):
                continue
            attr, recv = f.attr, f.value
            if isinstance(recv, ast.Name) and recv.id in ("self", "cls"):
                if (fn.rel, fn.cls) in store:
                    out.update(store_methods.get(attr, ()))
                elif fn.cls and attr in by_class[(fn.rel, fn.cls)]:
                    out.add(by_class[(fn.rel, fn.cls)][attr])
            elif isinstance(recv, ast.Name) and recv.id in modules:
                target = by_module[modules[recv.id]].get(attr)
                if target:
                    out.add(target)
            elif attr in store_methods and STORE_RECEIVER.search(ast.unparse(recv)):
                out.update(store_methods[attr])
            elif (len(other_methods.get(attr, ())) == 1
                  and not (isinstance(recv, ast.Name) and recv.id in aliases)):
                out.add(other_methods[attr][0])
        return out

    calls = {fn: resolve(fn) for fn in functions}
    callers: Dict[Fn, Set[Fn]] = defaultdict(set)
    for fn, targets in calls.items():
        for target in targets:
            callers[target].add(fn)

    def refuses(fn: Fn) -> bool:
        if fn.rel == "core/read_fallback.py":
            return False
        for n in _own(fn.node):
            if isinstance(n, ast.Call):
                name = (n.func.attr if isinstance(n.func, ast.Attribute)
                        else getattr(n.func, "id", None))
                if name in REFUSERS:
                    return True
        return False

    reaching = {fn for fn in functions if refuses(fn)}
    changed = True
    while changed:
        changed = False
        for fn, targets in calls.items():
            if fn not in reaching and targets & reaching:
                reaching.add(fn)
                changed = True
    return Graph(functions, calls, callers, reaching, store, trees)


def _in_router_layer(fn: Fn, graph: Graph) -> bool:
    return ((fn.rel, fn.cls) in graph.store
            or fn.rel.startswith("core/repositories/")
            or re.fullmatch(r"core/(pg|ch)_[^/]*\.py", fn.rel) is not None)


def entry_points(graph: Graph) -> List[Fn]:
    """Functions that reach a refusal and that nothing in the repository
    calls — invoked by reference. Dead router methods are not entries."""
    return sorted((fn for fn in graph.reaching
                   if not graph.callers[fn] and not _in_router_layer(fn, graph)),
                  key=lambda fn: fn.key)


def _routes(graph: Graph):
    """Every HTTP endpoint the app registers, keyed by its function."""
    from tests.routes_helper import iter_endpoints
    from web.main import app

    index = {fn.key: fn for fn in graph.functions}
    routes: Dict[Fn, List[Tuple[str, str]]] = defaultdict(list)
    for endpoint in iter_endpoints(app):
        func = inspect.unwrap(endpoint.route.endpoint)
        rel = pathlib.Path(inspect.getsourcefile(func)).resolve().relative_to(
            REPO).as_posix()
        fn = index.get(f"{rel}:{func.__qualname__}")
        assert fn is not None, f"{endpoint.path}: {rel}:{func.__qualname__}"
        for method in sorted(endpoint.methods):
            routes[fn].append((method, endpoint.path))
    return routes


def configures_the_mode(tree: ast.AST) -> bool:
    """The module reads KS_READ_FALLBACK itself — calls `configure_modes()`
    (or the mode's own `configure_mode()`). A process that never does runs
    under `duckdb` whatever the variable says, and cannot refuse."""
    return any(
        isinstance(n, ast.Call)
        and (n.func.attr if isinstance(n.func, ast.Attribute)
             else getattr(n.func, "id", None)) in ("configure_modes", "configure_mode")
        for n in ast.walk(tree))


def classify(graph: Graph):
    """(swept routes reached, other routes reached, non-HTTP entries).

    A script is its own process: its entries count only if it configures the
    mode. `scripts/weekly_report_preview.py` reaches the weekly report's
    router and never reads KS_READ_FALLBACK, so it cannot be refused."""
    from tests.unit.test_read_fallback_http import NETWORK_ROUTES

    routes = _routes(graph)
    swept, unswept, consumers = set(), set(), set()
    for fn in entry_points(graph):
        if (fn.rel.startswith("scripts/")
                and not configures_the_mode(graph.trees[fn.rel])):
            continue
        served = routes.get(fn)
        if not served:
            consumers.add(fn.key)
            continue
        for method, path in served:
            if (method == "GET" and path.startswith("/api/")
                    and path not in NETWORK_ROUTES):
                swept.add(f"{method} {path}")
            elif path in NETWORK_ROUTES:
                consumers.add(fn.key)
            else:
                unswept.add(f"{method} {path}")
    return swept, unswept, consumers


@pytest.fixture(scope="module")
def graph() -> Graph:
    return build_graph()


# ─── The pins, derived ────────────────────────────────────────────────────

class TestWhereARefusalGoes:
    def test_the_non_http_consumers_are_the_ones_written_down(self, graph):
        """DN-20c's list, derived. A new entry here is a consumer that would
        receive a refusal as an exception: decide what it answers, then add
        it — and to DN-20c."""
        _, _, consumers = classify(graph)
        assert consumers == NON_HTTP_CONSUMERS, (
            f"new: {sorted(consumers - NON_HTTP_CONSUMERS)}; "
            f"gone: {sorted(NON_HTTP_CONSUMERS - consumers)}")

    def test_the_unswept_routes_are_the_ones_written_down(self, graph):
        _, unswept, _ = classify(graph)
        assert unswept == UNSWEPT_ROUTES, (
            f"new: {sorted(unswept - UNSWEPT_ROUTES)}; "
            f"gone: {sorted(UNSWEPT_ROUTES - unswept)}")

    def test_the_seasonality_job_is_found_by_the_walk(self, graph):
        """The one the remembered list missed, reached the way it really is:
        job → calculate_suggested_goals → get_historical_revenue → _goals_run,
        which refuses."""
        index = {fn.key: fn for fn in graph.functions}
        job = index["core/scheduler.py:BackgroundScheduler._run_seasonality_calc"]
        goals_run = index["core/repositories/goals.py:GoalsMixin._goals_run"]
        seen, frontier = set(), [job]
        while frontier:
            fn = frontier.pop()
            if fn in seen:
                continue
            seen.add(fn)
            frontier.extend(graph.calls[fn])
        assert goals_run in seen

    def test_it_is_looking(self, graph):
        """Non-vacuity: the walk reaches the swept routes the sweep proves —
        sixty-odd when it was written — and the refusers themselves."""
        swept, _, _ = classify(graph)
        assert len(swept) >= 50, sorted(swept)
        assert {"GET /api/summary", "GET /api/traffic/analytics",
                "GET /api/customers/cohort-retention",
                "GET /api/revenue/forecast"} <= swept
        refusers = {fn.key for fn in graph.reaching if not graph.calls[fn] & graph.reaching}
        assert "core/repositories/traffic.py:TrafficMixin._traffic_run" in refusers

    def test_resolution_does_not_follow_a_name_into_another_library(self, graph):
        """`lgb.train(...)` is LightGBM's, not `PredictionService.train`, and
        `self._client._get` is KeyCRM's, not the store's: the two false edges
        a name-only walk draws here."""
        index = {fn.key: fn for fn in graph.functions}
        train = index["core/prediction_service.py:PredictionService.train"]
        assert index["core/prediction_service.py:_train_model"] not in graph.callers[train]
        assert not any(fn.rel == "core/keycrm.py" for fn in graph.reaching)


class TestOnlyTheseProcessesRefuse:
    def test_a_script_that_never_configures_the_mode_is_not_a_consumer(self, graph):
        """Non-vacuity for the rule in `classify`: the preview script does
        reach a refusing router, and it is left out only because it never
        reads the mode — the day it calls `configure_modes`, it is listed."""
        entries = {fn.key for fn in entry_points(graph)}
        assert "scripts/weekly_report_preview.py:main" in entries
        assert not configures_the_mode(graph.trees["scripts/weekly_report_preview.py"])
        assert configures_the_mode(graph.trees["scripts/force_resync.py"])

    def test_the_bot_never_configures_the_mode(self):
        """`bot/` is outside the walk because it cannot refuse: it never reads
        KS_READ_FALLBACK, so `refusing()` is False there for good. The day it
        calls `configure_modes`, its callers belong in the walk."""
        offenders = []
        for rel, tree in _parse(("bot",)).items():
            for n in ast.walk(tree):
                if isinstance(n, ast.Call):
                    name = (n.func.attr if isinstance(n.func, ast.Attribute)
                            else getattr(n.func, "id", None))
                    if name in ("configure_modes", "configure_mode"):
                        offenders.append(f"{rel}:{n.lineno}")
        assert not offenders, offenders


# ─── The walk seen to bite ────────────────────────────────────────────────

class TestTheWalkBites:
    def test_an_entry_is_one_only_while_it_reaches_a_refusal(self):
        graph = build_graph()
        index = {fn.key: fn for fn in graph.functions}
        job = index["core/scheduler.py:BackgroundScheduler._run_seasonality_calc"]
        assert job in entry_points(graph)
        # Take its one route to a refusal away, and it is no longer an entry.
        graph.calls[job] = set()
        graph.reaching.discard(job)
        assert job not in entry_points(graph)
