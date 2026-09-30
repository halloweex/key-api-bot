"""Where a refusal goes when no swept HTTP request is waiting for it
(DN-20b), and where it stops (DN-20c).

Under `KS_READ_FALLBACK=off` a router raises `ReadUnavailable`, and one place
raises for every caller. An HTTP GET route turns it into a 503 naming the
surface, and `tests/unit/test_read_fallback_http.py` proves that for every
such route by running it. Everything else that can reach a refusing router
answers it itself (DN-20c) — `test_read_fallback_off_consumers.py` runs each
answer. That list used to be remembered — "the weekly reports, the
assistant, training, the sync" — and it missed the Monday goals job:
`seasonality_calc` reaches `_goals_run` through `calculate_suggested_goals`,
and under `off` with Postgres down it wrote `seasonal_indices`, then raised
before `calculate_yoy_growth` ran, leaving the shared goal tables half
updated. It now asks that read first.

So the list is derived here. The walk starts at every function that refuses —
a call to `fall_back`, `no_address` or `no_engine` — and follows callers up
through the repository until it reaches an **entry point**: a function a
scheduler, an executor, an HTTP route or a script's `__main__` invokes by
reference — nothing in the repository calls it, or something hands it on as
a value (`func=self._run_x`), which keeps a job an entry on the day a route
calls it directly too. Each entry point is then one of:

- a GET route under `/api/` that the sweep runs — proved there, not here;
- another HTTP route (`POST`, `DELETE`) — answered by the same 503 handler,
  but not swept, because a sweep that writes is not a probe;
- the assistant's two routes, `POST /api/chat` and `GET /api/chat/stream` —
  HTTP, but a refusal there is answered inside the conversation: its tools
  return a named `data_unavailable` result the model reads, because a 503
  cannot be sent once a stream is open. Not swept either: each call spends
  the Anthropic API. These are the only routes a named answer may sit on
  (`IN_BAND_ROUTES`);
- an entry that is no HTTP request's at all — a scheduler job, the boot sync,
  a script. These are DN-20c's, pinned below.

Both pins are **derived and then compared**: a new consumer fails this test
until somebody writes it down, which is the moment to decide what it answers.

WHERE EACH ONE STOPS (DN-20c)

Then the walk goes back down from each consumer, following every edge
through the `try` its call sits in, and finds the handler that keeps the
refusal in: one naming `ReadUnavailable` (the jobs' deferrals, the
assistant's `data_unavailable`), or an `except Exception` that already
contained every failure of its step (the buyers step, the boot's). No
consumer may let a refusal out as an exception, where each one stops is
pinned (`ANSWERS`, and `UNSWEPT_STOPS` for the routes the sweep does not
run), and a named answer may sit only in a function reached from those
consumers and the assistant's routes alone — the exemption
`test_read_fallback_http.py` makes from its rule that a handler naming the
refusal must raise it. What the walk cannot see is order: that the goals
job asks before it writes is proved by running it.

WHAT THE WALK RESOLVES, AND WHAT IT CANNOT

Calls are resolved, not matched by name alone: a name like `train` or `_get`
means five things in this repository. `self.x()` inside the store's mixins is
any mixin's `x` — they compose one class; elsewhere it is the class's own
method. `x()` is the module's function, or the one a `from core.m import x`
names — function-local imports included, which is where most of them are
here. `m.x()` follows an imported repository module. `obj.x()` on an object
a factory made — `service = get_chat_service()`, in the function or one
enclosing it, or `get_chat_service().x()` — is that class's `x` when the
factory is annotated to return a repository class. A store method is
reached through a receiver called `store` (`store.x()`, `self.store.x()`,
`get_store().x()`). Any other `obj.x()` resolves only when exactly one class
in the repository defines `x` and `obj` is not an imported module
(`lgb.train` is not `PredictionService.train`). A method nothing calls in
the router layer itself is not an entry point but dead code, and is not
listed.

A name two classes define is where that last rule goes blind, and it went
blind on the assistant: `chat` is `LLMClient`'s and `ChatService`'s, so the
routes' `service.chat(...)` resolved to nothing, `ChatService.chat` looked
like an entry point of its own, and the rule below — a named answer on no
HTTP path — passed while both assistant routes reached one. So every call
left unresolved that bears the name of a function reaching a refusal is
derived and pinned (`UNRESOLVED_NAMESAKES`), each read and found to be
something else: the next such miss fails there instead of hiding.

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
}

# The assistant's routes. HTTP, but its tools run inside a conversation,
# where a 503 cannot be sent once the stream is open, so a refusal is a
# named `data_unavailable` tool result the model reads and relays. The only
# routes a named answer may take the 503 handler's place on.
IN_BAND_ROUTES = {
    "GET /api/chat/stream",
    "POST /api/chat",
}

# HTTP routes the sweep does not run, so a handler on their path is not
# proved absent by running them. The writing ones, because a sweep that
# writes is not a probe; the assistant's, because each call spends the
# Anthropic API (and the stream is one of the sweep's NETWORK_ROUTES).
UNSWEPT_ROUTES = {
    "DELETE /api/goals/{period_type}",
    "POST /api/duckdb/sync-buyers",
    "POST /api/goals",
    "POST /api/revenue/forecast/train",
    "POST /api/revenue/forecast/tune",
} | IN_BAND_ROUTES

# DN-20c: where each consumer's refusal stops, as `file:function:kind`.
# `named` is a handler naming `ReadUnavailable` that answers it — the job's
# result says `read_unavailable` and the surface. `broad` is an `except
# Exception` that was there already and contains every failure of a step
# the same way, a refusal included: the buyers step holds its watermark and
# the tick goes on (the plan's "record the failure and skip that step"), and
# the boot keeps serving. Derived and compared, like the lists above: a
# handler added on a consumer's path — or one taken away, which would let a
# refusal out as an exception — fails here until somebody writes it down.
_BUYERS = "core/sync_service.py:SyncService.sync_missing_buyers:broad"
_TOOLS = "core/chat_tools.py:execute_tool:named"
ANSWERS = {
    "core/scheduler.py:BackgroundScheduler._run_incremental_sync": {_BUYERS},
    "core/scheduler.py:BackgroundScheduler._run_meilisearch_sync": {
        "core/scheduler.py:BackgroundScheduler._run_meilisearch_sync:named"},
    "core/scheduler.py:BackgroundScheduler._run_revenue_prediction": {
        "core/scheduler.py:BackgroundScheduler._run_revenue_prediction:named"},
    "core/scheduler.py:BackgroundScheduler._run_seasonality_calc": {
        "core/scheduler.py:BackgroundScheduler._run_seasonality_calc:named"},
    "core/scheduler.py:BackgroundScheduler._run_traffic_report": {
        "core/scheduler.py:BackgroundScheduler._run_traffic_report:named"},
    "core/scheduler.py:BackgroundScheduler._run_weekly_report": {
        "core/scheduler.py:BackgroundScheduler._run_weekly_report:named"},
    # The boot: each step it runs contains its own failures, and a refused
    # read is one of them. Nothing is served before this returns, so there
    # is no request to answer 503 to.
    "web/main.py:startup_event": {
        _BUYERS,
        "core/sync_service.py:init_and_sync:broad",
        "web/main.py:_train_prediction_model:broad",
    },
}

# The routes the sweep does not run, and where a refusal stops on each before
# the 503 handler — empty means it reaches the handler. The assistant's stop
# at its tools, by design (`IN_BAND_ROUTES`). One writing route does too, and
# not by design: the buyers step is shared with the scheduler's tick, and its
# `except Exception` answers the route — "Synced 0 buyers" under `off`. It is
# written down rather than changed here, because chain 4 (PR #249) rebuilds
# that step and that route; see DN-20c's deviations.
UNSWEPT_STOPS = {
    "DELETE /api/goals/{period_type}": set(),
    "GET /api/chat/stream": {_TOOLS},
    "POST /api/chat": {_TOOLS},
    "POST /api/duckdb/sync-buyers": {_BUYERS},
    "POST /api/goals": set(),
    "POST /api/revenue/forecast/train": set(),
    "POST /api/revenue/forecast/tune": set(),
}

# Calls the resolver leaves unresolved although a function that reaches a
# refusal bears the same name — `file:function: call`, each read by hand and
# found to be something else. Derived and compared: a call to a name two
# classes define, on a receiver the walk cannot type, lands here first, which
# is how the assistant's routes were missed (`service.chat(...)`).
UNRESOLVED_NAMESAKES = {
    # KeyCRM's API client, not the store's `get_categories`.
    "core/keycrm.py:SyncKeyCRMClient._get: client.get_categories",
    # LightGBM's `train`, not PredictionService's.
    "core/prediction_service.py:_run_evaluation: lgb.train",
    "core/prediction_service.py:_train_model: lgb.train",
    "core/prediction_service.py:_tune_hyperparameters: lgb.train",
    # The Anthropic client under the assistant, which reads no store.
    "web/services/chat_service.py:ChatService.chat: self.llm.chat",
    "web/services/chat_service.py:ChatService.chat_stream: self.llm.chat_stream",
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
    # Every call in a function's own body, with what it resolves to — the
    # per-call form of `calls`, which is what the containment walk needs:
    # whether a refusal escapes depends on which `try` a call sits in.
    sites: Dict[Fn, List[Tuple[ast.Call, Set[Fn]]]] = field(default_factory=dict)
    # Functions some other function names as a value rather than calling —
    # `func=self._run_x` handed to the scheduler. Each is an entry point
    # whether or not something also calls it.
    referenced: Set[Fn] = field(default_factory=set)


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

    def class_named(rel: str, annotation) -> Optional[Tuple[str, str]]:
        """The repository class a return annotation names — `-> ChatService`,
        `-> "ChatService"`, `-> Optional[ChatService]` — defined in `rel` or
        imported into it by name. None for anything else."""
        if isinstance(annotation, ast.Subscript):
            annotation = annotation.slice
        if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
            name = annotation.value
        elif isinstance(annotation, ast.Name):
            name = annotation.id
        else:
            return None
        if (rel, name) in classes:
            return (rel, name)
        imported = top_level[rel][1].get(name)
        return imported if imported in classes else None

    # What a factory hands back: `def get_chat_service() -> ChatService`.
    returns = {fn: class_named(fn.rel, fn.node.returns)
               for fn in functions if fn.node.returns is not None}

    def callee(call: ast.Call, fn: Fn, modules, names) -> Optional[Fn]:
        """The one repository function a plain call names — `f()`, an
        imported `f()`, or `m.f()` on a repository module."""
        f = call.func
        if isinstance(f, ast.Name):
            if f.id in names:
                rel, name = names[f.id]
                return by_module[rel].get(name)
            return by_module[fn.rel].get(f.id)
        if (isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name)
                and f.value.id in modules):
            return by_module[modules[f.value.id]].get(f.attr)
        return None

    def made_by(value, fn: Fn, modules, names) -> Optional[Tuple[str, str]]:
        """The class of an object a factory returned: `get_x()` or
        `await get_x()`, where `get_x` is annotated to return a class."""
        if isinstance(value, ast.Await):
            value = value.value
        if not isinstance(value, ast.Call):
            return None
        target = callee(value, fn, modules, names)
        return returns.get(target) if target is not None else None

    def bound(fn: Fn, modules, names) -> Dict[str, Tuple[str, str]]:
        """Local names bound from a factory, in this function and every one
        enclosing it — `service = get_chat_service()` in a route is what its
        nested `event_generator` calls `service.chat_stream` on. A method
        name defined by two classes (`chat` is LLMClient's and ChatService's)
        resolves through this and nothing else."""
        out: Dict[str, Tuple[str, str]] = {}
        chain, g = [], fn
        while g is not None:
            chain.append(g)
            g = g.outer
        for g in reversed(chain):
            for node in _own(g.node):
                if (isinstance(node, ast.Assign) and len(node.targets) == 1
                        and isinstance(node.targets[0], ast.Name)):
                    cls = made_by(node.value, g, modules, names)
                    if cls is not None:
                        out[node.targets[0].id] = cls
        return out

    def resolve(fn: Fn) -> List[Tuple[ast.Call, Set[Fn]]]:
        modules, names, aliases = scope(fn)
        objects = bound(fn, modules, names)
        found: List[Tuple[ast.Call, Set[Fn]]] = []
        for call in (n for n in _own(fn.node) if isinstance(n, ast.Call)):
            out: Set[Fn] = set()
            found.append((call, out))
            f = call.func
            if isinstance(f, ast.Name):
                target = callee(call, fn, modules, names)
                if target:
                    out.add(target)
                continue
            if not isinstance(f, ast.Attribute):
                continue
            attr, recv = f.attr, f.value
            made = (objects.get(recv.id) if isinstance(recv, ast.Name)
                    else made_by(recv, fn, modules, names))
            if made is not None and attr in by_class[made]:
                out.add(by_class[made][attr])
            elif isinstance(recv, ast.Name) and recv.id in ("self", "cls"):
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
        return found

    def references(fn: Fn) -> Set[Fn]:
        """The repository functions `fn` names without calling them: `f`,
        an imported `f`, `m.f`, or `self.f` on its own class — what a job
        registration, an executor or a callback is handed."""
        modules, names, _aliases = scope(fn)
        called = {id(n.func) for n in _own(fn.node) if isinstance(n, ast.Call)}
        out: Set[Fn] = set()
        for n in _own(fn.node):
            if id(n) in called or not isinstance(getattr(n, "ctx", None), ast.Load):
                continue
            target = None
            if isinstance(n, ast.Name):
                if n.id in names:
                    rel, name = names[n.id]
                    target = by_module[rel].get(name)
                else:
                    target = by_module[fn.rel].get(n.id)
            elif isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name):
                if n.value.id in ("self", "cls") and fn.cls:
                    target = by_class[(fn.rel, fn.cls)].get(n.attr)
                elif n.value.id in modules:
                    target = by_module[modules[n.value.id]].get(n.attr)
            if target is not None and target is not fn:
                out.add(target)
        return out

    sites = {fn: resolve(fn) for fn in functions}
    referenced = set().union(*(references(fn) for fn in functions))
    calls = {fn: set().union(*(t for _c, t in found)) if found else set()
             for fn, found in sites.items()}
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
    return Graph(functions, calls, callers, reaching, store, trees, sites, referenced)


def _in_router_layer(fn: Fn, graph: Graph) -> bool:
    return ((fn.rel, fn.cls) in graph.store
            or fn.rel.startswith("core/repositories/")
            or re.fullmatch(r"core/(pg|ch)_[^/]*\.py", fn.rel) is not None)


def _is_entry(fn: Fn, graph: Graph) -> bool:
    """Invoked by reference: nothing in the repository calls it, or
    something hands it on as a value — a job the scheduler runs is its
    consumer even on the day a route calls it directly too."""
    return not graph.callers[fn] or fn in graph.referenced


def entry_points(graph: Graph) -> List[Fn]:
    """Functions that reach a refusal and are invoked by reference. Dead
    router methods are not entries."""
    return sorted((fn for fn in graph.reaching
                   if _is_entry(fn, graph) and not _in_router_layer(fn, graph)),
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
            else:
                unswept.add(f"{method} {path}")
    return swept, unswept, consumers


# ─── Where a refusal stops (DN-20c) ───────────────────────────────────────
#
# The walk above follows every call. Whether a refusal *leaves* a function
# depends on the `try` its call sits in, so this follows the same edges and
# cuts one where a handler catches `ReadUnavailable` and does not raise it
# again. A handler catches it when it names it, or when it is bare or names
# `Exception`/`BaseException`; the first such handler of the innermost `try`
# decides, and one that ends in `raise` passes it to the next `try` out —
# which is how `_train_impl` lets a refusal through its `except Exception`.

_TRY = tuple(t for t in (ast.Try, getattr(ast, "TryStar", None)) if t)
_BROAD = {"Exception", "BaseException"}


def _catches(handler: ast.ExceptHandler) -> Optional[str]:
    """`named`, `broad`, or None when the handler lets a refusal pass."""
    if handler.type is None:
        return "broad"
    names = {n.id if isinstance(n, ast.Name) else n.attr
             for n in ast.walk(handler.type)
             if isinstance(n, (ast.Name, ast.Attribute))}
    if "ReadUnavailable" in names:
        return "named"
    return "broad" if names & _BROAD else None


def _guarded_calls(node: ast.AST) -> Dict[ast.Call, List[ast.AST]]:
    """Every call in a function's own body, with the `try` statements whose
    *body* holds it, innermost first. A handler, an `else` or a `finally` is
    not protected by its own `try`."""
    out: Dict[ast.Call, List[ast.AST]] = {}

    def visit(parent, tries):
        for name, value in ast.iter_fields(parent):
            for child in (value if isinstance(value, list) else [value]):
                if not isinstance(child, ast.AST) or isinstance(
                        child, (ast.FunctionDef, ast.AsyncFunctionDef,
                                ast.Lambda, ast.ClassDef)):
                    continue
                inner = ([parent] + tries
                         if isinstance(parent, _TRY) and name == "body" else tries)
                if isinstance(child, ast.Call):
                    out[child] = inner
                visit(child, inner)

    visit(node, [])
    return out


def _has_return(node: ast.AST) -> bool:
    return isinstance(node, ast.Return) or any(
        _has_return(child) for child in ast.iter_child_nodes(node)
        if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                                  ast.Lambda, ast.ClassDef)))


def _passes_it_on(handler: ast.ExceptHandler) -> bool:
    """The handler raises the refusal again — a bare `raise`, or `raise`
    of the name it bound — as a statement of its own body, with no `return`
    in any statement before it. The raise need not be last: what follows an
    unconditional `raise` never runs. A `return` on the way is an exit that
    is not the raise, so that handler is read as a stop — the conservative
    side, because a stop is pinned in `ANSWERS` where somebody reads it."""
    for statement in handler.body:
        if isinstance(statement, ast.Raise) and statement.cause is None and (
                statement.exc is None
                or (isinstance(statement.exc, ast.Name)
                    and statement.exc.id == handler.name)):
            return True
        if _has_return(statement):
            return False
    return False


def _stopped_by(tries) -> Optional[str]:
    """The kind of the handler that keeps a refusal raised under these
    `try`s from going further, or None when it leaves the function."""
    for t in tries:
        for handler in t.handlers:
            kind = _catches(handler)
            if kind is None:
                continue
            if _passes_it_on(handler):
                break
            return kind
    return None


def _is_refusal(call: ast.Call) -> bool:
    f = call.func
    return (f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)) in REFUSERS


@dataclass
class Containment:
    graph: Graph
    escaping: Set[Fn]
    stop: Dict[Tuple[Fn, int], Optional[str]]

    def stops_of(self, start: Fn) -> Set[str]:
        """Every handler that stops a refusal on some path from `start`, as
        `file:function:kind`. Follows every edge toward a refusal, stopped or
        not, because a stop that catches nothing — `startup_event`'s own
        `except` around a boot whose steps all contain themselves — is not
        where the refusal ends and is not listed."""
        found, seen, frontier = set(), set(), [start]
        while frontier:
            fn = frontier.pop()
            if fn in seen:
                continue
            seen.add(fn)
            for call, targets in self.graph.sites[fn]:
                kind = self.stop.get((fn, id(call)))
                if kind and (_is_refusal(call) or targets & self.escaping):
                    found.add(f"{fn.key}:{kind}")
                frontier.extend(targets & self.graph.reaching)
        return found


def containment(graph: Graph) -> Containment:
    stop: Dict[Tuple[Fn, int], Optional[str]] = {}
    for fn in graph.reaching:
        guarded = _guarded_calls(fn.node)
        for call, _targets in graph.sites[fn]:
            stop[(fn, id(call))] = _stopped_by(guarded.get(call, ()))
    escaping: Set[Fn] = set()
    changed = True
    while changed:
        changed = False
        for fn in graph.reaching:
            if fn in escaping:
                continue
            if any(stop[(fn, id(call))] is None
                   and (_is_refusal(call) or targets & escaping)
                   for call, targets in graph.sites[fn]):
                escaping.add(fn)
                changed = True
    return Containment(graph, escaping, stop)


def entries_reaching(graph: Graph, target: Fn) -> Set[Fn]:
    """The entry points that reach `target` — the ones a handler in
    `target` answers for."""
    found, seen, frontier = set(), set(), [target]
    while frontier:
        fn = frontier.pop()
        if fn in seen:
            continue
        seen.add(fn)
        if _is_entry(fn, graph):
            found.add(fn)
        frontier.extend(graph.callers[fn])
    return found


def named_answers_out_of_place(graph: Graph, routes=None) -> Dict[str, List[str]]:
    """Every named answer reached from an entry that is neither a non-HTTP
    consumer nor one of the assistant's routes — `{file:function: [entry]}`,
    an entry as `METHOD /path` when it is a route and `file:function`
    otherwise. Empty when each answer sits only where it is allowed to."""
    routes = _routes(graph) if routes is None else routes
    index = {fn.key: fn for fn in graph.functions}
    allowed = NON_HTTP_CONSUMERS | IN_BAND_ROUTES
    out: Dict[str, List[str]] = {}
    for fn_key in sorted(named_answer_functions()):
        labels = set()
        for entry in entries_reaching(graph, index[fn_key]):
            served = routes.get(entry)
            labels |= ({f"{m} {p}" for m, p in served} if served else {entry.key})
        if labels - allowed:
            out[fn_key] = sorted(labels - allowed)
    return out


def unresolved_namesakes(graph: Graph) -> Set[str]:
    """`file:function: call` for every call the resolver left without a
    target whose name is that of a function reaching a refusal."""
    reaching = {fn.name for fn in graph.reaching}
    found = set()
    for fn, sites in graph.sites.items():
        for call, targets in sites:
            f = call.func
            name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
            if not targets and name in reaching:
                found.add(f"{fn.key}: {ast.unparse(f)}")
    return found


@pytest.fixture(scope="module")
def graph() -> Graph:
    return build_graph()


@pytest.fixture(scope="module")
def contained(graph) -> Containment:
    return containment(graph)


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


class TestEveryConsumerAnswers:
    """DN-20c: a refusal never leaves a consumer no HTTP request is waiting
    for as an exception, and each one stops where it is written down."""

    def test_no_refusal_escapes_a_non_http_consumer(self, graph, contained):
        index = {fn.key: fn for fn in graph.functions}
        escaped = sorted(k for k in NON_HTTP_CONSUMERS
                         if index[k] in contained.escaping)
        assert not escaped, (
            "a refusal can leave these as an exception — give each an "
            f"answer: {escaped}")

    def test_each_consumer_answers_where_it_is_written_down(self, graph, contained):
        index = {fn.key: fn for fn in graph.functions}
        derived = {k: contained.stops_of(index[k]) for k in NON_HTTP_CONSUMERS}
        assert derived == ANSWERS, {
            k: {"derived": sorted(derived[k]), "pinned": sorted(ANSWERS.get(k, ()))}
            for k in derived if derived[k] != ANSWERS.get(k)}

    def test_a_named_answer_is_on_no_http_path_but_the_assistants(self, graph):
        """A handler that answers a refusal takes the 503 away from any route
        that reaches it. So every function holding one is reached only from
        the non-HTTP consumers above and the assistant's two routes, which
        answer in the conversation — and `test_read_fallback_http.py` exempts
        these functions, and no others, from its rule that a handler naming
        the refusal must raise it."""
        assert named_answer_functions(), "no named answer is pinned at all"
        assert named_answers_out_of_place(graph) == {}

    def test_the_assistant_is_reached_from_its_routes(self, graph):
        """The edge a name-only walk missed: `service = get_chat_service()`
        in each route, annotated `-> ChatService`, is what makes
        `service.chat(...)` ChatService's and not LLMClient's. Without it
        the assistant looked like an entry point of its own and the rule
        above held by not seeing the routes."""
        index = {fn.key: fn for fn in graph.functions}
        chat = index["web/services/chat_service.py:ChatService.chat"]
        stream = index["web/services/chat_service.py:ChatService.chat_stream"]
        assert chat in graph.calls[index["web/routes/chat.py:chat"]]
        # The stream is read in a generator nested in the route.
        assert stream in graph.calls[index["web/routes/chat.py:event_generator"]]
        assert index["core/llm_client.py:LLMClient.chat"] not in graph.calls[
            index["web/routes/chat.py:chat"]]
        routes, served = _routes(graph), set()
        for fn in entries_reaching(graph, index["core/chat_tools.py:execute_tool"]):
            served |= {f"{m} {p}" for m, p in routes.get(fn, ())}
        assert served == IN_BAND_ROUTES

    def test_every_namesake_left_unresolved_is_written_down(self, graph):
        """The resolver's blind spot, derived: a call it cannot type whose
        name is that of a function reaching a refusal. Each written down has
        been read and is something else; a new one is a possible missed
        edge — resolve it, or read it and write it down."""
        derived = unresolved_namesakes(graph)
        assert derived == UNRESOLVED_NAMESAKES, (
            f"new: {sorted(derived - UNRESOLVED_NAMESAKES)}; "
            f"gone: {sorted(UNRESOLVED_NAMESAKES - derived)}")

    def test_the_unswept_routes_stop_where_they_are_written_down(self, graph, contained):
        routes = _routes(graph)
        derived = {}
        for fn, served in routes.items():
            for method, path in served:
                label = f"{method} {path}"
                if label in UNSWEPT_ROUTES:
                    derived[label] = contained.stops_of(fn)
        assert derived == UNSWEPT_STOPS

    def test_the_walk_sees_a_refusal_escape(self, graph, contained):
        """Non-vacuity: the routers themselves escape — that is what a
        refusal is — and so do the helpers between a job and its answer."""
        index = {fn.key: fn for fn in graph.functions}
        for key in ("core/repositories/traffic.py:TrafficMixin._traffic_run",
                    "core/weekly_report.py:_weekly_run",
                    "core/weekly_report.py:warehouse_max_date",
                    "core/prediction_service.py:PredictionService._train_impl",
                    "core/sync_service.py:SyncService.sync_to_meilisearch"):
            assert index[key] in contained.escaping, key


def named_answer_functions() -> Set[str]:
    """`file:function` of every named answer in `ANSWERS` and
    `UNSWEPT_STOPS`."""
    return {entry.rsplit(":", 1)[0]
            for pins in (ANSWERS, UNSWEPT_STOPS)
            for stops in pins.values() for entry in stops
            if entry.endswith(":named")}


class TestTheContainmentRule:
    """`_stopped_by` on synthetic code: which handler keeps a refusal in."""

    @staticmethod
    def _kind(src: str) -> Optional[str]:
        tree = ast.parse(src)
        fn = tree.body[0]
        call = next(c for c, _t in _guarded_calls(fn).items()
                    if getattr(c.func, "attr", None) == "read")
        return _stopped_by(_guarded_calls(fn)[call])

    @pytest.mark.parametrize("handlers,kind", [
        ("    except read_fallback.ReadUnavailable as exc:\n        return 1\n", "named"),
        ("    except Exception:\n        return 1\n", "broad"),
        ("    except:\n        return 1\n", "broad"),
        ("    except (KeyError, BaseException):\n        return 1\n", "broad"),
        ("    except OSError:\n        return 1\n", None),
        ("    except ReadUnavailable:\n        raise\n"
         "    except Exception:\n        return 1\n", None),
        ("    except ReadUnavailable as exc:\n        log(exc)\n        raise exc\n", None),
        # What follows an unconditional raise never runs.
        ("    except ReadUnavailable:\n        raise\n        return 1\n", None),
        # A return on the way is an exit that is not the raise.
        ("    except ReadUnavailable:\n        if x:\n            return 1\n"
         "        raise\n", "named"),
        # Something raised in its place is not the refusal passed on.
        ("    except Exception as exc:\n        raise RuntimeError('x') from exc\n", "broad"),
    ])
    def test_the_first_handler_that_catches_it_decides(self, handlers, kind):
        src = "async def f(store):\n    try:\n        await store.read()\n" + handlers
        assert self._kind(src) == kind

    def test_an_outer_try_catches_what_an_inner_one_raises_again(self):
        src = ("async def f(store):\n"
               "    try:\n"
               "        try:\n"
               "            await store.read()\n"
               "        except Exception:\n"
               "            raise\n"
               "    except ReadUnavailable:\n"
               "        return 1\n")
        assert self._kind(src) == "named"

    def test_a_call_in_a_handler_is_not_guarded_by_its_own_try(self):
        src = ("async def f(store):\n"
               "    try:\n"
               "        pass\n"
               "    except Exception:\n"
               "        await store.read()\n")
        assert self._kind(src) is None


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

    def test_a_job_a_route_also_calls_is_still_a_consumer(self):
        """A job is invoked by reference, so it used to count as an entry
        only while nothing called it: give the Monday goals job a route that
        runs it directly, and the scheduler's invocation would have left the
        list. It is registered by value, and that keeps it there."""
        graph = build_graph()
        index = {fn.key: fn for fn in graph.functions}
        job = index["core/scheduler.py:BackgroundScheduler._run_seasonality_calc"]
        route = index["web/routes/chat.py:chat_status"]
        assert job in graph.referenced
        graph.calls[route].add(job)
        graph.callers[job].add(route)
        assert job in entry_points(graph)
        graph.referenced.discard(job)
        assert job not in entry_points(graph)

    def test_a_named_answer_on_a_swept_route_is_out_of_place(self):
        """The rule seen to bite: give `GET /api/chat/status` — swept, and
        holding the same `service = get_chat_service()` — a call into the
        assistant, and the tools' named answer is on a route that expects a
        503. The walk names that route."""
        graph = build_graph()
        index = {fn.key: fn for fn in graph.functions}
        status = index["web/routes/chat.py:chat_status"]
        chat = index["web/services/chat_service.py:ChatService.chat"]
        assert named_answers_out_of_place(graph) == {}
        graph.calls[status].add(chat)
        graph.callers[chat].add(status)
        assert named_answers_out_of_place(graph) == {
            "core/chat_tools.py:execute_tool": ["GET /api/chat/status"]}
