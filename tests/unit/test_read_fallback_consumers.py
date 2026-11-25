"""Every consumer no swept HTTP request is waiting for answers a refusal
itself (DN-20c): where a refusal goes, and where it stops.

A router whose engine fails raises `ReadUnavailable` (`read_fallback.refuse`),
whatever any variable says. An HTTP GET route turns it into a 503 naming the
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
a call to `read_fallback.refuse` (until step 14's S14-16 also `fall_back`,
`no_address`, `no_engine` and `chain_refusal`) — and follows callers up
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

THE KEPT ANSWERS

The scheduler's, the sync's, the chat's and the forecast's handlers that name
`ReadUnavailable` stay alive though since step 14's S14-07 no refusal reaches
some of them (`KEPT_ANSWERS`): the plan keeps every one through S14-16, and a
refusal that reaches one again finds an answer already there. Each is held
to still hold its handler, and the HTTP rule that a handler naming the
refusal must raise it exempts exactly these.

A SCRIPT IS A PERSON AT A TERMINAL, AND THE BOT REFUSES NOTHING

Until S14-16 a refusal happened only in a process that read
`KS_READ_FALLBACK` — whoever called `core.runtime_modes.configure_modes()`.
It happens wherever a router's engine fails now, so the rule is said by what
a process is instead. A script in `scripts/` is run by somebody watching it:
a refusal reaches them as the exception it is, naming its surface, and it is
not one of DN-20c's consumers, which answer for a job nobody watches. The
scripts that reach one are derived and pinned (`SCRIPT_ENTRIES`), so a new
one is seen. And no function of `bot/` reaches a refusal at all — it has no
routers — which a test below derives, so `bot/` stays outside the walk.
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
from tests.helpers.read_fallback_answers import named_answer_functions

REPO = pathlib.Path(__file__).resolve().parents[2]
WALKED = ("core", "web", "scripts")
# A router refuses its one engine's failure (step 14, S14-05). Qualified,
# because a route's own validation helper is called `refuse` too
# (`web/routes/api/customers.py`) and refuses nothing. The bare names that
# refused too — `fall_back`, `no_address`, `no_engine`, `chain_refusal` —
# went in S14-16.
QUALIFIED_REFUSERS = {("read_fallback", "refuse")}


def _names_a_refusal(call: ast.Call) -> bool:
    f = call.func
    return (isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name)
            and (f.value.id, f.attr) in QUALIFIED_REFUSERS)
# A receiver the store is reached through: `store`, `self.store`,
# `self._store`, `get_store()`, `(await get_store())`.
STORE_RECEIVER = re.compile(r"(^|[._])store$|get_store\(\)")


# ─── The pins ─────────────────────────────────────────────────────────────

# DN-20c's list: every entry point a refusal can reach that is no HTTP
# request's. Each must get an answer of its own — defer, keep the previous
# model, skip the step for the tick — and none may half-write on the way.
NON_HTTP_CONSUMERS = {
    # The Monday goals job, which writes seasonal_indices once every read it
    # makes has answered (chain 7b-1).
    "core/scheduler.py:BackgroundScheduler._run_seasonality_calc",
    # The weekly traffic message, through the tab's router.
    "core/scheduler.py:BackgroundScheduler._run_traffic_report",
    # The month's goal freeze (revision 0037): `generate_smart_goals` reads
    # through `/goals`' router, every sales type's answer before any row.
    "core/scheduler.py:BackgroundScheduler._run_month_goal_freeze",
    # Gone with step 14's S14-07: the sync and its buyers step, the search
    # index, training, the weekly sales report and the boot reached a
    # refusal only through `no_address` on a switch, and no reader asks it.
    # Their reads have no fallback: a Postgres failure is their own error,
    # as it always was.
}

# The assistant's routes. HTTP, but its tools run inside a conversation,
# where a 503 cannot be sent once the stream is open, so a refusal is a
# named `data_unavailable` tool result the model reads and relays. The only
# routes a named answer may take the 503 handler's place on.
IN_BAND_ROUTES = {
    "GET /api/chat/stream",
    "POST /api/chat",
}

# DN-20c: where each consumer's refusal stops, as `file:function:kind`.
# `named` is a handler naming `ReadUnavailable` that answers it — the job's
# result says `read_unavailable` and the surface. `broad` is an `except
# Exception` that was there already and contains every failure of a step
# the same way, a refusal included, and the boot keeps serving. Derived and
# compared, like the lists above: a handler added on a consumer's path — or
# one taken away, which would let a refusal out as an exception — fails here
# until somebody writes it down.
#
# The buyers step records a refusal and passes it on since chain 4's PR-3:
# the tick names it at the call (the plan's "record the failure and skip that
# step") and the manual route below reaches the 503. It used to stop in the
# step's own `except Exception`, which answered both. The tick's stats are
# summed and carry no strings, so `/api/jobs` still shows a step with no
# buyers; the refusal is in the log, in `buyer_sync`'s error class and in
# `refusals()`.
_BUYERS = "core/sync_service.py:SyncService.incremental_sync:named"

KEPT_ENTRIES = {
    "core/scheduler.py:BackgroundScheduler._run_weekly_report",
    "core/scheduler.py:BackgroundScheduler._run_revenue_prediction",
    "core/scheduler.py:BackgroundScheduler._run_meilisearch_sync",
    "core/scheduler.py:BackgroundScheduler._run_incremental_sync",
    "web/main.py:startup_event",
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
    # The calls resolved by name alone: `x.m()` on a receiver nothing types,
    # taken for the one method of that name in the repository. Sound for the
    # refusal walk, which must over-reach; a walk that must not — the slow
    # endpoints' DuckDB writes (`test_web_stop_grace.py`) — leaves them out.
    guessed: Set[ast.Call] = field(default_factory=set)


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

    # The store: AnalyticsStore and the mixins it is composed of (it was
    # `DuckDBStore` in `core/duckdb_store.py` until step 14's S14-18, which
    # left that module a shim of the name until the final stage deleted it).
    store_def = classes[("core/store.py", "AnalyticsStore")]
    composed = {b.id for b in store_def.bases if isinstance(b, ast.Name)}
    composed.add("AnalyticsStore")
    store = {k for k in classes if k[1] in composed and (
        k[0].startswith("core/repositories/") or k[0] == "core/store.py")}

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

    guessed: Set[ast.Call] = set()

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
                guessed.add(call)
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
            if isinstance(n, ast.Call) and _names_a_refusal(n):
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
    return Graph(functions, calls, callers, reaching, store, trees, sites, referenced,
                 guessed)


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
    allowed = NON_HTTP_CONSUMERS | IN_BAND_ROUTES | KEPT_ENTRIES
    out: Dict[str, List[str]] = {}
    for fn_key in sorted(named_answer_functions()):
        labels = set()
        for entry in entries_reaching(graph, index[fn_key]):
            served = routes.get(entry)
            labels |= ({f"{m} {p}" for m, p in served} if served else {entry.key})
        if labels - allowed:
            out[fn_key] = sorted(labels - allowed)
    return out


@pytest.fixture(scope="module")
def graph() -> Graph:
    return build_graph()


# ─── The pins, derived ────────────────────────────────────────────────────


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
