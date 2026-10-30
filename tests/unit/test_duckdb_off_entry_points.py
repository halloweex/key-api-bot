"""Stage 5 (PR-9): every way into the code that can reach the DuckDB file is
one the sweep opens — the static half of `test_duckdb_off_sweep.py`.

The sweep proves by running: under `KS_DUCKDB=off` nothing it runs reaches
the file. It runs what it can read off the running app — every route, every
socket, every job the scheduler registers, every event on the bus, every
tool, web's start and stop. What it cannot run is a way in it does not know
of: a callback handed to something outside the repository, a function the
runtime invokes by reference, a script. So this walks the code instead.

THE WALK

`test_read_fallback_consumers.build_graph`, over `core/`, `web/`, `scripts/`
and `deploy/`, with the calls it resolves by their name alone left out — a
walk that must not over-reach (`test_web_stop_grace.py` reads it the same
way). A function **opens the file** when its own body calls `connection()`
or `connect()` on the store — `store`, `self.store`, `get_store()`, or
`self` inside the store's own classes — or `open_file`. Everything that
reaches one of those through a call is followed up to its **entry points**:
the functions nothing in these trees calls, or that something hands on as a
value. The router layer's own methods are not entries — the store's mixins
and the `pg_*` readers are reached through the routers, and a method of
theirs nothing calls is dead code there, not a way in. That over-reaches on
purpose: a router's DuckDB branch, which a mode never takes under `off`,
still counts as reaching, so the question asked of each entry is not whether
it reaches but whether the sweep runs it.

EACH ENTRY IS ONE OF

- a door the sweep opens — an app endpoint (HTTP or WebSocket) or one of
  its dependencies, read off the app; a function the scheduler is handed
  (`_add_job(func=...)`); web's start and stop, and the handlers on the event
  bus;
- held by one of those: named as a value — not called — by a function that
  is itself a door or held, which is how the mirror job's comparisons, a
  route's background `run()` and a job's nested reader reach the runtime;
- a host tool, under `scripts/` or `deploy/` — `HOST_ENTRIES`, each with what
  it does under `off` (their refusals and early exits are PR-10's);
- dead — no production caller at all — `DEAD_ENTRIES`, each with why it
  stays until stage 5 deletes code.

Both pins are exact: an entry nobody wrote down fails, and so does a pin no
entry needs any more.

WHAT IT CANNOT SEE

A call by `getattr` or through a table the walk cannot read, a function
named by a string, and the order of anything — whether a mode routes a read
away from DuckDB is the sweep's to prove, by running it.
"""
from __future__ import annotations

import ast
import inspect
import pathlib
import re
from collections import defaultdict
from typing import Dict, Set

import pytest

from tests.unit.test_read_fallback_consumers import (
    STORE_RECEIVER, Fn, Graph, _in_router_layer, _own, build_graph,
)

REPO = pathlib.Path(__file__).resolve().parents[2]
WALKED = ("core", "web", "scripts", "deploy")
OPENERS = {"connection", "connect"}

# The host tools whose entries reach the store, and what each does under
# off. They run on the host or in a sidecar, never in web's process, and
# none is part of the week: `duckdb_switch` refuses each before the driver
# runs (`test_duckdb_switch.py` pins their opener use; PR-10 their early
# exits and messages).
HOST_ENTRIES: Dict[str, str] = {
    "deploy/measure_dlr_throughput.py:main": (
        "a one-off measurement of the DLR path; retired with PR-10"),
    "deploy/measure_dlr_throughput.py:rebuild_like": (
        "the same one-off's rebuild; retired with PR-10"),
    "scripts/backfill_gender.py:main": (
        "`--duckdb` reads the file; refused under off by the switch"),
    "scripts/backfill_utm.py:backfill_utm": (
        "re-parses in Postgres alone under step 13 (PR-6); the store is "
        "unconnected under off"),
    "scripts/chain_copy_back.py:main": (
        "the way back; refused under off, exit 2 (PR-0) — `-e KS_DUCKDB=on` "
        "is the decision to restart both clocks"),
    "scripts/check_date.py:main": (
        "compares DuckDB's counts with KeyCRM's; refused under off (PR-10)"),
    "scripts/compact_duckdb.py:main": (
        "the Sunday compaction's sidecar; refused under off before the "
        "driver runs, its cron line retired at the start of the week"),
    "scripts/force_resync.py:main": (
        "rebuilds DuckDB from KeyCRM; refused under off (PR-10)"),
    "scripts/weekly_report_preview.py:main": (
        "a preview on a laptop; reads through the report's own readers"),
}

# Entries no production code calls. Kept, not deleted: OD-11 forbids
# nothing here, but deleting code is stage 5's last step, not its first.
DEAD_ENTRIES: Dict[str, str] = {
    "core/sync_service.py:SyncService.sync_today": (
        "no production caller; tests drive it as one of the three sync paths"),
    "web/services/auth_service.py:get_user_role": (
        "imported by web/routes/auth.py and never called; the design names "
        "it for deletion at stage 5"),
}


def opens_the_file(fn: Fn, graph: Graph) -> bool:
    """`fn`'s own body opens the file: `connection()`/`connect()` on the
    store, or `open_file`."""
    in_store = (fn.rel, fn.cls) in graph.store
    for node in _own(fn.node):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in OPENERS:
            receiver = ast.unparse(func.value)
            if STORE_RECEIVER.search(receiver) or (receiver == "self" and in_store):
                return True
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if name == "open_file":
            return True
    return False


def resolved_edges(graph: Graph) -> Dict[Fn, Set[Fn]]:
    """Calls the walk resolved by more than a name alone."""
    return {fn: set().union(*(targets for call, targets in found
                              if call not in graph.guessed)) if found else set()
            for fn, found in graph.sites.items()}


def reaching_entries(graph: Graph) -> Set[Fn]:
    """Entry points that reach a function opening the file, outside the
    switch itself and the router layer."""
    edges = resolved_edges(graph)
    callers: Dict[Fn, Set[Fn]] = defaultdict(set)
    for fn, targets in edges.items():
        for target in targets:
            callers[target].add(fn)
    reach = {fn for fn in graph.functions
             if fn.rel != "core/duckdb_switch.py" and opens_the_file(fn, graph)}
    changed = True
    while changed:
        changed = False
        for fn, targets in edges.items():
            if fn not in reach and targets & reach:
                reach.add(fn)
                changed = True
    return {fn for fn in reach
            if (not callers[fn] or fn in graph.referenced)
            and not _in_router_layer(fn, graph)}


def _key_of(func) -> str:
    """`rel:qual` of a live function, as the walk keys it — a nested
    function by its own name, as `Fn.qual` does."""
    func = inspect.unwrap(func)
    rel = pathlib.Path(inspect.getsourcefile(func)).resolve().relative_to(REPO).as_posix()
    parts = [p for p in func.__qualname__.split(".") if p != "<locals>"]
    owner = func.__qualname__.split(".")
    # A method keeps its class; a function nested in a function is its name.
    if len(owner) >= 2 and owner[-2] != "<locals>":
        return f"{rel}:{owner[-2]}.{owner[-1]}"
    return f"{rel}:{parts[-1]}"


def app_doors() -> Set[str]:
    """Every endpoint of the app — HTTP and WebSocket — and every dependency
    that runs before one, read off the app."""
    from tests.routes_helper import iter_endpoints
    from web.main import app

    found = set()
    for endpoint in iter_endpoints(app):
        found.add(_key_of(endpoint.route.endpoint))
        for dependency in endpoint.dependencies:
            if inspect.isfunction(dependency) or inspect.ismethod(dependency):
                found.add(_key_of(dependency))
    return found


def scheduled(graph: Graph) -> Set[str]:
    """What the scheduler is handed: `self._add_job(func=self._run_x)`."""
    found = set()
    for node in ast.walk(graph.trees["core/scheduler.py"]):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "_add_job":
            for keyword in node.keywords:
                if keyword.arg == "func" and isinstance(keyword.value, ast.Attribute):
                    found.add(f"core/scheduler.py:BackgroundScheduler.{keyword.value.attr}")
    return found


def web_handlers(graph: Graph) -> Set[str]:
    """Web's start and stop (`@app.on_event`), and the event bus's handlers
    (`@events.on`)."""
    return {fn.key for fn in graph.functions
            if any(re.search(r"\b(on_event|events\.on)\(", ast.unparse(d))
                   for d in fn.node.decorator_list)}


def named_values(fn: Fn, graph: Graph) -> Set[str]:
    """Names `fn` hands on rather than calls: used as a value — a list of
    callables, a `func=`, a `create_task(run())` — or called inside a lambda
    of its own, which runs when whatever it was handed to calls it (the
    mirror job's `check(name, lambda: reconcile_x(store))`). The walk keeps
    a lambda's calls out of the function that wrote it, so without this they
    would read as calls nobody makes."""
    called = {id(n.func) for n in _own(fn.node) if isinstance(n, ast.Call)}
    names = set()
    for node in _own(fn.node):
        if isinstance(node, ast.Lambda):
            for inner in ast.walk(node.body):
                if isinstance(inner, ast.Call):
                    f = inner.func
                    names.add(f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", ""))
            continue
        if id(node) in called or not isinstance(getattr(node, "ctx", None), ast.Load):
            continue
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
    return names - {""}


def classify(graph: Graph) -> Dict[str, str]:
    """Each reaching entry → the kind of way in it is, or `unexplained`."""
    entries = reaching_entries(graph)
    doors = app_doors() | scheduled(graph) | web_handlers(graph)
    kinds = {fn.key: "door" for fn in entries if fn.key in doors}
    # Held: whatever a covered function calls, a module-level function or
    # method it names as a value — in its own module, or imported into it by
    # that name — and a function nested in it. To a fixed point, from the
    # doors: a held function may hold others (a route's background `run()`
    # calls a helper whose own nested reader is the entry).
    edges = resolved_edges(graph)
    covered = {fn for fn in graph.functions if fn.key in doors}
    by_name: Dict[str, Set[Fn]] = defaultdict(set)
    nested: Dict[Fn, Set[Fn]] = defaultdict(set)
    for fn in graph.functions:
        if fn.outer is None:
            by_name[fn.name].add(fn)
        else:
            nested[fn.outer].add(fn)
    frontier = list(covered)
    while frontier:
        fn = frontier.pop()
        held = set(edges.get(fn, ())) | nested[fn]
        imported = _imported_names(fn, graph)
        for name in named_values(fn, graph):
            held |= {h for h in by_name.get(name, ())
                     if h.rel == fn.rel or name in imported}
        for h in held - covered:
            covered.add(h)
            frontier.append(h)
    covered_keys = {fn.key for fn in covered}
    for fn in entries:
        if fn.key in kinds:
            continue
        if fn.key in covered_keys:
            kinds[fn.key] = "held"
        elif fn.key in HOST_ENTRIES:
            kinds[fn.key] = "host"
        elif fn.key in DEAD_ENTRIES:
            kinds[fn.key] = "dead"
        else:
            kinds[fn.key] = "unexplained"
    return kinds


def _imported_names(fn: Fn, graph: Graph) -> Set[str]:
    """Names bound by an import in `fn`, any function enclosing it, or its
    module's top level."""
    names = set()
    nodes = list(graph.trees[fn.rel].body)
    g = fn
    while g is not None:
        nodes.extend(_own(g.node))
        g = g.outer
    for node in nodes:
        if isinstance(node, ast.ImportFrom):
            names.update(a.asname or a.name for a in node.names)
    return names


@pytest.fixture(scope="module")
def graph() -> Graph:
    return build_graph(WALKED)


@pytest.fixture(scope="module")
def kinds(graph) -> Dict[str, str]:
    return classify(graph)


class TestEveryWayIn:
    def test_every_entry_is_a_door_the_sweep_opens_or_pinned(self, kinds):
        unexplained = sorted(k for k, kind in kinds.items() if kind == "unexplained")
        assert not unexplained, (
            "ways into the code that can reach the DuckDB file and that the "
            "sweep does not open — make each a door the sweep opens (and so "
            f"proves), or pin it with a reason: {unexplained}")

    def test_the_pins_are_exact(self, kinds):
        """A pin no entry needs — a tool deleted, a dead function given a
        caller or removed — fails, so the lists say only what is true."""
        stale = sorted((set(HOST_ENTRIES) | set(DEAD_ENTRIES))
                       - {k for k, kind in kinds.items() if kind in ("host", "dead")})
        assert not stale, f"pins no entry needs any more: {stale}"
        misplaced = sorted(k for k in HOST_ENTRIES if not k.startswith(("scripts/", "deploy/")))
        assert not misplaced, misplaced

    def test_the_walk_finds_the_ways_in(self, kinds):
        """Non-vacuity, by kind and by name: the routes, the jobs, web's
        start, and the callables the mirror job and the routes hand on."""
        count = defaultdict(int)
        for kind in kinds.values():
            count[kind] += 1
        # 135 doors until step 14's S14-08, which took the seventeen /sms and
        # access routes below off the file (118 after it). Each step-14 PR
        # that removes a DuckDB branch shrinks this; the floor is only that
        # the walk still finds the ways in, not a count to keep.
        assert count["door"] >= 100, dict(count)
        assert count["held"] >= 10, dict(count)
        assert kinds["web/main.py:startup_event"] == "door"
        assert kinds["core/scheduler.py:BackgroundScheduler._run_dq_digest"] == "door"
        assert kinds["web/routes/websocket.py:dashboard_websocket"] == "door"
        assert kinds["web/routes/auth.py:require_admin"] == "door"
        assert kinds["core/mirror_reconciliation.py:reconcile_operational"] == "held"
        assert kinds["core/scheduler.py:BackgroundScheduler.read_duckdb"] == "held"

    def test_sms_and_access_reach_no_file(self, kinds):
        """Step 14's S14-08: `_sms_tx`, `_sms_run`, the audience, `_users_run`
        and `_perms_run` lost their DuckDB branches, so none of these routes
        reaches the file any more — not even by a branch a mode never takes,
        which is what the walk counts (135 doors before, 118 after)."""
        gone = {
            "web/routes/api/customers.py:" + name for name in (
                "add_marketing_optout", "create_sms_campaign",
                "delete_sms_audience_preset", "export_sms_segments_csv",
                "get_sms_campaign_results", "list_sms_audience_presets",
                "list_sms_campaigns", "mark_sms_campaign_sent",
                "save_sms_audience_preset", "send_sms_campaign")
        } | {
            "web/routes/api/users.py:" + name for name in (
                "approve_access_request", "get_user", "list_users",
                "update_user_features", "update_user_role", "update_user_status")
        } | {"web/routes/api/webhooks.py:turbosms_delivery_report"}
        still = sorted(gone & set(kinds))
        assert not still, f"these reach the DuckDB file again: {still}"

    def test_a_call_by_name_alone_is_not_followed(self, graph):
        """`self._thread.start()` in the bot store's loop is not the
        scheduler's `start`; followed, it would make a logging filter and a
        thread a way into the file. Mutation: keep the guessed edges — the
        filter below becomes an entry that reaches."""
        keys = {fn.key for fn in reaching_entries(graph)}
        assert "core/observability.py:RowDumpFilter.filter" not in keys
