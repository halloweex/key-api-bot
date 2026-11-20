"""Every write chain's writers, read off the chain modules: what a first write
latches.
"""
from __future__ import annotations

import ast
import inspect
import pathlib

from core import write_chains


# Getting a connection. A public function of a chain module that reaches one
# of these is a writer unless it is named in `_READERS` below — so a writer is
# found by what it holds, not by whether the walk can read its statement.
_CONNECTION = {"get_pool", "_pool", "acquire"}


# The public functions of a chain module that take a connection only to read.
# Named rather than inferred: "reaches a connection" is how a writer is found,
# and these are the exceptions to it. `TestTheWalk` holds each one to writing
# nothing, so a write added to one fails there instead of hiding here.
_READERS = {
    "pg_inventory_write": {"read_snapshot_calendar"},
    # Chains 3's, 1's and 5's `preflight` were readers too, until step 14's
    # S14-09 (chain 3) and S14-11 (chains 1 and 5) deleted them.
    # Chain 9 (OD-02 (c)): the connection the journal's readers are handed —
    # the digest, the layer ages, the data-quality endpoint, the catch-up.
    "pg_dq_journal_write": {"reading"},
    # Chains 11a/11b: the gate's question, asked of the writer's store.
    "pg_weekly_ledger_write": {"find_sent"},
    "pg_traffic_ledger_write": {"find_sent"},
}


# A statement that writes, by how it starts. Upper-cased first; `setval` is a
# write to a sequence however the statement starts.
_WRITE_VERBS = ("INSERT", "UPDATE", "DELETE", "CREATE TEMP", "TRUNCATE")


def _index(source: str):
    """The module's functions by name, and the strings each module-level
    constant holds — what `_reached` follows out of a function body."""
    tree = ast.parse(source)
    funcs = {n.name: n for n in tree.body
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    consts = {}
    for n in tree.body:
        targets = (n.targets if isinstance(n, ast.Assign)
                   else [n.target] if isinstance(n, ast.AnnAssign) and n.value else [])
        for target in targets:
            if isinstance(target, ast.Name):
                consts[target.id] = [c.value for c in ast.walk(n.value)
                                     if isinstance(c, ast.Constant)
                                     and isinstance(c.value, str)]
    return tree, funcs, consts


def _reached(node, funcs, consts):
    """`(call names, strings)` a function reaches: its own body, every private
    function of the module it names — called or passed — transitively, and the
    strings of every module-level constant any of them names.

    The function body alone was the walk until 2026-09-25, and a public writer
    whose statement sat in a private helper or a constant was invisible to all
    four guards and to the integration list built from them (review of the
    latch-inside-acquire change, reproduced with `touch_expense`)."""
    seen, stack = set(), [node]
    calls, strings = set(), []
    while stack:
        fn = stack.pop()
        if fn.name in seen:
            continue
        seen.add(fn.name)
        for n in ast.walk(fn):
            if isinstance(n, ast.Call):
                calls.add(getattr(n.func, "attr", getattr(n.func, "id", "")))
            elif isinstance(n, ast.Constant) and isinstance(n.value, str):
                strings.append(n.value)
            elif isinstance(n, ast.Name):
                if n.id.startswith("_") and n.id in funcs and n.id != fn.name:
                    stack.append(funcs[n.id])
                strings.extend(consts.get(n.id, ()))
    return calls, strings


def _writes(strings) -> bool:
    return any(s.strip().upper().startswith(_WRITE_VERBS) or "SETVAL(" in s.upper()
               for s in strings)


def _writers_in(source: str, readers=frozenset()) -> dict:
    """`_writers` over a module's source — separate so the walk's own tests
    can hand it the shapes it used to miss."""
    tree, funcs, consts = _index(source)
    out = {}
    for node in tree.body:
        if (not isinstance(node, ast.AsyncFunctionDef) or node.name.startswith("_")
                or node.name in readers):
            continue
        calls, strings = _reached(node, funcs, consts)
        if calls & (_CONNECTION | {"_insert", "_upsert"}) or _writes(strings):
            out[node.name] = node
    return out


def _source(module) -> str:
    return pathlib.Path(inspect.getfile(module)).read_text(encoding="utf-8")


def _writers(module) -> dict:
    """`{name: node}` for every public async function in a chain module that
    reaches a connection or a statement that writes — through the module's
    private helpers and constants too — except the readers `_READERS` names.

    Parsed rather than listed, because a sixth inventory writer added without
    the latch is exactly the case this has to fail on. Found by the connection
    first and the statement second: a writer is what holds a connection, and a
    walk that had to *read* the statement missed every writer whose statement
    it could not see."""
    readers = _READERS.get(write_chains.chain_name(module), frozenset())
    return _writers_in(_source(module), readers)
