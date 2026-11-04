"""Every actor that writes DuckDB orders and then mirrors them holds the
scheduler's heavy-job lock across both.

The three scheduled syncs always did. The manual paths did not, and that is
where a fresher order got overwritten in Postgres by an older snapshot and
where header-only orders were manufactured: a resync, a status refresh or a
backfill chunk interleaving with the minute sync. Structural, because the
defect was structural — which call sites hold the lock.
"""
import inspect

from core import scheduler as scheduler_module
from web.routes.api import admin


def _source(module, name):
    return inspect.getsource(getattr(module, name))


def test_the_manual_resync_holds_the_lock_at_the_route():
    src = _source(admin, "trigger_resync")
    assert "_heavy_job_lock" in src
    # ...and not inside full_sync, which the weekly job calls already holding it.
    from core.sync_service import SyncService

    assert "_heavy_job_lock" not in inspect.getsource(SyncService.full_sync)


def test_the_manual_status_refresh_holds_the_lock():
    assert "_heavy_job_lock" in _source(admin, "refresh_order_statuses")


def test_the_manual_warehouse_refresh_holds_the_lock():
    """Interleaved with the two-minute job it produced false validation
    failures and raced its fired/resolved notices."""
    assert "_heavy_job_lock" in _source(admin, "refresh_warehouse")


# `test_the_backfill_route_passes_the_lock` held `POST
# /api/mirror/backfill/orders` to handing the backfill the lock, until step
# 14's S14-13 deleted the route with `core.pg_backfill`.


def test_every_repair_caller_passes_the_lock():
    src = inspect.getsource(scheduler_module.BackgroundScheduler)
    calls = [line for line in src.splitlines() if "repair_orders(" in line]
    assert calls, "no repair_orders callers found"
    for line in calls:
        # Either on the same line or the argument list continues below.
        idx = src.index(line)
        assert "lock=self._heavy_job_lock" in src[idx: idx + 200], line


def test_every_writer_of_orders_outside_core_answers_for_the_lock():
    """Walks `web/` and `scripts/` for a function that writes the orders — an
    UPDATE against `orders` in its SQL, or a call of chain 3's
    `restore_manager_comments` — and requires it to say how it is serialised
    against the sync.

    Named subjects were tried and are the reason this exists: the two
    `manager_comment` backfills (DN-17) updated DuckDB directly and neither
    held the lock nor reached Postgres, and no test noticed because every
    lock test above names the function it guards. A guard that names its
    subjects only ever guards the ones you were already thinking about —
    the same lesson as `docker rm -v` in the gate scripts.

    **Since step 14's S14-09 neither backfill writes DuckDB**: both restore
    the comments through `pg_orders_write.restore_manager_comments`, chain
    3's, and their DuckDB `UPDATE orders` went with `_orders_in_postgres`.
    The rule carries over unchanged to that call — it is the same write,
    racing the same sync, in the store the sync now writes.

    Two acceptable answers, because the callers are genuinely different:
    `_heavy_job_lock` for anything running inside the web process, and
    `core.pg_orders_write.WEB_MUST_BE_STOPPED` (in `core.pg_backfill` until
    step 14's S14-13) for a CLI, where that lock would be
    a fresh object guarding nothing. The answer may be given by the function
    or by a function of the same module that calls it (the CLI warns in
    `backfill_utm`, its entry point, and writes in the helper it calls).

    **The two are looked for differently, and the difference is the point.**
    The lock is `async with get_scheduler()._heavy_job_lock`, an attribute the
    code evaluates, and that is all it can be. The constant is not a magic
    word — the whole mechanism separating that CLI from a sync tick is an
    operator reading a sentence — so it has to be an **argument to a call the
    function makes**, which is what `logger.warning(...)` is. Two drafts were
    weaker: the first read `ast.get_source_segment`, so a comment three
    paragraphs below the deleted line still said the words; the second
    collected `ast.alias` nodes, so the bare `from core.pg_orders_write import
    WEB_MUST_BE_STOPPED` that a deleted warning leaves behind was accepted as
    proof the operator had been told. Both were found by mutation, the second
    by the review's.
    """
    import ast
    import pathlib
    import re

    repo = pathlib.Path(__file__).resolve().parents[2]
    LOCK, WARNING = "_heavy_job_lock", "WEB_MUST_BE_STOPPED"
    WRITER = "restore_manager_comments"
    updates = re.compile(r"update\s+(?:bronze\.)?orders\s+set", re.IGNORECASE)

    def _evaluated(node):
        """Every identifier the function evaluates. Imports are not evaluation
        — an unused name proves only that somebody once needed it."""
        out = set()
        for n in ast.walk(node):
            if isinstance(n, ast.Attribute):
                out.add(n.attr)
            elif isinstance(n, ast.Name):
                out.add(n.id)
        return out

    def _passed_to_a_call(node):
        """Every identifier handed to some call as an argument or keyword."""
        out = set()
        for n in ast.walk(node):
            if not isinstance(n, ast.Call):
                continue
            for arg in list(n.args) + [k.value for k in n.keywords]:
                for sub in ast.walk(arg):
                    if isinstance(sub, ast.Name):
                        out.add(sub.id)
                    elif isinstance(sub, ast.Attribute):
                        out.add(sub.attr)
        return out

    def _calls(node):
        return {(n.func.id if isinstance(n.func, ast.Name) else getattr(n.func, "attr", ""))
                for n in ast.walk(node) if isinstance(n, ast.Call)}

    found = []
    for root in ("web", "scripts"):
        for path in sorted((repo / root).rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
            # Docstrings are excluded deliberately: the prose in these modules
            # discusses updating orders, and a search that could not tell
            # prose from SQL would match the sentence explaining the rule.
            docstrings = set()
            for node in ast.walk(tree):
                if isinstance(node, (ast.Module, ast.ClassDef,
                                     ast.FunctionDef, ast.AsyncFunctionDef)):
                    first = node.body[0] if node.body else None
                    if (isinstance(first, ast.Expr)
                            and isinstance(first.value, ast.Constant)
                            and isinstance(first.value.value, str)):
                        docstrings.add(id(first.value))
            functions = [
                n for n in ast.walk(tree)
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            ]
            writes = [
                node for node in ast.walk(tree)
                if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                    and id(node) not in docstrings
                    and updates.search(" ".join(node.value.split())))
                or (isinstance(node, ast.Call)
                    and WRITER in {getattr(node.func, "attr", None),
                                   getattr(node.func, "id", None)})
            ]
            for node in writes:
                # Every function containing the write, innermost to
                # outermost — a nested helper may be wrapped by the caller
                # that takes the lock — and, to a fixed point, every function
                # of the module that calls one of them.
                enclosing = [
                    f for f in functions
                    if f.lineno <= node.lineno <= (f.end_lineno or f.lineno)
                ]
                assert enclosing, f"{path.name}: an orders write outside any function"
                answering = list(enclosing)
                while True:
                    names = {f.name for f in answering}
                    more = [f for f in functions
                            if f not in answering and _calls(f) & names]
                    if not more:
                        break
                    answering += more
                evaluated = set().union(*(_evaluated(f) for f in answering))
                told = set().union(*(_passed_to_a_call(f) for f in answering))
                found.append(f"{path.name}:{node.lineno}")
                assert LOCK in evaluated or WARNING in told, (
                    f"{path.name}:{node.lineno} writes orders without taking "
                    f"the heavy-job lock, and without passing {WARNING} to "
                    f"anything that would put it in front of an operator"
                )

    assert len(found) >= 2, f"no orders write walked: {found}"
