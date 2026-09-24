"""Every actor that writes DuckDB orders and then mirrors them holds the
scheduler's heavy-job lock across both.

The three scheduled syncs always did. The manual paths did not, and that is
where a fresher order got overwritten in Postgres by an older snapshot and
where header-only orders were manufactured: a resync, a status refresh or a
backfill chunk interleaving with the minute sync. Structural, because the
defect was structural — which call sites hold the lock.
"""
import inspect

import pytest

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


def test_the_backfill_route_passes_the_lock():
    src = inspect.getsource(admin)
    assert "lock=get_scheduler()._heavy_job_lock" in src


def test_every_reconciliation_caller_passes_the_lock():
    """The daily reconciliation resyncs drifted orders through the same writer
    as a repair, and wrote without the lock — found reviewing chain 2, where it
    would page a false Postgres validation failure. Walks every module that
    could call it rather than naming the two callers of today."""
    import ast
    import pathlib

    repo = pathlib.Path(__file__).resolve().parents[2]
    calls = []
    for root in ("core", "web", "bot", "scripts"):
        for path in (repo / root).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "reconcile_with_api"):
                    calls.append((path.name, node))
    assert calls, "no reconcile_with_api callers found"
    for name, node in calls:
        assert any(k.arg == "lock" for k in node.keywords), name


def test_every_repair_caller_passes_the_lock():
    src = inspect.getsource(scheduler_module.BackgroundScheduler)
    calls = [line for line in src.splitlines() if "repair_orders(" in line]
    assert calls, "no repair_orders callers found"
    for line in calls:
        # Either on the same line or the argument list continues below.
        idx = src.index(line)
        assert "lock=self._heavy_job_lock" in src[idx: idx + 200], line


def test_every_writer_of_duckdb_orders_outside_core_answers_for_the_lock():
    """Walks `web/` and `scripts/` for a function that executes an UPDATE
    against `orders`, and requires it to say how it is serialised against the
    sync.

    Named subjects were tried and are the reason this exists: the two
    `manager_comment` backfills (DN-17) updated DuckDB directly and neither
    held the lock nor reached Postgres, and no test noticed because every
    lock test above names the function it guards. A guard that names its
    subjects only ever guards the ones you were already thinking about —
    the same lesson as `docker rm -v` in the gate scripts.

    Two acceptable answers, because the callers are genuinely different:
    `_heavy_job_lock` for anything running inside the web process, and
    `core.pg_backfill.WEB_MUST_BE_STOPPED` for a CLI, where that lock would be
    a fresh object guarding nothing.

    **The two are looked for differently, and the difference is the point.**
    The lock is `async with get_scheduler()._heavy_job_lock`, an attribute the
    code evaluates, and that is all it can be. The constant is not a magic
    word — the whole mechanism separating that CLI from a sync tick is an
    operator reading a sentence — so it has to be an **argument to a call the
    function makes**, which is what `logger.warning(...)` is. Two drafts were
    weaker: the first read `ast.get_source_segment`, so a comment three
    paragraphs below the deleted line still said the words; the second
    collected `ast.alias` nodes, so the bare `from core.pg_backfill import
    WEB_MUST_BE_STOPPED` that a deleted warning leaves behind was accepted as
    proof the operator had been told. Both were found by mutation, the second
    by the review's.
    """
    import ast
    import pathlib
    import re

    repo = pathlib.Path(__file__).resolve().parents[2]
    LOCK, WARNING = "_heavy_job_lock", "WEB_MUST_BE_STOPPED"
    updates = re.compile(r"update\s+orders\s+set", re.IGNORECASE)

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
            for node in ast.walk(tree):
                if (not isinstance(node, ast.Constant)
                        or not isinstance(node.value, str)
                        or id(node) in docstrings
                        or not updates.search(" ".join(node.value.split()))):
                    continue
                # Every function containing the statement, innermost to
                # outermost — a nested helper may be wrapped by the caller
                # that takes the lock.
                enclosing = [
                    f for f in functions
                    if f.lineno <= node.lineno <= (f.end_lineno or f.lineno)
                ]
                assert enclosing, f"{path.name}: UPDATE orders outside any function"
                evaluated = set().union(*(_evaluated(f) for f in enclosing))
                told = set().union(*(_passed_to_a_call(f) for f in enclosing))
                found.append(f"{path.name}:{node.lineno}")
                assert LOCK in evaluated or WARNING in told, (
                    f"{path.name}:{node.lineno} updates DuckDB orders without "
                    f"taking the heavy-job lock, and without passing "
                    f"{WARNING} to anything that would put it in front of an "
                    f"operator"
                )

    assert len(found) >= 2, f"no UPDATE orders statements walked: {found}"


@pytest.mark.asyncio
async def test_the_comment_backfill_route_ships_what_it_changed_under_the_lock(tmp_path):
    """Behavioural twin of the walk above, for the route DN-17 rewired.

    Three claims, none of which the structural test can make: the KeyCRM
    fetch is outside the lock, the DuckDB write and the ship are inside one
    hold of it, and the ship carries only the orders whose comment actually
    moved. That last one is not a nicety — `bronze.orders` upserts
    `manager_comment` as `COALESCE(EXCLUDED, stored)`, so an order that
    already had a comment has nothing to carry, and shipping it anyway would
    be a version written for a row that did not change.
    """
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    from core.duckdb_store import DuckDBStore
    from web.routes.api import traffic

    lock = asyncio.Lock()
    seen = {}

    class Client:
        async def paginate(self, *_a, **_k):
            seen["fetch_locked"] = lock.locked()
            # Both orders come back from KeyCRM with a comment; only one of
            # them is missing it here.
            yield [{"id": 1, "manager_comment": "utm_source=fb"},
                   {"id": 2, "manager_comment": "utm_source=tg"}]

    async def ship(_store, ids, *, version_kind, **_kw):
        seen["ship_locked"] = lock.locked()
        seen["shipped"] = list(ids)
        seen["kind"] = version_kind
        return {"orders_shipped": len(ids)}

    store = DuckDBStore(db_path=tmp_path / "analytics.duckdb")
    await store.connect()
    try:
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total,"
                " ordered_at, buyer_id, manager_comment) VALUES"
                " (1, 1, 1, 10.0, TIMESTAMPTZ '2026-08-01 10:00:00+00', 1, 'kept'),"
                " (2, 1, 1, 10.0, TIMESTAMPTZ '2026-08-01 10:00:00+00', 2, NULL)"
            )
        scheduler = MagicMock()
        scheduler._heavy_job_lock = lock
        before = dict(traffic._backfill_status)
        with patch.object(traffic, "get_store", AsyncMock(return_value=store)), \
             patch("core.keycrm.get_async_client", AsyncMock(return_value=Client())), \
             patch("core.scheduler.get_scheduler", return_value=scheduler), \
             patch("core.pg_backfill.ship_orders_by_id", new=ship), \
             patch.object(traffic, "reparse_router", new=AsyncMock()), \
             patch("asyncio.sleep", new=AsyncMock()):
            # Bounded for the reason the reconciliation test below is: a lock
            # taken and never released is a deadlock, and a test that hangs on
            # one fails nobody in time.
            await asyncio.wait_for(traffic._run_backfill_inner(1), timeout=20)
        traffic._backfill_status.clear()
        traffic._backfill_status.update(before)

        async with store.connection() as conn:
            stored = dict(conn.execute(
                "SELECT id, manager_comment FROM orders ORDER BY id").fetchall())
    finally:
        await store.close()

    assert seen == {"fetch_locked": False, "ship_locked": True,
                    "shipped": [2], "kind": "backfill"}
    assert stored == {1: "kept", 2: "utm_source=tg"}


def test_the_reconciliation_resync_writes_inside_the_lock_and_fetches_outside_it():
    """Behavioural, because the caller test above only proves the lock is
    handed over — not that the write happens under it, nor that the KeyCRM
    fetch does not."""
    import asyncio
    from datetime import date, timedelta
    from unittest.mock import AsyncMock, MagicMock, patch

    from core.sync_service import SyncService

    lock = asyncio.Lock()
    seen = {}
    day = date.today() - timedelta(days=1)
    order = {"id": 7, "status_id": 1, "grand_total": 100,
             "ordered_at": f"{day.isoformat()}T12:00:00+03:00"}

    class Client:
        async def paginate(self, *_a, **_k):
            seen["fetch_locked"] = lock.locked()
            yield [order]

    store = MagicMock()
    store.get_order_summaries_by_date = AsyncMock(return_value={})
    store.log_reconciliation = AsyncMock(return_value={"status": "drift"})
    store.mark_warehouse_dirty = AsyncMock(
        side_effect=lambda *_a: seen.__setitem__("mark_locked", lock.locked()))
    service = SyncService(store=store)

    async def upsert(orders, force_update):
        seen["write_locked"] = lock.locked()
        return len(orders), 0

    with patch("core.sync_service.get_async_client", AsyncMock(return_value=Client())), \
         patch.object(service, "_upsert_orders_with_expenses", side_effect=upsert):
        # Bounded: a lock taken and never released is a deadlock, and a test
        # that hangs on it fails nobody in time. Found by a mutation that took
        # the lock around the fetch and stalled the run for ten minutes.
        asyncio.run(asyncio.wait_for(
            service.reconcile_with_api(days_back=2, lock=lock), timeout=5))

    assert seen == {"fetch_locked": False, "write_locked": True, "mark_locked": True}
