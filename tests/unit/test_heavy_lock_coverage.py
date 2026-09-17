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
        asyncio.run(service.reconcile_with_api(days_back=2, lock=lock))

    assert seen == {"fetch_locked": False, "write_locked": True, "mark_locked": True}
