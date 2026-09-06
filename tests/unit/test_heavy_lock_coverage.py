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


def test_the_backfill_route_passes_the_lock():
    src = inspect.getsource(admin)
    assert "lock=get_scheduler()._heavy_job_lock" in src


def test_every_repair_caller_passes_the_lock():
    src = inspect.getsource(scheduler_module.BackgroundScheduler)
    calls = [line for line in src.splitlines() if "repair_orders(" in line]
    assert calls, "no repair_orders callers found"
    for line in calls:
        # Either on the same line or the argument list continues below.
        idx = src.index(line)
        assert "lock=self._heavy_job_lock" in src[idx: idx + 200], line
