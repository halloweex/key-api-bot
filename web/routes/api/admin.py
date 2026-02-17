"""Admin operations: DuckDB management, warehouse, cache, jobs, sync, events."""
import asyncio
import logging

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional

from web.routes.auth import require_admin
from web.schemas import JobsResponse
from ._deps import limiter, get_store

router = APIRouter()
logger = logging.getLogger(__name__)


# ─── DuckDB Admin ─────────────────────────────────────────────────────────────

@router.post("/duckdb/resync")
@limiter.limit("1/minute")
async def trigger_resync(
    request: Request,
    days: int = 365,
    admin: dict = Depends(require_admin),
):
    """Force a complete resync of orders from KeyCRM API. Requires admin."""
    from core.sync_service import force_resync

    try:
        stats = await force_resync(days_back=days)
        return {
            "status": "success",
            "message": f"Resync complete - synced last {days} days",
            "stats": stats,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Resync failed: {str(e)}")


@router.post("/duckdb/refresh-statuses")
@limiter.limit("10/hour")
async def refresh_order_statuses(
    request: Request,
    days: int = Query(30, ge=1, le=90, description="Days to look back for status changes"),
    background: bool = Query(True, description="Run in background (recommended)"),
):
    """Refresh order statuses from KeyCRM API."""
    from core.sync_service import get_sync_service

    async def run_refresh():
        try:
            sync_service = await get_sync_service()
            result = await sync_service.refresh_order_statuses(days_back=days)
            logger.info(f"Background status refresh completed: {result}")
            return result
        except asyncio.CancelledError:
            logger.warning("Background status refresh was cancelled")
            raise
        except Exception as e:
            logger.error(f"Background status refresh failed: {e}", exc_info=True)
            raise

    if background:
        task = asyncio.create_task(run_refresh(), name=f"refresh_statuses_{days}d")
        task.add_done_callback(
            lambda t: logger.error(f"Background task failed: {t.exception()}")
            if t.exception() else None
        )
        return {
            "status": "started",
            "message": f"Status refresh started in background - checking last {days} days",
            "note": "Check /api/jobs for progress or wait ~60 seconds and verify data",
        }

    try:
        stats = await run_refresh()
        return {
            "status": "success",
            "message": f"Status refresh complete - checked last {days} days",
            "stats": stats,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Status refresh failed: {str(e)}")


# ─── Warehouse ─────────────────────────────────────────────────────────────────

@router.get("/warehouse/status")
@limiter.limit("60/minute")
async def get_warehouse_status(request: Request):
    """Get warehouse layer (Silver/Gold) status and last refresh info."""
    store = await get_store()
    return await store.get_warehouse_status()


@router.post("/warehouse/refresh")
@limiter.limit("5/minute")
async def refresh_warehouse(
    request: Request,
    admin: dict = Depends(require_admin),
):
    """Manually trigger warehouse layer refresh (Silver -> Gold). Requires admin."""
    store = await get_store()
    return await store.refresh_warehouse_layers(trigger="manual")


# ─── Buyer Sync ────────────────────────────────────────────────────────────────

@router.post("/duckdb/sync-buyers")
@limiter.limit("120/minute")
async def sync_buyers(
    request: Request,
    limit: int = Query(100, ge=1, le=500, description="Maximum buyers to sync"),
):
    """Manually sync missing buyers from KeyCRM."""
    from core.sync_service import get_sync_service

    try:
        sync_service = await get_sync_service()
        count = await sync_service.sync_missing_buyers(limit=limit)
        return {
            "status": "success",
            "message": f"Synced {count} buyers from KeyCRM",
            "buyers_synced": count,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Buyer sync failed: {str(e)}")


@router.post("/duckdb/sync-all-buyers")
@limiter.limit("1/hour")
async def sync_all_buyers(request: Request, admin: dict = Depends(require_admin)):
    """Sync ALL buyers from KeyCRM (including those without orders). Requires admin."""
    from core.keycrm import KeyCRMClient

    try:
        store = await get_store()

        async with store.connection() as conn:
            before_count = conn.execute("SELECT COUNT(*) FROM buyers").fetchone()[0]

        async with KeyCRMClient() as client:
            buyers = await client.fetch_all_buyers()

        if buyers:
            await store.upsert_buyers(buyers)

        async with store.connection() as conn:
            after_count = conn.execute("SELECT COUNT(*) FROM buyers").fetchone()[0]

        return {
            "status": "success",
            "message": "Synced all buyers from KeyCRM",
            "buyers_fetched": len(buyers),
            "before_count": before_count,
            "after_count": after_count,
            "new_buyers": after_count - before_count,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Full buyer sync failed: {str(e)}")


@router.get("/buyers/stats")
@limiter.limit("60/minute")
async def get_buyer_stats(request: Request):
    """Get buyer sync statistics."""
    store = await get_store()
    async with store.connection() as conn:
        orders_buyers = conn.execute(
            "SELECT COUNT(DISTINCT buyer_id) FROM orders WHERE buyer_id IS NOT NULL"
        ).fetchone()[0]
        silver_buyers = conn.execute(
            "SELECT COUNT(DISTINCT buyer_id) FROM silver_orders WHERE buyer_id IS NOT NULL"
        ).fetchone()[0]
        synced = conn.execute("SELECT COUNT(*) FROM buyers").fetchone()[0]
        missing = conn.execute("""
            SELECT COUNT(DISTINCT s.buyer_id)
            FROM silver_orders s
            LEFT JOIN buyers b ON s.buyer_id = b.id
            WHERE s.buyer_id IS NOT NULL AND b.id IS NULL
        """).fetchone()[0]

    return {
        "unique_in_orders": orders_buyers,
        "unique_in_silver_orders": silver_buyers,
        "synced_to_buyers_table": synced,
        "missing": missing,
    }


# ─── Cache ─────────────────────────────────────────────────────────────────────

@router.get("/cache/stats")
@limiter.limit("60/minute")
async def get_cache_stats(request: Request):
    """Get Redis cache statistics."""
    from core.cache import cache
    return cache.get_stats()


@router.post("/cache/invalidate")
@limiter.limit("10/minute")
async def invalidate_cache(
    request: Request,
    pattern: str = Query(..., description="Cache key pattern to invalidate"),
    admin: dict = Depends(require_admin),
):
    """Manually invalidate cache by pattern. Requires admin."""
    from core.cache import cache

    if not cache.is_connected:
        raise HTTPException(status_code=503, detail="Cache not connected")

    deleted = await cache.invalidate_pattern(pattern)
    return {"status": "success", "pattern": pattern, "keys_deleted": deleted}


# ─── Jobs & Sync ───────────────────────────────────────────────────────────────

@router.get("/jobs", response_model=JobsResponse)
@limiter.limit("60/minute")
async def get_jobs(request: Request):
    """Get background job scheduler status."""
    from core.scheduler import get_scheduler

    scheduler = get_scheduler()
    if scheduler is None:
        return {"status": "not_running", "jobs": [], "history": []}

    all_history = []
    jobs = scheduler.get_jobs()
    job_names = {j["id"]: j["name"] for j in jobs}

    for job in jobs:
        job_history = scheduler.get_job_history(job["id"], limit=5)
        for h in job_history:
            all_history.append({
                "job_id": job["id"],
                "job_name": job_names.get(job["id"], job["id"]),
                "started_at": h.get("started_at") or "",
                "completed_at": h.get("finished_at"),
                "duration_ms": h.get("duration_ms"),
                "status": h.get("status", "unknown"),
                "error": h.get("error"),
                "result": None,
            })
    all_history.sort(key=lambda x: x.get("started_at") or "", reverse=True)

    from core.sync_service import get_sync_service
    try:
        sync_service = await get_sync_service()
        sync_stats = sync_service.get_sync_stats()
    except Exception:
        sync_stats = None

    return {
        "status": "running",
        "jobs": jobs,
        "history": all_history[:20],
        "adaptive_sync": sync_stats,
    }


@router.post("/jobs/{job_id}/trigger")
@limiter.limit("5/minute")
async def trigger_job(
    request: Request,
    job_id: str,
    admin: dict = Depends(require_admin),
):
    """Manually trigger a background job. Requires admin."""
    from core.scheduler import get_scheduler

    scheduler = get_scheduler()
    if scheduler is None:
        raise HTTPException(status_code=503, detail="Scheduler not running")

    result = await scheduler.trigger_job(job_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    return {"status": "triggered", "job_id": job_id, "result": result}


@router.get("/sync/stats")
@limiter.limit("60/minute")
async def get_sync_stats(request: Request):
    """Get adaptive sync statistics."""
    from core.sync_service import get_sync_service

    try:
        sync_service = await get_sync_service()
        stats = sync_service.get_sync_stats()
        return {
            "status": "ok",
            **stats,
            "config": {
                "base_interval_seconds": sync_service.BACKOFF_BASE_SECONDS,
                "max_interval_seconds": sync_service.BACKOFF_MAX_SECONDS,
                "backoff_multiplier": sync_service.BACKOFF_MULTIPLIER,
                "off_hours": f"{sync_service.OFF_HOURS_START}:00 - {sync_service.OFF_HOURS_END}:00",
                "off_hours_interval_seconds": sync_service.OFF_HOURS_INTERVAL,
            },
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/events")
@limiter.limit("60/minute")
async def get_events(
    request: Request,
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    limit: int = Query(20, ge=1, le=100, description="Number of events to return"),
):
    """Get recent event history."""
    from core.events import events, SyncEvent

    filter_type = None
    if event_type:
        try:
            filter_type = SyncEvent(event_type)
        except ValueError:
            for et in SyncEvent:
                if et.name.lower() == event_type.lower():
                    filter_type = et
                    break

    return {
        "events": events.get_history(event_type=filter_type, limit=limit),
        "handlers": events.get_handlers(),
    }
