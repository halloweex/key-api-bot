"""Admin operations: DuckDB management, warehouse, cache, jobs, sync, events."""
import asyncio
import contextlib
import logging

from datetime import date

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional

from core import read_fallback
from web.routes.auth import require_admin
from web.schemas import JobsResponse
from ._deps import limiter, get_store, validate_db_int

router = APIRouter()
logger = logging.getLogger(__name__)

# Strong references to detached work, so nothing is collected mid-flight.
_BACKGROUND_TASKS: set = set()


# ─── DuckDB Admin ─────────────────────────────────────────────────────────────

@router.post("/duckdb/resync")
@limiter.limit("1/minute")
async def trigger_resync(
    request: Request,
    days: int = 365,
    admin: dict = Depends(require_admin),
):
    """Force a complete resync of orders from KeyCRM API. Requires admin."""
    from core.scheduler import get_scheduler
    from core.sync_service import force_resync

    try:
        # Under the heavy-job lock, like the weekly full sync that runs the
        # same code: a resync interleaving with the minute sync or the status
        # refresh is how header-only orders got manufactured and how a fresher
        # mirror write got overwritten by an older snapshot. Taken here, at
        # the route, never inside full_sync — the weekly job already holds it.
        async with get_scheduler()._heavy_job_lock:
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
    admin: dict = Depends(require_admin),
):
    """Refresh order statuses from KeyCRM API."""
    from core.sync_service import get_sync_service

    async def run_refresh():
        try:
            from core.scheduler import get_scheduler

            sync_service = await get_sync_service()
            # The 05:15 job holds the heavy-job lock across this same call;
            # the manual path did not, so it could interleave with the minute
            # sync and mirror an older snapshot over a fresher order.
            async with get_scheduler()._heavy_job_lock:
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
    """The warehouse's state: Postgres derives Silver and Gold alone.

    Until step 14's S14-15 the answer also carried `duckdb_frozen` — DuckDB's
    last refresh, as old as step 13's switch, read from the hourly copy of its
    `warehouse_refreshes` — and `cutover`, step 13's readiness (the variable,
    the recorded writer, every precondition by name). The switch is a
    constant and its module is deleted; nothing read either block, the
    frontend included. `app.warehouse_refreshes` stays as history (OD-S5-4)."""
    return {"writer": "postgres", "postgres": await _pg_derivation_status()}


async def _pg_derivation_status() -> dict:
    """The owed state and the last journalled run, or the error reading them."""
    from core import pg_derivation
    from core.pg import get_pool

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            requested, built, built_at = await pg_derivation.read_owed(conn)
            last = await conn.fetchrow(
                "SELECT trigger, started_at, ended_at, validation_passed, error"
                " FROM meta.derivation_runs ORDER BY id DESC LIMIT 1")
    except Exception as e:  # noqa: BLE001 — a status page reports, it does not fail
        return {"error": f"{type(e).__name__}: {e}"}
    return {
        "requested": requested,
        "built": built,
        "owed": requested > built,
        "built_at": built_at.isoformat() if built_at else None,
        "last_run": None if last is None else {
            "trigger": last["trigger"],
            "started_at": last["started_at"].isoformat(),
            "ended_at": last["ended_at"].isoformat() if last["ended_at"] else None,
            "validation_passed": last["validation_passed"],
            "error": last["error"],
        },
    }


async def _derive_postgres_now(trigger: str) -> dict:
    """Run the Postgres derivation immediately, past its floor; takes the
    heavy lock itself, so call it after releasing yours."""
    from core.scheduler import get_scheduler

    return await get_scheduler()._run_pg_derivation(trigger=trigger, force=True)


@router.post("/warehouse/refresh")
@limiter.limit("5/minute")
async def refresh_warehouse(
    request: Request,
    admin: dict = Depends(require_admin),
):
    """Rebuild the warehouse (Silver -> Gold) now, in Postgres. Requires admin.

    The lever every Silver/Gold alert names. Until step 14's S14-15 it ran
    DuckDB's rebuild first while DuckDB derived, and answered `skipped` for
    that half since step 13; the Postgres derivation is the whole of it now."""
    return {"trigger": "manual", "postgres": await _derive_postgres_now("manual")}


@router.post("/warehouse/rebuild-silver")
@limiter.limit("1/minute")
async def rebuild_silver_from_scratch(
    request: Request,
    admin: dict = Depends(require_admin),
):
    """Rebuild Silver from scratch — in Postgres, the derivation run past its
    floor, which rebuilds `silver.orders` whole (TRUNCATE and both passes in
    one transaction, `core.pg_silver.rebuild_silver`).

    Until step 14's S14-15 this DROPped, re-created and re-filled DuckDB's
    `silver_orders` first — a lever for DuckDB rows that blocked its
    DELETE+INSERT rebuild — and answered `skipped` for that half since step
    13, when DuckDB stopped deriving."""
    return {"postgres": await _derive_postgres_now("manual")}


# POST /api/duckdb/purge-orders was retired by the owner's decision OD-10
# (2026-09-30). It deleted orders from DuckDB's landing and Silver and ran a
# CHECKPOINT — a one-shot for the April 2026 DuckDB 1.5 MVCC incident — and
# nothing called it. It never reached Postgres, so it had already stopped
# removing anything from the numbers every tab reads, and the order it let
# the next sync re-insert came back with the payload's NULL `manager_comment`
# in DuckDB alone. Deleting an order from Postgres is chain 3's to design.


# ─── Buyer Sync ────────────────────────────────────────────────────────────────

# How long the manual buyer sync waits for the heavy-job lock before answering
# 409. It has to fit inside RequestTimeoutMiddleware's 30 s budget with room for
# the sync itself: at 60 s the middleware answered a bare 504 first, and the
# handler went on and wrote after that answer.
HEAVY_LOCK_WAIT_S = 20
# The full buyer sync runs detached, outside any request budget, and waits this
# long for the lock before each portion. A full sync or a resync holds the lock
# for minutes; the minute tick for seconds.
SYNC_ALL_LOCK_WAIT_S = 300


@contextlib.asynccontextmanager
async def _heavy_lock(wait_s: float):
    """The scheduler's heavy-job lock, waited for boundedly.

    Yields True with the lock held, False when it could not be had in time.
    `wait_for` over `acquire` cannot leak it: asyncio.Lock marks itself taken
    only after the waiter returns normally, so a timeout leaves it free.
    """
    from core.scheduler import get_scheduler

    lock = get_scheduler()._heavy_job_lock
    try:
        await asyncio.wait_for(lock.acquire(), wait_s)
    except asyncio.TimeoutError:
        yield False
        return
    try:
        yield True
    finally:
        lock.release()


@contextlib.asynccontextmanager
async def _heavy_lock_or_409(what: str):
    """The heavy-job lock for a request, or a 409 inside its time budget."""
    async with _heavy_lock(HEAVY_LOCK_WAIT_S) as held:
        if not held:
            raise HTTPException(
                status_code=409,
                detail=f"{what}: a heavy job holds the warehouse; try again shortly",
            )
        yield


async def _sync_all_buyers(store) -> dict:
    """Every buyer KeyCRM has, written in portions under the heavy-job lock.

    Detached from the request that starts it: the ~460 KeyCRM pages alone take
    minutes, so no request budget could hold it. The fetch runs OUTSIDE any
    lock; each portion takes the lock, writes (and mirrors, inside
    `upsert_buyers`), and lets go. A portion that cannot get the lock in
    `SYNC_ALL_LOCK_WAIT_S` stops the run; what was written stays written — each
    portion is a complete, mirrored upsert — and a rerun writes it again.
    """
    from core import pg_buyer_sync_read
    from core.keycrm import KeyCRMClient

    # The portions below land in Postgres alone (chain 4), so the count is
    # Postgres's: DuckDB's stands still. Until step 14's S14-11 DuckDB's was
    # read when the chain's flag said duckdb.
    async def count() -> int:
        return await pg_buyer_sync_read.count_buyers()

    before = await count()
    async with KeyCRMClient() as client:
        buyers = await client.fetch_all_buyers() or []

    portion, written = store.BUYER_WRITE_PORTION, 0
    for start in range(0, len(buyers), portion):
        async with _heavy_lock(SYNC_ALL_LOCK_WAIT_S) as held:
            if not held:
                return {"status": "stopped", "reason": "the heavy-job lock stayed busy",
                        "buyers_fetched": len(buyers), "buyers_written": written}
            written += await store.upsert_buyers(buyers[start:start + portion])

    after = await count()
    return {"status": "done", "buyers_fetched": len(buyers), "buyers_written": written,
            "before_count": before, "after_count": after, "new_buyers": after - before}


# `_refuse_unreadable_buyers_chain` stood here until step 14's S14-11: a 409
# when `KS_WRITE_BUYERS` was not understood, asked by both doors into the
# write before anything started. The flag is not read any more; the buyers
# are written to Postgres whatever it says.


@router.post("/duckdb/sync-buyers")
@limiter.limit("120/minute")
async def sync_buyers(
    request: Request,
    limit: int = Query(100, ge=1, le=500, description="Maximum buyers to sync"),
):
    """Manually sync missing buyers from KeyCRM. (Admin enforced at router level.)"""
    from core.sync_service import get_sync_service

    async with _heavy_lock_or_409("Buyer sync"):
        try:
            sync_service = await get_sync_service()
            count = await sync_service.sync_missing_buyers(limit=limit)
        except read_fallback.ReadUnavailable:
            # The step recorded it; the 503 handler names the surface, as for
            # every route (DN-20c). A 500 here would name nothing.
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Buyer sync failed: {str(e)}")

    # The step never raises any more — it records a failure and returns 0 — so
    # "0 synced" and "failed" would read the same here without this. The class
    # only, as on /api/health: the text of a write error can carry a buyer's data.
    state = sync_service.buyer_sync_state
    if state.last_ok_at is None or state.last_ok_at < state.last_attempt_at:
        raise HTTPException(
            status_code=500,
            detail=f"Buyer sync failed: {state.last_error_class}; "
                   f"the watermark was not moved. See web's log.")
    return {
        "status": "success",
        "message": f"Synced {count} buyers from KeyCRM",
        "buyers_synced": count,
    }


@router.post("/duckdb/sync-all-buyers")
@limiter.limit("1/hour")
async def sync_all_buyers(request: Request, admin: dict = Depends(require_admin)):
    """Start a sync of ALL buyers from KeyCRM, detached. Requires admin.

    DO NOT RUN THIS IN PRODUCTION TO SEE WHETHER IT WORKS. It is the only path
    that fetches buyers with `include=loyalty,shipping`, so it fills city and
    region for every buyer — which moves `customer_profile.city` and the SMS
    audience's city filter. That is a decision, not a test.

    Answers at once and runs in the background: the fetch alone takes
    minutes, and a request held that long was answered 504 by the app's own
    30 s timeout while the handler kept writing behind it,
    with its result thrown away. The outcome is logged as `Full buyer sync:`.
    See `_sync_all_buyers` for what one run does.
    """
    if any(t.get_name() == "sync_all_buyers" and not t.done() for t in _BACKGROUND_TASKS):
        raise HTTPException(status_code=409, detail="A full buyer sync is already running")

    store = await get_store()

    async def run():
        try:
            result = await _sync_all_buyers(store)
            logger.info(f"Full buyer sync: {result}")
            return result
        except Exception as e:
            logger.error(f"Full buyer sync failed: {type(e).__name__}: {e}", exc_info=True)
            raise

    task = asyncio.create_task(run(), name="sync_all_buyers")
    # A strong reference, or the loop may collect the task mid-flight.
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)
    return {"status": "started", "message": "Full buyer sync started; see web's log "
            "for 'Full buyer sync:'"}


# GET /api/buyers/stats was retired by the owner's decision OD-10 (2026-09-30).
# It counted buyers in DuckDB's `orders`, its Silver and its `buyers`, and
# nothing called it. After step 13 its Silver count freezes, after a compaction
# it reads "all synced" beside a full `silver.orders`, and chain 4 freezes the
# third table too. The number that mattered, the buyers orders name and nobody
# has fetched, is what the buyer step's own selection
# (`get_missing_buyer_ids`, KS_READ_BUYER_SYNC) computes from Postgres; DuckDB's
# raw counts stay at GET /api/duckdb/stats.


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
    """Manually trigger a background job. Requires admin.

    `job_id` reaches nothing but the scheduler's own registry, which is the
    whitelist. Neither refusal below used to be reachable: `get_scheduler`
    constructs the singleton rather than answering None, and `trigger_job`
    raises on an id it does not hold instead of returning it — so an unknown
    job answered 500 and both branches read as behaviour that never ran.
    """
    from core.scheduler import get_scheduler

    scheduler = get_scheduler()
    if not scheduler.is_running:
        raise HTTPException(status_code=503, detail="Scheduler not running")

    try:
        result = await scheduler.trigger_job(job_id)
    except ValueError:
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


# ─── Managers: who counts as retail ───────────────────────────────────────────

@router.get("/managers")
@limiter.limit("60/minute")
async def list_managers(request: Request, admin: dict = Depends(require_admin)):
    """Managers with their retail classification and how much they sell.

    The classification decides `sales_type`, and every dashboard endpoint
    defaults to `retail` — so a manager left unclassified sells into
    `other` and appears on no page. This is the list you answer that from.
    """
    store = await get_store()
    managers = await store.get_all_managers()

    # Read the sales_type the warehouse actually assigned rather than deriving
    # it from is_retail here. Deriving it got the B2B manager wrong — is_retail
    # is FALSE for them, which is not the same as unclassified — and labelled
    # ₴15.5M of wholesale as `other` on the very screen meant to tell the two
    # apart. There is one CASE for this, it lives in Silver, and this reports
    # its output, from Postgres's Silver.
    rows = await store.get_manager_sales_365d()

    revenue: dict = {}
    by_type: dict = {}
    for manager_id, sales_type, amount in rows:
        revenue[manager_id] = revenue.get(manager_id, 0.0) + float(amount)
        by_type.setdefault(manager_id, {})[sales_type] = float(amount)

    for m in managers:
        types = by_type.get(m["id"], {})
        m["revenue_365d"] = float(revenue.get(m["id"], 0.0))
        # A manager maps to exactly one sales_type today; join them rather than
        # pick one if that ever stops being true.
        m["sales_type"] = (
            "+".join(sorted(types, key=lambda t: -types[t])) if types else None
        )
    managers.sort(key=lambda m: m["revenue_365d"], reverse=True)
    return {"status": "ok", "managers": managers}


@router.post("/managers/{manager_id}/retail-status")
@limiter.limit("30/minute")
async def set_manager_retail_status(
    request: Request,
    manager_id: int,
    is_retail: bool = Query(..., description="TRUE counts this manager's orders as retail"),
    effective_from: Optional[date] = Query(
        None,
        description=(
            "First order date the new classification applies to (YYYY-MM-DD). "
            "Defaults to today: past reports keep the answer they were given. "
            "Backdate only to correct a genuine misclassification."
        ),
    ),
    note: Optional[str] = Query(
        None, max_length=500, description="Why, for the audit trail"
    ),
    admin: dict = Depends(require_admin),
):
    """Classify a manager as retail or not, durably and from a date.

    `upsert_managers` seeds this from a constant for managers it has never
    seen and never touches it again, so what is set here survives the next
    sync. `sales_type` is materialised into Silver, so the warehouse is
    marked dirty: the classification only reaches the pages after a rebuild.

    **The change applies forward from `effective_from`, not to all history.**
    Until 2026-08-20 there was no choice in it: one UPDATE moved every order
    the manager had ever taken, so correcting somebody today restated last
    year's reports on the next refresh. Backdating is still available, but it
    is now something you ask for.
    """
    validate_db_int(manager_id, "manager_id", allow_none=False)
    # Chain 5 (`core/pg_managers_write.py`): a Postgres failure is a 503 rather
    # than a 500 — the decision was not stored, and nothing fell back to
    # DuckDB. Until step 14's S14-11 a `KS_WRITE_MANAGERS` nobody could read
    # was a 409 here, and a chain on DuckDB let the error through as a 500.
    from core import pg_managers_write

    store = await get_store()
    try:
        managers = {m["id"] for m in await store.get_all_managers()}
        if manager_id not in managers:
            raise HTTPException(status_code=404, detail=f"Manager {manager_id} not found")

        # set_manager_retail_status marks the warehouse dirty itself now, so every
        # caller gets the rebuild, not only this one.
        await store.set_manager_retail_status(
            manager_id, is_retail,
            effective_from=effective_from,
            set_by=admin.get("user_id"),
            note=note,
        )
    except HTTPException:
        raise
    except pg_managers_write.ManagerNotFound:
        raise HTTPException(status_code=404, detail=f"Manager {manager_id} not found")
    except pg_managers_write.BackdateBehindLatest as exc:
        # OD-C5-1 (a): DuckDB would have written two open intervals.
        raise HTTPException(status_code=409, detail=str(exc))
    except pg_managers_write.ClassificationRefused as exc:
        # A value Postgres would refuse, refused before Postgres was asked: a
        # bad request, not an outage — a 503 here would send the operator to
        # look at a Postgres that was never involved. The message names the
        # field and the kind of value, never the value.
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.error("Manager %s not classified: Postgres failed under %s: %s",
                     manager_id, pg_managers_write.WRITE_ENV, type(exc).__name__,
                     exc_info=True)
        raise HTTPException(status_code=503, detail=(
            "The classification is written to Postgres, which did not answer; "
            f"nothing was stored ({type(exc).__name__})"))
    # Chain 5's writer is the Postgres write, so nothing is copied after it.
    # Until step 14's S14-13 `replicate_managers` was called here and its
    # answer returned as `replica` — `{"skipped": "stood down: …"}` ever since
    # chain 5 owned the two tables; the copy and the key went with it.
    logger.info(
        "Manager %s retail status set to %s from %s by admin %s",
        manager_id, is_retail, effective_from or "today", admin.get("user_id"),
    )
    return {
        "status": "ok",
        "manager_id": manager_id,
        "is_retail": is_retail,
        "effective_from": str(effective_from) if effective_from else "today",
        "note": "warehouse marked dirty; sales_type updates on the next refresh",
    }


@router.post("/reconcile")
@limiter.limit("2/hour")
async def reconcile_on_demand(
    request: Request,
    days: int = Query(90, ge=1, le=400, description="Window in days"),
    background: bool = Query(True, description="Return at once and run detached"),
    admin: dict = Depends(require_admin),
):
    """Run the source reconciliation over an arbitrary window, now.

    Detached by default. Run inline and a 365-day window returns 504 after two
    minutes while the work carries on for another eight and persists correctly
    — no harm done, but it reads like a failure, and the next person to see it
    will believe the reconciliation broke.

    The scheduled job covers 90 days, so months older than that are checked by
    nobody — and the months when this job was dying on 429s were never checked
    at all. Same code, same rules on both sides, result persisted to
    `data_quality_runs` like any other run.

    Costs roughly one API call per 20 orders and runs for minutes, not seconds;
    hence the rate limit.
    """
    from core.scheduler import get_scheduler

    scheduler = get_scheduler()
    if scheduler is None:
        raise HTTPException(status_code=503, detail="Scheduler not running")

    logger.info("On-demand reconciliation over %s days by admin %s (background=%s)",
                days, admin.get("user_id"), background)

    if not background:
        return await scheduler._run_dq_reconciliation(window_days=days)

    task = asyncio.create_task(
        scheduler._run_dq_reconciliation(window_days=days),
        name=f"reconcile_{days}d",
    )
    # Hold a reference: asyncio keeps only a weak one, and a task nobody holds
    # can be collected mid-flight.
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)
    task.add_done_callback(
        lambda t: logger.error("On-demand reconciliation failed: %s", t.exception())
        if not t.cancelled() and t.exception() else None
    )
    return {
        "status": "started",
        "window_days": days,
        "note": (
            "A 365-day window takes ~10 minutes and ~1000 API calls. The result "
            "persists like any scheduled run — read it from "
            "/api/health/data-quality when it lands."
        ),
    }


# GET /api/debug/stale-returns and /api/debug/order-status/{id} were retired by
# the owner's decision OD-10 (2026-09-30). They compared DuckDB's Bronze with
# its Silver, which step 13 freezes, and nothing called them: the first checked
# four of the six return statuses, the second's KeyCRM half had raised on every
# stored order since it was written. The Postgres twin `pg_silver_row_values`
# asks the same question, on every column, of the store every tab reads — in
# `dq_integrity_check` (01, 07, 13, 19) under KS_DQ_PG_WAREHOUSE=on.


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


# ─── Reconciliation ──────────────────────────────────────────────────────────
#
# GET /api/reconciliation and POST /api/reconciliation/run were retired with the
# legacy 06:00 job by the owner's decision OD-10 (2026-09-30). The detection is
# POST /api/reconcile (dq_reconciliation over any window, all three stores);
# the repair is POST /api/duckdb/refresh-statuses. `reconciliation_log` keeps
# its history in DuckDB and in `app.reconciliation_log`.
