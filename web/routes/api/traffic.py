"""Traffic analytics, trend, transactions, refresh endpoints."""
import asyncio
import logging
from datetime import datetime as _dt, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional

from core.pg_utm_parse import reparse_router
from web.services import dashboard_service
from web.routes.auth import require_admin
from ._deps import (
    limiter, get_store,
    validate_period, validate_source_id, validate_sales_type,
    ValidationError,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/traffic/analytics")
@limiter.limit("30/minute")
async def get_traffic_analytics(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get traffic analytics with platform and traffic type breakdown."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_traffic_analytics(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type, source_id=source_id,
    )


@router.get("/traffic/trend")
@limiter.limit("30/minute")
async def get_traffic_trend(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get daily traffic trend with paid/organic breakdown."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    result = await store.get_traffic_trend(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type, source_id=source_id,
    )
    return {"trend": result}


@router.get("/traffic/transactions")
@limiter.limit("30/minute")
async def get_traffic_transactions(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    traffic_type: Optional[str] = Query(None),
    platform: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Get individual orders with traffic attribution details."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if traffic_type and traffic_type not in (
        "paid_confirmed", "paid_likely", "manager", "organic", "pixel_only", "unknown",
    ):
        raise HTTPException(status_code=400, detail=f"Invalid traffic_type: {traffic_type}")

    valid_platforms = ("facebook", "instagram", "google", "tiktok", "email", "telegram",
                       "ai", "manager", "other", "unattributed")
    if platform and platform not in valid_platforms:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {platform}")

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_traffic_transactions(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type,
        source_id=source_id, traffic_type=traffic_type, platform=platform,
        limit=limit, offset=offset,
    )


@router.get("/traffic/utm-campaigns")
@limiter.limit("30/minute")
async def get_traffic_utm_campaigns(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    traffic_type: Optional[str] = Query(None),
    platform: Optional[str] = Query(None),
    sort_by: str = Query("revenue"),
    sort_dir: str = Query("desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Get orders and revenue aggregated per UTM campaign."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if traffic_type and traffic_type not in (
        "paid_confirmed", "paid_likely", "manager", "organic", "pixel_only", "unknown",
    ):
        raise HTTPException(status_code=400, detail=f"Invalid traffic_type: {traffic_type}")

    valid_platforms = ("facebook", "instagram", "google", "tiktok", "email", "telegram",
                       "ai", "manager", "other", "unattributed")
    if platform and platform not in valid_platforms:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {platform}")

    if sort_by not in ("campaign", "utm_source", "platform", "traffic_type", "orders", "revenue"):
        raise HTTPException(status_code=400, detail=f"Invalid sort_by: {sort_by}")
    if sort_dir not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail=f"Invalid sort_dir: {sort_dir}")

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_traffic_utm_campaigns(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type,
        source_id=source_id, traffic_type=traffic_type, platform=platform,
        sort_by=sort_by, sort_dir=sort_dir, limit=limit, offset=offset,
    )


@router.get("/traffic/roas")
@limiter.limit("30/minute")
async def get_traffic_roas(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get blended and per-platform ROAS with bonus tier."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_traffic_roas(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type,
    )


@router.post("/traffic/refresh")
@limiter.limit("5/minute")
async def refresh_traffic_data(
    request: Request,
    user: dict = Depends(require_admin),
):
    """Force refresh UTM and traffic layers (admin only)."""
    store = await get_store()

    try:
        utm_count = len(await store.refresh_utm_silver_layer())
        # And on to Postgres, which is what the tab reads. These endpoints do
        # not mark the warehouse dirty, so without this the reclassification
        # sits in DuckDB until the next dirty tick while the page keeps
        # rendering the previous one. The router ships DuckDB's parse, or
        # under KS_UTM_PARSE=postgres parses in Postgres what is new; never
        # raises — see `core/pg_utm_parse.py`.
        await reparse_router(store)

        return {
            "success": True,
            "utm_orders_parsed": utm_count,
        }
    except Exception as e:
        logger.error(f"Traffic refresh failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")


@router.post("/traffic/reclassify")
@limiter.limit("2/minute")
async def reclassify_traffic(
    request: Request,
    user: dict = Depends(require_admin),
):
    """Re-parse ALL UTM data with current classification rules (admin only).

    Use after classification rule changes. Deletes silver_order_utm
    and re-parses everything from scratch.

    **A 504 in the browser is expected and does not mean it failed.** The work
    is synchronous and longer than any proxy will wait: 2026-09-09 it took
    **281 s** for 32,966 orders, while nginx gave up well before that and
    served the "warming up" page. The application still finished and returned
    200. Read the outcome in the log, not in the response:

        docker compose logs web | grep -E "Parsed UTM data for|reclassify"

    A successful run prints `Parsed UTM data for N orders (M batches)`, then
    `Request completed ... 200`. It printed a `Refreshed gold_daily_traffic`
    line between the two until that layer was retired.

    Left synchronous on purpose. `backfill-utm` next door is a background task
    with a status endpoint, and copying that shape here would buy a subsystem
    for an operation run **twice in seven months** — the classification rules
    changed on 2026-02-17 and 2026-09-08, and nothing else calls this. A
    docstring costs less than a task registry, and the log already holds the
    answer.

    A client that hangs up does not stop the work, and the tab does not see
    the middle of it. DuckDB *is* inconsistent for those minutes — the DELETE
    commits on its own connection and the re-parse writes in batches of a
    thousand on theirs — but the page reads Postgres, and Postgres keeps the
    previous complete copy until `reparse_router` replaces the whole table at
    the end: shipped from DuckDB by `ship_after_reparse`, or under
    KS_UTM_PARSE=postgres re-parsed by `parse_full` in one transaction. So the
    window is invisible from the screen rather than merely short.
    """
    store = await get_store()

    try:
        async with store.connection() as conn:
            conn.execute("DELETE FROM silver_order_utm")

        utm_count = len(await store.refresh_utm_silver_layer())
        # And on to Postgres, which is what the tab reads. These endpoints do
        # not mark the warehouse dirty, so without this the reclassification
        # sits in DuckDB until the next dirty tick while the page keeps
        # rendering the previous one. `full`: everything was re-parsed, so
        # under KS_UTM_PARSE=postgres the table is replaced whole — the only
        # way a rule change reaches orders whose `updated_at` has not moved.
        # Never raises — see `core/pg_utm_parse.py`.
        await reparse_router(store, full=True)

        return {
            "success": True,
            "utm_records": utm_count,
        }
    except Exception as e:
        logger.error(f"Traffic reclassify failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Reclassify failed: {e}")


_backfill_status: dict = {"running": False, "result": None}


_BACKGROUND_TASKS: set = set()


async def _run_backfill(days: int):
    """Background task: backfill manager_comment from KeyCRM API.

    The `running` flag is cleared on every way out. It used to be set before
    the two awaits that open the store and the client, and only the body's
    own handlers reset it — a failure in either, or a cancellation at a
    deploy, left the endpoint answering "already running" until a restart.
    """
    try:
        await _run_backfill_inner(days)
    finally:
        _backfill_status["running"] = False


async def _run_backfill_inner(days: int):
    from core.keycrm import get_async_client

    store = await get_store()
    client = await get_async_client()
    tz = ZoneInfo("Europe/Kyiv")

    try:
        async with store.connection() as conn:
            null_count = conn.execute(
                "SELECT COUNT(*) FROM orders WHERE manager_comment IS NULL"
            ).fetchone()[0]
            total = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]

        if null_count == 0:
            _backfill_status.update(running=False, result={
                "status": "skip", "message": "All orders already have manager_comment",
            })
            return

        logger.info(f"UTM backfill: {null_count}/{total} orders missing manager_comment")

        final_end = _dt.now(tz) + timedelta(days=1)
        chunk_days = 30  # Smaller chunks to stay well within pagination limit
        current_start = _dt.now(tz) - timedelta(days=days)
        api_fetched_total = 0
        db_updated_total = 0
        pg_shipped_total = 0
        # Orders this run changed in DuckDB and could not ship. Reported
        # rather than retried: a re-run cannot find them again, so the list
        # itself is what an operator needs.
        pg_failed_ids: list[int] = []
        chunks_processed = 0

        while current_start < final_end:
            current_end = min(current_start + timedelta(days=chunk_days), final_end)
            start_str = current_start.strftime('%Y-%m-%d')
            end_str = current_end.strftime('%Y-%m-%d')
            chunks_processed += 1

            orders_by_id = {}
            try:
                params = {
                    "filter[created_between]": f"{start_str}, {end_str}",
                    "limit": 50,
                }
                async for batch in client.paginate("order", params=params, page_size=50):
                    for order in batch:
                        mc = order.get("manager_comment")
                        if mc:
                            orders_by_id[order["id"]] = mc
            except Exception as e:
                logger.warning(f"UTM backfill chunk {start_str}-{end_str} failed: {e}")
                current_start = current_end
                continue

            api_fetched_total += len(orders_by_id)
            chunk_updated = 0
            chunk_shipped = 0

            if orders_by_id:
                # Both halves under the scheduler's heavy-job lock, because
                # they are one write: the UPDATE moves DuckDB and the ship
                # carries the same rows to `bronze.orders`. That is not what
                # `/traffic` reads — the tab reads `silver.orders LEFT JOIN
                # silver.order_utm` and the parsed rows reach Postgres
                # through `reparse_router` below — it is what the daily
                # `mirror_landing` fingerprint compares (`manager_comment` is
                # one of its columns) and what step 9's Postgres UTM parser
                # reads under KS_UTM_PARSE=postgres. The three scheduled syncs hold this lock across
                # their own DuckDB write *and* their mirror call, so a chunk
                # taken under it cannot straddle one — and without it a sync
                # writing between our UPDATE and our read would leave Postgres
                # holding whichever copy committed last.
                #
                # The KeyCRM pagination above stays outside it: the lock is
                # for the two stores, and holding it across a network fetch
                # would stall the two-minute warehouse tick for the length of
                # a page.
                from core.pg_backfill import ship_orders_by_id
                from core.pg_order_versions import BACKFILL
                from core.scheduler import get_scheduler

                async with get_scheduler()._heavy_job_lock:
                    async with store.connection() as conn:
                        # The ids this chunk will actually change, read before
                        # the UPDATE rather than counted after it. The old
                        # before/after count over the whole table was both
                        # racy with the sync and unable to say *which* orders
                        # moved, and the ship needs exactly that list —
                        # `COALESCE(EXCLUDED, stored)` means an order whose
                        # comment did not change has nothing to carry.
                        changed_ids = [
                            int(r[0]) for r in conn.execute(
                                "SELECT id FROM orders WHERE id IN "
                                f"({', '.join('?' for _ in orders_by_id)}) "
                                "AND manager_comment IS NULL",
                                list(orders_by_id),
                            ).fetchall()
                        ]
                        for order_id in changed_ids:
                            conn.execute(
                                "UPDATE orders SET manager_comment = ? "
                                "WHERE id = ? AND manager_comment IS NULL",
                                [orders_by_id[order_id], order_id],
                            )
                        conn.execute("CHECKPOINT")
                    chunk_updated = len(changed_ids)
                    db_updated_total += chunk_updated
                    # Already holding the lock, so the helper is told not to
                    # take it: `asyncio.Lock` is not reentrant.
                    #
                    # Caught per chunk rather than allowed to end the run.
                    # `ship_orders_by_id` raises — `backfill_orders`' contract
                    # — but this caller has a second job the helper knows
                    # nothing about: the DuckDB re-parse below, which was
                    # Postgres-independent before DN-17 and must stay so. An
                    # unreachable Postgres (or `web` running ahead of
                    # `migrate`, where `require_revision()` raises) would
                    # otherwise abort the run with the comments already
                    # restored in DuckDB — and since the SELECT above only
                    # offers rows whose comment is still NULL, a re-run never
                    # re-offers them. The ids are kept so an operator has the
                    # list the daily reconciliation will otherwise hand them
                    # one bucket at a time.
                    try:
                        shipped = await ship_orders_by_id(
                            store, changed_ids, version_kind=BACKFILL,
                        )
                        chunk_shipped = shipped["orders_shipped"]
                        pg_shipped_total += chunk_shipped
                    except Exception as ship_error:
                        pg_failed_ids.extend(changed_ids)
                        logger.error(
                            "UTM backfill: shipping %d order(s) to Postgres "
                            "failed, carrying on with the DuckDB re-parse: %s",
                            len(changed_ids), ship_error, exc_info=True,
                        )

            logger.info(
                f"UTM backfill chunk {chunks_processed} ({start_str} to {end_str}): "
                f"api={len(orders_by_id)}, db_changed={chunk_updated}, "
                f"pg_shipped={chunk_shipped}"
            )

            _backfill_status["result"] = {
                "status": "in_progress",
                "chunks_processed": chunks_processed,
                "api_fetched": api_fetched_total,
                "db_updated": db_updated_total,
                "pg_shipped": pg_shipped_total,
                "pg_failed_ids": list(pg_failed_ids),
            }
            current_start = current_end
            # Pause between chunks to avoid tripping the circuit breaker
            await asyncio.sleep(3)

        logger.info(
            f"UTM backfill: api_fetched={api_fetched_total}, "
            f"db_updated={db_updated_total}, pg_shipped={pg_shipped_total} "
            f"across {chunks_processed} chunks"
        )
        if pg_failed_ids:
            logger.error(
                "UTM backfill: %d order(s) were restored in DuckDB and never "
                "reached Postgres. Re-ship them by id; a re-run of this "
                "endpoint will not, because their comment is no longer NULL. "
                "Ids: %s", len(pg_failed_ids), pg_failed_ids,
            )

        # Force final checkpoint before UTM refresh
        async with store.connection() as conn:
            conn.execute("CHECKPOINT")

        # Verify the update worked
        async with store.connection() as conn:
            remaining_null = conn.execute(
                "SELECT COUNT(*) FROM orders WHERE manager_comment IS NULL"
            ).fetchone()[0]
        logger.info(f"UTM backfill: {remaining_null} orders still have NULL mc (was {null_count})")

        utm_count = len(await store.refresh_utm_silver_layer())
        # And on to Postgres, which is what the tab reads. These endpoints do
        # not mark the warehouse dirty, so without this the reclassification
        # sits in DuckDB until the next dirty tick while the page keeps
        # rendering the previous one. Not `full`: this run only adds comments,
        # and under KS_UTM_PARSE=postgres the incremental parse reads them
        # out of `bronze.orders`, where `ship_orders_by_id` put them above.
        # Never raises — see `core/pg_utm_parse.py`.
        await reparse_router(store)

        logger.info(f"UTM backfill complete: {utm_count} UTM records")

        _backfill_status.update(running=False, result={
            # `partial` rather than `success` when anything failed to ship:
            # DuckDB and Postgres disagree about those orders until somebody
            # re-ships them, and a green status is how that gets forgotten.
            "status": "partial" if pg_failed_ids else "success",
            "orders_missing_before": null_count,
            "orders_remaining_null": remaining_null,
            "api_fetched": api_fetched_total,
            "db_updated": db_updated_total,
            "pg_shipped": pg_shipped_total,
            "pg_failed_ids": pg_failed_ids,
            "chunks_processed": chunks_processed,
            "utm_records_parsed": utm_count,
        })
    except Exception as e:
        logger.error(f"UTM backfill failed: {e}", exc_info=True)
        _backfill_status.update(running=False, result={
            "status": "error", "error": "UTM backfill failed — see server logs",
        })


@router.post("/traffic/backfill-utm")
@limiter.limit("1/minute")
async def backfill_utm_data(
    request: Request,
    days: int = Query(730, ge=30, le=1000),
    user: dict = Depends(require_admin),
):
    """Start UTM backfill as background task (admin only). Check status via GET.

    Admin for the same reason as its two neighbours above, and one more: the
    job walks up to `days` of KeyCRM through the process-wide circuit breaker
    and takes the single DuckDB write lock once per chunk to UPDATE and
    CHECKPOINT. Nothing stops it once started.
    """
    if _backfill_status["running"]:
        return {"status": "already_running", "progress": _backfill_status["result"]}

    _backfill_status.update(running=True, result={"status": "started"})
    task = asyncio.create_task(_run_backfill(days))
    # A strong reference: the loop holds tasks weakly, and a collected task
    # is a backfill that stops without a trace.
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)
    return {"status": "started", "message": "Backfill started in background. GET /traffic/backfill-utm/status to check."}


@router.get("/traffic/backfill-utm/status")
@limiter.limit("30/minute")
async def backfill_utm_status(
    request: Request,
    user: dict = Depends(require_admin),
):
    """Check status of UTM backfill background task (admin only).

    The read half of an admin-only operation, with no frontend caller: it
    reports how far the job has got and how many orders are still missing
    attribution.
    """
    return {
        "running": _backfill_status["running"],
        "result": _backfill_status["result"],
    }
