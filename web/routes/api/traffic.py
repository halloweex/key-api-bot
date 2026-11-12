"""Traffic analytics, trend, transactions, refresh endpoints."""
import asyncio
import logging
from datetime import datetime as _dt, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional

from core import ga4
from core.pg_utm_parse import reparse_router
from web.services import dashboard_service, ga4_funnel_service
from web.routes.auth import require_admin
from ._deps import (
    limiter, get_store,
    validate_period, validate_source_id, validate_sales_type, validate_db_int,
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
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_traffic_analytics(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type, source_id=source_id,
    )


@router.get("/traffic/organic")
@limiter.limit("30/minute")
async def get_traffic_organic(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Organic traffic by channel, against the period just before it."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_traffic_organic(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type, source_id=source_id,
    )


@router.get("/traffic/pixel-only")
@limiter.limit("30/minute")
async def get_traffic_pixel_only(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Pixel-only orders by buyer: new or returning, and their first channel."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_traffic_pixel_only(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type, source_id=source_id,
    )


@router.get("/traffic/funnel")
@limiter.limit("30/minute")
async def get_traffic_funnel(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    country: str = Query(ga4_funnel_service.DEFAULT_SCOPE),
):
    """The site's purchase funnel from GA4: the period, the one before, a year ago.

    GA4 sees the website only, so the tab's source and sales-type filters do
    not apply. ``country`` is whose visits count: ``ua`` (Ukraine and the
    visitors GA4 could not place, the default) or ``all``. See
    ``web/services/ga4_funnel_service.py``.
    """
    try:
        validate_period(period)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if country not in ga4_funnel_service.SCOPES:
        raise HTTPException(status_code=400, detail="country must be 'ua' or 'all'")

    if not ga4.configured():
        raise HTTPException(status_code=503, detail="GA4 is not configured")
    try:
        return await ga4_funnel_service.get_funnel(
            _dt.strptime(start, "%Y-%m-%d").date(), _dt.strptime(end, "%Y-%m-%d").date(),
            scope=country,
        )
    except ga4.GA4Unavailable as e:
        logger.warning("GA4 funnel unavailable: %s", e)
        raise HTTPException(status_code=503, detail="GA4 is unavailable")


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
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

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
        validate_db_int(offset, "offset", bits=64)
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

    try:
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
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
        validate_db_int(offset, "offset", bits=64)
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

    try:
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
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
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

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
    """Parse the UTM verdicts that are new or changed, now (admin only).

    Postgres alone parses (`parse_incremental_locked`, through the router).
    Until step 14's S14-15 this ran DuckDB's parse first while DuckDB
    derived; since step 13 it parsed in Postgres alone, as now."""
    parsed = await _parse_in_postgres(full=False, door="Refresh")
    return {"success": True, "utm_orders_parsed": int(parsed.get("parsed") or 0),
            "engine": "postgres"}


@router.post("/traffic/reclassify")
@limiter.limit("2/minute")
async def reclassify_traffic(
    request: Request,
    user: dict = Depends(require_admin),
):
    """Re-parse ALL UTM data with current classification rules (admin only).

    Use after classification rule changes. Runs `pg_utm_parse.parse_full`
    through `reparse_router`: every commented order in `bronze.orders` is
    re-parsed, and `silver.order_utm` is replaced whole — a `DELETE` of every
    row and the re-parsed verdicts inserted, in one transaction. A result that
    would shrink the table under `FULL_PARSE_FLOOR_PCT` of its rows is refused
    (409) and the table keeps what it holds.

    **A 504 in the browser does not mean it failed.** The work is synchronous,
    and a proxy that gives up first serves its own error page while the
    application finishes and returns 200. Read the outcome in the log, not in
    the response:

        docker compose logs web | grep -E "Order UTM parsed in Postgres|reclassify"

    A successful run prints `Order UTM parsed in Postgres (full): …`, then
    `Request completed ... 200`. (DuckDB's parse printed `Parsed UTM data for
    N orders (M batches)` until step 14's S14-15 deleted it.)

    Left synchronous on purpose. `backfill-utm` next door is a background task
    with a status endpoint, and copying that shape here would buy a subsystem
    for an operation run **twice in seven months** — the classification rules
    changed on 2026-02-17 and 2026-09-08, and nothing else calls this. A
    docstring costs less than a task registry, and the log already holds the
    answer.

    A client that hangs up does not stop the work, and the tab does not see
    the middle of it: `parse_full` replaces the whole table in one
    transaction, so a reader sees the old verdicts or the new ones.

    Postgres alone re-parses. Until step 14's S14-15 the door noted the
    re-parse for a way back from step 13 first — in DuckDB's recorded writer,
    or under KS_DUCKDB=off in `data/warehouse-utm-reparsed-at` — so that an
    image from before step 14, taking the way back, would re-parse DuckDB's
    verdicts too. Step 13's switch is gone, and the note with it; the marker
    already on disk is left as it is.
    """
    parsed = await _parse_in_postgres(full=True, door="Reclassify")
    return {"success": True, "utm_records": int(parsed.get("rows") or 0),
            "engine": "postgres"}


async def _parse_in_postgres(*, full: bool, door: str) -> dict:
    """A door's re-parse, in Postgres. The router's answer is the door's
    answer — an error is a 500 and a refused full parse a 409. `full` is the
    reclassify's."""
    parsed = await reparse_router(full=full)
    if "error" in parsed or "skipped" in parsed:
        raise HTTPException(
            status_code=500,
            detail=f"{door} failed in Postgres: "
                   f"{parsed.get('error') or parsed.get('skipped')}")
    if "refused" in parsed:
        raise HTTPException(status_code=409,
                            detail=f"{door} refused in Postgres: {parsed['refused']}")
    return parsed


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
    """The backfill's body: chain 3's (`_run_backfill_postgres`).

    The orders are written to Postgres alone, and a comment restored in
    DuckDB would be one nobody reads. Until step 14's S14-09 this asked
    `duckdb_store._orders_in_postgres()` first — a constant since S14-00 —
    and kept a DuckDB walk (an UPDATE of `orders`, a CHECKPOINT and a ship
    of the changed rows) for an answer it no longer gave; S14-09 deleted it.
    """
    return await _run_backfill_postgres(days)


async def _run_backfill_postgres(days: int):
    """`_run_backfill_inner` under chain 3 (`core/pg_orders_write.py`).

    The same walk over KeyCRM, and the same answer shape, with one store:
    each chunk's comments go to `pg_orders_write.restore_manager_comments`,
    which fills in only the orders whose comment Postgres holds as NULL and
    archives each as `kind='backfill'` (OD-20 (b)) in the row's own
    transaction. Nothing is written to DuckDB, whose orders the chain froze.
    The write holds the scheduler's heavy-job lock, as the DuckDB path did
    until step 14's S14-09; the KeyCRM pagination stays outside it. Under the chain step 13 is in
    force, so Postgres alone parses what landed (`reparse_router`).
    """
    from core import pg_orders_read, pg_orders_write
    from core.keycrm import get_async_client
    from core.scheduler import get_scheduler

    client = await get_async_client()
    tz = ZoneInfo("Europe/Kyiv")
    try:
        null_count = await pg_orders_read.null_comment_count()
        if null_count == 0:
            _backfill_status.update(running=False, result={
                "status": "skip", "message": "All orders already have manager_comment",
            })
            return
        final_end = _dt.now(tz) + timedelta(days=1)
        current_start = _dt.now(tz) - timedelta(days=days)
        api_fetched_total = restored_total = chunks_processed = 0
        failed_chunks: list[str] = []
        while current_start < final_end:
            current_end = min(current_start + timedelta(days=30), final_end)
            start_str, end_str = current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')
            chunks_processed += 1
            comments: dict = {}
            try:
                params = {"filter[created_between]": f"{start_str}, {end_str}", "limit": 50}
                async for batch in client.paginate("order", params=params, page_size=50):
                    for order in batch:
                        if order.get("manager_comment"):
                            comments[order["id"]] = order["manager_comment"]
            except Exception as e:
                logger.warning(f"UTM backfill chunk {start_str}-{end_str} failed: {e}")
                current_start = current_end
                continue
            api_fetched_total += len(comments)
            if comments:
                try:
                    async with get_scheduler()._heavy_job_lock:
                        restored = await pg_orders_write.restore_manager_comments(comments)
                    restored_total += len(restored)
                except Exception as write_error:
                    # The chain's writer raises (OD-13 (a)). One chunk's
                    # failure is reported and the walk goes on: a re-run
                    # offers the same comments again, since Postgres still
                    # holds them as NULL.
                    failed_chunks.append(f"{start_str}..{end_str}")
                    logger.error("UTM backfill (chain 3): chunk %s..%s not "
                                 "written: %s", start_str, end_str,
                                 type(write_error).__name__, exc_info=True)
            _backfill_status["result"] = {
                "status": "in_progress", "chunks_processed": chunks_processed,
                "api_fetched": api_fetched_total, "pg_restored": restored_total,
            }
            current_start = current_end
            await asyncio.sleep(3)

        remaining_null = await pg_orders_read.null_comment_count()
        parsed = await reparse_router()
        pg_parse_error = parsed.get("error") or parsed.get("skipped")
        _backfill_status.update(running=False, result={
            "status": "partial" if failed_chunks or pg_parse_error else "success",
            **({"pg_parse_error": pg_parse_error} if pg_parse_error else {}),
            "orders_missing_before": null_count,
            "orders_remaining_null": remaining_null,
            "api_fetched": api_fetched_total,
            "pg_restored": restored_total,
            "failed_chunks": failed_chunks,
            "chunks_processed": chunks_processed,
            "utm_records_parsed": int(parsed.get("parsed") or 0),
        })
    except Exception as e:
        logger.error(f"UTM backfill (chain 3) failed: {e}", exc_info=True)
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
    and takes the scheduler's heavy-job lock once per chunk to write the
    comments to Postgres. Nothing stops it once started.
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
