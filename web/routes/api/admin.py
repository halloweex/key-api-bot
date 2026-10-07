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
from ._deps import limiter, get_store

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


# ─── Postgres mirror ───────────────────────────────────────────────────────────

@router.post("/mirror/backfill/orders")
@limiter.limit("2/hour")
async def backfill_mirror_orders(
    request: Request,
    chunk_size: int = Query(2000, ge=100, le=10000),
    max_chunks: Optional[int] = Query(None, ge=1, description="Cap one invocation"),
    background: bool = Query(True, description="Run detached (recommended)"),
    admin: dict = Depends(require_admin),
):
    """Ship the orders Postgres is missing. Idempotent; safe to re-run.

    Step 05. The sync mirrors only what it writes, so Postgres starts at the
    last few minutes of activity with 46,446 orders of history behind it. This
    closes that gap, computing the difference fresh on every call rather than
    remembering a cursor — interrupt it and the next run resumes by definition.

    Foreground for a capped run you want to watch; background for the whole
    thing, which takes minutes and would otherwise sit on an HTTP request.

    409 once a write chain owns either order table (DN-22a). Answered here,
    before anything starts, because the background form would otherwise say
    "started" and leave the refusal in a log line nobody reads.

    On either copy of the latch, as the backfill itself asks: the local
    answer first, then — with the mirror on, so the backfill would go on to
    read Postgres anyway — the owner rows, after `require_revision()`. A lost
    marker leaves only the owner rows to say the tables moved, and asking
    just the local answer here answered "started" to a run the backfill then
    refused. An owner read that fails is a 503, never "started": nothing can
    be verified, and the run it would start would fail the same read in the
    background.

    And 409 with the mirror switched off, before any of that and without
    asking Postgres anything: `backfill_orders` refuses a run with
    `KS_MIRROR_LANDING` off, so the background form answered "started" to a
    run that then raised into the web log, and the foreground form a 500.
    """
    from core.pg_backfill import backfill_orders
    from core.pg_landing import (
        enabled, order_tables_stood_down, order_tables_stood_down_or_owned,
    )

    if not enabled():
        raise HTTPException(
            status_code=409, detail="KS_MIRROR_LANDING is off; nothing was started")

    moved = order_tables_stood_down()
    if not moved:
        from core.pg import get_pool, require_revision

        try:
            pool = await get_pool()
            await require_revision()
            moved = await order_tables_stood_down_or_owned(pool)
        except Exception as e:  # noqa: BLE001 — any failure is "cannot tell"
            logger.error("Mirror backfill: cannot read who owns the order "
                         "tables: %s: %s", type(e).__name__, e)
            raise HTTPException(
                status_code=503,
                detail=(
                    "Cannot tell whether a write chain owns the order tables, "
                    f"so nothing was started: {type(e).__name__}: {e}"
                ),
            ) from e
    if moved:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{', '.join(sorted(moved))} is written by a write chain, not "
                "shipped out of DuckDB; a backfill would overwrite rows only "
                "Postgres holds and archive each overwrite as an order change."
            ),
        )

    store = await get_store()

    async def run():
        try:
            from core.scheduler import get_scheduler

            return await backfill_orders(
                store, chunk_size=chunk_size, max_chunks=max_chunks,
                lock=get_scheduler()._heavy_job_lock,
            )
        except asyncio.CancelledError:
            logger.warning("Mirror backfill was cancelled")
            raise
        except Exception as e:
            logger.error(f"Mirror backfill failed: {e}", exc_info=True)
            raise

    if background:
        task = asyncio.create_task(run(), name="mirror_backfill_orders")
        # A strong reference, or the loop may collect the task mid-flight.
        _BACKGROUND_TASKS.add(task)
        task.add_done_callback(_BACKGROUND_TASKS.discard)
        task.add_done_callback(
            lambda t: logger.error(f"Mirror backfill failed: {t.exception()}")
            if not t.cancelled() and t.exception() else None
        )
        return {
            "status": "started",
            "message": "Backfill running in background",
            "note": "Progress is in the web log; re-run to see what is left.",
        }

    try:
        return {"status": "success", "stats": await run()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backfill failed: {e}")


@router.post("/mirror/backfill/expenses")
@limiter.limit("2/hour")
async def backfill_mirror_expenses(
    request: Request,
    chunk_size: int = Query(1000, ge=100, le=10000),
    admin: dict = Depends(require_admin),
):
    """Ship the order-level expenses Postgres is missing. Idempotent.

    Revision 0020. `mirror_expenses` ships what a sync fetched, so after the
    first deploy Postgres holds the last few days against DuckDB's 15,020 rows
    — and until this has run, `reconcile_expenses` reports one
    `mirror_backfill_pending` and suppresses the row-level comparison, because
    before it every expense older than the mirror looks exactly like a lost
    one.

    Foreground, unlike the orders backfill: 15,020 narrow rows is seconds
    rather than minutes, so a detached task would only make the result harder
    to read.

    409 once a write chain owns `bronze.expenses` (DN-22b), asked here before
    anything starts, as the orders route asks: the local answer first, then
    the owner rows after `require_revision()`. The backfill refuses that state
    itself, but as a raise, which this route used to turn into a 500 reading
    "Backfill failed" — about a run that must not happen, not one that broke.
    An owner read that fails is a 503: nothing can be verified.
    """
    from core.pg_expense_backfill import EXPENSES_UNIT, backfill_expenses
    from core.pg_landing import tables_stood_down, tables_stood_down_or_owned

    moved = tables_stood_down(EXPENSES_UNIT)
    if not moved:
        from core.pg import get_pool, require_revision

        try:
            pool = await get_pool()
            await require_revision()
            moved = await tables_stood_down_or_owned(pool, EXPENSES_UNIT)
        except Exception as e:  # noqa: BLE001 — any failure is "cannot tell"
            logger.error("Expense backfill: cannot read who owns "
                         "bronze.expenses: %s: %s", type(e).__name__, e)
            raise HTTPException(
                status_code=503,
                detail=(
                    "Cannot tell whether a write chain owns bronze.expenses, "
                    f"so nothing was started: {type(e).__name__}: {e}"
                ),
            ) from e
    if moved:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{', '.join(sorted(moved))} is written by a write chain, not "
                "shipped out of DuckDB; a backfill would overwrite rows only "
                "Postgres holds."
            ),
        )

    store = await get_store()
    try:
        return {"status": "success", "stats": await backfill_expenses(
            store, chunk_size=chunk_size,
        )}
    except Exception as e:
        logger.error(f"Expense backfill failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Backfill failed: {e}")


@router.post("/mirror/backfill/catalogue")
@limiter.limit("5/minute")
async def backfill_mirror_catalogue(
    request: Request,
    dry_run: bool = Query(True, description="Say what would be carried; write nothing"),
    admin: dict = Depends(require_admin),
):
    """Carry the catalogue rows only DuckDB holds into Postgres — the ones the
    daily comparison calls retired (`mirror_retired_rows`; product 1055).
    A dry run by default. Chain 6's pre-flip lever (OD-15 (a)).

    Foreground: production carries about one row. The carried rows keep a
    `mirrored_at` before the mirror's last whole-catalogue success, so they
    still read as retired, and `meta.mirror_state` is not touched
    (`core.pg_landing.carry_retired_catalogue`).

    The expenses route's order, for its reasons: 409 with the mirror off,
    before Postgres is asked anything; 409 once a write chain owns either
    table, on the local answer and then on the owner rows after
    `require_revision()`; 503 when the owner rows cannot be read.
    """
    from core.pg_landing import (
        CatalogueCarryRefused, carry_retired_catalogue, enabled,
        tables_stood_down, tables_stood_down_or_owned,
    )

    if not enabled():
        raise HTTPException(
            status_code=409, detail="KS_MIRROR_LANDING is off; nothing was carried")

    catalogue = ("bronze.products", "bronze.categories")
    moved = frozenset().union(*(tables_stood_down((t,)) for t in catalogue))
    if not moved:
        from core.pg import get_pool, require_revision

        try:
            pool = await get_pool()
            await require_revision()
            for table in catalogue:
                moved |= await tables_stood_down_or_owned(pool, (table,))
        except Exception as e:  # noqa: BLE001 — any failure is "cannot tell"
            logger.error("Catalogue carry: cannot read who owns the catalogue: "
                         "%s: %s", type(e).__name__, e)
            raise HTTPException(
                status_code=503,
                detail=(
                    "Cannot tell whether a write chain owns the catalogue, so "
                    f"nothing was carried: {type(e).__name__}: {e}"
                ),
            ) from e
    if moved:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{', '.join(sorted(moved))} is written by a write chain, not "
                "shipped by the mirror; a row only DuckDB holds is the copy-back "
                "handover's to decide now (scripts/chain_copy_back.py catalogue "
                "--handover)."
            ),
        )

    store = await get_store()
    try:
        stats = await carry_retired_catalogue(store, dry_run=dry_run)
    except CatalogueCarryRefused as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Catalogue carry failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Carry failed: {e}")
    return {"status": "dry_run" if dry_run else "success", "stats": stats}


@router.post("/mirror/backfill/buyers")
@limiter.limit("2/hour")
async def backfill_mirror_buyers(
    request: Request,
    chunk_size: int = Query(2000, ge=100, le=10000),
    admin: dict = Depends(require_admin),
):
    """Re-ship every buyer DuckDB holds, with its contacts, into Postgres.

    The lever chain 4's handover names before the flip (decision 5): the
    hourly ids-diff ships only buyers Postgres is missing, so a buyer on both
    sides that differs, or a contact only Postgres holds, stays until this
    rewrites them — `pg_buyers.reship_buyers` says what it does and costs.

    Detached, like `sync-all-buyers`: ~20 000 buyers with a DELETE of each
    one's contacts is minutes, and a request held that long is answered 504 by
    the app's own 30 s timeout while the handler keeps writing. Each portion
    takes the heavy-job lock, waited for boundedly, so it lands between two
    sync ticks; a lock that stays busy stops the run, and running it again
    finishes it. The outcome is logged as `Buyer reship:`.

    Idempotent. Refused before anything starts, the other two backfills'
    arrangement: 409 with the mirror off, 409 while a reship is running (the
    slot is claimed before the first await, so two requests at once start
    one), 409 once a write chain
    owns the buyers or their contacts — asked of the local answer and then of
    the owner rows after `require_revision()` — and 503 when the owner read
    fails, because nothing can be verified.
    """
    from core.pg_buyers import BUYER_UNIT, reship_buyers
    from core.pg_landing import enabled, tables_stood_down, tables_stood_down_or_owned

    if not enabled():
        raise HTTPException(
            status_code=409, detail="KS_MIRROR_LANDING is off; nothing was started")
    # Checked and claimed with no await between the two, so two requests in
    # the same second cannot both pass: the task only exists after the owner
    # read below, and the review showed a double click starting two reships
    # through that gap. Released when the run ends or this request refuses.
    if _RESHIP_SLOT["claimed"] or any(
            t.get_name() == "reship_buyers" and not t.done() for t in _BACKGROUND_TASKS):
        raise HTTPException(status_code=409, detail="A buyer reship is already running")
    _RESHIP_SLOT["claimed"] = True
    try:
        moved = tables_stood_down(BUYER_UNIT)
        if not moved:
            from core.pg import get_pool, require_revision

            try:
                pool = await get_pool()
                await require_revision()
                moved = await tables_stood_down_or_owned(pool, BUYER_UNIT)
            except Exception as e:  # noqa: BLE001 — any failure is "cannot tell"
                logger.error("Buyer reship: cannot read who owns the buyers: %s: %s",
                             type(e).__name__, e)
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Cannot tell whether a write chain owns the buyers, so "
                        f"nothing was started: {type(e).__name__}: {e}"
                    ),
                ) from e
        if moved:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{', '.join(sorted(moved))} is written by a write chain, not "
                    "shipped out of DuckDB; a reship would overwrite rows only "
                    "Postgres holds."
                ),
            )

        store = await get_store()

        async def run():
            try:
                result = await reship_buyers(
                    store, chunk=chunk_size,
                    portion_guard=lambda: _heavy_lock(SYNC_ALL_LOCK_WAIT_S),
                )
                logger.info(f"Buyer reship: {result}")
                return result
            except Exception as e:
                logger.error(f"Buyer reship failed: {type(e).__name__}: {e}", exc_info=True)
                raise

        task = asyncio.create_task(run(), name="reship_buyers")
        # A strong reference, or the loop may collect the task mid-flight.
        _BACKGROUND_TASKS.add(task)
        task.add_done_callback(_BACKGROUND_TASKS.discard)
        task.add_done_callback(_release_reship_slot)
    except BaseException:
        _RESHIP_SLOT["claimed"] = False
        raise
    return {"status": "started", "message": "Buyer reship started; see web's log "
            "for 'Buyer reship:'"}


# Whether a request has claimed a reship that has not finished. A dict, so the
# route and the task's callback share it without a `global`.
_RESHIP_SLOT = {"claimed": False}


def _release_reship_slot(_task) -> None:
    _RESHIP_SLOT["claimed"] = False


# ─── Warehouse ─────────────────────────────────────────────────────────────────

@router.get("/warehouse/status")
@limiter.limit("60/minute")
async def get_warehouse_status(request: Request):
    """Get warehouse layer (Silver/Gold) status and last refresh info."""
    store = await get_store()
    from core import pg_derivation, warehouse_cutover

    if warehouse_cutover.writes_postgres():
        # Postgres alone derives (KS_WRITE_WAREHOUSE=postgres, DN-29): the
        # Postgres block leads, and DuckDB's last refresh — as old as the
        # switch — is nested and named for what it is, never the top-level
        # answer an operator reads as the warehouse's state.
        status = {"writer": warehouse_cutover.POSTGRES,
                  "postgres": await _pg_derivation_status(),
                  "duckdb_frozen": await store.get_warehouse_status()}
    else:
        status = await store.get_warehouse_status()
        # Under KS_PG_DERIVE=own Postgres derives on its own signal, and a
        # status page that showed only DuckDB's refreshes would describe the
        # engine the dashboard is leaving.
        if pg_derivation.owns():
            status = {**status, "postgres": await _pg_derivation_status()}
    # Step 13's readiness (DN-28, DN-29): KS_WRITE_WAREHOUSE as this process
    # read it, what settling the writer found, and every precondition of the
    # switch still unmet, each by name. Published whatever the mode — the list
    # is what a person reads before the flip, and after it what a restart
    # would need to stay switched.
    try:
        cutover = await warehouse_cutover.readiness()
    except Exception as e:  # noqa: BLE001 — a status page reports, it does not fail
        # The class alone, as everywhere readiness reports: the text is a
        # driver's and names the database user, host and port. Whole in the log.
        logger.error("cutover readiness raised: %s: %s", type(e).__name__, e)
        cutover = {**warehouse_cutover.status(),
                   "readiness_error": type(e).__name__}
    return {**status, "cutover": cutover}


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
        return {"mode": pg_derivation.mode(), "error": f"{type(e).__name__}: {e}"}
    return {
        "mode": pg_derivation.mode(),
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
    """Run the Postgres derivation immediately, past its floor. Under
    KS_PG_DERIVE=own only; takes the heavy lock itself, so call it after
    releasing yours."""
    from core.scheduler import get_scheduler

    return await get_scheduler()._run_pg_derivation(trigger=trigger, force=True)


@router.post("/warehouse/refresh")
@limiter.limit("5/minute")
async def refresh_warehouse(
    request: Request,
    admin: dict = Depends(require_admin),
):
    """Manually trigger warehouse layer refresh (Silver -> Gold). Requires admin."""
    from core import warehouse_cutover
    from core.scheduler import get_scheduler

    if warehouse_cutover.duckdb_derives():
        store = await get_store()
        # Under the heavy-job lock, like the two-minute job that runs the same
        # code. Without it a manual refresh interleaved with the scheduled one:
        # a validation reading Bronze/Silver across the other's commits reported
        # a correct rebuild as failed, and the two raced each other's fired and
        # resolved notices for the same alert group.
        async with get_scheduler()._heavy_job_lock:
            result = await store.refresh_warehouse_layers(trigger="manual")
    else:
        # KS_WRITE_WAREHOUSE=postgres (DN-29): DuckDB no longer derives, and
        # the lever is the Postgres derivation below — own is a precondition
        # of the switch, so it always runs here.
        result = {"status": "skipped", "trigger": "manual",
                  "reason": "KS_WRITE_WAREHOUSE=postgres: DuckDB does not derive"}
    # The lever every Silver/Gold alert names. Under KS_PG_DERIVE=own it
    # rebuilds the Postgres layers too — otherwise it would repair the engine
    # the dashboard no longer reads and leave the one it does as it was.
    from core import pg_derivation

    if pg_derivation.owns():
        result = {**result, "postgres": await _derive_postgres_now("manual")}
    return result


@router.post("/warehouse/rebuild-silver")
@limiter.limit("1/minute")
async def rebuild_silver_from_scratch(
    request: Request,
    admin: dict = Depends(require_admin),
):
    """
    DROP + CREATE + INSERT silver_orders from bronze. Bypasses MVCC —
    use when silver has corrupted rows blocking DELETE+INSERT rebuild.
    """
    from core import warehouse_cutover
    from core.duckdb_store import (
        SILVER_ORDERS_DDL, silver_select_sql, silver_pass2_sql,
    )

    if not warehouse_cutover.duckdb_derives():
        # KS_WRITE_WAREHOUSE=postgres (DN-29): DuckDB's Silver is frozen on
        # purpose, and rebuilding it would hand no tick anything to validate —
        # the refresh job is not registered. The Postgres half below still runs.
        result = {"status": "skipped",
                  "reason": "KS_WRITE_WAREHOUSE=postgres: DuckDB does not derive"}
    else:
        store = await get_store()

        async with store.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS silver_orders")
            conn.execute(SILVER_ORDERS_DDL)
            conn.execute(f"INSERT INTO silver_orders SELECT {silver_select_sql()} FROM orders o")
            conn.execute(silver_pass2_sql())
            count = conn.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0]
            max_date = conn.execute("SELECT MAX(order_date) FROM silver_orders").fetchone()[0]

        logger.info(f"Rebuilt silver_orders from scratch: {count} rows, max_date={max_date}")
        # DROP/CREATE/INSERT are autocommit statements on purpose (this endpoint
        # exists to bypass the transactional path when Silver is already
        # misbehaving), so an INSERT that dies leaves Silver empty. Marking dirty
        # hands the result to the next refresh tick, which validates it and, if it
        # is half-done, rebuilds it.
        await store.mark_warehouse_dirty(None)
        result = {"status": "ok", "silver_rows": count, "max_order_date": str(max_date)}
    from core import pg_derivation

    if pg_derivation.owns():
        result["postgres"] = await _derive_postgres_now("manual")
    return result


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
    from core import pg_buyer_sync_read, pg_buyers_write
    from core.keycrm import KeyCRMClient

    # Under chain 4 the portions below land in Postgres alone, so DuckDB's
    # count stands still and "new buyers" would always read zero. Read once:
    # the first portion may latch the chain, which only confirms "postgres".
    in_postgres = pg_buyers_write.mode() == "postgres"

    async def count() -> int:
        if in_postgres:
            return await pg_buyer_sync_read.count_buyers()
        async with store.connection() as conn:
            return conn.execute("SELECT COUNT(*) FROM buyers").fetchone()[0]

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


def _refuse_unreadable_buyers_chain() -> None:
    """409 when `KS_WRITE_BUYERS` is not understood and no latch overrides it:
    the buyers have nowhere known to be written, and the step would fetch from
    KeyCRM first. Asked before anything starts, by both doors into the write."""
    from core import pg_buyers_write

    if pg_buyers_write.mode() is None:
        raise HTTPException(
            status_code=409,
            detail=f"{pg_buyers_write.WRITE_ENV} is not understood, so the buyers "
                   "have nowhere known to be written; correct it, or remove it for "
                   "DuckDB, and retry")


@router.post("/duckdb/sync-buyers")
@limiter.limit("120/minute")
async def sync_buyers(
    request: Request,
    limit: int = Query(100, ge=1, le=500, description="Maximum buyers to sync"),
):
    """Manually sync missing buyers from KeyCRM. (Admin enforced at router level.)"""
    from core.sync_service import get_sync_service

    _refuse_unreadable_buyers_chain()
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

    Answers at once and runs in the background, like /mirror/backfill/orders:
    the fetch alone takes minutes, and a request held that long was answered
    504 by the app's own 30 s timeout while the handler kept writing behind it,
    with its result thrown away. The outcome is logged as `Full buyer sync:`.
    See `_sync_all_buyers` for what one run does.
    """
    if any(t.get_name() == "sync_all_buyers" and not t.done() for t in _BACKGROUND_TASKS):
        raise HTTPException(status_code=409, detail="A full buyer sync is already running")
    # Before "started", not in the log afterwards: every portion would raise.
    _refuse_unreadable_buyers_chain()

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
    # its output — from whichever Silver `KS_READ_SILVER` names.
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
    store = await get_store()
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
    # Step 05. This endpoint is the whole reason the classification cannot be
    # re-derived on the other side, so Postgres is updated here rather than
    # waiting for the daily manager sync — otherwise Silver computed in
    # Postgres would carry yesterday's answer for up to a day.
    from core.pg_replication import replicate_managers

    replica = await replicate_managers(store)
    if replica and replica.get("error"):
        # The classification is stored and the warehouse marked dirty; only
        # the Postgres copy is behind, until the next manager sync. Said in
        # the response rather than discovered in the 09:00 digest. A copy
        # that was skipped — the mirror off, or a write chain owning the
        # tables (DN-22b) — did not fail, and its reason is in `replica`.
        logger.warning("Manager %s classified, but the Postgres replica failed: %s",
                       manager_id, replica.get("error"))

    logger.info(
        "Manager %s retail status set to %s from %s by admin %s",
        manager_id, is_retail, effective_from or "today", admin.get("user_id"),
    )
    return {
        "replica": replica,
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
