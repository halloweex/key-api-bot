"""
FastAPI web application for KeyCRM Dashboard.
"""
import asyncio
import os
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import ORJSONResponse, JSONResponse, FileResponse
from starlette.middleware.gzip import GZipMiddleware

from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from web.config import STATIC_DIR, STATIC_V2_DIR, VERSION
from web.ratelimit import limiter
from web.routes import api, pages, auth, chat, websocket
from web.routes.auth import api_gate, require_admin
from web.middleware import RequestLoggingMiddleware, RequestTimeoutMiddleware
from bot.database import init_database
from core.duckdb_store import get_store, close_store
from core.sync_service import init_and_sync
from core.config import validate_config, ConfigurationError
from core.observability import setup_logging, get_logger
from core.scheduler import start_scheduler, stop_scheduler
from core.events import events, SyncEvent

# Configure structured logging
# Use JSON format in production (LOG_FORMAT=json), human-readable otherwise
log_format = os.getenv("LOG_FORMAT", "text")
log_level = os.getenv("LOG_LEVEL", "INFO")
setup_logging(level=log_level, json_format=(log_format == "json"))
logger = get_logger(__name__)

# Rate limiter is the shared instance from web.ratelimit (proxy-aware key func)

# Create FastAPI app
app = FastAPI(
    title="KeyCRM Dashboard",
    description="Sales analytics dashboard for KeyCRM",
    version=VERSION,
    default_response_class=ORJSONResponse  # 3-10x faster JSON serialization
)

# Add rate limiter to app state
app.state.limiter = limiter

# Custom rate limit exceeded handler
@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    logger.warning(f"Rate limit exceeded for {get_remote_address(request)}")
    return JSONResponse(
        status_code=429,
        content={
            "error": "Rate limit exceeded",
            "detail": "Too many requests. Please try again later.",
            "retry_after": exc.detail
        }
    )


# Centralized exception handlers for consistent error responses
from core.exceptions import ValidationError, QueryTimeoutError


@app.exception_handler(ValidationError)
async def validation_error_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=400,
        content={"error": "Validation Error", "detail": str(exc)}
    )


@app.exception_handler(QueryTimeoutError)
async def query_timeout_handler(request: Request, exc: QueryTimeoutError):
    logger.error(f"Query timeout on {request.url.path}: {exc}")
    return JSONResponse(
        status_code=504,
        content={"error": "Query Timeout", "detail": str(exc)}
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception on {request.method} {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "detail": "An unexpected error occurred."}
    )


# Add request logging middleware (adds correlation IDs and timing)
app.add_middleware(RequestLoggingMiddleware)

# Add request timeout middleware (prevents long-running requests)
# Must be AFTER logging so correlation_id is set when timeout fires
app.add_middleware(RequestTimeoutMiddleware)

# Add Gzip compression (min 500 bytes to compress)
app.add_middleware(GZipMiddleware, minimum_size=500)

# No CORSMiddleware: the SPA is served same-origin in every deployment (prod via
# nginx, dev via Vite's `/api`/`/ws` proxy → same-origin with localhost:5173),
# so cross-origin requests do not happen. The browser's same-origin policy is
# fail-closed by default; adding CORS would only *widen* the surface.

# Apple touch icon at root for iOS home screen
@app.get("/apple-touch-icon.png", include_in_schema=False)
@app.get("/apple-touch-icon-precomposed.png", include_in_schema=False)
async def apple_touch_icon():
    return FileResponse(STATIC_DIR / "apple-touch-icon.png", media_type="image/png")

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Mount React v2 static files (create directory if needed)
STATIC_V2_DIR.mkdir(exist_ok=True)
app.mount("/static-v2", StaticFiles(directory=str(STATIC_V2_DIR)), name="static-v2")

# Include routers (pages router LAST — it has a catch-all /{path:path})
# api_gate is the SINGLE authentication point for /api/*: any new endpoint
# under /api inherits it automatically. The only public paths are listed in
# PUBLIC_API_PATHS in web/routes/auth.py — one set to audit.
_api_gate = [Depends(api_gate)]
app.include_router(auth.router)  # /login, /logout, /auth/* — explicitly public
app.include_router(api.router, prefix="/api", dependencies=_api_gate)
app.include_router(websocket.router)  # WebSocket routes (no /api prefix) — self-gated
app.include_router(chat.router, prefix="/api", dependencies=[Depends(api_gate), Depends(require_admin)])
app.include_router(pages.router)  # SPA catch-all must be last


@app.on_event("startup")
async def startup_event():
    logger.info("KoreanStory Dashboard starting...")

    # Log sync mode for visibility
    from core.config import config as app_config
    logger.info(f"Sync mode: {app_config.sync.mode}")

    # A half-enabled read switch is the same trap as a typo in it: the flag
    # says postgres, the missing DSN quietly serves the old store, and nobody
    # learns until the numbers disagree. Loud at startup, once.
    import os as _os
    if (
        _os.getenv("KS_READ_GOLD", "").strip().lower() == "postgres"
        and not _os.getenv("KS_PG_DSN", "").strip()
    ):
        logger.error(
            "KS_READ_GOLD=postgres but KS_PG_DSN is not set — every Gold "
            "read will silently fall back to DuckDB. Set the DSN or unset "
            "the flag."
        )

    # Validate configuration early - fail fast with clear errors
    try:
        validate_config(require_bot=False, require_api=True, require_secret_key=True)
        logger.info("Configuration validated")
    except ConfigurationError as e:
        logger.critical(f"Configuration error: {e}")
        raise SystemExit(1)

    # Initialize SQLite database (for bot operations)
    init_database()
    logger.info("SQLite database initialized")

    # Initialize DuckDB analytics store and sync from API
    logger.info("Initializing DuckDB analytics store...")
    try:
        await init_and_sync(full_sync_days=730)
        store = await get_store()
        stats = await store.get_stats()
        logger.info(
            f"DuckDB ready: {stats['orders']} orders, "
            f"{stats['products']} products, "
            f"{stats['categories']} categories, "
            f"{stats['db_size_mb']} MB"
        )
    except Exception as e:
        logger.error(f"DuckDB sync failed on startup: {e}", exc_info=True)
        # Don't crash if store has data — serve stale data, scheduler will retry sync
        store = await get_store()
        stats = await store.get_stats()
        if stats.get("orders", 0) == 0:
            raise  # Fail fast only if DuckDB has no data at all
        logger.warning(f"Serving stale data ({stats['orders']} orders) — sync will retry via scheduler")

    # Migrate users from SQLite to DuckDB (one-time, idempotent)

    # ── One historical campaign, restored by hand ────────────────────────
    # `aug-promo-birthday-website` was sent on 2026-08-05, before the columns
    # recording the text and the bill existed, so its card had nothing to show.
    # The figures below are the ones established when that campaign was
    # audited: 5 550 recipients, but 8 375 messages billed at 1.2744 ₴ — a
    # second press resent 2 825 of them, which is the defect PR #23 closed.
    # Restoring the count instead of the bill would show a campaign that cost
    # 7 104 ₴, and it did not.
    #
    # The write fills NULLs only, so this is a no-op on every boot after the
    # first, and it can never overwrite a campaign the app recorded itself.
    try:
        restored = await store.backfill_sms_campaign_record(
            campaign="aug-promo-birthday-website",
            message_text=(
                "Красуне, нашому сайту 2 роки \u2665 -30%, лише 2 дні: "
                "koreanstory.com.ua"
            ),
            message_parts=1,
            recipients_sent=8375,
            price_per_part=1.2744,
            cost_total=10673.00,
            notes=(
                "Текст и стоимость восстановлены вручную: кампания отправлена "
                "до того, как приложение стало их записывать. 5 550 получателей, "
                "8 375 сообщений — 2 825 из них дубль от повторной отправки."
            ),
        )
        if restored:
            logger.info("Restored the August campaign's message and cost")
    except Exception as e:
        # A campaign card missing one line must never cost a startup.
        logger.warning(f"Campaign record restore skipped: {e}")

    # Start background job scheduler (replaces old asyncio background sync)
    try:
        await start_scheduler()
        logger.info("Background job scheduler started")
    except Exception as e:
        logger.error(f"Scheduler initialization failed: {e}", exc_info=True)
        # Non-fatal - dashboard can work without scheduler

    # Register event handlers for sync events
    _register_event_handlers()
    logger.info("Event handlers registered")

    # Train revenue prediction model in background (non-blocking)
    try:
        from core.prediction_service import get_prediction_service
        prediction_service = get_prediction_service()
        if not prediction_service.is_ready:
            asyncio.create_task(_train_prediction_model())
            logger.info("Revenue prediction model training scheduled")
        else:
            logger.info("Revenue prediction model loaded from disk")
    except Exception as e:
        logger.warning(f"Prediction service initialization skipped: {e}")

    logger.info("Dashboard ready - all queries use DuckDB")


# `_migrate_sqlite_users_to_duckdb` lived here until 2026-09-07.
#
# It read `data/bot.db` with `sqlite3` on every boot and created any user the
# DuckDB `users` table did not have. Two things retired it. The bot moved to
# Postgres on 2026-08-27, so that file's `authorized_users` has been a frozen
# artefact since — and it was already a no-op before that: measured the day it
# was removed, the frozen file and the DuckDB table held the *same 24 people*,
# so it had nothing to create. It also went round the seam
# (`core/bot_store.py`) and, once `KS_USER_STORE=postgres` made Postgres the
# writer, it would have been writing to the store nobody reads.
#
# The comment it carried is worth keeping, because the bug it describes is the
# kind that comes back: a "one-time repair" (070af9a, 2026-03-25) set
# role='viewer' on every boot for any admin outside ADMIN_USER_IDS. It was
# never retired, so every promotion made through the admin page lasted exactly
# until the next deploy, compact or OOM restart, with one log line to say so.


async def _train_prediction_model():
    """Train revenue prediction model in background after startup."""
    # Wait for DuckDB to be fully ready with data
    await asyncio.sleep(10)
    try:
        from core.prediction_service import get_prediction_service
        service = get_prediction_service()
        result = await service.train(sales_type="retail")
        logger.info(f"Prediction model training result: {result.get('status')}")
    except Exception as e:
        logger.warning(f"Background prediction training failed: {e}")


def _register_event_handlers():
    """Register handlers for sync events."""
    from core.websocket_manager import manager as ws_manager, WebSocketEvent

    @events.on(SyncEvent.ORDERS_SYNCED)
    async def on_orders_synced(data: dict):
        """Log orders synced and broadcast to WebSocket clients."""
        count = data.get("count", 0)
        if count > 0:
            logger.debug(f"Orders synced: {count} orders")

            # Broadcast to connected dashboard clients
            await ws_manager.broadcast(
                "dashboard",
                WebSocketEvent.ORDERS_SYNCED,
                {
                    "count": count,
                    "duration_ms": data.get("duration_ms", 0),
                }
            )

    @events.on(SyncEvent.PRODUCTS_SYNCED)
    async def on_products_synced(data: dict):
        """Log products synced and broadcast to WebSocket clients."""
        count = data.get("count", 0)
        if count > 0:
            logger.debug(f"Products synced: {count} products")

            # Broadcast to connected dashboard clients
            await ws_manager.broadcast(
                "dashboard",
                WebSocketEvent.PRODUCTS_SYNCED,
                {"count": count}
            )

    @events.on(SyncEvent.INVENTORY_UPDATED)
    async def on_inventory_updated(data: dict):
        """Broadcast inventory updates to WebSocket clients."""
        await ws_manager.broadcast(
            "dashboard",
            WebSocketEvent.INVENTORY_UPDATED,
            data
        )

    @events.on(SyncEvent.SYNC_FAILED)
    async def on_sync_failed(data: dict):
        """Log sync failures and notify admin WebSocket clients."""
        sync_type = data.get("sync_type", "unknown")
        error = data.get("error", "unknown error")
        logger.warning(f"Sync failed: {sync_type} - {error}")

        # Notify admin clients
        await ws_manager.broadcast(
            "admin",
            "sync_failed",
            {"sync_type": sync_type, "error": error}
        )

    @events.on(SyncEvent.GOALS_UPDATED)
    async def on_goals_updated(data: dict):
        """Broadcast goal updates to WebSocket clients."""
        await ws_manager.broadcast(
            "dashboard",
            WebSocketEvent.GOAL_PROGRESS,
            data
        )


@app.on_event("shutdown")
async def shutdown_event():
    # Stop scheduler first (graceful shutdown of background jobs)
    try:
        stop_scheduler()
        logger.info("Scheduler stopped")
    except Exception as e:
        logger.warning(f"Error stopping scheduler: {e}")

    # Shutdown prediction service (ThreadPoolExecutor cleanup)
    try:
        from core.prediction_service import shutdown_prediction_service
        shutdown_prediction_service()
    except Exception as e:
        logger.warning(f"Error shutting down prediction service: {e}")

    # Close KeyCRM client (HTTP connection cleanup)
    try:
        from core.keycrm import close_client
        await close_client()
    except Exception as e:
        logger.warning(f"Error closing KeyCRM client: {e}")

    # Close DuckDB
    try:
        await close_store()
        logger.info("DuckDB closed")
    except Exception as e:
        logger.warning(f"Error closing DuckDB: {e}")
    logger.info("KoreanStory Dashboard stopped")
