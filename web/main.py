"""
FastAPI web application for KeyCRM Dashboard.
"""
import asyncio
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import ORJSONResponse, JSONResponse
from starlette.middleware.gzip import GZipMiddleware

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from web.config import STATIC_DIR, STATIC_V2_DIR, VERSION
from web.routes import api, pages, auth, chat
from web.middleware import RequestLoggingMiddleware, RequestTimeoutMiddleware
from bot.database import init_database
from core.duckdb_store import get_store, close_store
from core.sync_service import init_and_sync, get_sync_service
from core.config import validate_config, ConfigurationError
from core.observability import setup_logging, get_logger
from core.scheduler import start_scheduler, stop_scheduler
from core.events import events, SyncEvent
from core.cache import cache, register_cache_invalidation_handlers

# Configure structured logging
# Use JSON format in production (LOG_FORMAT=json), human-readable otherwise
log_format = os.getenv("LOG_FORMAT", "text")
log_level = os.getenv("LOG_LEVEL", "INFO")
setup_logging(level=log_level, json_format=(log_format == "json"))
logger = get_logger(__name__)

# Rate limiter configuration
limiter = Limiter(key_func=get_remote_address)

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

# Add request logging middleware (adds correlation IDs and timing)
app.add_middleware(RequestLoggingMiddleware)

# Add request timeout middleware (prevents long-running requests)
# Must be AFTER logging so correlation_id is set when timeout fires
app.add_middleware(RequestTimeoutMiddleware)

# Add Gzip compression (min 500 bytes to compress)
app.add_middleware(GZipMiddleware, minimum_size=500)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Mount React v2 static files (create directory if needed)
STATIC_V2_DIR.mkdir(exist_ok=True)
app.mount("/static-v2", StaticFiles(directory=str(STATIC_V2_DIR)), name="static-v2")

# Include routers
app.include_router(auth.router)  # Auth routes first (login, logout, callback)
app.include_router(pages.router)
app.include_router(api.router, prefix="/api")
app.include_router(chat.router, prefix="/api")


@app.on_event("startup")
async def startup_event():
    logger.info("KoreanStory Dashboard starting...")

    # Validate configuration early - fail fast with clear errors
    try:
        validate_config(require_bot=False, require_api=True)
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
        await init_and_sync(full_sync_days=365)
        store = await get_store()
        stats = await store.get_stats()
        logger.info(
            f"DuckDB ready: {stats['orders']} orders, "
            f"{stats['products']} products, "
            f"{stats['categories']} categories, "
            f"{stats['db_size_mb']} MB"
        )
    except Exception as e:
        logger.error(f"DuckDB initialization failed: {e}", exc_info=True)
        raise  # Fail fast - DuckDB is required

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

    # Initialize Redis cache (non-fatal if unavailable)
    try:
        if await cache.connect():
            register_cache_invalidation_handlers()
            logger.info("Redis cache connected")
        else:
            logger.info("Redis cache not available, running without cache")
    except Exception as e:
        logger.warning(f"Redis cache initialization failed: {e}")

    logger.info("Dashboard ready - all queries use DuckDB")


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

    @events.on(SyncEvent.ORDERS_SYNCED)
    async def on_orders_synced(data: dict):
        """Log orders synced and potentially invalidate caches."""
        count = data.get("count", 0)
        if count > 0:
            logger.debug(f"Orders synced: {count} orders")
            # Future: Invalidate dashboard cache here
            # await cache.invalidate_pattern("dashboard:*")

    @events.on(SyncEvent.PRODUCTS_SYNCED)
    async def on_products_synced(data: dict):
        """Log products synced."""
        count = data.get("count", 0)
        if count > 0:
            logger.debug(f"Products synced: {count} products")

    @events.on(SyncEvent.SYNC_FAILED)
    async def on_sync_failed(data: dict):
        """Log sync failures for monitoring."""
        sync_type = data.get("sync_type", "unknown")
        error = data.get("error", "unknown error")
        logger.warning(f"Sync failed: {sync_type} - {error}")
        # Future: Send alert to admin
        # await notify_admin(f"Sync failed: {error}")


@app.on_event("shutdown")
async def shutdown_event():
    # Stop scheduler first (graceful shutdown of background jobs)
    try:
        stop_scheduler()
        logger.info("Scheduler stopped")
    except Exception as e:
        logger.warning(f"Error stopping scheduler: {e}")

    # Disconnect Redis cache
    try:
        await cache.disconnect()
    except Exception as e:
        logger.warning(f"Error disconnecting Redis: {e}")

    # Stop legacy background sync (if running) and close DuckDB
    try:
        sync_service = await get_sync_service()
        sync_service.stop_background_sync()
        await close_store()
        logger.info("DuckDB closed")
    except Exception as e:
        logger.warning(f"Error closing DuckDB: {e}")
    logger.info("KoreanStory Dashboard stopped")
