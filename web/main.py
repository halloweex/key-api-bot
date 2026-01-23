"""
FastAPI web application for KeyCRM Dashboard.
"""
import logging
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import ORJSONResponse, JSONResponse
from starlette.middleware.gzip import GZipMiddleware

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from web.config import STATIC_DIR, STATIC_V2_DIR, VERSION
from web.routes import api, pages, auth
from bot.database import init_database
from core.duckdb_store import get_store, close_store
from core.sync_service import init_and_sync, get_sync_service
from core.config import validate_config, ConfigurationError

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

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

    logger.info("Dashboard ready - all queries use DuckDB")


@app.on_event("shutdown")
async def shutdown_event():
    # Stop background sync and close DuckDB
    try:
        sync_service = await get_sync_service()
        sync_service.stop_background_sync()
        await close_store()
        logger.info("DuckDB closed")
    except Exception as e:
        logger.warning(f"Error closing DuckDB: {e}")
    logger.info("KoreanStory Dashboard stopped")
