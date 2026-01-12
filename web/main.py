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

from web.config import STATIC_DIR, VERSION
from web.routes import api, pages, auth
from web.services.dashboard_service import start_cache_warming, stop_cache_warming
from web.services.category_service import warm_product_cache
from bot.database import init_database

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

# Include routers
app.include_router(auth.router)  # Auth routes first (login, logout, callback)
app.include_router(pages.router)
app.include_router(api.router, prefix="/api")


@app.on_event("startup")
async def startup_event():
    logger.info("KoreanStory Dashboard started")
    # Initialize database (creates tables if not exist)
    init_database()
    logger.info("Database initialized")
    # Pre-load product categories for filtering
    logger.info("Warming product category cache...")
    warm_product_cache()
    logger.info("Product category cache ready")
    # Start background cache warming
    start_cache_warming()
    logger.info("Background cache warming started")


@app.on_event("shutdown")
async def shutdown_event():
    stop_cache_warming()
    logger.info("KeyCRM Dashboard stopped")
