"""
FastAPI web application for KeyCRM Dashboard.
"""
import logging
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import ORJSONResponse
from starlette.middleware.gzip import GZipMiddleware

from web.config import STATIC_DIR, VERSION
from web.routes import api, pages
from web.services.dashboard_service import start_cache_warming, stop_cache_warming
from web.services.category_service import warm_product_cache

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="KeyCRM Dashboard",
    description="Sales analytics dashboard for KeyCRM",
    version=VERSION,
    default_response_class=ORJSONResponse  # 3-10x faster JSON serialization
)

# Add Gzip compression (min 500 bytes to compress)
app.add_middleware(GZipMiddleware, minimum_size=500)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Include routers
app.include_router(pages.router)
app.include_router(api.router, prefix="/api")


@app.on_event("startup")
async def startup_event():
    logger.info("KeyCRM Dashboard started")
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
