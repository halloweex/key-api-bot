"""
FastAPI web application for KeyCRM Dashboard.
"""
import logging
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from web.config import STATIC_DIR
from web.routes import api, pages

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
    version="1.0.0"
)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Include routers
app.include_router(pages.router)
app.include_router(api.router, prefix="/api")


@app.on_event("startup")
async def startup_event():
    logger.info("KeyCRM Dashboard started")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("KeyCRM Dashboard stopped")
