"""Health check, metrics, and DuckDB stats endpoints."""
import asyncio
import time

from fastapi import APIRouter, Request

from core.observability import get_correlation_id, metrics, Timer
from web.config import VERSION
from web.schemas import HealthResponse, MetricsResponse
from ._deps import limiter, get_store, get_logger, START_TIME

router = APIRouter()
logger = get_logger(__name__)

# Health check stats cache (60 second TTL) with thread-safe lock
_stats_cache: dict = {"data": None, "expires_at": 0}
_stats_cache_lock = asyncio.Lock()
_STATS_CACHE_TTL = 60


@router.get("/health", response_model=HealthResponse)
@limiter.limit("60/minute")
async def health_check(request: Request):
    """Health check endpoint for Docker/load balancer monitoring."""
    uptime_seconds = int(time.time() - START_TIME)

    now = time.time()
    async with _stats_cache_lock:
        if _stats_cache["data"] and now < _stats_cache["expires_at"]:
            duckdb_stats = _stats_cache["data"]
            duckdb_status = "connected"
            db_latency_ms = 0.0
        else:
            db_latency_ms = None
            try:
                with Timer("health_check_db") as timer:
                    store = await get_store()
                    duckdb_stats = await store.get_stats()
                duckdb_status = "connected"
                db_latency_ms = round(timer.elapsed_ms, 2)
                _stats_cache["data"] = duckdb_stats
                _stats_cache["expires_at"] = now + _STATS_CACHE_TTL
            except Exception as e:
                duckdb_stats = None
                duckdb_status = f"error: {e}"

    sync_status = None
    try:
        from core.sync_service import get_sync_service
        sync_service = await get_sync_service()
        sync_stats = sync_service.get_sync_stats()

        seconds_since_sync = None
        if sync_stats.get("last_sync_time"):
            from datetime import datetime
            from core.config import DEFAULT_TZ
            last_sync = datetime.fromisoformat(sync_stats["last_sync_time"])
            seconds_since_sync = int((datetime.now(DEFAULT_TZ) - last_sync).total_seconds())

        if seconds_since_sync is None:
            status = "idle"
        elif seconds_since_sync > 900:
            status = "stale"
        else:
            status = "active"

        sync_status = {
            "status": status,
            "last_sync_time": sync_stats.get("last_sync_time"),
            "seconds_since_sync": seconds_since_sync,
            "consecutive_empty_syncs": sync_stats.get("consecutive_empty_syncs", 0),
            "current_backoff_seconds": sync_stats.get("current_backoff_seconds", 300),
            "is_off_hours": sync_stats.get("is_off_hours", False),
        }
    except Exception as e:
        logger.debug(f"Could not get sync status: {e}")

    return {
        "status": "healthy" if duckdb_stats else "degraded",
        "version": VERSION,
        "uptime_seconds": uptime_seconds,
        "correlation_id": get_correlation_id(),
        "duckdb": {
            "status": duckdb_status,
            "latency_ms": db_latency_ms,
            **(duckdb_stats or {})
        },
        "sync": sync_status,
    }


@router.get("/health/detailed")
@limiter.limit("30/minute")
async def detailed_health_check(request: Request):
    """Detailed health check with component-level status."""
    import psutil

    components = {}
    overall_status = "healthy"

    # 1. DuckDB check
    try:
        with Timer("health_duckdb") as timer:
            store = await get_store()
            duckdb_stats = await store.get_stats()
        components["duckdb"] = {
            "status": "connected",
            "latency_ms": round(timer.elapsed_ms, 2),
            **duckdb_stats,
        }
    except Exception as e:
        components["duckdb"] = {"status": "error", "error": str(e)}
        overall_status = "degraded"

    # 2. Redis check
    try:
        from core.cache import cache
        if cache.is_connected:
            components["redis"] = {"status": "connected", **cache.get_stats()}
        else:
            components["redis"] = {"status": "not_connected"}
    except Exception as e:
        components["redis"] = {"status": "error", "error": str(e)}

    # 3. Meilisearch check
    try:
        import httpx
        import os
        meili_url = os.getenv("MEILI_URL", "http://meilisearch:7700")
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(f"{meili_url}/health")
            if response.status_code == 200:
                components["meilisearch"] = {"status": "healthy"}
            else:
                components["meilisearch"] = {"status": "unhealthy", "code": response.status_code}
    except Exception as e:
        components["meilisearch"] = {"status": "unavailable", "error": str(e)}

    # 4. WebSocket connections
    try:
        from core.websocket_manager import manager as ws_manager
        ws_stats = ws_manager.get_stats()
        components["websocket"] = {"status": "active", **ws_stats}
    except Exception as e:
        components["websocket"] = {"status": "error", "error": str(e)}

    # 5. Sync service status
    try:
        from core.sync_service import get_sync_service
        sync_service = await get_sync_service()
        sync_stats = sync_service.get_sync_stats()
        components["sync"] = {
            "status": "active" if sync_stats.get("last_sync_time") else "idle",
            **sync_stats,
        }
    except Exception as e:
        components["sync"] = {"status": "error", "error": str(e)}

    # 6. Prediction service
    try:
        from core.prediction_service import get_prediction_service
        pred_service = get_prediction_service()
        components["prediction"] = {
            "status": "ready" if pred_service.is_ready else "not_ready",
            "model_loaded": pred_service.is_ready,
        }
    except Exception as e:
        components["prediction"] = {"status": "unavailable", "error": str(e)}

    # System metrics
    uptime_seconds = int(time.time() - START_TIME)
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        sys_metrics = {
            "uptime_seconds": uptime_seconds,
            "memory_mb": round(memory_info.rss / 1024 / 1024, 1),
            "memory_percent": round(process.memory_percent(), 1),
            "cpu_percent": round(process.cpu_percent(interval=0.1), 1),
            "threads": process.num_threads(),
        }
    except Exception:
        sys_metrics = {"uptime_seconds": uptime_seconds}

    return {
        "status": overall_status,
        "version": VERSION,
        "correlation_id": get_correlation_id(),
        "components": components,
        "metrics": sys_metrics,
    }


@router.get("/duckdb/stats")
@limiter.limit("60/minute")
async def get_duckdb_stats(request: Request):
    """Get DuckDB analytics store statistics."""
    try:
        store = await get_store()
        stats = await store.get_stats()
        return {"status": "connected", **stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/metrics", response_model=MetricsResponse)
@limiter.limit("60/minute")
async def get_metrics_endpoint(request: Request):
    """Get application metrics."""
    return {
        "uptime_seconds": int(time.time() - START_TIME),
        "correlation_id": get_correlation_id(),
        **metrics.get_stats(),
    }
