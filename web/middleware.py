"""
FastAPI middleware for observability.

Provides:
- Request correlation ID injection
- Request/response logging
- Timing metrics
- Request timeout protection
"""
import asyncio
import time
from typing import Callable
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

from core.observability import (
    get_logger,
    generate_correlation_id,
    set_correlation_id,
    get_correlation_id,
    metrics,
    Timer,
)

logger = get_logger(__name__)

# Request timeout settings (seconds)
DEFAULT_REQUEST_TIMEOUT = 30.0
SLOW_ENDPOINT_TIMEOUT = 300.0  # For heavy analytics endpoints (tune can take minutes)

# Endpoints that get extended timeout
SLOW_ENDPOINTS = {
    "/api/duckdb/resync",
    "/api/goals/recalculate",
    "/api/stocks/analysis",
    "/api/revenue/forecast/train",
    "/api/revenue/forecast/evaluate",
    "/api/revenue/forecast/tune",
}


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware that:
    1. Assigns correlation ID to each request
    2. Logs request start/end with timing
    3. Records metrics
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Get or generate correlation ID
        correlation_id = request.headers.get("X-Request-ID") or generate_correlation_id()
        set_correlation_id(correlation_id)

        # Start timing
        start_time = time.perf_counter()

        # Get request info
        method = request.method
        path = request.url.path
        client_ip = request.client.host if request.client else "unknown"

        # Skip logging for health checks to reduce noise
        is_health_check = path in ("/api/health", "/health")

        if not is_health_check:
            logger.info(
                f"Request started: {method} {path}",
                extra={
                    "method": method,
                    "path": path,
                    "client_ip": client_ip,
                }
            )

        # Process request
        try:
            response = await call_next(request)
        except Exception as e:
            # Log error
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.error(
                f"Request failed: {method} {path}",
                extra={
                    "method": method,
                    "path": path,
                    "duration_ms": round(duration_ms, 2),
                    "error": str(e),
                }
            )
            metrics.record_error(type(e).__name__)
            raise

        # Calculate duration
        duration_ms = (time.perf_counter() - start_time) * 1000

        # Add correlation ID to response headers
        response.headers["X-Request-ID"] = correlation_id
        response.headers["X-Response-Time"] = f"{duration_ms:.2f}ms"

        # Log completion
        if not is_health_check:
            level_name = "info" if response.status_code < 400 else "warning"
            getattr(logger, level_name)(
                f"Request completed: {method} {path}",
                extra={
                    "method": method,
                    "path": path,
                    "status_code": response.status_code,
                    "duration_ms": round(duration_ms, 2),
                }
            )

        # Record metrics
        endpoint = f"{method} {path}"
        metrics.record_request(endpoint)
        metrics.record_timing(endpoint, duration_ms)

        if response.status_code >= 400:
            metrics.record_error(f"HTTP_{response.status_code}")

        return response


class RequestTimeoutMiddleware(BaseHTTPMiddleware):
    """
    Middleware that enforces request timeout.

    Prevents long-running requests from blocking resources.
    Returns 504 Gateway Timeout if request exceeds timeout.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.url.path

        # Determine timeout based on endpoint
        if path in SLOW_ENDPOINTS:
            timeout = SLOW_ENDPOINT_TIMEOUT
        else:
            timeout = DEFAULT_REQUEST_TIMEOUT

        # Skip timeout for health checks and static files
        if path.startswith("/static") or path in ("/api/health", "/health", "/"):
            return await call_next(request)

        try:
            return await asyncio.wait_for(
                call_next(request),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"Request timeout: {request.method} {path}",
                extra={
                    "method": request.method,
                    "path": path,
                    "timeout": timeout,
                }
            )
            metrics.record_error("REQUEST_TIMEOUT")
            return JSONResponse(
                status_code=504,
                content={
                    "error": "Request Timeout",
                    "detail": f"Request exceeded {timeout}s timeout",
                    "path": path,
                    "correlation_id": get_correlation_id(),
                }
            )
