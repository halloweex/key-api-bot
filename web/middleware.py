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
from typing import Any, Callable, Mapping
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
    "/api/duckdb/refresh-statuses",
    "/api/goals/recalculate",
    "/api/stocks/analysis",
    "/api/revenue/forecast/train",
    "/api/revenue/forecast/evaluate",
    "/api/revenue/forecast/tune",
    "/api/traffic/reclassify",
}


UNMATCHED_METRIC_KEY = "<unmatched>"


def _templated(path: str, path_params: Mapping[str, Any]) -> str:
    """`path` with every value the router bound to a parameter put back as its name."""
    names_by_value: dict[str, str] = {}
    for name, value in path_params.items():
        text = str(value)
        # An empty value matches between every character; it names nothing.
        if text:
            names_by_value.setdefault(text, name)
    if not names_by_value:
        return path

    # A `:path` converter spans several segments, so its value has to go before
    # the segment walk below can recognise the segments either side of it.
    for text in sorted((v for v in names_by_value if "/" in v), key=len, reverse=True):
        path = path.replace(text, "{%s}" % names_by_value[text], 1)

    return "/".join(
        "{%s}" % names_by_value[segment] if segment in names_by_value else segment
        for segment in path.split("/")
    )


def request_metric_key(method: str, path: str, scope: Any) -> str:
    """The metric name for one request — the route, never the identifiers in it.

    `metrics.get_stats()` is returned verbatim by `GET /api/metrics`, which any
    approved viewer may read. Keyed on the path as asked, that dictionary was a
    directory of the ids other people had used — buyer ids, the Telegram id of
    an admin whose role was changed, campaign and preset names — which turns
    "you must know a valid id" into "here is the list of valid ids". Refused
    requests are recorded too, so a 403 published the id it had just refused.
    It was also unbounded: one entry per distinct id for the life of the
    process, each with its own list of latency samples.

    Templated from the request's own path and `scope["path_params"]` rather
    than from `route.path_format`, which would be the obvious source: the
    parameters are Starlette-level and read the same on every version, while a
    route reached through an included router carries an un-prefixed path (see
    tests/routes_helper), so its `path_format` loses the `/api` on the FastAPI
    generation production runs.

    A request that matched no route has no template — a mounted application
    (`/static`) publishes none, and neither does a 404. It is counted under its
    mount or under one sentinel, never under the path that was asked for, which
    is the same attacker-chosen key by another name.
    """
    # This runs on every request, including the health checks Docker and nginx
    # depend on. Nothing here is worth a 500, and the fallback discloses
    # nothing, so an unreadable scope costs the breakdown and not the request.
    try:
        if not isinstance(scope, Mapping):
            return f"{method} {UNMATCHED_METRIC_KEY}"
        if scope.get("route") is None:
            mount = scope.get("root_path") or ""
            return f"{method} {mount}/*" if mount else f"{method} {UNMATCHED_METRIC_KEY}"
        return f"{method} {_templated(path, scope.get('path_params') or {})}"
    except Exception:  # noqa: BLE001 — see above
        return f"{method} {UNMATCHED_METRIC_KEY}"


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

        # Record metrics. The scope is read only now, after `call_next`: the
        # router writes the matched route and its parameters into it, and
        # before the call there is nothing there to template with.
        endpoint = request_metric_key(method, path, request.scope)
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
