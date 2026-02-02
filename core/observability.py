"""
Observability module for structured logging, correlation IDs, and metrics.

Usage:
    from core.observability import setup_logging, get_logger, correlation_context

    # In app startup:
    setup_logging()

    # In handlers:
    logger = get_logger(__name__)

    # In middleware:
    with correlation_context(request_id):
        logger.info("Processing request", extra={"path": request.url.path})
"""
import logging
import json
import time
import uuid
import functools
from contextvars import ContextVar
from typing import Optional, Any, Dict, Callable
from datetime import datetime, timezone

# Context variable for request correlation ID
_correlation_id: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)

# Context variable for additional context (user_id, etc.)
_log_context: ContextVar[Dict[str, Any]] = ContextVar("log_context", default={})


def get_correlation_id() -> Optional[str]:
    """Get the current correlation ID from context."""
    return _correlation_id.get()


def set_correlation_id(correlation_id: str) -> None:
    """Set correlation ID in context."""
    _correlation_id.set(correlation_id)


def generate_correlation_id() -> str:
    """Generate a new correlation ID."""
    return str(uuid.uuid4())[:8]


class correlation_context:
    """Context manager for setting correlation ID."""

    def __init__(self, correlation_id: Optional[str] = None):
        self.correlation_id = correlation_id or generate_correlation_id()
        self.token = None

    def __enter__(self):
        self.token = _correlation_id.set(self.correlation_id)
        return self.correlation_id

    def __exit__(self, *args):
        _correlation_id.reset(self.token)


def add_log_context(**kwargs) -> None:
    """Add extra context to be included in all log messages."""
    current = _log_context.get()
    _log_context.set({**current, **kwargs})


def clear_log_context() -> None:
    """Clear the log context."""
    _log_context.set({})


class StructuredFormatter(logging.Formatter):
    """
    JSON log formatter for structured logging.

    Outputs logs as JSON with:
    - timestamp, level, logger, message
    - correlation_id (if set)
    - extra fields from log context
    - exception info (if present)
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add correlation ID if available
        correlation_id = get_correlation_id()
        if correlation_id:
            log_entry["correlation_id"] = correlation_id

        # Add context vars
        context = _log_context.get()
        if context:
            log_entry.update(context)

        # Add extra fields from record (but exclude standard attributes)
        standard_attrs = {
            "name", "msg", "args", "created", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs",
            "pathname", "process", "processName", "relativeCreated",
            "stack_info", "exc_info", "exc_text", "thread", "threadName",
            "message", "asctime", "taskName"
        }
        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith("_"):
                log_entry[key] = value

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


class HumanReadableFormatter(logging.Formatter):
    """
    Human-readable log formatter with correlation ID.

    Format: TIMESTAMP - LEVEL - LOGGER [CORRELATION_ID] - MESSAGE
    """

    def format(self, record: logging.LogRecord) -> str:
        correlation_id = get_correlation_id()
        correlation_str = f" [{correlation_id}]" if correlation_id else ""

        # Format timestamp
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        base_msg = f"{timestamp} - {record.levelname:8} - {record.name}{correlation_str} - {record.getMessage()}"

        # Add extra fields if present
        standard_attrs = {
            "name", "msg", "args", "created", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs",
            "pathname", "process", "processName", "relativeCreated",
            "stack_info", "exc_info", "exc_text", "thread", "threadName",
            "message", "asctime", "taskName"
        }
        extras = {k: v for k, v in record.__dict__.items()
                  if k not in standard_attrs and not k.startswith("_")}
        if extras:
            base_msg += f" | {extras}"

        if record.exc_info:
            base_msg += f"\n{self.formatException(record.exc_info)}"

        return base_msg


def setup_logging(
    level: str = "INFO",
    json_format: bool = False,
    include_libs: bool = False
) -> None:
    """
    Configure logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: If True, output JSON logs; otherwise human-readable
        include_libs: If True, also log from third-party libraries
    """
    # Create formatter
    if json_format:
        formatter = StructuredFormatter()
    else:
        formatter = HumanReadableFormatter()

    # Configure root handler
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, level.upper()))

    # Quiet down noisy libraries unless explicitly included
    if not include_libs:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
        logging.getLogger("watchfiles").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name."""
    return logging.getLogger(name)


# ═══════════════════════════════════════════════════════════════════════════════
# TIMING METRICS
# ═══════════════════════════════════════════════════════════════════════════════

class Timer:
    """
    Context manager for timing operations.

    Usage:
        with Timer("query_orders") as t:
            result = await store.get_orders()
        print(f"Query took {t.elapsed_ms}ms")
    """

    def __init__(self, name: str, logger: Optional[logging.Logger] = None):
        self.name = name
        self.logger = logger
        self.start_time: float = 0
        self.end_time: float = 0
        self.elapsed_ms: float = 0

    def __enter__(self) -> "Timer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end_time = time.perf_counter()
        self.elapsed_ms = (self.end_time - self.start_time) * 1000

        if self.logger:
            level = logging.WARNING if self.elapsed_ms > 1000 else logging.DEBUG
            self.logger.log(
                level,
                f"{self.name} completed",
                extra={"duration_ms": round(self.elapsed_ms, 2)}
            )


def timed(name: Optional[str] = None, warn_threshold_ms: float = 1000):
    """
    Decorator for timing function execution.

    Args:
        name: Operation name (defaults to function name)
        warn_threshold_ms: Log at WARNING level if exceeds this threshold
    """
    def decorator(func: Callable) -> Callable:
        operation_name = name or func.__name__
        func_logger = get_logger(func.__module__)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return await func(*args, **kwargs)
            finally:
                elapsed_ms = (time.perf_counter() - start) * 1000
                level = logging.WARNING if elapsed_ms > warn_threshold_ms else logging.DEBUG
                func_logger.log(
                    level,
                    f"{operation_name} completed",
                    extra={"duration_ms": round(elapsed_ms, 2)}
                )

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed_ms = (time.perf_counter() - start) * 1000
                level = logging.WARNING if elapsed_ms > warn_threshold_ms else logging.DEBUG
                func_logger.log(
                    level,
                    f"{operation_name} completed",
                    extra={"duration_ms": round(elapsed_ms, 2)}
                )

        if asyncio_iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


def asyncio_iscoroutinefunction(func: Callable) -> bool:
    """Check if function is async."""
    import asyncio
    return asyncio.iscoroutinefunction(func)


# ═══════════════════════════════════════════════════════════════════════════════
# METRICS COLLECTOR (simple in-memory stats)
# ═══════════════════════════════════════════════════════════════════════════════

class MetricsCollector:
    """
    Simple in-memory metrics collector.

    Tracks:
    - Request counts by endpoint
    - Error counts
    - Query timing histograms
    """

    def __init__(self):
        self._request_counts: Dict[str, int] = {}
        self._error_counts: Dict[str, int] = {}
        self._timing_samples: Dict[str, list] = {}
        self._max_samples = 100  # Keep last N samples per metric

    def record_request(self, endpoint: str) -> None:
        """Record a request to an endpoint."""
        self._request_counts[endpoint] = self._request_counts.get(endpoint, 0) + 1

    def record_error(self, error_type: str) -> None:
        """Record an error."""
        self._error_counts[error_type] = self._error_counts.get(error_type, 0) + 1

    def record_timing(self, operation: str, duration_ms: float) -> None:
        """Record timing for an operation."""
        if operation not in self._timing_samples:
            self._timing_samples[operation] = []

        samples = self._timing_samples[operation]
        samples.append(duration_ms)

        # Keep only last N samples
        if len(samples) > self._max_samples:
            self._timing_samples[operation] = samples[-self._max_samples:]

    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        stats = {
            "requests": dict(self._request_counts),
            "errors": dict(self._error_counts),
            "timing": {}
        }

        for operation, samples in self._timing_samples.items():
            if samples:
                sorted_samples = sorted(samples)
                stats["timing"][operation] = {
                    "count": len(samples),
                    "avg_ms": round(sum(samples) / len(samples), 2),
                    "min_ms": round(min(samples), 2),
                    "max_ms": round(max(samples), 2),
                    "p50_ms": round(sorted_samples[len(sorted_samples) // 2], 2),
                    "p95_ms": round(sorted_samples[int(len(sorted_samples) * 0.95)], 2) if len(sorted_samples) >= 20 else None,
                }

        return stats

    def reset(self) -> None:
        """Reset all metrics."""
        self._request_counts.clear()
        self._error_counts.clear()
        self._timing_samples.clear()


# Global metrics instance
metrics = MetricsCollector()
