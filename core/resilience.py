"""
Resilience patterns for API clients.

Provides:
- Exponential backoff retry
- Circuit breaker
- Rate limiter
"""
import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable, Any, TypeVar
from functools import wraps

from core.observability import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0  # seconds
    max_delay: float = 30.0  # seconds
    exponential_base: float = 2.0
    jitter: float = 0.1  # random jitter factor


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5  # failures before opening
    recovery_timeout: float = 60.0  # seconds before trying again
    half_open_requests: int = 1  # requests to test in half-open


@dataclass
class CircuitBreaker:
    """
    Circuit breaker implementation.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Circuit is tripped, requests fail immediately
    - HALF_OPEN: Testing if service recovered
    """
    config: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: float = 0
    half_open_attempts: int = 0

    def __post_init__(self):
        self._lock = asyncio.Lock()

    async def can_execute(self) -> bool:
        """Check if request can proceed."""
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                return True

            if self.state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                if time.time() - self.last_failure_time >= self.config.recovery_timeout:
                    logger.info("Circuit breaker entering half-open state")
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_attempts = 0
                    return True
                return False

            if self.state == CircuitState.HALF_OPEN:
                # Allow limited requests in half-open
                if self.half_open_attempts < self.config.half_open_requests:
                    self.half_open_attempts += 1
                    return True
                return False

            return False

    async def record_success(self) -> None:
        """Record successful request."""
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                logger.info("Circuit breaker closing after successful half-open request")
                self.state = CircuitState.CLOSED
            self.failure_count = 0

    async def record_failure(self) -> None:
        """Record failed request."""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == CircuitState.HALF_OPEN:
                logger.warning("Circuit breaker re-opening after failed half-open request")
                self.state = CircuitState.OPEN

            elif self.state == CircuitState.CLOSED:
                if self.failure_count >= self.config.failure_threshold:
                    logger.warning(
                        f"Circuit breaker opening after {self.failure_count} failures"
                    )
                    self.state = CircuitState.OPEN

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (rejecting requests)."""
        return self.state == CircuitState.OPEN


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


@dataclass
class RateLimiter:
    """
    Token bucket rate limiter.

    Args:
        rate: Requests per second
        burst: Maximum burst size
    """
    rate: float = 10.0
    burst: int = 20
    tokens: float = field(default=0, init=False)
    last_update: float = field(default=0, init=False)

    def __post_init__(self):
        self.tokens = float(self.burst)
        self.last_update = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, timeout: float = 10.0) -> bool:
        """
        Acquire a token, waiting if necessary.

        Args:
            timeout: Maximum time to wait for a token

        Returns:
            True if token acquired, False if timeout
        """
        start_time = time.time()

        while True:
            async with self._lock:
                now = time.time()
                # Replenish tokens
                elapsed = now - self.last_update
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return True

            # Check timeout
            if time.time() - start_time >= timeout:
                return False

            # Wait for tokens to replenish
            wait_time = (1 - self.tokens) / self.rate
            await asyncio.sleep(min(wait_time, 0.1))


async def retry_with_backoff(
    func: Callable[..., Any],
    *args,
    config: Optional[RetryConfig] = None,
    retryable_exceptions: tuple = (Exception,),
    **kwargs
) -> Any:
    """
    Execute function with exponential backoff retry.

    Args:
        func: Async function to execute
        *args: Positional arguments for func
        config: Retry configuration
        retryable_exceptions: Exceptions to retry on
        **kwargs: Keyword arguments for func

    Returns:
        Result of func

    Raises:
        Last exception if all retries fail
    """
    config = config or RetryConfig()
    last_exception = None

    for attempt in range(1, config.max_attempts + 1):
        try:
            return await func(*args, **kwargs)
        except retryable_exceptions as e:
            last_exception = e

            if attempt == config.max_attempts:
                logger.error(
                    f"All {config.max_attempts} retry attempts failed",
                    extra={"error": str(e)}
                )
                raise

            # Calculate delay with exponential backoff
            delay = min(
                config.base_delay * (config.exponential_base ** (attempt - 1)),
                config.max_delay
            )

            # Add jitter
            import random
            jitter = delay * config.jitter * random.random()
            delay += jitter

            logger.warning(
                f"Attempt {attempt} failed, retrying in {delay:.2f}s",
                extra={"attempt": attempt, "delay": delay, "error": str(e)}
            )

            await asyncio.sleep(delay)

    raise last_exception


def with_retry(
    config: Optional[RetryConfig] = None,
    retryable_exceptions: tuple = (Exception,)
):
    """
    Decorator for adding retry logic to async functions.

    Usage:
        @with_retry(RetryConfig(max_attempts=3))
        async def fetch_data():
            ...
    """
    config = config or RetryConfig()

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await retry_with_backoff(
                func, *args,
                config=config,
                retryable_exceptions=retryable_exceptions,
                **kwargs
            )
        return wrapper
    return decorator


def with_circuit_breaker(circuit_breaker: CircuitBreaker):
    """
    Decorator for adding circuit breaker to async functions.

    Usage:
        cb = CircuitBreaker()

        @with_circuit_breaker(cb)
        async def call_api():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if not await circuit_breaker.can_execute():
                raise CircuitOpenError(
                    f"Circuit breaker is open, request rejected"
                )

            try:
                result = await func(*args, **kwargs)
                await circuit_breaker.record_success()
                return result
            except Exception as e:
                await circuit_breaker.record_failure()
                raise
        return wrapper
    return decorator
