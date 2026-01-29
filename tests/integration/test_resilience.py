"""
Integration tests for core/resilience.py

Tests circuit breaker, retry with backoff, and rate limiter patterns.
"""
import asyncio
import pytest
import time

from core.resilience import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CircuitOpenError,
    RetryConfig,
    retry_with_backoff,
    with_retry,
    RateLimiter,
)


class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""

    @pytest.mark.asyncio
    async def test_initial_state_closed(self):
        """Circuit breaker starts in closed state."""
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    @pytest.mark.asyncio
    async def test_records_success(self):
        """Success resets failure count."""
        cb = CircuitBreaker()
        await cb.record_failure()
        await cb.record_failure()
        assert cb.failure_count == 2

        await cb.record_success()
        assert cb.failure_count == 0
        assert cb.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_opens_after_threshold_failures(self):
        """Circuit opens after reaching failure threshold."""
        config = CircuitBreakerConfig(failure_threshold=3, recovery_timeout=1.0)
        cb = CircuitBreaker(config=config)

        for _ in range(3):
            await cb.record_failure()

        assert cb.state == CircuitState.OPEN
        assert not await cb.can_execute()

    @pytest.mark.asyncio
    async def test_can_execute_when_closed(self):
        """Allows requests when circuit is closed."""
        cb = CircuitBreaker()
        assert await cb.can_execute()

    @pytest.mark.asyncio
    async def test_block_request_when_open(self):
        """Blocks requests when circuit is open."""
        config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=10.0)
        cb = CircuitBreaker(config=config)

        await cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert not await cb.can_execute()

    @pytest.mark.asyncio
    async def test_half_open_after_recovery_timeout(self):
        """Circuit enters half-open state after recovery timeout."""
        config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.1)
        cb = CircuitBreaker(config=config)

        await cb.record_failure()
        assert cb.state == CircuitState.OPEN

        # Wait for recovery timeout
        await asyncio.sleep(0.15)

        # Should allow one request (half-open)
        assert await cb.can_execute()
        assert cb.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_closes_after_success_in_half_open(self):
        """Circuit closes after successful request in half-open state."""
        config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.1)
        cb = CircuitBreaker(config=config)

        await cb.record_failure()
        await asyncio.sleep(0.15)

        await cb.can_execute()  # Transition to half-open
        await cb.record_success()

        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    @pytest.mark.asyncio
    async def test_reopens_after_failure_in_half_open(self):
        """Circuit reopens after failure in half-open state."""
        config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.1)
        cb = CircuitBreaker(config=config)

        await cb.record_failure()
        await asyncio.sleep(0.15)

        await cb.can_execute()  # Transition to half-open
        await cb.record_failure()

        assert cb.state == CircuitState.OPEN

    def test_is_open_property(self):
        """is_open property returns correct state."""
        cb = CircuitBreaker()
        assert not cb.is_open


class TestRetryWithBackoff:
    """Tests for retry_with_backoff function."""

    @pytest.mark.asyncio
    async def test_succeeds_first_try(self):
        """Returns result if first attempt succeeds."""
        async def my_func():
            return "success"

        result = await retry_with_backoff(
            my_func,
            config=RetryConfig(max_attempts=3, base_delay=0.01)
        )
        assert result == "success"

    @pytest.mark.asyncio
    async def test_retries_on_failure(self):
        """Retries on exception up to max_attempts."""
        call_count = 0

        async def my_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Network error")
            return "success"

        result = await retry_with_backoff(
            my_func,
            config=RetryConfig(max_attempts=3, base_delay=0.01)
        )
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_raises_after_max_attempts(self):
        """Raises exception after all attempts exhausted."""
        async def my_func():
            raise ConnectionError("Always fails")

        with pytest.raises(ConnectionError):
            await retry_with_backoff(
                my_func,
                config=RetryConfig(max_attempts=2, base_delay=0.01)
            )

    @pytest.mark.asyncio
    async def test_respects_retryable_exceptions(self):
        """Only retries on specified exception types."""
        async def my_func():
            raise ValueError("Not retryable")

        with pytest.raises(ValueError):
            await retry_with_backoff(
                my_func,
                config=RetryConfig(max_attempts=3, base_delay=0.01),
                retryable_exceptions=(ConnectionError,)
            )


class TestWithRetryDecorator:
    """Tests for with_retry decorator."""

    @pytest.mark.asyncio
    async def test_decorator_succeeds(self):
        """Decorated function returns result on success."""

        @with_retry(RetryConfig(max_attempts=3, base_delay=0.01))
        async def my_func():
            return "decorated_success"

        result = await my_func()
        assert result == "decorated_success"

    @pytest.mark.asyncio
    async def test_decorator_retries(self):
        """Decorated function retries on failure."""
        call_count = 0

        @with_retry(RetryConfig(max_attempts=3, base_delay=0.01))
        async def my_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Fail once")
            return "success_after_retry"

        result = await my_func()
        assert result == "success_after_retry"
        assert call_count == 2


class TestRateLimiter:
    """Tests for RateLimiter class."""

    @pytest.mark.asyncio
    async def test_allows_within_rate(self):
        """Allows requests within rate limit."""
        limiter = RateLimiter(rate=10.0, burst=10)

        for _ in range(5):
            assert await limiter.acquire(timeout=0.1)

    @pytest.mark.asyncio
    async def test_blocks_when_exhausted(self):
        """Blocks when tokens exhausted."""
        limiter = RateLimiter(rate=1.0, burst=2)

        # Use up all tokens
        assert await limiter.acquire(timeout=0.01)
        assert await limiter.acquire(timeout=0.01)

        # Should fail immediately (no tokens left)
        assert not await limiter.acquire(timeout=0.01)

    @pytest.mark.asyncio
    async def test_refills_over_time(self):
        """Tokens refill over time."""
        limiter = RateLimiter(rate=10.0, burst=2)  # 10 tokens/sec

        # Use all tokens
        await limiter.acquire(timeout=0.01)
        await limiter.acquire(timeout=0.01)

        # Wait for refill (0.15s = 1.5 tokens at 10/sec)
        await asyncio.sleep(0.15)

        # Should have 1 token available
        assert await limiter.acquire(timeout=0.01)
