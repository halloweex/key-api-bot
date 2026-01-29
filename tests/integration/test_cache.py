"""
Integration tests for core/cache.py

Tests Redis caching layer (with fallback behavior when Redis unavailable).
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from core.cache import RedisCache, CacheStats


class TestCacheStats:
    """Tests for CacheStats class."""

    def test_initial_values(self):
        """Stats start at zero."""
        stats = CacheStats()
        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.errors == 0
        assert stats.sets == 0
        assert stats.invalidations == 0

    def test_hit_rate_empty(self):
        """Hit rate is 0 when no requests."""
        stats = CacheStats()
        assert stats.hit_rate == 0.0

    def test_hit_rate_calculation(self):
        """Hit rate calculated correctly."""
        stats = CacheStats(hits=75, misses=25)
        assert stats.hit_rate == 75.0

    def test_to_dict(self):
        """Converts to dictionary correctly."""
        stats = CacheStats(hits=10, misses=5, errors=1, sets=8, invalidations=2)
        d = stats.to_dict()

        assert d["hits"] == 10
        assert d["misses"] == 5
        assert d["errors"] == 1
        assert d["sets"] == 8
        assert d["invalidations"] == 2
        assert d["hit_rate_percent"] == pytest.approx(66.67, rel=0.01)

    def test_reset(self):
        """Reset clears all counters."""
        stats = CacheStats(hits=10, misses=5, errors=1)
        stats.reset()

        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.errors == 0


class TestRedisCacheDisabled:
    """Tests for RedisCache when disabled."""

    def test_disabled_by_config(self):
        """Cache can be disabled via config."""
        cache = RedisCache(enabled=False)
        assert cache.enabled is False
        assert cache.is_connected is False

    @pytest.mark.asyncio
    async def test_connect_when_disabled(self):
        """Connect returns False when disabled."""
        cache = RedisCache(enabled=False)
        result = await cache.connect()
        assert result is False

    @pytest.mark.asyncio
    async def test_get_when_not_connected(self):
        """Get returns None when not connected."""
        cache = RedisCache(enabled=False)
        result = await cache.get("test_key")
        assert result is None
        assert cache._stats.misses == 1

    @pytest.mark.asyncio
    async def test_set_when_not_connected(self):
        """Set returns False when not connected."""
        cache = RedisCache(enabled=False)
        result = await cache.set("test_key", {"data": "value"})
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_when_not_connected(self):
        """Delete returns False when not connected."""
        cache = RedisCache(enabled=False)
        result = await cache.delete("test_key")
        assert result is False

    @pytest.mark.asyncio
    async def test_invalidate_pattern_when_not_connected(self):
        """Invalidate pattern returns 0 when not connected."""
        cache = RedisCache(enabled=False)
        result = await cache.invalidate_pattern("test:*")
        assert result == 0

    def test_get_stats_when_disabled(self):
        """Get stats works when disabled."""
        cache = RedisCache(enabled=False)
        stats = cache.get_stats()

        assert stats["enabled"] is False
        assert stats["connected"] is False
        assert stats["url"] is None


class TestRedisCacheKeyBuilder:
    """Tests for cache key building."""

    def test_build_key_simple(self):
        """Builds simple key correctly."""
        cache = RedisCache()
        key = cache._build_key("prefix", ("arg1", "arg2"), {})
        assert key == "prefix:arg1:arg2"

    def test_build_key_with_kwargs(self):
        """Builds key with kwargs correctly."""
        cache = RedisCache()
        key = cache._build_key("prefix", (), {"a": 1, "b": 2})
        assert key == "prefix:a=1:b=2"

    def test_build_key_skips_none(self):
        """Skips None values in kwargs."""
        cache = RedisCache()
        key = cache._build_key("prefix", (), {"a": 1, "b": None})
        assert key == "prefix:a=1"

    def test_build_key_sorted_kwargs(self):
        """Kwargs are sorted for deterministic keys."""
        cache = RedisCache()
        key1 = cache._build_key("prefix", (), {"z": 1, "a": 2})
        key2 = cache._build_key("prefix", (), {"a": 2, "z": 1})
        assert key1 == key2

    def test_build_key_hash_long_keys(self):
        """Long keys are hashed."""
        cache = RedisCache()
        long_args = tuple(f"arg{i}" for i in range(100))
        key = cache._build_key("prefix", long_args, {})
        assert len(key) <= 200
        assert key.startswith("prefix:")


class TestRedisCacheDecorator:
    """Tests for @cache.cached decorator."""

    @pytest.mark.asyncio
    async def test_decorator_calls_function_when_not_connected(self):
        """Decorator calls function when cache not connected."""
        cache = RedisCache(enabled=False)
        call_count = 0

        @cache.cached(ttl=60, key_prefix="test")
        async def my_func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result = await my_func(5)
        assert result == 10
        assert call_count == 1

        # Call again - should call function again (no cache)
        result = await my_func(5)
        assert result == 10
        assert call_count == 2


class TestRedisCacheWithMock:
    """Tests for RedisCache with mocked Redis client."""

    @pytest.mark.asyncio
    async def test_get_hit(self):
        """Get returns cached value on hit."""
        cache = RedisCache()
        cache._connected = True
        cache._client = AsyncMock()
        cache._client.get.return_value = '{"data": "cached_value"}'

        result = await cache.get("test_key")

        assert result == {"data": "cached_value"}
        assert cache._stats.hits == 1
        cache._client.get.assert_called_once_with("test_key")

    @pytest.mark.asyncio
    async def test_get_miss(self):
        """Get returns None on miss."""
        cache = RedisCache()
        cache._connected = True
        cache._client = AsyncMock()
        cache._client.get.return_value = None

        result = await cache.get("test_key")

        assert result is None
        assert cache._stats.misses == 1

    @pytest.mark.asyncio
    async def test_set_success(self):
        """Set stores value in cache."""
        cache = RedisCache()
        cache._connected = True
        cache._client = AsyncMock()

        result = await cache.set("test_key", {"data": "value"}, ttl=60)

        assert result is True
        assert cache._stats.sets == 1
        cache._client.setex.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_success(self):
        """Delete removes key from cache."""
        cache = RedisCache()
        cache._connected = True
        cache._client = AsyncMock()

        result = await cache.delete("test_key")

        assert result is True
        assert cache._stats.invalidations == 1
        cache._client.delete.assert_called_once_with("test_key")

    @pytest.mark.asyncio
    async def test_get_or_set_cached(self):
        """get_or_set returns cached value if available."""
        cache = RedisCache()
        cache._connected = True
        cache._client = AsyncMock()
        cache._client.get.return_value = '{"cached": true}'

        factory_called = False

        async def factory():
            nonlocal factory_called
            factory_called = True
            return {"fresh": True}

        result = await cache.get_or_set("test_key", factory, ttl=60)

        assert result == {"cached": True}
        assert factory_called is False

    @pytest.mark.asyncio
    async def test_get_or_set_computes(self):
        """get_or_set computes value if not cached."""
        cache = RedisCache()
        cache._connected = True
        cache._client = AsyncMock()
        cache._client.get.return_value = None

        async def factory():
            return {"fresh": True}

        result = await cache.get_or_set("test_key", factory, ttl=60)

        assert result == {"fresh": True}
        cache._client.setex.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Errors are caught and logged."""
        cache = RedisCache()
        cache._connected = True
        cache._client = AsyncMock()
        cache._client.get.side_effect = Exception("Redis error")

        result = await cache.get("test_key")

        assert result is None
        assert cache._stats.errors == 1
