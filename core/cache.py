"""
Redis caching layer for dashboard API.

Provides:
- Async Redis client with connection pooling
- Automatic JSON serialization/deserialization
- TTL-based expiration
- Pattern-based cache invalidation
- Graceful fallback when Redis is unavailable
- Integration with event system for invalidation

Usage:
    from core.cache import cache

    # Get/set values
    await cache.set("key", {"data": "value"}, ttl=300)
    data = await cache.get("key")

    # Use as decorator
    @cache.cached(ttl=60, key_prefix="summary")
    async def get_summary(period: str):
        ...

    # Invalidate by pattern
    await cache.invalidate_pattern("summary:*")
"""
import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

from core.observability import get_logger, Timer
from core.events import events, SyncEvent, emit_cache_invalidated

logger = get_logger(__name__)

T = TypeVar("T")

# Configuration from environment
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "true").lower() == "true"
DEFAULT_TTL = int(os.getenv("CACHE_DEFAULT_TTL", "300"))  # 5 minutes


@dataclass
class CacheStats:
    """Cache statistics for monitoring."""

    hits: int = 0
    misses: int = 0
    errors: int = 0
    sets: int = 0
    invalidations: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0

    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "errors": self.errors,
            "sets": self.sets,
            "invalidations": self.invalidations,
            "hit_rate_percent": round(self.hit_rate, 2),
        }

    def reset(self) -> None:
        """Reset all counters."""
        self.hits = 0
        self.misses = 0
        self.errors = 0
        self.sets = 0
        self.invalidations = 0


class RedisCache:
    """
    Async Redis cache with graceful degradation.

    Features:
    - Async operations with connection pooling
    - JSON serialization for complex objects
    - Pattern-based invalidation
    - Statistics tracking
    - Graceful fallback (no-op) when Redis unavailable
    """

    def __init__(self, url: str = REDIS_URL, enabled: bool = CACHE_ENABLED):
        self.url = url
        self.enabled = enabled
        self._client = None
        self._connected = False
        self._stats = CacheStats()
        self._lock = asyncio.Lock()

    async def connect(self) -> bool:
        """
        Connect to Redis server.

        Returns:
            True if connected successfully, False otherwise
        """
        if not self.enabled:
            logger.info("Redis cache disabled by configuration")
            return False

        try:
            import redis.asyncio as redis

            async with self._lock:
                if self._client is None:
                    self._client = redis.from_url(
                        self.url,
                        encoding="utf-8",
                        decode_responses=True,
                        socket_timeout=5.0,
                        socket_connect_timeout=5.0,
                    )
                    # Test connection
                    await self._client.ping()
                    self._connected = True
                    logger.info(f"Redis connected: {self.url}")
                    return True
        except ImportError:
            logger.warning("Redis package not installed, cache disabled")
            self.enabled = False
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}")
            self._connected = False

        return False

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.close()
            self._client = None
            self._connected = False
            logger.info("Redis disconnected")

    @property
    def is_connected(self) -> bool:
        """Check if Redis is connected."""
        return self._connected and self._client is not None

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/error
        """
        if not self.is_connected:
            self._stats.misses += 1
            return None

        try:
            with Timer("cache_get"):
                value = await self._client.get(key)

            if value is not None:
                self._stats.hits += 1
                return json.loads(value)
            else:
                self._stats.misses += 1
                return None

        except Exception as e:
            self._stats.errors += 1
            logger.debug(f"Cache get error for {key}: {e}")
            return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set value in cache.

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time-to-live in seconds (default: DEFAULT_TTL)

        Returns:
            True if set successfully
        """
        if not self.is_connected:
            return False

        ttl = ttl or DEFAULT_TTL

        try:
            with Timer("cache_set"):
                serialized = json.dumps(value, default=str)
                await self._client.setex(key, ttl, serialized)

            self._stats.sets += 1
            return True

        except Exception as e:
            self._stats.errors += 1
            logger.debug(f"Cache set error for {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """
        Delete a key from cache.

        Args:
            key: Cache key to delete

        Returns:
            True if deleted
        """
        if not self.is_connected:
            return False

        try:
            await self._client.delete(key)
            self._stats.invalidations += 1
            return True
        except Exception as e:
            self._stats.errors += 1
            logger.debug(f"Cache delete error for {key}: {e}")
            return False

    async def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate all keys matching pattern.

        Args:
            pattern: Redis glob pattern (e.g., "summary:*")

        Returns:
            Number of keys deleted
        """
        if not self.is_connected:
            return 0

        try:
            # Use SCAN to find keys (safer than KEYS for production)
            deleted = 0
            cursor = 0

            while True:
                cursor, keys = await self._client.scan(
                    cursor=cursor, match=pattern, count=100
                )

                if keys:
                    await self._client.delete(*keys)
                    deleted += len(keys)

                if cursor == 0:
                    break

            if deleted > 0:
                self._stats.invalidations += deleted
                logger.debug(f"Invalidated {deleted} keys matching '{pattern}'")

                # Emit cache invalidated event
                await emit_cache_invalidated(
                    keys=[pattern],
                    reason="pattern_invalidation",
                    count=deleted,
                )

            return deleted

        except Exception as e:
            self._stats.errors += 1
            logger.debug(f"Cache invalidate pattern error for {pattern}: {e}")
            return 0

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: Optional[int] = None,
    ) -> Any:
        """
        Get from cache, or compute and set if missing.

        Args:
            key: Cache key
            factory: Async function to compute value if not cached
            ttl: Time-to-live in seconds

        Returns:
            Cached or computed value
        """
        # Try to get from cache
        value = await self.get(key)
        if value is not None:
            return value

        # Compute value
        if asyncio.iscoroutinefunction(factory):
            value = await factory()
        else:
            value = factory()

        # Cache it
        await self.set(key, value, ttl)

        return value

    def cached(
        self,
        ttl: Optional[int] = None,
        key_prefix: str = "",
        key_builder: Optional[Callable[..., str]] = None,
    ):
        """
        Decorator for caching async function results.

        Args:
            ttl: Time-to-live in seconds
            key_prefix: Prefix for cache key
            key_builder: Custom function to build cache key

        Usage:
            @cache.cached(ttl=60, key_prefix="summary")
            async def get_summary(period: str, source_id: int = None):
                ...
        """

        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @wraps(func)
            async def wrapper(*args, **kwargs) -> T:
                # Build cache key
                if key_builder:
                    cache_key = key_builder(*args, **kwargs)
                else:
                    cache_key = self._build_key(key_prefix or func.__name__, args, kwargs)

                # Try cache first
                cached_value = await self.get(cache_key)
                if cached_value is not None:
                    return cached_value

                # Call function
                result = await func(*args, **kwargs)

                # Cache result
                await self.set(cache_key, result, ttl)

                return result

            # Store reference to original function
            wrapper.__wrapped__ = func
            return wrapper

        return decorator

    def _build_key(self, prefix: str, args: tuple, kwargs: dict) -> str:
        """Build cache key from function arguments."""
        # Create deterministic key from arguments
        key_parts = [prefix]

        # Add positional args (skip 'self' if present)
        for arg in args:
            if hasattr(arg, "__dict__"):
                # Skip complex objects like Request
                continue
            key_parts.append(str(arg))

        # Add keyword args (sorted for consistency)
        for k, v in sorted(kwargs.items()):
            if v is not None:
                key_parts.append(f"{k}={v}")

        key_str = ":".join(key_parts)

        # Hash if too long
        if len(key_str) > 200:
            hash_suffix = hashlib.md5(key_str.encode()).hexdigest()[:12]
            key_str = f"{prefix}:{hash_suffix}"

        return key_str

    def get_stats(self) -> dict:
        """Get cache statistics."""
        return {
            "enabled": self.enabled,
            "connected": self.is_connected,
            "url": self.url if self.is_connected else None,
            **self._stats.to_dict(),
        }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        self._stats.reset()


# Global cache instance
cache = RedisCache()


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT HANDLERS FOR CACHE INVALIDATION
# ═══════════════════════════════════════════════════════════════════════════════


def register_cache_invalidation_handlers():
    """Register event handlers for automatic cache invalidation."""

    @events.on(SyncEvent.ORDERS_SYNCED)
    async def invalidate_on_orders_synced(data: dict):
        """Invalidate order-related caches when orders are synced."""
        count = data.get("count", 0)
        if count > 0:
            await cache.invalidate_pattern("summary:*")
            await cache.invalidate_pattern("revenue:*")
            await cache.invalidate_pattern("sales:*")
            logger.debug(f"Invalidated caches after {count} orders synced")

    @events.on(SyncEvent.PRODUCTS_SYNCED)
    async def invalidate_on_products_synced(data: dict):
        """Invalidate product-related caches when products are synced."""
        count = data.get("count", 0)
        if count > 0:
            await cache.invalidate_pattern("products:*")
            await cache.invalidate_pattern("brands:*")
            await cache.invalidate_pattern("categories:*")
            logger.debug(f"Invalidated caches after {count} products synced")

    @events.on(SyncEvent.INVENTORY_UPDATED)
    async def invalidate_on_inventory_updated(data: dict):
        """Invalidate inventory caches when stock is updated."""
        await cache.invalidate_pattern("stocks:*")
        await cache.invalidate_pattern("inventory:*")
        logger.debug("Invalidated inventory caches")

    logger.info("Cache invalidation handlers registered")
