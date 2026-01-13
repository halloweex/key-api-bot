"""
Async-first in-memory cache with TTL support.

Provides a thread-safe caching abstraction for both bot and web services.
"""
import asyncio
import logging
import time
from typing import Any, Callable, Dict, Optional, TypeVar, Awaitable

logger = logging.getLogger(__name__)

T = TypeVar("T")


class AsyncCache:
    """
    Async-safe in-memory cache with TTL.

    Usage:
        cache = AsyncCache(ttl=300)  # 5 minute TTL

        # Basic get/set
        await cache.set("key", value)
        value = await cache.get("key")

        # Get or compute
        value = await cache.get_or_set("key", compute_func)

        # With decorator
        @cache.cached("prefix")
        async def fetch_data(id: int):
            return await api.get(id)
    """

    def __init__(self, ttl: int = 300, name: str = "default"):
        """
        Initialize cache.

        Args:
            ttl: Time-to-live in seconds (default: 300 = 5 minutes)
            name: Cache name for logging
        """
        self.ttl = ttl
        self.name = name
        self._store: Dict[str, tuple[Any, float]] = {}
        self._lock = asyncio.Lock()
        self._stats = {"hits": 0, "misses": 0}

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache if not expired.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        async with self._lock:
            if key in self._store:
                value, timestamp = self._store[key]
                if time.time() - timestamp < self.ttl:
                    self._stats["hits"] += 1
                    return value
                # Expired - remove it
                del self._store[key]

            self._stats["misses"] += 1
            return None

    async def set(self, key: str, value: Any) -> None:
        """
        Store value in cache.

        Args:
            key: Cache key
            value: Value to store
        """
        async with self._lock:
            self._store[key] = (value, time.time())

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[T]],
    ) -> T:
        """
        Get from cache or compute and store.

        Args:
            key: Cache key
            factory: Async function to compute value if not cached

        Returns:
            Cached or computed value
        """
        # Check cache first
        value = await self.get(key)
        if value is not None:
            return value

        # Compute and store
        value = await factory()
        await self.set(key, value)
        return value

    async def delete(self, key: str) -> bool:
        """
        Delete key from cache.

        Args:
            key: Cache key

        Returns:
            True if key was found and deleted
        """
        async with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False

    async def clear(self) -> int:
        """
        Clear all cached values.

        Returns:
            Number of items cleared
        """
        async with self._lock:
            count = len(self._store)
            self._store.clear()
            logger.info(f"Cache '{self.name}' cleared: {count} items")
            return count

    async def clear_expired(self) -> int:
        """
        Remove expired entries from cache.

        Returns:
            Number of items removed
        """
        now = time.time()
        removed = 0

        async with self._lock:
            expired_keys = [
                key for key, (_, timestamp) in self._store.items()
                if now - timestamp >= self.ttl
            ]
            for key in expired_keys:
                del self._store[key]
                removed += 1

        if removed:
            logger.debug(f"Cache '{self.name}': cleared {removed} expired items")

        return removed

    def cached(self, prefix: str = ""):
        """
        Decorator for caching async function results.

        Args:
            prefix: Key prefix for namespacing

        Example:
            @cache.cached("orders")
            async def get_orders(start: str, end: str):
                return await api.fetch_orders(start, end)
        """
        def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
            async def wrapper(*args, **kwargs) -> T:
                # Build cache key from function name and arguments
                key_parts = [prefix, func.__name__]
                key_parts.extend(str(arg) for arg in args)
                key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
                cache_key = ":".join(filter(None, key_parts))

                return await self.get_or_set(
                    cache_key,
                    lambda: func(*args, **kwargs)
                )
            return wrapper
        return decorator

    @property
    def size(self) -> int:
        """Number of items in cache."""
        return len(self._store)

    @property
    def stats(self) -> Dict[str, Any]:
        """Cache statistics."""
        total = self._stats["hits"] + self._stats["misses"]
        hit_rate = (self._stats["hits"] / total * 100) if total > 0 else 0
        return {
            "name": self.name,
            "size": len(self._store),
            "ttl": self.ttl,
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "hit_rate": f"{hit_rate:.1f}%",
        }


# ─── Global Cache Instances ──────────────────────────────────────────────────

# Dashboard data cache (5 min TTL)
dashboard_cache = AsyncCache(ttl=300, name="dashboard")

# Category/brand metadata cache (1 hour TTL)
metadata_cache = AsyncCache(ttl=3600, name="metadata")


# ─── Helper Functions ────────────────────────────────────────────────────────

async def get_cache_stats() -> Dict[str, Any]:
    """Get stats for all cache instances."""
    return {
        "dashboard": dashboard_cache.stats,
        "metadata": metadata_cache.stats,
    }


async def clear_all_caches() -> Dict[str, int]:
    """Clear all cache instances."""
    return {
        "dashboard": await dashboard_cache.clear(),
        "metadata": await metadata_cache.clear(),
    }
