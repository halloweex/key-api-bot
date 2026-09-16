"""Sync watermarks that belong to Postgres — one store for every write chain.

A write chain that moves to Postgres takes its `last_sync_*` keys with it, and
the three things that touch a watermark must then agree about where it lives:
the setter, the getter the sync gate reads, and the freshness check that pages
on a stalled sync. They disagreed once — written to Postgres, read from DuckDB,
wiped hourly — and revision 0032 records how.

Chain-agnostic on purpose. `core.write_chains.stood_down_sync_keys()` decides
WHICH keys are owned here; this module only stores them. Chains 3, 4 and 5 each
carry `last_sync_*` keys of their own, and a store per chain is how the getter
and the setter came apart the first time.
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, Optional


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def set_value(key: str, value: str) -> None:
    pool = await _pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
            "VALUES ($1, $2, now()) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, "
            "updated_at = EXCLUDED.updated_at",
            key, value,
        )


async def get_value(key: str) -> Optional[datetime]:
    """The stamp as `get_last_sync_time` returns it: a datetime, or None."""
    pool = await _pool()
    async with pool.acquire() as conn:
        raw = await conn.fetchval(
            "SELECT value FROM meta.chain_watermarks WHERE key = $1", key)
    return datetime.fromisoformat(raw) if raw else None


async def read_values(keys: Iterable[str]) -> Dict[str, str]:
    """`{key: raw value}` for the given keys that exist — the freshness check's
    input, in the shape `sync_metadata` rows have. Missing keys are absent, so
    "never synced since the switch" still reads as never synced."""
    keys = list(keys)
    if not keys:
        return {}
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT key, value FROM meta.chain_watermarks WHERE key = ANY($1)", keys)
    return {r["key"]: r["value"] for r in rows}
