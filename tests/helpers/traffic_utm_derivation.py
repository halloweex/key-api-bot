"""The Postgres derivation's job over the test Postgres, its UTM parse last.

One website order with a UTM comment in Bronze, and a scheduler whose
process has not derived yet, so its next tick rebuilds Silver and Gold
whatever is owed — the first tick after a deploy. Bronze, Silver, Gold and
`silver.order_utm` are the test's while it runs, emptied before and after as
`tests/integration/pg_tab_seed.py` empties its tables; everything else the
run writes is put back as it was: the derivation signal, the journal and the
verdict log above the ids the test found, the watermark rows in
`meta.mirror_state`, and the state the job keeps in the process.

The shape of `tests/integration/test_pg_own_derivation.py`'s `own` and
`utm_postgres` fixtures, which the derivation's own tests use.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any, Dict
from unittest.mock import AsyncMock, patch

ORDER_ID = 974_001
ORDER_DAY = date(2026, 11, 2)
TOTAL = 120
COMMENT = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: spring"

# Derived first, so nothing is deleted from under a row that came from it.
CLEARED = ("gold.daily_revenue", "silver.order_utm", "silver.orders",
           "bronze.order_products", "bronze.orders")
DERIVED_STAMPS = ("silver.orders", "gold.daily_revenue", "silver.order_utm")
STAMPS = DERIVED_STAMPS + ("meta.derivation_signal", "meta.derivation_runs",
                           "capture.app.order_channel_verdicts")


class Derivation:
    """The pool and the scheduler of one test, and what the run left."""

    def __init__(self, pool, scheduler, runs_before: int):
        self.pool = pool
        self.scheduler = scheduler
        self._runs_before = runs_before

    async def state(self) -> Dict[str, Any]:
        async with self.pool.acquire() as conn:
            return {
                # (error, validation_passed) of every run this test journaled
                "journal": [tuple(r) for r in await conn.fetch(
                    "SELECT error, validation_passed FROM meta.derivation_runs"
                    " WHERE id > $1 ORDER BY id", self._runs_before)],
                "owed": await conn.fetchval(
                    "SELECT requested > built FROM meta.derivation_signal"
                    " WHERE layer = 'warehouse'"),
                "silver": {r["id"]: r["grand_total"] for r in await conn.fetch(
                    "SELECT id, grand_total FROM silver.orders")},
                "gold": {(r["date"], r["sales_type"]): r["revenue"] for r in await conn.fetch(
                    "SELECT date, sales_type, revenue FROM gold.daily_revenue"
                    " WHERE source_id IS NULL")},
                "verdicts": await conn.fetchval("SELECT count(*) FROM silver.order_utm"),
                "failures": {r["table_name"]: r["failures_since_ok"] for r in await conn.fetch(
                    "SELECT table_name, failures_since_ok FROM meta.mirror_state"
                    " WHERE table_name = ANY($1::text[])", list(DERIVED_STAMPS))},
            }


async def _clear(conn) -> None:
    for table in CLEARED:
        await conn.execute(f"DELETE FROM {table}")


async def _put_back(conn, table: str, rows) -> None:
    for row in rows:
        columns = list(row.keys())
        await conn.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES "
            f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
            *row.values())


@asynccontextmanager
async def derivation_over_postgres(dsn: str, monkeypatch):
    """Yields a `Derivation`: the order landed, nothing derived yet, the
    derived tables' watermarks absent, `core.pg.get_pool` answering with the
    test's pool. Alerts are kept from the alert journal."""
    import asyncpg

    from core import pg_derivation, pg_silver
    from core.scheduler import BackgroundScheduler

    monkeypatch.setenv("KS_PG_DSN", dsn)
    monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
    monkeypatch.setattr(BackgroundScheduler, "_pg_derive_ran", False)
    monkeypatch.setattr(BackgroundScheduler, "_pg_derive_started", None)
    monkeypatch.setattr(pg_derivation, "_signal_failures", 0)
    monkeypatch.setattr(pg_silver, "FIRST_REBUILT_AT", pg_silver.FIRST_REBUILT_AT)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    try:
        async with pool.acquire() as conn:
            signal = await conn.fetchrow(
                "SELECT requested, built, requested_at, built_at"
                " FROM meta.derivation_signal WHERE layer = 'warehouse'")
            runs_before = await conn.fetchval(
                "SELECT COALESCE(max(id), 0) FROM meta.derivation_runs")
            log_before = await conn.fetchval(
                "SELECT COALESCE(max(id), 0) FROM app.order_channel_verdicts")
            stamps = await conn.fetch(
                "SELECT * FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                list(STAMPS))
        try:
            async with pool.acquire() as conn:
                await _clear(conn)
                await conn.execute(
                    "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                    list(STAMPS))
                at = datetime(ORDER_DAY.year, ORDER_DAY.month, ORDER_DAY.day, 9,
                              tzinfo=timezone.utc)
                await conn.execute(
                    "INSERT INTO bronze.orders (id, source_id, status_id, grand_total,"
                    " ordered_at, created_at, updated_at, manager_comment)"
                    " VALUES ($1, 4, 1, $2, $3, $3, $3, $4)",
                    ORDER_ID, TOTAL, at, COMMENT)
            with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
                 patch("core.pg_silver.PG_LAYER_LOCK", asyncio.Lock()), \
                 patch("core.alerting.raise_alert", AsyncMock(return_value=1)), \
                 patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
                yield Derivation(pool, BackgroundScheduler(), runs_before)
        finally:
            async with pool.acquire() as conn:
                await _clear(conn)
                await conn.execute(
                    "DELETE FROM meta.derivation_runs WHERE id > $1", runs_before)
                await conn.execute(
                    "DELETE FROM app.order_channel_verdicts WHERE id > $1", log_before)
                if signal is not None:
                    await conn.execute(
                        "UPDATE meta.derivation_signal SET requested = $1, built = $2,"
                        " requested_at = $3, built_at = $4 WHERE layer = 'warehouse'",
                        *signal.values())
                await conn.execute(
                    "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                    list(STAMPS))
                await _put_back(conn, "meta.mirror_state", stamps)
    finally:
        await pool.close()
