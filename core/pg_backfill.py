"""Carrying history into the mirror, which the mirror itself cannot do.

The catalogue needed no backfill: every hourly sync ships the whole 1,003-row
catalogue, so Postgres was complete one hour after the mirror shipped. Orders
are the opposite. `DuckDBStore.upsert_orders` mirrors exactly what it wrote —
`updated_ids`, the delta — because shipping the whole page every minute would
re-send thousands of rows nothing had touched. That is the right behaviour for
a sync and it means Postgres starts at whatever the last few minutes happened
to move, with 46,446 orders and 147,508 line items of history behind it.

WHY IT IS A SET DIFFERENCE AND NOT A CURSOR

No progress table, no resume token, no high-water mark. The backfill asks both
stores which order ids they hold, ships the difference, and stops. Interrupt it
at any point and the next run picks up exactly what is left, because "what is
left" is recomputed rather than remembered — and a cursor that is wrong is
worse than no cursor, since it claims coverage it does not have. Reading 46,446
integers out of each side costs milliseconds.

WHAT IT DOES NOT DO

It does not repair an order that exists on both sides and differs. An order
whose line items changed in DuckDB while the mirror was off is present by id
and will not be re-shipped here. That is deliberate: this closes the gap
between "the mirror started" and "history is present", and repairing
disagreements is the reconciliation's business, downstream of a check that can
say which rows disagree. Running this after such a divergence will report
nothing missing and be right.

WHY IT CHUNKS, AND WHAT THE CHUNK BOUNDARY MEANS

The DuckDB connection is a lock every request shares. A single read of 46,446
orders and their line items would hold it for as long as that takes, so the
work is done in chunks with the lock taken and released per chunk. Between two
chunks Postgres holds a *prefix* of history — complete orders with complete
line items, just not all of them. That intermediate state is why revision 0003
declares no foreign key from `order_products` to `orders`, and it is also why
each chunk's header and line items go in one transaction: the boundary may fall
between orders, never inside one.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

# Orders per chunk. 2,000 orders carry ~6,400 line items, which is one
# comfortable transaction and about a second of the DuckDB lock.
DEFAULT_CHUNK_SIZE = 2000

_ORDER_SELECT = """
    SELECT id, source_id, status_id, status_group_id, grand_total,
           ordered_at, created_at, updated_at, buyer_id, manager_id,
           manager_comment, promocode
    FROM orders
    WHERE id IN ({placeholders})
    ORDER BY id
"""

_ORDER_PRODUCT_SELECT = """
    SELECT id, order_id, product_id, name, quantity, price_sold
    FROM order_products
    WHERE order_id IN ({placeholders})
    ORDER BY id
"""


async def _postgres_order_ids(pool) -> set:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id FROM bronze.orders")
    return {int(r[0]) for r in rows}


def _duckdb_order_ids(conn) -> set:
    return {int(r[0]) for r in conn.execute("SELECT id FROM orders").fetchall()}


def _read_chunk(conn, ids: Sequence[int]) -> tuple:
    """One chunk's headers and line items, in the stores' shared column order."""
    placeholders = ", ".join("?" for _ in ids)
    orders = conn.execute(
        _ORDER_SELECT.format(placeholders=placeholders), list(ids),
    ).fetchall()
    products = conn.execute(
        _ORDER_PRODUCT_SELECT.format(placeholders=placeholders), list(ids),
    ).fetchall()
    return orders, products


async def _mark_backfilled(pool) -> None:
    """Stamp `backfilled_at` for both order tables.

    Both, not one: the backfill carries a header and its line items in the same
    transaction, so history is present for both tables or for neither, and a
    check gated on only one of them would open the other's comparison too
    early.

    `ON CONFLICT` rather than `UPDATE`: a table whose mirror has never
    succeeded has no watermark row yet, and a backfill into an empty Postgres
    is exactly that case.
    """
    from core.pg_landing import ORDERS_TABLE, ORDER_PRODUCTS_TABLE

    async with pool.acquire() as conn:
        for table in (ORDERS_TABLE, ORDER_PRODUCTS_TABLE):
            await conn.execute(
                """
                INSERT INTO meta.mirror_state (table_name, backfilled_at)
                VALUES ($1, now())
                ON CONFLICT (table_name) DO UPDATE SET backfilled_at = now()
                """,
                table,
            )


async def backfill_orders(
    store,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_chunks: Optional[int] = None,
    lock: "asyncio.Lock | None" = None,
) -> Dict[str, Any]:
    """Ship every order Postgres is missing. Idempotent; safe to re-run.

    `max_chunks` bounds one invocation — useful for a first careful run, and
    for a caller that would rather come back than hold anything for minutes.
    What it does not do is remember where it stopped; the next call recomputes
    the difference.

    `lock` is the scheduler's heavy-job lock, held for each chunk's read and
    write together and never for the ids-diff. The sync jobs hold it across
    their DuckDB write *and* their mirror call, so a chunk taken under it
    cannot straddle one: without it, a chunk read before a sync wrote an
    order and mirrored after it left Postgres holding the older copy and
    the version archive recording a transition that never happened.

    Raises on failure rather than swallowing, unlike the mirror in the sync
    path: nothing depends on this call succeeding, so there is no working
    system to protect by staying quiet, and a backfill that reports success
    while shipping nothing is the worst of the available outcomes.
    """
    from core.pg import get_pool, require_revision
    from core.pg_landing import enabled, write_orders

    if not enabled():
        raise RuntimeError(
            "The mirror is switched off (KS_MIRROR_LANDING); refusing to "
            "backfill into a store the sync will not keep up to date."
        )

    pool = await get_pool()
    await require_revision()

    existing = await _postgres_order_ids(pool)
    async with store.connection() as conn:
        available = _duckdb_order_ids(conn)

    missing = sorted(available - existing)
    chunks = [
        missing[i:i + chunk_size] for i in range(0, len(missing), chunk_size)
    ]
    planned = len(chunks)
    if max_chunks is not None:
        chunks = chunks[:max_chunks]

    cap = f", capped at {max_chunks}" if max_chunks is not None else ""
    logger.info(
        "Backfill: DuckDB %d orders, Postgres %d, missing %d, %d chunk(s) of %d%s",
        len(available), len(existing), len(missing), planned, chunk_size, cap,
    )

    shipped_orders = 0
    shipped_products = 0
    held = lock if lock is not None else contextlib.nullcontext()
    for number, ids in enumerate(chunks, start=1):
        async with held:
            async with store.connection() as conn:
                orders, products = _read_chunk(conn, ids)
            await write_orders(orders, products, replace_products=True)
        shipped_orders += len(orders)
        shipped_products += len(products)
        logger.info(
            "Backfill chunk %d/%d: %d order(s), %d line item(s) — %d/%d done",
            number, len(chunks), len(orders), len(products),
            shipped_orders, len(missing),
        )

    remaining = len(missing) - shipped_orders
    if remaining == 0:
        # The gate Reconciliation A reads. Written only on a run that left
        # nothing behind, because a partial backfill that claimed completion
        # would turn every un-carried order into a CRITICAL finding — which is
        # exactly the noise the gate exists to prevent.
        await _mark_backfilled(pool)

    result = {
        "duckdb_orders": len(available),
        "postgres_orders_before": len(existing),
        "missing": len(missing),
        "chunks_planned": planned,
        "chunks_run": len(chunks),
        "orders_shipped": shipped_orders,
        "line_items_shipped": shipped_products,
        "remaining": remaining,
        "complete": remaining == 0,
    }
    logger.info("Backfill finished: %s", result)
    return result


async def hourly_orders_ids_diff(store, *, lock: "asyncio.Lock | None" = None) -> Dict[str, Any]:
    """Run the orders ids-diff every hour, not only when a human remembers.

    The mirror ships `updated_ids` after the DuckDB commit, on a separate
    store, and never raises. A write lost between the two commits — Postgres
    unreachable, a deadlock, the container stopped — was therefore never
    re-shipped on any schedule: the next tick skipped the order as unchanged,
    and the only path that re-issued the write was this backfill, run by hand.
    The 05:15 refresh happened to re-ship *headers* of orders created in the
    last 30 days; line items and the version archive it never repaired.

    Same shape as `core.pg_buyers.hourly_ids_diff`, for the same reason: the
    diff costs two ~46k-id scans and ships nothing when nothing is missing, so
    it simply runs every tick. A heal after the first complete pass is logged
    WARNING — the mirror lost something since the last hour. Never raises: the
    job's contract. Runs under the heavy-job lock per chunk (see
    `backfill_orders`), which is also why it rides a job rather than a route.
    """
    from core import pg_landing

    if not pg_landing.enabled():
        return {"skipped": "KS_PG_DSN is not set"}
    try:
        from core.pg import get_pool
        from core.pg_landing import ORDERS_TABLE

        pool = await get_pool()
        async with pool.acquire() as conn:
            had_history = await conn.fetchval(
                "SELECT backfilled_at IS NOT NULL FROM meta.mirror_state "
                "WHERE table_name = $1",
                ORDERS_TABLE,
            )
        result = await backfill_orders(store, lock=lock)
        if result.get("orders_shipped"):
            if had_history:
                logger.warning("pg_backfill: hourly ids-diff healed missing orders: %s", result)
            else:
                logger.info("pg_backfill: initial orders backfill: %s", result)
        return result
    except Exception as e:  # noqa: BLE001 — the hourly job must survive it
        detail = f"{type(e).__name__}: {e}"
        logger.error("pg_backfill: hourly orders ids-diff failed: %s", detail)
        return {"error": detail}
