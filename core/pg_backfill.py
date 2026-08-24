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
) -> Dict[str, Any]:
    """Ship every order Postgres is missing. Idempotent; safe to re-run.

    `max_chunks` bounds one invocation — useful for a first careful run, and
    for a caller that would rather come back than hold anything for minutes.
    What it does not do is remember where it stopped; the next call recomputes
    the difference.

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
    for number, ids in enumerate(chunks, start=1):
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
