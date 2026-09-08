"""The mirror: the same KeyCRM payload, written to Postgres as well.

Step 05's first write. DuckDB stays the system of record for the entire
parallel period — nothing reads Postgres, no dashboard depends on it, and the
read switch is a later step entirely. What this establishes is that the two
stores can be kept in step at all, on tables whose loss costs one resync.

THE FAILURE POLICY IS THE WHOLE DESIGN

A failed Postgres write must not break the sync. DuckDB is what the business
looks at, KeyCRM is what it is built from, and a mirror that can stop either is
worse than no mirror — charter rule 8, the step leaves the system working.

But it must not be silent. That is the 2026-08-09 shape exactly: an `ALTER`
failed inside `try/except` with a DEBUG log, the code read the column anyway,
and a one-off became a rebuild every two minutes for hours. So a failure here
is caught, counted, logged at ERROR, written to the watermark when the database
is reachable enough to take it, and published on `/api/health`.

WHY A WATERMARK AND NOT JUST A COUNTER

`meta.mirror_state` is what lets the daily reconciliation tell **"not shipped
yet"** from **"shipped wrong"**. Without it every lag reads as a discrepancy,
and the first week of a mirror is nothing but lag. `last_ok_at` and
`last_attempted_at` are both kept: equal means healthy, and the gap between
them is how long this table has been behind.

THE PARSE IS SHARED, THE WRITE IS NOT

Both stores read the payload through `core.landing_rows` — one home for the
rule (charter rule 1). The parse runs twice over a thousand dicts, which costs
microseconds and buys the guarantee that neither store can drift into its own
reading of a brand.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

# One deploy turns the mirror off, which is charter rule 6's rollback. It is ON
# by default on purpose: a safety mechanism nobody exercises is not a safety
# mechanism, and this repository has the scar — a Redis client lived here for
# months without executing in any environment and was then blamed, in an audit,
# for the dashboard's latency.
MIRROR_ENV = "KS_MIRROR_LANDING"


def enabled() -> bool:
    return os.getenv(MIRROR_ENV, "1").strip().lower() not in {"0", "false", "no", ""}


@dataclass
class MirrorOutcome:
    """What one mirroring attempt did. Never raises; this is the report."""
    table: str
    rows: int = 0
    ok: bool = False
    skipped: Optional[str] = None      # why it did not run, if it did not
    error: Optional[str] = None

    @property
    def attempted(self) -> bool:
        return self.skipped is None


# Process-local, for /api/health. The watermark in Postgres is the durable
# record; this is what can still be reported when Postgres is the thing that is
# down.
_failures: Dict[str, int] = {}
_last_error: Dict[str, str] = {}


def health() -> Dict[str, Any]:
    """Mirror state as `/api/health` shows it, readable with Postgres down."""
    return {
        "enabled": enabled(),
        "failures": dict(_failures),
        "last_error": dict(_last_error),
    }


def reset_health() -> None:
    """Test seam. Production has one process and no reason to call this."""
    _failures.clear()
    _last_error.clear()


_UPSERT = """
INSERT INTO {table} ({cols}) VALUES ({vals})
ON CONFLICT (id) DO UPDATE SET {sets}, mirrored_at = now()
"""


_WATERMARK_OK = """
INSERT INTO meta.mirror_state
       (table_name, last_attempted_at, last_ok_at,
        failures_since_ok, last_error, last_rows)
VALUES ($1, now(), now(), 0, NULL, $2)
ON CONFLICT (table_name) DO UPDATE SET
    last_attempted_at = now(),
    last_ok_at        = now(),
    failures_since_ok = 0,
    last_error        = NULL,
    last_rows         = EXCLUDED.last_rows
"""


def _record_ok(table: str, rows: int) -> None:
    _failures.pop(table, None)
    _last_error.pop(table, None)
    logger.info("mirror: %s ok, %d row(s)", table, rows)


def _record_error(table: str, rows: int, detail: str) -> None:
    _failures[table] = _failures.get(table, 0) + 1
    _last_error[table] = detail
    # ERROR, not DEBUG. The whole point.
    logger.error("mirror: %s failed after %d row(s): %s", table, rows, detail)


def _statement(table: str, columns: Sequence[str]) -> str:
    cols = ", ".join(columns)
    vals = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    # `id` is the conflict target, so it is not in the SET list — assigning a
    # column to itself is legal and pointless, and leaving it out keeps the
    # statement saying what it means.
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "id")
    return _UPSERT.format(table=table, cols=cols, vals=vals, sets=sets)


async def _write(table: str, columns: Sequence[str], rows: Sequence[tuple]) -> None:
    """Upsert rows and move the watermark, in one transaction.

    One transaction on purpose: a watermark claiming a success that did not
    happen is worse than no watermark, because the reconciliation would then
    read a real gap as "already shipped".
    """
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            if rows:
                await conn.executemany(_statement(table, columns), rows)
            await conn.execute(_WATERMARK_OK, table, len(rows))


async def _record_failure(table: str, error: str) -> None:
    """Best effort: say in Postgres that the mirror failed.

    Usually pointless — if the write failed because Postgres is unreachable,
    so will this. It is here for the other case, the one that actually
    misleads: a write that failed on *its own* data while the connection was
    fine. Without this the watermark would keep the last success and read as
    healthy.
    """
    try:
        from core.pg import get_pool

        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO meta.mirror_state
                       (table_name, last_attempted_at, failures_since_ok, last_error)
                VALUES ($1, now(), 1, $2)
                ON CONFLICT (table_name) DO UPDATE SET
                    last_attempted_at = now(),
                    failures_since_ok = meta.mirror_state.failures_since_ok + 1,
                    last_error        = EXCLUDED.last_error
                """,
                table, error[:2000],
            )
    except Exception as exc:
        logger.debug("mirror: could not record the failure either: %s", exc)


async def _mirror(table: str, columns: Sequence[str], rows: List[tuple]) -> MirrorOutcome:
    out = MirrorOutcome(table=table, rows=len(rows))

    if not enabled():
        out.skipped = f"{MIRROR_ENV} is off"
        return out

    try:
        await _write(table, columns, rows)
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        out.error = detail
        _record_error(table, len(rows), detail)
        await _record_failure(table, detail)
        return out

    out.ok = True
    _record_ok(table, len(rows))
    return out


async def mirror_categories(payloads: List[Dict[str, Any]]) -> MirrorOutcome:
    from core.landing_rows import CATEGORY_COLUMNS, category_rows

    rows = [tuple(r) for r in category_rows(payloads)]
    return await _mirror("bronze.categories", CATEGORY_COLUMNS, rows)


async def mirror_products(payloads: List[Dict[str, Any]]) -> MirrorOutcome:
    from core.landing_rows import PRODUCT_COLUMNS, product_rows

    rows = [tuple(r) for r in product_rows(payloads)]
    return await _mirror("bronze.products", PRODUCT_COLUMNS, rows)


async def mirror_expenses(orders_with_expenses: List[Dict[str, Any]]) -> MirrorOutcome:
    """The order-level costs carried by a batch of orders.

    **A delta, like orders and unlike the catalogue** — it ships what this sync
    fetched, not the whole 15,020 rows — so, like orders, it needs a backfill
    to carry history across and its comparison must be gated until that has
    run. `core/pg_expense_backfill.py` is that backfill.

    **Upserted, never deleted-and-reinserted.** That is the opposite of line
    items, and the difference is the id: an expense carries KeyCRM's own id,
    which is stable, whereas a line item's is `order_id * 1000 + position` and
    therefore shifts when a basket shrinks. Nothing here leaves a `…002`
    behind.

    An expense KeyCRM has *removed* from an order is therefore kept, exactly as
    DuckDB keeps it — the two stores agree, which is what the comparison
    measures. Whether either should keep it is a question about the sync, and
    the same one `feedback_never_delete_retired_products` answers for products.
    """
    from core.landing_rows import EXPENSE_COLUMNS, expense_rows

    rows = [tuple(r) for r in expense_rows(orders_with_expenses)]
    return await _mirror("bronze.expenses", EXPENSE_COLUMNS, rows)


# ─── Orders ───────────────────────────────────────────────────────────────────
#
# Two tables, one transaction, and three things the catalogue never had to
# think about.
#
# 1. `manager_comment` IS NOT OVERWRITTEN WITH NULL.
#
#    `DuckDBStore.upsert_orders` updates it as
#    `COALESCE(?, manager_comment)` — a payload that omits the field keeps
#    whatever is stored — because that column carries UTM attribution that a
#    backfill restored and the Silver UTM layer parses out of it. 14,179 of
#    46,446 orders have it NULL today, so a plain `EXCLUDED.manager_comment`
#    here would not be a subtle divergence: it would erase attribution on one
#    side only, and the reconciliation would report a difference this mirror
#    had itself created.
#
# 2. LINE ITEMS ARE REPLACED, NOT UPSERTED.
#
#    Their ids are `order_id * 1000 + position`, so an order that drops from
#    three items to two leaves `…002` behind under a plain upsert — a line item
#    Postgres holds and DuckDB does not. DuckDB deletes an order's products and
#    reinserts them; so does this, in the same transaction as the header.
#
# 3. THE PRODUCTS WATERMARK ONLY MOVES WHEN PRODUCTS MOVED.
#
#    `skip_products=True` is the status-only refresh path: it rewrites headers
#    and deliberately leaves line items alone. Stamping `bronze.order_products`
#    as freshly shipped there would tell the reconciliation that line items are
#    current when nothing looked at them, which is the one thing the watermark
#    exists to prevent.

ORDERS_TABLE = "bronze.orders"
ORDER_PRODUCTS_TABLE = "bronze.order_products"

# Every column except `manager_comment`, which has the COALESCE above.
_ORDER_UPSERT = """
INSERT INTO bronze.orders
       (id, source_id, status_id, status_group_id, grand_total,
        ordered_at, created_at, updated_at, buyer_id, manager_id,
        manager_comment, promocode)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (id) DO UPDATE SET
    source_id       = EXCLUDED.source_id,
    status_id       = EXCLUDED.status_id,
    status_group_id = EXCLUDED.status_group_id,
    grand_total     = EXCLUDED.grand_total,
    ordered_at      = EXCLUDED.ordered_at,
    created_at      = EXCLUDED.created_at,
    updated_at      = EXCLUDED.updated_at,
    buyer_id        = EXCLUDED.buyer_id,
    manager_id      = EXCLUDED.manager_id,
    manager_comment = COALESCE(EXCLUDED.manager_comment, bronze.orders.manager_comment),
    promocode       = EXCLUDED.promocode,
    mirrored_at     = now()
"""

_ORDER_PRODUCT_INSERT = """
INSERT INTO bronze.order_products
       (id, order_id, product_id, name, quantity, price_sold)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO UPDATE SET
    order_id    = EXCLUDED.order_id,
    product_id  = EXCLUDED.product_id,
    name        = EXCLUDED.name,
    quantity    = EXCLUDED.quantity,
    price_sold  = EXCLUDED.price_sold,
    mirrored_at = now()
"""


def _numeric(value: Any) -> Optional[Decimal]:
    """A money column as Postgres wants it.

    `NUMERIC(12,2)` on this side, `DECIMAL(12,2)` on the other, and a Python
    float in between — `OrderRow.grand_total` is `float(order.grand_total)`
    because that is what the DuckDB path has always been handed. Converted via
    `str` rather than `Decimal(float)` so that 100.10 stays 100.10 instead of
    becoming 100.099999999999994315658113919198513031005859375, and left
    unquantized so the column's own rounding applies — the same rounding
    DuckDB's DECIMAL(12,2) applies to the same value.
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _order_params(row: Sequence[Any]) -> tuple:
    """One OrderRow with its money column typed for Postgres."""
    values = list(row)
    values[4] = _numeric(values[4])          # grand_total
    return tuple(values)


def _order_product_params(row: Sequence[Any]) -> tuple:
    values = list(row)
    values[5] = _numeric(values[5])          # price_sold
    return tuple(values)


async def write_orders(
    orders: Sequence[Any],
    products: Sequence[Any],
    *,
    replace_products: bool,
) -> None:
    """Headers, line items and both watermarks, in one transaction.

    Public and **raises** — the opposite contract to `mirror_orders`, which
    wraps this and never does. The sync path must not be broken by Postgres;
    the backfill has nothing to protect and every reason to stop loudly.

    One transaction across both tables because a half-applied order — header
    updated, line items still the old ones — is the exact state the
    `orders_without_line_items` check spent months chasing on the other store.
    """
    from core.pg import get_pool

    order_ids = [int(r[0]) for r in orders]

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            if orders:
                await conn.executemany(
                    _ORDER_UPSERT, [_order_params(r) for r in orders],
                )

                # Step 0. Archive a version of every header that actually
                # moved — here, and not in `mirror_orders`, because this is
                # the transaction that wrote the row being described and
                # because the value to archive is the one COALESCE settled on,
                # not the one that arrived. `core/pg_order_versions.py` has the
                # argument in full; the short version is that the mirror can
                # afford to lose a write and the archive cannot.
                from core.pg_order_versions import capture_versions

                versions = await capture_versions(conn, order_ids)
                if versions:
                    logger.info(
                        f"order_versions: {versions} new version(s) "
                        f"from {len(order_ids)} written order(s)"
                    )

            if replace_products and order_ids:
                # Delete first, then insert: the ids are positional, so the
                # only way to lose a dropped line item is to remove the whole
                # order's set and lay down what the payload actually carries.
                await conn.execute(
                    "DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])",
                    order_ids,
                )
                if products:
                    # Deduplicate by id, keeping the last — the API repeats an
                    # order across paginated pages and a repeat would otherwise
                    # collide with itself inside one statement.
                    deduped = {int(p[0]): p for p in products}
                    await conn.executemany(
                        _ORDER_PRODUCT_INSERT,
                        [_order_product_params(p) for p in deduped.values()],
                    )

            await conn.execute(_WATERMARK_OK, ORDERS_TABLE, len(orders))
            if replace_products:
                await conn.execute(
                    _WATERMARK_OK, ORDER_PRODUCTS_TABLE, len(products),
                )


async def mirror_orders(
    orders: Sequence[Any],
    products: Sequence[Any],
    *,
    replace_products: bool = True,
) -> List[MirrorOutcome]:
    """Mirror order headers and their line items. Never raises.

    Takes rows, not payloads: `DuckDBStore.upsert_orders` has already parsed
    them through `core.landing_rows` and — more importantly — has already
    decided which ones it wrote. The mirror ships that decision rather than
    making its own, so the two stores cannot disagree about what a sync did.
    """
    tables = [ORDERS_TABLE] + ([ORDER_PRODUCTS_TABLE] if replace_products else [])
    counts = {ORDERS_TABLE: len(orders), ORDER_PRODUCTS_TABLE: len(products)}
    outcomes = [
        MirrorOutcome(table=t, rows=counts[t]) for t in tables
    ]

    if not enabled():
        for out in outcomes:
            out.skipped = f"{MIRROR_ENV} is off"
        return outcomes

    if not orders:
        # Nothing to ship is not a success: moving the watermark here would
        # date-stamp a shipment that did not happen.
        for out in outcomes:
            out.skipped = "no rows"
        return outcomes

    try:
        await write_orders(orders, products, replace_products=replace_products)
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        for out in outcomes:
            out.error = detail
            _record_error(out.table, out.rows, detail)
            await _record_failure(out.table, detail)
        return outcomes

    for out in outcomes:
        out.ok = True
        _record_ok(out.table, out.rows)
    return outcomes
