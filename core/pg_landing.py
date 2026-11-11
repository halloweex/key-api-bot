"""The order tables' writer and the stamps of `meta.mirror_state`.

Step 05's first write was a mirror: the same KeyCRM payload written to
Postgres beside DuckDB. Step 14's S14-13 deleted the mirror's half of this
module — `_mirror`, `mirror_categories`, `mirror_products`,
`mirror_expenses`, `mirror_orders`, the catalogue carry and the stand-down
helpers every shipper out of DuckDB asked — with the shippers themselves
(`pg_backfill`, `pg_expense_backfill`, `pg_buyers`, `pg_replication`,
`pg_operational`). What is left is what the write chains run:
`_write_order_rows` and `_write_rows` (chain 3), `WATERMARK_OK_SQL` (chain 6
and every derived layer's stamp) and `_record_failure`, byte for byte as they
were.

THE FAILURE POLICY

A failure is caught, counted, logged at ERROR, written to the watermark when
the database is reachable enough to take it, and published on `/api/health`.
That is the 2026-08-09 lesson: an `ALTER` failed inside `try/except` with a
DEBUG log, the code read the column anyway, and a one-off became a rebuild
every two minutes for hours.

WHY A WATERMARK AND NOT JUST A COUNTER

`meta.mirror_state` is what tells **"not written yet"** from **"written
wrong"**. `last_ok_at` and `last_attempted_at` are both kept: equal means
healthy, and the gap between them is how long this table has been behind.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Optional, Sequence

# At module level, unlike the other `core.pg_*` imports here, because
# `write_orders` names one of its constants in a default argument. Safe: that
# module imports nothing from this package and holds no connection of its own.
from core import pg_order_versions as order_versions

logger = logging.getLogger(__name__)


# `mirror_on()` and `enabled()` stood here until step 14's S14-15: whether
# `KS_MIRROR_LANDING` turned the landing mirror on, a constant True since
# S14-00. Their last consumer was step 13's `evaluate_preconditions`, deleted
# with `core/warehouse_cutover.py`; nothing ships out of DuckDB since S14-13.


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

# The statement that says "a whole catalogue landed at this instant", public
# because two writers stamp it: the mirror here, and chain 6's writer
# (`core/pg_catalogue_write.py`), which takes the catalogue's writes from it.
# One text, so `last_ok_at` means the same thing whichever of them wrote it —
# the instant every row of the last full write carries as its `mirrored_at`,
# which is what tells a product KeyCRM retired from one that was lost
# (owner decision OD-15 (a)).
WATERMARK_OK_SQL = _WATERMARK_OK


def _statement(table: str, columns: Sequence[str]) -> str:
    cols = ", ".join(columns)
    vals = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    # `id` is the conflict target, so it is not in the SET list — assigning a
    # column to itself is legal and pointless, and leaving it out keeps the
    # statement saying what it means.
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "id")
    return _UPSERT.format(table=table, cols=cols, vals=vals, sets=sets)


async def _write_rows(conn, table: str, columns: Sequence[str],
                      rows: Sequence[tuple]) -> None:
    """`_write`'s statements on the caller's connection and transaction.

    The mirror runs them in a transaction of its own; chain 3's writer runs
    them for `bronze.expenses` in the transaction that writes the same tick's
    orders, so an order's header and its costs land together or neither does
    (`core/pg_orders_write.py`). One statement for both, so `mirrored_at` —
    the copy-back's rewrite clock and the comparison's — means the same thing
    whichever of them wrote the row.
    """
    if rows:
        await conn.executemany(_statement(table, columns), rows)
    await conn.execute(_WATERMARK_OK, table, len(rows))


async def _record_failure(table: str, error: str) -> bool:
    """Best effort: say in Postgres that the mirror failed.

    Usually pointless — if the write failed because Postgres is unreachable,
    so will this. It is here for the other case, the one that actually
    misleads: a write that failed on *its own* data while the connection was
    fine. Without this the watermark would keep the last success and read as
    healthy.

    Returns whether the failure was written, so a caller stamping many tables
    can stop at the first one that could not be, rather than waiting on an
    unreachable server once per table.
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
        return True
    except Exception as exc:
        logger.debug("mirror: could not record the failure either: %s", exc)
        return False


# ─── Orders ───────────────────────────────────────────────────────────────────
#
# Two tables, one transaction, and three things the catalogue never had to
# think about.
#
# 1. `manager_comment` IS NOT OVERWRITTEN WITH NULL.
#
#    `DuckDBStore.upsert_orders` updated it as `COALESCE(?, manager_comment)`
#    until step 14's S14-09 deleted it, so a payload that omits the field
#    keeps whatever is stored, and this does the same, for chain 3's writer
#    too (it runs `_write_order_rows`). That column carries UTM attribution a
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
    version_kind: str = order_versions.CHANGE,
) -> None:
    """Headers, line items and both watermarks, in one transaction.

    Public and **raises** — the opposite contract to `mirror_orders`, which
    wraps this and never does: the sync path, which ran through it until step
    14's S14-09, must not be broken by Postgres; the backfill has nothing to
    protect and every reason to stop loudly.

    One transaction across both tables because a half-applied order — header
    updated, line items still the old ones — is the exact state the
    `orders_without_line_items` check spent months chasing on the other store.

    `version_kind` labels what the archive calls a header that moved. It
    defaults to the sync's `'change'`, so every caller that existed before
    OD-20 writes exactly what it wrote before; `core/pg_backfill.py` passed
    `'backfill'` for the two `manager_comment` repairs until step 14's S14-09,
    and `pg_orders_write.restore_manager_comments` passes it to the core
    since.

    The statements themselves are `_write_order_rows`, which runs on the
    caller's transaction; this is the acquire and the transaction around them
    for every path that ships DuckDB's orders.
    """
    from core.pg import get_pool

    # Validated here, before the pool is acquired, rather than inside the
    # capture. A typo could only come from code, but a raise from inside the
    # transaction would roll back the header upsert as well — and on the sync
    # path, until step 14's S14-09, `mirror_orders` would have swallowed it, so
    # the order would be lost to Postgres over a misspelt constant. Refusing
    # early costs nothing.
    check_version_kind(version_kind)

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _write_order_rows(
                conn, orders, products,
                replace_products=replace_products, version_kind=version_kind)


def check_version_kind(version_kind: str) -> None:
    """Refuse a `version_kind` the archive does not know. Raises ValueError."""
    if version_kind not in order_versions.WRITER_KINDS:
        raise ValueError(
            f"version_kind={version_kind!r} is not one of "
            f"{order_versions.WRITER_KINDS}"
        )


async def _write_order_rows(
    conn,
    orders: Sequence[Any],
    products: Sequence[Any],
    *,
    replace_products: bool,
    version_kind: str,
) -> None:
    """The order tables' statements, on the caller's transaction.

    **The one writer of the order tables, a write chain's included** (DN-22b).
    Two paths run it: `write_orders`, for everything that ships DuckDB's
    orders, and chain 3's writer (`core/pg_orders_write.py`), which writes
    Postgres directly once `KS_WRITE_ORDERS` moves the orders. Both are handed
    the rows `core.landing_rows` parsed, and both run these statements, so
    this is where the version is captured in the row's own transaction, and
    where the `bronze.orders` watermark moves that the canary's `mirror_stale`
    and the archive's `order_versions_stalled` read — both keep their meaning
    under the chain only because it does. `tests/unit/test_write_chains.py`
    walks for a second writer.

    On the caller's transaction, never one of its own around the rows: chain
    3 proves its first write by the owner row sharing an `xmin` with the row
    it was taken for, and a savepoint would give the rows a subtransaction's
    XID. Only the derivation mark sits in a savepoint of its own, which
    `mark()` opens anyway and the derivation guard reads. Every caller holds
    the transaction (`tests/unit/test_pg_own_derivation.py` walks for one
    that does not). `version_kind` is the caller's to have checked.
    """
    order_ids = [int(r[0]) for r in orders]

    if orders:
        await conn.executemany(
            _ORDER_UPSERT, [_order_params(r) for r in orders],
        )

        # Step 0. Archive a version of every header that actually moved —
        # here, and not in `mirror_orders`, because this is the transaction
        # that wrote the row being described and because the value to archive
        # is the one COALESCE settled on, not the one that arrived.
        # `core/pg_order_versions.py` has the argument in full; the short
        # version is that the mirror can afford to lose a write and the
        # archive cannot.
        from core.pg_order_versions import capture_versions

        versions = await capture_versions(conn, order_ids, version_kind)
        if versions:
            logger.info(
                f"order_versions: {versions} new {version_kind} "
                f"version(s) from {len(order_ids)} written order(s)"
            )

    if replace_products and order_ids:
        # Delete first, then insert: the ids are positional, so the only way
        # to lose a dropped line item is to remove the whole order's set and
        # lay down what the payload actually carries.
        await conn.execute(
            "DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])",
            order_ids,
        )
        if products:
            # Deduplicate by id, keeping the last — the API repeats an order
            # across paginated pages and a repeat would otherwise collide with
            # itself inside one statement.
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

    # Chain 2: Postgres owes a rebuild of what it derives from these rows.
    # Last, and in a savepoint of its own — no mark without the rows, no rows
    # without the mark — and unable to cost the transaction anything, the
    # order versions above included: see `core/pg_derivation.py`. Postgres
    # derives on its own signal unconditionally since step 14's S14-15, which
    # removed `KS_PG_DERIVE` and the `owns()` this asked first.
    if orders:
        from core.pg_derivation import mark

        async with conn.transaction():
            await mark(conn)
