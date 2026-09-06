"""Replicating the five tables that have no source to be rebuilt from.

`core/pg_landing.py` mirrors a KeyCRM payload: parsed once, written twice,
recoverable from KeyCRM if Postgres is ever lost. `core/pg_replication.py`
copies the manager classification, which a human decided and KeyCRM never knew.
This module is the second kind, five times over — see revision 0008 for what
each table knows that its source does not.

WHY REPLICATED AND NOT MIRRORED, IN ONE SENTENCE EACH

`stock_movements` is a delta against the *previous* contents of `offer_stocks`;
a second computation on this side would need its own previous state and would
diverge the first time either side missed a sync. `sku_inventory_status`
carries `first_seen_at` forward out of its own previous contents.
`inventory_sku_history` and `inventory_history` are snapshots of a moment that
has passed. `order_backfill_misses` records what an API call did *not* return,
which no API call can be made to return.

TWO SHAPES, CHOSEN BY HOW DUCKDB WRITES EACH ONE

**Full replace** — `order_backfill_misses` (43 rows), `inventory_history` (188)
and `sku_inventory_status` (887). Small, and the last is a `DELETE`+`INSERT` on
the DuckDB side too, so replacing it whole is the same operation rather than a
decision. A full replace also removes the "lost or retired" question the
catalogue mirror needed a watermark rule to answer: the writer wrote every row
it holds, so anything missing was lost.

**Append above a watermark** — `inventory_sku_history` (143,274 rows) and
`stock_movements` (50,386). Both are append-only: no `UPDATE` and no `DELETE`
against either exists anywhere in this repository, which was checked rather
than assumed. Rewriting 193,000 rows every hour to gain the ~900 that changed
would be waste with a straight face.

**Neither keeps a cursor.** The watermark is `MAX(date)` and `MAX(id)` read
back out of Postgres at the start of each run — the same choice
`core/pg_backfill.py` made and for the same reason: there is no stored position
to be wrong, and an interrupted run is resumed by recomputing rather than by
trusting a number somebody wrote down.

`inventory_sku_history` asks for `date >= MAX(date)`, not `>`. A day's ~887
rows go in one statement, so a partial day should not be possible — but `>=`
plus an upsert costs one re-shipped day and makes it not matter, where `>`
would leave a hole nothing would ever fill.

`stock_movements` asks for `id > MAX(id)` because its rows go in one
transaction ordered by id, so what Postgres holds is always a complete prefix.

AND THE HOLE A WATERMARK CANNOT SEE

A row lost from Postgres *below* the watermark is invisible to both of those
queries forever — `id > MAX(id)` steps over it and `date >= MAX(date)` never
looks back. That is not hypothetical; it is what a watermark means. The daily
comparison finds such a row (proved by deleting one: `mirror_missing_rows`,
CRITICAL, with the id), and `full=True` is what puts it back: watermarks
ignored, everything shipped, both append tables upserted rather than inserted
so a row that is present and *wrong* is corrected too.

Not automatic, deliberately. Reconciliation A reports and does not repair,
because a check that fixes what it finds destroys the evidence it found
anything — so the repair is a separate act by somebody who has read the
finding, exactly like `POST /api/mirror/backfill/orders`. Full costs ~7 s
against the production catalogue; incremental costs ~120 ms.

FAILURE POLICY

Never raises. This is called from the inventory sync and from the repair jobs,
and neither may be broken by a Postgres fault — charter rule 8, and the same
policy `pg_landing` and `pg_replication` already have. The watermark it does
not move is what Reconciliation A reads as "not replicated yet".
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Sequence, Tuple

logger = logging.getLogger(__name__)

MISSES_TABLE = "app.order_backfill_misses"
INVENTORY_HISTORY_TABLE = "app.inventory_history"
SKU_STATUS_TABLE = "app.sku_inventory_status"
SKU_HISTORY_TABLE = "app.inventory_sku_history"
MOVEMENTS_TABLE = "app.stock_movements"

MISS_COLUMNS: Tuple[str, ...] = ("order_id", "checked_at", "reason")

INVENTORY_HISTORY_COLUMNS: Tuple[str, ...] = (
    "date", "total_quantity", "total_value", "total_reserve", "sku_count",
    "recorded_at",
)

SKU_STATUS_COLUMNS: Tuple[str, ...] = (
    "offer_id", "product_id", "sku", "name", "brand", "category_id",
    "quantity", "reserve", "price", "purchased_price", "last_sale_date",
    "first_seen_at", "updated_at", "last_stock_out_at",
)

SKU_HISTORY_COLUMNS: Tuple[str, ...] = (
    "date", "offer_id", "quantity", "reserve", "price",
)

MOVEMENT_COLUMNS: Tuple[str, ...] = (
    "id", "offer_id", "product_id", "movement_type", "quantity_before",
    "quantity_after", "delta", "reserve_before", "reserve_after",
    "recorded_at", "source",
)

# The DuckDB table each one is read from. Postgres qualifies by schema and
# DuckDB does not, so the pair is spelled out rather than derived by stripping
# a prefix — a rule that guesses a table name is a rule that will guess wrong.
_FULL_REPLACE: Tuple[Tuple[str, str, Tuple[str, ...], str], ...] = (
    (MISSES_TABLE, "order_backfill_misses", MISS_COLUMNS, "order_id"),
    (INVENTORY_HISTORY_TABLE, "inventory_history", INVENTORY_HISTORY_COLUMNS, "date"),
    (SKU_STATUS_TABLE, "sku_inventory_status", SKU_STATUS_COLUMNS, "offer_id"),
)

# How many rows one `executemany` carries. 143,274 in a single call is one
# round trip holding one very large parameter list; chunking keeps the
# statement cache and the server's memory both bored.
CHUNK = 5000


def _insert(table: str, columns: Sequence[str]) -> str:
    values = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values})"


def _upsert(table: str, columns: Sequence[str], keys: Sequence[str]) -> str:
    """Insert, or overwrite the row already there.

    `inventory_sku_history` needs it on every run, because it re-ships the day
    it is resuming from. `stock_movements` needs it only under `full=True`,
    where the point is to correct rows below the watermark that are missing or
    wrong; on the incremental path it takes a plain INSERT so that a conflict
    is an error rather than a silent overwrite.
    """
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c not in keys
    )
    return (
        f"{_insert(table, columns)} "
        f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}"
    )


async def _write_chunked(conn, sql: str, rows: Sequence[tuple]) -> None:
    for start in range(0, len(rows), CHUNK):
        await conn.executemany(sql, rows[start:start + CHUNK])


def read_full_replace(conn) -> Dict[str, List[tuple]]:
    """The three small tables out of DuckDB, in the column order Postgres wants."""
    out: Dict[str, List[tuple]] = {}
    for pg_table, dk_table, columns, order_by in _FULL_REPLACE:
        rows = conn.execute(
            f"SELECT {', '.join(columns)} FROM {dk_table} ORDER BY {order_by}"
        ).fetchall()
        out[pg_table] = [tuple(r) for r in rows]
    return out


def read_appends(
    conn, since_date, since_movement_id: int,
) -> Tuple[List[tuple], List[tuple]]:
    """The two append-only tables, from where Postgres left off.

    `since_date` of None and `since_movement_id` of 0 both mean "Postgres holds
    nothing" and ask for the whole table, which is what the first run does.
    """
    if since_date is None:
        sku_history = conn.execute(
            f"SELECT {', '.join(SKU_HISTORY_COLUMNS)} FROM inventory_sku_history "
            "ORDER BY date, offer_id"
        ).fetchall()
    else:
        sku_history = conn.execute(
            f"SELECT {', '.join(SKU_HISTORY_COLUMNS)} FROM inventory_sku_history "
            "WHERE date >= ? ORDER BY date, offer_id",
            [since_date],
        ).fetchall()

    movements = conn.execute(
        f"SELECT {', '.join(MOVEMENT_COLUMNS)} FROM stock_movements "
        "WHERE id > ? ORDER BY id",
        [int(since_movement_id)],
    ).fetchall()

    return [tuple(r) for r in sku_history], [tuple(r) for r in movements]


async def replicate_operational(store, *, full: bool = False) -> Dict[str, Any]:
    """Copy all five tables from DuckDB to Postgres. Never raises.

    `full=True` ignores both watermarks and re-ships everything, upserting the
    two append-only tables so a row that is missing *or* wrong below the
    watermark is put right. It is the repair path for a finding the daily
    comparison raised, and it is never taken on a schedule.

    The DuckDB reads and the Postgres round trips are in separate blocks: the
    store's lock is not reentrant and every other reader waits behind it, so
    awaiting the network while holding it would stall the dashboard for the
    length of the copy.

    The two watermark reads happen **before** the DuckDB read, so a row written
    between them is re-shipped rather than skipped. The other order loses rows;
    this one costs a duplicate that the upsert and the `id >` filter both
    absorb.
    """
    from core.mirror_reconciliation import configured

    if not configured():
        return {"skipped": "KS_PG_DSN is not set"}

    started = time.monotonic()
    try:
        from core.pg import get_pool, require_revision
        from core.pg_landing import _WATERMARK_OK

        pool = await get_pool()
        await require_revision()

        if full:
            since_date, since_movement_id = None, 0
        else:
            async with pool.acquire() as conn:
                since_date = await conn.fetchval(
                    f"SELECT MAX(date) FROM {SKU_HISTORY_TABLE}"
                )
                since_movement_id = await conn.fetchval(
                    f"SELECT COALESCE(MAX(id), 0) FROM {MOVEMENTS_TABLE}"
                )

        async with store.connection() as conn:
            replaced = read_full_replace(conn)
            sku_history, movements = read_appends(
                conn, since_date, int(since_movement_id or 0),
            )
            totals = {
                SKU_HISTORY_TABLE: conn.execute(
                    "SELECT COUNT(*) FROM inventory_sku_history"
                ).fetchone()[0],
                MOVEMENTS_TABLE: conn.execute(
                    "SELECT COUNT(*) FROM stock_movements"
                ).fetchone()[0],
            }

        async with pool.acquire() as conn:
            async with conn.transaction():
                for pg_table, _dk, columns, _order in _FULL_REPLACE:
                    rows = replaced[pg_table]
                    await conn.execute(f"DELETE FROM {pg_table}")
                    if rows:
                        await _write_chunked(
                            conn, _insert(pg_table, columns), rows,
                        )
                    await conn.execute(_WATERMARK_OK, pg_table, len(rows))

                if sku_history:
                    await _write_chunked(
                        conn,
                        _upsert(
                            SKU_HISTORY_TABLE, SKU_HISTORY_COLUMNS,
                            ("date", "offer_id"),
                        ),
                        sku_history,
                    )
                if movements:
                    # A plain INSERT on the incremental path on purpose: rows
                    # above `MAX(id)` cannot already be there, so a conflict
                    # would mean the watermark logic is wrong and should say
                    # so loudly rather than overwrite and look fine.
                    await _write_chunked(
                        conn,
                        _upsert(MOVEMENTS_TABLE, MOVEMENT_COLUMNS, ("id",))
                        if full else
                        _insert(MOVEMENTS_TABLE, MOVEMENT_COLUMNS),
                        movements,
                    )
                # `last_rows` is the whole table, not this run's delta. The
                # comparison reads it as "how much should be here", and an
                # append that shipped nothing would otherwise record zero and
                # read as an empty table.
                await conn.execute(
                    _WATERMARK_OK, SKU_HISTORY_TABLE, int(totals[SKU_HISTORY_TABLE]),
                )
                await conn.execute(
                    _WATERMARK_OK, MOVEMENTS_TABLE, int(totals[MOVEMENTS_TABLE]),
                )

        result = {
            "full": full,
            "replaced": {t: len(replaced[t]) for t, _d, _c, _o in _FULL_REPLACE},
            "sku_history_appended": len(sku_history),
            "movements_appended": len(movements),
            "duration_ms": int((time.monotonic() - started) * 1000),
        }
        logger.info("Operational history replicated: %s", result)
        return result
    except Exception as e:
        # ERROR, not DEBUG. A copy that fails quietly is the 2026-08-09 shape,
        # and the watermark it did not move is what Reconciliation A reads.
        logger.error(
            "Operational history replication failed: %s", e, exc_info=True,
        )
        return {"error": f"{type(e).__name__}: {e}"}
