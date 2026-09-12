"""Replicating the seven tables that have no source to be rebuilt from — and
two that do.

`core/pg_landing.py` mirrors a KeyCRM payload: parsed once, written twice,
recoverable from KeyCRM if Postgres is ever lost. `core/pg_replication.py`
copies the manager classification, which a human decided and KeyCRM never knew.
This module is the second kind, seven times over — see revision 0008 for what
each table knows that its source does not; 0017 and 0018 added the two a human
types, `revenue_goals` and `manual_expenses`.

WHY REPLICATED AND NOT MIRRORED, IN ONE SENTENCE EACH

`stock_movements` is a delta against the *previous* contents of `offer_stocks`;
a second computation on this side would need its own previous state and would
diverge the first time either side missed a sync. `sku_inventory_status`
carries `first_seen_at` forward out of its own previous contents.
`inventory_sku_history` and `inventory_history` are snapshots of a moment that
has passed. `order_backfill_misses` records what an API call did *not* return,
which no API call can be made to return. `revenue_goals` and `manual_expenses`
are both numbers a human typed into a form — a target and an ad spend — and
KeyCRM has never heard of either.

TWO SHAPES, CHOSEN BY HOW DUCKDB WRITES EACH ONE

**Full replace** — `offer_stocks` (892 rows), `revenue_goals` (3),
`order_backfill_misses` (43), `inventory_history` (188) and
`sku_inventory_status` (887). Small, and the last is a `DELETE`+`INSERT` on
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

THE SIXTH TABLE, WHICH IS NOT LIKE THE OTHER FIVE

`bronze.offer_stocks` is landing data: written straight from KeyCRM's
offers/stocks payload, and re-fetchable if Postgres ever lost it. By the
argument above it belongs in `core/pg_landing.py`, not here.

It rides here because of where it was, and why that stopped working. It shipped
inside `replicate_sms` — sensibly, since `ltv_basis=margin` needs
`purchased_price` and the SMS tab was the only Postgres reader of it. Then
`KS_SMS_STORE=postgres` made DuckDB no longer the writer of the SMS state, and
`replicate_sms` correctly stood down **as a whole**: a full replace out of a
frozen DuckDB would roll back every opt-out recorded since the switch. The five
SMS tables wanted exactly that. `offer_stocks` did not — DuckDB still receives
it from KeyCRM every sync — so it silently stopped moving, at 08:00:53 on
2026-09-06, and `reconcile_sms` stands down on the same flag, so nothing would
ever have reported the drift. The tab kept computing margin from a snapshot
that was hours old and would have been months old, with new SKUs missing
entirely and their COGS therefore zero.

Here it cannot be caught by that class of switch again: this module has no
writer-side flag to stand down on, and `stock_movements` — a delta against the
*previous* contents of `offer_stocks` — is already computed on this tick, so
the two now travel together rather than on schedules that can diverge.

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

OFFER_STOCKS_TABLE = "bronze.offer_stocks"
GOALS_TABLE = "app.revenue_goals"
MANUAL_EXPENSES_TABLE = "app.manual_expenses"
EXPENSE_TYPES_TABLE = "bronze.expense_types"
MISSES_TABLE = "app.order_backfill_misses"
INVENTORY_HISTORY_TABLE = "app.inventory_history"
SKU_STATUS_TABLE = "app.sku_inventory_status"
SKU_HISTORY_TABLE = "app.inventory_sku_history"
MOVEMENTS_TABLE = "app.stock_movements"
BUYER_GENDER_TABLE = "app.buyer_gender"

BUYER_GENDER_COLUMNS: Tuple[str, ...] = (
    "buyer_id", "gender", "method", "confidence", "decided_from",
    "rules_version", "override_by_human", "decided_at",
)

# Bookkeeping is excluded by construction: DuckDB stamps `synced_at`, Postgres
# `mirrored_at`, and two correct copies differ on those.
OFFER_STOCK_COLUMNS: Tuple[str, ...] = (
    "id", "sku", "price", "purchased_price", "quantity", "reserve",
)

# Three rows at most, and the one the monthly report draws its target line
# from. Revision 0017 says why a number a human typed is replicated rather
# than derived.
GOAL_COLUMNS: Tuple[str, ...] = (
    "period_type", "goal_amount", "is_custom", "calculated_goal",
    "growth_factor", "updated_at",
)

# DuckDB's column order, `platform` last because a later migration added it
# to the table rather than into the middle of it. The comparison reads the
# same tuple, so the two cannot drift apart.
MANUAL_EXPENSE_COLUMNS: Tuple[str, ...] = (
    "id", "expense_date", "category", "expense_type", "amount",
    "currency", "note", "created_at", "updated_at", "platform",
)

# Imported from the shared parse, not restated: the *display* name is built
# there from a localisation key, and a second list here would be a second
# chance to disagree about what an expense is called.
from core.landing_rows import EXPENSE_TYPE_COLUMNS  # noqa: E402

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
    (OFFER_STOCKS_TABLE, "offer_stocks", OFFER_STOCK_COLUMNS, "id"),
    (GOALS_TABLE, "revenue_goals", GOAL_COLUMNS, "period_type"),
    # Full replace and not an upsert, because the expenses form genuinely
    # deletes: `DELETE FROM manual_expenses WHERE id = ?` is one of its three
    # statements, and a ghost here is ad spend that was withdrawn and still
    # divides the ROAS.
    (MANUAL_EXPENSES_TABLE, "manual_expenses", MANUAL_EXPENSE_COLUMNS, "id"),
    # Landing, and replicated rather than mirrored — `bronze.offer_stocks`'
    # exact situation and its reason. KeyCRM serves this dictionary, but only
    # to the **weekly** full sync, so a payload-fed mirror leaves Postgres
    # empty for up to a week. That is not a freshness detail: `/expenses`
    # renders the breakdown *by name*, so an empty dictionary collapses every
    # type into "Other" and empties the filter. Measured on production the
    # hour the flag first went on. DuckDB has the 27 rows every minute of that
    # week, so this copies them.
    (EXPENSE_TYPES_TABLE, "expense_types", EXPENSE_TYPE_COLUMNS, "id"),
    (MISSES_TABLE, "order_backfill_misses", MISS_COLUMNS, "order_id"),
    (INVENTORY_HISTORY_TABLE, "inventory_history", INVENTORY_HISTORY_COLUMNS, "date"),
    (SKU_STATUS_TABLE, "sku_inventory_status", SKU_STATUS_COLUMNS, "offer_id"),
    # Replicated for `app.manager_classifications`' reason, not `bronze.*`'s:
    # KeyCRM has no gender field, so nothing here can ever be re-fetched from
    # the source. It is decided by `core/gender.py` against the name DuckDB
    # holds, and a second derivation on the Postgres side would need the same
    # tables and would drift the day either copy was edited.
    #
    # Full replace, like `sku_inventory_status` and for the same reason: the
    # DuckDB writer is a DELETE+INSERT too (scripts/backfill_gender.py), so
    # this is the same operation rather than a separate decision. It also means
    # a row that is present and WRONG is corrected, which matters when
    # RULES_VERSION moves and every verdict is re-derived at once.
    #
    # 20 145 rows an hour to ship the ~19 new buyers a day is the cost. Measured
    # against the alternative — a `decided_at` watermark — it is not worth the
    # second mechanism: an upsert keyed on a clock cannot see a row whose
    # verdict was withdrawn to NULL, and withdrawal is exactly what a rules
    # change does.
    (BUYER_GENDER_TABLE, "buyer_gender", BUYER_GENDER_COLUMNS, "buyer_id"),
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
    """The seven small tables out of DuckDB, in the column order Postgres wants."""
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
    """Copy all nine tables from DuckDB to Postgres. Never raises.

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


async def replicate_after_manual_expense(store) -> Dict[str, Any]:
    """Carry a manual expense to Postgres now, rather than within the hour.

    WHY THIS EXISTS AT ALL, GIVEN THE ONE-CALL-SITE RULE ABOVE

    `replicate_operational` is hourly and has exactly one scheduled caller,
    because five code paths write these tables and hooking each is how the
    sixth gets forgotten — which is precisely what `update_manager_stats` did
    until `cf34e8b`. That rule is about *writers*, and this is not a second
    writer: it is the same function, called sooner.

    What changed is who reads. While `manual_expenses` was written in DuckDB
    and read in DuckDB, an hour of lag on the copy cost nothing —
    `core/pg_landing.py`'s revision 0018 note says so in as many words, and
    adds that the one-line fix is available if it ever stops being true.
    `KS_READ_EXPENSES=postgres` is when it stops: the expenses form writes one
    store and the page it returns to reads the other, so without this a human
    types an amount and watches it not appear.

    Measured: the incremental replication is 116 ms against the production
    catalogue, which is nothing on a form submit. The full first run is 9.3 s,
    and only ever happens once.

    **Never raises.** The DuckDB write has already committed and the endpoint
    must report it; a Postgres fault costs freshness until the hourly job,
    which is exactly where this table was before this function existed.
    """
    from core.mirror_reconciliation import configured

    if not configured():
        return {"skipped": "KS_PG_DSN is not set"}
    try:
        return await replicate_operational(store)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Manual expense not replicated — Postgres keeps the previous "
            "copy until the hourly job: %s", exc, exc_info=True,
        )
        return {"error": f"{type(exc).__name__}: {exc}"}
