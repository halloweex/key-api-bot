"""Capture a version of an order header, every time one actually changes.

Step 0 of "Одна бронза". The archive is `app.order_versions` (revision 0010);
this is the one thing that writes to it.

WHERE THE CHANGES COME FROM, AND WHY NOT FROM A POLL

`DuckDBStore.upsert_orders` already computes `updated_ids` on every tick — the
exact set of orders it wrote, with rows that were already in the desired state
excluded into `skipped_unchanged`. Today that set is used to decide what the
mirror ships and is then discarded. It is the capture point, and it is free.

The alternative — a periodic scan keyed on `updated_at` — cannot work here, and
fails on precisely the transitions the archive exists for: KeyCRM does not bump
`updated_at` when an order's status changes. That is the whole reason
`force_update` exists. A poll would also collapse two flips between two scans
into one.

WHY THIS RUNS INSIDE THE MIRROR'S TRANSACTION

Two reasons, and the second one only shows up on real data.

1. `mirror_orders` never raises: a Postgres outage must not break the sync,
   because whatever the mirror missed the backfill can ship later. **The
   archive has no backfill and cannot have one** — KeyCRM serves current state,
   so a transition that was not written when it happened is not recoverable
   from anywhere. It therefore cannot inherit the mirror's "lose it quietly"
   contract. Writing it inside `write_orders`, in the same transaction as the
   header it describes, means the version and the row it is a version of land
   together or neither does.

2. It must record the row **as written**, not as it arrived. The header upsert
   sets `manager_comment = COALESCE(EXCLUDED.manager_comment, stored)` because
   that column carries UTM attribution a backfill restored. 32,437 of 46,685
   production orders have a value in it. Comparing an incoming payload that
   omits the field against the last stored version would see "comment removed"
   on two orders in three, every tick, forever — an archive whose commonest
   entry is an event that never happened. Reading `bronze.orders` after the
   upsert, inside the same transaction, is what makes the comparison true.

WHAT COUNTS AS A CHANGE

`VERSIONED_COLUMNS`, and nothing else. `created_at` is absent because an order
is created once. `updated_at` is stored for forensics and deliberately not
compared: if it moves and every column held here is identical, then what this
store holds has not changed, and a row saying otherwise is an *observation*
rather than a *change*. `bronze_order_events` is the standing proof of what
that costs — ~150K rows a day, 43 GB, switched off by default.

Line items are not here at all. `order_status_refresh` runs daily at 05:15 with
`skip_products=True`, so it carries no line items by design; an archive that
recorded them would minute a mass deletion of ~1,400 baskets every morning.
Revision 0010 has the measurements.

IF THE TABLE IS NOT THERE YET

The INSERT raises, `write_orders` raises, and on the sync path `mirror_orders`
catches it and records a mirror failure against `bronze.orders` — visible in
`meta.mirror_state.failures_since_ok`, in the morning digest, and in the layer
age. That is the same way deploying `web` before `migrate` already fails for
revision 0007, and it fails closed rather than quietly.
"""
from __future__ import annotations

from typing import Sequence

TABLE = "app.order_versions"

# The columns that decide whether an order has changed. One home, rule 1: the
# capture below renders them, and `tests/unit/test_order_versions.py` asserts
# revision 0010 declares exactly these.
VERSIONED_COLUMNS: tuple[str, ...] = (
    "source_id",
    "status_id",
    "status_group_id",
    "grand_total",
    "ordered_at",
    "buyer_id",
    "manager_id",
    "manager_comment",
    "promocode",
)

# Stored on every row, never compared. See the module docstring.
CARRIED_COLUMNS: tuple[str, ...] = ("updated_at",)

_ALL = VERSIONED_COLUMNS + CARRIED_COLUMNS


def _capture_sql() -> str:
    """The whole capture, as one statement.

    One statement because it has to be: reading the latest version, comparing
    it and inserting the difference in three round-trips would leave room for
    the header to move in between, and would cost three times as much on the
    05:15 batch of ~1,400 orders.

    `DISTINCT ON` rather than a correlated subquery so the index
    `(order_id, captured_at DESC, id DESC)` serves the whole lookup, and
    `ROW(...) IS DISTINCT FROM ROW(...)` rather than a chain of `IS NOT
    DISTINCT FROM` so that NULL compares as a value — `manager_id` is NULL on
    every Shopify order, and a NULL-unsafe comparison would write a version for
    each of them on every tick.
    """
    versioned = ", ".join(VERSIONED_COLUMNS)
    stored = ", ".join(_ALL)
    o_stored = ", ".join(f"o.{c}" for c in _ALL)
    o_versioned = ", ".join(f"o.{c}" for c in VERSIONED_COLUMNS)
    l_versioned = ", ".join(f"l.{c}" for c in VERSIONED_COLUMNS)

    return f"""
WITH latest AS (
    SELECT DISTINCT ON (order_id) order_id, {versioned}
    FROM {TABLE}
    WHERE order_id = ANY($1::int[])
    ORDER BY order_id, captured_at DESC, id DESC
)
INSERT INTO {TABLE} (order_id, kind, {stored})
SELECT o.id,
       CASE WHEN l.order_id IS NULL THEN 'create' ELSE 'change' END,
       {o_stored}
FROM bronze.orders o
LEFT JOIN latest l ON l.order_id = o.id
WHERE o.id = ANY($1::int[])
  AND (l.order_id IS NULL
       OR ROW({o_versioned}) IS DISTINCT FROM ROW({l_versioned}))
RETURNING order_id
"""


CAPTURE_SQL = _capture_sql()


async def capture_versions(conn, order_ids: Sequence[int]) -> int:
    """Archive a version for each of `order_ids` whose header actually moved.

    Takes an open connection and no pool: the caller is `write_orders`, already
    inside its transaction, and acquiring a second connection here would put the
    version outside the transaction that writes the row it describes — which is
    the one thing this must not do.

    Returns the number of versions written, which is `<= len(order_ids)` by
    construction and usually far less: the 05:15 status refresh offers ~1,400
    ids and, on a day when nothing moved, writes none.
    """
    if not order_ids:
        return 0
    rows = await conn.fetch(CAPTURE_SQL, list(order_ids))
    return len(rows)
