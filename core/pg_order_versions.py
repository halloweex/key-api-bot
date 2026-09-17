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

# The four things a row of this table can be. `kind` has no CHECK constraint
# (revision 0010), so these are the whole vocabulary and they live here.
CREATE = "create"        # the archive's first sighting of an order
CHANGE = "change"        # the sync wrote a header that actually moved
BASELINE = "baseline"    # seeded by the migration, one per order it found
BACKFILL = "backfill"    # a `manager_comment` restored from KeyCRM — OD-20 (b)

# OD-20 (b), 2026-09-17. The two `manager_comment` backfills re-fetch a comment
# KeyCRM has always held and fill it in where the column is NULL; the row does
# change, so it is archived rather than skipped, but calling it a 'change'
# would date a transition on the day an operator ran a script. Labelled instead
# — the value, the order and the moment are all still recorded, and the two
# liveness checks below can tell an operator's run from the writer's output.
#
# Neither of these is the sync writer's work, so neither may answer "is the
# writer still running" or "is it writing too much":
# `core/mirror_reconciliation.py` renders this tuple into both statements. The
# baseline exclusion was found on production, filing a WARN with count=46,695
# on the day revision 0010 landed — one seeded row per order the migration
# found, and forty-six times the flood threshold.
#
# The backfill exclusion is that lesson taken before the event rather than
# after it, and OD-20 asked for the volume to be measured before it was taken.
# Measured on production 2026-09-17, and the numbers are smaller than the
# argument first drafted here assumed: **0** orders where Postgres holds a
# NULL `manager_comment` and DuckDB holds one, so nothing diverges today;
# 10,192 orders carry a NULL comment in DuckDB inside the backfills' own
# 730-day window, of which **26** are website orders (`source_id = 4`) — the
# upper bound on what a KeyCRM re-fetch could actually fill, because an order
# taken by hand in the Instagram inbox never carried a tag and never will.
# So one run today is ~26 rows against a threshold of 1,000, and the flood is
# not at risk at all.
#
# The exclusion stands anyway, because it is about what the count *means*
# rather than how large it is: a repair an operator deliberately started is
# not the content comparison having stopped discriminating, which is the one
# failure `order_versions_flooding` exists to name. The volume can move — the
# window is an argument (`--days`), and the shop's comment template has broken
# once already, in July 2026, which is what put 10,192 NULLs there — while the
# meaning cannot.
NOT_THE_WRITER: tuple[str, ...] = (BASELINE, BACKFILL)

# What a capture may label a row that moved. `create` is not here: a first
# sighting is named by the statement itself, because it is a fact about the
# archive rather than about the caller.
WRITER_KINDS: tuple[str, ...] = (CHANGE, BACKFILL)

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

    The label of a moved row is `$2`, not a rendered literal, so that one
    statement text serves every caller (OD-20 (b)) instead of a second SQL
    string per kind. That is all the parameter buys: it is **not** a guarantee
    about the vocabulary. `kind` has never carried a CHECK — verified on a
    migrated revision 0033 database, where `app.order_versions` holds only its
    primary key and an `INSERT ... VALUES (1, 'nonsense')` is accepted — so
    the vocabulary is enforced in code, by `capture_versions` before it binds
    and by `write_orders` earlier still, before the pool is acquired. A first
    sighting stays `'create'` under every caller, because that is the archive
    speaking about itself rather than about the write.
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
       CASE WHEN l.order_id IS NULL THEN '{CREATE}' ELSE $2::text END,
       {o_stored}
FROM bronze.orders o
LEFT JOIN latest l ON l.order_id = o.id
WHERE o.id = ANY($1::int[])
  AND (l.order_id IS NULL
       OR ROW({o_versioned}) IS DISTINCT FROM ROW({l_versioned}))
RETURNING order_id
"""


CAPTURE_SQL = _capture_sql()


async def capture_versions(
    conn, order_ids: Sequence[int], kind: str = CHANGE,
) -> int:
    """Archive a version for each of `order_ids` whose header actually moved.

    Takes an open connection and no pool: the caller is `write_orders`, already
    inside its transaction, and acquiring a second connection here would put the
    version outside the transaction that writes the row it describes — which is
    the one thing this must not do.

    `kind` is what a moved row is called — `CHANGE` for the sync, `BACKFILL`
    for the two `manager_comment` backfills. It defaults to the sync's answer
    so that every existing caller keeps writing exactly what it wrote before.

    **The label is checked here, and not only in `write_orders`.** This is a
    public function and it is where the string actually reaches the archive;
    a caller arriving directly would otherwise write an unvetted `kind` into
    an append-only table nothing can delete from, and both liveness checks
    match kinds exactly, so they would silently count it as the writer's
    output. `write_orders` keeps its own copy of the check because it can
    refuse before acquiring the pool, which is a different thing worth having.

    Returns the number of versions written, which is `<= len(order_ids)` by
    construction and usually far less: the 05:15 status refresh offers ~1,400
    ids and, on a day when nothing moved, writes none.
    """
    if kind not in WRITER_KINDS:
        raise ValueError(f"kind={kind!r} is not one of {WRITER_KINDS}")
    if not order_ids:
        return 0
    rows = await conn.fetch(CAPTURE_SQL, list(order_ids), kind)
    return len(rows)
