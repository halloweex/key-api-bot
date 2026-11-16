"""The discount on an order and on each of its lines, as KeyCRM serves it

Revision ID: 0037_order_discounts
Revises: 0036_analytics_captures
Created: 2026-11-16

WHY

On 2026-11-16 the owner asked for every buyer with, among other things, how
many of their orders were bought at a discount. The store could answer only by
the promocode: of KeyCRM's discount fields the parse kept `promocode` and
dropped the rest, so a discount typed by a manager, a loyalty programme's
percent and the discount on a single line were nowhere but in KeyCRM. That
export had to page through KeyCRM again. These columns land the figures with
the order (`core.landing_rows`, `ORDER_DISCOUNT_NUMERICS` and
`LINE_DISCOUNT_NUMERICS`), so the next such question is a query.

WHAT THEY HOLD (confirmed on a live `GET /order?include=products` page,
2026-11-16)

`bronze.orders`:
- `products_total` — the lines' sum before the order's own discount;
- `total_discount` — that discount in money (4085 at −10 % is 409, and
  `grand_total` 3676);
- `discount_amount`, `discount_percent` — the discount as typed;
- `discount_data` — KeyCRM's object as it came (a `loyalty` key when a
  programme applied), NULL when it was empty or null.

`bronze.order_products`:
- `price` — the unit price before any discount (`price_sold` is after);
- `total_discount` — the discount PER UNIT (980 → 591 is 389), so a line's
  discount in money is `total_discount * quantity`;
- `discount_percent`, `discount_amount` — the line's discount as typed;
- `loyalty_discount`, `individual_discount` — the loyalty programme's and the
  buyer's individual percent.

A GIFT CERTIFICATE IS A DISCOUNT HERE, AND THAT IS KEYCRM'S DOING

KeyCRM records a certificate redemption as the order's own discount: the
certificate's face value typed as `discount_amount`, so `total_discount`
carries it, `discount_percent` is 0, no promocode, no loyalty. Measured
2026-11-16 on the four certificate orders of the 2026-08-22 note — 5 000,
3 000, 2 000 and 5 000.68 off, each — and the payments list only the cash
part. The certificate was sold earlier as a sale of its own, so this is money
already counted, not a price cut. On the newest 1 000 orders 164 had an
order-level discount: 12 by promocode, 21 by a typed percent, 38 by loyalty
and 93 by a typed amount (₴115 021, 35 of them with `grand_total` 0), which
is where certificates sit beside a manager's manual discount, with nothing
in these columns to tell the two apart. So `total_discount > 0` is not
"bought at a discount": a reader decides what a typed amount with no percent
means. Line discounts (`order_products.total_discount`) are price cuts on the
goods and carry no certificate: 637 of the 1 000 had a discount of either
kind. `products_total - total_discount = grand_total` held on all 1 000.

TYPES

Money KeyCRM states to the kopeck is NUMERIC(12,2), as `grand_total` and
`price_sold` are. A percent, and a line's discount per unit — which an
order-level percent spread over a line's units need not leave whole kopecks
of — are NUMERIC(12,4). The parse rounds each value to its column's scale
before it is written, and a value the column cannot hold lands as NULL rather
than refusing the order (`core.landing_rows._discount`): a discount figure is
a fact beside the order and must never cost it.

Every column is nullable and has no default. NULL means "not landed": every
order synced before this revision, until the backfill
(`scripts/backfill_discounts.py`) reaches it. Adding a nullable column with no
default is a catalogue change in PostgreSQL 11 and later — no table rewrite,
only a brief ACCESS EXCLUSIVE lock each table queues for behind whatever is
reading it.

WHAT IS NOT VERSIONED

`app.order_versions` compares `pg_order_versions.VERSIONED_COLUMNS`, and the
discount is not among them, deliberately: the backfill writes these columns
on ~50 000 stored orders, and a compared column would archive each of them as
a version of an order that did not change — `order_versions_flooding`, and an
archive whose history says every order moved on one day. A discount that
moves the money moves `grand_total`, which is versioned.

THE DOWNGRADE KEEPS THE COLUMNS

As 0036's does (owner decision D2), and for the same reason one schema down:
an image that requires an earlier revision names its order columns one by
one (`pg_landing._ORDER_UPSERT`, `_ORDER_PRODUCT_INSERT`,
`pg_orders_write.restore_manager_comments`), so it neither reads nor writes
these and runs beside them; dropping them would throw away a backfill that
costs ~1 000 KeyCRM requests to repeat. So `alembic downgrade` below this
revision moves the stamp and executes nothing, and the next `upgrade head` is
a no-op, every statement being IF NOT EXISTS. While an older image runs, the
orders it writes keep whatever discount they held; the backfill brings them
back to KeyCRM's figures after the roll forward.
"""
from __future__ import annotations

from alembic import op

revision = "0037_order_discounts"
down_revision = "0036_analytics_captures"
branch_labels = None
depends_on = None


# What each column says, written into the catalogue so a reader of the table
# needs nothing else. The per-unit line discount above all: read as the line's
# total it would understate a line of five units fivefold.
ORDER_COMMENTS = {
    "products_total": "KeyCRM products_total: the lines' sum before the order's "
                      "own discount. NULL: not landed yet (revision 0037).",
    "total_discount": "KeyCRM total_discount: the order's discount in money; "
                      "products_total less it is grand_total. Carries a gift "
                      "certificate's redemption too (see discount_amount). "
                      "NULL: not landed yet.",
    "discount_amount": "KeyCRM discount_amount: the order's discount as typed, "
                       "in money. A gift certificate's redemption is recorded "
                       "here by KeyCRM (face value, discount_percent 0, no "
                       "promocode), so a typed amount is not always a price cut. "
                       "NULL: not landed yet.",
    "discount_percent": "KeyCRM discount_percent: the order's discount as typed, "
                        "in percent. NULL: not landed yet.",
    "discount_data": "KeyCRM discount_data as served (a loyalty key when a "
                     "loyalty programme applied); NULL when empty or not landed.",
}
LINE_COMMENTS = {
    "price": "KeyCRM price: the unit price before any discount (price_sold is "
             "after it). NULL: not landed yet (revision 0037).",
    "total_discount": "KeyCRM total_discount: the discount PER UNIT; the line's "
                      "discount in money is total_discount * quantity. NULL: "
                      "not landed yet.",
    "discount_percent": "KeyCRM discount_percent: the line's discount as typed, "
                        "in percent. NULL: not landed yet.",
    "discount_amount": "KeyCRM discount_amount: the line's discount as typed. "
                       "NULL: not landed yet.",
    "loyalty_discount": "KeyCRM loyalty_discount: the loyalty programme's "
                        "percent on this line. NULL: not landed yet.",
    "individual_discount": "KeyCRM individual_discount: the buyer's individual "
                           "discount on this line. NULL: not landed yet.",
}


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE bronze.orders
            ADD COLUMN IF NOT EXISTS products_total   NUMERIC(12,2),
            ADD COLUMN IF NOT EXISTS total_discount   NUMERIC(12,2),
            ADD COLUMN IF NOT EXISTS discount_amount  NUMERIC(12,2),
            ADD COLUMN IF NOT EXISTS discount_percent NUMERIC(12,4),
            ADD COLUMN IF NOT EXISTS discount_data    JSONB
        """
    )
    op.execute(
        """
        ALTER TABLE bronze.order_products
            ADD COLUMN IF NOT EXISTS price               NUMERIC(12,2),
            ADD COLUMN IF NOT EXISTS total_discount      NUMERIC(12,4),
            ADD COLUMN IF NOT EXISTS discount_percent    NUMERIC(12,4),
            ADD COLUMN IF NOT EXISTS discount_amount     NUMERIC(12,4),
            ADD COLUMN IF NOT EXISTS loyalty_discount    NUMERIC(12,4),
            ADD COLUMN IF NOT EXISTS individual_discount NUMERIC(12,4)
        """
    )
    for table, comments in (("bronze.orders", ORDER_COMMENTS),
                            ("bronze.order_products", LINE_COMMENTS)):
        for column, text in comments.items():
            op.execute(
                f"COMMENT ON COLUMN {table}.{column} IS "
                + "'" + text.replace("'", "''") + "'"
            )


def downgrade() -> None:
    """Nothing: Alembic moves the stamp to 0036 and every column stays.

    An image at an earlier revision names its order columns one by one and
    never sees these; dropping them would discard the backfill. The module
    docstring has the rest.
    """
