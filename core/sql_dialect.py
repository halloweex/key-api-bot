"""One Silver projection, two engines.

Step 05 ends with Silver computed inside Postgres. The naive way to get there
is to write the projection a second time in Postgres dialect, and that is
exactly the mistake charter rule 1 exists to prevent: a rule with two homes is
a rule that will differ. The shortest recorded time it took to differ in this
codebase is **under a day** — #101 updated `sales_type` in one copy of the
Silver projection and not the other.

The good news, found by reading rather than assumed: the projection is almost
entirely ordinary SQL. Between DuckDB and PostgreSQL it differs in exactly
**two** places.

    the Kyiv date   DATE(timezone('Europe/Kyiv', x))     DuckDB
                    (timezone('Europe/Kyiv', x))::date   PostgreSQL

    table names     orders                    bronze.orders
                    managers                  bronze.managers
                    manager_classifications   app.manager_classifications

Everything else — the CASE, the EXISTS subqueries, the status tuple, the
boolean expressions — is identical text. So the projection stays in one place
and takes a dialect.

`timezone(zone, timestamptz)` means the same thing in both engines, which is
the one thing here worth doubting and was checked against production: 1,007
orders have a Kyiv date different from their UTC one, and the two stores agree
on every one of them.

WHY A DIALECT OBJECT AND NOT AN `if postgres:`

Because the difference is data, and writing it as data makes it testable. A
test renders both and asserts they are the same string once the two known
substitutions are undone — which proves there is no *third* divergence, the
thing a branchy implementation could hide indefinitely.
"""
from __future__ import annotations

from dataclasses import dataclass

from core.duckdb_constants import DISPLAY_TIMEZONE


@dataclass(frozen=True)
class Dialect:
    """Where the Silver projection's tables live, and how it reads a date."""

    name: str
    orders: str
    managers: str
    classifications: str
    silver_orders: str
    # The order-lines level joins three more tables. Same rule as above: the
    # names are the *only* thing allowed to differ between the two renderings.
    order_products: str
    products: str
    categories: str
    # A format template with one `{column}` hole. Not a function, so the whole
    # dialect stays comparable, printable and trivially frozen.
    date_template: str

    def kyiv_date(self, column: str) -> str:
        """The order's calendar date in the timezone the business reads."""
        return self.date_template.format(column=column, zone=DISPLAY_TIMEZONE)


DUCKDB = Dialect(
    name="duckdb",
    orders="orders",
    managers="managers",
    classifications="manager_classifications",
    silver_orders="silver_orders",
    order_products="order_products",
    products="products",
    categories="categories",
    # Byte-for-byte what `core.duckdb_constants._date_in_kyiv` has always
    # emitted. Changing it here changes stored Silver on the next rebuild.
    date_template="DATE(timezone('{zone}', {column}))",
)

POSTGRES = Dialect(
    name="postgres",
    # `bronze` is landing from KeyCRM; `app` is what nothing can decide again.
    # `postgres/initdb/30-app.sql` draws that line — see revision 0005 for why
    # the classification sits on the other side of it from the managers.
    orders="bronze.orders",
    managers="bronze.managers",
    classifications="app.manager_classifications",
    silver_orders="silver.orders",
    order_products="bronze.order_products",
    products="bronze.products",
    categories="bronze.categories",
    # `DATE(x)` also exists in PostgreSQL, but the cast is what the rest of
    # this repository's Postgres SQL uses, so it reads the same as its
    # neighbours in `core/reconciliation_io.py`.
    date_template="(timezone('{zone}', {column}))::date",
)


# ─── The order-lines level, one body for two engines ────────────────────────
#
# Step 2 of «Одна бронза» needs the line level in Postgres, and the line level
# already exists in DuckDB as a view. Same charter-rule-1 move as the Silver
# projection above: one body, the table names as the only difference. DuckDB
# renders it into `SILVER_ORDER_LINES_VIEW_SQL`; migration 0011 froze the
# Postgres rendering as `silver.order_lines`, and a test re-renders this
# template and asserts the two agree — the same contract revision 0010 has
# with `core/pg_order_versions.py`.

_ORDER_LINES_BODY = """SELECT
    op.id                                        AS line_id,
    op.order_id,
    op.product_id,
    op.name                                      AS product_name,  -- as sold
    op.quantity,
    op.price_sold,
    CAST(op.quantity * op.price_sold AS DECIMAL(14, 2)) AS line_amount,
    -- the order, denormalised: every predicate a page applies to
    -- revenue applies here too, and re-deriving them was the bug
    s.order_date,
    s.ordered_at,
    s.sales_type,
    s.source_id,
    s.source_name,
    s.is_return,
    s.is_active_source,
    s.buyer_id,
    s.manager_id,
    s.is_new_customer,
    s.buyer_first_order_date,
    s.promocode,
    s.grand_total                                AS order_grand_total,
    -- the catalog, denormalised. 8.2 % of lines carry no product_id and
    -- 31 % no category; both stay NULL rather than being dropped, which
    -- is the difference between a level and a filter.
    p.name                                       AS catalog_product_name,
    p.brand,
    p.sku,
    p.category_id,
    c.name                                       AS category_name,
    c.parent_id                                  AS parent_category_id,
    parent_c.name                                AS parent_category_name
FROM {order_products} op
JOIN {silver_orders} s ON s.id = op.order_id
LEFT JOIN {products} p ON p.id = op.product_id
LEFT JOIN {categories} c ON c.id = p.category_id
LEFT JOIN {categories} parent_c ON parent_c.id = c.parent_id"""


def order_lines_select(dialect: Dialect) -> str:
    """The order-lines SELECT, rendered for one engine."""
    return _ORDER_LINES_BODY.format(
        order_products=dialect.order_products,
        silver_orders=dialect.silver_orders,
        products=dialect.products,
        categories=dialect.categories,
    )
