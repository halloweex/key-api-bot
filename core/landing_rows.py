"""One reading of a KeyCRM payload, for however many stores want it.

Step 05 writes landing to Postgres alongside DuckDB. The two must not disagree,
and the cheapest way to guarantee that is for both to receive the *same*
already-parsed rows rather than each reading the payload for itself.

Today the reading lives inside `DuckDBStore.upsert_*`, interleaved with the SQL
— which is fine while there is one store and becomes a second home for the rule
the moment there are two. Charter rule 1: a rule with two homes is a rule that
will differ, and the shortest recorded time it took to differ in this codebase
is under a day (#101 — the `sales_type` rule was updated in one copy of the
Silver projection and not the other).

So the parsing moves here, pure: dict in, typed row out, no I/O, no store, no
connection. A store's job becomes writing a row it was handed.

WHAT IS AND IS NOT A COLUMN HERE

Only facts that came from KeyCRM. `synced_at`, `first_seen_at` and
`update_count` are bookkeeping a store keeps about its own copy — they differ
between two correct stores by construction, and putting them in a shared row
would make every row look like a discrepancy to the reconciliation that
compares the two.

BEHAVIOUR IS PRESERVED EXACTLY, INCLUDING THE PARTS THAT LOOK WRONG

This is a refactor and nothing more: a payload that produced a given row before
produces the identical row now, defaults, fallbacks and all. Two of them are
worth naming so that nobody "fixes" them here by accident:

* `name` falls back to `"Unknown"`, so a nameless category becomes a category
  named Unknown rather than a failure.
* `price` is `min_price or price` — an `or`, so a `min_price` of 0 falls
  through to `price`. Whether that is right is a question about KeyCRM's
  catalogue, not about this move.

Rows are NOT validated here. `id` may come back None and the database will
refuse it, which is where that refusal belongs — a pure function that raises on
bad input would move the failure away from the transaction that has to roll
back because of it.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, NamedTuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover — types only, no import cost at runtime
    from core.models import Order

# The KeyCRM custom field that carries a brand. Matched by uuid first and by
# name second, because a KeyCRM account can rename a field but not re-issue its
# uuid — and this codebase has met both spellings.
BRAND_FIELD_UUID = "CT_1001"
BRAND_FIELD_NAME = "Brand"


class CategoryRow(NamedTuple):
    """A category as KeyCRM describes it. Column order is the write order."""
    id: Optional[int]
    name: str
    parent_id: Optional[int]


class ProductRow(NamedTuple):
    """A product as KeyCRM describes it. Column order is the write order."""
    id: Optional[int]
    name: str
    category_id: Optional[int]
    brand: Optional[str]
    sku: Optional[str]
    price: Optional[float]


CATEGORY_COLUMNS = CategoryRow._fields
PRODUCT_COLUMNS = ProductRow._fields


def brand_of(product: Dict[str, Any]) -> Optional[str]:
    """The brand from a product's custom fields, or None.

    KeyCRM has no brand column: a human types it into a custom field when the
    product is created, which is why 423 of 948 products have none. See
    `core/duckdb_constants.brand_where` for what the dashboard does with that.

    `value[0]` — a multi-valued field loses everything after the first entry,
    silently, and did so before this function existed. Preserved deliberately:
    changing it would move brand revenue between buckets, and that is a
    decision about the catalogue rather than about where the code lives.
    """
    for field in product.get("custom_fields") or []:
        if field.get("uuid") == BRAND_FIELD_UUID or field.get("name") == BRAND_FIELD_NAME:
            values = field.get("value")
            if values and isinstance(values, list):
                return values[0]
            return None
    return None


def category_row(payload: Dict[str, Any]) -> CategoryRow:
    """One KeyCRM category payload as a row."""
    return CategoryRow(
        id=payload.get("id"),
        name=payload.get("name", "Unknown"),
        parent_id=payload.get("parent_id"),
    )


def product_row(payload: Dict[str, Any]) -> ProductRow:
    """One KeyCRM product payload as a row."""
    return ProductRow(
        id=payload.get("id"),
        name=payload.get("name", "Unknown"),
        category_id=payload.get("category_id"),
        brand=brand_of(payload),
        sku=payload.get("sku"),
        # `or`, not `if is None`: a min_price of 0 falls through to price. That
        # is what it did before; see the module docstring.
        price=payload.get("min_price") or payload.get("price"),
    )


def category_rows(payloads: List[Dict[str, Any]]) -> List[CategoryRow]:
    return [category_row(p) for p in payloads]


def product_rows(payloads: List[Dict[str, Any]]) -> List[ProductRow]:
    return [product_row(p) for p in payloads]


# ─── Orders ───────────────────────────────────────────────────────────────────
#
# Orders arrive through `core.models.Order.from_api`, which already has one
# home and keeps it. What lived in two places was the shaping *after* that —
# the field mapping, the `buyer.id if buyer else None` dance, the synthetic
# line-item id — and that is what moves here, for the same reason the catalogue
# did: two stores, one rule.

# A line item's id is `order_id * STRIDE + position`. KeyCRM does supply its own
# `id` per line (`OrderProduct.line_item_id`), and this deliberately does not use
# it: the stored ids would change under every existing row and nothing in the
# warehouse could tell an id change from a data change.
#
# The stride bounds two things at once — products per order, and how far order
# ids can grow before the arithmetic collides. Measured on production
# 2026-08-24: the largest order carries **47** line items and the largest id is
# 46,489,001, so the ceiling is order id ~2.1M for a 32-bit column. Nothing to
# do yet; a note so the next person does not have to derive it.
LINE_ITEM_ID_STRIDE = 1000


class OrderRow(NamedTuple):
    """An order header as KeyCRM describes it. Column order is the write order."""
    id: Optional[int]
    source_id: Optional[int]
    status_id: Optional[int]
    status_group_id: Optional[int]
    grand_total: float
    ordered_at: Optional[datetime]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    buyer_id: Optional[int]
    manager_id: Optional[int]
    manager_comment: Optional[str]
    promocode: Optional[str]


class OrderProductRow(NamedTuple):
    """One line item. Column order is the write order."""
    id: int
    order_id: int
    product_id: Optional[int]
    name: str
    quantity: int
    price_sold: float


ORDER_COLUMNS = OrderRow._fields
ORDER_PRODUCT_COLUMNS = OrderProductRow._fields


class LandedOrders(NamedTuple):
    """One walk over a page of orders, both tables' worth of rows."""
    orders: List[OrderRow]
    products: List[OrderProductRow]


def order_row(order: "Order") -> OrderRow:
    """One parsed order as a header row."""
    return OrderRow(
        id=order.id,
        source_id=order.source_id,
        status_id=order.status_id,
        status_group_id=order.status_group_id,
        grand_total=float(order.grand_total),
        ordered_at=order.ordered_at,
        created_at=order.created_at,
        updated_at=order.updated_at,
        buyer_id=order.buyer.id if order.buyer else None,
        manager_id=order.manager.id if order.manager else None,
        manager_comment=order.manager_comment,
        promocode=order.promocode,
    )


def order_product_rows(order: "Order") -> List[OrderProductRow]:
    """One parsed order's line items, with the synthetic ids the stores use."""
    return [
        OrderProductRow(
            id=order.id * LINE_ITEM_ID_STRIDE + position,
            order_id=order.id,
            product_id=item.product_id,
            name=item.name,
            quantity=item.quantity,
            price_sold=float(item.price_sold),
        )
        for position, item in enumerate(order.products)
    ]


def landed_orders(
    payloads: List[Dict[str, Any]], *, with_products: bool = True,
) -> LandedOrders:
    """Parse a page of KeyCRM orders into rows for both stores.

    **An order with no `ordered_at` is dropped, header and line items alike.**
    That is the existing rule in `DuckDBStore.upsert_orders` and it is preserved
    exactly, including the part that matters here: the header is skipped and its
    products are skipped with it, so a store can never end up holding line items
    whose order it refused. Production has 0 such orders today, which is why the
    rule has never been visible.

    Duplicates are NOT removed here. `upsert_orders` drops them keeping the
    last, and doing it twice — once per store, from a different code path —
    would be a second home for a rule about which copy of a repeated order wins.
    """
    from core.models import Order

    orders: List[OrderRow] = []
    products: List[OrderProductRow] = []
    for payload in payloads:
        order = Order.from_api(payload)
        if not order.ordered_at:
            continue
        orders.append(order_row(order))
        if with_products:
            products.extend(order_product_rows(order))
    return LandedOrders(orders=orders, products=products)
