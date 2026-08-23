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

from typing import Any, Dict, List, NamedTuple, Optional

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
