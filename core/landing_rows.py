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

import re
from datetime import date, datetime
from typing import (
    Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Tuple,
    TYPE_CHECKING,
)

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


# ── offers and their stock, and the one computation that is not a parse ──────
#
# `offers` is an ordinary payload read and looks like its neighbours above.
# `plan_stock_movements` is the exception in this module and the reason it is
# here anyway: it is not a reading of a payload, it is a reading of the payload
# *against the previous state*, and it is the single most dangerous rule in the
# inventory chain to have two copies of.
#
# A stock movement exists nowhere else. KeyCRM serves current stock only — it
# has no history endpoint — so a movement that is not computed at the moment it
# happens is gone, and no backfill can ever recover it. That is why the whole
# of stage 4 puts this chain first. Two implementations of the classification
# would not fail loudly; they would disagree about whether a change was
# `stock_in`, `stock_out` or `reserve_change`, and the wrong answer would be
# written into the only record there will ever be.
#
# So: previous state in, payload in, movements out. No connection, no store, no
# SQL. Whichever engine holds the previous state reads it and hands it here.


class OfferRow(NamedTuple):
    """An offer as KeyCRM serves it: which product it belongs to, and its SKU."""
    id: int
    product_id: int
    sku: Optional[str]


class StockMovement(NamedTuple):
    """One change to one offer's stock, classified.

    `product_id` is denormalised onto the movement rather than joined at read
    time, and it is Optional because it comes from a lookup that can miss: an
    offer whose `offers` row has not arrived yet produces a movement with no
    product. That is the failure mode which makes the ordering inside this
    chain matter — see `plan_stock_movements`.
    """
    offer_id: int
    product_id: Optional[int]
    movement_type: str
    quantity_before: int
    quantity_after: int
    delta: int
    reserve_before: int
    reserve_after: int


OFFER_COLUMNS = OfferRow._fields
STOCK_MOVEMENT_COLUMNS = StockMovement._fields

# The four verdicts, named once. `initial` is not a change — it is the first
# time this store ever saw the offer carrying stock.
MOVEMENT_INITIAL = "initial"
MOVEMENT_IN = "stock_in"
MOVEMENT_OUT = "stock_out"
MOVEMENT_RESERVE = "reserve_change"


def offer_row(payload: Dict[str, Any]) -> OfferRow:
    return OfferRow(
        id=payload.get("id"),
        product_id=payload.get("product_id"),
        sku=payload.get("sku"),
    )


def offer_rows(payloads: List[Dict[str, Any]]) -> List[OfferRow]:
    return [offer_row(p) for p in payloads]


def plan_stock_movements(
    stocks: List[Dict[str, Any]],
    *,
    current: Mapping[int, Tuple[int, int]],
    product_map: Mapping[int, Optional[int]],
) -> List[StockMovement]:
    """Which movements this batch of stock levels implies.

    Args:
        stocks: the KeyCRM `offers/stocks` payload.
        current: offer_id -> (quantity, reserve) as the store holds it *now*.
            This is the delta base and it exists in exactly one place; an
            empty or stale mapping does not fail, it re-labels every changed
            offer as `initial` against a zero base, which is why the caller
            must read it inside the same transaction it writes.
        product_map: offer_id -> product_id. A miss yields None and the
            movement is still recorded — losing the movement would be worse
            than losing the denormalised product, and the column is nullable
            for exactly that reason.

    Behaviour is preserved verbatim from the DuckDB implementation, including
    the two edges worth naming:

      * a brand-new offer with **no** stock produces no movement at all. It is
        not interesting and it would otherwise write one row per new SKU per
        catalogue sync.
      * a change with `delta == 0` is a `reserve_change`. Quantity did not
        move, so calling it `stock_in` or `stock_out` would put a zero into a
        column that is read as a direction.
    """
    movements: List[StockMovement] = []
    for stock in stocks:
        offer_id = stock.get("id")
        new_qty = stock.get("quantity", 0)
        new_rsv = stock.get("reserve", 0)
        old = current.get(offer_id)
        pid = product_map.get(offer_id)

        if old is None:
            if new_qty > 0 or new_rsv > 0:
                movements.append(StockMovement(
                    offer_id, pid, MOVEMENT_INITIAL,
                    0, new_qty, new_qty, 0, new_rsv,
                ))
        elif old[0] != new_qty or old[1] != new_rsv:
            delta = new_qty - old[0]
            if delta != 0:
                mtype = MOVEMENT_OUT if delta < 0 else MOVEMENT_IN
            else:
                mtype = MOVEMENT_RESERVE
            movements.append(StockMovement(
                offer_id, pid, mtype,
                old[0], new_qty, delta, old[1], new_rsv,
            ))
    return movements


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


# ─── Expenses ─────────────────────────────────────────────────────────────────
#
# The order-level costs KeyCRM serves with `include=expenses`, and the small
# dictionary that names their kinds. Both land in two stores, so both are
# parsed once here.
#
# `expense_types` is the one place in this module where the parse is a real
# transformation rather than a lookup: KeyCRM returns some names as
# localisation keys — `dictionaries.expense_types.delivery` — and the display
# name is built from the alias instead. A mirror that re-read the payload
# would either repeat that rule or drop it, and the two stores would then
# disagree about what an expense is *called*, which is exactly the class of
# difference the comparison would report every morning without being able to
# say which store was right.


class ExpenseTypeRow(NamedTuple):
    """An expense type as KeyCRM describes it, after the name is made human.
    Column order is the write order."""
    id: Optional[int]
    name: str
    alias: Optional[str]
    is_active: bool


class ExpenseRow(NamedTuple):
    """One order-level expense. Column order is the write order.

    `order_id` is not in the payload — it is the order the expense was served
    under — so it is passed in rather than read out.
    """
    id: Optional[int]
    order_id: Optional[int]
    expense_type_id: Optional[int]
    amount: float
    description: Optional[str]
    status: Optional[str]
    # Parsed, not passed through: KeyCRM sends ISO strings and asyncpg needs
    # datetimes. See `_keycrm_datetime`.
    payment_date: Optional[datetime]
    created_at: Optional[datetime]


EXPENSE_TYPE_COLUMNS = ExpenseTypeRow._fields
EXPENSE_COLUMNS = ExpenseRow._fields

_LOCALISATION_PREFIX = "dictionaries.expense_types."


def _keycrm_datetime(value: Any) -> Optional[datetime]:
    """A KeyCRM timestamp as a datetime, or None.

    THE BUG THIS EXISTS FOR

    KeyCRM serves these as ISO strings — `2026-09-07T14:31:22.000000Z`. DuckDB
    accepts the string and parses it on the way in; asyncpg does not, and
    raises `invalid input for query argument ... expected a datetime`. The
    first cut of `expense_row` passed the payload value straight through, so
    DuckDB was written and the mirror failed on all 66 rows of the first
    batch — caught within the minute by the failure watermark rather than the
    next morning, which is what that watermark is for.

    That is exactly the class "one parse, two stores" exists to prevent: a
    parse is not finished until it has produced a value *both* stores accept.

    `.replace("Z", "+00:00")` and the swallowed error are `core/models.py`'s
    handling of the same field, copied deliberately rather than improved: an
    unparseable stamp becomes NULL in both stores instead of failing the batch
    that carries it.

    ONE RESIDUAL DIFFERENCE, LEFT ALONE ON PURPOSE

    A KeyCRM stamp with no zone parses to a naive datetime, and the two stores
    then disagree about what it means: asyncpg reads naive as UTC, DuckDB reads
    it in the session timezone, which is `Europe/Kyiv` in these containers —
    three hours apart. Every value observed on production carries `Z`, so this
    is hypothetical; normalising it would change what DuckDB stores today on
    the strength of that hypothesis, and the daily comparison reports a
    three-hour difference loudly if it ever stops being one.
    """
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def expense_type_row(payload: Dict[str, Any]) -> ExpenseTypeRow:
    """One KeyCRM expense-type payload as a row.

    Preserved exactly, including the fallback chain: a localisation key
    becomes the alias in Title Case, and without an alias it becomes the key's
    own tail in Title Case. `is_active` defaults to True, which is what a
    dictionary entry KeyCRM bothered to return should be.
    """
    name = payload.get("name", "Unknown")
    alias = payload.get("alias")
    if name.startswith(_LOCALISATION_PREFIX):
        if alias:
            name = alias.replace("_", " ").title()
        else:
            name = name.replace(_LOCALISATION_PREFIX, "").replace("_", " ").title()
    return ExpenseTypeRow(
        id=payload.get("id"),
        name=name,
        alias=alias,
        is_active=payload.get("is_active", True),
    )


def expense_row(order_id: Optional[int], payload: Dict[str, Any]) -> ExpenseRow:
    """One KeyCRM expense payload as a row, under the order that carried it."""
    return ExpenseRow(
        id=payload.get("id"),
        order_id=order_id,
        expense_type_id=payload.get("expense_type_id"),
        # `0`, not None: the column is NOT NULL in both stores, and an expense
        # KeyCRM served without an amount is a zero-cost expense rather than a
        # write that fails the whole batch.
        amount=payload.get("amount", 0),
        description=payload.get("description"),
        status=payload.get("status"),
        payment_date=_keycrm_datetime(payload.get("payment_date")),
        created_at=_keycrm_datetime(payload.get("created_at")),
    )


def expense_type_rows(payloads: List[Dict[str, Any]]) -> List[ExpenseTypeRow]:
    return [expense_type_row(p) for p in payloads]


def expense_rows(orders_with_expenses: List[Dict[str, Any]]) -> List[ExpenseRow]:
    """Every expense across a batch of orders, flattened.

    The input is the shape the sync already has — orders each carrying an
    `expenses` list — so the flattening lives here rather than being repeated
    by each store.
    """
    rows: List[ExpenseRow] = []
    for order in orders_with_expenses:
        order_id = order.get("id")
        for payload in order.get("expenses", []) or []:
            rows.append(expense_row(order_id, payload))
    return rows


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


# ─────────────────────────────────────────────────────────────────────────────
# Buyers
# ─────────────────────────────────────────────────────────────────────────────
#
# The third landing table to get one reading, and the one where the two
# readings had actually drifted. Each writer used to take the Buyer model and
# render it for itself: DuckDB was handed the raw `birthday` string and cast it
# on the way in, the Postgres mirror cut it with `date.fromisoformat(v[:10])`.
# So `1990-1-5` landed in DuckDB and failed the mirror, and a value DuckDB
# refuses — `''`, `'0000-00-00'`, `'1990-02-30'` — did worse than that: it
# rolled back the whole buyer batch, left the watermark where it was, and the
# next tick fetched the same batch and failed the same way, once a minute. The
# sync tick catches only KeyCRM errors, so every one of those minutes also cost
# the offers and stocks syncs that run after buyers. No production buyer has
# carried such a value yet; the path was measured, not observed.
#
# Now both stores are handed the SAME parsed row. That makes them agree by
# construction, whatever the parser decides; what the matrix test against
# DuckDB's own CAST protects is that the parser changes no birthday DuckDB
# accepted before.


class BuyerRow(NamedTuple):
    """A buyer as both stores hold it, bookkeeping aside. Column order is the
    write order, and `BUYER_COLUMNS` is the contract the daily comparison reads."""
    id: Optional[int]
    full_name: str
    birthday: Optional[date]
    note: Optional[str]
    phone: Optional[str]
    email: Optional[str]
    manager_id: Optional[int]
    company_id: Optional[int]
    company_name: Optional[str]
    city: Optional[str]
    region: Optional[str]
    loyalty_program_name: Optional[str]
    loyalty_level_name: Optional[str]
    loyalty_discount: Optional[float]
    loyalty_amount: Optional[float]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class ContactRow(NamedTuple):
    """One phone or email. Postgres keys these on the natural triple and has no
    surrogate id, so the triple is the whole identity."""
    buyer_id: Optional[int]
    contact_type: str
    value: str
    is_primary: bool


BUYER_COLUMNS = BuyerRow._fields
CONTACT_COLUMNS = ContactRow._fields

# `core.models.Buyer.from_api` already writes this for a missing or empty name;
# it is the one spelling a nameless buyer has in either store.
UNKNOWN_NAME = "Unknown"

# DuckDB 1.5.5's date grammar, as far as it matters here, measured against its
# own CAST: an optional run of whitespace, a year of one to four digits, one
# separator out of - / \ or space used twice, a month and a day of one or two
# digits, and then anything at all that does not start with another digit —
# `1990-01-05T10:00`, `1990-01-05 garbage` and `1990-1-5-` are all 5 January
# 1990, while `1990-01-051` is refused, which is also why a day-first
# `05-01-1990` never reads as the year 5.
_DATE_HEAD = re.compile(r"(\d{1,4})([-/\\ ])(\d{1,2})\2(\d{1,2})(?!\d)")
# The one tail that changes the meaning: DuckDB reads it as Before Christ.
_BC_TAIL = re.compile(r"\s*\(BC\)")


def _clean(value: Any) -> Any:
    """Text without U+0000.

    DuckDB stores a NUL inside a string; Postgres refuses the whole statement.
    So one such character used to land in DuckDB and fail that buyer's mirror
    on every attempt, forever, and under chain 4 it would fail the only writer.
    Nothing a customer meant is lost by dropping it.
    """
    return value.replace("\x00", "") if isinstance(value, str) else value


def _as_date(value: Any) -> Optional[date]:
    """A birthday as a date, or None when it cannot be one.

    Everything DuckDB's CAST accepts becomes the date DuckDB would have made —
    except a date DuckDB can hold and Python cannot (a year of 0, a BC year,
    a year past 9999), which becomes None: asyncpg could not have written it
    either. Everything DuckDB refuses becomes None instead of an exception, so
    one unreadable birthday costs that birthday and nothing else.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _clean(str(value)).lstrip()
    head = _DATE_HEAD.match(text)
    if head is None or _BC_TAIL.match(text, head.end()):
        return None
    try:
        return date(int(head.group(1)), int(head.group(3)), int(head.group(4)))
    except ValueError:  # 1990-02-30, month 13, year 0
        return None


def birthday_is_unreadable(value: Any) -> bool:
    """True when KeyCRM sent a birthday and it could not be read.

    Distinct from "no birthday": None means KeyCRM had nothing to say, and is
    not worth a warning."""
    return value is not None and _as_date(value) is None


def _as_ts(value: Any) -> Optional[datetime]:
    """A timestamp, strictly.

    `Buyer.from_api` has already parsed these, so anything still a string here
    is a value DuckDB would store and a None would contradict — raising is the
    honest answer, and `test_an_unparseable_timestamp_raises_rather_than_writing_none`
    pins it."""
    if value is None or isinstance(value, datetime):
        return value
    # KeyCRM serialises as ISO with either 'T' or a space; both parse.
    return datetime.fromisoformat(str(value).replace(" ", "T"))


def _name(value: Any) -> str:
    """A name, or `UNKNOWN_NAME` when there is nothing but whitespace.

    `Buyer.from_api` already turns a missing or empty name into it; a name of
    spaces, or of NULs alone, slipped past. It matters beyond tidiness: the
    buyer selection treats an empty name as "not synced yet" and fetches it
    again every hour, and the copy-back refuses a blank name outright."""
    cleaned = _clean(value)
    if cleaned is None or not str(cleaned).strip():
        return UNKNOWN_NAME
    return cleaned


def buyer_row(buyer: Any) -> BuyerRow:
    """One Buyer model rendered as the row both stores agree on."""
    return BuyerRow(
        id=buyer.id,
        full_name=_name(buyer.full_name),
        birthday=_as_date(buyer.birthday),
        note=_clean(buyer.note),
        phone=_clean(buyer.phone),
        email=_clean(buyer.email),
        manager_id=buyer.manager_id,
        company_id=buyer.company_id,
        company_name=_clean(buyer.company_name),
        city=_clean(buyer.city),
        region=_clean(buyer.region),
        loyalty_program_name=_clean(buyer.loyalty_program_name),
        loyalty_level_name=_clean(buyer.loyalty_level_name),
        loyalty_discount=buyer.loyalty_discount,
        loyalty_amount=buyer.loyalty_amount,
        created_at=_as_ts(buyer.created_at),
        updated_at=_as_ts(buyer.updated_at),
    )


def contact_rows(buyer: Any) -> List[ContactRow]:
    """The contact list exactly as it has always been written.

    First occurrence wins and carries its own `i == 0` flag; empties are
    skipped; duplicates collapse on the natural key. So `['', 'p', 'p']` is ONE
    row, `p`, and NOT primary — the empty string sat in position 0. That reads
    like a bug and is pinned as it stands: both stores have always written it
    this way, and changing it would make every such buyer a difference the
    daily comparison reports against the other store."""
    rows: Dict[Tuple[Any, str, str], bool] = {}
    for kind, values in (("phone", buyer.phones), ("email", buyer.emails)):
        for i, value in enumerate(values or []):
            value = _clean(value)
            if value:
                rows.setdefault((buyer.id, kind, value), i == 0)
    return [ContactRow(b, k, v, p) for (b, k, v), p in rows.items()]


class ParsedBuyers(NamedTuple):
    """A batch, parsed once, for however many stores want it."""
    rows: List[BuyerRow]
    contacts: List[Tuple[Any, List[ContactRow]]]
    # Ids only. The value itself is not logged: it is a date of birth, and
    # where it is garbage it may be somebody's phone number typed into the
    # wrong field.
    unreadable_birthdays: List[Any]


def parse_buyers(buyers: Iterable[Any]) -> ParsedBuyers:
    """Every buyer in the batch as rows and contacts, plus which birthdays
    could not be read. Pure: the writer decides how loudly to say so."""
    rows: List[BuyerRow] = []
    contacts: List[Tuple[Any, List[ContactRow]]] = []
    unreadable: List[Any] = []
    for buyer in buyers:
        rows.append(buyer_row(buyer))
        contacts.append((buyer.id, contact_rows(buyer)))
        if birthday_is_unreadable(buyer.birthday):
            unreadable.append(buyer.id)
    return ParsedBuyers(rows, contacts, unreadable)
