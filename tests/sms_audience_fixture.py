"""Customers for the audience-group tests, each the answer to one question.

The owner's campaign is the shape these rows are built around: "two or more
purchases since a date" OR "a purchase in the last 92 days". Every buyer here
either belongs to exactly the groups written beside it or is excluded by one
named rule, so a test can state its expected set by hand rather than by
re-running the query it is testing.

Dates are counted back from **today in Kyiv**, because that is the day the
query counts from (`TODAY_IN_KYIV`). A fixture written against the laptop's
`date.today()` and a store reading Kyiv's calendar disagrees for three hours
every night, and a test of a 92-day edge is exactly the test that notices.

And today is read **when it is used** — when a row is seeded, an answer
computed or a case's filters built — never at import. The query reads its
today when it runs, and CI imports this module at collection, half an hour
before it reaches these tests: a suite started at 23:47 in Kyiv and reaching
them at 00:11 seeded every row a day older than the query thought, and buyer
12, at exactly 92 days, left the 92-day window (PR #305's run 39494883413).
So nothing below holds a date; whatever needs one asks `kyiv_today()`, and
`tests/unit/test_sms_audience_fixture_clock.py` loads this module and its
tests under a clock that crosses midnight to prove it.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

KYIV = ZoneInfo("Europe/Kyiv")


def kyiv_today() -> date:
    return datetime.now(KYIV).date()


def ago(days: int) -> date:
    return kyiv_today() - timedelta(days=days)


def period_from() -> date:
    """"Since" for group A: 200 days back, clear of any year boundary."""
    return ago(200)


# (buyer, name, city, phone)
BUYERS = [
    (1, "A only", "Київ", "380500000001"),
    (2, "B only", "Львів", "380500000002"),
    (3, "Both", "Київ", "380500000003"),
    (4, "Neither", "Львів", "380500000004"),
    (5, "Return does not count", "Київ", "380500000005"),
    (6, "Retired source does not count", "Львів", "380500000006"),
    (7, "B2B does not count in retail", "Київ", "380500000007"),
    (8, "A only, loses its phone", "Львів", "380500000089"),
    (9, "B only, wins the shared phone", "Київ", "380500000089"),
    (10, "Both, opted out", "Київ", "380500000010"),
    (11, "B, phone unusable", "Київ", "12345"),
    (12, "B, exactly 92 days", "Львів", "380500000012"),
    (13, "Not B, 93 days", "Київ", "380500000013"),
    (14, "A, last order 185 days ago", "Львів", "380500000014"),
    # Older than any window up to two years: only an "all time" window
    # (3 650 days) reaches it. Outside every group above, so no answer below
    # moves for it.
    (15, "Lapsed, one order 1000 days ago", "Київ", "380500000015"),
]

# (order, buyer, days ago, total, sales_type, is_return, is_active, source, promo)
ORDERS = [
    (101, 1, 150, 1000, "retail", False, True, 1, None),
    (102, 1, 120, 1000, "retail", False, True, 1, "AUG"),
    (201, 2, 400, 1000, "retail", False, True, 4, None),
    (202, 2, 30, 1000, "retail", False, True, 4, None),
    (301, 3, 150, 1000, "retail", False, True, 1, None),
    (302, 3, 10, 1000, "retail", False, True, 2, None),
    (401, 4, 300, 1000, "retail", False, True, 1, None),
    (402, 4, 250, 1000, "retail", False, True, 1, None),
    (501, 5, 150, 1000, "retail", False, True, 1, None),
    (502, 5, 140, 1000, "retail", True, True, 1, None),
    (601, 6, 150, 1000, "retail", False, True, 1, None),
    (602, 6, 130, 1000, "retail", False, False, 3, None),
    (701, 7, 150, 1000, "retail", False, True, 1, None),
    (702, 7, 130, 1000, "b2b", False, True, 1, None),
    (801, 8, 160, 500, "retail", False, True, 1, None),
    (802, 8, 150, 500, "retail", False, True, 1, None),
    (901, 9, 20, 3000, "retail", False, True, 1, None),
    (1001, 10, 150, 1000, "retail", False, True, 1, None),
    (1002, 10, 5, 1000, "retail", False, True, 1, None),
    (1101, 11, 15, 1000, "retail", False, True, 1, None),
    (1201, 12, 92, 1000, "retail", False, True, 1, None),
    (1301, 13, 93, 1000, "retail", False, True, 1, None),
    (1401, 14, 190, 1000, "retail", False, True, 1, None),
    (1402, 14, 185, 1000, "retail", False, True, 1, "AUG"),
    (1501, 15, 1000, 1000, "retail", False, True, 1, None),
]

# Every order has one line. Even orders are product 101 (another brand and a
# category of its own), so a content filter splits the base.
PRODUCTS = [(100, "Cosrx", "SKU-100", 11), (101, "Anua", "SKU-101", 20)]
CATEGORIES = [(10, "Face care", None), (11, "Masks", 10), (20, "Hair", None)]
STOCKS = [(1, "SKU-100", 400)]
OPTOUTS = [(10, "sms", "380500000010")]


def _lines():
    for oid, _buyer, _days, total, *_ in ORDERS:
        yield (oid, oid, 100 if oid % 2 else 101, f"Product {oid}", 1, total)


# ─── The answers ────────────────────────────────────────────────────────────
#
# Group A: at least two orders since period_from(), window 365.
# Group B: last order within 92 days.

A_MATCHED = {1, 3, 8, 10, 14}                     # its conditions, before phones
B_MATCHED = {2, 3, 9, 10, 11, 12}
A_ALONE = {1, 3, 8, 14}                           # 10 opted out
B_ALONE = {2, 3, 9, 12}                           # 10 opted out, 11 no phone
# 8 and 9 share a number and 9 is worth more, so in the union 9 takes it —
# and the number is reached through both groups, A by 8 and B by 9. Each
# group's share is counted in phones, so it is what the group reaches alone.
UNION = {1, 2, 3, 9, 12, 14}
A_RECIPIENTS = len(A_ALONE)        # 1, 3, 14 and the shared number
B_RECIPIENTS = len(B_ALONE)        # 2, 3, 12 and the shared number
A_ONLY = 2                         # 1, 14
B_ONLY = 2                         # 2, 12


# ─── An oracle for the rest ─────────────────────────────────────────────────
#
# The sets above are written by hand. The period's shapes and bounds have too
# many combinations for that, so these answer them from the rows themselves,
# in Python, with nothing of the query in them: the orders the audience counts,
# a group's own conditions, and the rules common to the campaign. Each case
# that uses them also states its answer by hand, so a mistake here and the
# same mistake in the SQL cannot pass together.

def counted_orders(sales_type: str = "retail") -> dict:
    """buyer → [(order_id, order_date, total)] of the orders the audience
    counts: not returns, not a retired source, the campaign's sales type."""
    out: dict = {}
    for oid, buyer, days, total, stype, ret, active, _src, _promo in ORDERS:
        if ret or not active or (sales_type != "all" and stype != sales_type):
            continue
        out.setdefault(buyer, []).append((oid, ago(days), total))
    return out


def matched(window: int, *, in_period=None, at_least=None, at_most=None,
            sales_type: str = "retail") -> set:
    """The buyers one group's own conditions select: last order inside
    `window` days and, when `in_period` (a predicate over a date) is given,
    between `at_least` and `at_most` of their orders dated in it — at least
    one when neither bound is set, no floor when only `at_most` is."""
    if in_period is not None and at_least is None and at_most is None:
        at_least = 1
    out = set()
    for buyer, orders in counted_orders(sales_type).items():
        if (kyiv_today() - max(d for _, d, _ in orders)).days > window:
            continue
        if in_period is not None:
            n = sum(1 for _, d, _ in orders if in_period(d))
            if at_least is not None and n < at_least:
                continue
            if at_most is not None and n > at_most:
                continue
        out.add(buyer)
    return out


def recipients(buyers, sales_type: str = "retail") -> set:
    """What the rules common to the campaign leave of `buyers`: a full
    Ukrainian number, not opted out (by buyer or by number), and one buyer per
    number — the one worth more, then the lower id. Revenue basis: every
    order here has one line, so a buyer's value is the sum of their totals."""
    phones = {b: "".join(ch for ch in phone if ch.isdigit())
              for b, _name, _city, phone in BUYERS}
    opted_buyers = {b for b, _channel, _phone in OPTOUTS}
    opted_phones = {p for _b, _channel, p in OPTOUTS}
    value = {b: sum(t for _, _, t in orders)
             for b, orders in counted_orders(sales_type).items()}
    usable = [b for b in buyers
              if len(phones[b]) == 12 and phones[b].startswith("380")
              and b not in opted_buyers and phones[b] not in opted_phones]
    winners: dict = {}
    for b in sorted(usable, key=lambda b: (-value[b], b)):
        winners.setdefault(phones[b], b)
    return set(winners.values())


def range_from() -> date:
    """"From 200 to 100 days ago": the range's first day."""
    return ago(200)


def range_to() -> date:
    return ago(100)


# The period's shapes, its bounds and the widest window, one group each:
# (label, window, the group's filter fields, the oracle's terms, the answer by
# hand). Shared by the store tests and the two-engine ones so both ask the
# same questions. Dates are the oracle's own, so a case cannot drift from it.
# The fields are a function, called when the test runs: pytest builds this
# table at collection, and a date in it would be the collection's.
PERIOD_CASES = [
    # 3 and 10 have two orders since 200 days ago but one of them after the
    # range ends — `period_to` is what takes them out. 8 keeps its number,
    # because 9, who shares it, is not in this audience.
    ("from and to, at least two", 365,
     lambda: dict(period_orders_min=2, period_from=range_from(),
                  period_to=range_to()),
     dict(in_period=lambda d: range_from() <= d <= range_to(), at_least=2),
     {1, 8, 14}),
    # Every order up to a date: 2 (400 days), 4 (300 and 250) and 15 (1000),
    # which only an all-time window holds.
    ("up to a date", 3650,
     lambda: dict(period_to=period_from()),
     dict(in_period=lambda d: d <= period_from()),
     {2, 4, 15}),
    # Exactly one order in 200 days: min = max. 11 has one too and no usable
    # phone; 1, 3, 8, 10 and 14 have two.
    ("exactly one in 200 days", 365,
     lambda: dict(period_orders_min=1, period_orders_max=1, period_days=200),
     dict(in_period=lambda d: (kyiv_today() - d).days <= 200, at_least=1,
          at_most=1),
     {2, 5, 6, 7, 9, 12, 13}),
    # Bought, but nothing since the date: 4 and 15 bought only before it;
    # 2 and 14 bought before it and in it, so they are out.
    ("none since a date, all time", 3650,
     lambda: dict(period_orders_max=0, period_from=period_from()),
     dict(in_period=lambda d: d >= period_from(), at_most=0),
     {4, 15}),
    # The same within two years: the window still cuts a group, so 15 goes.
    ("none since a date, two years", 730,
     lambda: dict(period_orders_max=0, period_from=period_from()),
     dict(in_period=lambda d: d >= period_from(), at_most=0),
     {4}),
    # Nothing in the last 92 days — 12, at exactly 92, is in the period and
    # so out; 13 at 93 is in. 8 keeps the shared number: 9 bought 20 days ago.
    ("none in 92 days", 3650,
     lambda: dict(period_orders_max=0, period_days=92),
     dict(in_period=lambda d: (kyiv_today() - d).days <= 92, at_most=0),
     {1, 4, 5, 6, 7, 8, 13, 14, 15}),
    # All time and no condition: everyone reachable, 15 included.
    ("all time", 3650, lambda: {}, {}, {1, 2, 3, 4, 5, 6, 7, 9, 12, 13, 14, 15}),
]


async def seed_postgres(conn) -> None:
    await conn.execute(
        "TRUNCATE silver.orders, bronze.order_products, bronze.products,"
        " bronze.categories, bronze.buyers, bronze.offer_stocks,"
        " app.marketing_optouts, app.buyer_gender, app.sms_campaigns,"
        " app.sms_campaign_members, app.sms_audience_presets, app.sms_dlr_events")
    await conn.executemany(
        "INSERT INTO bronze.categories (id, name, parent_id) VALUES ($1,$2,$3)",
        CATEGORIES)
    await conn.executemany(
        "INSERT INTO bronze.products (id, name, brand, sku, category_id)"
        " VALUES ($1,$2,$3,$4,$5)",
        [(p, f"p{p}", b, s, c) for p, b, s, c in PRODUCTS])
    await conn.executemany(
        "INSERT INTO bronze.offer_stocks (id, sku, purchased_price)"
        " VALUES ($1,$2,$3)", STOCKS)
    await conn.executemany(
        "INSERT INTO bronze.buyers (id, full_name, city, phone)"
        " VALUES ($1,$2,$3,$4)", BUYERS)
    await conn.executemany(
        "INSERT INTO app.marketing_optouts (buyer_id, channel, phone)"
        " VALUES ($1,$2,$3)", OPTOUTS)
    await conn.executemany(
        "INSERT INTO silver.orders (id, source_id, status_id, grand_total,"
        " ordered_at, buyer_id, manager_id, order_date, is_return, sales_type,"
        " is_active_source, source_name, is_new_customer,"
        " buyer_first_order_date, promocode)"
        " VALUES ($1,$2,1,$3,$4,$5,NULL,$6,$7,$8,$9,$10,FALSE,$11,$12)",
        [(oid, src, total,
          datetime.fromisoformat(f"{ago(days).isoformat()} 10:00:00+00:00"), buyer,
          ago(days), ret, stype, active, f"src{src}", ago(days), promo)
         for oid, buyer, days, total, stype, ret, active, src, promo in ORDERS])
    await conn.executemany(
        "INSERT INTO bronze.order_products (id, order_id, product_id, name,"
        " quantity, price_sold) VALUES ($1,$2,$3,$4,$5,$6)", list(_lines()))
