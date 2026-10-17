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
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

KYIV_TODAY = datetime.now(ZoneInfo("Europe/Kyiv")).date()

# "Since" for group A: 200 days back, clear of any year boundary.
PERIOD_FROM = KYIV_TODAY - timedelta(days=200)


def ago(days: int) -> date:
    return KYIV_TODAY - timedelta(days=days)


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
# Group A: at least two orders since PERIOD_FROM, window 365.
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


def seed_duckdb_conn(conn) -> None:
    for pid, brand, sku, cat in PRODUCTS:
        conn.execute("INSERT INTO products (id, name, brand, sku, category_id)"
                     " VALUES (?,?,?,?,?)", [pid, f"p{pid}", brand, sku, cat])
    for cid, name, parent in CATEGORIES:
        conn.execute("INSERT INTO categories (id, name, parent_id) VALUES (?,?,?)",
                     [cid, name, parent])
    for oid, sku, cost in STOCKS:
        conn.execute("INSERT INTO offer_stocks (id, sku, purchased_price)"
                     " VALUES (?,?,?)", [oid, sku, cost])
    for bid, name, city, phone in BUYERS:
        conn.execute("INSERT INTO buyers (id, full_name, city, phone)"
                     " VALUES (?,?,?,?)", [bid, name, city, phone])
    for bid, channel, phone in OPTOUTS:
        conn.execute("INSERT INTO marketing_optouts (buyer_id, channel, phone)"
                     " VALUES (?,?,?)", [bid, channel, phone])
    for oid, buyer, days, total, stype, ret, active, src, promo in ORDERS:
        day = ago(days)
        conn.execute(
            "INSERT INTO silver_orders (id, source_id, status_id, grand_total,"
            " ordered_at, buyer_id, manager_id, order_date, is_return,"
            " sales_type, is_active_source, source_name, is_new_customer,"
            " buyer_first_order_date, promocode)"
            " VALUES (?,?,1,?,?,?,NULL,?,?,?,?,?,FALSE,?,?)",
            [oid, src, total, f"{day.isoformat()} 10:00:00+00", buyer, day, ret,
             stype, active, f"src{src}", day, promo])
    for line in _lines():
        conn.execute("INSERT INTO order_products (id, order_id, product_id, name,"
                     " quantity, price_sold) VALUES (?,?,?,?,?,?)", list(line))


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
