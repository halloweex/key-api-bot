"""The audience query means the same thing in both engines.

`tests/unit/test_sms_segments_dialect.py` proves the two renderings are *one
text*. That is necessary and not sufficient: identical SQL can still mean
different things where the engines' semantics differ — integer division, NULL
ordering, how a window function breaks a tie, what `ROUND` does at .005.

So this runs the real thing. The same synthetic customers go into a DuckDB
store and a live PostgreSQL, both renderings execute, and the rosters are
compared row by row. Skipped without `KS_PG_DSN`, the same contract
`test_bot_database.py` has with its Postgres arm.

The fixture is small and deliberately awkward, because a roster that agrees on
easy data proves nothing. It carries a product name long enough to be
truncated, two buyers sharing one phone number (only the higher-value one may
survive), an opted-out customer, a return, an order on a retired source, and a
b2b order that must be invisible unless `sales_type=all` asks for it.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from core.repositories.customers import SmsAudienceFilters
from core.sql_dialect import DUCKDB, POSTGRES, sms_segments_select

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

LONG = "Очищувальна пінка з екстрактом зеленого чаю та центелою азійською 150мл"

# (id, buyer, date, grand_total, sales_type, is_return, is_active, source, promo)
ORDERS = [
    (1,  1, "2026-08-01", 6000, "retail", False, True, 1, None),
    (2,  1, "2026-07-01", 5000, "retail", False, True, 1, "AUG"),
    (3,  2, "2026-08-10", 12000, "retail", False, True, 2, None),
    (4,  3, "2026-06-01", 3000, "retail", False, True, 1, None),
    (5,  4, "2026-08-15", 900, "retail", False, True, 1, None),   # opted out
    (6,  5, "2026-08-05", 7000, "b2b", False, True, 1, None),     # wrong type
    (7,  6, "2026-08-05", 4000, "retail", True, True, 1, None),   # a return
    (8,  7, "2026-08-05", 4000, "retail", False, False, 3, None), # retired source
    (9,  8, "2026-08-12", 2500, "retail", False, True, 1, None),  # shares a phone
    (10, 9, "2026-08-12", 2600, "retail", False, True, 1, None),  # …and wins it
]
LINES = [
    (1, 1, 100, LONG, 3, 1000), (2, 1, 101, "Тонер", 2, 900),
    (3, 1, 102, "Сироватка", 1, 800), (4, 1, 103, "Патчі", 1, 700),
    (5, 2, 100, LONG, 2, 1200), (6, 3, 101, "Тонер", 5, 1500),
    (7, 4, 102, "Сироватка", 1, 1500),
    (8, 4, 104, "Крем без ціни", 1, 1500),   # no cost row: margin must skip it
    (9, 5, 103, "Патчі", 1, 900), (10, 6, 100, LONG, 4, 1000),
    (11, 7, 101, "Тонер", 2, 1000), (12, 8, 101, "Тонер", 2, 1000),
    (13, 9, 102, "Сироватка", 1, 2500), (14, 10, 102, "Сироватка", 1, 2600),
]
PRODUCTS = [(100, "Cosrx", "SKU-100", 10), (101, "Beauty", "SKU-101", 10),
            (102, "Cosrx", "SKU-102", 11), (103, "Beauty", "SKU-103", 11),
            (104, "Cosrx", "SKU-104", 11)]
CATEGORIES = [(10, "Очищення", None), (11, "Догляд", 10)]
STOCKS = [(1, "SKU-100", 400), (2, "SKU-101", 500), (3, "SKU-102", 900)]
BUYERS = [
    (1, "Анна", "Київ", "+38 (050) 111-11-11"),   # punctuation must normalise
    (2, "Богдан", "Львів", "380502222222"),
    (3, "Віра", "київ", "380503333333"),          # lower case city
    (4, "Галина", "Одеса", "380504444444"),
    (5, "Дмитро", "Київ", "380505555555"),
    (6, "Олена", "Київ", "380506666666"),
    (7, "Женя", "Київ", "380507777777"),
    (8, "Ігор", "Київ", "380508888888"),
    (9, "Ірина", "Київ", "380508888888"),
]
OPTOUTS = [(4, "sms", "380504444444")]

CASES = [
    ("rfm, revenue basis", SmsAudienceFilters(), "revenue_ltv", "retail", (), "rfm"),
    ("single arm, margin basis", SmsAudienceFilters(), "margin_ltv", "retail", (),
     "single"),
    ("brand filter (the content EXISTS)", SmsAudienceFilters(brands=("Cosrx",)),
     "revenue_ltv", "retail", (), "single"),
    ("category matches the whole branch", SmsAudienceFilters(category_ids=(10,)),
     "revenue_ltv", "retail", (), "single"),
    ("city and order count", SmsAudienceFilters(cities=("Київ",), orders_min=2),
     "revenue_ltv", "retail", (), "single"),
    ("one level only", SmsAudienceFilters(), "revenue_ltv", "retail", ("VIP",), "rfm"),
    ("promocode", SmsAudienceFilters(promocode="aug"), "revenue_ltv", "retail", (),
     "single"),
    ("sales_type=all lets b2b in", SmsAudienceFilters(), "revenue_ltv", "all", (),
     "single"),
]


def _render(dialect, filters, ltv_column, sales_type, tiers, grouping):
    tier_case = f"""CASE
        WHEN c.{ltv_column} >= ? THEN 'VIP'
        WHEN c.orders >= ? OR c.{ltv_column} >= ? THEN 'CORE'
        WHEN c.recency <= ? THEN 'REACTIVATION' END"""
    filter_sql, filter_params = filters.predicate(
        ltv_column, sales_type, dialect.order_lines,
    )
    sql = sms_segments_select(
        dialect,
        ltv_column=ltv_column,
        sales_type_filter="" if sales_type == "all" else "AND l.sales_type = ?",
        tier_case=tier_case,
        arm_expr="'ALL'" if grouping == "single" else "tier_level",
        ok_tier_expr="TRUE" if grouping == "single" else "tier_level IS NOT NULL",
        filter_sql=filter_sql,
        tier_subset=(f"WHERE tier_level IN ({', '.join('?' * len(tiers))})"
                     if tiers else ""),
    )
    params: list = []
    if sales_type != "all":
        params.append(sales_type)
    params += [5000.0, 2, 2000.0, 120, 270]
    params += filter_params
    params += list(tiers)
    return sql, params


def _numbered(sql: str) -> str:
    """`?` placeholders renumbered for asyncpg, which wants `$1 … $n`.

    Positional in both drivers, so the order the caller built them in is the
    order they bind in — no mapping to keep in step.
    """
    out, n = [], 0
    for char in sql:
        if char == "?":
            n += 1
            out.append(f"${n}")
        else:
            out.append(char)
    return "".join(out)


def _comparable(rows):
    """Money as float at two decimals: DuckDB hands back DECIMAL, asyncpg
    Decimal, and the roster is the thing being compared, not the type."""
    out = []
    for row in rows:
        out.append(tuple(
            round(float(v), 2) if hasattr(v, "as_tuple") else v for v in row
        ))
    return out


async def _seed_duckdb(store):
    async with store.connection() as conn:
        for pid, brand, sku, cat in PRODUCTS:
            conn.execute("INSERT INTO products (id, name, brand, sku, category_id)"
                         " VALUES (?,?,?,?,?)", [pid, f"p{pid}", brand, sku, cat])
        for cid, name, parent in CATEGORIES:
            conn.execute("INSERT INTO categories (id, name, parent_id)"
                         " VALUES (?,?,?)", [cid, name, parent])
        for oid, sku, cost in STOCKS:
            conn.execute("INSERT INTO offer_stocks (id, sku, purchased_price)"
                         " VALUES (?,?,?)", [oid, sku, cost])
        for bid, name, city, phone in BUYERS:
            conn.execute("INSERT INTO buyers (id, full_name, city, phone)"
                         " VALUES (?,?,?,?)", [bid, name, city, phone])
        for bid, channel, phone in OPTOUTS:
            conn.execute("INSERT INTO marketing_optouts (buyer_id, channel, phone)"
                         " VALUES (?,?,?)", [bid, channel, phone])
        for oid, buyer, day, total, stype, ret, active, src, promo in ORDERS:
            conn.execute(
                "INSERT INTO silver_orders (id, source_id, status_id, grand_total,"
                " ordered_at, buyer_id, manager_id, order_date, is_return,"
                " sales_type, is_active_source, source_name, is_new_customer,"
                " buyer_first_order_date, promocode)"
                " VALUES (?,?,1,?,?,?,NULL,?,?,?,?,?,FALSE,?,?)",
                [oid, src, total, f"{day} 10:00:00+00", buyer, day, ret, stype,
                 active, f"src{src}", day, promo])
        for lid, oid, pid, name, qty, price in LINES:
            conn.execute("INSERT INTO order_products (id, order_id, product_id,"
                         " name, quantity, price_sold) VALUES (?,?,?,?,?,?)",
                         [lid, oid, pid, name, qty, price])


async def _seed_postgres(conn):
    await conn.execute(
        "TRUNCATE silver.orders, bronze.order_products, bronze.products,"
        " bronze.categories, bronze.buyers, bronze.offer_stocks,"
        " app.marketing_optouts")
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
        [(o, src, total,
          datetime.fromisoformat(f"{day} 10:00:00+00:00"), buyer,
          date.fromisoformat(day), ret, stype, active, f"src{src}",
          date.fromisoformat(day), promo)
         for o, buyer, day, total, stype, ret, active, src, promo in ORDERS])
    await conn.executemany(
        "INSERT INTO bronze.order_products (id, order_id, product_id, name,"
        " quantity, price_sold) VALUES ($1,$2,$3,$4,$5,$6)", LINES)


@pytest_asyncio.fixture
async def engines(tmp_path):
    store = DuckDBStore(db_path=tmp_path / "segments.duckdb")
    await store.connect()
    conn = await asyncpg.connect(DSN)
    try:
        await _seed_duckdb(store)
        await _seed_postgres(conn)
        yield store, conn
    finally:
        await conn.close()
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "label,filters,ltv_column,sales_type,tiers,grouping", CASES,
    ids=[c[0] for c in CASES],
)
async def test_both_engines_return_the_same_roster(
    engines, label, filters, ltv_column, sales_type, tiers, grouping,
):
    store, conn = engines

    duck_sql, params = _render(DUCKDB, filters, ltv_column, sales_type, tiers, grouping)
    pg_sql, pg_params = _render(POSTGRES, filters, ltv_column, sales_type, tiers,
                                grouping)

    async with store.connection() as duck:
        duck_rows = duck.execute(duck_sql, params).fetchall()
    pg_rows = [tuple(r) for r in await conn.fetch(_numbered(pg_sql), *pg_params)]

    assert _comparable(duck_rows) == _comparable(pg_rows), (
        f"the two engines disagree about the {label} audience"
    )


@pytest.mark.asyncio
async def test_the_fixture_actually_exercises_the_exclusions(engines):
    """A roster that agrees on easy data proves nothing, so this asserts the
    awkward rows really are being excluded — otherwise the agreement above
    could be two engines both returning everybody."""
    store, _conn = engines
    sql, params = _render(DUCKDB, SmsAudienceFilters(), "revenue_ltv", "retail",
                          (), "single")
    async with store.connection() as duck:
        rows = duck.execute(sql, params).fetchall()

    selected = {r[0] for r in rows if r[0] is not None}
    assert 4 not in selected, "an opted-out customer was messaged"
    assert 5 not in selected, "a b2b order leaked into a retail audience"
    assert 6 not in selected, "a return counted as a purchase"
    assert 7 not in selected, "a retired source counted"
    assert 8 not in selected and 9 in selected, (
        "the shared phone number was not de-duplicated to the higher-value buyer"
    )


@pytest.mark.asyncio
async def test_the_whole_result_matches_through_the_store(engines, monkeypatch):
    """The end-to-end shape, not just the rows.

    Above, both renderings are executed side by side. This drives the real
    `get_sms_segments` twice — once per engine, chosen by `KS_SMS_STORE` — and
    compares what the API would actually return: the level summaries, the
    funnel, the customer rows and the arm each customer landed in.

    The arms are the reason this is worth running separately. They are decided
    in Python now (`core.sms_holdout`) precisely so the engines cannot
    disagree, and an assertion that never looked at them would not notice if
    that stopped being true.
    """
    store, conn = engines

    monkeypatch.delenv("KS_SMS_STORE", raising=False)
    duck = await store.get_sms_segments(
        grouping="single", include_customers=True, campaign="aug-compare",
    )

    monkeypatch.setenv("KS_SMS_STORE", "postgres")
    with patch("core.pg.get_pool", new=AsyncMock(return_value=_PoolOf(conn))), \
         patch("core.pg.require_revision", new=AsyncMock()):
        postgres = await store.get_sms_segments(
            grouping="single", include_customers=True, campaign="aug-compare",
        )

    assert duck["funnel"] == postgres["funnel"]
    assert duck["segments"] == postgres["segments"]
    assert duck["totals"] == postgres["totals"]
    assert [c["buyerId"] for c in duck["customers"]] == \
           [c["buyerId"] for c in postgres["customers"]]
    assert [c["assignment"] for c in duck["customers"]] == \
           [c["assignment"] for c in postgres["customers"]]
    assert duck["customers"] == postgres["customers"]


class _PoolOf:
    """The one connection the fixture already opened, shaped like a pool."""

    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()
