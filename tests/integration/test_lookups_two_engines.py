"""The filter bar means the same thing in DuckDB and Postgres.

`tests/unit/test_pg_lookups_read.py` proves the bodies are one text and that
nothing goes round the router. Necessary and not sufficient: identical SQL
still means different things where the engines differ — and for four queries
whose whole output is *an ordered list of strings*, the difference that
matters is **collation**.

THE FIXTURE IS DELIBERATELY AWKWARD

Three of the four end in `ORDER BY` on a text column, and that order reaches
the user as the order of a dropdown. PostgreSQL sorts by the database
collation, DuckDB by its own default, and the two agree only while both are
byte order — measured on production 2026-09-12 (`datcollate = C`, DuckDB
1.5.5 binary). So the brands here are chosen to separate byte order from
locale order rather than to look like a catalogue: mixed case, a leading
apostrophe, a diaeresis and Cyrillic in one list. Under a locale collation
`abib` sorts beside `Abib`; under byte order every capital precedes every
lowercase, and this test fails the day the database is restored under a
different locale.

It also carries the two rows that make the unknown-brand bucket interesting:
`NULL` and a brand of nothing but spaces. `_BRANDS_SQL` keeps the spaces
(`brand != ''` is true of `'   '`) while `_UNBRANDED_EXISTS_SQL` counts it as
unbranded (`TRIM(brand) = ''`), so the same product appears in the list *and*
summons the bucket. That is existing behaviour, and the point here is that
both engines reproduce it rather than that it is right.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

from core.duckdb_store import DuckDBStore

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

# (id, name, parent_id) — a root with children, a childless root, one two deep.
CATEGORIES = [
    (1, "Care", None),
    (2, "Serums", 1),
    (3, "Ampoules", 1),
    (4, "Makeup", None),      # childless: an empty list is an answer, not a fault
    (5, "Night serums", 2),   # two deep
]

# (id, name, category_id, brand, sku, price) — brands picked to separate byte
# order from locale order, plus the two unbranded shapes.
PRODUCTS = [
    (100, "Serum A",    2, "Abib",    "SKU-A", 500.0),
    (200, "Serum B",    2, "ANUA",    "SKU-B", 400.0),
    (300, "Cushion",    4, "A'pieu",  "SKU-C", 300.0),
    (400, "Toner",      3, "abib",    "SKU-D", 250.0),   # lowercase twin
    (500, "Cream",      4, "Ünique",  "SKU-E", 700.0),   # diaeresis
    (600, "Mask",       3, "Ампула",  "SKU-F", 150.0),   # Cyrillic
    (700, "No brand",   4, None,      "SKU-G", 100.0),   # NULL
    (800, "Spaces",     4, "   ",     "SKU-H", 120.0),   # in the list AND unbranded
    (900, "Empty",      4, "",        "SKU-I", 130.0),   # excluded from the list
]

# (order_id, source_id, grand_total, days_ago, promocode)
ORDERS = [
    (1, 1, 1200.0, 2, "SUMMER"),
    (2, 2,  600.0, 3, "summer"),    # case twin — DISTINCT must keep both
    (3, 1,  400.0, 4, "SUMMER"),    # duplicate — DISTINCT must collapse
    (4, 4,  900.0, 5, None),        # NULL — excluded
    (5, 1,  750.0, 6, ""),          # empty — excluded
    (6, 2,  300.0, 7, "ЗНИЖКА"),    # Cyrillic
]


async def _seed_duckdb(store):
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        conn.executemany(
            "INSERT INTO categories (id, name, parent_id) VALUES (?,?,?)", CATEGORIES)
        conn.executemany(
            "INSERT INTO products (id, name, category_id, brand, sku, price)"
            " VALUES (?,?,?,?,?,?)", PRODUCTS)
        for oid, src, total, days, promo in ORDERS:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total,"
                " ordered_at, buyer_id, manager_id, promocode)"
                " VALUES (?,?,1,?,?,7,NULL,?)",
                [oid, src, total, now - timedelta(days=days), promo])
    await store.refresh_warehouse_layers(trigger="manual")


async def _seed_postgres(conn, store):
    """Postgres gets the catalogue as written and the Silver the rebuild made.

    Silver is copied rather than recomputed for `test_reports_two_engines`'
    reason: this test is about the four lookups agreeing, and rebuilding twice
    would fold in whatever the warehouse layer does differently, which is
    `reconcile_silver`'s job and not this one.
    """
    await conn.execute("DELETE FROM silver.orders")
    await conn.execute("DELETE FROM bronze.products")
    await conn.execute("DELETE FROM bronze.categories")

    async with store.connection() as duck:
        silver = duck.execute(
            "SELECT id, source_id, status_id, grand_total, ordered_at, buyer_id,"
            " manager_id, order_date, is_return, sales_type, is_active_source,"
            " source_name, is_new_customer, buyer_first_order_date, promocode"
            " FROM silver_orders ORDER BY id").fetchall()

    await conn.executemany(
        "INSERT INTO bronze.categories (id, name, parent_id) VALUES ($1,$2,$3)",
        CATEGORIES)
    await conn.executemany(
        "INSERT INTO bronze.products (id, name, category_id, brand, sku, price)"
        " VALUES ($1,$2,$3,$4,$5,$6)", PRODUCTS)
    await conn.executemany(
        "INSERT INTO silver.orders (id, source_id, status_id, grand_total,"
        " ordered_at, buyer_id, manager_id, order_date, is_return, sales_type,"
        " is_active_source, source_name, is_new_customer,"
        " buyer_first_order_date, promocode)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        [tuple(r) for r in silver])


async def _clean(conn):
    await conn.execute("DELETE FROM silver.orders")
    await conn.execute("DELETE FROM bronze.products")
    await conn.execute("DELETE FROM bronze.categories")


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "lookups.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        await _seed_duckdb(store)
        async with pool.acquire() as conn:
            await _seed_postgres(conn, store)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield store
    finally:
        async with pool.acquire() as conn:
            await _clean(conn)
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, args=()):
    """Each engine once — and the Postgres leg with DuckDB made fatal.

    These four fall back to DuckDB when Postgres raises, so a comparison that
    merely calls the method twice can pass while Postgres never answers: the
    second call falls back and the two "engines" agree because they were the
    same engine. `get_margin_trend` shipped that way and the gate was green.
    """
    monkeypatch.delenv("KS_READ_LOOKUPS", raising=False)
    duck = await getattr(store, name)(*args)

    monkeypatch.setenv("KS_READ_LOOKUPS", "postgres")

    def _no_duckdb(*_a, **_k):
        raise AssertionError(
            f"{name} fell back to DuckDB — Postgres did not answer, so the "
            f"comparison would have compared DuckDB with itself"
        )

    with patch.object(type(store), "connection", _no_duckdb):
        postgres = await getattr(store, name)(*args)
    monkeypatch.delenv("KS_READ_LOOKUPS", raising=False)
    return duck, postgres


CALLS = (
    ("get_categories", ()),
    ("get_child_categories", (1,)),       # two children
    ("get_child_categories", (2,)),       # one child, two deep
    ("get_child_categories", (4,)),       # childless — an empty list is an answer
    ("get_child_categories", (99999,)),   # absent — must not be a crash
    ("get_brands", ()),
    ("get_promocodes", ()),
)


class TestTheTwoEnginesAgree:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,args", CALLS,
        ids=[f"{n}{args if args else ''}" for n, args in CALLS],
    )
    async def test_the_whole_result_is_identical(
        self, both_engines, monkeypatch, name, args,
    ):
        duck, postgres = await _both(both_engines, monkeypatch, name, args)
        assert duck == postgres, f"{name}{args}: {duck!r} != {postgres!r}"


class TestTheOrderIsTheOrderOnScreen:
    """Equality above would also hold if both engines returned the same rows in
    the same wrong order. These pin the order itself, so a collation change
    fails here with a readable diff rather than as an inequality."""

    @pytest.mark.asyncio
    async def test_brands_come_back_in_byte_order(self, both_engines, monkeypatch):
        duck, postgres = await _both(both_engines, monkeypatch, "get_brands")
        names = [b["name"] for b in postgres]
        listed = [n for n in names if n != "Unknown"]
        assert listed == sorted(listed), (
            f"Postgres is not sorting by byte order — the database may have "
            f"been restored under a different collation: {listed}"
        )
        assert [b["name"] for b in duck] == names

    @pytest.mark.asyncio
    async def test_the_unknown_bucket_is_offered_by_both(self, both_engines, monkeypatch):
        """Products 700 (NULL) and 800 (spaces) are unbranded, so the bucket
        exists — and 800 is *also* in the list above it, which is the quirk
        both engines must reproduce."""
        duck, postgres = await _both(both_engines, monkeypatch, "get_brands")
        for result in (duck, postgres):
            names = [b["name"] for b in result]
            assert "Unknown" in names
            assert "   " in names, "the whitespace brand left the list"
            assert "" not in names, "the empty brand entered the list"

    @pytest.mark.asyncio
    async def test_promocodes_keep_case_twins_and_collapse_duplicates(
        self, both_engines, monkeypatch,
    ):
        duck, postgres = await _both(both_engines, monkeypatch, "get_promocodes")
        for result in (duck, postgres):
            names = [p["name"] for p in result]
            assert names == sorted(names)
            assert names.count("SUMMER") == 1, "DISTINCT did not collapse"
            assert "summer" in names, "the case twin was folded away"
            assert "ЗНИЖКА" in names
            assert "" not in names and None not in names


class TestWhenNothingIsUnbranded:
    """The other side of the EXISTS branch: with no unbranded product, neither
    engine may offer the bucket. Without this the test above passes on a body
    that always appends it."""

    @pytest.mark.asyncio
    async def test_neither_engine_offers_the_bucket(
        self, both_engines, monkeypatch,
    ):
        store = both_engines
        async with store.connection() as conn:
            conn.execute("DELETE FROM products WHERE id IN (700, 800, 900)")
        from core.pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bronze.products WHERE id IN (700,800,900)")

        duck, postgres = await _both(store, monkeypatch, "get_brands")
        assert duck == postgres
        assert "Unknown" not in [b["name"] for b in postgres]
        assert "   " not in [b["name"] for b in postgres]
