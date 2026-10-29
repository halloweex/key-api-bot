"""The filter bar answers in Postgres what DuckDB answered.

`tests/unit/test_pg_lookups_read.py` proves the bodies render and that nothing
goes round the router. Necessary and not sufficient — and for four queries
whose whole output is *an ordered list of strings*, the difference that
matters is **collation**.

ONE ENGINE SINCE STEP 14 (S14-05)

The four read Postgres and nothing else. Each is compared with a literal
(`tests/integration/golden/lookups.json`): DuckDB's answer on this fixture,
recorded while this file still asserted Postgres answered the same (rule 9 of
the step-14 plan) — so the order of a dropdown is still pinned to what it was
when two engines agreed on it. Postgres derives Silver from the seeded Bronze
(`tests/integration/pg_tab_seed.py`), and the store's DuckDB connection is a
trap.

THE FIXTURE IS DELIBERATELY AWKWARD

Three of the four end in `ORDER BY` on a text column, and that order reaches
the user as the order of a dropdown. PostgreSQL sorts by the database
collation and the literal is DuckDB's byte order — measured on production
2026-09-12 (`datcollate = C`, DuckDB 1.5.5 binary). So the brands here are
chosen to separate byte order from locale order rather than to look like a
catalogue: mixed case, a leading apostrophe, a diaeresis and Cyrillic in one
list. Under a locale collation `abib` sorts beside `Abib`; under byte order
every capital precedes every lowercase, and this test fails the day the
database is restored under a different locale.

It also carries the two rows that make the unknown-brand bucket interesting:
`NULL` and a brand of nothing but spaces. `_BRANDS_SQL` keeps the spaces
(`brand != ''` is true of `'   '`) while `_UNBRANDED_EXISTS_SQL` counts it as
unbranded (`TRIM(brand) = ''`), so the same product appears in the list *and*
summons the bucket. That is existing behaviour, and the point here is that it
is reproduced rather than that it is right.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from tests.integration.pg_tab_seed import (
    call_key, comparable, derive, golden, seed, tab_in_postgres,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

# Pinned (step 14, S14-05): the answers are literals taken from DuckDB on
# this fixture, so the calendar under them cannot move with the run.
NOW = datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)   # midday Kyiv

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


async def _populate(conn, pool):
    await seed(
        conn, categories=CATEGORIES, products=PRODUCTS,
        orders=[(oid, src, 1, total, NOW - timedelta(days=days), 7, None, promo)
                for oid, src, total, days, promo in ORDERS])
    await derive(pool)


@pytest_asyncio.fixture
async def in_postgres(tmp_path, monkeypatch):
    """The filter bar's store, never connected: its DuckDB connection is a
    trap, so an answer below can only be Postgres's."""
    store = DuckDBStore(db_path=tmp_path / "lookups.duckdb")

    def _no_duckdb(*_a, **_k):
        raise AssertionError("the filter bar reached for DuckDB")

    monkeypatch.setattr(type(store), "connection", _no_duckdb)
    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        yield store, pool


GOLDEN = golden("lookups")


CALLS = (
    ("get_categories", ()),
    ("get_child_categories", (1,)),       # two children
    ("get_child_categories", (2,)),       # one child, two deep
    ("get_child_categories", (4,)),       # childless — an empty list is an answer
    ("get_child_categories", (99999,)),   # absent — must not be a crash
    ("get_brands", ()),
    ("get_promocodes", ()),
)


class TestPostgresAnswersWhatDuckdbAnswered:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,args", CALLS,
        ids=[f"{n}{args if args else ''}" for n, args in CALLS],
    )
    async def test_the_whole_result_is_the_literal(self, in_postgres, name, args):
        store, _pool = in_postgres
        answer = await getattr(store, name)(*args)
        assert comparable(answer) == GOLDEN[call_key(name, args)], (
            f"{name}{args}: {answer!r}")

    def test_every_literal_is_a_call_and_every_call_has_one(self):
        assert set(GOLDEN) == {call_key(n, a) for n, a in CALLS}


class TestTheOrderIsTheOrderOnScreen:
    """Equality above would also hold if the literal had been recorded in a
    wrong order. These pin the order itself, so a collation change fails here
    with a readable diff rather than as an inequality."""

    @pytest.mark.asyncio
    async def test_brands_come_back_in_byte_order(self, in_postgres):
        store, _pool = in_postgres
        names = [b["name"] for b in await store.get_brands()]
        listed = [n for n in names if n != "Unknown"]
        assert listed == sorted(listed), (
            f"Postgres is not sorting by byte order — the database may have "
            f"been restored under a different collation: {listed}"
        )

    @pytest.mark.asyncio
    async def test_the_unknown_bucket_is_offered(self, in_postgres):
        """Products 700 (NULL) and 800 (spaces) are unbranded, so the bucket
        exists — and 800 is *also* in the list above it, which is the quirk
        the port reproduces."""
        store, _pool = in_postgres
        names = [b["name"] for b in await store.get_brands()]
        assert "Unknown" in names
        assert "   " in names, "the whitespace brand left the list"
        assert "" not in names, "the empty brand entered the list"

    @pytest.mark.asyncio
    async def test_promocodes_keep_case_twins_and_collapse_duplicates(self, in_postgres):
        store, _pool = in_postgres
        names = [p["name"] for p in await store.get_promocodes()]
        assert names == sorted(names)
        assert names.count("SUMMER") == 1, "DISTINCT did not collapse"
        assert "summer" in names, "the case twin was folded away"
        assert "ЗНИЖКА" in names
        assert "" not in names and None not in names


class TestWhenNothingIsUnbranded:
    """The other side of the EXISTS branch: with no unbranded product, the
    bucket is not offered. Without this the test above passes on a body that
    always appends it."""

    @pytest.mark.asyncio
    async def test_the_bucket_is_not_offered(self, in_postgres):
        store, pool = in_postgres
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM bronze.products WHERE id = ANY($1)", [700, 800, 900])

        names = [b["name"] for b in await store.get_brands()]
        assert "Unknown" not in names
        assert "   " not in names
