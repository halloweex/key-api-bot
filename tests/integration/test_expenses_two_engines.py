"""`/expenses` means the same thing in DuckDB and Postgres.

Six read methods across two shapes: three that read only what a human typed —
`app.manual_expenses`, already here since revision 0018 — and three that read
the order-level costs KeyCRM serves, which revision 0020 mirrors.

The fixture is built to attack the two things the port changed in both
engines:

* **the fan-out.** Order 1 carries three expenses and order 2 carries two, so
  a query that sums `grand_total` across the join reports their revenue three
  and two times over. That was live on production — ₴314,365.50 over 90 days —
  and the fix has to hold identically in both stores.
* **the move to Silver.** The old bodies joined raw `orders` and narrowed with
  an `EXISTS` against Silver; these read Silver directly. A return and a b2b
  order are here so the flags and the classification are actually exercised.

`tests/unit/test_pg_expenses_read.py` proves the wiring and the shared parse.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

from core.duckdb_store import DuckDBStore

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

TODAY = date.today()
W = (TODAY - timedelta(days=25), TODAY)

# (id, source_id, grand_total, days_ago, status_id, manager_id)
# status 19 is KeyCRM's lost/cancel group; manager 15 is the wholesale manager,
# so order 4 classifies as b2b and must vanish under the retail default.
ORDERS = [
    (1, 1, 1000.0, 2, 1, None),
    (2, 1, 2000.0, 3, 1, None),
    (3, 2, 500.0, 4, 1, None),
    (4, 1, 9000.0, 5, 1, 15),
    (5, 1, 700.0, 6, 19, None),
]
TYPES = [
    (1, "dictionaries.expense_types.delivery", "delivery", True),
    (2, "Комісія", "commission", True),
    (3, "Retired", "retired", False),
]
# (id, order_id, expense_type_id, amount)
EXPENSES = [
    (10, 1, 1, 100.0), (11, 1, 2, 50.0), (12, 1, 1, 25.0),   # three on one order
    (13, 2, 1, 200.0), (14, 2, 2, 30.0),                      # two on another
    (15, 3, 1, 40.0),
    (16, 4, 1, 500.0),                                        # b2b
    (17, 5, 1, 60.0),                                         # on a return
]
SPEND = [
    (1, 3, "marketing", "Facebook Ads", 500.0, "facebook"),
    (2, 4, "marketing", "TikTok Ads", 250.0, "tiktok"),
    (3, 5, "salary", "Salary", 9999.0, None),
]

EXP_COLS = ("id", "order_id", "expense_type_id", "amount", "description",
            "status", "payment_date", "created_at")
TYPE_COLS = ("id", "name", "alias", "is_active")
SPEND_COLS = ("id", "expense_date", "category", "expense_type", "amount",
              "currency", "note", "created_at", "updated_at", "platform")


async def _seed_duckdb(store):
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        for oid, src, total, days, status, mgr in ORDERS:
            conn.execute(
                "INSERT INTO orders (id,source_id,status_id,grand_total,"
                "ordered_at,buyer_id,manager_id) VALUES (?,?,?,?,?,?,?)",
                [oid, src, status, total, now - timedelta(days=days), oid, mgr])
        conn.execute("INSERT INTO managers (id,full_name,is_retail) "
                     "VALUES (15,'Wholesale',FALSE)")
    # Written through the store, so the shared parse is the thing under test.
    await store.upsert_expense_types([
        {"id": i, "name": n, "alias": a, "is_active": act}
        for i, n, a, act in TYPES
    ])
    await store.upsert_expenses_batch([
        {"id": oid, "expenses": [
            {"id": eid, "expense_type_id": tid, "amount": amt,
             "description": f"exp {eid}", "status": "paid",
             "payment_date": None, "created_at": now - timedelta(days=1)}
            for eid, o, tid, amt in EXPENSES if o == oid
        ]}
        for oid in {o for _, o, _, _ in EXPENSES}
    ])
    async with store.connection() as conn:
        for eid, days, cat, etype, amt, plat in SPEND:
            conn.execute(
                "INSERT INTO manual_expenses (id,expense_date,category,"
                "expense_type,amount,currency,note,created_at,platform)"
                " VALUES (?,?,?,?,?,'UAH',NULL,?,?)",
                [eid, TODAY - timedelta(days=days), cat, etype, amt,
                 now - timedelta(days=days), plat])
    await store.refresh_warehouse_layers(trigger="manual")


async def _seed_postgres(conn, store):
    for t in ("bronze.expenses", "bronze.expense_types", "app.manual_expenses",
              "silver.orders"):
        await conn.execute(f"DELETE FROM {t}")

    async with store.connection() as duck:
        silver = duck.execute(
            "SELECT id, source_id, status_id, grand_total, ordered_at, buyer_id,"
            " manager_id, order_date, is_return, sales_type, is_active_source,"
            " source_name, is_new_customer, buyer_first_order_date, promocode"
            " FROM silver_orders ORDER BY id").fetchall()
        types = duck.execute(
            f"SELECT {', '.join(TYPE_COLS)} FROM expense_types ORDER BY id").fetchall()
        exps = duck.execute(
            f"SELECT {', '.join(EXP_COLS)} FROM expenses ORDER BY id").fetchall()
        spend = duck.execute(
            f"SELECT {', '.join(SPEND_COLS)} FROM manual_expenses ORDER BY id").fetchall()

    await conn.executemany(
        "INSERT INTO silver.orders (id,source_id,status_id,grand_total,ordered_at,"
        "buyer_id,manager_id,order_date,is_return,sales_type,is_active_source,"
        "source_name,is_new_customer,buyer_first_order_date,promocode)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        [tuple(r) for r in silver])
    await conn.executemany(
        f"INSERT INTO bronze.expense_types ({', '.join(TYPE_COLS)}) "
        f"VALUES ($1,$2,$3,$4)", [tuple(r) for r in types])
    await conn.executemany(
        f"INSERT INTO bronze.expenses ({', '.join(EXP_COLS)}) "
        f"VALUES ({', '.join(f'${i}' for i in range(1, len(EXP_COLS) + 1))})",
        [tuple(r) for r in exps])
    await conn.executemany(
        f"INSERT INTO app.manual_expenses ({', '.join(SPEND_COLS)}) "
        f"VALUES ({', '.join(f'${i}' for i in range(1, len(SPEND_COLS) + 1))})",
        [tuple(r) for r in spend])


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "expenses.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        await _seed_duckdb(store)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            async with pool.acquire() as conn:
                await _seed_postgres(conn, store)
            yield store
    finally:
        async with pool.acquire() as conn:
            for t in ("bronze.expenses", "bronze.expense_types",
                      "app.manual_expenses", "silver.orders"):
                await conn.execute(f"DELETE FROM {t}")
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, args, kwargs):
    """Each engine once — and the Postgres leg with DuckDB made fatal.

    Without this the comparison passes while Postgres never answers: the second
    call falls back, returns the DuckDB answer, and the two "engines" agree
    because they were the same engine. `get_margin_trend` shipped a
    DuckDB-only function that way.
    """
    monkeypatch.delenv("KS_READ_EXPENSES", raising=False)
    duck = await getattr(store, name)(*args, **kwargs)

    monkeypatch.setenv("KS_READ_EXPENSES", "postgres")

    def _no_duckdb(*_a, **_k):
        raise AssertionError(
            f"{name} fell back to DuckDB — the comparison below would have "
            f"compared DuckDB with itself"
        )

    with patch.object(type(store), "connection", _no_duckdb):
        postgres = await getattr(store, name)(*args, **kwargs)
    monkeypatch.delenv("KS_READ_EXPENSES", raising=False)
    return duck, postgres


def _comparable(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, dict):
        return {k: _comparable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_comparable(x) for x in v]
    if hasattr(v, "as_tuple") or isinstance(v, float):
        return round(float(v), 6)
    return v


CALLS = (
    ("get_expense_types", (), {}),
    ("get_expense_summary", W, {}),
    ("get_expense_summary", W, {"sales_type": "all"}),
    ("get_expense_summary", W, {"sales_type": "b2b"}),
    ("get_expense_summary", W, {"source_id": 1}),
    ("get_expense_summary", W, {"expense_type_id": 1}),
    ("get_profit_analysis", W, {}),
    ("get_profit_analysis", W, {"sales_type": "all"}),
    ("get_profit_analysis", W, {"source_id": 2}),
    ("list_expenses", (), {}),
    ("list_expenses", (), {"limit": 2}),
    ("list_expenses", (), {"category": "marketing"}),
    ("get_ad_spend_by_platform", W, {}),
    ("get_expenses_summary", (), {}),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,args,kwargs", CALLS,
    ids=[f"{n}{tuple(k.values()) if k else ''}" for n, _, k in CALLS],
)
async def test_both_engines_return_the_same_answer(
    both_engines, monkeypatch, name, args, kwargs,
):
    duck, postgres = await _both(both_engines, monkeypatch, name, args, kwargs)
    assert _comparable(postgres) == _comparable(duck)


@pytest.mark.asyncio
async def test_revenue_is_not_multiplied_by_the_expense_count(
    both_engines, monkeypatch,
):
    """Order 1 has three expenses and order 2 has two. Under the old join
    their ₴1 000 and ₴2 000 were counted three and two times; the truth is
    each once."""
    duck, pg = await _both(both_engines, monkeypatch, "get_profit_analysis", W, {})
    retail = [o for o in ORDERS if o[5] is None and o[4] != 19]
    expected = round(sum(o[2] for o in retail), 2)
    for out in (duck, pg):
        revenue = round(sum(out["chart"]["datasets"][0]["data"]), 2)
        assert revenue == expected, (
            f"revenue {revenue} against a true {expected} — the join fans out"
        )


@pytest.mark.asyncio
async def test_the_expense_total_survives_the_fix(both_engines, monkeypatch):
    """Folding the expenses before the join must not lose any of them."""
    duck, pg = await _both(both_engines, monkeypatch, "get_profit_analysis", W, {})
    retail_ids = {o[0] for o in ORDERS if o[5] is None and o[4] != 19}
    expected = round(sum(a for _, o, _, a in EXPENSES if o in retail_ids), 2)
    for out in (duck, pg):
        total = round(sum(out["chart"]["datasets"][1]["data"]), 2)
        assert total == expected


@pytest.mark.asyncio
async def test_the_localisation_key_reaches_both_stores_resolved(
    both_engines, monkeypatch,
):
    """The parse that had to move before the table gained a second writer."""
    duck, pg = await _both(both_engines, monkeypatch, "get_expense_types", (), {})
    for out in (duck, pg):
        names = {t["name"] for t in out}
        assert "Delivery" in names
        assert not any(n.startswith("dictionaries.") for n in names)


@pytest.mark.asyncio
async def test_an_inactive_type_is_absent_from_both(both_engines, monkeypatch):
    duck, pg = await _both(both_engines, monkeypatch, "get_expense_types", (), {})
    for out in (duck, pg):
        assert "Retired" not in {t["name"] for t in out}
