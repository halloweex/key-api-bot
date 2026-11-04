"""A tab's fixture in Postgres alone: Bronze seeded, Silver and Gold derived.

The six tabs step 14's S14-05 left on Postgres alone — `/traffic`, `/margin`,
`/products` intelligence, `/reports`, `/marketing` and the filter bar — were
proven against DuckDB by comparing the two engines on one fixture. With the
DuckDB side gone, each of their `*_two_engines.py` tests seeds what KeyCRM
lands — `bronze.orders`, `bronze.order_products`, `bronze.products`,
`bronze.categories`, `bronze.offer_stocks` — lets Postgres derive Silver and
Gold the way production does (`rebuild_silver`, `rebuild_gold`), and compares
every answer with a literal taken from DuckDB's answer on the same fixture
before that side was deleted (rule 9 of the step-14 plan). The off sweep's
integration flavour seeds Bronze and derives the same way.

Deriving rather than copying is what the literal is checked against: the
expected answers came out of DuckDB's own warehouse rebuild, so a Postgres
Silver that derived anything differently fails here too.

The watermark rows the two rebuilds stamp (`meta.mirror_state`) are put back
as they were, and so is `pg_silver.FIRST_REBUILT_AT`, so a test here leaves
the database as it found it for the suite's other users of it.
"""
from __future__ import annotations

import json
import os
import pathlib
from contextlib import asynccontextmanager
from typing import Any, Iterable, Sequence

import pytest
from unittest.mock import AsyncMock, patch

DSN = os.getenv("KS_PG_DSN")
# The skip CI's last step greps for: it fails the job if any test takes it.
needs_postgres = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# Derived first, so a DELETE never meets a row still referring to another.
DERIVED = ("gold.daily_revenue", "silver.order_utm", "silver.orders")
LANDED = ("bronze.order_products", "bronze.orders", "bronze.products",
          "bronze.categories", "bronze.offer_stocks")
APP = ("app.manual_expenses", "app.revenue_goals")
TABLES = DERIVED + LANDED + APP
WATERMARKS = ("silver.orders", "gold.daily_revenue")

GOLDEN = pathlib.Path(__file__).resolve().parent / "golden"


def golden(name: str) -> dict:
    """The literal answers for one `*_two_engines.py`: DuckDB's, on that
    file's fixture, recorded at step 14's S14-05 while the file still proved
    DuckDB and Postgres equal on every one of them."""
    doc = json.loads((GOLDEN / f"{name}.json").read_text(encoding="utf-8"))
    return doc["answers"]


def call_key(name: str, call: Any) -> str:
    """How `golden()` keys an answer: the method and its arguments."""
    return f"{name}{json.dumps(call, sort_keys=True, default=list)}"


def comparable(value):
    """Money to six places, dates as ISO text, tuples as lists — the form the
    literals were recorded in. DuckDB divided a DECIMAL into a DOUBLE and
    Postgres into a NUMERIC, so the two differed in the last bits; six places
    is far below anything a tab renders and far above that noise."""
    import datetime as _dt

    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): comparable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [comparable(v) for v in value]
    if isinstance(value, (_dt.date, _dt.datetime)):
        return value.isoformat()
    if hasattr(value, "as_tuple") or isinstance(value, float):
        return round(float(value), 6)
    return value


async def clear(conn) -> None:
    for table in TABLES:
        await conn.execute(f"DELETE FROM {table}")


async def seed(
    conn,
    *,
    categories: Iterable[Sequence] = (),
    products: Iterable[Sequence] = (),
    orders: Iterable[Sequence] = (),
    lines: Iterable[Sequence] = (),
    stocks: Iterable[Sequence] = (),
) -> None:
    """Bronze, as the landing writes it.

    `orders` are `(id, source_id, status_id, grand_total, ordered_at,
    buyer_id, manager_id, promocode)`, `lines` `(id, order_id, product_id,
    name, quantity, price_sold)` and `stocks` `(id, sku, price,
    purchased_price)`; categories and products in their table's order."""
    await conn.executemany(
        "INSERT INTO bronze.categories (id, name, parent_id) VALUES ($1,$2,$3)",
        list(categories))
    await conn.executemany(
        "INSERT INTO bronze.products (id, name, category_id, brand, sku, price)"
        " VALUES ($1,$2,$3,$4,$5,$6)", list(products))
    await conn.executemany(
        "INSERT INTO bronze.orders (id, source_id, status_id, grand_total,"
        " ordered_at, buyer_id, manager_id, promocode)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", list(orders))
    await conn.executemany(
        "INSERT INTO bronze.order_products (id, order_id, product_id, name,"
        " quantity, price_sold) VALUES ($1,$2,$3,$4,$5,$6)", list(lines))
    await conn.executemany(
        "INSERT INTO bronze.offer_stocks (id, sku, price, purchased_price,"
        " quantity, reserve) VALUES ($1,$2,$3,$4,0,0)", list(stocks))


async def derive(pool) -> None:
    """Silver from Bronze, then Gold from Silver: the derivation's own two
    rebuilds, in its order."""
    from core.pg_gold import rebuild_gold
    from core.pg_silver import rebuild_silver

    await rebuild_silver(pool)
    await rebuild_gold(pool)


@asynccontextmanager
async def tab_in_postgres(dsn: str, monkeypatch, populate):
    """A pool on `dsn`, the tables cleared, `populate(conn, pool)` run, and
    `core.pg.get_pool` answering with the pool for the test's duration —
    then everything put back."""
    import asyncpg

    from core import pg_silver

    monkeypatch.setenv("KS_PG_DSN", dsn)
    monkeypatch.setattr(pg_silver, "FIRST_REBUILT_AT", pg_silver.FIRST_REBUILT_AT)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    stamps: list = []
    try:
        async with pool.acquire() as conn:
            stamps = await conn.fetch(
                "SELECT * FROM meta.mirror_state WHERE table_name = ANY($1)",
                list(WATERMARKS))
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            async with pool.acquire() as conn:
                await clear(conn)
                await populate(conn, pool)
            yield pool
    finally:
        try:
            async with pool.acquire() as conn:
                await clear(conn)
                await conn.execute(
                    "DELETE FROM meta.mirror_state WHERE table_name = ANY($1)",
                    list(WATERMARKS))
                for row in stamps:
                    columns = list(row.keys())
                    await conn.execute(
                        f"INSERT INTO meta.mirror_state ({', '.join(columns)}) "
                        f"VALUES ({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                        *row.values())
        finally:
            await pool.close()


def store_that_never_reaches_duckdb(tmp_path, monkeypatch, what: str = "the tab"):
    """The repository, on a store that is never connected and whose DuckDB
    connection is a trap: an answer it gives can only be Postgres's."""
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "never.duckdb")

    def _no_duckdb(*_a, **_k):
        raise AssertionError(f"{what} reached for DuckDB")

    monkeypatch.setattr(type(store), "connection", _no_duckdb)
    return store
