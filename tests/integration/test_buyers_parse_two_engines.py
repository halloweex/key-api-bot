"""One awkward buyer batch, written by both real writers, reads back equal.

The unit tests prove the parser; this proves the stores. A batch built from real
KeyCRM-shaped payloads goes through `DuckDBStore.upsert_buyers` into a real
DuckDB file and through `pg_buyers.mirror_buyers` into a real PostgreSQL, and
the two are compared value by value after parsing — never by rendering, which
has manufactured three false differences in this repository already.

Every buyer in it used to break something:

* `1990-1-5` — DuckDB stored it, the mirror refused it;
* `1990-02-30` — DuckDB refused it and rolled back the WHOLE batch, and the
  sync tick retried it every minute, taking offers and stocks with it;
* a NUL in the note — DuckDB stored it, Postgres refuses the statement, so the
  mirror failed for that buyer forever;
* a name of spaces — neither store refused it, but the selection treats a blank
  name as "not synced yet" and fetched it again every hour.

Ids are taken from far above production's range and removed afterwards, so the
test shares the CI database without touching anybody else's rows.
"""
from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from core.landing_rows import BUYER_COLUMNS
from core.models import Buyer

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 9_400_000
MIRROR_TABLES = ("bronze.buyers", "bronze.buyer_contacts")


def _payload(n, **kw):
    data = {
        "id": BASE + n, "full_name": f"Покупець {n}",
        "created_at": "2026-09-01 10:00:00+00:00",
        "updated_at": "2026-09-02T11:30:00Z",
        "phone": [f"+38050000{n:04d}"], "email": [],
    }
    data.update(kw)
    return Buyer.from_api(data)


BATCH = [
    _payload(1, birthday="1990-1-5"),
    _payload(2, birthday="1990-02-30"),
    _payload(3, note="до\x00ставка"),
    _payload(4, full_name="   "),
    _payload(5, phone=["", "+380501112233", "+380501112233"]),
    _payload(6, birthday="1992-04-17",
             shipping=[{"city": "Київ", "region": "Київська"}],
             loyalty=[{"loyalty_program_name": "Club", "loyalty_level_name": "Gold",
                       "discount": 5, "amount": 1234.5}]),
]
IDS = [b.id for b in BATCH]


def _normal(value):
    """A value as Python means it, whichever driver produced it."""
    if isinstance(value, Decimal):
        return float(value)
    return value


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "buyers.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)

    async def clear():
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM bronze.buyer_contacts WHERE buyer_id = ANY($1::int[])", IDS)
            await conn.execute(
                "DELETE FROM bronze.buyers WHERE id = ANY($1::int[])", IDS)

    # The mirror stamps its watermark; put back whatever another test left.
    async with pool.acquire() as conn:
        before = await conn.fetch(
            "SELECT * FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(MIRROR_TABLES))
    await clear()
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield store, pool
    finally:
        await clear()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                list(MIRROR_TABLES))
            for row in before:
                cols = list(row.keys())
                await conn.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(cols)}) VALUES "
                    f"({', '.join(f'${i}' for i in range(1, len(cols) + 1))})",
                    *row.values())
        await pool.close()
        await store.close()


@pytest.mark.asyncio
async def test_one_awkward_batch_reads_back_equal_from_both_stores(both_engines):
    from core.pg_buyers import mirror_buyers

    store, pool = both_engines
    # Neither writer may refuse the batch: that refusal was the whole bug.
    assert await store.upsert_buyers(BATCH) == len(BATCH)
    result = await mirror_buyers(BATCH)
    assert result.get("error") is None, result

    cols = ", ".join(BUYER_COLUMNS)
    async with store.connection() as conn:
        duck = {r[0]: tuple(_normal(v) for v in r) for r in conn.execute(
            f"SELECT {cols} FROM buyers WHERE id IN ({', '.join('?' * len(IDS))})",
            IDS).fetchall()}
        duck_contacts = sorted(tuple(r) for r in conn.execute(
            "SELECT buyer_id, contact_type, value, is_primary FROM buyer_contacts "
            f"WHERE buyer_id IN ({', '.join('?' * len(IDS))})", IDS).fetchall())
    async with pool.acquire() as conn:
        pg = {r["id"]: tuple(_normal(v) for v in r.values()) for r in await conn.fetch(
            f"SELECT {cols} FROM bronze.buyers WHERE id = ANY($1::int[])", IDS)}
        pg_contacts = sorted(tuple(r.values()) for r in await conn.fetch(
            "SELECT buyer_id, contact_type, value, is_primary FROM bronze.buyer_contacts "
            "WHERE buyer_id = ANY($1::int[])", IDS))

    assert set(duck) == set(pg) == set(IDS)
    for bid in IDS:
        assert duck[bid] == pg[bid], f"buyer {bid} differs between the stores"
    assert duck_contacts == pg_contacts

    by = {bid: dict(zip(BUYER_COLUMNS, duck[bid])) for bid in IDS}
    assert by[BASE + 1]["birthday"] == date(1990, 1, 5)
    assert by[BASE + 2]["birthday"] is None
    assert by[BASE + 3]["note"] == "доставка"
    assert by[BASE + 4]["full_name"] == "Unknown"
    assert by[BASE + 6]["city"] == "Київ"
    assert [c for c in duck_contacts if c[0] == BASE + 5] == \
        [(BASE + 5, "phone", "+380501112233", False)]
