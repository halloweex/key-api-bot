"""The one writer of buyer rows, against a real PostgreSQL (chain 4, PR-2, 2.1).

`core.pg_buyer_rows._write_buyer_rows` is what the landing mirror runs now and
what chain 4's writer will run. These pin what both depend on:

- the mirror still writes exactly the rows the shared parse produced, and
  stamps its watermarks as before;
- an UPDATE moves `mirrored_at`, which is how the search index finds a changed
  buyer (`pg_search_index_read.high_watermark`/`buyers_frame`) — nothing
  tested that before;
- a shrunk contact list shrinks in Postgres;
- the derivation signal rises once for a batch that wrote something, never for
  an empty one;
- the core alone never touches `meta.mirror_state`, because the chain must not
  stamp the mirror's watermark.

Ids come from far above production's range and are removed afterwards; the
watermark rows and the signal are compared as deltas or restored, because the
database is shared with the rest of the suite.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.landing_rows import BUYER_COLUMNS, parse_buyers
from core.models import Buyer

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 9_600_000
STATES = ("bronze.buyers", "bronze.buyer_contacts")


def _buyer(n, *, name=None, phones=None):
    return Buyer.from_api({
        "id": BASE + n, "full_name": name or f"Покупець {n}",
        "created_at": "2026-09-01 10:00:00+00:00",
        "updated_at": "2026-09-02T11:30:00Z",
        "phone": phones if phones is not None else [f"+38050{n:07d}"],
        "email": [f"b{n}@example.test"],
    })


@pytest_asyncio.fixture
async def pg(monkeypatch):
    from core import pg_derivation

    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_PG_DERIVE", "own")
    before_mode = (pg_derivation._mode, pg_derivation._mode_error)
    pg_derivation.configure_mode()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)

    async def clear():
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM bronze.buyer_contacts WHERE buyer_id >= $1 AND buyer_id < $2",
                BASE, BASE + 1000)
            await conn.execute(
                "DELETE FROM bronze.buyers WHERE id >= $1 AND id < $2", BASE, BASE + 1000)

    async with pool.acquire() as conn:
        saved = await conn.fetch(
            "SELECT * FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(STATES))
    await clear()
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield pool
    finally:
        await clear()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                list(STATES))
            for row in saved:
                cols = list(row.keys())
                await conn.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(cols)}) VALUES "
                    f"({', '.join(f'${i}' for i in range(1, len(cols) + 1))})",
                    *row.values())
        await pool.close()
        pg_derivation._mode, pg_derivation._mode_error = before_mode


async def _buyers(pool, ids):
    async with pool.acquire() as conn:
        return {r["id"]: r for r in await conn.fetch(
            f"SELECT {', '.join(BUYER_COLUMNS)}, mirrored_at FROM bronze.buyers "
            "WHERE id = ANY($1::int[])", list(ids))}


async def _contacts(pool, ids):
    async with pool.acquire() as conn:
        return sorted(tuple(r) for r in await conn.fetch(
            "SELECT buyer_id, contact_type, value, is_primary FROM bronze.buyer_contacts "
            "WHERE buyer_id = ANY($1::int[])", list(ids)))


async def _state(pool):
    async with pool.acquire() as conn:
        return {r["table_name"]: dict(r) for r in await conn.fetch(
            "SELECT table_name, last_ok_at, last_rows, failures_since_ok "
            "FROM meta.mirror_state WHERE table_name = ANY($1::text[])", list(STATES))}


async def _requested(pool):
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT requested FROM meta.derivation_signal WHERE layer = 'warehouse'")


@pytest.mark.asyncio
async def test_the_mirror_writes_exactly_the_parsed_rows_and_stamps_as_before(pg):
    from core.pg_buyers import mirror_buyers

    batch = [_buyer(1), _buyer(2, phones=["+380500000009", "+380500000010"]), _buyer(3, phones=[])]
    parsed = parse_buyers(batch)
    ids = [b.id for b in batch]

    result = await mirror_buyers(batch)

    assert result.get("error") is None and result["rows"] == 3, result
    stored = await _buyers(pg, ids)
    for row in parsed.rows:
        assert tuple(stored[row.id][c] for c in BUYER_COLUMNS) == tuple(row), row.id
    expected = sorted(tuple(c) for _id, cs in parsed.contacts for c in cs)
    assert await _contacts(pg, ids) == expected
    state = await _state(pg)
    assert state["bronze.buyers"]["last_rows"] == 3
    assert state["bronze.buyer_contacts"]["last_rows"] == len(expected)
    assert state["bronze.buyers"]["failures_since_ok"] == 0


@pytest.mark.asyncio
async def test_an_update_moves_mirrored_at_and_the_search_index_sees_it(pg):
    from core import pg_search_index_read
    from core.pg_buyers import mirror_buyers

    await mirror_buyers([_buyer(4)])
    first = (await _buyers(pg, [BASE + 4]))[BASE + 4]["mirrored_at"]
    since = await pg_search_index_read.high_watermark()
    assert since >= first

    await mirror_buyers([_buyer(4, name="Покупець Перейменований")])

    stored = (await _buyers(pg, [BASE + 4]))[BASE + 4]
    assert stored["full_name"] == "Покупець Перейменований"
    assert stored["mirrored_at"] > first, "an UPDATE left mirrored_at alone"
    frame = await pg_search_index_read.buyers_frame(since)
    assert BASE + 4 in set(frame["id"]), "the index would never pick up the rename"


@pytest.mark.asyncio
async def test_a_shrunk_contact_list_shrinks(pg):
    from core.pg_buyers import mirror_buyers

    await mirror_buyers([_buyer(5, phones=["+380501", "+380502", "+380503"])])
    await mirror_buyers([_buyer(5, phones=["+380501", "+380503"])])

    phones = [c[2] for c in await _contacts(pg, [BASE + 5]) if c[1] == "phone"]
    assert phones == ["+380501", "+380503"]


@pytest.mark.asyncio
async def test_the_signal_rises_once_for_a_batch_and_never_for_an_empty_one(pg):
    from core.pg_buyer_rows import _write_buyer_rows
    from core.pg_buyers import _write

    parsed = parse_buyers([_buyer(6)])

    before = await _requested(pg)
    async with pg.acquire() as conn:
        async with conn.transaction():
            await _write_buyer_rows(conn, parsed.rows, parsed.contacts)
    assert await _requested(pg) == before + 1

    await _write([], [])
    assert await _requested(pg) == before + 1, "an empty batch raised the signal"


@pytest.mark.asyncio
async def test_the_core_alone_never_touches_mirror_state(pg):
    from core.pg_buyer_rows import _write_buyer_rows
    from core.pg_buyers import _write

    # Both watermark rows must exist first, or a core that UPDATEd them would
    # change nothing and pass — which the first draft of this test did.
    await _write([], [])
    parsed = parse_buyers([_buyer(7), _buyer(8)])
    before = await _state(pg)
    assert set(before) == set(STATES), before
    async with pg.acquire() as conn:
        async with conn.transaction():
            written = await _write_buyer_rows(conn, parsed.rows, parsed.contacts)

    assert written == sum(len(cs) for _id, cs in parsed.contacts)
    assert set(await _buyers(pg, [BASE + 7, BASE + 8])) == {BASE + 7, BASE + 8}
    assert await _state(pg) == before


@pytest.mark.asyncio
async def test_the_rows_share_the_callers_transaction(pg):
    """Chain 4 proves its first write by the owner row sharing an `xmin` with
    the buyer it was taken for; rows written inside a savepoint would carry a
    subtransaction's XID instead. So the core must not wrap the rows."""
    from core.pg_buyer_rows import _write_buyer_rows

    parsed = parse_buyers([_buyer(9)])
    async with pg.acquire() as conn:
        async with conn.transaction():
            top = await conn.fetchval("SELECT txid_current()::text")
            await _write_buyer_rows(conn, parsed.rows, parsed.contacts)
            xmin = await conn.fetchval(
                "SELECT xmin::text FROM bronze.buyers WHERE id = $1", BASE + 9)
    assert int(xmin) == int(top) % (2 ** 32)
