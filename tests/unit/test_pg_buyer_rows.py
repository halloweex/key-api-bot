"""`core.pg_buyer_rows._write_buyer_rows`: the order it takes its row locks
in, and what it hands the statements for an emptied contact list.

The integration file (`tests/integration/test_pg_buyer_rows.py`) proves what
lands in Postgres. Order is invisible there — any order lands the same rows —
and it is the one property two concurrent writers depend on: chain 4's writer
takes no lock that serialises it with another buyer write first (its claim is
`ON CONFLICT DO NOTHING`), nor does the hourly `derive_gender_pg`, which takes
id-ordered share locks of its own. Two transactions writing overlapping buyers
in opposite orders deadlock; the review reproduced it in 4 runs of 5 with the
sort removed. So the order is shown on the test Postgres, by the row locks a
write takes.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from tests.helpers.buyers_fixture import buyers_in_postgres
from tests.integration.pg_tab_seed import needs_postgres


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Conn:
    """Records every statement and its arguments, in order."""

    def __init__(self):
        self.calls = []

    async def executemany(self, sql, rows):
        self.calls.append((sql, [tuple(r) for r in rows]))

    async def execute(self, sql, *args):
        self.calls.append((sql, args))

    def transaction(self):
        return _Tx()


IDS = (1, 3, 5, 9)


def _buyer(buyer_id: int, name: str = "Покупець"):
    """A buyer as KeyCRM serves it, with one phone that is obviously fake."""
    from core.models import Buyer

    return Buyer.from_api({"id": buyer_id, "full_name": f"{name} {buyer_id}",
                           "phone": [f"38096{buyer_id:07d}"]})


async def _until_a_session_waits_on_a_lock(pool) -> None:
    for _ in range(200):
        async with pool.acquire() as conn:
            if await conn.fetchval(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = "
                    "current_database() AND wait_event_type = 'Lock'"):
                return
        await asyncio.sleep(0.05)
    pytest.fail("the write never came to wait on buyer 1's row")


@needs_postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("table,column", [("bronze.buyers", "id"),
                                          ("bronze.buyer_contacts", "buyer_id")])
async def test_a_batch_takes_its_row_locks_in_id_order(table, column):
    """F4 (review of #265). Handed 9, 5, 3, 1 through chain 4's writer, a
    write locks buyer 1's rows first — its buyer row, then, after every buyer
    row, its contacts. A session holding buyer 1's row stops the write there,
    before it has locked buyer 9's, so two writers handed overlapping buyers
    in opposite orders wait for each other instead of deadlocking."""
    from core import pg_buyers_write as chain

    async with buyers_in_postgres() as pool:
        assert await chain.upsert_buyers([_buyer(i) for i in IDS]) == len(IDS)
        async with pool.acquire() as holder:
            tx = holder.transaction()
            await tx.start()
            write = None
            try:
                await holder.execute(f"SELECT 1 FROM {table} WHERE {column} = 1 FOR UPDATE")
                write = asyncio.ensure_future(chain.upsert_buyers(
                    [_buyer(i, "Нова") for i in reversed(IDS)]))
                await _until_a_session_waits_on_a_lock(pool)
                free = await holder.fetchval(
                    f"SELECT count(*) FROM (SELECT 1 FROM {table} WHERE {column} = 9 "
                    "FOR UPDATE SKIP LOCKED) s")
                assert free == 1, f"buyer 9's {table} row was locked before buyer 1's"
            finally:
                await tx.rollback()
                if write is not None:
                    assert await asyncio.wait_for(write, timeout=30) == len(IDS)


@pytest.mark.asyncio
async def test_an_empty_contact_list_still_deletes(monkeypatch):
    """F2 (review of #265), the statement half: `(id, [])` reaches the DELETE
    and no INSERT. Mutation killed: the DELETE under `if buyer_contacts:`."""
    from core import pg_buyer_rows

    monkeypatch.setattr("core.pg_derivation.mark", AsyncMock())
    conn = _Conn()

    written = await pg_buyer_rows._write_buyer_rows(conn, [(7, "Покупець 7")], [(7, [])])

    assert written == 0
    assert [(sql, args) for sql, args in conn.calls
            if sql in (pg_buyer_rows._DELETE_CONTACTS, pg_buyer_rows._INSERT_CONTACT)] == [
        (pg_buyer_rows._DELETE_CONTACTS, (7,))]
