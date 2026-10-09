"""`core.pg_buyer_rows._write_buyer_rows` without a database: what it hands
the statements, in which order.

The integration file (`tests/integration/test_pg_buyer_rows.py`) proves what
lands in Postgres. Order is invisible there — any order lands the same rows —
and it is the one property two concurrent writers depend on: the mirror, the
reship and chain 4's writer all run this function, and chain 4's writer takes
no lock that serialises it with another buyer write first (its claim is
`ON CONFLICT DO NOTHING`), nor does the hourly `derive_gender_pg`, which takes
id-ordered share locks of its own. Two transactions writing overlapping buyers
in opposite orders deadlock; the review reproduced it in 4 runs of 5 with the
sort removed. So the order is pinned by what reaches the connection.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest


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


@pytest.mark.asyncio
async def test_buyers_and_their_contacts_are_written_in_id_order(monkeypatch):
    """F4 (review of #265). Handed in reverse, written ascending: the buyer
    upsert's rows, then one contacts DELETE per buyer and that buyer's
    INSERT. Mutations killed: `rows = [tuple(r) for r in buyer_rows]` and
    `contacts = list(contacts_by_buyer)` — the two sorts removed."""
    from core import pg_buyer_rows

    monkeypatch.setattr("core.pg_derivation.mark_if_owned", AsyncMock())
    ids = [9, 3, 5, 1]
    rows = [(i, f"Покупець {i}") for i in ids]
    contacts = [(i, [] if i == 3 else [(i, "phone", f"+38050{i}", True)])
                for i in ids]
    conn = _Conn()

    written = await pg_buyer_rows._write_buyer_rows(conn, rows, contacts)

    assert written == 3
    (upsert,) = [args for sql, args in conn.calls if sql == pg_buyer_rows._UPSERT_BUYER]
    assert [r[0] for r in upsert] == [1, 3, 5, 9]
    deleted = [args[0] for sql, args in conn.calls
               if sql == pg_buyer_rows._DELETE_CONTACTS]
    assert deleted == [1, 3, 5, 9]
    inserted = [args[0][0] for sql, args in conn.calls
                if sql == pg_buyer_rows._INSERT_CONTACT]
    assert inserted == [1, 5, 9]
    # Each buyer's contacts are replaced together: its DELETE, then its INSERT.
    order = [(("D", args[0]) if sql == pg_buyer_rows._DELETE_CONTACTS else ("I", args[0][0]))
             for sql, args in conn.calls
             if sql in (pg_buyer_rows._DELETE_CONTACTS, pg_buyer_rows._INSERT_CONTACT)]
    assert order == [("D", 1), ("I", 1), ("D", 3), ("D", 5), ("I", 5), ("D", 9), ("I", 9)]


@pytest.mark.asyncio
async def test_an_empty_contact_list_still_deletes(monkeypatch):
    """F2 (review of #265), the statement half: `(id, [])` reaches the DELETE
    and no INSERT. Mutation killed: the DELETE under `if buyer_contacts:`."""
    from core import pg_buyer_rows

    monkeypatch.setattr("core.pg_derivation.mark_if_owned", AsyncMock())
    conn = _Conn()

    written = await pg_buyer_rows._write_buyer_rows(conn, [(7, "Покупець 7")], [(7, [])])

    assert written == 0
    assert [(sql, args) for sql, args in conn.calls
            if sql in (pg_buyer_rows._DELETE_CONTACTS, pg_buyer_rows._INSERT_CONTACT)] == [
        (pg_buyer_rows._DELETE_CONTACTS, (7,))]
