"""The one set of statements that writes buyer rows into Postgres.

Two paths will write `bronze.buyers` and `bronze.buyer_contacts`: the mirror
of DuckDB's landing (`core.pg_buyers`), which ships what the sync already
wrote there, and chain 4's writer, which writes Postgres directly once
`KS_WRITE_BUYERS` moves the buyers. Both are handed the same parsed rows
(`core.landing_rows.parse_buyers`), and this module is what they both run, so
the two cannot come to disagree about what a buyer looks like in Postgres —
the same argument as `landing_rows` one layer up.

It is deliberately not the chain module. The registry walk in
`tests/unit/test_write_chains.py` derives the shipping units from the
functions that write them and leaves registered chain modules out; a core
living in the chain would take `BUYER_UNIT` out of that derivation the moment
the chain registered. Here the unit stays derived from the one function that
writes it, for the mirror and for the chain alike.

What it does, on the caller's connection and in the caller's transaction:

- every buyer upserted whole, `mirrored_at = now()` on UPDATE as on INSERT
  (the column's DEFAULT) — the search index picks up a changed buyer by that
  column, so an UPDATE that left it alone would be a change nobody indexes;
- every buyer's contacts deleted and written again, never upserted: a phone
  list that shrank must not keep the old number in one store only;
- both in id order, so two writers updating overlapping buyers take their row
  locks in the same order;
- the derivation mark last, and only when a buyer was written.

What it does not do: open a transaction of its own around the rows, or touch
`meta.mirror_state`. The rows go on the caller's transaction because chain 4
proves its first write by the owner row sharing an `xmin` with the row it was
taken for, and a savepoint would give the rows a subtransaction's XID. The
watermark belongs to the mirror — the chain must never stamp it, or the daily
comparison reads the chain's writes as the mirror shipping over them. Only the
mark sits in a savepoint of its own, which `mark()` opens anyway and the
derivation guard reads.
"""
from __future__ import annotations

from typing import Any, Iterable, List, Sequence, Tuple

from core.landing_rows import BUYER_COLUMNS

_UPSERT_BUYER = f"""
INSERT INTO bronze.buyers ({", ".join(BUYER_COLUMNS)})
VALUES ({", ".join(f"${i}" for i in range(1, len(BUYER_COLUMNS) + 1))})
ON CONFLICT (id) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in BUYER_COLUMNS if c != "id")},
    mirrored_at = now()
"""

_DELETE_CONTACTS = "DELETE FROM bronze.buyer_contacts WHERE buyer_id = $1"

_INSERT_CONTACT = """
INSERT INTO bronze.buyer_contacts (buyer_id, contact_type, value, is_primary)
VALUES ($1, $2, $3, $4)
"""


async def _write_buyer_rows(
    conn,
    buyer_rows: Sequence[Sequence[Any]],
    contacts_by_buyer: Iterable[Tuple[Any, List[Sequence[Any]]]],
) -> int:
    """Write the buyers and replace their contacts; return how many contacts
    were written. Runs on `conn`'s current transaction and never opens one
    around the rows (the module docstring says why)."""
    if not buyer_rows:
        return 0
    rows = sorted((tuple(r) for r in buyer_rows), key=lambda r: r[0])
    contacts = sorted(contacts_by_buyer, key=lambda pair: pair[0])

    await conn.executemany(_UPSERT_BUYER, rows)
    written = 0
    for buyer_id, buyer_contacts in contacts:
        await conn.execute(_DELETE_CONTACTS, buyer_id)
        if buyer_contacts:
            await conn.executemany(_INSERT_CONTACT, buyer_contacts)
            written += len(buyer_contacts)

    # `app.customer_profile` joins `bronze.buyers`. A no-op unless
    # KS_PG_DERIVE=own; `core/pg_derivation.py` says why it cannot cost this
    # transaction.
    from core.pg_derivation import mark_if_owned

    async with conn.transaction():
        await mark_if_owned(conn)
    return written
