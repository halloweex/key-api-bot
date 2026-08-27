"""bronze.buyers / bronze.buyer_contacts — the buyer landing, mirrored.

Step 2 of «Одна бронза». `upsert_buyers` writes what the KeyCRM buyers API
returned, so the mirror takes the same parsed Buyer batch in the same call —
`mirror_products`' shape, with `mirror_orders`' feeding pattern: the sync is a
delta by construction (it fetches only buyers referenced by orders and missing
from the table), so history crosses once via `backfill_buyers`, an ids-diff
with no cursor, and the daily comparison is gated on `backfilled_at`.

Contacts follow the line-items rule: DELETE + INSERT per buyer, never an
upsert — a buyer whose phone list shrank must not keep the old number in one
store only. The key is the natural triple `(buyer_id, contact_type, value)`;
DuckDB's sequence id is deliberately not carried, because a generated id would
give one contact two names and the comparison would be meaningless before it
began (`stock_movements`' lesson, solved by not having the column).

Timestamps arrive as strings on the Buyer model and are parsed here, loudly:
a value Python cannot parse raises and lands in the mirror's failure record,
because DuckDB will have stored it — a silent None would be a discrepancy
this module manufactured.

Never raises out of `mirror_buyers`: failures are logged and recorded in
`meta.mirror_state`, `pg_landing`'s contract — the sync must not die because
the mirror hiccuped.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

BUYERS_STATE = "bronze.buyers"
CONTACTS_STATE = "bronze.buyer_contacts"

# The shared contract: what both stores hold for a buyer, bookkeeping aside.
BUYER_COLUMNS: Tuple[str, ...] = (
    "id", "full_name", "birthday", "note", "phone", "email",
    "manager_id", "company_id", "company_name", "city", "region",
    "loyalty_program_name", "loyalty_level_name",
    "loyalty_discount", "loyalty_amount",
    "created_at", "updated_at",
)

CONTACT_COLUMNS: Tuple[str, ...] = ("buyer_id", "contact_type", "value", "is_primary")

_UPSERT_BUYER = f"""
INSERT INTO bronze.buyers ({", ".join(BUYER_COLUMNS)})
VALUES ({", ".join(f"${i}" for i in range(1, len(BUYER_COLUMNS) + 1))})
ON CONFLICT (id) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in BUYER_COLUMNS if c != "id")},
    mirrored_at = now()
"""

_WATERMARK_OK = """
INSERT INTO meta.mirror_state
       (table_name, last_attempted_at, last_ok_at,
        failures_since_ok, last_error, last_rows)
VALUES ($1, now(), now(), 0, NULL, $2)
ON CONFLICT (table_name) DO UPDATE SET
    last_attempted_at = now(),
    last_ok_at        = now(),
    failures_since_ok = 0,
    last_error        = NULL,
    last_rows         = EXCLUDED.last_rows
"""


def _as_date(value: Any) -> Optional[date]:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _as_ts(value: Any) -> Optional[datetime]:
    if value is None or isinstance(value, datetime):
        return value
    # KeyCRM serialises as ISO with either 'T' or a space; both parse.
    return datetime.fromisoformat(str(value).replace(" ", "T"))


def buyer_row(buyer: Any) -> Tuple[Any, ...]:
    """One Buyer model rendered as the tuple both stores agree on."""
    return (
        buyer.id, buyer.full_name, _as_date(buyer.birthday), buyer.note,
        buyer.phone, buyer.email, buyer.manager_id, buyer.company_id,
        buyer.company_name, buyer.city, buyer.region,
        buyer.loyalty_program_name, buyer.loyalty_level_name,
        buyer.loyalty_discount, buyer.loyalty_amount,
        _as_ts(buyer.created_at), _as_ts(buyer.updated_at),
    )


def contact_rows(buyer: Any) -> List[Tuple[int, str, str, bool]]:
    """The contact list exactly as `upsert_buyers` writes it: first is
    primary, empties skipped, duplicates collapsed by the natural key."""
    rows: Dict[Tuple[int, str, str], bool] = {}
    for kind, values in (("phone", buyer.phones), ("email", buyer.emails)):
        for i, value in enumerate(values or []):
            if value:
                rows.setdefault((buyer.id, kind, value), i == 0)
    return [(b, k, v, p) for (b, k, v), p in rows.items()]


async def _write(buyers_rows, contacts_by_buyer) -> None:
    """Upsert buyers, replace their contacts, move both watermarks — one
    transaction, `pg_landing._write`'s reasoning."""
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            if buyers_rows:
                await conn.executemany(_UPSERT_BUYER, buyers_rows)
                contact_count = 0
                for buyer_id, contacts in contacts_by_buyer:
                    await conn.execute(
                        "DELETE FROM bronze.buyer_contacts WHERE buyer_id = $1",
                        buyer_id,
                    )
                    if contacts:
                        await conn.executemany(
                            """
                            INSERT INTO bronze.buyer_contacts
                                   (buyer_id, contact_type, value, is_primary)
                            VALUES ($1, $2, $3, $4)
                            """,
                            contacts,
                        )
                        contact_count += len(contacts)
            else:
                contact_count = 0
            await conn.execute(_WATERMARK_OK, BUYERS_STATE, len(buyers_rows))
            await conn.execute(_WATERMARK_OK, CONTACTS_STATE, contact_count)


async def _record_failure(error: str) -> None:
    try:
        from core.pg import get_pool

        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO meta.mirror_state
                       (table_name, last_attempted_at, failures_since_ok, last_error)
                VALUES ($1, now(), 1, $2)
                ON CONFLICT (table_name) DO UPDATE SET
                    last_attempted_at = now(),
                    failures_since_ok = meta.mirror_state.failures_since_ok + 1,
                    last_error        = EXCLUDED.last_error
                """,
                BUYERS_STATE, error[:2000],
            )
    except Exception as exc:  # pragma: no cover - double fault
        logger.debug("pg_buyers: could not record the failure either: %s", exc)


async def mirror_buyers(buyers: Sequence[Any]) -> Dict[str, Any]:
    """Ship one parsed Buyer batch. Never raises."""
    from core import pg_landing

    if not pg_landing.enabled():
        return {"skipped": "KS_PG_DSN is not set"}
    if not buyers:
        return {"rows": 0}
    try:
        rows = [buyer_row(b) for b in buyers]
        contacts = [(b.id, contact_rows(b)) for b in buyers]
        await _write(rows, contacts)
        logger.info("pg_buyers: mirrored %d buyer(s)", len(rows))
        return {"rows": len(rows)}
    except Exception as e:
        detail = f"{type(e).__name__}: {e}"
        logger.error("pg_buyers: mirror failed: %s", detail)
        await _record_failure(detail)
        return {"error": detail}


# ─── The backfill: history across once, no cursor ────────────────────────────

_DK_BUYERS_SELECT = f"""
    SELECT {", ".join(BUYER_COLUMNS)} FROM buyers WHERE id IN ({{ph}})
"""

_DK_CONTACTS_SELECT = """
    SELECT buyer_id, contact_type, value, is_primary
    FROM buyer_contacts WHERE buyer_id IN ({ph})
"""


async def backfill_buyers(store, *, chunk: int = 2000) -> Dict[str, Any]:
    """Ship the buyers Postgres is missing; idempotent, resumes by recompute.

    `core/pg_backfill.py`'s shape: ask both stores which ids they hold, ship
    the difference, stamp `backfilled_at` only on a run that finished with
    nothing remaining. It does not repair a buyer present on both sides and
    differing — that is the daily comparison's business.
    """
    from core.pg import get_pool

    pool = await get_pool()
    async with store.connection() as conn:
        dk_ids = {r[0] for r in conn.execute("SELECT id FROM buyers").fetchall()}
    async with pool.acquire() as conn:
        pg_ids = {r[0] for r in await conn.fetch("SELECT id FROM bronze.buyers")}

    missing = sorted(dk_ids - pg_ids)
    shipped = 0
    for start in range(0, len(missing), chunk):
        ids = missing[start:start + chunk]
        ph = ", ".join("?" for _ in ids)
        async with store.connection() as conn:
            buyer_rows = conn.execute(
                _DK_BUYERS_SELECT.format(ph=ph), ids
            ).fetchall()
            contact_rows_ = conn.execute(
                _DK_CONTACTS_SELECT.format(ph=ph), ids
            ).fetchall()
        by_buyer: Dict[int, List[Tuple[int, str, str, bool]]] = {i: [] for i in ids}
        for row in contact_rows_:
            by_buyer[row[0]].append((row[0], row[1], row[2], bool(row[3])))
        await _write(
            [tuple(r) for r in buyer_rows],
            list(by_buyer.items()),
        )
        shipped += len(buyer_rows)

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE meta.mirror_state SET backfilled_at = now() "
            "WHERE table_name = $1",
            BUYERS_STATE,
        )
    return {"shipped": shipped, "missing_was": len(missing)}
