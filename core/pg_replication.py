"""Replicating what KeyCRM cannot re-supply: the manager classification.

Everything in `core/pg_landing.py` is a *mirror* — a KeyCRM payload parsed once
and handed to two stores, recoverable from the source if Postgres is ever lost.
This module is deliberately not that, and the distinction is the whole point of
keeping it in a separate file.

WHY THIS CANNOT BE A MIRROR

`sales_type` is decided by `silver_sales_type_case()`, and four of its six
branches read `manager_classifications` and `managers.is_retail`. KeyCRM does
not know whether a manager sells retail, wholesale, internally, or ships to
bloggers — **only a human knows**, and `upsert_managers` seeds `is_retail` from
`RETAIL_MANAGER_IDS` for managers it has never seen and never touches it after.
That rule exists because recomputing the column on every sync made it
unfixable: whatever a human set was overwritten within the minute, and eight
managers sold ₴3.1M into the wrong bucket for months.

A payload-fed mirror would re-seed from the same constant, the two stores would
disagree about who is retail, and the Gold reconciliation that follows would be
measuring the classification rather than the migration. So these rows are read
out of DuckDB and written to Postgres as they stand.

**Replicated, not migrated.** DuckDB remains where a classification is decided
for the whole parallel period; this is a read-only copy that exists so Silver
can be computed on the Postgres side against the same answer.

WHY IT IS A FULL REPLACE AND NOT AN UPSERT

An interval can be *deleted* — a reclassification closes one row and opens
another, and correcting a mistake removes one. An upsert leaves the deleted row
behind, and a stale interval does not fail loudly: it silently classifies some
orders in the past. Twenty managers and a few dozen intervals cost nothing to
rewrite whole, and a full replace cannot leave a ghost.

Both tables go in one transaction, because `silver_sales_type_case()` reads
them together and a half-applied pair is a window in which `sales_type` is
decided from one store's managers and the other's intervals.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Sequence, Tuple

logger = logging.getLogger(__name__)

MANAGERS_TABLE = "bronze.managers"
CLASSIFICATIONS_TABLE = "app.manager_classifications"

MANAGER_COLUMNS: Tuple[str, ...] = (
    "id", "name", "email", "status", "is_retail",
    "first_order_date", "last_order_date", "order_count",
)
CLASSIFICATION_COLUMNS: Tuple[str, ...] = (
    "manager_id", "is_retail", "valid_from", "valid_to", "set_by", "set_at",
    "note",
)


def read_managers(conn) -> Tuple[List[tuple], List[tuple]]:
    """Both tables out of DuckDB, in the column order Postgres expects."""
    managers = conn.execute(
        f"SELECT {', '.join(MANAGER_COLUMNS)} FROM managers ORDER BY id"
    ).fetchall()
    classifications = conn.execute(
        f"SELECT {', '.join(CLASSIFICATION_COLUMNS)} FROM manager_classifications "
        "ORDER BY manager_id, valid_from"
    ).fetchall()
    return [tuple(r) for r in managers], [tuple(r) for r in classifications]


def _insert(table: str, columns: Sequence[str]) -> str:
    values = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values})"


async def write_managers(
    managers: Sequence[tuple], classifications: Sequence[tuple],
) -> None:
    """Replace both tables and move both watermarks. Raises.

    Refuses an empty manager list. `managers` is never legitimately empty in a
    running system — KeyCRM always returns at least the accounts placing
    orders — so an empty read is a failure somewhere upstream, and replacing a
    populated table with nothing would take `sales_type` with it: every order
    whose manager vanished falls through to `internal`, which is admin-only and
    would empty the dashboard for everyone else.
    """
    from core.pg import get_pool
    from core.pg_landing import _WATERMARK_OK

    if not managers:
        raise ValueError(
            "refusing to replicate an empty manager list — see write_managers"
        )

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"DELETE FROM {CLASSIFICATIONS_TABLE}")
            await conn.execute(f"DELETE FROM {MANAGERS_TABLE}")
            await conn.executemany(
                _insert(MANAGERS_TABLE, MANAGER_COLUMNS), managers,
            )
            if classifications:
                await conn.executemany(
                    _insert(CLASSIFICATIONS_TABLE, CLASSIFICATION_COLUMNS),
                    classifications,
                )
            await conn.execute(_WATERMARK_OK, MANAGERS_TABLE, len(managers))
            await conn.execute(
                _WATERMARK_OK, CLASSIFICATIONS_TABLE, len(classifications),
            )


async def replicate_managers(store) -> Dict[str, Any]:
    """Copy the classification from DuckDB to Postgres. Never raises.

    Same failure policy as the mirror, for the same reason: this is called from
    the sync path and from an admin endpoint, and neither may be broken by
    Postgres being unreachable. A failure is counted, logged at ERROR and
    written to the watermark — never swallowed at DEBUG, which is the shape of
    the 2026-08-09 incident.
    """
    from core import pg_landing

    out: Dict[str, Any] = {
        "managers": 0, "classifications": 0, "ok": False, "skipped": None,
        "error": None,
    }

    if not pg_landing.enabled():
        out["skipped"] = f"{pg_landing.MIRROR_ENV} is off"
        return out

    try:
        async with store.connection() as conn:
            managers, classifications = read_managers(conn)
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
        logger.error("replicate: could not read DuckDB: %s", out["error"])
        return out

    out["managers"] = len(managers)
    out["classifications"] = len(classifications)

    try:
        await write_managers(managers, classifications)
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        out["error"] = detail
        for table, rows in (
            (MANAGERS_TABLE, len(managers)),
            (CLASSIFICATIONS_TABLE, len(classifications)),
        ):
            pg_landing._record_error(table, rows, detail)
            await pg_landing._record_failure(table, detail)
        return out

    out["ok"] = True
    pg_landing._record_ok(MANAGERS_TABLE, len(managers))
    pg_landing._record_ok(CLASSIFICATIONS_TABLE, len(classifications))
    return out
