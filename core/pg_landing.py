"""The mirror: the same KeyCRM payload, written to Postgres as well.

Step 05's first write. DuckDB stays the system of record for the entire
parallel period — nothing reads Postgres, no dashboard depends on it, and the
read switch is a later step entirely. What this establishes is that the two
stores can be kept in step at all, on tables whose loss costs one resync.

THE FAILURE POLICY IS THE WHOLE DESIGN

A failed Postgres write must not break the sync. DuckDB is what the business
looks at, KeyCRM is what it is built from, and a mirror that can stop either is
worse than no mirror — charter rule 8, the step leaves the system working.

But it must not be silent. That is the 2026-08-09 shape exactly: an `ALTER`
failed inside `try/except` with a DEBUG log, the code read the column anyway,
and a one-off became a rebuild every two minutes for hours. So a failure here
is caught, counted, logged at ERROR, written to the watermark when the database
is reachable enough to take it, and published on `/api/health`.

WHY A WATERMARK AND NOT JUST A COUNTER

`meta.mirror_state` is what lets the daily reconciliation tell **"not shipped
yet"** from **"shipped wrong"**. Without it every lag reads as a discrepancy,
and the first week of a mirror is nothing but lag. `last_ok_at` and
`last_attempted_at` are both kept: equal means healthy, and the gap between
them is how long this table has been behind.

THE PARSE IS SHARED, THE WRITE IS NOT

Both stores read the payload through `core.landing_rows` — one home for the
rule (charter rule 1). The parse runs twice over a thousand dicts, which costs
microseconds and buys the guarantee that neither store can drift into its own
reading of a brand.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

# One deploy turns the mirror off, which is charter rule 6's rollback. It is ON
# by default on purpose: a safety mechanism nobody exercises is not a safety
# mechanism, and this repository has the scar — a Redis client lived here for
# months without executing in any environment and was then blamed, in an audit,
# for the dashboard's latency.
MIRROR_ENV = "KS_MIRROR_LANDING"


def enabled() -> bool:
    return os.getenv(MIRROR_ENV, "1").strip().lower() not in {"0", "false", "no", ""}


@dataclass
class MirrorOutcome:
    """What one mirroring attempt did. Never raises; this is the report."""
    table: str
    rows: int = 0
    ok: bool = False
    skipped: Optional[str] = None      # why it did not run, if it did not
    error: Optional[str] = None

    @property
    def attempted(self) -> bool:
        return self.skipped is None


# Process-local, for /api/health. The watermark in Postgres is the durable
# record; this is what can still be reported when Postgres is the thing that is
# down.
_failures: Dict[str, int] = {}
_last_error: Dict[str, str] = {}


def health() -> Dict[str, Any]:
    """Mirror state as `/api/health` shows it, readable with Postgres down."""
    return {
        "enabled": enabled(),
        "failures": dict(_failures),
        "last_error": dict(_last_error),
    }


def reset_health() -> None:
    """Test seam. Production has one process and no reason to call this."""
    _failures.clear()
    _last_error.clear()


_UPSERT = """
INSERT INTO {table} ({cols}) VALUES ({vals})
ON CONFLICT (id) DO UPDATE SET {sets}, mirrored_at = now()
"""


def _statement(table: str, columns: Sequence[str]) -> str:
    cols = ", ".join(columns)
    vals = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    # `id` is the conflict target, so it is not in the SET list — assigning a
    # column to itself is legal and pointless, and leaving it out keeps the
    # statement saying what it means.
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "id")
    return _UPSERT.format(table=table, cols=cols, vals=vals, sets=sets)


async def _write(table: str, columns: Sequence[str], rows: Sequence[tuple]) -> None:
    """Upsert rows and move the watermark, in one transaction.

    One transaction on purpose: a watermark claiming a success that did not
    happen is worse than no watermark, because the reconciliation would then
    read a real gap as "already shipped".
    """
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            if rows:
                await conn.executemany(_statement(table, columns), rows)
            await conn.execute(
                """
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
                """,
                table, len(rows),
            )


async def _record_failure(table: str, error: str) -> None:
    """Best effort: say in Postgres that the mirror failed.

    Usually pointless — if the write failed because Postgres is unreachable,
    so will this. It is here for the other case, the one that actually
    misleads: a write that failed on *its own* data while the connection was
    fine. Without this the watermark would keep the last success and read as
    healthy.
    """
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
                table, error[:2000],
            )
    except Exception as exc:
        logger.debug("mirror: could not record the failure either: %s", exc)


async def _mirror(table: str, columns: Sequence[str], rows: List[tuple]) -> MirrorOutcome:
    out = MirrorOutcome(table=table, rows=len(rows))

    if not enabled():
        out.skipped = f"{MIRROR_ENV} is off"
        return out

    try:
        await _write(table, columns, rows)
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        out.error = detail
        _failures[table] = _failures.get(table, 0) + 1
        _last_error[table] = detail
        # ERROR, not DEBUG. The whole point.
        logger.error(
            "mirror: %s failed after %d row(s): %s", table, len(rows), detail,
        )
        await _record_failure(table, detail)
        return out

    out.ok = True
    _failures.pop(table, None)
    _last_error.pop(table, None)
    logger.info("mirror: %s ok, %d row(s)", table, len(rows))
    return out


async def mirror_categories(payloads: List[Dict[str, Any]]) -> MirrorOutcome:
    from core.landing_rows import CATEGORY_COLUMNS, category_rows

    rows = [tuple(r) for r in category_rows(payloads)]
    return await _mirror("bronze.categories", CATEGORY_COLUMNS, rows)


async def mirror_products(payloads: List[Dict[str, Any]]) -> MirrorOutcome:
    from core.landing_rows import PRODUCT_COLUMNS, product_rows

    rows = [tuple(r) for r in product_rows(payloads)]
    return await _mirror("bronze.products", PRODUCT_COLUMNS, rows)
