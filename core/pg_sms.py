"""The SMS tab's state, copied into Postgres.

Five of revision 0013's six tables, filled. All five are REPLICATED in
`core/pg_operational.py`'s sense — derived from state only DuckDB holds, with
no payload to re-parse, so a second computation here would diverge the first
time either side missed a tick.

The sixth, `bronze.offer_stocks`, shipped here until 2026-09-06 and now rides
`replicate_operational` instead. It is landing data, and the guard below
correctly stands this whole module down once Postgres becomes the writer — so
a table DuckDB *keeps receiving from KeyCRM* froze the moment the flag went on,
with the comparison standing down beside it. The rule that fell out of it:
**nothing under this guard may be a table DuckDB still writes.** Pinned by a
test, because the next table added here will look just as harmless.

WHY THIS EXISTS AT ALL, GIVEN THAT POSTGRES IS MEANT TO BECOME THE WRITER

It is transitional scaffolding, and saying so is what keeps it from becoming
permanent. While DuckDB still writes campaigns and opt-outs, any Postgres-side
answer about them is wrong the moment it is stale — so the copy has to be
current before a single read can move. Once the writer moves, this stands down
the way `replicate_bot_state` does under `KS_BOT_STORE=postgres`: a full
replace running after the switch would roll back every opt-out recorded since
it, once an hour, looking fine in between.

THE FRESHNESS STAMP, WHICH IS THE ONE NON-OBVIOUS PART

The comparison forgives a row still in flight, and to do that it needs to know
when the row last changed. Four of the six carry that directly. Members do not:
`record_sms_send` writes `message_id` and `delivery_status` onto the member row
with no timestamp of its own.

It stamps the *campaign* in the same transaction, though, and that is enough.
The three write paths and where each leaves a mark:

    freeze_sms_campaign   → sms_campaigns.exported_at
    record_sms_send       → sms_campaigns.sent_at   (same transaction)
    record_sms_delivery   → sms_campaign_members.delivered_at

so the row's clock is `delivered_at`, falling back to its campaign's
`sent_at`, falling back to `exported_at`. Read through a correlated subquery
rather than a join, because `read_duckdb_side` selects the stamp as one extra
expression on a single-table SELECT. The alternative was adding `updated_at` to
a live DuckDB table on the way out of that store, which is a schema change
bought to describe rows that are about to stop being written there.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

OPTOUTS_TABLE = "app.marketing_optouts"
PRESETS_TABLE = "app.sms_audience_presets"
CAMPAIGNS_TABLE = "app.sms_campaigns"
MEMBERS_TABLE = "app.sms_campaign_members"
DLR_TABLE = "app.sms_dlr_events"

# The shared contract per table: the columns both stores hold and compare.
# Bookkeeping is excluded by construction — DuckDB stamps `synced_at`,
# Postgres `mirrored_at`, and two correct copies differ on those.
OPTOUT_COLUMNS: Tuple[str, ...] = (
    "buyer_id", "channel", "phone", "reason", "source", "opted_out_at",
)

PRESET_COLUMNS: Tuple[str, ...] = (
    "name", "criteria", "created_by", "created_at", "updated_at",
)

CAMPAIGN_COLUMNS: Tuple[str, ...] = (
    "campaign", "ltv_basis", "sales_type", "holdout_pct", "criteria",
    "promocode", "exported_at", "sent_at", "notes", "message_text",
    "message_parts", "recipients_sent", "price_per_part", "cost_total",
)

MEMBER_COLUMNS: Tuple[str, ...] = (
    "campaign", "buyer_id", "phone", "tier", "assignment", "orders_at_export",
    "revenue_ltv_at_export", "margin_ltv_at_export", "recency_at_export",
    "message_id", "delivery_status", "delivered", "delivered_at",
)

DLR_COLUMNS: Tuple[str, ...] = ("event_id", "message_id", "first_seen_at")

# See the module docstring. Selected as one extra expression beside the row.
MEMBER_STAMP = (
    "COALESCE(delivered_at, (SELECT COALESCE(c.sent_at, c.exported_at) "
    "FROM sms_campaigns c WHERE c.campaign = sms_campaign_members.campaign))"
)

# (postgres table, duckdb table, columns, ORDER BY). Spelled out rather than
# derived by stripping the schema: `app.marketing_optouts` and
# `marketing_optouts` happen to agree, but a rule that guesses a table name is
# a rule that will guess wrong.
#
# Every entry must be a table DuckDB stops writing when the guard below fires —
# see the module docstring, and `test_nothing_under_the_guard_is_still_written`.
_FULL_REPLACE: Tuple[Tuple[str, str, Tuple[str, ...], str], ...] = (
    (OPTOUTS_TABLE, "marketing_optouts", OPTOUT_COLUMNS, "buyer_id, channel"),
    (PRESETS_TABLE, "sms_audience_presets", PRESET_COLUMNS, "name"),
    (CAMPAIGNS_TABLE, "sms_campaigns", CAMPAIGN_COLUMNS, "campaign"),
    (MEMBERS_TABLE, "sms_campaign_members", MEMBER_COLUMNS, "campaign, buyer_id"),
)

CHUNK = 5000


def _insert(table: str, columns: Sequence[str]) -> str:
    values = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values})"


async def _write_chunked(conn, sql: str, rows: Sequence[tuple]) -> None:
    for start in range(0, len(rows), CHUNK):
        await conn.executemany(sql, rows[start:start + CHUNK])


def _coerce(columns: Sequence[str], row: Sequence[Any]) -> tuple:
    """DuckDB's shapes into asyncpg's.

    One column needs it and the reason is revision 0009's: asyncpg refuses an
    int where a BOOLEAN is declared, and `delivered` is written by handlers
    that have historically used 1/0 for booleans elsewhere in this codebase.
    NULL survives as NULL — it is the third state, "the operator has not
    reported yet", and coercing it to False would move every pending recipient
    into the failed bucket of a table whose only purpose is measuring a number.
    """
    out = list(row)
    for i, name in enumerate(columns):
        if name == "delivered" and out[i] is not None:
            out[i] = bool(out[i])
    return tuple(out)


def source_tables(conn) -> set:
    """Which of these tables the DuckDB side actually has today.

    Not defensive programming — the source schema is genuinely mid-flight.
    `sms_dlr_events` arrives with the TurboSMS signature fix, which binds each
    delivery-report event id to the message it first named; until that lands,
    the table does not exist and a copier that assumes it does raises on every
    tick. The tables are asked for rather than assumed so that the binding
    starts being copied the day it appears, with no second deploy.

    Absence is reported by the watermark, not by a finding: `replicate_sms`
    writes no `last_ok_at` for a table it never read, and `reconcile_sms`
    compares only what both sides have. A daily finding for a merge everyone
    is already expecting is the noise the alerts charter exists to prevent.
    """
    rows = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
    return {r[0] for r in rows}


def read_full_replace(conn, present: Optional[set] = None) -> Dict[str, List[tuple]]:
    """The replaced-whole tables out of DuckDB, in Postgres' column order."""
    present = source_tables(conn) if present is None else present
    out: Dict[str, List[tuple]] = {}
    for pg_table, dk_table, columns, order_by in _FULL_REPLACE:
        if dk_table not in present:
            continue
        rows = conn.execute(
            f"SELECT {', '.join(columns)} FROM {dk_table} ORDER BY {order_by}"
        ).fetchall()
        out[pg_table] = [_coerce(columns, r) for r in rows]
    return out


def read_dlr_appends(conn, since) -> List[tuple]:
    """The delivery-report bindings Postgres does not have yet.

    `>=` and not `>`, `inventory_sku_history`'s reason: several events can share
    one `first_seen_at`, so a strict comparison drops every binding that landed
    in the same instant as the watermark. Re-shipping the boundary costs a
    conflict the insert already ignores; skipping it costs a security control
    its row.
    """
    if since is None:
        rows = conn.execute(
            f"SELECT {', '.join(DLR_COLUMNS)} FROM sms_dlr_events "
            "ORDER BY first_seen_at, event_id"
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT {', '.join(DLR_COLUMNS)} FROM sms_dlr_events "
            "WHERE first_seen_at >= ? ORDER BY first_seen_at, event_id",
            [since],
        ).fetchall()
    return [tuple(r) for r in rows]


async def replicate_sms(store, *, full: bool = False) -> Dict[str, Any]:
    """Copy the five SMS-state tables from DuckDB to Postgres. Never raises.

    `full=True` ignores the delivery-report watermark and re-ships the whole
    binding table. It is the repair path for a finding the daily comparison
    raised — a row lost *below* the watermark is invisible to `first_seen_at >=
    MAX(first_seen_at)` forever — and it is never taken on a schedule.

    The DuckDB reads and the Postgres round trips are in separate blocks:
    the store's lock is not reentrant and every other reader waits behind it,
    so awaiting the network while holding it would stall the dashboard for the
    length of the copy.

    The watermark is read **before** the DuckDB read, so a binding written
    between the two is re-shipped rather than skipped. The other order loses
    rows; this one costs a duplicate the `ON CONFLICT DO NOTHING` absorbs.
    """
    from core.mirror_reconciliation import configured

    if not configured():
        return {"skipped": "KS_PG_DSN is not set"}

    # The guard `replicate_bot_state` earned. Once Postgres is the writer, a
    # full replace out of DuckDB rolls back every opt-out and every delivery
    # report recorded since the switch — once an hour, looking healthy in
    # between, and an opt-out rolled back is a message sent to somebody who
    # asked not to receive one.
    if sms_store_is_postgres():
        return {"skipped": "KS_SMS_STORE=postgres — DuckDB is no longer the writer"}

    started = time.monotonic()
    try:
        from core.pg import get_pool, require_revision
        from core.pg_landing import _WATERMARK_OK

        pool = await get_pool()
        await require_revision()

        if full:
            since = None
        else:
            async with pool.acquire() as conn:
                since = await conn.fetchval(
                    f"SELECT MAX(first_seen_at) FROM {DLR_TABLE}"
                )

        async with store.connection() as conn:
            present = source_tables(conn)
            replaced = read_full_replace(conn, present)
            has_dlr = "sms_dlr_events" in present
            dlr = read_dlr_appends(conn, since) if has_dlr else []
            dlr_total = conn.execute(
                "SELECT COUNT(*) FROM sms_dlr_events"
            ).fetchone()[0] if has_dlr else 0

        async with pool.acquire() as conn:
            async with conn.transaction():
                for pg_table, dk_table, columns, _order in _FULL_REPLACE:
                    if pg_table not in replaced:
                        continue
                    rows = replaced[pg_table]
                    await conn.execute(f"DELETE FROM {pg_table}")
                    if rows:
                        await _write_chunked(conn, _insert(pg_table, columns), rows)
                    await conn.execute(_WATERMARK_OK, pg_table, len(rows))

                if has_dlr:
                    if dlr:
                        await _write_chunked(
                            conn,
                            _insert(DLR_TABLE, DLR_COLUMNS)
                            + " ON CONFLICT (event_id) DO NOTHING",
                            dlr,
                        )
                    await conn.execute(_WATERMARK_OK, DLR_TABLE, dlr_total)

        missing = [dk for _pg, dk, _c, _o in _FULL_REPLACE if dk not in present]
        if not has_dlr:
            missing.append("sms_dlr_events")
        if missing:
            logger.warning(
                "SMS replication skipped %s — not in this DuckDB yet; their "
                "watermarks stay unset until the table arrives",
                ", ".join(missing),
            )

        elapsed = time.monotonic() - started
        counts = {t: len(rows) for t, rows in replaced.items()}
        if has_dlr:
            counts[DLR_TABLE] = len(dlr)
        logger.info(
            "SMS state replicated in %.2fs: %s",
            elapsed,
            ", ".join(f"{t.split('.')[-1]}={n}" for t, n in counts.items()),
        )
        return {"tables": counts, "elapsed_s": round(elapsed, 3), "full": full}

    except Exception as exc:  # noqa: BLE001 — a copy must not take the tick down
        logger.error("SMS replication failed: %s", exc, exc_info=True)
        await _record_failure(str(exc))
        return {"error": str(exc)}


async def _record_failure(error: str) -> None:
    """Leave the failure where the watermark check will find it.

    Silence here is the failure mode that matters: the tables look present and
    merely stop moving, which reads as "nothing happened" rather than "the copy
    is broken".
    """
    from core.pg_landing import _record_failure as _landing_failure

    for table in (*[t for t, _d, _c, _o in _FULL_REPLACE], DLR_TABLE):
        await _landing_failure(table, error)


def sms_store_is_postgres() -> bool:
    """Which store answers `/sms`.

    `KS_BOT_STORE`'s rule, for `KS_BOT_STORE`'s reason: an unknown value
    raises rather than falling back. A typo in the one variable deciding where
    six thousand names and phone numbers are read from should stop the
    container, not quietly point it at the copy that is about to go stale.
    """
    import os

    value = os.getenv("KS_SMS_STORE", "duckdb").strip().lower()
    if value not in ("duckdb", "postgres"):
        raise ValueError(
            f"KS_SMS_STORE must be 'duckdb' or 'postgres', got {value!r}"
        )
    return value == "postgres"


# The names still answered by DuckDB alone. Empty: every path `/sms` uses now
# runs against whichever store `KS_SMS_STORE` names. The guard and this tuple
# stay rather than being deleted — the next thing to move onto two engines will
# want the same half-switch protection, and a mechanism removed the day it
# first reaches empty is a mechanism nobody rebuilds in time.
UNPORTED: Tuple[str, ...] = ()


def refuse_while_unported(operation: str) -> None:
    """Stop a half-switched `/sms` from writing to the store nobody is reading.

    `KS_SMS_STORE=postgres` moves the audience read. It does not yet move the
    roster, the opt-outs or the presets, and a flag that moved one without the
    other is the exact failure this codebase keeps paying for: the audience
    would be computed from Postgres while the campaign it produced was frozen
    into DuckDB, so the frozen roster and the store being read would drift
    apart silently — and a campaign whose roster is not the audience it was
    built from cannot be measured at all.

    So the unported half refuses, loudly, rather than writing somewhere the
    reader will not look. When each name below moves, it drops off `UNPORTED`
    and this call goes with it; `tests/unit/test_pg_sms_read.py` fails if the
    list and the guarded methods stop agreeing.
    """
    if sms_store_is_postgres():
        raise NotImplementedError(
            f"{operation} still writes DuckDB, and KS_SMS_STORE=postgres has "
            f"moved the audience read to Postgres. Flipping one without the "
            f"other would freeze a roster into a store nobody is reading. "
            f"Unset KS_SMS_STORE until the remaining paths are ported."
        )
