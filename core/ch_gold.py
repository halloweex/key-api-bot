"""gold.daily_revenue in ClickHouse — step 4 of «Одна бронза»: a copy, on purpose.

This module does deliberately little. It copies Postgres `gold.daily_revenue`
into ClickHouse verbatim, hourly, and verifies the copy daily. It does NOT
compute anything: `uniqExact` states and the real ClickHouse derivation arrive
with step 5 (silver), and pretending otherwise here would make step 4 look
finished when the only thing it proves is plumbing. What this step exercises is
exactly the three things that have never existed in this system:

  1. connectivity — the web container reaching `ks-clickhouse` over the
     `ks-data` network (docker-compose.yml grew the membership);
  2. the shipping shape — staging table + atomic `EXCHANGE TABLES`, because
     ClickHouse has no transactions and TRUNCATE+INSERT on the live table
     would give a reader an empty table mid-refresh;
  3. the daily comparison against a second engine, with its findings riding
     the same `mirror_landing` layer as every other copy this system keeps.

WHY THE COMPARISON SHIPS FIRST AND THEN COMPARES

Postgres Gold is rebuilt every ~10 minutes and carries no per-row timestamp,
so a copy that is legitimately an hour old differs from the source on every
cell of today — a zero-tolerance comparison against a *stale* copy would cry
every morning about freshness it was never meant to measure. Freshness is the
watermark's job (`meta.mirror_state`, row `clickhouse.gold_daily_revenue`).
So `reconcile_ch_gold` ships a fresh copy and then reads it back: what it
measures is round-trip fidelity — types, decimals, NULLs, encoding, row count —
which is the thing a copy pipeline can actually break. This is not the
comparison repairing what it measures: shipping is the pipeline's normal
hourly operation, and the read-back never touches the thing being verified.

TOLERANCE IS ZERO ON EVERY COLUMN, INCLUDING avg_order_value

The cent allowance in `reconcile_gold` exists because two engines *compute*
the value and DECIMAL division rounds differently. Here nothing computes —
both sides hold the same written number, and a copy that differs by a cent is
a defect in the copy.

SUPERSEDED BY STEP 5, KEPT AS THE FALLBACK AND THE CLIENT

`core/ch_silver.py` now ships silver and derives Gold *inside* ClickHouse,
and the scheduler's hourly tick calls it instead of `ship_gold`. This module
stays as the copy-mode fallback (a Gold copy needs nothing but bronze-free
connectivity) and as the home of the Gold table DDL, which step 5 imports
rather than restates. The shared HTTP client and the rest of the plumbing
moved to `core/ch_common.py`.

CONFIGURATION

`KS_CH_URL` (e.g. `http://ks-clickhouse:8123`) turns the module on; absent, a
scheduler tick and the reconciliation both stand down silently — the same
shape as `KS_PG_DSN` gating the mirror. `KS_CH_USER` defaults to `ks_app`,
which the platform has already provisioned with rights on `gold.*`;
`KS_CH_PASSWORD` comes from `.env`. Credentials travel as headers, never in
the URL, so they cannot leak into a logged query string.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core.ch_common import (
    PASSWORD_ENV,
    URL_ENV,
    USER_ENV,
    configured,
    date_sample_id,
    ensure_database,
    execute as _execute,
    mark_failed as _mark_failed_common,
    mark_ok as _mark_ok_common,
    parse_tsv as _parse_generic,
    render_tsv as _render_generic,
)
from core.data_quality import IntegrityIssue, Severity
from core.pg_gold import GOLD_COLUMNS as COLUMNS

logger = logging.getLogger(__name__)

# The row in meta.mirror_state. Prefixed so the store is unmistakable in a
# listing where every other row is a Postgres table.
STATE_ROW = "clickhouse.gold_daily_revenue"

TABLE = "gold.daily_revenue"
STAGING = "gold.daily_revenue_staging"

# COLUMNS is imported from core.pg_gold.GOLD_COLUMNS — the one home for the
# cell shape; a test pins the identity so a 0012 cannot drift this copy.

_DECIMAL_COLUMNS = {"revenue", "returns_revenue", "avg_order_value"}
_INT_COLUMNS = {
    "orders_count", "unique_customers", "new_customers",
    "returning_customers", "returns_count",
}

# MergeTree, not Replacing: the table is replaced whole via EXCHANGE, so there
# are never two versions of a row for a Replacing engine to collapse. ORDER BY
# stays non-nullable on purpose — source_id (NULL = the roll-up) rides as a
# data column, because MergeTree enforces no uniqueness either way and the
# comparison keys on all three columns itself.
_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {name} (
    date                Date,
    sales_type          LowCardinality(String),
    source_id           Nullable(Int32),
    revenue             Decimal(14, 2),
    orders_count        Int32,
    unique_customers    Int32,
    new_customers       Int32,
    returning_customers Int32,
    returns_count       Int32,
    returns_revenue     Decimal(14, 2),
    avg_order_value     Decimal(12, 2)
)
ENGINE = MergeTree
ORDER BY (date, sales_type)
"""


# configured() and the HTTP client live in core.ch_common now; `_execute`
# is imported under its old name so tests keep patching ch_gold._execute.


# ─── TSV: the wire format both directions ────────────────────────────────────
#
# TabSeparated with ClickHouse's own escaping. sales_type is the only string
# column and its values are a known closed set, but the escaping is written
# anyway: a format that is correct only for today's data is a format that
# breaks silently.

_GOLD_TYPES = {
    "date": "date", "sales_type": "str",
    **{c: "int" for c in _INT_COLUMNS}, "source_id": "int",
    **{c: "decimal" for c in _DECIMAL_COLUMNS},
}


def render_tsv(rows):
    return _render_generic(rows, COLUMNS, _GOLD_TYPES)


def parse_tsv(text):
    return _parse_generic(text, COLUMNS, _GOLD_TYPES)


def _key(row: Tuple[Any, ...]) -> Tuple[Any, Any, Any]:
    return (row[0], row[1], row[2])


def _sample_id(row_key: Tuple[Any, Any, Any]) -> int:
    return date_sample_id(row_key[0])


# ─── Postgres side ───────────────────────────────────────────────────────────

async def _fetch_pg_rows() -> List[Tuple[Any, ...]]:
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch(
            f"SELECT {', '.join(COLUMNS)} FROM gold.daily_revenue"
        )
    return [tuple(r[c] for c in COLUMNS) for r in records]


async def _watermark_ok(rows: int) -> None:
    await _mark_ok_common(STATE_ROW, rows)


async def _watermark_failed(error: str) -> None:
    await _mark_failed_common(STATE_ROW, error)


# ─── The shipper ─────────────────────────────────────────────────────────────

async def _ensure_database(name: str) -> None:
    await ensure_database(name, execute=_execute)


async def ensure_schema() -> None:
    await _ensure_database("gold")
    await _execute(_TABLE_DDL.format(name=TABLE))
    await _execute(_TABLE_DDL.format(name=STAGING))


async def _ship(rows: Sequence[Tuple[Any, ...]]) -> None:
    """Stage, count, EXCHANGE. Raises on any fault.

    The count check runs BEFORE the swap, so a truncated INSERT leaves the
    live table untouched and the failure in the watermark, instead of leaving
    a short table for the reader and a success for the operator.
    """
    await ensure_schema()
    await _execute(f"TRUNCATE TABLE {STAGING}")
    if rows:
        await _execute(
            f"INSERT INTO {STAGING} ({', '.join(COLUMNS)}) FORMAT TabSeparated",
            body=render_tsv(rows),
        )
    staged = int((await _execute(f"SELECT count() FROM {STAGING}")).strip() or 0)
    if staged != len(rows):
        raise RuntimeError(
            f"staging holds {staged} row(s), Postgres supplied {len(rows)} — "
            f"refusing to EXCHANGE a short copy"
        )
    await _execute(f"EXCHANGE TABLES {TABLE} AND {STAGING}")


async def ship_gold() -> Dict[str, Any]:
    """Copy Postgres gold.daily_revenue into ClickHouse, atomically.

    Never raises — the scheduler wrapper's contract.
    """
    if not configured():
        return {"skipped": f"{URL_ENV} is not set"}
    try:
        rows = await _fetch_pg_rows()
        await _ship(rows)
        await _watermark_ok(len(rows))
        logger.info("ch_gold: shipped %d row(s)", len(rows))
        return {"rows": len(rows)}
    except Exception as e:
        detail = f"{type(e).__name__}: {e}"
        logger.error("ch_gold: ship failed: %s", detail)
        await _watermark_failed(detail)
        return {"error": detail}


# ─── The comparison ──────────────────────────────────────────────────────────

async def _fetch_ch_rows() -> List[Tuple[Any, ...]]:
    text = await _execute(
        f"SELECT {', '.join(COLUMNS)} FROM {TABLE} FORMAT TabSeparated"
    )
    return parse_tsv(text)


def compare_rows(
    shipped: Sequence[Tuple[Any, ...]],
    readback: Sequence[Tuple[Any, ...]],
    *,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Zero-tolerance comparison of the shipped snapshot against the read-back.

    Pure, so the tests can hold it still. Keys on (date, sales_type,
    source_id); every other column must be `==` — Decimals against Decimals,
    which is exactly why `parse_tsv` types its cells.
    """
    issues: List[IntegrityIssue] = []
    want = {_key(r): r for r in shipped}
    got = {_key(r): r for r in readback}

    missing = sorted(k for k in want if k not in got)
    extra = sorted(k for k in got if k not in want)
    differing: List[Tuple[Any, Any, Any]] = []
    detail = ""
    for k in want:
        if k in got and want[k] != got[k]:
            differing.append(k)
            if not detail:
                for name, a, b in zip(COLUMNS, want[k], got[k]):
                    if a != b:
                        detail = f"first: {k} {name} {a!r} → {b!r}"
                        break
    differing.sort()

    if missing:
        issues.append(IntegrityIssue(
            check_name="ch_gold_row_missing",
            table_name=TABLE,
            severity=Severity.CRITICAL,
            count=len(missing),
            sample_ids=tuple(_sample_id(k) for k in missing[:max_samples]),
            description=(
                f"{len(missing)} cell(s) shipped to ClickHouse did not come "
                f"back — the copy lost rows"
            ),
        ))
    if extra:
        issues.append(IntegrityIssue(
            check_name="ch_gold_row_extra",
            table_name=TABLE,
            severity=Severity.CRITICAL,
            count=len(extra),
            sample_ids=tuple(_sample_id(k) for k in extra[:max_samples]),
            description=(
                f"{len(extra)} cell(s) in ClickHouse were never shipped — the "
                f"copy invented rows"
            ),
        ))
    if differing:
        issues.append(IntegrityIssue(
            check_name="ch_gold_value_mismatch",
            table_name=TABLE,
            severity=Severity.CRITICAL,
            count=len(differing),
            sample_ids=tuple(_sample_id(k) for k in differing[:max_samples]),
            description=(
                f"{len(differing)} cell(s) changed value crossing the wire; "
                f"{detail}. Tolerance is zero: nothing computes here, so a "
                f"cent of drift is a defect in the copy, not in arithmetic"
            ),
        ))
    return issues


async def reconcile_ch_gold(*, max_samples: int = 10) -> List[IntegrityIssue]:
    """Ship a fresh copy, read it back, compare — see the module docstring.

    Unreachable ClickHouse is a WARN finding, not an exception: this rides
    `dq_mirror_landing`, where a raise fails the whole layer, and an optional
    store being down must not silence the comparisons of the mandatory ones.
    """
    if not configured():
        return []

    # Dormant today (step 5's reconcile superseded this), but if the fallback
    # is ever revived it shares the staging tables with _derive_gold — so it
    # takes the same layer lock, or race №3 comes back with it.
    from core.pg_silver import PG_LAYER_LOCK

    async with PG_LAYER_LOCK:
        return await _reconcile_copy_mode(max_samples=max_samples)


async def _reconcile_copy_mode(*, max_samples: int = 10) -> List[IntegrityIssue]:
    try:
        shipped = await _fetch_pg_rows()
        await _ship(shipped)
        await _watermark_ok(len(shipped))
    except Exception as e:
        detail = f"{type(e).__name__}: {e}"
        logger.error("ch_gold: reconcile could not ship: %s", detail)
        await _watermark_failed(detail)
        return [IntegrityIssue(
            check_name="ch_gold_ship_failed",
            table_name=TABLE,
            severity=Severity.WARN,
            count=1,
            description=(
                f"could not ship gold to ClickHouse: {detail}. The live table "
                f"keeps its previous copy; freshness is visible in "
                f"meta.mirror_state ({STATE_ROW})"
            ),
        )]

    try:
        readback = await _fetch_ch_rows()
    except Exception as e:
        return [IntegrityIssue(
            check_name="ch_gold_unreachable",
            table_name=TABLE,
            severity=Severity.WARN,
            count=1,
            description=f"shipped, but the read-back failed: {type(e).__name__}: {e}",
        )]

    # Compared against the exact snapshot that was shipped, held in memory —
    # never against a fresh Postgres read, which a rebuild tick could have
    # moved past the copy while the wire was busy.
    return compare_rows(shipped, readback, max_samples=max_samples)
