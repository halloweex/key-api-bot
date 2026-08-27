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
connectivity) and as the home of the shared HTTP client (`_execute`) and the
Gold table DDL, which step 5 imports rather than restates.

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

from core.data_quality import IntegrityIssue, Severity

logger = logging.getLogger(__name__)

URL_ENV = "KS_CH_URL"
USER_ENV = "KS_CH_USER"
PASSWORD_ENV = "KS_CH_PASSWORD"

# The row in meta.mirror_state. Prefixed so the store is unmistakable in a
# listing where every other row is a Postgres table.
STATE_ROW = "clickhouse.gold_daily_revenue"

TABLE = "gold.daily_revenue"
STAGING = "gold.daily_revenue_staging"

# The exact column set of Postgres gold.daily_revenue (revision 0007), in
# order. The shipper renders these, the DDL declares them, and the comparison
# walks them by index — one list, three readers.
COLUMNS: Tuple[str, ...] = (
    "date", "sales_type", "source_id",
    "revenue", "orders_count", "unique_customers", "new_customers",
    "returning_customers", "returns_count", "returns_revenue",
    "avg_order_value",
)

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


def configured() -> bool:
    return bool(os.getenv(URL_ENV, "").strip())


def _auth_headers() -> Dict[str, str]:
    headers = {"X-ClickHouse-User": os.getenv(USER_ENV, "ks_app").strip() or "ks_app"}
    password = os.getenv(PASSWORD_ENV, "")
    if password:
        headers["X-ClickHouse-Key"] = password
    return headers


async def _execute(sql: str, *, body: Optional[bytes] = None) -> str:
    """One ClickHouse HTTP round trip.

    DDL/SELECT travel as the request body; an INSERT travels as `?query=` with
    the TSV payload as the body, which is the documented way to stream data.
    httpx, because it is already in the lock (the bot brought it) and because
    urllib would block the event loop this application spends its life
    defending.
    """
    import httpx

    url = os.getenv(URL_ENV, "").strip().rstrip("/") + "/"
    params: Dict[str, str] = {}
    content: bytes
    if body is None:
        content = sql.encode("utf-8")
    else:
        params["query"] = sql
        content = body

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            url, params=params, content=content, headers=_auth_headers(),
        )
    if response.status_code != 200:
        raise RuntimeError(
            f"ClickHouse HTTP {response.status_code}: {response.text[:300]}"
        )
    return response.text


# ─── TSV: the wire format both directions ────────────────────────────────────
#
# TabSeparated with ClickHouse's own escaping. sales_type is the only string
# column and its values are a known closed set, but the escaping is written
# anyway: a format that is correct only for today's data is a format that
# breaks silently.

def _escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _unescape(value: str) -> str:
    out: List[str] = []
    i = 0
    while i < len(value):
        ch = value[i]
        if ch == "\\" and i + 1 < len(value):
            nxt = value[i + 1]
            out.append({"t": "\t", "n": "\n", "r": "\r", "\\": "\\"}.get(nxt, nxt))
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def render_tsv(rows: Sequence[Tuple[Any, ...]]) -> bytes:
    lines: List[str] = []
    for row in rows:
        cells: List[str] = []
        for value in row:
            if value is None:
                cells.append("\\N")
            elif isinstance(value, (date, datetime)):
                cells.append(value.strftime("%Y-%m-%d"))
            elif isinstance(value, str):
                cells.append(_escape(value))
            else:
                cells.append(str(value))
        lines.append("\t".join(cells))
    return ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")


def parse_tsv(text: str) -> List[Tuple[Any, ...]]:
    """Parse a `SELECT ... FORMAT TabSeparated` response into typed rows.

    Types come from COLUMNS by position: date → date, the three money columns
    → Decimal, counters → int, sales_type → str. source_id's `\\N` → None.
    """
    rows: List[Tuple[Any, ...]] = []
    for line in text.split("\n"):
        if not line:
            continue
        cells = line.split("\t")
        typed: List[Any] = []
        for name, cell in zip(COLUMNS, cells):
            if cell == "\\N":
                typed.append(None)
            elif name == "date":
                typed.append(date.fromisoformat(cell))
            elif name in _DECIMAL_COLUMNS:
                typed.append(Decimal(cell))
            elif name in _INT_COLUMNS or name == "source_id":
                typed.append(int(cell))
            else:
                typed.append(_unescape(cell))
        rows.append(tuple(typed))
    return rows


def _key(row: Tuple[Any, ...]) -> Tuple[Any, Any, Any]:
    return (row[0], row[1], row[2])


def _sample_id(row_key: Tuple[Any, Any, Any]) -> int:
    """A cell key rendered as the one int `IntegrityIssue.sample_ids` holds.

    yyyymmdd of the cell's date — enough to find the cell, which is all a
    sample is for.
    """
    d = row_key[0]
    return d.year * 10000 + d.month * 100 + d.day


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
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
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
            STATE_ROW, rows,
        )


async def _watermark_failed(error: str) -> None:
    """Best effort, `pg_landing._record_failure`'s reasoning: the case that
    misleads is a ClickHouse fault while Postgres is fine, and without this
    the row would keep its last success and read as healthy."""
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
                STATE_ROW, error[:2000],
            )
    except Exception as exc:  # pragma: no cover - double fault
        logger.debug("ch_gold: could not record the failure either: %s", exc)


# ─── The shipper ─────────────────────────────────────────────────────────────

async def _ensure_database(name: str) -> None:
    """CREATE DATABASE IF NOT EXISTS, tolerating a denied global right.

    On production the platform pre-creates bronze/silver/gold and `ks_app`
    holds no global CREATE DATABASE — the statement is denied even when the
    database exists. An existing database is the expected case there, and the
    CREATE TABLE that follows is the real probe: it fails honestly when the
    database truly is not there.
    """
    try:
        await _execute(f"CREATE DATABASE IF NOT EXISTS {name}")
    except RuntimeError as e:
        if "ACCESS_DENIED" not in str(e) and "Code: 497" not in str(e):
            raise


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
