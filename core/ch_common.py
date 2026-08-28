"""The ClickHouse plumbing, in one home.

Extracted from ch_gold/ch_silver/ch_history after the review counted the
duplicates: the TSV escaping written three times, the watermark SQL three
times, the HTTP client's auth twice — in a series whose own manifest is «одно
определение — один дом». The per-table pieces stay where they belong (each
copy owns its column list and type map); what lives here is everything that
was the same text three times.

The HTTP client: httpx because it is already in the lock, credentials as
headers so they cannot leak into a logged query string, a fresh client per
statement because at ~10 statements an hour connection reuse buys nothing
worth a pooled resource's lifecycle. The 30-second timeout is sized for
today's 46k-row INSERTs; it is named here so the day a bigger table trips it,
the grep finds one number, not three.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

URL_ENV = "KS_CH_URL"
USER_ENV = "KS_CH_USER"
PASSWORD_ENV = "KS_CH_PASSWORD"

TIMEOUT_S = 30.0


def configured() -> bool:
    return bool(os.getenv(URL_ENV, "").strip())


def _auth_headers() -> Dict[str, str]:
    headers = {"X-ClickHouse-User": os.getenv(USER_ENV, "ks_app").strip() or "ks_app"}
    password = os.getenv(PASSWORD_ENV, "")
    if password:
        headers["X-ClickHouse-Key"] = password
    return headers


async def execute(sql: str, *, body: Optional[bytes] = None) -> str:
    """One ClickHouse HTTP round trip.

    DDL/SELECT travel as the request body; an INSERT travels as `?query=`
    with the TSV payload as the body, which is the documented way to stream
    data.
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

    async with httpx.AsyncClient(timeout=TIMEOUT_S) as client:
        response = await client.post(
            url, params=params, content=content, headers=_auth_headers(),
        )
    if response.status_code != 200:
        raise RuntimeError(
            f"ClickHouse HTTP {response.status_code}: {response.text[:300]}"
        )
    return response.text


async def ensure_database(name: str, *, execute=None) -> None:
    """CREATE DATABASE IF NOT EXISTS, tolerating a denied global right.

    `execute` lets a caller route the statement through its own module-level
    client binding, so a test that patches that binding fences ALL of the
    module's network traffic — the review-era shipper tests rely on it.

    On production the platform pre-creates the databases and `ks_app` holds
    no global CREATE DATABASE — the statement is denied even when the
    database exists. An existing database is the expected case there, and the
    CREATE TABLE that follows is the real probe: it fails honestly when the
    database truly is not there.
    """
    run = execute if execute is not None else globals()["execute"]
    try:
        await run(f"CREATE DATABASE IF NOT EXISTS {name}")
    except RuntimeError as e:
        if "ACCESS_DENIED" not in str(e) and "Code: 497" not in str(e):
            raise


# ─── TSV, both directions, typed per table by its own map ────────────────────
#
# Type keys: int · decimal · str · date · bool · ts (DateTime, seconds, UTC)
# · ts6 (DateTime64(6), microseconds, UTC). Escaping is ClickHouse's own and
# is written for data that never needs it, because a format that is correct
# only for today's values breaks silently.

def escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\").replace("\t", "\\t")
        .replace("\n", "\\n").replace("\r", "\\r")
    )


def unescape(value: str) -> str:
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


def render_tsv(
    rows: Sequence[Tuple[Any, ...]],
    columns: Sequence[str],
    types: Dict[str, str],
) -> bytes:
    lines: List[str] = []
    for row in rows:
        cells: List[str] = []
        for name, value in zip(columns, row):
            kind = types[name]
            if value is None:
                cells.append("\\N")
            elif kind == "bool":
                cells.append("true" if value else "false")
            elif kind == "ts":
                cells.append(
                    value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                )
            elif kind == "ts6":
                cells.append(
                    value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
                )
            elif kind == "date":
                cells.append(value.strftime("%Y-%m-%d"))
            elif kind == "str":
                cells.append(escape(str(value)))
            else:
                cells.append(str(value))
        lines.append("\t".join(cells))
    return ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")


def parse_tsv(
    text: str,
    columns: Sequence[str],
    types: Dict[str, str],
) -> List[Tuple[Any, ...]]:
    rows: List[Tuple[Any, ...]] = []
    for line in text.split("\n"):
        if not line:
            continue
        typed: List[Any] = []
        for name, cell in zip(columns, line.split("\t")):
            kind = types[name]
            if cell == "\\N":
                typed.append(None)
            elif kind == "bool":
                typed.append(cell == "true")
            elif kind == "ts":
                typed.append(
                    datetime.strptime(cell, "%Y-%m-%d %H:%M:%S")
                    .replace(tzinfo=timezone.utc)
                )
            elif kind == "ts6":
                typed.append(
                    datetime.strptime(cell, "%Y-%m-%d %H:%M:%S.%f")
                    .replace(tzinfo=timezone.utc)
                )
            elif kind == "date":
                typed.append(date.fromisoformat(cell))
            elif kind == "decimal":
                typed.append(Decimal(cell))
            elif kind == "int":
                typed.append(int(cell))
            else:
                typed.append(unescape(cell))
        rows.append(tuple(typed))
    return rows


def date_sample_id(d: date) -> int:
    """A cell's date rendered as the one int `IntegrityIssue.sample_ids`
    holds — yyyymmdd, enough to find the cell."""
    return d.year * 10000 + d.month * 100 + d.day


# ─── The watermark, meta.mirror_state's shape ───────────────────────────────
#
# Same SQL the Postgres mirrors use, parameterised by the state row. This is
# the *separate-transaction* variant: the ClickHouse write it reports on has
# already happened in another store, so unlike pg_landing there is no data
# transaction to share.

async def mark_ok(state_row: str, rows: int) -> None:
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
                last_attempted_at = now(), last_ok_at = now(),
                failures_since_ok = 0, last_error = NULL,
                last_rows = EXCLUDED.last_rows
            """,
            state_row, rows,
        )


async def mark_failed(state_row: str, error: str) -> None:
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
                    last_error = EXCLUDED.last_error
                """,
                state_row, error[:2000],
            )
    except Exception as exc:  # pragma: no cover - double fault
        logger.debug("ch_common: could not record the failure either: %s", exc)
