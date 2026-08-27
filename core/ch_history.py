"""history.order_versions — step 6 of «Одна бронза»: the archive, inherited.

Not a layer of the trunk and not a second bronze: `app.order_versions` in
Postgres remains the archive of record (it is where the RETURNING writes, in
the transaction that writes the order), and ClickHouse *inherits* it — an
append-only feed above `MAX(id)`, `core/pg_operational.py`'s watermark shape
with `pg_backfill`'s no-cursor rule: the watermark is read back out of
ClickHouse itself, so there is nothing stored to be wrong, and an interrupted
run resumes by recomputing. The first run inherits everything.

Append-only is a property of the engine here — plain MergeTree receives only
INSERTs from one statement in this module — and of the source: revision 0010
pins by test that nothing UPDATEs or DELETEs the Postgres side.

The database is `history` (`KS_CH_HISTORY_DB` to override). The platform's
grants cover bronze/silver/gold only, so the deploy checklist carries two
one-line statements for the platform side: CREATE DATABASE history, and the
same GRANT the other three databases already have. Until then every ship
fails loudly into `meta.mirror_state` — a missing grant is a failed row, not
a silent stand-down.

The comparison is bucketed counts, `dq_mirror_landing`'s cheapest shape: both
sides are append-only, so below ClickHouse's own MAX(id) the sets must match
exactly — a count per 10,000-id bucket sees any loss, and there is no update
for it to miss (the fingerprint subtlety that haunts `bronze.orders` does not
exist for a table nothing rewrites).
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core.data_quality import IntegrityIssue, Severity
from core.ch_gold import URL_ENV, _execute, configured

logger = logging.getLogger(__name__)

DB_ENV = "KS_CH_HISTORY_DB"
STATE_ROW = "clickhouse.order_versions"

COLUMNS: Tuple[str, ...] = (
    "id", "order_id", "captured_at", "kind",
    "source_id", "status_id", "status_group_id", "grand_total",
    "ordered_at", "buyer_id", "manager_id", "manager_comment",
    "promocode", "updated_at",
)

_TYPES: Dict[str, str] = {
    "id": "int", "order_id": "int", "captured_at": "ts", "kind": "str",
    "source_id": "int", "status_id": "int", "status_group_id": "int",
    "grand_total": "decimal", "ordered_at": "ts", "buyer_id": "int",
    "manager_id": "int", "manager_comment": "str", "promocode": "str",
    "updated_at": "ts",
}

_DDL = """
CREATE TABLE IF NOT EXISTS {name} (
    id              Int64,
    order_id        Int32,
    captured_at     DateTime64(6, 'UTC'),
    kind            LowCardinality(String),
    source_id       Nullable(Int32),
    status_id       Nullable(Int32),
    status_group_id Nullable(Int32),
    grand_total     Nullable(Decimal(12, 2)),
    ordered_at      Nullable(DateTime64(6, 'UTC')),
    buyer_id        Nullable(Int32),
    manager_id      Nullable(Int32),
    manager_comment Nullable(String),
    promocode       Nullable(String),
    updated_at      Nullable(DateTime64(6, 'UTC'))
)
ENGINE = MergeTree
ORDER BY (order_id, captured_at, id)
"""


def table() -> str:
    db = os.getenv(DB_ENV, "history").strip() or "history"
    return f"{db}.order_versions"


def _escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\").replace("\t", "\\t")
        .replace("\n", "\\n").replace("\r", "\\r")
    )


def render_tsv(rows: Sequence[Tuple[Any, ...]]) -> bytes:
    lines: List[str] = []
    for row in rows:
        cells: List[str] = []
        for name, value in zip(COLUMNS, row):
            if value is None:
                cells.append("\\N")
            elif _TYPES[name] == "ts":
                cells.append(
                    value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
                )
            elif _TYPES[name] == "str":
                cells.append(_escape(str(value)))
            else:
                cells.append(str(value))
        lines.append("\t".join(cells))
    return ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")


async def _ch_max_id(*, ensure: bool = False) -> int:
    """The copy's own high-water mark. `ensure` creates the schema first —
    the shipper's business; the reconciliation only reads, and a missing
    table there is an honest WARN, not something a check should build."""
    if ensure:
        from core.ch_gold import _ensure_database

        await _ensure_database(table().split(".")[0])
        await _execute(_DDL.format(name=table()))
    text = await _execute(f"SELECT coalesce(max(id), 0) FROM {table()}")
    return int(text.strip() or 0)


async def ship_history(*, chunk: int = 50_000) -> Dict[str, Any]:
    """Append everything Postgres holds above ClickHouse's MAX(id).

    Never raises. The chunked read keeps the first inheritance (46,699 rows
    today, all of history forever) from ever being one giant statement.
    """
    from core.pg import get_pool

    if not configured():
        return {"skipped": f"{URL_ENV} is not set"}
    try:
        floor = await _ch_max_id(ensure=True)
        pool = await get_pool()
        shipped = 0
        while True:
            async with pool.acquire() as conn:
                records = await conn.fetch(
                    f"SELECT {', '.join(COLUMNS)} FROM app.order_versions "
                    f"WHERE id > $1 ORDER BY id LIMIT $2",
                    floor, chunk,
                )
            if not records:
                break
            rows = [tuple(r) for r in records]
            await _execute(
                f"INSERT INTO {table()} ({', '.join(COLUMNS)}) "
                f"FORMAT TabSeparated",
                body=render_tsv(rows),
            )
            shipped += len(rows)
            floor = rows[-1][0]
        from core.ch_silver import _mark_ok

        await _mark_ok(STATE_ROW, shipped)
        if shipped:
            logger.info("ch_history: appended %d version(s)", shipped)
        return {"appended": shipped}
    except Exception as e:
        detail = f"{type(e).__name__}: {e}"
        logger.error("ch_history: ship failed: %s", detail)
        from core.ch_silver import _mark_failed

        await _mark_failed(STATE_ROW, detail)
        return {"error": detail}


async def reconcile_ch_history(*, max_samples: int = 10) -> List[IntegrityIssue]:
    """Bucketed counts below ClickHouse's own high-water mark, tolerance zero."""
    from core.pg import get_pool

    if not configured():
        return []
    try:
        ceiling = await _ch_max_id()
        ch_text = await _execute(
            f"SELECT intDiv(id, 10000) AS b, count() FROM {table()} "
            f"WHERE id <= {ceiling} GROUP BY b FORMAT TabSeparated"
        )
    except Exception as e:
        return [IntegrityIssue(
            check_name="ch_history_unreachable",
            table_name=table(), severity=Severity.WARN, count=1,
            description=f"could not read the archive copy: {type(e).__name__}: {e}",
        )]

    ch_buckets: Dict[int, int] = {}
    for line in ch_text.split("\n"):
        if line:
            b, n = line.split("\t")
            ch_buckets[int(b)] = int(n)

    pool = await get_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT id / 10000 AS b, COUNT(*) FROM app.order_versions "
            "WHERE id <= $1 GROUP BY 1",
            ceiling,
        )
    pg_buckets = {int(r[0]): int(r[1]) for r in records}

    bad = sorted(
        b for b in set(ch_buckets) | set(pg_buckets)
        if ch_buckets.get(b, 0) != pg_buckets.get(b, 0)
    )
    if not bad:
        return []
    return [IntegrityIssue(
        check_name="ch_history_buckets",
        table_name=table(), severity=Severity.CRITICAL,
        count=len(bad),
        sample_ids=tuple(bad[:max_samples]),
        description=(
            f"{len(bad)} id-bucket(s) below the archive copy's own MAX(id) "
            f"disagree on row count. Both sides are append-only, so below the "
            f"watermark the sets must match exactly — a short bucket is loss, "
            f"a long one is a writer that is not this module"
        ),
    )]
