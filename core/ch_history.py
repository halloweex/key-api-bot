"""history.order_versions — step 6 of «Одна бронза»: the archive, inherited.

Not a layer of the trunk and not a second bronze: `app.order_versions` in
Postgres remains the archive of record (it is where the RETURNING writes, in
the transaction that writes the order), and ClickHouse *inherits* it — an
hourly **ids-diff**, `pg_backfill`'s no-cursor rule: nothing stored to be
wrong, an interrupted run resumes by recomputing, the first run inherits
everything, and a row lost below the copy's own high-water mark heals within
the hour instead of being reported CRITICAL forever. A heal is loud — a
WARNING in the log and an INFO finding in the daily check — because a repair
the comparison never sees would otherwise be measuring the already-repaired.

Append-only is a property of the engine here — plain MergeTree receives only
INSERTs from one statement in this module — and of the source: revision 0010
pins by test that nothing UPDATEs or DELETEs the Postgres side.

The database is `history` (`KS_CH_HISTORY_DB` to override). The platform's
grants cover bronze/silver/gold only, so the deploy checklist carries two
one-line statements for the platform side: CREATE DATABASE history, and the
same GRANT the other three databases already have. Until then every ship
fails loudly into `meta.mirror_state` — a missing grant is a failed row, not
a silent stand-down.

The comparison is bucketed counts, `dq_mirror_landing`'s cheapest shape:
both sides are append-only, so below ClickHouse's own MAX(id) the sets must
match exactly — a count per 10,000-id bucket sees any loss, and there is no
update for it to miss (the fingerprint subtlety that haunts `bronze.orders`
does not exist for a table nothing rewrites). A short bucket the next hourly
diff will refill still files its finding first: the check runs on the state
it sees, and the heal leaves its own trace.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core.ch_common import (
    URL_ENV,
    configured,
    ensure_database,
    execute as _execute,
    mark_failed as _mark_failed,
    mark_ok as _mark_ok,
    render_tsv as _render_generic,
)
from core.data_quality import IntegrityIssue, Severity

logger = logging.getLogger(__name__)

# The last below-watermark heal this process performed, read by
# reconcile_ch_history — the buyers mirror's rule applied to its own sibling:
# a repair the daily check never gets to see must leave a trace in the
# findings, not only in a log line. Process-local, same caveat, same reason.
last_heal: Dict[str, Any] = {}

DB_ENV = "KS_CH_HISTORY_DB"
STATE_ROW = "clickhouse.order_versions"

COLUMNS: Tuple[str, ...] = (
    "id", "order_id", "captured_at", "kind",
    "source_id", "status_id", "status_group_id", "grand_total",
    "ordered_at", "buyer_id", "manager_id", "manager_comment",
    "promocode", "updated_at",
)

_TYPES: Dict[str, str] = {
    "id": "int", "order_id": "int", "captured_at": "ts6", "kind": "str",
    "source_id": "int", "status_id": "int", "status_group_id": "int",
    "grand_total": "decimal", "ordered_at": "ts6", "buyer_id": "int",
    "manager_id": "int", "manager_comment": "str", "promocode": "str",
    "updated_at": "ts6",
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


def render_tsv(rows: Sequence[Tuple[Any, ...]]) -> bytes:
    return _render_generic(rows, COLUMNS, _TYPES)


async def _ch_max_id(*, ensure: bool = False) -> int:
    """The copy's own high-water mark. `ensure` creates the schema first —
    the shipper's business; the reconciliation only reads, and a missing
    table there is an honest WARN, not something a check should build."""
    if ensure:
        await ensure_database(table().split(".")[0], execute=_execute)
        await _execute(_DDL.format(name=table()))
    text = await _execute(f"SELECT coalesce(max(id), 0) FROM {table()}")
    return int(text.strip() or 0)


async def ship_history(*, chunk: int = 50_000) -> Dict[str, Any]:
    """Append every version Postgres holds that the copy lacks.

    An ids-diff, not a high-water mark — the review's finding: BIGSERIAL ids
    can commit out of order, and a `> MAX(id)` shipper skips forever a version
    whose id committed after a larger one had already shipped; the bucket
    check would then file its absence CRITICAL to the end of time with no
    repair path. The diff ships exactly the missing set, is idempotent,
    resumes by recomputing, inherits everything on the first run, and heals a
    below-watermark loss within the hour — the buyers mirror's pattern, for
    the buyers mirror's reason. Two id scans an hour is its whole price.

    Never raises. The chunked read keeps the first inheritance (46,699 rows
    today, all of history forever) from ever being one giant statement.
    """
    from core.pg import get_pool

    if not configured():
        return {"skipped": f"{URL_ENV} is not set"}
    try:
        # The ceiling BEFORE shipping is what tells a heal from an append:
        # anything we ship at or below it is a row the copy had lost.
        ceiling = await _ch_max_id(ensure=True)
        # A full id scan of a forever-growing table, hourly: ~47k ids today,
        # ~100 new a day — an int column stays trivial for decades, and the
        # day it is not, this comment is where the pagination goes.
        ch_text = await _execute(f"SELECT id FROM {table()} FORMAT TabSeparated")
        ch_ids = {int(line) for line in ch_text.split("\n") if line}
        pool = await get_pool()
        async with pool.acquire() as conn:
            pg_ids = {r[0] for r in await conn.fetch("SELECT id FROM app.order_versions")}
        missing = sorted(pg_ids - ch_ids)

        shipped = 0
        for start in range(0, len(missing), chunk):
            ids = missing[start:start + chunk]
            async with pool.acquire() as conn:
                records = await conn.fetch(
                    f"SELECT {', '.join(COLUMNS)} FROM app.order_versions "
                    f"WHERE id = ANY($1::bigint[]) ORDER BY id",
                    ids,
                )
            rows = [tuple(r) for r in records]
            await _execute(
                f"INSERT INTO {table()} ({', '.join(COLUMNS)}) "
                f"FORMAT TabSeparated",
                body=render_tsv(rows),
            )
            shipped += len(rows)
        await _mark_ok(STATE_ROW, shipped)
        healed = sum(1 for i in missing if i <= ceiling)
        if healed:
            logger.warning(
                "ch_history: ids-diff healed %d version(s) below the "
                "watermark — rows the copy had lost", healed,
            )
            from datetime import datetime, timezone

            last_heal.update(at=datetime.now(timezone.utc), shipped=healed)
        elif shipped:
            logger.info("ch_history: appended %d version(s)", shipped)
        return {"appended": shipped}
    except Exception as e:
        detail = f"{type(e).__name__}: {e}"
        logger.error("ch_history: ship failed: %s", detail)
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

    issues: List[IntegrityIssue] = []
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    if last_heal and (now - last_heal["at"]).total_seconds() < 24 * 3600:
        issues.append(IntegrityIssue(
            check_name="ch_history_selfhealed",
            table_name=table(), severity=Severity.INFO,
            count=int(last_heal["shipped"]),
            description=(
                f"the hourly ids-diff re-shipped {last_heal['shipped']} "
                f"version(s) below the copy's watermark at "
                f"{last_heal['at'].isoformat()} — loss this comparison would "
                f"otherwise never have seen. Once is housekeeping; daily is "
                f"a leak wearing a bandage."
            ),
        ))

    bad = sorted(
        b for b in set(ch_buckets) | set(pg_buckets)
        if ch_buckets.get(b, 0) != pg_buckets.get(b, 0)
    )
    if not bad:
        return issues
    issues.append(IntegrityIssue(
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
    ))
    return issues
