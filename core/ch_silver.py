"""silver.orders in ClickHouse, and the Gold that is finally *computed* there.

Step 5 of «Одна бронза», and the step where ClickHouse stops being a copy:

  1. `silver.orders` ships from Postgres whole — staging + EXCHANGE,
     `ch_gold`'s shape — and is verified as a round trip, tolerance zero,
     because a copy that differs is a defect in the copy.
  2. `gold.daily_revenue` is then DERIVED inside ClickHouse from that silver,
     both grains, and compared against the Gold Postgres computed from the
     same rows. That comparison is the page's «сверка навсегда» line: two
     engines, two independent aggregations, one answer.

WHY THERE IS NO THIRD DIALECT OF THE MEASURES

The step-5 objection on record was that a hand-written ClickHouse derivation
would be рукописная проекция №3, gated on И1. It is not, because of a fact
checked rather than assumed: `GOLD_MEASURES` is written as
`COUNT(DISTINCT CASE WHEN … THEN … END)` and `COALESCE(SUM(CASE …), 0)` —
ordinary aggregate SQL that ClickHouse 24.8 parses and computes with the same
meaning (aggregates skip NULLs, `CASE` without `ELSE` yields NULL, Bool works
in predicates). So the derivation below renders the same expressions verbatim
from the same dict PG and DuckDB render from — one home, three engines — and
the daily zero-tolerance comparison against Postgres Gold is what would name
a divergence the morning it appeared.

The one licensed difference is `avg_order_value`: the engines divide DECIMALs
differently, so it carries the same one-cent tolerance the DuckDB↔PG
comparison already documents. Everything else — including the three
`uniqExact` counts — compares exact.

WHY SILVER IS A COPY AND NOT A COMPUTATION

Postgres stays the computing engine — the page keeps PG silver/gold forever
as the cross-engine check — and «не заводить вторую бронзу» rules out
shipping bronze into ClickHouse to derive silver locally. A silver computed
in CH would need the orders, the managers and the classifications — a third
copy of each with a third сверка. The copy costs one ship of 46,698 rows
(~1 s at the measured rate) and proves the same thing.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core.ch_common import (
    URL_ENV,
    configured,
    date_sample_id,
    ensure_database,
    execute as _execute,
    mark_failed as _mark_failed,
    mark_ok as _mark_ok,
    parse_tsv as _parse_generic,
    render_tsv as _render_generic,
)
from core.data_quality import IntegrityIssue, Severity
from core.pg_gold import GOLD_COLUMNS
from core.pg_silver import SILVER_COLUMNS

logger = logging.getLogger(__name__)

SILVER_STATE = "clickhouse.silver_orders"
GOLD_STATE = "clickhouse.gold_daily_revenue"

SILVER_TABLE = "silver.orders"
SILVER_STAGING = "silver.orders_staging"
GOLD_TABLE = "gold.daily_revenue"
GOLD_STAGING = "gold.daily_revenue_staging"

# SILVER_COLUMNS is imported from core.pg_silver — the projection's own
# write order, one home; a test pins the identity.

# Column -> TSV type. One map, three readers: the DDL below, the renderer and
# the parser — a test walks the DDL and asserts it agrees.
_TYPES: Dict[str, str] = {
    "id": "int", "source_id": "int", "status_id": "int",
    "grand_total": "decimal", "ordered_at": "ts",
    "buyer_id": "int", "manager_id": "int", "order_date": "date",
    "is_return": "bool", "sales_type": "str", "is_active_source": "bool",
    "source_name": "str", "is_new_customer": "bool",
    "buyer_first_order_date": "date", "promocode": "str",
}

_SILVER_DDL = """
CREATE TABLE IF NOT EXISTS {name} (
    id                     Int32,
    source_id              Int32,
    status_id              Int32,
    grand_total            Decimal(12, 2),
    ordered_at             Nullable(DateTime('UTC')),
    buyer_id               Nullable(Int32),
    manager_id             Nullable(Int32),
    order_date             Date,
    is_return              Bool,
    sales_type             LowCardinality(String),
    is_active_source       Bool,
    source_name            String,
    is_new_customer        Bool,
    buyer_first_order_date Nullable(Date),
    promocode              Nullable(String)
)
ENGINE = MergeTree
ORDER BY (order_date, sales_type, id)
"""


def render_tsv(rows):
    """Typed TSV over the shared codec — bools as true/false, DateTime as
    UTC seconds, the two spots a generic str() writes wrong."""
    return _render_generic(rows, SILVER_COLUMNS, _TYPES)


def parse_tsv(text):
    return _parse_generic(text, SILVER_COLUMNS, _TYPES)


async def _fetch_pg_silver() -> List[Tuple[Any, ...]]:
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch(
            f"SELECT {', '.join(SILVER_COLUMNS)} FROM silver.orders"
        )
    out: List[Tuple[Any, ...]] = []
    for r in records:
        row = []
        for name in SILVER_COLUMNS:
            v = r[name]
            # asyncpg returns aware datetimes and floats-as-Decimal already;
            # seconds precision matches the DDL, which a sub-second
            # ordered_at would silently violate — truncate deliberately.
            if _TYPES[name] == "ts" and v is not None:
                v = v.astimezone(timezone.utc).replace(microsecond=0)
            row.append(v)
        out.append(tuple(row))
    return out


async def _ship_silver_rows(rows: Sequence[Tuple[Any, ...]]) -> None:
    await ensure_database("silver", execute=_execute)
    await _execute(_SILVER_DDL.format(name=SILVER_TABLE))
    await _execute(_SILVER_DDL.format(name=SILVER_STAGING))
    await _execute(f"TRUNCATE TABLE {SILVER_STAGING}")
    if rows:
        await _execute(
            f"INSERT INTO {SILVER_STAGING} ({', '.join(SILVER_COLUMNS)}) "
            f"FORMAT TabSeparated",
            body=render_tsv(rows),
        )
    staged = int((await _execute(f"SELECT count() FROM {SILVER_STAGING}")).strip() or 0)
    if staged != len(rows):
        raise RuntimeError(
            f"staging holds {staged} row(s), Postgres supplied {len(rows)} — "
            f"refusing to EXCHANGE a short copy"
        )
    await _execute(f"EXCHANGE TABLES {SILVER_TABLE} AND {SILVER_STAGING}")


# ─── Gold, derived where it lives ────────────────────────────────────────────

def _gold_measure_items() -> str:
    """The eight measures, verbatim from the one home all engines render."""
    from core.duckdb_store import GOLD_MEASURES

    return ",\n    ".join(
        f"{expr} AS {name}" for name, expr in GOLD_MEASURES.items()
    )


def derive_gold_sql() -> Tuple[str, str]:
    """(fine grain, roll-up) INSERT statements into the staging table."""
    items = _gold_measure_items()
    fine = f"""
INSERT INTO {GOLD_STAGING}
    ({', '.join(GOLD_COLUMNS)})
SELECT order_date AS date, sales_type, source_id,
    {items}
FROM {SILVER_TABLE}
GROUP BY order_date, sales_type, source_id
"""
    rollup = f"""
INSERT INTO {GOLD_STAGING}
    ({', '.join(GOLD_COLUMNS)})
SELECT order_date AS date, sales_type, NULL,
    {items}
FROM {SILVER_TABLE}
GROUP BY order_date, sales_type
"""
    return fine, rollup


async def _derive_gold() -> int:
    """Recompute gold.daily_revenue from the silver just shipped, atomically."""
    from core.ch_gold import _TABLE_DDL

    await ensure_database("gold", execute=_execute)
    await _execute(_TABLE_DDL.format(name=GOLD_TABLE))
    await _execute(_TABLE_DDL.format(name=GOLD_STAGING))
    await _execute(f"TRUNCATE TABLE {GOLD_STAGING}")
    fine, rollup = derive_gold_sql()
    await _execute(fine)
    await _execute(rollup)
    rows = int((await _execute(f"SELECT count() FROM {GOLD_STAGING}")).strip() or 0)
    await _execute(f"EXCHANGE TABLES {GOLD_TABLE} AND {GOLD_STAGING}")
    return rows


async def ch_silver_sync() -> Dict[str, Any]:
    """Ship silver, derive gold — the hourly tick. Never raises."""
    from core.pg_silver import PG_LAYER_LOCK

    if not configured():
        return {"skipped": f"{URL_ENV} is not set"}
    try:
        # Under PG_LAYER_LOCK for two of the review's races: the hourly tick
        # and the daily reconciliation both drive TRUNCATE/EXCHANGE on the
        # same staging tables, and interleaved they can swap an empty staging
        # into place under a fresh OK watermark.
        async with PG_LAYER_LOCK:
            rows = await _fetch_pg_silver()
            await _ship_silver_rows(rows)
            await _mark_ok(SILVER_STATE, len(rows))
            gold_rows = await _derive_gold()
            await _mark_ok(GOLD_STATE, gold_rows)
        logger.info(
            "ch_silver: shipped %d silver row(s), derived %d gold row(s)",
            len(rows), gold_rows,
        )
        return {"silver_rows": len(rows), "gold_rows": gold_rows}
    except Exception as e:
        detail = f"{type(e).__name__}: {e}"
        logger.error("ch_silver: sync failed: %s", detail)
        await _mark_failed(SILVER_STATE, detail)
        return {"error": detail}


# _mark_ok / _mark_failed are core.ch_common.mark_ok / mark_failed.


# ─── The comparisons ─────────────────────────────────────────────────────────

def _key3(row: Tuple[Any, ...]) -> Tuple[Any, Any, Any]:
    return (row[0], row[1], row[2])


def compare_gold_cells(
    pg_cells: Sequence[Tuple[Any, ...]],
    ch_cells: Sequence[Tuple[Any, ...]],
    *,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Two engines' Gold, cell by cell.

    Zero tolerance on every column except `avg_order_value`, which may differ
    by one cent because the engines divide DECIMALs differently — the same
    allowance `MATERIAL_THRESHOLDS` already documents for DuckDB↔PG. The
    `uniqExact` counts compare exact: an approximation is not an answer here.
    """
    issues: List[IntegrityIssue] = []
    want = {_key3(r): r for r in pg_cells}
    got = {_key3(r): r for r in ch_cells}
    columns = GOLD_COLUMNS
    avg_i = columns.index("avg_order_value")

    missing = sorted(k for k in want if k not in got)
    extra = sorted(k for k in got if k not in want)
    differing = []
    detail = ""
    for k, pg_row in want.items():
        ch_row = got.get(k)
        if ch_row is None:
            continue
        for i, (name, a, b) in enumerate(zip(columns, pg_row, ch_row)):
            if i == avg_i:
                if abs(Decimal(str(a)) - Decimal(str(b))) > Decimal("0.01"):
                    differing.append(k)
                    detail = detail or f"first: {k} {name} {a!r} vs {b!r}"
                    break
            elif a != b:
                differing.append(k)
                detail = detail or f"first: {k} {name} {a!r} vs {b!r}"
                break

    def _sid(k):
        return date_sample_id(k[0])

    if missing:
        issues.append(IntegrityIssue(
            check_name="ch_engines_gold_missing",
            table_name=GOLD_TABLE, severity=Severity.CRITICAL,
            count=len(missing),
            sample_ids=tuple(_sid(k) for k in missing[:max_samples]),
            description=(
                f"{len(missing)} Gold cell(s) Postgres computed have no "
                f"counterpart in ClickHouse's own derivation"
            ),
        ))
    if extra:
        issues.append(IntegrityIssue(
            check_name="ch_engines_gold_extra",
            table_name=GOLD_TABLE, severity=Severity.CRITICAL,
            count=len(extra),
            sample_ids=tuple(_sid(k) for k in extra[:max_samples]),
            description=(
                f"{len(extra)} Gold cell(s) ClickHouse derived that Postgres "
                f"did not — the two engines disagree on which cells exist"
            ),
        ))
    if differing:
        differing.sort()
        issues.append(IntegrityIssue(
            check_name="ch_engines_gold_mismatch",
            table_name=GOLD_TABLE, severity=Severity.CRITICAL,
            count=len(differing),
            sample_ids=tuple(_sid(k) for k in differing[:max_samples]),
            description=(
                f"{len(differing)} Gold cell(s) differ between the two "
                f"engines' independent aggregations of one silver; {detail}. "
                f"Same measures, same rows — this is an engine-semantics "
                f"defect, and it is exactly what this comparison exists to "
                f"catch"
            ),
        ))
    return issues


async def _fetch_ch_gold_cells() -> List[Tuple[Any, ...]]:
    text = await _execute(
        f"SELECT {', '.join(GOLD_COLUMNS)} "
        f"FROM {GOLD_TABLE} FORMAT TabSeparated"
    )
    from core.ch_gold import parse_tsv as parse_gold_tsv

    return parse_gold_tsv(text)


async def _fetch_pg_gold_cells() -> List[Tuple[Any, ...]]:
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch(
            f"SELECT {', '.join(GOLD_COLUMNS)} FROM gold.daily_revenue"
        )
    return [tuple(r) for r in records]


async def reconcile_clickhouse(*, max_samples: int = 10) -> List[IntegrityIssue]:
    """The daily ClickHouse verdict: ship, round-trip silver, then set the two
    engines' Gold against each other. `ch_gold.reconcile_ch_gold`'s contract:
    stands down unconfigured, WARNs instead of raising — an optional store
    must not silence the mandatory comparisons in this layer.

    The whole body runs under PG_LAYER_LOCK, and the reason is the review's
    sharpest finding: without it, a silver rebuild committing between "fetch
    the silver we ship" and "fetch the gold we compare against" makes
    Postgres Gold reflect a *different* silver than the one ClickHouse
    derived from — and the zero-tolerance verdict files a CRITICAL about an
    engine-semantics defect that does not exist. Under the lock the pair is
    consistent by construction, and the tolerance stays honestly zero.
    """
    from core.pg_silver import PG_LAYER_LOCK

    if not configured():
        return []

    async with PG_LAYER_LOCK:
        try:
            shipped = await _fetch_pg_silver()
            await _ship_silver_rows(shipped)
            await _mark_ok(SILVER_STATE, len(shipped))
            gold_rows = await _derive_gold()
            await _mark_ok(GOLD_STATE, gold_rows)
        except Exception as e:
            detail = f"{type(e).__name__}: {e}"
            await _mark_failed(SILVER_STATE, detail)
            return [IntegrityIssue(
                check_name="ch_silver_sync_failed",
                table_name=SILVER_TABLE, severity=Severity.WARN, count=1,
                description=(
                    f"could not ship silver / derive gold in ClickHouse: {detail}. "
                    f"Both tables keep their previous copy; freshness is in "
                    f"meta.mirror_state"
                ),
            )]

        try:
            readback = parse_tsv(await _execute(
                f"SELECT {', '.join(SILVER_COLUMNS)} FROM {SILVER_TABLE} "
                f"FORMAT TabSeparated"
            ))
            pg_gold_cells = await _fetch_pg_gold_cells()
            ch_gold_cells = await _fetch_ch_gold_cells()
        except Exception as e:
            # The optional store failing mid-comparison must degrade to a
            # WARN, not an exception — a raise here fails the whole
            # mirror_landing run and silences the mandatory comparisons,
            # which is exactly what this module's contract forbids.
            return [IntegrityIssue(
                check_name="ch_silver_unreachable",
                table_name=SILVER_TABLE, severity=Severity.WARN, count=1,
                description=f"shipped, but a read-back failed: {type(e).__name__}: {e}",
            )]

    issues: List[IntegrityIssue] = []

    want = {r[0]: r for r in shipped}
    got = {r[0]: r for r in readback}
    lost = sorted(set(want) - set(got))
    invented = sorted(set(got) - set(want))
    changed = sorted(k for k in set(want) & set(got) if want[k] != got[k])
    if lost or invented or changed:
        issues.append(IntegrityIssue(
            check_name="ch_silver_roundtrip",
            table_name=SILVER_TABLE, severity=Severity.CRITICAL,
            count=len(lost) + len(invented) + len(changed),
            sample_ids=tuple((lost + invented + changed)[:max_samples]),
            description=(
                f"the silver copy came back different: {len(lost)} lost, "
                f"{len(invented)} invented, {len(changed)} changed. A copy "
                f"that differs is a defect in the copy — tolerance zero"
            ),
        ))

    issues += compare_gold_cells(
        pg_gold_cells, ch_gold_cells, max_samples=max_samples,
    )
    return issues
