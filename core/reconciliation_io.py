"""I/O helpers for Layer-2 reconciliation.

Builds (month, source_id) → metric rollups from DuckDB and KeyCRM. Format
matches what core.data_quality.classify_discrepancies expects:

    {(month_yyyy_mm, source_id): {orders, qty, revenue, returns_count, returns_revenue}}

Both helpers apply the same watermark (exclude orders with updated_at >= watermark)
to avoid in-flight false positives.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Tuple
from zoneinfo import ZoneInfo

from core.data_quality import Rollup
from core.models import OrderStatus

logger = logging.getLogger(__name__)

KYIV = ZoneInfo("Europe/Kyiv")
ACTIVE_SOURCES = (1, 2, 3, 4, 5)
RETURN_STATUS_IDS = tuple(int(s) for s in OrderStatus.return_statuses())


# ─── DuckDB rollup ────────────────────────────────────────────────────────────


def duckdb_monthly_source_rollup(
    conn,
    window_start: date,
    window_end: date,
    *,
    watermark: datetime,
) -> Rollup:
    """Group DuckDB orders by (month_in_kyiv_tz, source_id).

    Args:
        conn: DuckDB connection.
        window_start, window_end: Kyiv-local date bounds (inclusive start, inclusive end).
        watermark: UTC datetime — orders with updated_at >= watermark are
            excluded (in-flight). Pass `now - 2h` for a sane stability window.

    Returns: rollup as defined in core.data_quality.Rollup.
    """
    sources_list = ", ".join(str(s) for s in ACTIVE_SOURCES)
    returns_list = ", ".join(str(s) for s in RETURN_STATUS_IDS)
    start_str = window_start.isoformat()
    end_str = window_end.isoformat()
    watermark_str = watermark.astimezone(timezone.utc).isoformat()

    # Non-return rollup (orders, qty, revenue)
    rows_net = conn.execute(f"""
        SELECT
            STRFTIME(DATE_TRUNC('month', o.ordered_at AT TIME ZONE 'Europe/Kyiv'), '%Y-%m') AS m,
            o.source_id,
            COUNT(*) AS orders,
            COALESCE(SUM(o.grand_total), 0) AS revenue
        FROM orders o
        WHERE (o.ordered_at AT TIME ZONE 'Europe/Kyiv')::DATE BETWEEN ?::DATE AND ?::DATE
          AND o.source_id IN ({sources_list})
          AND o.status_id NOT IN ({returns_list})
          AND (o.updated_at IS NULL OR o.updated_at < ?::TIMESTAMP WITH TIME ZONE)
        GROUP BY 1, 2
    """, [start_str, end_str, watermark_str]).fetchall()

    # qty rollup (sum order_products.quantity for non-return orders only)
    rows_qty = conn.execute(f"""
        SELECT
            STRFTIME(DATE_TRUNC('month', o.ordered_at AT TIME ZONE 'Europe/Kyiv'), '%Y-%m') AS m,
            o.source_id,
            COALESCE(SUM(op.quantity), 0) AS qty
        FROM orders o
        JOIN order_products op ON op.order_id = o.id
        WHERE (o.ordered_at AT TIME ZONE 'Europe/Kyiv')::DATE BETWEEN ?::DATE AND ?::DATE
          AND o.source_id IN ({sources_list})
          AND o.status_id NOT IN ({returns_list})
          AND (o.updated_at IS NULL OR o.updated_at < ?::TIMESTAMP WITH TIME ZONE)
        GROUP BY 1, 2
    """, [start_str, end_str, watermark_str]).fetchall()
    qty_map: Dict[Tuple[str, int], int] = {(m, int(s)): int(q) for m, s, q in rows_qty}

    # Returns rollup
    rows_ret = conn.execute(f"""
        SELECT
            STRFTIME(DATE_TRUNC('month', o.ordered_at AT TIME ZONE 'Europe/Kyiv'), '%Y-%m') AS m,
            o.source_id,
            COUNT(*) AS rn,
            COALESCE(SUM(o.grand_total), 0) AS rr
        FROM orders o
        WHERE (o.ordered_at AT TIME ZONE 'Europe/Kyiv')::DATE BETWEEN ?::DATE AND ?::DATE
          AND o.source_id IN ({sources_list})
          AND o.status_id IN ({returns_list})
          AND (o.updated_at IS NULL OR o.updated_at < ?::TIMESTAMP WITH TIME ZONE)
        GROUP BY 1, 2
    """, [start_str, end_str, watermark_str]).fetchall()
    ret_map: Dict[Tuple[str, int], Tuple[int, float]] = {
        (m, int(s)): (int(rn), float(rr)) for m, s, rn, rr in rows_ret
    }

    rollup: Dict[Tuple[str, int], Dict[str, float]] = defaultdict(
        lambda: {"orders": 0, "qty": 0, "revenue": 0.0,
                 "returns_count": 0, "returns_revenue": 0.0}
    )
    for m, src, orders, rev in rows_net:
        key = (m, int(src))
        cell = rollup[key]
        cell["orders"] = int(orders)
        cell["revenue"] = float(rev)
        cell["qty"] = qty_map.get(key, 0)
    for key, (rn, rr) in ret_map.items():
        cell = rollup[key]
        cell["returns_count"] = rn
        cell["returns_revenue"] = rr

    return dict(rollup)


# ─── KeyCRM rollup ────────────────────────────────────────────────────────────


def _enumerate_months(start: date, end: date) -> list[str]:
    """Inclusive list of 'YYYY-MM' strings for each calendar month touching
    the date range."""
    months = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m, y = 1, y + 1
    return months


def _month_to_local_bounds(month: str) -> tuple[date, date]:
    """'YYYY-MM' → (first_day, last_day) inclusive."""
    y, m = int(month[:4]), int(month[5:7])
    start = date(y, m, 1)
    if m == 12:
        end = date(y + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(y, m + 1, 1) - timedelta(days=1)
    return start, end


async def keycrm_monthly_source_rollup(
    window_start: date,
    window_end: date,
    *,
    watermark: datetime,
) -> Tuple[Rollup, int]:
    """Fetch KeyCRM orders for the window and roll up by (month, source).

    KeyCRM has a 5000-row pagination cap, so we fetch one month at a time
    with ±2 day widening for backdated orders. Each request is counted in
    api_calls.

    Watermark: orders with updated_at >= watermark are excluded.

    Returns: (rollup, api_calls_used).
    """
    from core.keycrm import KeyCRMClient

    months = _enumerate_months(window_start, window_end)
    rollup: Dict[Tuple[str, int], Dict[str, float]] = defaultdict(
        lambda: {"orders": 0, "qty": 0, "revenue": 0.0,
                 "returns_count": 0, "returns_revenue": 0.0}
    )
    watermark_utc = watermark.astimezone(timezone.utc)
    seen: set[int] = set()
    api_calls = 0

    client = KeyCRMClient()
    await client.connect()
    try:
        for m_str in months:
            m_start, m_end = _month_to_local_bounds(m_str)
            f_start = (m_start - timedelta(days=2)).isoformat()
            f_end = (m_end + timedelta(days=2)).isoformat()

            # Pass 1: created_between
            page_count = 0
            params = {
                "include": "products",
                "filter[created_between]": f"{f_start},{f_end}",
            }
            async for batch in client.paginate("order", params=params, page_size=50):
                page_count += 1
                _process_batch(batch, m_str, rollup, seen, watermark_utc)
            # Pass 2: updated_between (status changes on backdated orders)
            params_upd = {
                "include": "products",
                "filter[updated_between]": f"{f_start},{f_end}",
            }
            async for batch in client.paginate("order", params=params_upd, page_size=50):
                page_count += 1
                _process_batch(batch, m_str, rollup, seen, watermark_utc)

            api_calls += page_count
            logger.debug(f"DQ reconciliation: month={m_str} pages={page_count}")

    finally:
        await client.close()

    return dict(rollup), api_calls


def _process_batch(
    batch: list,
    target_month: str,
    rollup: Dict[Tuple[str, int], Dict[str, float]],
    seen: set,
    watermark_utc: datetime,
) -> None:
    """Update rollup in-place with one KeyCRM page of orders, deduplicating
    by order id."""
    for o in batch:
        oid = o.get("id")
        if oid in seen:
            continue
        seen.add(oid)
        src = o.get("source_id")
        if src not in ACTIVE_SOURCES:
            continue
        oa = o.get("ordered_at")
        if not oa:
            continue

        # Watermark check
        ua = o.get("updated_at") or oa
        try:
            ua_dt = datetime.fromisoformat(str(ua).replace("Z", "+00:00"))
            if ua_dt.tzinfo is None:
                ua_dt = ua_dt.replace(tzinfo=timezone.utc)
            if ua_dt >= watermark_utc:
                continue  # in-flight
        except (ValueError, TypeError):
            pass

        # Kyiv month
        try:
            dt = datetime.fromisoformat(str(oa).replace("Z", "+00:00")).astimezone(KYIV)
        except (ValueError, TypeError):
            continue
        m = dt.strftime("%Y-%m")
        if m != target_month:
            # Only count when the order belongs to the month we are
            # currently iterating — prevents double-counting when a backdated
            # order appears in two adjacent month windows.
            continue

        status = o.get("status_id")
        try:
            gt = float(o.get("grand_total") or 0)
        except (TypeError, ValueError):
            gt = 0.0
        qty = 0
        for p in (o.get("products") or []):
            try:
                qty += int(p.get("quantity") or 0)
            except (TypeError, ValueError):
                pass

        cell = rollup[(m, int(src))]
        if status in RETURN_STATUS_IDS:
            cell["returns_count"] += 1
            cell["returns_revenue"] += gt
        else:
            cell["orders"] += 1
            cell["qty"] += qty
            cell["revenue"] += gt
