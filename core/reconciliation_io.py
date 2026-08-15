"""I/O helpers for Layer-2 reconciliation.

Builds (month, source_id) → metric rollups from DuckDB and KeyCRM. Format
matches what core.data_quality.classify_discrepancies expects:

    {(month_yyyy_mm, source_id): {orders, qty, revenue, returns_count, returns_revenue}}

Both sides must count the same orders, or the job reports drift that only
exists in the comparison. Three rules keep them aligned:

  * both extract per-order facts first, and `rollup_from_orders` is the only
    place a rollup is computed. Two independent aggregations were how the two
    sides came to disagree in the first place;
  * both are clipped to [window_start, window_end] — the KeyCRM side iterates
    whole calendar months only as a fetching strategy;
  * the watermark is applied on the KeyCRM side, and the ids it holds back are
    passed to the DuckDB side. DuckDB's updated_at is a synced copy that can
    only be older, so applying the same timestamp independently cuts a smaller
    set here than there.

Keeping the per-order facts also makes the order-level comparison free: the API
calls are the expensive part, and a rollup throws away exactly the detail that
would show an error offsetting another.
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


# ─── Per-order facts ──────────────────────────────────────────────────────────

# The fields compared order by order. Money fields are compared with a tolerance;
# everything else must match exactly.
ORDER_FIELDS = (
    "status_id", "source_id", "manager_id", "buyer_id",
    "grand_total", "order_date", "n_lines", "qty", "line_amount",
)
MONEY_FIELDS = frozenset({"grand_total", "line_amount"})

# An order id → its facts. Both extractors below produce this shape, and the
# rollup is derived from it, so the two sides cannot aggregate differently.
OrderFacts = Dict[int, Dict[str, object]]


def rollup_from_orders(orders: OrderFacts) -> Rollup:
    """Group per-order facts into the (month, source_id) rollup.

    Deliberately the ONLY place a rollup is computed. Both sides used to
    aggregate independently, and the day they stopped agreeing on which orders
    to include, the job blamed the warehouse for the difference.
    """
    rollup: Dict[Tuple[str, int], Dict[str, float]] = defaultdict(
        lambda: {"orders": 0, "qty": 0, "revenue": 0.0,
                 "returns_count": 0, "returns_revenue": 0.0}
    )
    for facts in orders.values():
        order_date = facts["order_date"]
        key = (order_date.strftime("%Y-%m"), int(facts["source_id"]))
        cell = rollup[key]
        total = float(facts["grand_total"])
        if facts["status_id"] in RETURN_STATUS_IDS:
            cell["returns_count"] += 1
            cell["returns_revenue"] += total
        else:
            cell["orders"] += 1
            cell["qty"] += int(facts["qty"])
            cell["revenue"] += total
    return dict(rollup)


def duckdb_orders_in_window(
    conn,
    window_start: date,
    window_end: date,
    *,
    watermark: datetime,
    exclude_ids: "set[int] | frozenset[int] | None" = None,
) -> OrderFacts:
    """Per-order facts from the warehouse, on the same terms as the KeyCRM side."""
    excluded = [int(i) for i in (exclude_ids or ())]
    rows = conn.execute(f"""
        SELECT o.id, o.status_id, o.source_id, o.manager_id, o.buyer_id,
               CAST(o.grand_total AS DOUBLE),
               (o.ordered_at AT TIME ZONE 'Europe/Kyiv')::DATE,
               COALESCE(li.n, 0), COALESCE(li.qty, 0), COALESCE(li.amount, 0)
        FROM orders o
        LEFT JOIN (
            SELECT order_id, COUNT(*) AS n, SUM(quantity) AS qty,
                   ROUND(SUM(price_sold * quantity), 2) AS amount
            FROM order_products GROUP BY order_id
        ) li ON li.order_id = o.id
        WHERE (o.ordered_at AT TIME ZONE 'Europe/Kyiv')::DATE BETWEEN ?::DATE AND ?::DATE
          AND o.source_id IN ({", ".join(str(s) for s in ACTIVE_SOURCES)})
          AND (o.updated_at IS NULL OR o.updated_at < ?::TIMESTAMP WITH TIME ZONE)
          AND NOT list_contains(CAST(? AS BIGINT[]), o.id)
    """, [window_start.isoformat(), window_end.isoformat(),
          watermark.astimezone(timezone.utc).isoformat(), excluded]).fetchall()
    return {
        int(r[0]): {
            "status_id": r[1], "source_id": r[2],
            "manager_id": r[3], "buyer_id": r[4],
            "grand_total": float(r[5] or 0), "order_date": r[6],
            "n_lines": int(r[7]), "qty": int(r[8]), "line_amount": float(r[9] or 0),
        }
        for r in rows
    }


# ─── DuckDB rollup ────────────────────────────────────────────────────────────


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


async def keycrm_orders_in_window(
    window_start: date,
    window_end: date,
    *,
    watermark: datetime,
) -> Tuple[OrderFacts, int, set]:
    """Fetch per-order facts from KeyCRM for the window.

    KeyCRM has a 5000-row pagination cap, so we fetch one month at a time with
    ±2 day widening for backdated orders. Each request is counted in api_calls.

    Returns: (orders, api_calls_used, inflight_ids).
    """
    from core.keycrm import KeyCRMClient

    months = _enumerate_months(window_start, window_end)
    orders: OrderFacts = {}
    watermark_utc = watermark.astimezone(timezone.utc)
    inflight: set[int] = set()
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
                "include": "products,manager,buyer",
                "filter[created_between]": f"{f_start},{f_end}",
            }
            async for batch in client.paginate("order", params=params, page_size=50):
                page_count += 1
                _process_batch(batch, orders, watermark_utc,
                               window_start, window_end, inflight)
            # Pass 2: updated_between (status changes on backdated orders)
            params_upd = {
                "include": "products,manager,buyer",
                "filter[updated_between]": f"{f_start},{f_end}",
            }
            async for batch in client.paginate("order", params=params_upd, page_size=50):
                page_count += 1
                _process_batch(batch, orders, watermark_utc,
                               window_start, window_end, inflight)

            api_calls += page_count
            logger.debug(f"DQ reconciliation: month={m_str} pages={page_count}")

    finally:
        await client.close()

    return orders, api_calls, inflight


def _process_batch(
    batch: list,
    orders: OrderFacts,
    watermark_utc: datetime,
    window_start: date,
    window_end: date,
    inflight: set,
) -> None:
    """Record one KeyCRM page of orders as per-order facts, deduplicating by id.

    Every order is kept under its own id no matter which month's fetch surfaced
    it. The month loop only decides what we ask KeyCRM for; it has no business
    deciding what counts. It used to: orders whose month did not match the pass
    being iterated were thrown away, which silently lost anything whose
    ordered_at, created_at and updated_at fall in different months. Order 40028
    — created 2026-05-01, ordered 2026-06-04, updated 2026-07-03 — is reachable
    only through July's updated_between window, by which point June had already
    been processed.

    Membership of `orders`/`inflight` is the dedupe: an order already resolved
    is skipped, and one merely glimpsed by a neighbouring month's ±2 day
    widening is left for whichever pass can actually resolve it.
    """
    for o in batch:
        oid = o.get("id")
        if oid is None or oid in orders or oid in inflight:
            continue
        src = o.get("source_id")
        if src not in ACTIVE_SOURCES:
            continue
        oa = o.get("ordered_at")
        if not oa:
            continue

        try:
            dt = datetime.fromisoformat(str(oa).replace("Z", "+00:00")).astimezone(KYIV)
        except (ValueError, TypeError):
            continue
        if not (window_start <= dt.date() <= window_end):
            continue

        # Watermark: orders touched since as_of are still settling. Their ids go
        # back to the caller so the warehouse side can hold back the same set.
        ua = o.get("updated_at") or oa
        try:
            ua_dt = datetime.fromisoformat(str(ua).replace("Z", "+00:00"))
            if ua_dt.tzinfo is None:
                ua_dt = ua_dt.replace(tzinfo=timezone.utc)
            if ua_dt >= watermark_utc:
                inflight.add(int(oid))
                continue
        except (ValueError, TypeError):
            pass

        status = o.get("status_id")
        if status is None and isinstance(o.get("status"), dict):
            status = o["status"].get("id")
        try:
            gt = float(o.get("grand_total") or 0)
        except (TypeError, ValueError):
            gt = 0.0

        manager = o.get("manager")
        buyer = o.get("buyer")
        products = o.get("products") or []
        qty = 0
        line_amount = 0.0
        for p in products:
            try:
                q = int(p.get("quantity") or 0)
            except (TypeError, ValueError):
                q = 0
            try:
                price = float(p.get("price_sold") or 0)
            except (TypeError, ValueError):
                price = 0.0
            qty += q
            line_amount += price * q

        orders[int(oid)] = {
            "status_id": status,
            "source_id": int(src),
            "manager_id": manager.get("id") if isinstance(manager, dict) else None,
            "buyer_id": buyer.get("id") if isinstance(buyer, dict) else None,
            "grand_total": gt,
            "order_date": dt.date(),
            "n_lines": len(products),
            "qty": qty,
            "line_amount": round(line_amount, 2),
        }
