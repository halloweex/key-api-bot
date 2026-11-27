"""Managers and a year of their orders, landed in Bronze for Silver to derive.

For the managers screen (`GET /api/managers`), which lists each manager with
the sales type Silver gave their orders and a year of their revenue. Seeded
as KeyCRM lands it — `bronze.managers`, a baseline interval in
`app.manager_classifications` per manager, `bronze.orders` — and derived the
way production derives it (`tests/integration/pg_tab_seed.derive`), so the
sales types are Silver's own CASE, never a fake's.

The managers are the three kinds the screen must tell apart, plus one who has
sold nothing: the wholesale manager (`is_retail` FALSE, and `b2b` all the
same), a retail one, a staff one (classified, not retail: `internal`). Every
"N days ago" is noon on a Kyiv calendar day, so no order sits near a window
edge. The ids and the kinds come from `test_managers_two_engines.py`.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from core.duckdb_constants import B2B_MANAGER_ID

KYIV = ZoneInfo("Europe/Kyiv")
BASELINE = date(1970, 1, 1)

RETAIL_MANAGER = 22
STAFF_MANAGER = 34
NEVER_SOLD = 99
RETURN_STATUS = 19          # KeyCRM's lost/cancel group: no revenue

# (id, name, is_retail), each with a baseline interval of the same answer.
MANAGERS = [
    (B2B_MANAGER_ID, "Wholesale", False),
    (RETAIL_MANAGER, "Retail", True),
    (STAFF_MANAGER, "Staff", False),
    (NEVER_SOLD, "Never Sold", False),
]


def today_in_kyiv() -> date:
    return datetime.now(timezone.utc).astimezone(KYIV).date()


def kyiv_noon(days_ago: int) -> datetime:
    """Noon on the Kyiv day `days_ago` before today, as the UTC instant."""
    day = today_in_kyiv() - timedelta(days=days_ago)
    return datetime.combine(day, time(12), tzinfo=KYIV).astimezone(timezone.utc)


def orders():
    """`(id, source_id, status_id, grand_total, ordered_at, manager_id)`."""
    return [
        (1, 1, 1, Decimal("7000.00"), kyiv_noon(20), B2B_MANAGER_ID),
        (2, 2, 1, Decimal("333.33"), kyiv_noon(100), B2B_MANAGER_ID),
        (3, 2, 1, Decimal("2500.00"), kyiv_noon(10), RETAIL_MANAGER),
        # A return and an order from before the year: neither is revenue here.
        (4, 1, RETURN_STATUS, Decimal("900.00"), kyiv_noon(5), RETAIL_MANAGER),
        (5, 1, 1, Decimal("650.00"), kyiv_noon(400), RETAIL_MANAGER),
        (6, 4, 1, Decimal("450.00"), kyiv_noon(30), STAFF_MANAGER),
        # A website order with no manager: retail, and nobody's on the screen.
        (7, 4, 1, Decimal("1200.50"), kyiv_noon(3), None),
    ]


# What the screen must answer for that fixture: (is_retail, sales_type,
# revenue_365d) per manager, highest revenue first.
EXPECTED_LIST = [
    (B2B_MANAGER_ID, False, "b2b", 7333.33),
    (RETAIL_MANAGER, True, "retail", 2500.0),
    (STAFF_MANAGER, False, "internal", 450.0),
    (NEVER_SOLD, False, None, 0.0),
]


async def populate(conn, pool) -> None:
    """`tab_in_postgres`'s populate: the managers, their baselines and their
    orders landed, then Silver and Gold derived."""
    from tests.integration.pg_tab_seed import derive

    await conn.executemany(
        "INSERT INTO bronze.managers (id, name, is_retail) VALUES ($1, $2, $3)",
        MANAGERS)
    await conn.executemany(
        "INSERT INTO app.manager_classifications (manager_id, is_retail, valid_from)"
        " VALUES ($1, $2, $3)", [(m, retail, BASELINE) for m, _n, retail in MANAGERS])
    await conn.executemany(
        "INSERT INTO bronze.orders (id, source_id, status_id, grand_total,"
        " ordered_at, created_at, updated_at, manager_id)"
        " VALUES ($1, $2, $3, $4, $5, $5, $5, $6)", orders())
    await derive(pool)
