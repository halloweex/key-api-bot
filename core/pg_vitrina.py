"""app.customer_profile — the витрина: one row per retail customer.

Step 2 of «Одна бронза». Facts only — first/last order, counts, money, city.
Deliberately no value tier: tier thresholds are campaign policy living in the
SMS endpoints, and policy materialised into a table is policy that silently
disagrees with the code the day either changes. A reader computes a tier from
facts; nobody can un-bake a stale one.

Rebuilt whole from `silver.orders` in the same scheduler tick as Gold,
immediately after it — one floor, one tick, revision 0007's reasoning: the
витрина can never aggregate a Silver the next statement is about to replace.
TRUNCATE + INSERT in one transaction, `pg_silver`'s reasons.

Retail only, non-return, active sources — the same slice the SMS audience
machinery reads, which is what makes the numbers here recognisable to the one
consumer that exists today. `internal` is admin-gated on the dashboard, and a
customer row that mixed it in would leak what the API deliberately hides.

Which Postgres this ultimately lives in — this one or the shop's — is the
owner's open question. Until answered it is built here; being derived, moving
it costs one rebuild elsewhere and nothing here.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

# One statement, so the profile is one snapshot of one Silver. FILTER instead
# of a WHERE on returns: a customer whose only orders are returns still gets a
# row (orders_count 0, returns_count N) — invisible-because-filtered is how
# support ends up telling a customer they do not exist.
_REBUILD_SQL = """
INSERT INTO app.customer_profile
       (buyer_id, first_order_date, last_order_date, orders_count,
        ltv, avg_order, returns_count, city, refreshed_at)
SELECT
    s.buyer_id,
    MIN(s.order_date) FILTER (WHERE NOT s.is_return),
    MAX(s.order_date) FILTER (WHERE NOT s.is_return),
    COUNT(*)          FILTER (WHERE NOT s.is_return),
    COALESCE(SUM(s.grand_total) FILTER (WHERE NOT s.is_return), 0),
    COALESCE(
        ROUND(
            SUM(s.grand_total) FILTER (WHERE NOT s.is_return)
            / NULLIF(COUNT(*) FILTER (WHERE NOT s.is_return), 0),
            2
        ),
        0
    ),
    COUNT(*)          FILTER (WHERE s.is_return),
    MAX(b.city),
    now()
FROM silver.orders s
LEFT JOIN bronze.buyers b ON b.id = s.buyer_id
WHERE s.buyer_id IS NOT NULL
  AND s.sales_type = 'retail'
  AND s.is_active_source
GROUP BY s.buyer_id
"""


async def rebuild_customer_profile(pool=None) -> Dict[str, Any]:
    """Replace the витрина from the Silver just written. Raises on fault —
    the caller's except block is the single place these failures are logged."""
    from core.pg import get_pool, require_revision

    pool = pool or await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE app.customer_profile")
            result = await conn.execute(_REBUILD_SQL)
    # asyncpg returns "INSERT 0 <n>"
    rows = int(result.split()[-1])
    return {"rows": rows}


# An independent recomputation, deliberately not `_REBUILD_SQL` reversed: it
# aggregates without the buyers join, so a join that started fanning rows out
# would disagree with it rather than agree twice.
_CHECK_SQL = """
WITH s AS (
    SELECT buyer_id,
           COUNT(*) FILTER (WHERE NOT is_return)                        AS oc,
           COALESCE(SUM(grand_total) FILTER (WHERE NOT is_return), 0)   AS ltv
    FROM silver.orders
    WHERE buyer_id IS NOT NULL AND sales_type = 'retail' AND is_active_source
    GROUP BY buyer_id
)
SELECT
    COUNT(*) FILTER (WHERE p.buyer_id IS NULL)                          AS missing,
    COUNT(*) FILTER (WHERE s.buyer_id IS NULL)                          AS extra,
    COUNT(*) FILTER (WHERE s.buyer_id IS NOT NULL AND p.buyer_id IS NOT NULL
                     AND (s.oc <> p.orders_count OR s.ltv <> p.ltv))    AS differing
FROM s
FULL JOIN app.customer_profile p ON p.buyer_id = s.buyer_id
"""


async def reconcile_customer_profile(*, max_note: int = 3):
    """Rebuild the витрина and verify it against Silver in one snapshot.

    `ch_gold`'s shape, for `ch_gold`'s reason: the profile is legitimately one
    rebuild-tick behind Silver, so checking a *stale* profile with tolerance
    zero would report the schedule, not the data. Rebuild and check run inside
    one REPEATABLE READ transaction — one snapshot of Silver for both — and
    what the zero tolerance then measures is the materialisation itself.
    """
    from core.data_quality import IntegrityIssue, Severity
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read"):
            await conn.execute("TRUNCATE app.customer_profile")
            await conn.execute(_REBUILD_SQL)
            row = await conn.fetchrow(_CHECK_SQL)

    missing, extra, differing = int(row[0]), int(row[1]), int(row[2])
    if not (missing or extra or differing):
        return []
    return [IntegrityIssue(
        check_name="customer_profile_mismatch",
        table_name="app.customer_profile",
        severity=Severity.CRITICAL,
        count=missing + extra + differing,
        description=(
            f"the витрина disagrees with the Silver it was rebuilt from in "
            f"the same snapshot: {missing} customer(s) missing, {extra} "
            f"invented, {differing} with wrong counts or LTV. This is a "
            f"defect in the materialisation, not drift — the two queries saw "
            f"one Silver."
        ),
    )]
