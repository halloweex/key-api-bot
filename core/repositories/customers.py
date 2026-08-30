"""DuckDBStore customer insights methods."""
from __future__ import annotations

import json
import math
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from dataclasses import asdict, dataclass
from typing import Optional, Dict, Any, List, Sequence, Union


class DlrEventRebound(Exception):
    """A delivery callback named a different message than its event id first did.

    The gateway's signature covers the event id alone, so this is the only
    thing that separates a genuine report from a captured (id, signature) pair
    aimed at somebody else's message. It is raised rather than returned so a
    caller that has not thought about it fails loudly instead of writing.
    """

    def __init__(self, event_id: str, bound_message_id: str, message_id: str):
        super().__init__(
            f"event {event_id} first reported on message {bound_message_id}, "
            f"now claims {message_id}"
        )
        self.event_id = event_id
        self.bound_message_id = bound_message_id
        self.message_id = message_id


def _norm_cdf(x: float) -> float:
    """Standard normal CDF, via erf — avoids pulling scipy in for one number."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


# Purchases an arm needs before any verdict is worth printing.
#
# The interval arithmetic below stays defined at zero purchases, but "defined"
# is not "informative": a control that has not bought yet carries almost no
# information about the rate it will settle at, and the first day of a campaign
# always looks like that. Five is the usual floor for a normal approximation to
# a count, and it is the point below which the interval is driven by the prior
# baked into the correction rather than by anything observed.
MIN_EVENTS_FOR_VERDICT = 5


def _compare_groups(
    target: Dict[str, Any], holdout: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Compare a messaged group against its control.

    Returns the difference in conversion rate with a 95% interval and a
    two-sided p-value, plus incremental revenue and margin per contact. The
    interval is the point of the exercise: with a 10% holdout the control is
    small, so a difference that looks large can still be indistinguishable
    from noise, and the interval says so where a bare percentage would not.

    The interval is Agresti-Caffo, not Wald. Wald's standard error is
    ``sqrt(p_t(1-p_t)/n_t + p_h(1-p_h)/n_h)``, which contributes exactly zero
    variance from an arm where nobody has bought — it asserts that arm's rate
    is known to be precisely 0%. On the first real campaign that turned two
    tiers with empty control groups into "effect proven" within a day of
    sending, one of them at p = 0.0. With 162 people and no purchases the true
    rate can still be anything up to about 1.85% (the rule of three), which
    was the entire measured lift. Adding one success and one failure to each
    arm before forming the interval removes the collapse and has better
    coverage than Wald at every sample size worth caring about.

    The p-value is the pooled (score) test rather than Wald's, for the same
    reason: pooling the two arms keeps the standard error away from zero when
    one of them has no events.

    ``significant`` additionally requires ``MIN_EVENTS_FOR_VERDICT`` purchases
    in *both* arms. Widening the interval alone is not enough — at one purchase
    in a control of 646 the interval still clears zero, and the honest reading
    of that is "too early", not "proven".

    Returns None when either group is empty — there is nothing to compare.
    """
    t_n, h_n = target["contacts"], holdout["contacts"]
    if not t_n or not h_n:
        return None

    t_x, h_x = target["converted"], holdout["converted"]
    p_t, p_h = t_x / t_n, h_x / h_n
    diff = p_t - p_h

    # Agresti-Caffo: +1 success and +1 failure per arm, then an ordinary Wald
    # interval on the adjusted proportions. The point estimate stays the raw
    # difference — only the interval is adjusted.
    a_t, a_h = (t_x + 1) / (t_n + 2), (h_x + 1) / (h_n + 2)
    se_ac = math.sqrt(
        a_t * (1 - a_t) / (t_n + 2) + a_h * (1 - a_h) / (h_n + 2)
    )
    centre = a_t - a_h
    lo, hi = centre - 1.96 * se_ac, centre + 1.96 * se_ac

    p_pooled = (t_x + h_x) / (t_n + h_n)
    se_pooled = math.sqrt(p_pooled * (1 - p_pooled) * (1 / t_n + 1 / h_n))
    z = diff / se_pooled if se_pooled > 0 else 0.0
    p_value = 2 * (1 - _norm_cdf(abs(z)))

    enough_events = min(t_x, h_x) >= MIN_EVENTS_FOR_VERDICT

    rev_per_contact = target["revenue"] / t_n - holdout["revenue"] / h_n
    margin_per_contact = target["margin"] / t_n - holdout["margin"] / h_n

    return {
        "conversionTarget": round(100 * p_t, 2),
        "conversionHoldout": round(100 * p_h, 2),
        "liftPp": round(100 * diff, 2),
        "liftRelativePct": round(100 * diff / p_h, 1) if p_h > 0 else None,
        "ci95Pp": [round(100 * lo, 2), round(100 * hi, 2)],
        "pValue": round(p_value, 4),
        # The honest headline: if the interval spans zero, the campaign has
        # not been shown to have done anything, whatever the raw rates say —
        # and below the event floor there is nothing to say either way.
        "significant": enough_events and (lo > 0 or hi < 0),
        # Why a verdict is or is not being offered, so the page can say
        # "too early" instead of the much stronger "no effect".
        "verdictReady": enough_events,
        "eventsTarget": t_x,
        "eventsHoldout": h_x,
        "minEvents": MIN_EVENTS_FOR_VERDICT,
        "incrementalRevenuePerContact": round(rev_per_contact, 2),
        "incrementalMarginPerContact": round(margin_per_contact, 2),
        "incrementalRevenueTotal": round(rev_per_contact * t_n, 2),
        "incrementalMarginTotal": round(margin_per_contact * t_n, 2),
    }

from core.duckdb_constants import B2B_MANAGER_ID, RETAIL_MANAGER_IDS
from core.sms_holdout import assign_arm
from core.pg_sms import refuse_while_unported, sms_store_is_postgres
from core.sql_dialect import DUCKDB, POSTGRES, sms_segments_select

# Tier cut-offs per LTV basis for get_sms_segments.
#
# The margin figures are calibrated so each tier selects roughly the same
# share of the base as its revenue counterpart (blended margin runs ~54%):
# on current data revenue >=10000/5000 picks 1325/3108 customers, margin
# >=5500/2750 picks 1346/3149. Switching basis therefore re-ranks who lands
# in a tier without silently resizing it.
SMS_LTV_BASES = ("revenue", "margin")

SMS_TIER_DEFAULTS = {
    "revenue": {"vip": 10000.0, "core": 5000.0},
    "margin": {"vip": 5500.0, "core": 2750.0},
}


# How a campaign's audience is split into arms.
#
# `rfm` is the historical behaviour: three tiers by lifetime value and order
# count, and anyone who falls in none of them is dropped. It answers "who is
# worth what", which is the right question for a blanket offer and the wrong
# one for "everybody who bought this brand" — under `rfm` half of that audience
# would silently fall out for being a one-order buyer past the reactivation
# window.
#
# `single` puts everyone the filters kept into one arm named ALL. One arm is
# also the only honest split for a small cohort: three arms of 200 measure
# nothing at all.
SMS_GROUPINGS = ("rfm", "single")

# Audiences that ship with the page.
#
# The first one is the cohort every campaign before 2026-08 was sent to,
# written down: a 270-day window, three value tiers, 10% withheld. It was a
# set of defaults nobody could see or name, which made "the usual list" a
# thing only the code knew. Naming it costs nothing and makes the alternative
# — a filtered audience — an obvious choice rather than an unknown one.
BUILTIN_AUDIENCE_PRESETS: Dict[str, Dict[str, Any]] = {
    "RFM tiers": {
        "grouping": "rfm",
        "ltvBasis": "margin",
        "maxRecencyDays": 270,
        "coreMinOrders": 2,
        "reactivationMaxRecency": 120,
        "salesType": "retail",
        "holdoutPct": 10,
        "filters": {},
    },
    "Everyone reachable": {
        "grouping": "single",
        "ltvBasis": "margin",
        "maxRecencyDays": 270,
        "salesType": "retail",
        "holdoutPct": 20,
        "filters": {},
    },
}

SINGLE_GROUP_NAME = "ALL"


@dataclass(frozen=True)
class SmsAudienceFilters:
    """Who the campaign is for, beyond the tier rules.

    Every field is optional and an unset field is not a predicate — an empty
    filter set has to select exactly what the page selected before this
    existed, or every past campaign becomes unreproducible.

    Two families, and they read the customer differently:

    * aggregate — recency, order count, lifetime value, average order, when
      they first bought. Computed over the customer's whole history, so they
      narrow *who* is in the audience without changing what anyone is worth.
    * content — brand, category, source, promocode, optionally inside a
      window. These ask whether the customer ever bought a particular thing,
      as an EXISTS over their orders. Deliberately not a join: filtering the
      line items would recompute LTV from the matching lines alone, and a
      customer's value is not "what they spent on this brand".
    """

    recency_min_days: Optional[int] = None
    recency_max_days: Optional[int] = None
    orders_min: Optional[int] = None
    orders_max: Optional[int] = None
    ltv_min: Optional[float] = None
    ltv_max: Optional[float] = None
    aov_min: Optional[float] = None
    aov_max: Optional[float] = None
    first_order_from: Optional[date] = None
    first_order_to: Optional[date] = None
    cities: Sequence[str] = ()
    brands: Sequence[str] = ()
    category_ids: Sequence[int] = ()
    source_ids: Sequence[int] = ()
    promocode: Optional[str] = None
    bought_within_days: Optional[int] = None

    @property
    def content_only(self) -> tuple:
        """The content predicates, if any — the ones needing an EXISTS."""
        return (self.brands, self.category_ids, self.source_ids, self.promocode)

    def is_empty(self) -> bool:
        """No predicate at all, so no filtering stage to speak of."""
        return not any(
            v not in (None, (), [], "") for v in (
                self.recency_min_days, self.recency_max_days,
                self.orders_min, self.orders_max,
                self.ltv_min, self.ltv_max, self.aov_min, self.aov_max,
                self.first_order_from, self.first_order_to,
                tuple(self.cities), tuple(self.brands), tuple(self.category_ids),
                tuple(self.source_ids), self.promocode,
            )
        )

    def as_dict(self) -> Dict[str, Any]:
        """JSON-safe echo, unset fields dropped.

        Frozen with the campaign, so it has to survive a round trip: this is
        what tells a reader six months later which audience was messaged.
        """
        out: Dict[str, Any] = {}
        for key, value in asdict(self).items():
            if value in (None, (), [], ""):
                continue
            if isinstance(value, (list, tuple)):
                out[key] = list(value)
            elif isinstance(value, date):
                out[key] = value.isoformat()
            else:
                out[key] = value
        return out

    def predicate(
        self, ltv_column: str, sales_type: str, order_lines: str = "silver_order_lines",
    ) -> tuple:
        """SQL boolean over one row of `scored`, plus its bound parameters.

        Returns ``("TRUE", [])`` when nothing is set, which keeps the funnel
        stage present and equal to the stage before it rather than making the
        whole query shape conditional.

        `order_lines` is the only engine-dependent thing in here — every
        operator used below means the same in both. It comes from the dialect
        rather than being hardcoded, because this fragment is spliced into a
        query that may be running against either store.
        """
        parts: List[str] = []
        params: List[Any] = []

        def between(column: str, lo, hi):
            if lo is not None:
                parts.append(f"{column} >= ?")
                params.append(lo)
            if hi is not None:
                parts.append(f"{column} <= ?")
                params.append(hi)

        # Recency runs backwards: "at least 30 days ago" is a *minimum* on the
        # number of days, and reads as the older edge of the window.
        between("recency", self.recency_min_days, self.recency_max_days)
        between("orders", self.orders_min, self.orders_max)
        between(ltv_column, self.ltv_min, self.ltv_max)
        # AOV is derived, not stored; orders is never zero here (a customer
        # exists because they ordered), but NULLIF keeps that honest.
        between(f"({ltv_column} / NULLIF(orders, 0))", self.aov_min, self.aov_max)
        between("first_order_date", self.first_order_from, self.first_order_to)

        if self.cities:
            placeholders = ", ".join("?" * len(self.cities))
            # Cities are free text from KeyCRM: "Київ" and "київ" are the same
            # city and neither spelling is canonical.
            parts.append(f"lower(city) IN ({placeholders})")
            params += [c.strip().lower() for c in self.cities]

        content: List[str] = []
        content_params: List[Any] = []
        if self.brands:
            placeholders = ", ".join("?" * len(self.brands))
            content.append(f"cl.brand IN ({placeholders})")
            content_params += list(self.brands)
        if self.category_ids:
            placeholders = ", ".join("?" * len(self.category_ids))
            # A parent category means the whole branch: picking "Face care"
            # and getting nothing because every product hangs off a child of
            # it is the kind of empty result nobody debugs, they just stop
            # trusting the filter.
            content.append(
                f"(cl.category_id IN ({placeholders}) "
                f"OR cl.parent_category_id IN ({placeholders}))"
            )
            content_params += list(self.category_ids) + list(self.category_ids)
        if self.source_ids:
            placeholders = ", ".join("?" * len(self.source_ids))
            content.append(f"cl.source_id IN ({placeholders})")
            content_params += list(self.source_ids)
        if self.promocode:
            content.append("upper(cl.promocode) = upper(?)")
            content_params.append(self.promocode.strip())

        if content:
            window = ""
            if self.bought_within_days is not None:
                # Interval cannot be parameterised in DuckDB; the value is an
                # int by construction, never user text.
                window = (
                    f" AND cl.order_date >= CURRENT_DATE "
                    f"- INTERVAL '{int(self.bought_within_days)} days'"
                )
            sales_clause = "" if sales_type == "all" else "AND cl.sales_type = ?"
            exists_params: List[Any] = []
            if sales_type != "all":
                exists_params.append(sales_type)
            parts.append(f"""EXISTS (
                    SELECT 1 FROM {order_lines} cl
                    WHERE cl.buyer_id = scored.buyer_id
                      AND NOT cl.is_return
                      AND cl.is_active_source
                      {sales_clause}
                      AND {' AND '.join(content)}
                      {window}
                )""")
            params += exists_params + content_params

        if not parts:
            return "TRUE", []
        return "(" + " AND ".join(parts) + ")", params


def _load_json(raw: Any) -> Dict[str, Any]:
    """A stored JSON snapshot, or nothing. Never raises on a bad row."""
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _local_times(row: Sequence[Any]) -> tuple:
    """asyncpg's UTC timestamps in the offset DuckDB would have used.

    Both engines return the same *instant*; they disagree on how it is
    rendered, because DuckDB hands back values in its session timezone and
    asyncpg always returns UTC. Nothing downstream compares instants — the API
    calls `.isoformat()` — so without this the same campaign would carry
    `2026-08-20T13:00:00+04:00` from one store and `…T09:00:00+00:00` from the
    other, and a frontend that slices the first ten characters off a timestamp
    would show a different date either side of midnight.

    Converting here rather than rendering everything in UTC keeps today's
    responses byte-identical: the engine switch has to be invisible to the
    page, which means matching what the page already receives rather than
    picking the tidier convention.
    """
    return tuple(
        v.astimezone() if isinstance(v, datetime) and v.tzinfo is not None else v
        for v in row
    )


class _SmsTx:
    """Statements inside one transaction, rendered for one engine.

    The `{table}` holes and `?` markers are the same ones `_sms_run` fills, so
    a statement reads identically whether it runs alone or in a transaction.
    """

    def __init__(self, dialect):
        self._dialect = dialect

    def _render(self, sql: str) -> str:
        return sql.format(
            campaigns=self._dialect.sms_campaigns,
            members=self._dialect.sms_campaign_members,
            presets=self._dialect.sms_audience_presets,
            optouts=self._dialect.marketing_optouts,
            dlr_events=self._dialect.sms_dlr_events,
            lines=self._dialect.order_lines,
            stocks=self._dialect.offer_stocks,
            buyers=self._dialect.buyers,
        )


class _DuckTx(_SmsTx):
    def __init__(self, conn, dialect):
        super().__init__(dialect)
        self._conn = conn

    async def all(self, sql, params=None):
        return self._conn.execute(self._render(sql), list(params or [])).fetchall()

    async def one(self, sql, params=None):
        return self._conn.execute(self._render(sql), list(params or [])).fetchone()

    async def none(self, sql, params=None):
        self._conn.execute(self._render(sql), list(params or []))

    async def many(self, sql, rows):
        self._conn.executemany(self._render(sql), [list(r) for r in rows])


class _PgTx(_SmsTx):
    def __init__(self, conn, dialect):
        super().__init__(dialect)
        self._conn = conn

    def _sql(self, sql: str) -> str:
        from core import pg_sms_read

        return pg_sms_read.numbered(self._render(sql))

    async def all(self, sql, params=None):
        rows = await self._conn.fetch(self._sql(sql), *(params or []))
        return [_local_times(r) for r in rows]

    async def one(self, sql, params=None):
        row = await self._conn.fetchrow(self._sql(sql), *(params or []))
        return _local_times(row) if row is not None else None

    async def none(self, sql, params=None):
        await self._conn.execute(self._sql(sql), *(params or []))

    async def many(self, sql, rows):
        await self._conn.executemany(self._sql(sql), [tuple(r) for r in rows])


class CustomersMixin:

    async def get_customer_insights(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail",
        promocode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get customer insights: new vs returning, AOV trend (from Gold/Silver layers)."""
        async with self.connection() as conn:
            # ── Base metrics from gold_daily_revenue ──
            params = [start_date, end_date]
            where_clauses = ["date BETWEEN ? AND ?"]

            if sales_type != "all":
                where_clauses.append("sales_type = ?")
                params.append(sales_type)

            where_sql = " AND ".join(where_clauses)

            gold_result = conn.execute(f"""
                SELECT
                    SUM(unique_customers) as total_customers,
                    SUM(orders_count) as total_orders,
                    SUM(revenue) as total_revenue,
                    SUM(new_customers) as new_customers,
                    SUM(returning_customers) as returning_customers
                FROM gold_daily_revenue
                WHERE {where_sql}
            """, params).fetchone()

            total_customers = int(gold_result[0] or 0)
            total_orders = int(gold_result[1] or 0)
            total_revenue = float(gold_result[2] or 0)
            new_customers = int(gold_result[3] or 0)
            returning_customers = int(gold_result[4] or 0)

            # AOV trend from gold_daily_revenue
            aov_results = conn.execute(f"""
                SELECT date,
                       CASE WHEN orders_count > 0 THEN revenue / orders_count ELSE 0 END as aov
                FROM gold_daily_revenue
                WHERE {where_sql}
                ORDER BY date
            """, params).fetchall()
            aov_by_day = {row[0]: float(row[1]) for row in aov_results}

            labels = []
            aov_data = []
            current = start_date
            while current <= end_date:
                labels.append(current.strftime("%d.%m"))
                aov_data.append(round(aov_by_day.get(current, 0), 2))
                current += timedelta(days=1)

            overall_aov = total_revenue / total_orders if total_orders > 0 else 0

            # ── CLV metrics from silver_orders (need per-buyer aggregation) ──
            sales_where = "s.sales_type = ?" if sales_type != "all" else "1=1"
            clv_params = [sales_type] if sales_type != "all" else []

            clv_result = conn.execute(f"""
                WITH customer_stats AS (
                    SELECT
                        s.buyer_id,
                        COUNT(DISTINCT s.id) as order_count,
                        SUM(s.grand_total) as total_spent,
                        DATE_DIFF('day', MIN(s.ordered_at), MAX(s.ordered_at)) as lifespan_days
                    FROM silver_orders s
                    WHERE s.buyer_id IS NOT NULL
                      AND NOT s.is_return
                      AND s.is_active_source
                      AND {sales_where}
                    GROUP BY s.buyer_id
                    HAVING COUNT(DISTINCT s.id) > 1
                )
                SELECT
                    COUNT(*) as repeat_customer_count,
                    AVG(order_count) as avg_purchase_frequency,
                    AVG(lifespan_days) as avg_lifespan_days,
                    AVG(total_spent) as avg_customer_value
                FROM customer_stats
            """, clv_params).fetchone()

            repeat_customer_count = clv_result[0] or 0
            avg_purchase_frequency = float(clv_result[1] or 0)
            avg_lifespan_days = float(clv_result[2] or 0)
            avg_customer_value = float(clv_result[3] or 0)
            clv = avg_customer_value if repeat_customer_count > 0 else 0

            # Compute accurate unique customer counts from Silver
            # (Gold sums daily unique counts, double-counting multi-day buyers)
            pf_where = ["s.order_date BETWEEN ? AND ?", "NOT s.is_return", "s.is_active_source",
                        "s.buyer_id IS NOT NULL"]
            pf_params: list = [start_date, end_date]
            if sales_type != "all":
                pf_where.append("s.sales_type = ?")
                pf_params.append(sales_type)
            if promocode:
                pf_where.append("UPPER(s.promocode) = UPPER(?)")
                pf_params.append(promocode)
            pf_result = conn.execute(f"""
                SELECT
                    COUNT(DISTINCT s.buyer_id),
                    COUNT(DISTINCT CASE WHEN s.is_new_customer THEN s.buyer_id END),
                    COUNT(DISTINCT CASE WHEN NOT s.is_new_customer THEN s.buyer_id END)
                FROM silver_orders s
                WHERE {" AND ".join(pf_where)}
            """, pf_params).fetchone()
            unique_buyers = int(pf_result[0] or 0)
            new_customers = int(pf_result[1] or 0)
            returning_customers = int(pf_result[2] or 0)
            total_customers = unique_buyers
            purchase_frequency = total_orders / unique_buyers if unique_buyers > 0 else 0

            # All-time repeat rate from silver_orders
            alltime_result = conn.execute(f"""
                WITH customer_orders AS (
                    SELECT
                        s.buyer_id,
                        COUNT(DISTINCT s.id) as order_count
                    FROM silver_orders s
                    WHERE s.buyer_id IS NOT NULL
                      AND NOT s.is_return
                      AND s.is_active_source
                      AND {sales_where}
                    GROUP BY s.buyer_id
                )
                SELECT
                    COUNT(*) as total_customers,
                    SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) as repeat_customers,
                    AVG(order_count) as avg_orders_per_customer
                FROM customer_orders
            """, clv_params).fetchone()

            alltime_total_customers = alltime_result[0] or 0
            alltime_repeat_customers = alltime_result[1] or 0
            alltime_avg_orders = float(alltime_result[2] or 0)
            true_repeat_rate = (alltime_repeat_customers / alltime_total_customers * 100) if alltime_total_customers > 0 else 0

            return {
                "newVsReturning": {
                    "labels": ["New Customers", "Returning Customers"],
                    "data": [new_customers, returning_customers],
                    "backgroundColor": ["#2563EB", "#16A34A"]
                },
                "aovTrend": {
                    "labels": labels,
                    "datasets": [{
                        "label": "AOV (UAH)",
                        "data": aov_data,
                        "borderColor": "#F59E0B",
                        "backgroundColor": "rgba(245, 158, 11, 0.1)",
                        "fill": True,
                        "tension": 0.3
                    }]
                },
                "metrics": {
                    "totalCustomers": total_customers,
                    "newCustomers": new_customers,
                    "returningCustomers": returning_customers,
                    "totalOrders": total_orders,
                    "repeatRate": round((returning_customers / total_customers * 100) if total_customers > 0 else 0, 1),
                    "averageOrderValue": round(overall_aov, 2),
                    "customerLifetimeValue": round(clv, 2),
                    "avgPurchaseFrequency": round(avg_purchase_frequency, 2),
                    "avgCustomerLifespanDays": round(avg_lifespan_days, 0),
                    "purchaseFrequency": round(purchase_frequency, 2),
                    "totalCustomersAllTime": alltime_total_customers,
                    "repeatCustomersAllTime": alltime_repeat_customers,
                    "trueRepeatRate": round(true_repeat_rate, 1),
                    "avgOrdersPerCustomer": round(alltime_avg_orders, 2)
                }
            }

    async def get_cohort_retention(
        self,
        months_back: int = 12,
        retention_months: int = 6,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Get cohort retention analysis.

        Shows what percentage of customers from each cohort (first purchase month)
        returned to make purchases in subsequent months.

        Args:
            months_back: How many months of cohorts to analyze
            retention_months: How many months of retention to track (M0 to Mn)
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with cohorts, retention matrix, and summary metrics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_cohorts AS (
                -- Get each customer's first order month (their cohort)
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            ),
            customer_orders AS (
                -- Get all order months per customer
                SELECT DISTINCT
                    o.buyer_id,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since
                FROM silver_orders o
                JOIN customer_cohorts c ON o.buyer_id = c.buyer_id
                WHERE NOT o.is_return
                  {sales_type_filter}
            ),
            cohort_sizes AS (
                SELECT cohort_month, COUNT(DISTINCT buyer_id) AS size
                FROM customer_cohorts
                GROUP BY cohort_month
            ),
            retention_data AS (
                SELECT
                    r.cohort_month,
                    r.months_since,
                    COUNT(DISTINCT r.buyer_id) AS retained_customers
                FROM customer_orders r
                WHERE r.months_since <= ?
                GROUP BY r.cohort_month, r.months_since
            )
            SELECT
                strftime(r.cohort_month, '%Y-%m') as cohort,
                s.size as cohort_size,
                r.months_since as month_number,
                r.retained_customers,
                ROUND(100.0 * r.retained_customers / s.size, 1) as retention_pct
            FROM retention_data r
            JOIN cohort_sizes s ON r.cohort_month = s.cohort_month
            WHERE r.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{int(months_back)} months'
            ORDER BY r.cohort_month DESC, r.months_since
            """

            rows = conn.execute(query, [retention_months]).fetchall()

            # Build cohort data structure
            cohorts = {}
            for cohort, size, month_num, retained, pct in rows:
                if cohort not in cohorts:
                    cohorts[cohort] = {
                        "size": size,
                        "retention": {}
                    }
                cohorts[cohort]["retention"][month_num] = {
                    "count": retained,
                    "percent": pct
                }

            # Calculate summary metrics
            total_cohort_size = sum(c["size"] for c in cohorts.values())

            # Weighted average retention by month (weight = cohort size)
            avg_retention = {}
            for m in range(retention_months + 1):
                weighted_sum = 0
                total_weight = 0
                for c in cohorts.values():
                    entry = c["retention"].get(m)
                    if entry is not None:
                        pct = entry.get("percent", 0)
                        weighted_sum += pct * c["size"]
                        total_weight += c["size"]
                if total_weight > 0:
                    avg_retention[m] = round(weighted_sum / total_weight, 1)

            return {
                "cohorts": [
                    {
                        "month": cohort,
                        "size": data["size"],
                        "retention": [
                            data["retention"].get(m, {}).get("percent", None)
                            for m in range(retention_months + 1)
                        ]
                    }
                    for cohort, data in sorted(cohorts.items(), reverse=True)
                ],
                "retentionMonths": retention_months,
                "summary": {
                    "totalCohorts": len(cohorts),
                    "totalCustomers": total_cohort_size,
                    "avgRetention": avg_retention
                }
            }

    async def get_enhanced_cohort_retention(
        self,
        months_back: int = 12,
        retention_months: int = 6,
        sales_type: str = "retail",
        include_revenue: bool = True
    ) -> Dict[str, Any]:
        """
        Get enhanced cohort retention analysis with revenue tracking.

        Shows customer retention percentages AND revenue retention for each cohort.

        Args:
            months_back: How many months of cohorts to analyze
            retention_months: How many months of retention to track (M0 to Mn)
            sales_type: Filter by sales type (retail/b2b/all)
            include_revenue: Include revenue retention metrics

        Returns:
            Dict with cohorts, customer retention, revenue retention, and summary
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_first_order AS (
                -- Get each customer's first order month (cohort)
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            ),
            customer_cohorts AS (
                -- Add first month revenue per customer
                SELECT
                    c.buyer_id,
                    c.cohort_month,
                    COALESCE(SUM(o.grand_total), 0) AS first_month_revenue
                FROM customer_first_order c
                LEFT JOIN silver_orders o ON c.buyer_id = o.buyer_id
                    AND DATE_TRUNC('month', o.order_date) = c.cohort_month
                    AND NOT o.is_return
                GROUP BY c.buyer_id, c.cohort_month
            ),
            customer_orders AS (
                -- Get all order months per customer with revenue
                SELECT
                    o.buyer_id,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since,
                    o.grand_total AS revenue
                FROM silver_orders o
                JOIN customer_cohorts c ON o.buyer_id = c.buyer_id
                WHERE NOT o.is_return
                  {sales_type_filter}
            ),
            cohort_sizes AS (
                SELECT
                    cohort_month,
                    COUNT(DISTINCT buyer_id) AS size,
                    SUM(first_month_revenue) AS m0_revenue
                FROM customer_cohorts
                GROUP BY cohort_month
            ),
            retention_data AS (
                SELECT
                    r.cohort_month,
                    r.months_since,
                    COUNT(DISTINCT r.buyer_id) AS retained_customers,
                    SUM(r.revenue) AS period_revenue
                FROM customer_orders r
                WHERE r.months_since <= ?
                GROUP BY r.cohort_month, r.months_since
            )
            SELECT
                strftime(r.cohort_month, '%Y-%m') as cohort,
                s.size as cohort_size,
                s.m0_revenue,
                r.months_since as month_number,
                r.retained_customers,
                ROUND(100.0 * r.retained_customers / s.size, 1) as retention_pct,
                r.period_revenue,
                ROUND(100.0 * r.period_revenue / NULLIF(s.m0_revenue, 0), 1) as revenue_retention_pct
            FROM retention_data r
            JOIN cohort_sizes s ON r.cohort_month = s.cohort_month
            WHERE r.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{int(months_back)} months'
            ORDER BY r.cohort_month DESC, r.months_since
            """

            rows = conn.execute(query, [retention_months]).fetchall()

            # Build cohort data structure
            cohorts = {}
            for cohort, size, m0_rev, month_num, retained, pct, rev, rev_pct in rows:
                if cohort not in cohorts:
                    cohorts[cohort] = {
                        "size": size,
                        "m0_revenue": float(m0_rev or 0),
                        "retention": {},
                        "revenue_retention": {},
                        "revenue": {}
                    }
                cohorts[cohort]["retention"][month_num] = float(pct) if pct is not None else None
                cohorts[cohort]["revenue_retention"][month_num] = float(rev_pct) if rev_pct is not None else None
                cohorts[cohort]["revenue"][month_num] = float(rev or 0)

            # Calculate summary metrics
            total_cohort_size = sum(c["size"] for c in cohorts.values())
            total_revenue = sum(c["m0_revenue"] for c in cohorts.values())

            # Weighted average retention by month (weight = cohort size)
            avg_customer_retention = {}
            avg_revenue_retention = {}
            for m in range(retention_months + 1):
                cust_weighted_sum = 0
                cust_total_weight = 0
                rev_weighted_sum = 0
                rev_total_weight = 0
                for c in cohorts.values():
                    cust_pct = c["retention"].get(m)
                    if cust_pct is not None:
                        cust_weighted_sum += cust_pct * c["size"]
                        cust_total_weight += c["size"]
                    rev_pct = c["revenue_retention"].get(m)
                    if rev_pct is not None:
                        rev_weighted_sum += rev_pct * c["size"]
                        rev_total_weight += c["size"]
                if cust_total_weight > 0:
                    avg_customer_retention[m] = round(cust_weighted_sum / cust_total_weight, 1)
                if rev_total_weight > 0:
                    avg_revenue_retention[m] = round(rev_weighted_sum / rev_total_weight, 1)

            # ── Compute insights ──────────────────────────────────────
            sorted_cohort_list = [
                {"month": k, **v}
                for k, v in sorted(cohorts.items())
            ]

            insights = self._compute_cohort_insights(
                sorted_cohort_list, avg_customer_retention, retention_months
            )

            return {
                "cohorts": [
                    {
                        "month": cohort,
                        "size": data["size"],
                        "retention": [
                            data["retention"].get(m)
                            for m in range(retention_months + 1)
                        ],
                        "revenueRetention": [
                            data["revenue_retention"].get(m)
                            for m in range(retention_months + 1)
                        ] if include_revenue else None,
                        "revenue": [
                            round(data["revenue"].get(m, 0), 2)
                            for m in range(retention_months + 1)
                        ] if include_revenue else None
                    }
                    for cohort, data in sorted(cohorts.items(), reverse=True)
                ],
                "retentionMonths": retention_months,
                "summary": {
                    "totalCohorts": len(cohorts),
                    "totalCustomers": total_cohort_size,
                    "avgCustomerRetention": avg_customer_retention,
                    "avgRevenueRetention": avg_revenue_retention if include_revenue else None,
                    "totalRevenue": round(total_revenue, 2) if include_revenue else None
                },
                "insights": insights
            }

    @staticmethod
    def _compute_cohort_insights(
        sorted_cohorts: list,
        avg_customer_retention: dict,
        retention_months: int
    ) -> dict:
        """Compute derived insights from cohort retention data."""
        if not sorted_cohorts:
            return {
                "retentionTrend": None,
                "cohortQualityTrend": None,
                "revenueImpact": None,
                "decayAnalysis": None,
            }

        # Helper: weighted average M1 for a subset of cohorts
        def weighted_m1(cohort_list):
            total_w, total_s = 0.0, 0
            for c in cohort_list:
                m1 = c["retention"].get(1)
                if m1 is not None and c["size"] > 0:
                    total_w += float(m1) * c["size"]
                    total_s += c["size"]
            return round(total_w / total_s, 1) if total_s > 0 else None

        # ── 5A.2 Retention trend ──
        cohorts_with_m1 = [c for c in sorted_cohorts if c["retention"].get(1) is not None]
        retention_trend = None
        if len(cohorts_with_m1) >= 4:
            recent = cohorts_with_m1[-3:]
            older = cohorts_with_m1[:3]
            recent_avg = weighted_m1(recent)
            older_avg = weighted_m1(older)
            if recent_avg is not None and older_avg is not None:
                delta = round(recent_avg - older_avg, 1)
                if abs(delta) < 1:
                    direction = "stable"
                elif delta > 0:
                    direction = "improving"
                else:
                    direction = "declining"
                retention_trend = {
                    "recentM1": recent_avg,
                    "olderM1": older_avg,
                    "delta": delta,
                    "direction": direction,
                }

        # ── 5A.3 Cohort quality trend ──
        quality_scores = []
        for c in sorted_cohorts:
            m1 = c["retention"].get(1)
            if m1 is None:
                continue
            m1 = float(m1)
            m3 = c["retention"].get(3)
            score = round(0.6 * m1 + 0.4 * float(m3), 1) if m3 is not None else round(m1, 1)
            quality_scores.append({"month": c["month"], "score": score})

        cohort_quality = None
        if quality_scores:
            best = max(quality_scores, key=lambda x: x["score"])
            worst = min(quality_scores, key=lambda x: x["score"])
            latest = quality_scores[-1]
            avg_score = round(sum(q["score"] for q in quality_scores) / len(quality_scores), 1)
            cohort_quality = {
                "bestCohort": {"month": best["month"], "score": best["score"]},
                "worstCohort": {"month": worst["month"], "score": worst["score"]},
                "latestScore": latest["score"],
                "avgScore": avg_score,
            }

        # ── 5A.4 Revenue impact ──
        revenue_impact = None
        cohorts_with_data = [
            c for c in sorted_cohorts
            if c["retention"].get(1) is not None and c["size"] > 0
        ]
        if cohorts_with_data:
            best_m1 = max(float(c["retention"].get(1, 0)) for c in cohorts_with_data)
            total_extra_customers = 0.0
            total_m1_revenue = 0.0
            total_m1_customers = 0
            for c in cohorts_with_data:
                current_m1 = float(c["retention"].get(1, 0) or 0)
                m1_rev = float(c.get("revenue", {}).get(1, 0) or 0)
                m1_cust_count = round(c["size"] * current_m1 / 100) if current_m1 > 0 else 0
                total_m1_revenue += m1_rev
                total_m1_customers += m1_cust_count
                if current_m1 < best_m1:
                    extra = c["size"] * (best_m1 - current_m1) / 100
                    total_extra_customers += extra

            avg_rev_per_cust = (
                total_m1_revenue / total_m1_customers
                if total_m1_customers > 0 else 0
            )
            potential = round(total_extra_customers * avg_rev_per_cust, 2)
            num_cohorts = len(cohorts_with_data)
            monthly_potential = round(potential / num_cohorts, 2) if num_cohorts > 0 else 0

            revenue_impact = {
                "bestM1": best_m1,
                "potentialExtraCustomers": round(total_extra_customers),
                "potentialExtraRevenue": potential,
                "monthlyPotential": monthly_potential,
            }

        # ── 5A.5 Decay analysis ──
        m1_avg = float(avg_customer_retention.get(1, 0) or 0)
        half_life = None
        if m1_avg > 0:
            for m in range(2, retention_months + 1):
                val = float(avg_customer_retention.get(m, 0) or 0)
                if val <= m1_avg / 2:
                    half_life = m
                    break

        stabilization_month = None
        for m in range(2, retention_months + 1):
            prev = float(avg_customer_retention.get(m - 1, 0) or 0)
            curr = float(avg_customer_retention.get(m, 0) or 0)
            if prev > 0 and abs(prev - curr) < 2:
                stabilization_month = m
                break

        terminal_raw = avg_customer_retention.get(retention_months)
        terminal = float(terminal_raw) if terminal_raw is not None else None
        m3_val = float(avg_customer_retention.get(3, 0) or 0)
        m1_to_m3_drop = round(m1_avg - m3_val, 1) if m1_avg > 0 and m3_val else 0

        decay_analysis = {
            "halfLifeMonth": half_life,
            "stabilizationMonth": stabilization_month,
            "terminalRetention": terminal,
            "m1ToM3Drop": m1_to_m3_drop,
        }

        return {
            "retentionTrend": retention_trend,
            "cohortQualityTrend": cohort_quality,
            "revenueImpact": revenue_impact,
            "decayAnalysis": decay_analysis,
        }

    async def get_days_to_second_purchase(
        self,
        months_back: int = 12,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Analyze time between first and second purchase.

        Groups customers into buckets based on how many days it took them
        to make their second purchase. Useful for understanding repurchase cycles.

        Args:
            months_back: How many months of first-time customers to analyze
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with buckets, customer counts, and summary statistics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_orders_ranked AS (
                SELECT
                    o.buyer_id,
                    o.order_date,
                    ROW_NUMBER() OVER (PARTITION BY o.buyer_id ORDER BY o.order_date) AS order_num
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
            ),
            second_purchase AS (
                SELECT
                    c1.buyer_id,
                    DATEDIFF('day', c1.order_date, c2.order_date) AS days_to_second
                FROM customer_orders_ranked c1
                JOIN customer_orders_ranked c2
                    ON c1.buyer_id = c2.buyer_id
                    AND c1.order_num = 1
                    AND c2.order_num = 2
                WHERE c1.order_date >= CURRENT_DATE - INTERVAL '{int(months_back)} months'
            ),
            bucketed AS (
                SELECT
                    days_to_second,
                    CASE
                        WHEN days_to_second <= 30 THEN '0-30'
                        WHEN days_to_second <= 60 THEN '31-60'
                        WHEN days_to_second <= 90 THEN '61-90'
                        WHEN days_to_second <= 120 THEN '91-120'
                        WHEN days_to_second <= 180 THEN '121-180'
                        ELSE '180+'
                    END AS bucket,
                    CASE
                        WHEN days_to_second <= 30 THEN 1
                        WHEN days_to_second <= 60 THEN 2
                        WHEN days_to_second <= 90 THEN 3
                        WHEN days_to_second <= 120 THEN 4
                        WHEN days_to_second <= 180 THEN 5
                        ELSE 6
                    END AS bucket_order
                FROM second_purchase
            ),
            global_stats AS (
                SELECT
                    MEDIAN(days_to_second) AS median_days,
                    AVG(days_to_second) AS avg_days,
                    COUNT(*) AS total_count
                FROM second_purchase
            )
            SELECT
                b.bucket,
                COUNT(*) AS customers,
                ROUND(AVG(b.days_to_second), 1) AS avg_days,
                (SELECT median_days FROM global_stats) AS median_days,
                (SELECT avg_days FROM global_stats) AS avg_days_overall,
                (SELECT total_count FROM global_stats) AS total_count
            FROM bucketed b
            GROUP BY b.bucket, b.bucket_order
            ORDER BY b.bucket_order
            """

            rows = conn.execute(query).fetchall()

            # Extract global stats from first row
            median_days = rows[0][3] if rows else None
            avg_days_overall = rows[0][4] if rows else None

            # Calculate totals and percentages
            total_repeat = sum(row[1] for row in rows)
            buckets = []
            for row in rows:
                bucket, customers, avg_days = row[0], row[1], row[2]
                buckets.append({
                    "bucket": bucket,
                    "customers": customers,
                    "avgDays": avg_days,
                    "percentage": round(100.0 * customers / total_repeat, 1) if total_repeat > 0 else 0
                })

            return {
                "buckets": buckets,
                "summary": {
                    "totalRepeatCustomers": total_repeat,
                    "medianDays": round(median_days, 1) if median_days else None,
                    "avgDays": round(avg_days_overall, 1) if avg_days_overall else None
                }
            }

    async def get_cohort_ltv(
        self,
        months_back: int = 12,
        retention_months: int = 12,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Get cumulative lifetime value by cohort.

        Shows how much revenue each cohort has generated over time,
        with cumulative totals per month since first purchase.

        Args:
            months_back: How many months of cohorts to analyze
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with cohort LTV data and summary statistics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_cohorts AS (
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            ),
            customer_revenue AS (
                SELECT
                    o.buyer_id,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since,
                    SUM(o.grand_total) AS revenue
                FROM silver_orders o
                JOIN customer_cohorts c ON o.buyer_id = c.buyer_id
                WHERE NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id, c.cohort_month, DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date))
            ),
            cohort_monthly AS (
                SELECT
                    cohort_month,
                    months_since,
                    SUM(revenue) AS total_revenue,
                    COUNT(DISTINCT buyer_id) AS active_customers
                FROM customer_revenue
                WHERE months_since <= ?
                GROUP BY cohort_month, months_since
            ),
            cohort_sizes AS (
                SELECT cohort_month, COUNT(DISTINCT buyer_id) AS cohort_size
                FROM customer_cohorts
                GROUP BY cohort_month
            )
            SELECT
                strftime(cm.cohort_month, '%Y-%m') AS cohort,
                cs.cohort_size,
                cm.months_since,
                cm.total_revenue,
                cm.active_customers
            FROM cohort_monthly cm
            JOIN cohort_sizes cs ON cm.cohort_month = cs.cohort_month
            WHERE cm.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{int(months_back)} months'
            ORDER BY cm.cohort_month DESC, cm.months_since
            """

            rows = conn.execute(query, [retention_months]).fetchall()

            # Build cohort LTV structure with cumulative revenue
            cohorts = {}
            for cohort, size, months_since, revenue, active in rows:
                if cohort not in cohorts:
                    cohorts[cohort] = {
                        "size": size,
                        "monthly_revenue": {},
                        "cumulative": []
                    }
                cohorts[cohort]["monthly_revenue"][months_since] = revenue or 0

            # Calculate cumulative revenue for each cohort
            for cohort_data in cohorts.values():
                cumulative = 0
                cumulative_list = []
                for m in range(retention_months + 1):  # M0 to Mn
                    cumulative += cohort_data["monthly_revenue"].get(m, 0)
                    cumulative_list.append(round(cumulative, 2))
                cohort_data["cumulative"] = cumulative_list

            # Calculate weighted average LTV (weight = cohort size)
            total_rev = sum(c["cumulative"][-1] for c in cohorts.values())
            total_size = sum(c["size"] for c in cohorts.values())
            avg_ltv = round(total_rev / total_size, 2) if total_size > 0 else 0

            # Find best cohort
            best_cohort = max(
                cohorts.items(),
                key=lambda x: x[1]["cumulative"][-1] / x[1]["size"] if x[1]["size"] > 0 else 0,
                default=(None, {"cumulative": [0], "size": 1})
            )

            return {
                "cohorts": [
                    {
                        "month": cohort,
                        "customerCount": data["size"],
                        "cumulativeRevenue": data["cumulative"],
                        "avgLTV": round(data["cumulative"][-1] / data["size"], 2) if data["size"] > 0 else 0
                    }
                    for cohort, data in sorted(cohorts.items(), reverse=True)
                ],
                "summary": {
                    "avgLTV": avg_ltv,
                    "bestCohort": best_cohort[0],
                    "bestCohortLTV": round(best_cohort[1]["cumulative"][-1] / best_cohort[1]["size"], 2) if best_cohort[1]["size"] > 0 else 0
                }
            }

    async def get_at_risk_customers(
        self,
        days_threshold: int = 90,
        months_back: int = 12,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Identify at-risk customers who haven't purchased recently.

        Segments customers by their cohort and identifies those who haven't
        made a purchase in the specified number of days.

        Args:
            days_threshold: Days since last purchase to consider "at risk"
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with at-risk counts by cohort and summary statistics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            churn_threshold = days_threshold * 2

            query = f"""
            WITH customer_activity AS (
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month,
                    MAX(o.order_date) AS last_order_date,
                    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE) AS days_since_last,
                    COUNT(*) AS total_orders,
                    SUM(o.grand_total) AS total_revenue
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            )
            SELECT
                strftime(cohort_month, '%Y-%m') AS cohort,
                COUNT(*) AS total_customers,
                COUNT(*) FILTER (WHERE days_since_last > ? AND days_since_last <= ?) AS at_risk_count,
                ROUND(100.0 * COUNT(*) FILTER (WHERE days_since_last > ?) / COUNT(*), 1) AS at_risk_pct,
                SUM(total_revenue) FILTER (WHERE days_since_last > ?) AS at_risk_revenue,
                AVG(total_orders) FILTER (WHERE days_since_last > ?) AS avg_orders_at_risk,
                COUNT(*) FILTER (WHERE days_since_last > ?) AS churned_count
            FROM customer_activity
            WHERE cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{int(months_back)} months'
            GROUP BY cohort_month
            ORDER BY cohort_month DESC
            """

            rows = conn.execute(query, [
                days_threshold, churn_threshold,  # at_risk_count (between threshold and 2x)
                days_threshold,  # at_risk_pct (> threshold)
                days_threshold,  # at_risk_revenue
                days_threshold,  # avg_orders_at_risk
                churn_threshold,  # churned_count (> 2x threshold)
            ]).fetchall()

            cohorts = []
            total_at_risk = 0
            total_churned = 0
            total_customers = 0
            for cohort, total, at_risk, pct, revenue, avg_orders, churned in rows:
                cohorts.append({
                    "cohort": cohort,
                    "totalCustomers": total,
                    "atRiskCount": at_risk,
                    "atRiskPct": pct,
                    "atRiskRevenue": round(revenue, 2) if revenue else 0,
                    "avgOrdersAtRisk": round(avg_orders, 1) if avg_orders else 0,
                    "churnedCount": churned
                })
                total_at_risk += at_risk
                total_churned += churned
                total_customers += total

            return {
                "cohorts": cohorts,
                "daysThreshold": days_threshold,
                "summary": {
                    "totalAtRisk": total_at_risk,
                    "totalCustomers": total_customers,
                    "overallAtRiskPct": round(100.0 * total_at_risk / total_customers, 1) if total_customers > 0 else 0,
                    "totalChurned": total_churned,
                    "churnPct": round(100.0 * total_churned / total_customers, 1) if total_customers > 0 else 0
                }
            }

    # ─── Saved audiences ─────────────────────────────────────────────────

    # ── one statement, whichever store owns /sms ────────────────────────────
    #
    # The audience query has a shared body because it carries rules. These
    # statements carry almost none, so what they need is not a second dialect
    # but one place that knows which engine is answering and how it spells a
    # placeholder and a table name. The SQL below is written once, with
    # `{table}` holes and `?` markers, exactly as the audience body is.
    #
    # `sms_store_is_postgres()` is read before any connection is taken, §34's
    # invariant: the Postgres path must not queue behind DuckDB's single
    # writer on its way to another engine.

    @asynccontextmanager
    async def _sms_tx(self):
        """Several SMS statements, one transaction, on either engine.

        The freeze writes a campaign and its roster, and a failure between the
        two would leave a campaign whose control group does not exist — which
        looks like a campaign and cannot be measured. DuckDB's `connection()`
        only takes the store lock, so until now that pair was autocommitted one
        statement at a time; both engines get a real transaction here.
        """
        from core.sql_dialect import DUCKDB, POSTGRES

        if sms_store_is_postgres():
            from core.pg import get_pool, require_revision

            pool = await get_pool()
            await require_revision()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    yield _PgTx(conn, POSTGRES)
            return

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                yield _DuckTx(conn, DUCKDB)
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")

    async def _sms_run(self, sql: str, params: Optional[List[Any]] = None,
                       *, mode: str = "all"):
        """Run one SMS statement against the store that owns the tab."""
        from core.sql_dialect import DUCKDB, POSTGRES

        params = list(params or [])
        dialect = POSTGRES if sms_store_is_postgres() else DUCKDB
        rendered = sql.format(
            campaigns=dialect.sms_campaigns,
            members=dialect.sms_campaign_members,
            presets=dialect.sms_audience_presets,
            optouts=dialect.marketing_optouts,
            dlr_events=dialect.sms_dlr_events,
            lines=dialect.order_lines,
            stocks=dialect.offer_stocks,
            buyers=dialect.buyers,
        )

        if sms_store_is_postgres():
            from core import pg_sms_read

            rendered = pg_sms_read.numbered(rendered)
            if mode == "all":
                rows = await pg_sms_read.fetch(rendered, params)
                return [_local_times(r) for r in rows]
            if mode == "one":
                row = await pg_sms_read.fetch_one(rendered, params)
                return _local_times(row) if row is not None else None
            await pg_sms_read.execute(rendered, params)
            return None

        async with self.connection() as conn:
            cursor = conn.execute(rendered, params)
            if mode == "all":
                return cursor.fetchall()
            if mode == "one":
                return cursor.fetchone()
            return None

    async def list_sms_audience_presets(self) -> List[Dict[str, Any]]:
        """Saved audiences, built-in ones first.

        The built-ins live in code rather than in the table so they cannot be
        deleted or edited into something that no longer matches what the page
        describes — the classic RFM cohort is the reference every past campaign
        was built from, and it has to keep meaning the same thing.
        """
        rows = await self._sms_run("""
            SELECT name, criteria, created_by, created_at, updated_at
            FROM {presets}
            ORDER BY name
        """)

        saved = [
            {
                "name": name,
                "criteria": json.loads(criteria) if criteria else {},
                "createdBy": created_by,
                "createdAt": created_at.isoformat() if created_at else None,
                "updatedAt": updated_at.isoformat() if updated_at else None,
                "builtin": False,
            }
            for name, criteria, created_by, created_at, updated_at in rows
        ]
        builtin = [
            {
                "name": name,
                "criteria": dict(criteria),
                "createdBy": None,
                "createdAt": None,
                "updatedAt": None,
                "builtin": True,
            }
            for name, criteria in BUILTIN_AUDIENCE_PRESETS.items()
        ]
        return builtin + saved

    async def save_sms_audience_preset(
        self, name: str, criteria: Dict[str, Any], created_by: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Store or replace a saved audience.

        Raises ValueError on a built-in name: shadowing "RFM tiers" with
        something else would make every conversation about it ambiguous.
        """
        if name in BUILTIN_AUDIENCE_PRESETS:
            raise ValueError(f"{name!r} is a built-in audience and cannot be replaced")

        payload = json.dumps(criteria, ensure_ascii=False, default=str)
        await self._sms_run("""
            -- now(), not CURRENT_TIMESTAMP: inside an upsert DuckDB reads the
            -- bare keyword as a column reference and fails to bind it. Both
            -- engines take now().
            INSERT INTO {presets} (name, criteria, created_by, updated_at)
            VALUES (?, ?, ?, now())
            ON CONFLICT (name) DO UPDATE SET
                criteria = excluded.criteria,
                updated_at = now()
        """, [name, payload, created_by], mode="none")
        return {"name": name, "criteria": criteria, "builtin": False}

    async def delete_sms_audience_preset(self, name: str) -> bool:
        """Remove a saved audience. Returns False if there was none."""
        if name in BUILTIN_AUDIENCE_PRESETS:
            raise ValueError(f"{name!r} is a built-in audience and cannot be deleted")

        row = await self._sms_run(
            "DELETE FROM {presets} WHERE name = ? RETURNING name", [name], mode="one",
        )
        return row is not None

    async def freeze_sms_campaign(
        self,
        campaign: str,
        customers: List[Dict[str, Any]],
        criteria: Dict[str, Any],
        ltv_basis: str,
        sales_type: str,
        holdout_pct: int,
        promocode: Optional[str] = None,
        notes: Optional[str] = None,
        overwrite: bool = False,
    ) -> Dict[str, Any]:
        """
        Record who was in a campaign — target and holdout alike — at export time.

        The eligible population shifts daily, so the same segmentation query run
        after the send returns a different set of people. Freezing the roster is
        what makes the campaign measurable at all: the holdout recorded here is
        the only control group that will exist.

        Re-freezing an existing campaign is refused unless ``overwrite`` is set,
        so a roster cannot be silently rewritten after the file has gone out.

        Args:
            campaign: Campaign slug; the primary key.
            customers: Rows from get_sms_segments (needs include_customers).
            criteria: Threshold snapshot, stored as JSON for the record.
            ltv_basis: Which LTV drove the tiering.
            sales_type: retail / b2b / all.
            holdout_pct: Percent withheld, for the record.
            promocode: Optional code, if the campaign uses direct attribution.
            notes: Free-text note.
            overwrite: Replace an existing roster instead of refusing.

        Returns:
            Dict with the campaign and its per-tier target/holdout counts.

        Raises:
            ValueError: If the campaign exists and overwrite is False, or if
                customers is empty (an empty roster measures nothing).
        """
        if not customers:
            raise ValueError(
                "refusing to freeze an empty roster — nothing could be measured"
            )

        async with self._sms_tx() as tx:
            exists = await tx.one(
                "SELECT sent_at FROM {campaigns} WHERE campaign = ?", [campaign]
            )

            if exists is not None:
                if not overwrite:
                    raise ValueError(
                        f"campaign {campaign!r} is already frozen — pass overwrite "
                        f"to replace it, or use a new campaign name"
                    )
                if exists[0] is not None:
                    raise ValueError(
                        f"campaign {campaign!r} was already sent on {exists[0]} — "
                        f"its roster is the control group and cannot be rewritten"
                    )
                await tx.none(
                    "DELETE FROM {members} WHERE campaign = ?", [campaign])
                await tx.none(
                    "DELETE FROM {campaigns} WHERE campaign = ?", [campaign])

            await tx.none(
                """
                INSERT INTO {campaigns}
                    (campaign, ltv_basis, sales_type, holdout_pct, criteria,
                     promocode, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [campaign, ltv_basis, sales_type, holdout_pct,
                 json.dumps(criteria, ensure_ascii=False), promocode, notes],
            )

            await tx.many(
                """
                INSERT INTO {members}
                    (campaign, buyer_id, phone, tier, assignment, orders_at_export,
                     revenue_ltv_at_export, margin_ltv_at_export, recency_at_export)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    [campaign, c["buyerId"], c["phone"], c["tier"], c["assignment"],
                     c["orders"], c["revenueLtv"], c["marginLtv"], c["recencyDays"]]
                    for c in customers
                ],
            )

            rows = await tx.all(
                """
                SELECT tier,
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE assignment = 'target') AS target,
                       COUNT(*) FILTER (WHERE assignment = 'holdout') AS holdout
                FROM {members}
                WHERE campaign = ?
                GROUP BY tier
                """,
                [campaign],
            )

        tier_order = {SINGLE_GROUP_NAME: 0, "VIP": 0, "CORE": 1, "REACTIVATION": 2}
        segments = [
            {"tier": t, "total": total, "target": target, "holdout": holdout}
            for t, total, target, holdout in
            sorted(rows, key=lambda r: tier_order.get(r[0], 9))
        ]
        return {
            "campaign": campaign,
            "ltvBasis": ltv_basis,
            "promocode": promocode,
            "frozen": True,
            "segments": segments,
            "totals": {
                "customers": sum(s["total"] for s in segments),
                "target": sum(s["target"] for s in segments),
                "holdout": sum(s["holdout"] for s in segments),
            },
        }

    async def mark_sms_campaign_sent(
        self,
        campaign: str,
        sent_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Record when the file actually went to the SMS provider.

        Results are measured from this date, not from the export date — the two
        can differ by days, and attributing purchases to the wrong window is the
        easiest way to manufacture a result that is not there.

        Raises:
            ValueError: If the campaign does not exist.
        """
        row = await self._sms_run(
            "SELECT sent_at FROM {campaigns} WHERE campaign = ?", [campaign],
            mode="one",
        )
        if row is None:
            raise ValueError(f"campaign {campaign!r} is not frozen")

        updated = await self._sms_run(
            "UPDATE {campaigns} SET sent_at = ? WHERE campaign = ? "
            "RETURNING sent_at",
            [sent_at or datetime.now(), campaign], mode="one",
        )
        sent = updated[0] if updated else None

        return {
            "campaign": campaign,
            "sentAt": sent.isoformat() if sent else None,
            "previouslySentAt": row[0].isoformat() if row[0] else None,
        }

    async def get_sms_campaign_targets(self, campaign: str) -> List[Dict[str, Any]]:
        """
        Claim the campaign and hand back the phones to message.

        Stamping ``sent_at`` here, before a single message leaves, is the whole
        point. It used to be written only after the gateway had taken the last
        batch, which left the campaign unclaimed for the entire length of the
        send — and a send of thousands takes minutes. Two requests could both
        read "not sent yet" and both go, which is exactly what happened: a
        roster of 5,550 went out twice because a client-side timeout made the
        operator press send again while the first call was still running.

        The claim is **one conditional UPDATE**, not a read followed by a
        write. It used to be the latter, which was safe only because DuckDB
        admits a single writer and serialised the pair; against a connection
        pool two callers can interleave a read-then-write and both win, which
        is precisely the incident this guard exists for. `WHERE sent_at IS
        NULL` makes the claim atomic on either engine, and the row it returns
        is the proof of who got it.

        Callers that end up sending nothing must hand it back with
        :meth:`release_sms_campaign`, or the campaign is stuck.

        Raises:
            ValueError: If the campaign is unknown or already claimed.
        """
        claimed = await self._sms_run(
            "UPDATE {campaigns} SET sent_at = ? "
            "WHERE campaign = ? AND sent_at IS NULL RETURNING campaign",
            [datetime.now(), campaign], mode="one",
        )
        if claimed is None:
            # Nothing was claimed, and the two reasons need different words.
            camp = await self._sms_run(
                "SELECT sent_at FROM {campaigns} WHERE campaign = ?", [campaign],
                mode="one",
            )
            if camp is None:
                raise ValueError(f"campaign {campaign!r} is not frozen")
            raise ValueError(
                f"campaign {campaign!r} was already sent on {camp[0]} — "
                f"sending twice would double-message the roster"
            )

        rows = await self._sms_run(
            """
            SELECT buyer_id, phone, tier
            FROM {members}
            WHERE campaign = ? AND assignment = 'target'
            ORDER BY buyer_id
            """,
            [campaign],
        )

        return [{"buyerId": r[0], "phone": r[1], "tier": r[2]} for r in rows]

    async def release_sms_campaign(self, campaign: str) -> None:
        """
        Hand a claimed campaign back, for when nothing was actually sent.

        Only safe when no message left: clearing the stamp makes the campaign
        sendable again, which is a double-send if anything did go out.
        """
        await self._sms_run(
            "UPDATE {campaigns} SET sent_at = NULL WHERE campaign = ?",
            [campaign], mode="none",
        )

    async def record_sms_send(
        self,
        campaign: str,
        accepted: Dict[int, str],
        stoplisted: List[int],
        failed: Dict[int, str],
        sent_at: Optional[datetime] = None,
        message_text: Optional[str] = None,
        message_parts: Optional[int] = None,
        price_per_part: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Store the gateway's answer and stamp the campaign as sent.

        ``accepted`` maps buyer_id to the gateway's message id, ``stoplisted``
        lists buyers the gateway refused because they opted out, and ``failed``
        maps buyer_id to a status string for everything else.

        Stoplisted buyers are also written to marketing_optouts: the provider
        already refuses to deliver to them, and without a record of our own they
        would be re-selected by every future export.

        ``message_text``, ``message_parts`` and ``price_per_part`` record what
        the send cost. The total is ``accepted × parts × price``, computed from
        what the gateway actually took rather than from the roster — a send that
        dies partway bills for what left, not for what was planned. Passing them
        is optional so a caller that only wants the roster stamped still can,
        but without them the results page cannot say whether the campaign paid
        for itself.
        """
        async with self._sms_tx() as tx:
            # Batched rather than one statement per recipient: a send is up to
            # 5,000 people, and against a connection pool that was 5,000 round
            # trips where DuckDB paid none.
            if accepted:
                await tx.many(
                    """
                    UPDATE {members}
                    SET message_id = ?, delivery_status = 'Accepted'
                    WHERE campaign = ? AND buyer_id = ?
                    """,
                    [[message_id, campaign, buyer_id]
                     for buyer_id, message_id in accepted.items()],
                )

            if failed:
                await tx.many(
                    """
                    UPDATE {members}
                    SET delivery_status = ?, delivered = FALSE
                    WHERE campaign = ? AND buyer_id = ?
                    """,
                    [[status, campaign, buyer_id]
                     for buyer_id, status in failed.items()],
                )

            if stoplisted:
                await tx.many(
                    """
                    UPDATE {members}
                    SET delivery_status = 'Stoplist', delivered = FALSE
                    WHERE campaign = ? AND buyer_id = ?
                    """,
                    [[campaign, buyer_id] for buyer_id in stoplisted],
                )
                await tx.many(
                    """
                    INSERT INTO {optouts}
                        (buyer_id, channel, phone, reason, source)
                    SELECT ?, 'sms', phone, 'stoplist', 'turbosms'
                    FROM {members}
                    WHERE campaign = ? AND buyer_id = ?
                    -- The gateway is where a phone-less manual opt-out finally
                    -- gets its number; see add_marketing_optout.
                    ON CONFLICT (buyer_id, channel) DO UPDATE
                        SET phone = COALESCE(EXCLUDED.phone, {optouts}.phone)
                    """,
                    [[buyer_id, campaign, buyer_id] for buyer_id in stoplisted],
                )

            # Whoever the gateway never answered for was never messaged. A send
            # that dies partway leaves them behind, and they must not sit in the
            # target arm unable to respond to a message they never got.
            #
            # The results already exclude this status — but nothing wrote it.
            # It existed only because a roster was repaired by hand after the
            # fact, which meant the exclusion was dead code resting on a manual
            # step. A member with neither a message id nor a status is exactly
            # the one nobody heard about.
            not_sent = await tx.all(
                """
                UPDATE {members}
                SET delivery_status = 'NotSent', delivered = FALSE
                WHERE campaign = ? AND assignment = 'target'
                  AND message_id IS NULL AND delivery_status IS NULL
                RETURNING buyer_id
                """,
                [campaign],
            )

            # The cost is recorded here, from what actually left, rather than
            # estimated later from the roster: only the gateway knows how many
            # it accepted, and only this moment knows the tariff in force.
            cost_total = (
                round(len(accepted) * message_parts * price_per_part, 2)
                if message_parts and price_per_part is not None
                else None
            )
            await tx.none(
                """
                UPDATE {campaigns}
                SET sent_at = ?, message_text = ?, message_parts = ?,
                    recipients_sent = ?, price_per_part = ?, cost_total = ?
                WHERE campaign = ?
                """,
                [sent_at or datetime.now(), message_text, message_parts,
                 len(accepted), price_per_part, cost_total, campaign],
            )

        return {
            "campaign": campaign,
            "accepted": len(accepted),
            "stoplisted": len(stoplisted),
            "failed": len(failed),
            "notSent": len(not_sent),
        }

    async def record_sms_delivery(
        self,
        message_id: str,
        status: str,
        delivered: Optional[bool],
        delivered_at: Optional[datetime] = None,
        *,
        event_id: str,
    ) -> bool:
        """
        Apply one delivery report. Returns False if the message id is unknown.

        ``delivered=None`` means the operator has not reported a final state
        yet, so the flag is left untouched rather than guessed at.

        Delivery is terminal: a report that would flip a message already
        reported ``delivered=True`` to a failure, or reopen it with a non-final
        status, is dropped. That keeps the campaign's ground truth intact
        against a reordered or duplicated gateway callback. A ``True`` report
        always applies, so the Sent -> DELIVRD upgrade and the gateway's
        idempotent retries are untouched. The row still exists either way, so
        the caller still learns the id is known and can acknowledge the
        callback.

        ``event_id`` is the gateway event this report arrived under, and it is
        the reason this is not a free write. The webhook signature covers the
        event id and nothing else, so the caller proves it knew the secret and
        proves nothing about which message it is reporting on. The first report
        under an event id binds that id to its message; a later one naming a
        different message is a captured pair re-pointed at somebody else and
        raises ``DlrEventRebound``. Keyword-only and required so it cannot be
        left off by accident — a binding a caller can silently skip is not one.

        The **status** is deliberately not bound. A gateway that re-renders
        current state when it retries would then contradict its own first
        attempt, and rejecting a legitimate retry is worse than what this
        defends against: the gateway tries nine times over 4.5 hours and offers
        no replay. The terminal-delivery rule above is what guards the status.
        """
        async with self._sms_tx() as tx:
            # The binding and the write share one transaction. They always
            # should have: an event id spent without its write, or a write
            # without its id spent, both leave the pair re-pointable — and the
            # pair never expires, because the scheme carries no nonce.
            bound = await tx.one(
                "SELECT message_id FROM {dlr_events} WHERE event_id = ?",
                [event_id],
            )
            if bound is None:
                # Bound even for a message id the roster does not know: an event
                # spent against nothing must still be spent, or the pair stays
                # re-pointable.
                await tx.none(
                    "INSERT INTO {dlr_events} (event_id, message_id) VALUES (?, ?)"
                    " ON CONFLICT DO NOTHING",
                    [event_id, message_id],
                )
            elif bound[0] != message_id:
                raise DlrEventRebound(event_id, bound[0], message_id)

            if delivered is True:
                await tx.none(
                    """
                    UPDATE {members}
                    SET delivery_status = ?, delivered = ?, delivered_at = ?
                    WHERE message_id = ?
                    """,
                    [status, True, delivered_at or datetime.now(), message_id],
                )
            elif delivered is False:
                await tx.none(
                    """
                    UPDATE {members}
                    SET delivery_status = ?, delivered = ?, delivered_at = ?
                    WHERE message_id = ?
                      AND (delivered IS NULL OR delivered = FALSE)
                    """,
                    [status, False, delivered_at or datetime.now(), message_id],
                )
            else:
                await tx.none(
                    """
                    UPDATE {members} SET delivery_status = ?
                    WHERE message_id = ?
                      AND (delivered IS NULL OR delivered = FALSE)
                    """,
                    [status, message_id],
                )

            found = (await tx.one(
                "SELECT COUNT(*) FROM {members} WHERE message_id = ?",
                [message_id],
            ))[0]

        return bool(found)

    async def add_marketing_optout(
        self,
        buyer_id: int,
        phone: Optional[str] = None,
        reason: str = "manual",
        source: str = "dashboard",
        channel: str = "sms",
    ) -> Dict[str, Any]:
        """Record that a customer asked not to receive marketing on this channel.

        A repeat only ever *adds* the phone, and only when the stored row has
        none. The refusal usually arrives before the number does — opted out by
        buyer id off the CRM screen, with the number turning up on the next
        send's stoplist — and until it lands the phone column suppresses
        nothing, so the same person under a second buyer record keeps being
        selected. Overwriting a phone already there is the opposite mistake: it
        would move one person's suppression onto another's number, on a write
        with no undo. Everything else is left as first written, because the
        first refusal is the fact and its `source` is who to ask about it.
        """
        await self._sms_run(
            """
            INSERT INTO {optouts} (buyer_id, channel, phone, reason, source)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (buyer_id, channel) DO UPDATE
                SET phone = COALESCE(EXCLUDED.phone, {optouts}.phone)
            """,
            [buyer_id, channel, phone, reason, source], mode="none",
        )
        total = (await self._sms_run(
            "SELECT COUNT(*) FROM {optouts} WHERE channel = ?", [channel], mode="one",
        ))[0]

        return {"buyerId": buyer_id, "channel": channel, "totalOptouts": total}

    async def get_sms_campaign_results(
        self,
        campaign: str,
        window_days: int = 30,
        delivered_only: bool = False,
    ) -> Dict[str, Any]:
        """
        Measure a campaign: what the messaged group did versus the control.

        By default every roster member counts, delivered or not. That is the
        intention-to-treat reading, and it is the one that is actually a clean
        randomised comparison: it answers "what did running this campaign do".

        ``delivered_only`` drops from the target arm the recipients the gateway
        actively refused. It is tempting — a refused number cannot have
        responded, so including them drags the lift down — but it is NOT an
        equivalent comparison. Unreachable customers are removed from the
        target arm while their counterparts stay in the control arm, and if
        unreachable people buy at a different rate the difference is biased.
        Read it as an optimistic bound, not as the result.

        It deliberately keeps recipients with no delivery report at all. A
        receipt that never arrived is not a failed delivery, and since the
        provider's webhook does not reach us most receipts never arrive.

        Either way the target group's delivery counts come back, so the size of
        the problem is visible instead of implied.

        Counts purchases in the ``window_days`` after the send, for the frozen
        roster only, and compares target against holdout per tier. The raw
        conversion of the target group on its own is not a result — most of it
        would have happened anyway. The difference is the result.

        Args:
            campaign: Campaign slug.
            window_days: Days after the send to attribute purchases to.

        Returns:
            Per-tier and overall comparison, plus promo-code counts when the
            campaign carried one.

        Raises:
            ValueError: If the campaign is unknown or has no send date — an
                unsent campaign has no window to measure over.
        """
        camp = await self._sms_run(
            "SELECT sent_at, promocode, ltv_basis, holdout_pct, cost_total"
            " FROM {campaigns} WHERE campaign = ?", [campaign], mode="one",
        )
        if camp is None:
            raise ValueError(f"campaign {campaign!r} is not frozen")
        sent_at, promocode, ltv_basis, holdout_pct, cost_total = camp
        if sent_at is None:
            raise ValueError(
                f"campaign {campaign!r} has no send date — mark it sent before "
                f"measuring, or results would cover an arbitrary window"
            )

        rows = await self._sms_run(
            f"""
            WITH linked AS (
                -- Every customer record that is the same person as a roster
                -- member, matched on the phone the message went to.
                --
                -- Responding to the campaign is itself a way to acquire a
                -- second customer record: the recipient follows the link,
                -- checks out on the storefront, and a fresh buyer row is
                -- created because the name is spelled differently
                -- ("Наталія Дяків" against "Дяків Наталія"). Matching the
                -- purchase back by buyer_id then misses it. On the first
                -- real campaign that hid 6 of 55 responses, ~27 700 UAH,
                -- every one of them in the target arm — the arm is the only
                -- one holding a link to click, so the loss is one-sided and
                -- always understates the campaign.
                --
                -- Last nine digits, because the roster stores 380XXXXXXXXX
                -- and buyers may carry a +, spaces or brackets.
                SELECT m.buyer_id AS member_id, b.id AS buyer_id
                FROM {{members}} m
                JOIN {{buyers}} b
                  ON right(regexp_replace(b.phone, '[^0-9]', '', 'g'), 9)
                   = right(m.phone, 9)
                WHERE m.campaign = ?
                UNION  -- the member's own row, even with no usable phone
                SELECT buyer_id, buyer_id
                FROM {{members}} WHERE campaign = ?
            ),
            window_orders AS (
                SELECT l.buyer_id, l.order_id, l.order_grand_total AS grand_total,
                       l.promocode,
                       l.line_amount AS line_revenue,
                       CASE WHEN os.purchased_price > 0
                            THEN os.purchased_price * l.quantity END AS line_cogs
                FROM {{lines}} l
                LEFT JOIN {{stocks}} os ON os.sku = l.sku
                WHERE NOT l.is_return
                  AND l.is_active_source
                  -- From the moment the message went out, not from midnight
                  -- that day. Rounding the start down to a date credited
                  -- the campaign with every purchase made earlier the same
                  -- day — hours of ordinary trading, split at random
                  -- between the two arms, which on day one is the whole
                  -- reading. Both columns carry a timezone, so this
                  -- compares instants.
                  AND l.ordered_at >= ?
                  -- The parameter is cast explicitly because PostgreSQL cannot
                  -- infer its type from `? + INTERVAL` alone: it reads the sum
                  -- as an interval, which then refuses to compare against a
                  -- timestamp. DuckDB is happy either way, so the cast is the
                  -- portable spelling rather than a concession to one engine.
                  AND l.ordered_at < CAST(? AS TIMESTAMPTZ)
                                     + INTERVAL '{int(window_days)} days'
            ),
            alloc AS (
                SELECT buyer_id, order_id, promocode, line_cogs,
                       COALESCE(grand_total * line_revenue
                           / NULLIF(SUM(line_revenue) OVER (PARTITION BY order_id), 0),
                         0) AS revenue
                FROM window_orders
            ),
            per_buyer AS (
                SELECT buyer_id,
                       COUNT(DISTINCT order_id) AS orders,
                       SUM(revenue) AS revenue,
                       COALESCE(SUM(revenue - line_cogs)
                           FILTER (WHERE line_cogs IS NOT NULL), 0) AS margin,
                       COUNT(DISTINCT CASE WHEN promocode = ? THEN order_id END)
                           AS promo_orders
                FROM alloc
                GROUP BY buyer_id
            ),
            per_member AS (
                -- Roll every linked record up onto the roster member, so a
                -- person counts once however many customer rows they have.
                -- Orders belong to exactly one buyer row and roster phones
                -- are unique, so nothing is double counted here.
                SELECT l.member_id,
                       SUM(pb.orders) AS orders,
                       SUM(pb.revenue) AS revenue,
                       SUM(pb.margin) AS margin,
                       SUM(pb.promo_orders) AS promo_orders
                FROM linked l
                JOIN per_buyer pb ON pb.buyer_id = l.buyer_id
                GROUP BY l.member_id
            )
            -- Members the gateway never took are excluded from the arm
            -- itself. They could not respond to a message they never
            -- received, so counting them as contacts understates the rate
            -- on every reading. They stay visible as not_sent below.
            SELECT m.tier, m.assignment,
                   COUNT(*) FILTER (WHERE m.delivery_status IS DISTINCT FROM 'NotSent') AS contacts,
                   COUNT(pb.member_id) FILTER (WHERE m.delivery_status IS DISTINCT FROM 'NotSent') AS converted,
                   COALESCE(SUM(pb.orders) FILTER (WHERE m.delivery_status IS DISTINCT FROM 'NotSent'), 0) AS orders,
                   COALESCE(SUM(pb.revenue) FILTER (WHERE m.delivery_status IS DISTINCT FROM 'NotSent'), 0) AS revenue,
                   COALESCE(SUM(pb.margin) FILTER (WHERE m.delivery_status IS DISTINCT FROM 'NotSent'), 0) AS margin,
                   COALESCE(SUM(pb.promo_orders) FILTER (WHERE m.delivery_status IS DISTINCT FROM 'NotSent'), 0)
                       AS promo_orders,
                   COUNT(*) FILTER (WHERE m.delivered) AS delivered,
                   COUNT(*) FILTER (WHERE m.delivered = FALSE) AS undelivered,
                   -- Never handed to the gateway at all. Counted apart from
                   -- undelivered because it is a different failure: these
                   -- people were never treated, so leaving them in the
                   -- target arm dilutes whatever the message did.
                   COUNT(*) FILTER (WHERE m.delivery_status = 'NotSent')
                       AS not_sent
            FROM {{members}} m
            LEFT JOIN per_member pb ON pb.member_id = m.buyer_id
            WHERE m.campaign = ?
              -- The control arm was never sent to, so a delivery filter
              -- must not touch it, or the comparison loses its baseline.
              --
              -- IS NOT FALSE, not a bare truth test: delivered is NULL for
              -- everyone the gateway accepted but never reported on, and
              -- with the delivery webhook not reaching us those receipts
              -- never arrive. On the first campaign that was 246 people
              -- against 25 actual rejections — treating "in transit" as
              -- "undelivered" drops nine responders for every one refusal.
              {"AND (m.assignment = 'holdout' OR m.delivered IS NOT FALSE)"
               if delivered_only else ""}
            GROUP BY m.tier, m.assignment
            """,
            [campaign, campaign, sent_at, sent_at, promocode, campaign],
        )

        by_tier: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for (tier, assignment, contacts, converted, orders, revenue, margin,
             promo, delivered, undelivered, not_sent) in rows:
            by_tier.setdefault(tier, {})[assignment] = {
                "contacts": contacts,
                "converted": converted,
                "orders": orders,
                "revenue": float(revenue or 0),
                "margin": float(margin or 0),
                "promoOrders": promo,
                "delivered": delivered,
                "undelivered": undelivered,
                "notSent": not_sent,
            }

        empty = {"contacts": 0, "converted": 0, "orders": 0,
                 "revenue": 0.0, "margin": 0.0, "promoOrders": 0,
                 "delivered": 0, "undelivered": 0, "notSent": 0}

        def _blank() -> Dict[str, Any]:
            return dict(empty)

        tier_order = {SINGLE_GROUP_NAME: 0, "VIP": 0, "CORE": 1, "REACTIVATION": 2}
        segments = []
        overall_t, overall_h = _blank(), _blank()

        for tier in sorted(by_tier, key=lambda t: tier_order.get(t, 9)):
            t = by_tier[tier].get("target", _blank())
            h = by_tier[tier].get("holdout", _blank())
            for acc, src in ((overall_t, t), (overall_h, h)):
                for k in empty:
                    acc[k] += src[k]
            segments.append({
                "tier": tier,
                "target": t,
                "holdout": h,
                "comparison": _compare_groups(t, h),
            })

        return {
            "campaign": campaign,
            "sentAt": sent_at.isoformat(),
            "windowDays": window_days,
            "deliveredOnly": delivered_only,
            "ltvBasis": ltv_basis,
            "holdoutPct": holdout_pct,
            "promocode": promocode,
            # What the send cost, so the one question the whole page exists to
            # answer — did this pay for itself — can be asked on the page rather
            # than against a provider invoice. None for campaigns sent before
            # the cost was recorded, and for those the page must not guess.
            "costTotal": float(cost_total) if cost_total is not None else None,
            "segments": segments,
            "overall": {
                "target": overall_t,
                "holdout": overall_h,
                "comparison": _compare_groups(overall_t, overall_h),
            },
        }

    async def backfill_sms_campaign_record(
        self,
        campaign: str,
        message_text: str,
        message_parts: int,
        recipients_sent: int,
        price_per_part: float,
        cost_total: float,
        notes: str,
    ) -> bool:
        """Fill in what a campaign sent, for one that predates the recording.

        Writes **only columns that are still NULL**, and returns False if the
        campaign already carries a record. The text of a sent campaign is
        evidence of what reached people's phones; a path that can overwrite it
        is a path by which every card stops being trustworthy.

        `notes` is where the provenance goes — restored by hand is not the same
        fact as recorded at send, and the page says which one it is looking at.
        """
        row = await self._sms_run("""
            UPDATE {campaigns}
            SET message_text = ?, message_parts = ?, recipients_sent = ?,
                price_per_part = ?, cost_total = ?, notes = ?
            WHERE campaign = ? AND message_text IS NULL
            RETURNING campaign
        """, [message_text, message_parts, recipients_sent, price_per_part,
              cost_total, notes, campaign], mode="one")
        return row is not None

    async def list_sms_campaigns(self) -> List[Dict[str, Any]]:
        """List frozen campaigns, newest export first.

        Carries what a campaign *was*, not only how big it was: the audience as
        it was frozen, the text as it went out, and what the gateway billed.
        All three were recorded from the start and none were readable anywhere
        — "which text went out in August, and to whom" had no answer short of a
        SQL prompt.
        """
        rows = await self._sms_run(
            """
            SELECT c.campaign, c.ltv_basis, c.sales_type, c.holdout_pct,
                   c.promocode, c.exported_at, c.sent_at, c.notes,
                   COUNT(m.buyer_id) AS members,
                   COUNT(m.buyer_id) FILTER (WHERE m.assignment = 'target') AS target,
                   COUNT(m.buyer_id) FILTER (WHERE m.assignment = 'holdout') AS holdout,
                   c.criteria, c.message_text, c.message_parts,
                   c.recipients_sent, c.price_per_part, c.cost_total,
                   COUNT(m.buyer_id) FILTER (WHERE m.delivered) AS delivered,
                   COUNT(m.buyer_id) FILTER (WHERE m.delivered IS FALSE)
                       AS undelivered
            FROM {campaigns} c
            LEFT JOIN {members} m ON m.campaign = c.campaign
            -- Spelled out rather than `GROUP BY ALL`, which only DuckDB has.
            GROUP BY c.campaign, c.ltv_basis, c.sales_type, c.holdout_pct,
                     c.promocode, c.exported_at, c.sent_at, c.notes,
                     c.criteria, c.message_text, c.message_parts,
                     c.recipients_sent, c.price_per_part, c.cost_total
            ORDER BY c.exported_at DESC
            """
        )

        return [
            {
                "campaign": r[0],
                "ltvBasis": r[1],
                "salesType": r[2],
                "holdoutPct": r[3],
                "promocode": r[4],
                "exportedAt": r[5].isoformat() if r[5] else None,
                "sentAt": r[6].isoformat() if r[6] else None,
                "notes": r[7],
                "members": r[8],
                "target": r[9],
                "holdout": r[10],
                # The audience as frozen. Stored as JSON because it snapshots a
                # form, not a schema; handed back as an object so the page can
                # say it in words.
                "criteria": _load_json(r[11]),
                "messageText": r[12],
                "messageParts": r[13],
                "recipientsSent": r[14],
                "pricePerPart": float(r[15]) if r[15] is not None else None,
                "costTotal": float(r[16]) if r[16] is not None else None,
                "delivered": r[17],
                "undelivered": r[18],
            }
            for r in rows
        ]

    async def get_sms_segments(
        self,
        max_recency_days: int = 270,
        vip_ltv: Optional[float] = None,
        core_ltv: Optional[float] = None,
        core_min_orders: int = 2,
        reactivation_max_recency: int = 120,
        sales_type: str = "retail",
        ltv_basis: str = "revenue",
        holdout_pct: int = 10,
        campaign: str = "default",
        tier: Optional[Union[str, Sequence[str]]] = None,
        filters: Optional["SmsAudienceFilters"] = None,
        grouping: str = "rfm",
        include_customers: bool = False,
        limit: int = 20000,
    ) -> Dict[str, Any]:
        """
        Build RFM-based SMS campaign segments with a deterministic holdout group.

        Customers are scored on recency / frequency / lifetime value and assigned
        to exactly one tier (most valuable wins):

        * ``VIP``          - ltv >= vip_ltv. High baseline repeat rate; discounting
          them mostly cannibalises purchases that would have happened anyway.
        * ``CORE``         - orders >= core_min_orders OR ltv >= core_ltv.
        * ``REACTIVATION`` - single-order buyers still inside
          ``reactivation_max_recency`` days. Lowest baseline, highest headroom.

        Everyone outside ``max_recency_days``, without a usable phone number, or
        a single-order buyer past ``reactivation_max_recency`` is dropped.

        ``ltv_basis`` picks which lifetime value drives the tiering:

        * ``revenue`` - lifetime revenue.
        * ``margin``  - lifetime contribution margin (revenue minus COGS from
          ``offer_stocks.purchased_price``). Preferred when margin varies by
          brand, since revenue ranking otherwise steers budget towards
          low-margin customers.

        Both figures are always returned, so the two bases can be compared on
        the same people. Margin can be negative (goods sold below cost) and is
        deliberately not clipped. Where a SKU has no cost, that line is left out
        of the margin but still counted in revenue — ``costCoverage`` reports
        the costed share so under-scored customers are visible.

        ``holdout_pct`` of each tier is marked ``holdout`` instead of ``target``
        so campaign uplift can be measured. The split is a hash of
        (buyer_id, campaign): stable across reruns of the same campaign, and
        re-drawn when ``campaign`` changes so the same people are not always
        withheld.

        Args:
            max_recency_days: Drop customers whose last order is older than this.
            vip_ltv: VIP cut-off; defaults per basis (see SMS_TIER_DEFAULTS).
            core_ltv: CORE cut-off without repeat orders; defaults per basis.
            core_min_orders: Order count that qualifies for CORE.
            reactivation_max_recency: Recency cap for single-order buyers.
            sales_type: retail / b2b / all.
            ltv_basis: revenue or margin — which LTV drives tier assignment.
            holdout_pct: Percent of each tier withheld as control (0 disables).
            campaign: Campaign label; also seeds the holdout split.
            tier: Restrict the audience to these value levels (VIP / CORE /
                REACTIVATION). A filter on who is messaged, independent of how
                the result is measured — it applies under either grouping.
            filters: Extra audience predicates (recency, order count, LTV,
                average order, first purchase, city, brand, category, source,
                promocode). None or an empty set selects what the tier rules
                alone select.
            grouping: "rfm" for the three value tiers, "single" for one arm
                holding everyone the filters kept. A filtered audience usually
                wants "single": under "rfm" anyone outside all three tiers is
                dropped, which quietly removes most one-order buyers.
            include_customers: Include the customer rows, not just the summary.
            limit: Max customer rows returned when include_customers is set.

        Returns:
            Dict with per-tier summary, totals, the selection funnel (how many
            customers each rule left standing) and (optionally) customer rows.

        Raises:
            ValueError: If ltv_basis is not one of SMS_LTV_BASES.
        """
        if ltv_basis not in SMS_LTV_BASES:
            raise ValueError(
                f"ltv_basis must be one of {', '.join(SMS_LTV_BASES)}, got {ltv_basis!r}"
            )
        if grouping not in SMS_GROUPINGS:
            raise ValueError(
                f"grouping must be one of {', '.join(SMS_GROUPINGS)}, got {grouping!r}"
            )
        filters = filters or SmsAudienceFilters()

        defaults = SMS_TIER_DEFAULTS[ltv_basis]
        if vip_ltv is None:
            vip_ltv = defaults["vip"]
        if core_ltv is None:
            core_ltv = defaults["core"]

        # One tier or several: a discount aimed at Core and Reactivation but
        # not VIP is a single campaign with a single text, so the filter has to
        # take a set rather than forcing two rosters that must be reconciled.
        if isinstance(tier, str):
            tier = [tier]
        tiers = [t.upper() for t in tier] if tier else None

        # Silver already classifies each order, so filter on the column
        # rather than re-deriving retail/b2b from manager_id here.
        sales_type_filter = "" if sales_type == "all" else "AND l.sales_type = ?"

        # Phones are stored as free text; normalise to digits and keep only
        # full Ukrainian MSISDNs (380 + 9 digits). Everything shorter is a
        # partial record that no SMS gateway will accept.
        # Which lifetime value drives tiering. Both are always computed.
        ltv_column = "revenue_ltv" if ltv_basis == "revenue" else "margin_ltv"

        # The value level is always computed, because it does two separate
        # jobs and only one of them is splitting. As a *filter* — "send to
        # VIP only" — it applies whether or not the campaign is measured in
        # arms; tying it to the split is what made three tier cards read as
        # three audiences.
        tier_case = f"""CASE
                    WHEN c.{ltv_column} >= ? THEN 'VIP'
                    WHEN c.orders >= ? OR c.{ltv_column} >= ? THEN 'CORE'
                    WHEN c.recency <= ? THEN 'REACTIVATION'
                END"""

        # The arm is what the result is measured on. Under "single" there
        # is one, and nobody is dropped for belonging to no level; under
        # "rfm" the arm is the level, and whoever has none falls out.
        if grouping == "single":
            arm_expr = f"'{SINGLE_GROUP_NAME}'"
            ok_tier_expr = "TRUE"
        else:
            arm_expr = "tier_level"
            ok_tier_expr = "tier_level IS NOT NULL"

        # Which store answers. Decided out here on purpose: the Postgres path
        # must not take DuckDB's connection at all — §34's invariant, because a
        # read that queues behind the warehouse rebuild has moved the
        # bottleneck rather than left it behind.
        use_postgres = sms_store_is_postgres()
        dialect = POSTGRES if use_postgres else DUCKDB

        filter_sql, filter_params = filters.predicate(
            ltv_column, sales_type, dialect.order_lines,
        )
        fragments = dict(
            ltv_column=ltv_column,
            sales_type_filter=sales_type_filter,
            tier_case=tier_case,
            arm_expr=arm_expr,
            ok_tier_expr=ok_tier_expr,
            filter_sql=filter_sql,
            tier_subset=(
                f"WHERE tier_level IN ({', '.join('?' * len(tiers))})"
                if tiers else ""
            ),
        )

        # Bound in textual order of the `?` placeholders: the line-items
        # filter, the level CASE, the recency window, the audience predicate,
        # then the level subset. The holdout split is not among them — it is
        # `core.sms_holdout`, in Python, so that this store and Postgres cannot
        # disagree about who was withheld.
        params: list = []
        if sales_type != "all":
            params.append(sales_type)
        params += [vip_ltv, core_min_orders, core_ltv, reactivation_max_recency]
        params.append(max_recency_days)
        params += filter_params
        if tiers:
            params += tiers

        if use_postgres:
            from core.pg_sms_read import fetch_segments, render
            rows = await fetch_segments(render(**fragments), params)
        else:
            # One body, two engines — `core/sql_dialect.py`.
            query = sms_segments_select(DUCKDB, **fragments)
            async with self.connection() as conn:
                rows = conn.execute(query, params).fetchall()

        tiers: Dict[str, Dict[str, Any]] = {}
        customers = []
        funnel_counts = (0, 0, 0, 0, 0, 0, 0)
        for (buyer_id, full_name, phone, city, row_tier, orders, ltv, aov,
             revenue_ltv, margin_ltv, margin_pct, cost_coverage,
             recency, last_order, first_order,
             last_order_id, last_order_total, last_order_item_count, last_order_items,
             f_customers, f_in_window, f_filtered, f_tiered, f_phone, f_subscribed,
             f_eligible) in rows:
            funnel_counts = (f_customers, f_in_window, f_filtered, f_tiered,
                             f_phone, f_subscribed, f_eligible)
            # The funnel row survives the RIGHT JOIN even when no customer does.
            if buyer_id is None:
                continue

            assignment = assign_arm(buyer_id, campaign, holdout_pct)

            stats = tiers.setdefault(row_tier, {
                "tier": row_tier, "total": 0, "target": 0, "holdout": 0,
                "ltv": 0.0, "revenue": 0.0, "margin": 0.0,
                "_recency_sum": 0, "_orders_sum": 0,
            })
            stats["total"] += 1
            stats[assignment] += 1
            stats["ltv"] += float(ltv or 0)
            stats["revenue"] += float(revenue_ltv or 0)
            stats["margin"] += float(margin_ltv or 0)
            stats["_recency_sum"] += recency
            stats["_orders_sum"] += orders

            if include_customers and len(customers) < limit:
                customers.append({
                    "buyerId": buyer_id,
                    "fullName": full_name,
                    "phone": phone,
                    "city": city,
                    "tier": row_tier,
                    "orders": orders,
                    "ltv": float(ltv or 0),
                    "avgOrderValue": float(aov or 0),
                    "revenueLtv": float(revenue_ltv or 0),
                    "marginLtv": float(margin_ltv or 0),
                    "marginPct": float(margin_pct) if margin_pct is not None else None,
                    "costCoverage": float(cost_coverage or 0),
                    "recencyDays": recency,
                    "lastOrderDate": last_order.isoformat() if last_order else None,
                    "firstOrderDate": first_order.isoformat() if first_order else None,
                    "lastOrderId": last_order_id,
                    "lastOrderTotal": float(last_order_total or 0),
                    "lastOrderItemCount": last_order_item_count or 0,
                    "lastOrderItems": last_order_items,
                    "assignment": assignment,
                })

        tier_order = {SINGLE_GROUP_NAME: 0, "VIP": 0, "CORE": 1, "REACTIVATION": 2}
        summary = []
        for stats in sorted(tiers.values(), key=lambda s: tier_order.get(s["tier"], 9)):
            total = stats["total"]
            summary.append({
                "tier": stats["tier"],
                "total": total,
                "target": stats["target"],
                "holdout": stats["holdout"],
                "totalLtv": round(stats["ltv"], 2),
                "avgLtv": round(stats["ltv"] / total, 2) if total else 0,
                "totalRevenue": round(stats["revenue"], 2),
                "totalMargin": round(stats["margin"], 2),
                "marginPct": round(100.0 * stats["margin"] / stats["revenue"], 1)
                             if stats["revenue"] else None,
                "avgOrders": round(stats["_orders_sum"] / total, 2) if total else 0,
                "avgRecencyDays": round(stats["_recency_sum"] / total) if total else 0,
            })

        total_customers = sum(s["total"] for s in summary)
        (f_customers, f_in_window, f_filtered, f_tiered, f_phone, f_subscribed,
         f_eligible) = funnel_counts
        return {
            "campaign": campaign,
            "salesType": sales_type,
            "ltvBasis": ltv_basis,
            "grouping": grouping,
            "criteria": {
                "maxRecencyDays": max_recency_days,
                "ltvBasis": ltv_basis,
                "vipLtv": vip_ltv,
                "coreLtv": core_ltv,
                "coreMinOrders": core_min_orders,
                "reactivationMaxRecency": reactivation_max_recency,
                "holdoutPct": holdout_pct,
                "grouping": grouping,
                # Frozen with the campaign. Without it a filtered roster is a
                # list of phone numbers nobody can explain a month later.
                "filters": filters.as_dict(),
            },
            # How the base narrowed, rule by rule, in the order the query
            # applies them. Published because the tier sizes on their own look
            # arbitrary: the drop from every customer to a sendable list is
            # most of the story, and it is invisible in the segment totals.
            "funnel": [
                {"stage": "customers", "remaining": int(f_customers or 0)},
                {"stage": "inWindow", "remaining": int(f_in_window or 0)},
                {"stage": "filtered", "remaining": int(f_filtered or 0)},
                {"stage": "tiered", "remaining": int(f_tiered or 0)},
                {"stage": "phone", "remaining": int(f_phone or 0)},
                {"stage": "subscribed", "remaining": int(f_subscribed or 0)},
                {"stage": "uniquePhone", "remaining": int(f_eligible or 0)},
            ],
            "segments": summary,
            "totals": {
                "customers": total_customers,
                "target": sum(s["target"] for s in summary),
                "holdout": sum(s["holdout"] for s in summary),
            },
            "customers": customers if include_customers else [],
            "truncated": include_customers and total_customers > limit,
        }
