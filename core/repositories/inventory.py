"""DuckDBStore inventory methods."""
from __future__ import annotations

import logging
from datetime import date
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class InventoryMixin:

    async def upsert_offers(self, offers: List[Dict[str, Any]]) -> int:
        """Insert or update offers from KeyCRM API response.

        Offers link offer_id to product_id, enabling proper joins between
        offer_stocks and products tables.

        Args:
            offers: List of offer dicts from KeyCRM API (/offers endpoint)

        Returns:
            Number of offers upserted
        """
        if not offers:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                count = 0
                for offer in offers:
                    conn.execute("""
                        INSERT OR REPLACE INTO offers (id, product_id, sku, synced_at)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    """, [
                        offer.get("id"),
                        offer.get("product_id"),
                        offer.get("sku"),
                    ])
                    count += 1

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} offers to DuckDB")
                return count

            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    async def upsert_stocks(self, stocks: List[Dict[str, Any]]) -> int:
        """Insert or update offer stocks from KeyCRM API response.

        Detects stock changes and records movements for audit trail.

        Args:
            stocks: List of stock dicts from KeyCRM API (offers/stocks endpoint)

        Returns:
            Number of stocks upserted
        """
        if not stocks:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                # Fetch current state for delta detection
                current = {}
                for r in conn.execute("SELECT id, quantity, reserve FROM offer_stocks").fetchall():
                    current[r[0]] = (r[1], r[2])

                # Build offer_id → product_id mapping for denormalization
                product_map = {}
                for r in conn.execute("SELECT id, product_id FROM offers").fetchall():
                    product_map[r[0]] = r[1]

                count = 0
                movements = []
                for stock in stocks:
                    offer_id = stock.get("id")
                    new_qty = stock.get("quantity", 0)
                    new_rsv = stock.get("reserve", 0)
                    old = current.get(offer_id)
                    pid = product_map.get(offer_id)

                    if old is None:
                        # New offer — record initial state if it has stock
                        if new_qty > 0 or new_rsv > 0:
                            movements.append((offer_id, pid, "initial",
                                              0, new_qty, new_qty, 0, new_rsv))
                    elif old[0] != new_qty or old[1] != new_rsv:
                        # Changed — classify by delta direction
                        delta = new_qty - old[0]
                        if delta != 0:
                            mtype = "stock_out" if delta < 0 else "stock_in"
                        else:
                            mtype = "reserve_change"
                        movements.append((offer_id, pid, mtype,
                                          old[0], new_qty, delta, old[1], new_rsv))
                    count += 1

                conn.executemany("""
                    INSERT OR REPLACE INTO offer_stocks
                    (id, sku, price, purchased_price, quantity, reserve, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [
                    (s.get("id"), s.get("sku"), s.get("price"),
                     s.get("purchased_price"), s.get("quantity", 0),
                     s.get("reserve", 0))
                    for s in stocks
                ])

                if movements:
                    conn.executemany("""
                        INSERT INTO stock_movements
                        (offer_id, product_id, movement_type,
                         quantity_before, quantity_after, delta,
                         reserve_before, reserve_after)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, movements)
                    logger.info(f"Recorded {len(movements)} stock movements")

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} offer stocks to DuckDB")
                return count

            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    async def refresh_sku_inventory_status(self) -> int:
        """Refresh Layer 1: sku_inventory_status from source tables.

        Combines data from offer_stocks, offers, products, and orders to create
        a denormalized view of current inventory with last sale dates.

        Returns:
            Number of SKUs in the refreshed table
        """
        async with self.connection() as conn:
            # Calculate last sale date per product (using offer_id from order_products)
            # Then merge with stock data
            # Save first_seen_at before rebuild (self-referencing subquery needs it)
            conn.execute("CREATE TEMP TABLE _tmp_first_seen AS SELECT offer_id, first_seen_at FROM sku_inventory_status")
            conn.execute("DELETE FROM sku_inventory_status")
            conn.execute("""
                INSERT INTO sku_inventory_status
                SELECT
                    os.id as offer_id,
                    COALESCE(o.product_id, 0) as product_id,
                    COALESCE(os.sku, CAST(os.id AS VARCHAR)) as sku,
                    p.name,
                    p.brand,
                    p.category_id,
                    os.quantity,
                    os.reserve,
                    COALESCE(os.price, 0) as price,
                    os.purchased_price,
                    pls.last_sale_date,
                    COALESCE(
                        (SELECT first_seen_at FROM _tmp_first_seen WHERE offer_id = os.id),
                        fod.first_order_date,
                        CURRENT_DATE
                    ) as first_seen_at,
                    CURRENT_TIMESTAMP as updated_at,
                    smo.last_stock_out_date as last_stock_out_at
                FROM offer_stocks os
                LEFT JOIN offers o ON os.id = o.id
                LEFT JOIN products p ON o.product_id = p.id
                LEFT JOIN (
                    SELECT
                        op.product_id,
                        MAX(DATE(ord.ordered_at AT TIME ZONE 'Europe/Kyiv')) as last_sale_date
                    FROM order_products op
                    JOIN orders ord ON op.order_id = ord.id
                    WHERE ord.status_id NOT IN (19, 22, 21, 23)
                    GROUP BY op.product_id
                ) pls ON o.product_id = pls.product_id
                LEFT JOIN (
                    SELECT
                        op2.product_id,
                        MIN(DATE(ord2.ordered_at AT TIME ZONE 'Europe/Kyiv')) as first_order_date
                    FROM order_products op2
                    JOIN orders ord2 ON op2.order_id = ord2.id
                    GROUP BY op2.product_id
                ) fod ON o.product_id = fod.product_id
                LEFT JOIN (
                    SELECT
                        offer_id,
                        MAX(DATE(recorded_at AT TIME ZONE 'Europe/Kyiv')) as last_stock_out_date
                    FROM stock_movements
                    WHERE movement_type = 'stock_out'
                    GROUP BY offer_id
                ) smo ON os.id = smo.offer_id
            """)

            conn.execute("DROP TABLE IF EXISTS _tmp_first_seen")

            count = conn.execute("SELECT COUNT(*) FROM sku_inventory_status").fetchone()[0]
            logger.info(f"Refreshed sku_inventory_status: {count} SKUs")
            return count

    async def record_sku_inventory_snapshot(self) -> bool:
        """Record Layer 2: daily per-SKU snapshot.

        Only records one snapshot per day. Returns True if recorded, False if already exists.
        """
        async with self.connection() as conn:
            today = date.today()

            # Check if already recorded today
            exists = conn.execute(
                "SELECT 1 FROM inventory_sku_history WHERE date = ? LIMIT 1", [today]
            ).fetchone()

            if exists:
                return False

            # Record snapshot from current sku_inventory_status
            conn.execute("""
                INSERT INTO inventory_sku_history (date, offer_id, quantity, reserve, price)
                SELECT
                    CURRENT_DATE,
                    offer_id,
                    quantity,
                    reserve,
                    price
                FROM sku_inventory_status
            """)

            count = conn.execute(
                "SELECT COUNT(*) FROM inventory_sku_history WHERE date = ?", [today]
            ).fetchone()[0]
            logger.info(f"Recorded SKU inventory snapshot: {count} SKUs for {today}")
            return True

    async def get_stock_summary(self, limit: int = 20) -> Dict[str, Any]:
        """Get stock summary for dashboard display.

        Returns:
            Dict with total stats and top items by quantity and low stock alerts
        """
        async with self.connection() as conn:
            # Overall stats
            # Note: available = MAX(0, quantity - reserve) to match KeyCRM display
            stats = conn.execute("""
                SELECT
                    COUNT(*) as total_offers,
                    COUNT(*) FILTER (WHERE quantity > 0) as in_stock_count,
                    COUNT(*) FILTER (WHERE quantity = 0) as out_of_stock_count,
                    COUNT(*) FILTER (WHERE quantity > 0 AND quantity <= 5) as low_stock_count,
                    SUM(GREATEST(0, quantity - reserve)) as available_quantity,
                    SUM(reserve) as total_reserve,
                    SUM(GREATEST(0, quantity - reserve) * price) as available_value_sale,
                    SUM(reserve * price) as reserve_value_sale,
                    SUM(GREATEST(0, quantity - reserve) * COALESCE(purchased_price, 0)) as available_value_cost,
                    SUM(reserve * COALESCE(purchased_price, 0)) as reserve_value_cost
                FROM offer_stocks
            """).fetchone()

            # Top items by quantity (with product names via offers table)
            top_by_qty = conn.execute(f"""
                SELECT os.sku, os.quantity, os.reserve, os.price, p.name
                FROM offer_stocks os
                LEFT JOIN offers o ON os.id = o.id
                LEFT JOIN products p ON o.product_id = p.id
                WHERE os.quantity > 0
                ORDER BY os.quantity DESC
                LIMIT {limit}
            """).fetchall()

            # Low stock items (1-5 units, excluding 0)
            low_stock = conn.execute("""
                SELECT os.sku, os.quantity, os.reserve, os.price, p.name
                FROM offer_stocks os
                LEFT JOIN offers o ON os.id = o.id
                LEFT JOIN products p ON o.product_id = p.id
                WHERE os.quantity > 0 AND os.quantity <= 5
                ORDER BY os.quantity ASC
                LIMIT 20
            """).fetchall()

            # Out of stock items
            out_of_stock = conn.execute("""
                SELECT os.sku, os.price, p.name
                FROM offer_stocks os
                LEFT JOIN offers o ON os.id = o.id
                LEFT JOIN products p ON o.product_id = p.id
                WHERE os.quantity = 0
                ORDER BY os.price DESC
                LIMIT 20
            """).fetchall()

            # Last sync time
            last_sync = conn.execute("""
                SELECT value FROM sync_metadata WHERE key = 'stocks_last_sync'
            """).fetchone()

            # Get average inventory (30 days)
            avg_inv = conn.execute("""
                WITH period_data AS (
                    SELECT
                        total_quantity,
                        total_value,
                        ROW_NUMBER() OVER (ORDER BY date ASC) as rn_asc,
                        ROW_NUMBER() OVER (ORDER BY date DESC) as rn_desc
                    FROM inventory_history
                    WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
                )
                SELECT
                    MAX(CASE WHEN rn_asc = 1 THEN total_quantity END) as beginning_qty,
                    MAX(CASE WHEN rn_asc = 1 THEN total_value END) as beginning_value,
                    MAX(CASE WHEN rn_desc = 1 THEN total_quantity END) as ending_qty,
                    MAX(CASE WHEN rn_desc = 1 THEN total_value END) as ending_value,
                    COUNT(*) as data_points
                FROM period_data
            """).fetchone()

            # Calculate average inventory
            if avg_inv and avg_inv[0] and avg_inv[2]:
                avg_quantity = (avg_inv[0] + avg_inv[2]) / 2
                avg_value = ((avg_inv[1] or 0) + (avg_inv[3] or 0)) / 2
                avg_data_points = avg_inv[4]
            else:
                avg_quantity = stats[4] or 0  # Use current as fallback
                avg_value = float(stats[6] or 0)
                avg_data_points = 0

            return {
                "summary": {
                    "totalOffers": stats[0] or 0,
                    "inStockCount": stats[1] or 0,
                    "outOfStockCount": stats[2] or 0,
                    "lowStockCount": stats[3] or 0,
                    "totalQuantity": stats[4] or 0,
                    "totalReserve": stats[5] or 0,
                    "totalValue": float(stats[6] or 0),  # Sale price
                    "reserveValue": float(stats[7] or 0),  # Sale price
                    "costValue": float(stats[8] or 0),  # Purchase/cost price
                    "reserveCostValue": float(stats[9] or 0),  # Purchase/cost price
                    "averageQuantity": round(avg_quantity),
                    "averageValue": round(avg_value, 2),
                    "avgDataPoints": avg_data_points,
                },
                "topByQuantity": [
                    {"sku": r[0], "quantity": r[1], "reserve": r[2], "price": float(r[3] or 0), "name": r[4]}
                    for r in top_by_qty
                ],
                "lowStock": [
                    {"sku": r[0], "quantity": r[1], "reserve": r[2], "price": float(r[3] or 0), "name": r[4]}
                    for r in low_stock
                ],
                "outOfStock": [
                    {"sku": r[0], "price": float(r[1] or 0), "name": r[2]}
                    for r in out_of_stock
                ],
                "lastSync": last_sync[0] if last_sync else None,
            }

    async def record_inventory_snapshot(self, force: bool = False) -> bool:
        """Record daily inventory snapshot for average calculation.

        Only records one snapshot per day. Returns True if recorded, False if already exists.

        Args:
            force: If True, delete existing snapshot and re-record
        """
        async with self.connection() as conn:
            today = conn.execute("SELECT CURRENT_DATE").fetchone()[0]

            # Check if already recorded today
            exists = conn.execute(
                "SELECT 1 FROM inventory_history WHERE date = ?", [today]
            ).fetchone()

            if exists:
                if force:
                    conn.execute("DELETE FROM inventory_history WHERE date = ?", [today])
                    logger.info(f"Deleted existing inventory snapshot for {today}")
                else:
                    return False

            # Record snapshot (using available quantity = quantity - reserve, same as KeyCRM)
            conn.execute("""
                INSERT INTO inventory_history (date, total_quantity, total_value, total_reserve, sku_count)
                SELECT
                    CURRENT_DATE,
                    COALESCE(SUM(quantity - reserve), 0),
                    COALESCE(SUM((quantity - reserve) * price), 0),
                    COALESCE(SUM(reserve), 0),
                    COUNT(*)
                FROM offer_stocks
            """)
            logger.info(f"Recorded inventory snapshot for {today}")
            return True

    async def get_average_inventory(self, days: int = 30) -> Dict[str, Any]:
        """Calculate average inventory over a period.

        Uses formula: (Beginning Inventory + Ending Inventory) / 2
        Also provides daily average if more data points available.

        Args:
            days: Number of days to look back (default 30)

        Returns:
            Dict with average inventory metrics
        """
        async with self.connection() as conn:
            # Get beginning and ending inventory for the period
            result = conn.execute(f"""
                WITH period_data AS (
                    SELECT
                        date,
                        total_quantity,
                        total_value,
                        ROW_NUMBER() OVER (ORDER BY date ASC) as rn_asc,
                        ROW_NUMBER() OVER (ORDER BY date DESC) as rn_desc
                    FROM inventory_history
                    WHERE date >= CURRENT_DATE - INTERVAL '{int(days)} days'
                )
                SELECT
                    -- Beginning inventory (oldest in period)
                    MAX(CASE WHEN rn_asc = 1 THEN total_quantity END) as beginning_qty,
                    MAX(CASE WHEN rn_asc = 1 THEN total_value END) as beginning_value,
                    MAX(CASE WHEN rn_asc = 1 THEN date END) as beginning_date,
                    -- Ending inventory (most recent)
                    MAX(CASE WHEN rn_desc = 1 THEN total_quantity END) as ending_qty,
                    MAX(CASE WHEN rn_desc = 1 THEN total_value END) as ending_value,
                    MAX(CASE WHEN rn_desc = 1 THEN date END) as ending_date,
                    -- Daily averages
                    AVG(total_quantity) as avg_daily_qty,
                    AVG(total_value) as avg_daily_value,
                    COUNT(*) as data_points
                FROM period_data
            """).fetchone()

            if not result or not result[0]:
                # No historical data, use current snapshot (sale/retail price)
                current = conn.execute("""
                    SELECT
                        COALESCE(SUM(quantity - reserve), 0),
                        COALESCE(SUM((quantity - reserve) * price), 0)
                    FROM offer_stocks
                """).fetchone()

                return {
                    "averageQuantity": current[0] or 0,
                    "averageValue": float(current[1] or 0),
                    "beginningQuantity": None,
                    "endingQuantity": current[0] or 0,
                    "beginningValue": None,
                    "endingValue": float(current[1] or 0),
                    "dataPoints": 0,
                    "periodDays": days,
                    "message": "No historical data yet. Average based on current snapshot.",
                }

            beginning_qty = result[0] or 0
            beginning_value = float(result[1] or 0)
            ending_qty = result[3] or 0
            ending_value = float(result[4] or 0)

            # Calculate averages using (Beginning + Ending) / 2
            avg_qty = (beginning_qty + ending_qty) / 2
            avg_value = (beginning_value + ending_value) / 2

            return {
                "averageQuantity": round(avg_qty),
                "averageValue": round(avg_value, 2),
                "beginningQuantity": beginning_qty,
                "beginningValue": beginning_value,
                "beginningDate": str(result[2]) if result[2] else None,
                "endingQuantity": ending_qty,
                "endingValue": ending_value,
                "endingDate": str(result[5]) if result[5] else None,
                "dailyAverageQuantity": round(float(result[6] or 0)),
                "dailyAverageValue": round(float(result[7] or 0), 2),
                "dataPoints": result[8] or 0,
                "periodDays": days,
            }

    async def get_inventory_trend(
        self,
        days: int = 90,
        granularity: str = "daily"
    ) -> Dict[str, Any]:
        """Get inventory trend over time for charting.

        Args:
            days: Number of days to look back (default 90)
            granularity: 'daily' or 'monthly'

        Returns:
            Dict with labels, values, quantities for trend chart
        """
        async with self.connection() as conn:
            if granularity == "monthly":
                # Monthly aggregation
                result = conn.execute(f"""
                    SELECT
                        DATE_TRUNC('month', date) as period,
                        AVG(total_quantity) as avg_quantity,
                        AVG(total_value) as avg_value,
                        AVG(total_reserve) as avg_reserve,
                        MIN(total_quantity) as min_quantity,
                        MAX(total_quantity) as max_quantity,
                        MIN(total_value) as min_value,
                        MAX(total_value) as max_value,
                        COUNT(*) as data_points
                    FROM inventory_history
                    WHERE date >= CURRENT_DATE - INTERVAL '{int(days)} days'
                    GROUP BY DATE_TRUNC('month', date)
                    ORDER BY period
                """).fetchall()

                labels = [row[0].strftime('%b %Y') for row in result if row[0]]
                quantities = [round(row[1] or 0) for row in result]
                values = [round(float(row[2] or 0), 2) for row in result]
                reserves = [round(row[3] or 0) for row in result]

                return {
                    "labels": labels,
                    "quantity": quantities,
                    "value": values,
                    "reserve": reserves,
                    "granularity": "monthly",
                    "periodDays": days,
                    "dataPoints": len(result),
                }
            else:
                # Daily data
                result = conn.execute(f"""
                    SELECT
                        date,
                        total_quantity,
                        total_value,
                        total_reserve,
                        sku_count
                    FROM inventory_history
                    WHERE date >= CURRENT_DATE - INTERVAL '{int(days)} days'
                    ORDER BY date
                """).fetchall()

                labels = [row[0].strftime('%d %b') for row in result if row[0]]
                quantities = [row[1] or 0 for row in result]
                values = [float(row[2] or 0) for row in result]
                reserves = [row[3] or 0 for row in result]
                sku_counts = [row[4] or 0 for row in result]

                # Calculate changes
                changes = []
                for i, val in enumerate(values):
                    if i == 0:
                        changes.append(0)
                    else:
                        changes.append(round(val - values[i - 1], 2))

                return {
                    "labels": labels,
                    "quantity": quantities,
                    "value": values,
                    "reserve": reserves,
                    "skuCount": sku_counts,
                    "valueChange": changes,
                    "granularity": "daily",
                    "periodDays": days,
                    "dataPoints": len(result),
                    "summary": {
                        "startValue": values[0] if values else 0,
                        "endValue": values[-1] if values else 0,
                        "change": round(values[-1] - values[0], 2) if len(values) > 1 else 0,
                        "changePercent": round((values[-1] - values[0]) / values[0] * 100, 1) if len(values) > 1 and values[0] > 0 else 0,
                        "minValue": min(values) if values else 0,
                        "maxValue": max(values) if values else 0,
                    } if values else None,
                }

    async def get_inventory_summary_v2(self) -> Dict[str, Any]:
        """Get inventory summary using Layer 3 views.

        Returns:
            Dict with summary by status, aging buckets, and category velocity
        """
        async with self.connection() as conn:
            # Summary by status
            summary = conn.execute("SELECT * FROM v_inventory_summary").fetchall()
            summary_dict = {}
            for row in summary:
                status, sku_count, units, value, pct = row
                summary_dict[status] = {
                    "skuCount": sku_count,
                    "quantity": units or 0,
                    "value": float(value or 0),
                    "valuePercent": float(pct or 0),
                }

            # Total
            total = conn.execute("""
                SELECT COUNT(*), SUM(available), SUM(available_value)
                FROM v_sku_status
            """).fetchone()

            # Aging buckets
            aging = conn.execute("SELECT * FROM v_aging_buckets").fetchall()
            aging_buckets = [
                {"bucket": row[0], "skuCount": row[1], "units": row[2], "value": float(row[3] or 0)}
                for row in aging
            ]

            # Category velocity
            velocity = conn.execute("SELECT * FROM v_category_velocity").fetchall()
            category_thresholds = [
                {
                    "categoryId": row[0],
                    "categoryName": row[1] or "Uncategorized",
                    "sampleSize": row[2],
                    "p50": int(row[3]) if row[3] else None,
                    "p75": int(row[4]) if row[4] else None,
                    "p90": int(row[5]) if row[5] else None,
                    "thresholdDays": int(row[6]) if row[6] else 180,
                }
                for row in velocity
            ]

            return {
                "summary": {
                    "healthy": summary_dict.get("healthy", {"skuCount": 0, "quantity": 0, "value": 0, "valuePercent": 0}),
                    "overstocked": summary_dict.get("overstocked", {"skuCount": 0, "quantity": 0, "value": 0, "valuePercent": 0}),
                    "atRisk": summary_dict.get("at_risk", {"skuCount": 0, "quantity": 0, "value": 0, "valuePercent": 0}),
                    "deadStock": summary_dict.get("dead_stock", {"skuCount": 0, "quantity": 0, "value": 0, "valuePercent": 0}),
                    "neverSold": summary_dict.get("never_sold", {"skuCount": 0, "quantity": 0, "value": 0, "valuePercent": 0}),
                    "total": {
                        "skuCount": total[0] or 0,
                        "quantity": total[1] or 0,
                        "value": float(total[2] or 0),
                    },
                },
                "agingBuckets": aging_buckets,
                "categoryThresholds": category_thresholds,
            }

    async def get_dead_stock_items_v2(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get dead stock and at-risk items using Layer 3 views.

        Returns:
            List of items with status != healthy
        """
        async with self.connection() as conn:
            items = conn.execute(f"""
                SELECT
                    offer_id, sku, name, brand, category_name,
                    available, available_value, price,
                    days_since_sale, days_in_stock, threshold_days,
                    days_of_supply, status
                FROM v_sku_status
                WHERE status != 'healthy'
                ORDER BY available_value DESC
                LIMIT {limit}
            """).fetchall()

            return [
                {
                    "id": row[0],
                    "sku": row[1],
                    "name": row[2],
                    "brand": row[3],
                    "categoryName": row[4],
                    "quantity": row[5],
                    "value": float(row[6] or 0),
                    "price": float(row[7] or 0),
                    "daysSinceSale": row[8],
                    "daysInStock": row[9],
                    "thresholdDays": row[10],
                    "daysOfSupply": int(row[11]) if row[11] is not None else None,
                    "status": row[12],
                }
                for row in items
            ]

    async def get_all_skus_deep(
        self,
        carrying_rate: float = 0.25,
        liquidation_discount: float = 0.50,
    ) -> List[Dict[str, Any]]:
        """All SKUs with full NPV/decision computation, sorted by excess capital desc.

        Same per-SKU enrichment as get_dead_stock_deep, but returns only the items
        list (no aggregates). Used by the SKU rotation table — client filters/sorts.
        """
        full = await self.get_dead_stock_deep(
            limit=None,
            carrying_rate=carrying_rate,
            liquidation_discount=liquidation_discount,
        )
        return full["items"]

    async def get_dead_stock_deep(
        self,
        limit: int | None = 100,
        carrying_rate: float = 0.25,
        liquidation_discount: float = 0.50,
    ) -> Dict[str, Any]:
        """Dead-stock analysis with cost basis, GMROI, and NPV-based recommendation.

        Args:
            limit: Max items in the items list (sorted by excess capital cost)
            carrying_rate: Annual carrying cost as fraction of cost basis (default 25%)
            liquidation_discount: Discount fraction for liquidation NPV (default 0.5 = -50%)

        Returns:
            Dict with items, quadrantMatrix, concentration, costQuality, gmroiDistribution
        """
        # Velocity tier ordering — used everywhere we render the matrix
        TIERS = ["hot", "healthy", "warm", "cold", "frozen"]
        ABCS = ["A", "B", "C"]
        OPTIMAL_DAYS = 60  # baseline for excess_capital_cost

        async with self.connection() as conn:
            rows = conn.execute("""
                SELECT
                    offer_id, sku, name, brand, category_name,
                    available, price, purchased_price, effective_unit_cost, cost_quality,
                    sale_value, cost_basis,
                    days_since_sale, days_in_stock, last_sale_date,
                    qty_sold_30d, qty_sold_90d, revenue_30d, revenue_90d,
                    avg_daily_sales_30d, avg_daily_sales_90d,
                    days_of_supply, velocity_tier, velocity_ratio_30_90,
                    abc_class, annual_gross_profit, gmroi
                FROM v_sku_dead_stock_v2
            """).fetchall()

        items_full: List[Dict[str, Any]] = []
        for r in rows:
            (offer_id, sku, name, brand, category_name,
             available, price, purchased_price, effective_unit_cost, cost_quality,
             sale_value, cost_basis,
             days_since_sale, days_in_stock, last_sale_date,
             qty_sold_30d, qty_sold_90d, revenue_30d, revenue_90d,
             avg_daily_sales_30d, avg_daily_sales_90d,
             days_of_supply, velocity_tier, velocity_ratio_30_90,
             abc_class, annual_gross_profit, gmroi) = r

            available = available or 0
            cost_basis = float(cost_basis or 0)
            sale_value = float(sale_value or 0)
            adv90 = float(avg_daily_sales_90d or 0)
            dos = float(days_of_supply) if days_of_supply is not None else None

            # Excess capital cost (above OPTIMAL_DAYS supply)
            excess_units = max(0, available - adv90 * OPTIMAL_DAYS)
            unit_cost = float(effective_unit_cost or 0)
            excess_capital_cost = excess_units * unit_cost

            # NPV: hold for 1 year vs liquidate now at discount
            # Liquidate: sale at (1 - discount), recover cash immediately
            liq_revenue = sale_value * (1 - liquidation_discount)
            liq_npv = liq_revenue - cost_basis

            # Hold: gross profit from natural sell-through over 1 year, minus carrying
            if dos is not None and dos <= 365 and adv90 > 0:
                # All units sell within a year
                hold_gross_profit = float(annual_gross_profit or 0) * (min(dos, 365) / 365.0)
                # Approx avg capital tied up = cost_basis × (months_until_sold / 12) × 0.5
                months = min(12.0, dos / 30.0)
                hold_carry_cost = cost_basis * carrying_rate * (months / 12.0) * 0.5
                hold_npv = hold_gross_profit - hold_carry_cost
            elif adv90 > 0:
                # Only fraction sells in 1 year, rest stays frozen
                units_sold_yr = adv90 * 365
                fraction_sold = min(1.0, units_sold_yr / available) if available > 0 else 0
                hold_gross_profit = float(annual_gross_profit or 0)
                hold_carry_cost = cost_basis * carrying_rate
                hold_npv = hold_gross_profit - hold_carry_cost
            else:
                # No sales at all — pure burn
                hold_npv = -cost_basis * carrying_rate

            # Decision logic: liquidate if NPV-positive AND velocity is bad
            if velocity_tier in ("frozen", "cold") and liq_npv > hold_npv:
                decision = "LIQUIDATE"
            elif velocity_tier in ("frozen", "cold"):
                decision = "PROMO"
            elif velocity_tier == "warm" and dos is not None and dos > 180:
                decision = "PROMO"
            else:
                decision = "HOLD"

            items_full.append({
                "offerId": offer_id,
                "sku": sku,
                "name": name,
                "brand": brand,
                "categoryName": category_name,
                "abcClass": abc_class,
                "velocityTier": velocity_tier,
                "units": available,
                "price": float(price or 0),
                "purchasedPrice": float(purchased_price) if purchased_price else None,
                "effectiveUnitCost": unit_cost,
                "costQuality": cost_quality,  # 'actual' or 'fallback'
                "saleValue": sale_value,
                "costBasis": cost_basis,
                "excessCapitalCost": excess_capital_cost,
                "daysSinceSale": days_since_sale,
                "daysInStock": days_in_stock,
                "daysOfSupply": int(dos) if dos is not None else None,
                "qtySold30d": qty_sold_30d or 0,
                "qtySold90d": qty_sold_90d or 0,
                "revenue30d": float(revenue_30d or 0),
                "revenue90d": float(revenue_90d or 0),
                "avgDailySales30d": float(avg_daily_sales_30d or 0),
                "avgDailySales90d": adv90,
                "velocityRatio30to90": float(velocity_ratio_30_90) if velocity_ratio_30_90 is not None else None,
                "annualGrossProfit": float(annual_gross_profit or 0),
                "gmroi": float(gmroi) if gmroi is not None else None,
                "npvHold": hold_npv,
                "npvLiquidate": liq_npv,
                "decision": decision,
            })

        # ─── Build aggregates ─────────────────────────────────────────────────
        # Quadrant matrix: ABC × velocity_tier
        matrix = {a: {t: {"skuCount": 0, "units": 0, "costBasis": 0.0,
                          "saleValue": 0.0, "revenue90d": 0.0}
                      for t in TIERS} for a in ABCS}
        for it in items_full:
            cell = matrix[it["abcClass"]][it["velocityTier"]]
            cell["skuCount"] += 1
            cell["units"] += it["units"]
            cell["costBasis"] += it["costBasis"]
            cell["saleValue"] += it["saleValue"]
            cell["revenue90d"] += it["revenue90d"]

        # Concentration / Pareto over excess_capital_cost
        items_by_excess = sorted(items_full, key=lambda x: x["excessCapitalCost"], reverse=True)
        total_excess = sum(it["excessCapitalCost"] for it in items_full)
        total_cost_basis = sum(it["costBasis"] for it in items_full)

        def _share(n: int) -> Dict[str, Any]:
            head = items_by_excess[:n]
            head_sum = sum(it["excessCapitalCost"] for it in head)
            return {
                "topN": n,
                "excessCapitalCost": head_sum,
                "share": (head_sum / total_excess) if total_excess > 0 else 0,
            }

        concentration = {
            "totalExcessCapitalCost": total_excess,
            "totalCostBasis": total_cost_basis,
            "top10": _share(10),
            "top20": _share(20),
            "top50": _share(50),
        }

        # Cost quality
        actual_count = sum(1 for it in items_full if it["costQuality"] == "actual")
        fallback_count = len(items_full) - actual_count
        actual_basis = sum(it["costBasis"] for it in items_full if it["costQuality"] == "actual")
        cost_quality = {
            "actualSkus": actual_count,
            "fallbackSkus": fallback_count,
            "actualPct": round(100.0 * actual_count / len(items_full), 1) if items_full else 0,
            "actualCostBasis": actual_basis,
            "fallbackCostBasis": total_cost_basis - actual_basis,
        }

        # GMROI distribution
        gmrois = [it["gmroi"] for it in items_full if it["gmroi"] is not None and it["gmroi"] > 0]
        gmrois.sort()
        if gmrois:
            n = len(gmrois)
            median = gmrois[n // 2]
            p25 = gmrois[n // 4]
            p75 = gmrois[(3 * n) // 4]
        else:
            median = p25 = p75 = 0
        under_100_skus = [it for it in items_full
                          if it["gmroi"] is not None and it["gmroi"] < 1.0 and it["costBasis"] > 0]
        gmroi_distribution = {
            "median": median,
            "p25": p25,
            "p75": p75,
            "under100Count": len(under_100_skus),
            "under100CostBasis": sum(it["costBasis"] for it in under_100_skus),
            "benchmarkRange": [2.0, 4.0],  # 200-400% benchmark for cosmetics
        }

        # Liquidation queue summary (decision == LIQUIDATE)
        liq = [it for it in items_full if it["decision"] == "LIQUIDATE"]
        liquidation_summary = {
            "skuCount": len(liq),
            "costBasis": sum(it["costBasis"] for it in liq),
            "saleValue": sum(it["saleValue"] for it in liq),
            "recoveryAtDiscount": sum(it["saleValue"] * (1 - liquidation_discount) for it in liq),
            "carryingCostSavedPerYear": sum(it["costBasis"] * carrying_rate for it in liq),
            "discount": liquidation_discount,
        }

        return {
            "items": items_by_excess if limit is None else items_by_excess[:limit],
            "quadrantMatrix": matrix,
            "concentration": concentration,
            "costQuality": cost_quality,
            "gmroiDistribution": gmroi_distribution,
            "liquidationSummary": liquidation_summary,
            "params": {
                "carryingRate": carrying_rate,
                "liquidationDiscount": liquidation_discount,
                "optimalDays": OPTIMAL_DAYS,
            },
        }

    async def get_brand_rotation(self, min_skus: int = 1) -> List[Dict[str, Any]]:
        """Brand-level rotation scorecard from v_sku_dead_stock_v2.

        Returns list of brands sorted by frozen capital (descending).
        Each brand: rotation days, GMROI, cost basis, sale value, 90d revenue,
        SKU count, frozen SKU share.
        """
        async with self.connection() as conn:
            rows = conn.execute("""
                SELECT
                    COALESCE(NULLIF(brand, ''), '—') as brand,
                    COUNT(*) as sku_count,
                    SUM(CASE WHEN velocity_tier IN ('frozen','cold') THEN 1 ELSE 0 END) as frozen_skus,
                    SUM(available) as units,
                    SUM(cost_basis) as cost_basis,
                    SUM(sale_value) as sale_value,
                    SUM(revenue_90d) as revenue_90d,
                    SUM(qty_sold_90d) as qty_sold_90d,
                    SUM(annual_gross_profit) as annual_gross_profit
                FROM v_sku_dead_stock_v2
                GROUP BY 1
                HAVING COUNT(*) >= ?
                ORDER BY cost_basis DESC
            """, [min_skus]).fetchall()

        result = []
        for r in rows:
            (brand, sku_count, frozen_skus, units, cost_basis, sale_value,
             revenue_90d, qty_sold_90d, gross_profit) = r
            cost_basis = float(cost_basis or 0)
            sale_value = float(sale_value or 0)
            revenue_90d = float(revenue_90d or 0)
            gross_profit = float(gross_profit or 0)

            # Days to rotate at current 90d sales pace (cost_basis terms)
            cogs_90d = revenue_90d * (cost_basis / sale_value) if sale_value > 0 else 0
            rotation_days = (cost_basis / (cogs_90d / 90.0)) if cogs_90d > 0 else None
            gmroi = (gross_profit / cost_basis) if cost_basis > 0 else None

            # Health score: 0=red 1=green based on rotation_days
            if rotation_days is None or rotation_days > 365:
                health = "critical"
            elif rotation_days > 200:
                health = "poor"
            elif rotation_days > 120:
                health = "warning"
            elif rotation_days > 60:
                health = "ok"
            else:
                health = "great"

            result.append({
                "brand": brand,
                "skuCount": sku_count,
                "frozenSkus": frozen_skus or 0,
                "frozenShare": (frozen_skus / sku_count) if sku_count else 0,
                "units": units or 0,
                "costBasis": cost_basis,
                "saleValue": sale_value,
                "revenue90d": revenue_90d,
                "qtySold90d": qty_sold_90d or 0,
                "annualGrossProfit": gross_profit,
                "rotationDays": int(rotation_days) if rotation_days is not None else None,
                "gmroi": gmroi,
                "health": health,
            })
        return result

    async def get_recommended_actions(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recommended actions for dead stock using Layer 4 view.

        Returns:
            List of items with recommended actions
        """
        async with self.connection() as conn:
            items = conn.execute(f"""
                SELECT * FROM v_recommended_actions
                WHERE action IS NOT NULL
                LIMIT {limit}
            """).fetchall()

            return [
                {
                    "offerId": row[0],
                    "sku": row[1],
                    "name": row[2],
                    "brand": row[3],
                    "categoryName": row[4],
                    "units": row[5],
                    "value": float(row[6] or 0),
                    "daysSinceSale": row[7],
                    "daysInStock": row[8],
                    "status": row[9],
                    "action": row[10],
                }
                for row in items
            ]

    async def get_restock_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get low stock alerts using Layer 4 view.

        Returns:
            List of items that need restocking
        """
        async with self.connection() as conn:
            items = conn.execute(f"""
                SELECT * FROM v_restock_alerts
                WHERE alert_level IS NOT NULL
                LIMIT {limit}
            """).fetchall()

            return [
                {
                    "offerId": row[0],
                    "sku": row[1],
                    "name": row[2],
                    "brand": row[3],
                    "unitsLeft": row[4],
                    "daysSinceSale": row[5],
                    "alertLevel": row[6],
                }
                for row in items
            ]


    # ─── Inventory Turnover & Optimal Stock ──────────────────────────────────

    async def get_inventory_turnover(
        self,
        days: int = 30,
        *,
        lead_time_days: int = 14,
        safety_multiplier: float = 1.5,
        buffer_days: int = 5,
        max_acceptable_days: int = 60,
    ) -> Dict[str, Any]:
        """Compute inventory turnover KPIs, ABC analysis, sell-through, and excess stock.

        Runs multiple queries in a single connection and computes derived metrics
        in Python (DSI, turnover ratio, optimal stock model, frozen capital).

        Args:
            days: Look-back period for revenue calculation (7-90)
            lead_time_days: Supplier lead time in days
            safety_multiplier: Safety stock multiplier on lead time (e.g. 1.5 = 50% buffer)
            buffer_days: Extra buffer days for customs/logistics
            max_acceptable_days: Max acceptable stock days (yellow/red boundary)

        Returns:
            Dict with turnover, currentStock, kpis, optimal, excess,
            sellThrough, abc, and topExcess sections
        """
        async with self.connection() as conn:
            # Q1: Revenue over period (all sales types combined)
            rev = conn.execute(f"""
                SELECT COALESCE(SUM(revenue), 0), COUNT(DISTINCT date)
                FROM gold_daily_revenue
                WHERE date >= CURRENT_DATE - INTERVAL '{int(days)} days'
            """).fetchone()
            total_revenue = float(rev[0])
            actual_days = rev[1] or 1

            # Q2: Current stock totals
            stock = conn.execute("""
                SELECT
                    COALESCE(SUM(GREATEST(0, quantity - reserve) * price), 0),
                    COALESCE(SUM(GREATEST(0, quantity - reserve) * COALESCE(purchased_price, 0)), 0),
                    COALESCE(SUM(GREATEST(0, quantity - reserve)), 0),
                    COUNT(*) FILTER (WHERE quantity > 0)
                FROM offer_stocks
            """).fetchone()
            stock_value_sale = float(stock[0])
            stock_value_cost = float(stock[1])
            stock_units = int(stock[2])
            active_skus = int(stock[3])

            # Q3: ABC summary
            abc_rows = conn.execute("SELECT * FROM v_abc_summary").fetchall()

            # Q4: Sell-through distribution
            st = conn.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE sell_through_rate_30d >= 20),
                    COUNT(*) FILTER (WHERE sell_through_rate_30d > 0 AND sell_through_rate_30d < 20),
                    COUNT(*) FILTER (WHERE sell_through_rate_30d = 0 AND available > 0),
                    AVG(sell_through_rate_30d) FILTER (WHERE sell_through_rate_30d > 0),
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY days_of_supply)
                        FILTER (WHERE days_of_supply IS NOT NULL)
                FROM v_sku_sell_through
            """).fetchone()

            # Q5: Top excess SKUs (biggest capital traps)
            excess_rows = conn.execute("""
                SELECT offer_id, sku, name, brand, category_name,
                       available, available_value, days_of_supply,
                       sell_through_rate_30d, avg_daily_sales, revenue_90d
                FROM v_sku_sell_through
                WHERE days_of_supply > 90 OR (avg_daily_sales = 0 AND available > 0)
                ORDER BY available_value DESC
                LIMIT 20
            """).fetchall()

        # ── Python computations ──────────────────────────────────────────────
        daily_revenue = total_revenue / actual_days if actual_days > 0 else 0
        monthly_revenue = daily_revenue * 30

        # Core KPIs
        dsi = round(stock_value_sale / daily_revenue, 1) if daily_revenue > 0 else 0
        turnover_ratio = round((daily_revenue * 365) / stock_value_sale, 2) if stock_value_sale > 0 else 0
        stock_to_sales = round(stock_value_sale / monthly_revenue, 2) if monthly_revenue > 0 else 0

        # Optimal stock model (params from API, defaults: 14d lead + 7d safety + 5d buffer = 26d)
        safety_days = round(lead_time_days * (safety_multiplier - 1))
        optimal_days = lead_time_days + safety_days + buffer_days

        optimal_value = daily_revenue * optimal_days
        max_acceptable_value = daily_revenue * max_acceptable_days

        # Excess / frozen capital
        excess_value = max(0, stock_value_sale - optimal_value)
        excess_ratio = round(stock_value_sale / optimal_value, 2) if optimal_value > 0 else 0
        excess_days = round(dsi - optimal_days, 1) if dsi > optimal_days else 0
        carrying_cost_annual = round(excess_value * 0.25, 2)

        # ABC dict
        abc_dict: Dict[str, Any] = {}
        c_stock_pct = 0.0
        c_revenue_pct = 0.0
        for row in abc_rows:
            cls = row[0]  # abc_class
            entry = {
                "skuCount": int(row[1]),
                "totalUnits": int(row[2] or 0),
                "stockValue": float(row[3] or 0),
                "revenue": float(row[4] or 0),
                "stockPct": float(row[5] or 0),
                "revenuePct": float(row[6] or 0),
            }
            abc_dict[cls] = entry
            if cls == "C":
                c_stock_pct = entry["stockPct"]
                c_revenue_pct = entry["revenuePct"]

        imbalance_score = round(c_stock_pct / c_revenue_pct, 2) if c_revenue_pct > 0 else 0

        # Top excess list
        top_excess = [
            {
                "offerId": r[0],
                "sku": r[1],
                "name": r[2],
                "brand": r[3],
                "categoryName": r[4],
                "units": int(r[5] or 0),
                "value": float(r[6] or 0),
                "daysOfSupply": int(r[7]) if r[7] is not None else None,
                "sellThroughRate": float(r[8] or 0),
                "avgDailySales": float(r[9] or 0),
                "revenue90d": float(r[10] or 0),
            }
            for r in excess_rows
        ]

        return {
            "turnover": {
                "periodDays": days,
                "totalRevenue": round(total_revenue, 2),
                "actualDays": actual_days,
                "dailyRevenue": round(daily_revenue, 2),
                "monthlyRevenue": round(monthly_revenue, 2),
            },
            "currentStock": {
                "valueSale": round(stock_value_sale, 2),
                "valueCost": round(stock_value_cost, 2),
                "units": stock_units,
                "activeSkus": active_skus,
            },
            "kpis": {
                "dsi": dsi,
                "turnoverRatio": turnover_ratio,
                "stockToSales": stock_to_sales,
                "benchmarks": {
                    "dsi": [60, 90],
                    "turnoverRatio": [4, 6],
                    "stockToSales": [1.0, 2.5],
                },
            },
            "optimal": {
                "leadTimeDays": lead_time_days,
                "safetyDays": safety_days,
                "bufferDays": buffer_days,
                "totalDays": optimal_days,
                "totalValue": round(optimal_value, 2),
                "maxAcceptableDays": max_acceptable_days,
                "maxAcceptableValue": round(max_acceptable_value, 2),
            },
            "excess": {
                "excessValue": round(excess_value, 2),
                "excessRatio": excess_ratio,
                "excessDays": excess_days,
                "carryingCostAnnual": carrying_cost_annual,
            },
            "sellThrough": {
                "fastMovers": int(st[0] or 0),
                "slowMovers": int(st[1] or 0),
                "zeroVelocity": int(st[2] or 0),
                "avgSellThroughRate": round(float(st[3] or 0), 1),
                "medianDaysOfSupply": round(float(st[4] or 0), 0) if st[4] is not None else None,
            },
            "abc": {
                "A": abc_dict.get("A", {"skuCount": 0, "totalUnits": 0, "stockValue": 0, "revenue": 0, "stockPct": 0, "revenuePct": 0}),
                "B": abc_dict.get("B", {"skuCount": 0, "totalUnits": 0, "stockValue": 0, "revenue": 0, "stockPct": 0, "revenuePct": 0}),
                "C": abc_dict.get("C", {"skuCount": 0, "totalUnits": 0, "stockValue": 0, "revenue": 0, "stockPct": 0, "revenuePct": 0}),
                "imbalanceScore": imbalance_score,
            },
            "topExcess": top_excess,
        }

    async def get_abc_skus(self, abc_class: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get SKUs for a specific ABC class, sorted by revenue descending."""
        async with self.connection() as conn:
            rows = conn.execute("""
                SELECT offer_id, sku, name, brand, category_name,
                       available, available_value, price,
                       revenue_90d, qty_sold_90d
                FROM v_abc_classification
                WHERE abc_class = ?
                ORDER BY revenue_90d DESC
                LIMIT ?
            """, [abc_class, limit]).fetchall()

        return [
            {
                "offerId": r[0],
                "sku": r[1],
                "name": r[2],
                "brand": r[3],
                "categoryName": r[4],
                "units": int(r[5] or 0),
                "value": float(r[6] or 0),
                "price": float(r[7] or 0),
                "revenue90d": float(r[8] or 0),
                "qtySold90d": int(r[9] or 0),
            }
            for r in rows
        ]

    # ─── Revenue Predictions ─────────────────────────────────────────────────
