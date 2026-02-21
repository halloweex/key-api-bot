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
                conn.execute("ROLLBACK")
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

                    conn.execute("""
                        INSERT OR REPLACE INTO offer_stocks
                        (id, sku, price, purchased_price, quantity, reserve, synced_at)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, [offer_id, stock.get("sku"), stock.get("price"),
                          stock.get("purchased_price"), new_qty, new_rsv])
                    count += 1

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
                conn.execute("ROLLBACK")
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
            conn.execute("""
                INSERT OR REPLACE INTO sku_inventory_status
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
                        (SELECT first_seen_at FROM sku_inventory_status WHERE offer_id = os.id),
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
                    days_since_sale, days_in_stock, threshold_days, status
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
                    "status": row[11],
                }
                for row in items
            ]

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


    # ─── Revenue Predictions ─────────────────────────────────────────────────
