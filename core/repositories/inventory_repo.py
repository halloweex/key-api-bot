"""
Inventory repository for stock and inventory management.

Handles all inventory-related queries including:
- SKU inventory status (Layer 1)
- Inventory snapshots (Layer 2)
- Stock analysis views (Layer 3)
- Restock alerts and recommendations (Layer 4)
"""
from datetime import date, datetime, timedelta
from typing import Optional, List, Dict, Any

from core.repositories.base import BaseRepository, _date_in_kyiv
from core.observability import get_logger

logger = get_logger(__name__)


class InventoryRepository(BaseRepository):
    """Repository for inventory and stock management."""

    async def refresh_sku_inventory_status(self) -> int:
        """
        Refresh Layer 1: SKU Inventory Status.

        Joins offer_stocks, offers, and products to create a denormalized
        current state view of inventory.

        Returns:
            Number of SKUs refreshed
        """
        async with self.connection() as conn:
            # Calculate last sale date per offer
            conn.execute(f"""
                CREATE OR REPLACE TEMP TABLE last_sales AS
                SELECT
                    op.product_id,
                    MAX({_date_in_kyiv('o.ordered_at')}) as last_sale
                FROM order_products op
                JOIN orders o ON op.order_id = o.id
                WHERE o.status_id NOT IN (19, 21, 22, 23)
                GROUP BY op.product_id
            """)

            # Upsert into sku_inventory_status
            result = conn.execute("""
                INSERT INTO sku_inventory_status
                SELECT
                    os.id as offer_id,
                    COALESCE(off.product_id, 0) as product_id,
                    COALESCE(os.sku, '') as sku,
                    p.name,
                    p.brand,
                    p.category_id,
                    os.quantity,
                    os.reserve,
                    os.price,
                    os.purchased_price,
                    ls.last_sale as last_sale_date,
                    COALESCE(sis.first_seen_at, CURRENT_DATE) as first_seen_at,
                    CURRENT_TIMESTAMP as updated_at
                FROM offer_stocks os
                LEFT JOIN offers off ON os.id = off.id
                LEFT JOIN products p ON off.product_id = p.id
                LEFT JOIN last_sales ls ON off.product_id = ls.product_id
                LEFT JOIN sku_inventory_status sis ON os.id = sis.offer_id
                ON CONFLICT (offer_id) DO UPDATE SET
                    product_id = excluded.product_id,
                    sku = excluded.sku,
                    name = excluded.name,
                    brand = excluded.brand,
                    category_id = excluded.category_id,
                    quantity = excluded.quantity,
                    reserve = excluded.reserve,
                    price = excluded.price,
                    purchased_price = excluded.purchased_price,
                    last_sale_date = excluded.last_sale_date,
                    updated_at = CURRENT_TIMESTAMP
            """)

            count = conn.execute(
                "SELECT COUNT(*) FROM sku_inventory_status"
            ).fetchone()[0]
            logger.info(f"Refreshed SKU inventory status: {count} SKUs")
            return count

    async def record_sku_inventory_snapshot(self) -> bool:
        """
        Record Layer 2: Daily SKU Inventory Snapshot.

        Creates a point-in-time snapshot of all SKU inventory levels.

        Returns:
            True if snapshot was recorded
        """
        async with self.connection() as conn:
            today = date.today()

            # Check if already recorded today
            existing = conn.execute(
                "SELECT COUNT(*) FROM inventory_sku_history WHERE date = ?",
                [today]
            ).fetchone()[0]

            if existing > 0:
                logger.debug(f"SKU snapshot already exists for {today}")
                return False

            # Record snapshot from current inventory status
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
                "SELECT changes()"
            ).fetchone()[0]
            logger.info(f"Recorded SKU inventory snapshot: {count} SKUs for {today}")
            return True

    async def record_inventory_snapshot(self) -> bool:
        """
        Record legacy aggregated inventory snapshot.

        Kept for backwards compatibility with existing reports.

        Returns:
            True if snapshot was recorded
        """
        async with self.connection() as conn:
            today = date.today()

            # Check if already recorded today
            existing = conn.execute(
                "SELECT COUNT(*) FROM inventory_history WHERE date = ?",
                [today]
            ).fetchone()[0]

            if existing > 0:
                logger.debug(f"Inventory snapshot already exists for {today}")
                return False

            # Record aggregated snapshot
            conn.execute("""
                INSERT INTO inventory_history (date, total_quantity, total_value, total_reserve, sku_count)
                SELECT
                    CURRENT_DATE,
                    SUM(quantity),
                    SUM(quantity * price),
                    SUM(reserve),
                    COUNT(*)
                FROM offer_stocks
            """)

            logger.info(f"Recorded inventory snapshot for {today}")
            return True

    async def get_stock_summary(self, limit: int = 20) -> Dict[str, Any]:
        """
        Get inventory summary with status breakdown.

        Returns:
            Dict with total stats, status breakdown, and top items by value
        """
        async with self.connection() as conn:
            # Overall totals
            totals = conn.execute("""
                SELECT
                    COUNT(*) as sku_count,
                    SUM(quantity) as total_units,
                    SUM(quantity * price) as total_value,
                    SUM(reserve) as total_reserve
                FROM sku_inventory_status
                WHERE quantity > 0
            """).fetchone()

            # Status breakdown
            status_breakdown = conn.execute("""
                SELECT
                    status,
                    sku_count,
                    total_units,
                    total_value,
                    avg_days_since_sale
                FROM v_inventory_summary
                ORDER BY
                    CASE status
                        WHEN 'active' THEN 1
                        WHEN 'moderate' THEN 2
                        WHEN 'slow' THEN 3
                        WHEN 'dead' THEN 4
                    END
            """).fetchall()

            # Aging buckets
            aging = conn.execute("""
                SELECT bucket, sku_count, total_units, total_value
                FROM v_aging_buckets
                ORDER BY bucket_order
            """).fetchall()

            # Top items by value
            top_items = conn.execute(f"""
                SELECT
                    sku,
                    name,
                    brand,
                    quantity,
                    stock_value,
                    days_since_sale,
                    status
                FROM v_sku_status
                ORDER BY stock_value DESC
                LIMIT {limit}
            """).fetchall()

            return {
                "totals": {
                    "sku_count": totals[0] or 0,
                    "total_units": totals[1] or 0,
                    "total_value": float(totals[2] or 0),
                    "total_reserve": totals[3] or 0,
                },
                "status_breakdown": [{
                    "status": r[0],
                    "sku_count": r[1],
                    "total_units": r[2],
                    "total_value": float(r[3] or 0),
                    "avg_days_since_sale": float(r[4] or 0),
                } for r in status_breakdown],
                "aging_buckets": [{
                    "bucket": r[0],
                    "sku_count": r[1],
                    "total_units": r[2],
                    "total_value": float(r[3] or 0),
                } for r in aging],
                "top_items": [{
                    "sku": r[0],
                    "name": r[1],
                    "brand": r[2],
                    "quantity": r[3],
                    "stock_value": float(r[4] or 0),
                    "days_since_sale": r[5],
                    "status": r[6],
                } for r in top_items],
            }

    async def get_average_inventory(self, days: int = 30) -> Dict[str, Any]:
        """
        Calculate average inventory over a period.

        Uses (Beginning + Ending) / 2 method for average inventory value.

        Args:
            days: Number of days to look back

        Returns:
            Dict with beginning, ending, average, and turnover metrics
        """
        async with self.connection() as conn:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            # Get beginning inventory (earliest snapshot in range)
            beginning = conn.execute("""
                SELECT
                    SUM(quantity) as units,
                    SUM(quantity * price) as value
                FROM inventory_sku_history
                WHERE date = (
                    SELECT MIN(date) FROM inventory_sku_history
                    WHERE date >= ?
                )
            """, [start_date]).fetchone()

            # Get ending inventory (latest snapshot)
            ending = conn.execute("""
                SELECT
                    SUM(quantity) as units,
                    SUM(quantity * price) as value
                FROM inventory_sku_history
                WHERE date = (SELECT MAX(date) FROM inventory_sku_history)
            """).fetchone()

            # Calculate cost of goods sold in period
            cogs = conn.execute(f"""
                SELECT SUM(op.price_sold * op.quantity)
                FROM order_products op
                JOIN orders o ON op.order_id = o.id
                WHERE {_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?
                  AND o.status_id NOT IN (19, 21, 22, 23)
            """, [start_date, end_date]).fetchone()[0] or 0

            beginning_value = float(beginning[1] or 0) if beginning else 0
            ending_value = float(ending[1] or 0) if ending else 0
            average_value = (beginning_value + ending_value) / 2

            # Inventory turnover = COGS / Average Inventory
            turnover = cogs / average_value if average_value > 0 else 0

            return {
                "period_days": days,
                "beginning": {
                    "units": beginning[0] or 0 if beginning else 0,
                    "value": beginning_value,
                },
                "ending": {
                    "units": ending[0] or 0 if ending else 0,
                    "value": ending_value,
                },
                "average_value": average_value,
                "cogs": float(cogs),
                "turnover_ratio": round(turnover, 2),
                "days_on_hand": round(days / turnover, 1) if turnover > 0 else None,
            }

    async def get_inventory_trend(
        self,
        days: int = 30,
        granularity: str = "daily"
    ) -> List[Dict[str, Any]]:
        """
        Get inventory trend over time.

        Args:
            days: Number of days to look back
            granularity: 'daily' or 'monthly'

        Returns:
            List of trend data points
        """
        async with self.connection() as conn:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            if granularity == "monthly":
                result = conn.execute("""
                    SELECT
                        DATE_TRUNC('month', date) as period,
                        AVG(total_quantity) as avg_units,
                        AVG(total_value) as avg_value,
                        AVG(sku_count) as avg_sku_count
                    FROM inventory_history
                    WHERE date >= ?
                    GROUP BY DATE_TRUNC('month', date)
                    ORDER BY period
                """, [start_date]).fetchall()
            else:
                result = conn.execute("""
                    SELECT
                        date as period,
                        total_quantity as avg_units,
                        total_value as avg_value,
                        sku_count as avg_sku_count
                    FROM inventory_history
                    WHERE date >= ?
                    ORDER BY date
                """, [start_date]).fetchall()

            return [{
                "date": str(r[0]),
                "units": r[1] or 0,
                "value": float(r[2] or 0),
                "sku_count": r[3] or 0,
            } for r in result]

    async def get_inventory_summary_v2(self) -> Dict[str, Any]:
        """
        Get comprehensive inventory summary (v2 API).

        Returns:
            Dict with all inventory metrics and breakdowns
        """
        async with self.connection() as conn:
            # Active stock (items with quantity > 0)
            active = conn.execute("""
                SELECT
                    COUNT(*) as sku_count,
                    SUM(quantity) as total_units,
                    SUM(stock_value) as total_value
                FROM v_sku_status
                WHERE status = 'active'
            """).fetchone()

            # Dead stock
            dead = conn.execute("""
                SELECT
                    COUNT(*) as sku_count,
                    SUM(quantity) as total_units,
                    SUM(stock_value) as total_value
                FROM v_sku_status
                WHERE status = 'dead'
            """).fetchone()

            # Low stock alerts
            low_stock = conn.execute("""
                SELECT COUNT(*)
                FROM v_restock_alerts
            """).fetchone()[0]

            return {
                "active_stock": {
                    "sku_count": active[0] or 0,
                    "units": active[1] or 0,
                    "value": float(active[2] or 0),
                },
                "dead_stock": {
                    "sku_count": dead[0] or 0,
                    "units": dead[1] or 0,
                    "value": float(dead[2] or 0),
                },
                "low_stock_alerts": low_stock or 0,
            }

    async def get_dead_stock_items_v2(
        self, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get dead stock items with recommendations.

        Args:
            limit: Maximum items to return

        Returns:
            List of dead stock items with action recommendations
        """
        async with self.connection() as conn:
            result = conn.execute(f"""
                SELECT
                    offer_id,
                    sku,
                    name,
                    brand,
                    category_name,
                    quantity,
                    stock_value,
                    days_since_sale,
                    recommended_action,
                    potential_loss
                FROM v_recommended_actions
                WHERE status = 'dead'
                LIMIT {limit}
            """).fetchall()

            return [{
                "offer_id": r[0],
                "sku": r[1],
                "name": r[2],
                "brand": r[3],
                "category": r[4],
                "quantity": r[5],
                "stock_value": float(r[6] or 0),
                "days_since_sale": r[7],
                "recommended_action": r[8],
                "potential_loss": float(r[9] or 0),
            } for r in result]

    async def get_recommended_actions(
        self, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get recommended actions for slow/dead stock.

        Args:
            limit: Maximum items to return

        Returns:
            List of items with action recommendations
        """
        async with self.connection() as conn:
            result = conn.execute(f"""
                SELECT
                    offer_id,
                    sku,
                    name,
                    brand,
                    category_name,
                    quantity,
                    stock_value,
                    days_since_sale,
                    status,
                    recommended_action,
                    potential_loss
                FROM v_recommended_actions
                LIMIT {limit}
            """).fetchall()

            return [{
                "offer_id": r[0],
                "sku": r[1],
                "name": r[2],
                "brand": r[3],
                "category": r[4],
                "quantity": r[5],
                "stock_value": float(r[6] or 0),
                "days_since_sale": r[7],
                "status": r[8],
                "recommended_action": r[9],
                "potential_loss": float(r[10] or 0),
            } for r in result]

    async def get_restock_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get low stock alerts for active items.

        Args:
            limit: Maximum items to return

        Returns:
            List of items needing restock
        """
        async with self.connection() as conn:
            result = conn.execute(f"""
                SELECT
                    offer_id,
                    sku,
                    name,
                    brand,
                    category_name,
                    quantity,
                    reserve,
                    available,
                    days_since_sale,
                    alert_level
                FROM v_restock_alerts
                LIMIT {limit}
            """).fetchall()

            return [{
                "offer_id": r[0],
                "sku": r[1],
                "name": r[2],
                "brand": r[3],
                "category": r[4],
                "quantity": r[5],
                "reserve": r[6],
                "available": r[7],
                "days_since_sale": r[8],
                "alert_level": r[9],
            } for r in result]
