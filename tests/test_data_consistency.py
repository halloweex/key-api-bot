"""
Data consistency tests between KeyCRM API and DuckDB.

These tests verify that data in DuckDB matches what KeyCRM API returns,
accounting for timezone differences and return order filtering.

Run with: pytest tests/test_data_consistency.py -v
"""
import asyncio
import pytest
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal

from core.keycrm import get_async_client
from core.duckdb_store import get_store
from core.models import OrderStatus

KYIV_TZ = ZoneInfo("Europe/Kyiv")
RETURN_STATUSES = set(int(s) for s in OrderStatus.return_statuses())

# Acceptable revenue difference (due to price updates, rounding)
REVENUE_TOLERANCE_PERCENT = 0.5  # 0.5%
REVENUE_TOLERANCE_ABSOLUTE = 100  # ₴100


class KeyCRMDataFetcher:
    """Fetch data directly from KeyCRM API for comparison."""

    def __init__(self):
        self.client = None

    async def connect(self):
        # Always create a fresh client to avoid event loop issues
        from core.keycrm import KeyCRMClient
        self.client = KeyCRMClient()
        await self.client.connect()

    async def get_orders_for_date_kyiv(self, target_date: date) -> dict:
        """
        Get orders for a specific date in Kyiv timezone.

        Returns dict with:
            - total_orders: int
            - non_return_orders: int
            - total_revenue: float
            - by_source: dict[source_name, {count, revenue}]
        """
        if not self.client:
            await self.connect()

        # Fetch orders from a wide date range to capture all possible orders
        # that might have ordered_at on target_date in Kyiv timezone
        start_range = target_date - timedelta(days=2)
        end_range = target_date + timedelta(days=2)

        all_orders = []
        params = {
            "include": "products.offer,manager,buyer",
            "filter[created_between]": f"{start_range}, {end_range}",
        }

        async for batch in self.client.paginate("order", params=params, page_size=50):
            all_orders.extend(batch)

        # Also fetch by updated_between
        params_updated = {
            "include": "products.offer,manager,buyer",
            "filter[updated_between]": f"{start_range}, {end_range}",
        }

        try:
            async for batch in self.client.paginate("order", params=params_updated, page_size=50):
                for order in batch:
                    if order["id"] not in [o["id"] for o in all_orders]:
                        all_orders.append(order)
        except Exception:
            pass

        # Filter to orders with ordered_at on target_date in Kyiv timezone
        target_orders = []
        for order in all_orders:
            ordered_at_str = order.get("ordered_at")
            if ordered_at_str:
                try:
                    ordered_at_utc = datetime.fromisoformat(
                        ordered_at_str.replace("Z", "+00:00")
                    )
                    ordered_at_kyiv = ordered_at_utc.astimezone(KYIV_TZ)
                    if ordered_at_kyiv.date() == target_date:
                        target_orders.append(order)
                except (ValueError, TypeError):
                    pass

        # Calculate stats
        source_names = {1: "Instagram", 2: "Telegram", 4: "Shopify"}

        non_return_orders = [
            o for o in target_orders
            if o.get("status_id") not in RETURN_STATUSES
        ]

        by_source = {}
        for order in non_return_orders:
            sid = order.get("source_id")
            name = source_names.get(sid, f"Source_{sid}")
            if name not in by_source:
                by_source[name] = {"count": 0, "revenue": 0.0}
            by_source[name]["count"] += 1
            by_source[name]["revenue"] += float(order.get("grand_total", 0))

        total_revenue = sum(
            float(o.get("grand_total", 0)) for o in non_return_orders
        )

        return {
            "date": target_date,
            "total_orders": len(target_orders),
            "non_return_orders": len(non_return_orders),
            "return_orders": len(target_orders) - len(non_return_orders),
            "total_revenue": total_revenue,
            "by_source": by_source,
        }


class DuckDBDataFetcher:
    """Fetch data from DuckDB for comparison."""

    def __init__(self):
        self.store = None

    async def connect(self):
        # Always create fresh store to avoid event loop issues
        from core.duckdb_store import DuckDBStore
        self.store = DuckDBStore()
        await self.store.connect()

    async def get_orders_for_date(self, target_date: date) -> dict:
        """
        Get orders for a specific date from DuckDB.

        DuckDB queries already use Kyiv timezone conversion.
        """
        if not self.store:
            await self.connect()

        # Get summary stats
        summary = await self.store.get_summary_stats(
            target_date, target_date, sales_type="all"
        )

        # Get by source breakdown
        sales_by_source = await self.store.get_sales_by_source(
            target_date, target_date, sales_type="all"
        )

        by_source = {}
        for i, label in enumerate(sales_by_source.get("labels", [])):
            by_source[label] = {
                "count": sales_by_source["orders"][i],
                "revenue": float(sales_by_source["revenue"][i]),
            }

        return {
            "date": target_date,
            "total_orders": summary["totalOrders"] + summary["totalReturns"],
            "non_return_orders": summary["totalOrders"],
            "return_orders": summary["totalReturns"],
            "total_revenue": float(summary["totalRevenue"]),
            "by_source": by_source,
        }


def revenue_matches(expected: float, actual: float) -> bool:
    """Check if revenues match within tolerance."""
    if expected == 0 and actual == 0:
        return True

    diff = abs(expected - actual)

    # Check absolute tolerance
    if diff <= REVENUE_TOLERANCE_ABSOLUTE:
        return True

    # Check percentage tolerance
    if expected > 0:
        percent_diff = (diff / expected) * 100
        if percent_diff <= REVENUE_TOLERANCE_PERCENT:
            return True

    return False


class TestDataConsistency:
    """Test data consistency between KeyCRM and DuckDB."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.keycrm = KeyCRMDataFetcher()
        self.duckdb = DuckDBDataFetcher()

    @pytest.mark.asyncio
    async def test_today_data_matches(self):
        """Test that today's data matches between KeyCRM and DuckDB."""
        today = date.today()
        await self._compare_date(today)

    @pytest.mark.asyncio
    async def test_yesterday_data_matches(self):
        """Test that yesterday's data matches."""
        yesterday = date.today() - timedelta(days=1)
        await self._compare_date(yesterday)

    @pytest.mark.asyncio
    async def test_last_week_data_matches(self):
        """Test that data from 7 days ago matches."""
        last_week = date.today() - timedelta(days=7)
        await self._compare_date(last_week)

    @pytest.mark.asyncio
    async def test_specific_date_dec_7_2025(self):
        """Test the specific date that had discrepancy issues."""
        target = date(2025, 12, 7)
        await self._compare_date(target)

    @pytest.mark.asyncio
    async def test_month_boundary(self):
        """Test first day of current month."""
        today = date.today()
        first_of_month = today.replace(day=1)
        await self._compare_date(first_of_month)

    async def _compare_date(self, target_date: date):
        """Compare data for a specific date."""
        # Skip future dates
        if target_date > date.today():
            pytest.skip(f"Date {target_date} is in the future")

        # Fetch from both sources
        keycrm_data = await self.keycrm.get_orders_for_date_kyiv(target_date)
        duckdb_data = await self.duckdb.get_orders_for_date(target_date)

        # Log the comparison
        print(f"\n=== Comparing {target_date} ===")
        print(f"KeyCRM: {keycrm_data['non_return_orders']} orders, "
              f"₴{keycrm_data['total_revenue']:,.2f}")
        print(f"DuckDB: {duckdb_data['non_return_orders']} orders, "
              f"₴{duckdb_data['total_revenue']:,.2f}")

        # Compare order counts
        assert duckdb_data["non_return_orders"] == keycrm_data["non_return_orders"], (
            f"Order count mismatch for {target_date}: "
            f"DuckDB={duckdb_data['non_return_orders']}, "
            f"KeyCRM={keycrm_data['non_return_orders']}"
        )

        # Compare total revenue (with tolerance)
        assert revenue_matches(
            keycrm_data["total_revenue"],
            duckdb_data["total_revenue"]
        ), (
            f"Revenue mismatch for {target_date}: "
            f"DuckDB=₴{duckdb_data['total_revenue']:,.2f}, "
            f"KeyCRM=₴{keycrm_data['total_revenue']:,.2f}, "
            f"diff=₴{abs(keycrm_data['total_revenue'] - duckdb_data['total_revenue']):,.2f}"
        )

        # Compare by source
        for source, keycrm_source_data in keycrm_data["by_source"].items():
            duckdb_source_data = duckdb_data["by_source"].get(source, {"count": 0, "revenue": 0})

            assert duckdb_source_data["count"] == keycrm_source_data["count"], (
                f"{source} order count mismatch for {target_date}: "
                f"DuckDB={duckdb_source_data['count']}, "
                f"KeyCRM={keycrm_source_data['count']}"
            )

            assert revenue_matches(
                keycrm_source_data["revenue"],
                duckdb_source_data["revenue"]
            ), (
                f"{source} revenue mismatch for {target_date}: "
                f"DuckDB=₴{duckdb_source_data['revenue']:,.2f}, "
                f"KeyCRM=₴{keycrm_source_data['revenue']:,.2f}"
            )


class TestDataConsistencyRange:
    """Test data consistency over a date range."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.keycrm = KeyCRMDataFetcher()
        self.duckdb = DuckDBDataFetcher()

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_last_7_days(self):
        """Test all days in the last 7 days."""
        errors = []

        for days_ago in range(7):
            target = date.today() - timedelta(days=days_ago)
            try:
                keycrm_data = await self.keycrm.get_orders_for_date_kyiv(target)
                duckdb_data = await self.duckdb.get_orders_for_date(target)

                if duckdb_data["non_return_orders"] != keycrm_data["non_return_orders"]:
                    errors.append(
                        f"{target}: orders {duckdb_data['non_return_orders']} vs {keycrm_data['non_return_orders']}"
                    )
                elif not revenue_matches(keycrm_data["total_revenue"], duckdb_data["total_revenue"]):
                    errors.append(
                        f"{target}: revenue ₴{duckdb_data['total_revenue']:,.2f} vs ₴{keycrm_data['total_revenue']:,.2f}"
                    )
                else:
                    print(f"✓ {target}: {duckdb_data['non_return_orders']} orders, ₴{duckdb_data['total_revenue']:,.2f}")
            except Exception as e:
                errors.append(f"{target}: {str(e)}")

        assert not errors, f"Data mismatches found:\n" + "\n".join(errors)

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_last_30_days_sampling(self):
        """Test sampled days from the last 30 days (every 5th day)."""
        errors = []

        for days_ago in range(0, 30, 5):  # 0, 5, 10, 15, 20, 25
            target = date.today() - timedelta(days=days_ago)
            try:
                keycrm_data = await self.keycrm.get_orders_for_date_kyiv(target)
                duckdb_data = await self.duckdb.get_orders_for_date(target)

                if duckdb_data["non_return_orders"] != keycrm_data["non_return_orders"]:
                    errors.append(
                        f"{target}: orders {duckdb_data['non_return_orders']} vs {keycrm_data['non_return_orders']}"
                    )
                elif not revenue_matches(keycrm_data["total_revenue"], duckdb_data["total_revenue"]):
                    errors.append(
                        f"{target}: revenue ₴{duckdb_data['total_revenue']:,.2f} vs ₴{keycrm_data['total_revenue']:,.2f}"
                    )
                else:
                    print(f"✓ {target}: {duckdb_data['non_return_orders']} orders, ₴{duckdb_data['total_revenue']:,.2f}")
            except Exception as e:
                errors.append(f"{target}: {str(e)}")

        assert not errors, f"Data mismatches found:\n" + "\n".join(errors)


# Quick sanity check that can be run standalone
async def quick_check(target_date: date = None):
    """Quick standalone check for a single date."""
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    print(f"Checking data consistency for {target_date}...")

    keycrm = KeyCRMDataFetcher()
    duckdb = DuckDBDataFetcher()

    keycrm_data = await keycrm.get_orders_for_date_kyiv(target_date)
    duckdb_data = await duckdb.get_orders_for_date(target_date)

    print(f"\n{'='*50}")
    print(f"Date: {target_date}")
    print(f"{'='*50}")
    print(f"\n{'Source':<15} {'KeyCRM':>20} {'DuckDB':>20} {'Match':>10}")
    print("-" * 65)

    # Total
    orders_match = duckdb_data["non_return_orders"] == keycrm_data["non_return_orders"]
    rev_match = revenue_matches(keycrm_data["total_revenue"], duckdb_data["total_revenue"])

    print(f"{'Orders':<15} {keycrm_data['non_return_orders']:>20} {duckdb_data['non_return_orders']:>20} {'✓' if orders_match else '✗':>10}")
    print(f"{'Revenue':<15} {'₴{:,.2f}'.format(keycrm_data['total_revenue']):>20} {'₴{:,.2f}'.format(duckdb_data['total_revenue']):>20} {'✓' if rev_match else '✗':>10}")

    # By source
    print(f"\nBy Source:")
    all_sources = set(keycrm_data["by_source"].keys()) | set(duckdb_data["by_source"].keys())

    for source in sorted(all_sources):
        kc = keycrm_data["by_source"].get(source, {"count": 0, "revenue": 0})
        db = duckdb_data["by_source"].get(source, {"count": 0, "revenue": 0})

        count_match = kc["count"] == db["count"]
        rev_match = revenue_matches(kc["revenue"], db["revenue"])

        print(f"  {source}:")
        print(f"    Orders:  {kc['count']:>10} vs {db['count']:<10} {'✓' if count_match else '✗'}")
        print(f"    Revenue: ₴{kc['revenue']:>10,.2f} vs ₴{db['revenue']:<10,.2f} {'✓' if rev_match else '✗'}")

    return orders_match and rev_match


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Parse date argument
        target = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    else:
        target = None

    result = asyncio.run(quick_check(target))
    sys.exit(0 if result else 1)
