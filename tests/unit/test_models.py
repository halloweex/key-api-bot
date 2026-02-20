"""
Tests for core.models module.
"""
import pytest
from datetime import datetime, timezone

from core.models import (
    SourceId,
    OrderStatus,
    Buyer,
    Manager,
    ProductOffer,
    OrderProduct,
    Order,
    Product,
    Category,
    SalesSourceData,
    TopProduct,
    SummaryStats,
)


class TestSourceId:
    """Tests for SourceId enum."""

    def test_active_sources(self):
        """active_sources should exclude Opencart."""
        active = SourceId.active_sources()
        assert SourceId.INSTAGRAM in active
        assert SourceId.TELEGRAM in active
        assert SourceId.SHOPIFY in active
        assert SourceId.OPENCART not in active

    def test_display_name(self):
        """display_name should return human-readable name."""
        assert SourceId.INSTAGRAM.display_name == "Instagram"
        assert SourceId.TELEGRAM.display_name == "Telegram"
        assert SourceId.SHOPIFY.display_name == "Shopify"

    def test_emoji(self):
        """emoji should return appropriate emoji."""
        assert SourceId.INSTAGRAM.emoji == "📸"
        assert SourceId.TELEGRAM.emoji == "✈️"

    def test_color(self):
        """color should return hex color."""
        assert SourceId.INSTAGRAM.color == "#7C3AED"
        assert SourceId.TELEGRAM.color == "#2563EB"


class TestOrderStatus:
    """Tests for OrderStatus enum."""

    def test_return_statuses(self):
        """return_statuses should contain all return/cancel IDs."""
        statuses = OrderStatus.return_statuses()
        assert 19 in statuses  # RETURNED
        assert 21 in statuses  # CANCELED
        assert 22 in statuses  # REFUNDED
        assert 23 in statuses  # REJECTED


class TestBuyer:
    """Tests for Buyer dataclass."""

    def test_from_api_valid(self):
        """from_api should parse valid buyer data."""
        data = {
            "id": 123,
            "created_at": "2025-01-01T10:00:00Z",
            "phone": ["+380123456789"],
            "email": ["test@example.com"],
            "full_name": "John Doe",
        }
        buyer = Buyer.from_api(data)

        assert buyer.id == 123
        assert buyer.phone == "+380123456789"
        assert buyer.full_name == "John Doe"
        assert buyer.created_at is not None

    def test_from_api_none(self):
        """from_api should return None for None input."""
        assert Buyer.from_api(None) is None

    def test_full_name(self):
        """full_name should use the provided value or default to None."""
        buyer = Buyer(id=1, full_name="John Doe")
        assert buyer.full_name == "John Doe"

        buyer = Buyer(id=1)
        assert buyer.full_name is None

    def test_is_returning(self):
        """is_returning should check if buyer existed before period."""
        period_start = datetime(2025, 1, 15, tzinfo=timezone.utc)

        # Buyer created before period - returning
        old_buyer = Buyer(
            id=1,
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc)
        )
        assert old_buyer.is_returning(period_start) is True

        # Buyer created during period - new
        new_buyer = Buyer(
            id=2,
            created_at=datetime(2025, 1, 20, tzinfo=timezone.utc)
        )
        assert new_buyer.is_returning(period_start) is False

        # No created_at - not returning
        unknown_buyer = Buyer(id=3)
        assert unknown_buyer.is_returning(period_start) is False


class TestOrderProduct:
    """Tests for OrderProduct dataclass."""

    def test_from_api(self):
        """from_api should parse product data."""
        data = {
            "name": "Test Product",
            "quantity": 2,
            "price_sold": 100.50,
            "offer": {"product_id": 456},
        }
        product = OrderProduct.from_api(data)

        assert product.name == "Test Product"
        assert product.quantity == 2
        assert product.price_sold == 100.50
        assert product.product_id == 456

    def test_total(self):
        """total should calculate line total."""
        product = OrderProduct(name="Test", quantity=3, price_sold=50.0)
        assert product.total == 150.0


class TestOrder:
    """Tests for Order dataclass."""

    def test_from_api_full(self):
        """from_api should parse full order data."""
        data = {
            "id": 1001,
            "source_id": 1,
            "status_id": 5,
            "grand_total": 500.00,
            "ordered_at": "2025-01-10T14:30:00Z",
            "buyer": {"id": 123, "first_name": "John"},
            "manager": {"id": 19, "name": "Manager1"},
            "products": [
                {"name": "Product A", "quantity": 1, "price_sold": 200},
                {"name": "Product B", "quantity": 2, "price_sold": 150},
            ],
        }
        order = Order.from_api(data)

        assert order.id == 1001
        assert order.source_id == 1
        assert order.source == SourceId.INSTAGRAM
        assert order.grand_total == 500.00
        assert order.buyer is not None
        assert order.buyer.id == 123
        assert len(order.products) == 2

    def test_is_return(self):
        """is_return should check status."""
        normal_order = Order(id=1, source_id=1, status_id=5, grand_total=100)
        assert normal_order.is_return is False

        returned_order = Order(id=2, source_id=1, status_id=19, grand_total=100)
        assert returned_order.is_return is True

    def test_is_within_period(self):
        """is_within_period should check date range."""
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 31, tzinfo=timezone.utc)

        order_in = Order(
            id=1, source_id=1, status_id=5, grand_total=100,
            ordered_at=datetime(2025, 1, 15, tzinfo=timezone.utc)
        )
        assert order_in.is_within_period(start, end) is True

        order_out = Order(
            id=2, source_id=1, status_id=5, grand_total=100,
            ordered_at=datetime(2025, 2, 15, tzinfo=timezone.utc)
        )
        assert order_out.is_within_period(start, end) is False

    def test_matches_manager(self):
        """matches_manager should check manager ID list."""
        order = Order(
            id=1, source_id=2, status_id=5, grand_total=100,
            manager=Manager(id=19, name="Test")
        )
        assert order.matches_manager(["19", "22"]) is True
        assert order.matches_manager(["1", "2"]) is False


class TestProduct:
    """Tests for Product dataclass."""

    def test_from_api_with_brand(self):
        """from_api should extract brand from custom_fields."""
        data = {
            "id": 100,
            "name": "Test Product",
            "category_id": 5,
            "custom_fields": [
                {"name": "Brand", "value": ["TestBrand"]},
            ],
        }
        product = Product.from_api(data)

        assert product.id == 100
        assert product.name == "Test Product"
        assert product.category_id == 5
        assert product.brand == "TestBrand"

    def test_from_api_with_brand_uuid(self):
        """from_api should extract brand using CT_1001 uuid."""
        data = {
            "id": 101,
            "name": "Test Product 2",
            "category_id": 5,
            "custom_fields": [
                {"uuid": "CT_1001", "value": ["AnotherBrand"]},
            ],
        }
        product = Product.from_api(data)

        assert product.brand == "AnotherBrand"

    def test_from_api_no_brand(self):
        """from_api should handle missing brand."""
        data = {"id": 100, "name": "Test"}
        product = Product.from_api(data)
        assert product.brand is None


class TestSalesSourceData:
    """Tests for SalesSourceData dataclass."""

    def test_from_source(self):
        """from_source should create from SourceId enum."""
        data = SalesSourceData.from_source(
            SourceId.INSTAGRAM,
            orders=50,
            revenue=10000.555
        )
        assert data.source_id == 1
        assert data.name == "Instagram"
        assert data.orders == 50
        assert data.revenue == 10000.56  # Rounded
        assert data.color == "#7C3AED"

    def test_to_dict(self):
        """to_dict should return serializable dict."""
        data = SalesSourceData(
            source_id=1,
            name="Instagram",
            orders=10,
            revenue=1000.0,
            color="#7C3AED"
        )
        d = data.to_dict()
        assert d["source_id"] == 1
        assert d["name"] == "Instagram"


class TestTopProduct:
    """Tests for TopProduct dataclass."""

    def test_to_dict(self):
        """to_dict should round values."""
        product = TopProduct(
            name="Test",
            quantity=10,
            revenue=1234.567,
            percentage=15.789
        )
        d = product.to_dict()
        assert d["revenue"] == 1234.57
        assert d["percentage"] == 15.8


class TestSummaryStats:
    """Tests for SummaryStats dataclass."""

    def test_to_dict(self):
        """to_dict should round monetary values."""
        stats = SummaryStats(
            total_orders=100,
            total_revenue=50000.999,
            avg_check=500.555,
            returns_count=5,
            returns_revenue=2500.111
        )
        d = stats.to_dict()
        assert d["total_revenue"] == 50001.0
        assert d["avg_check"] == 500.56
        assert d["returns_revenue"] == 2500.11
