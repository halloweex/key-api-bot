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
)


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
