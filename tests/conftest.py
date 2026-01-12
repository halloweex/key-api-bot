"""
Pytest configuration and shared fixtures.
"""
import pytest
from datetime import date, datetime
from typing import Dict, List, Any
from unittest.mock import MagicMock


@pytest.fixture
def sample_order() -> Dict[str, Any]:
    """Sample order data from KeyCRM API."""
    return {
        "id": 12345,
        "ordered_at": "2026-01-10T14:30:00Z",
        "created_at": "2026-01-10T14:35:00Z",
        "source_id": 1,
        "status_id": 1,
        "grand_total": 2500.00,
        "products_total": 2500.00,
        "manager": {"id": "19", "name": "Manager 1"},
        "buyer": {
            "id": 1001,
            "created_at": "2025-12-01T10:00:00Z",
            "email": "customer@example.com"
        },
        "products": [
            {
                "name": "Product A",
                "quantity": 2,
                "price_sold": 1000.00,
                "offer": {"product_id": 101}
            },
            {
                "name": "Product B",
                "quantity": 1,
                "price_sold": 500.00,
                "offer": {"product_id": 102}
            }
        ]
    }


@pytest.fixture
def sample_orders() -> List[Dict[str, Any]]:
    """List of sample orders for testing aggregation."""
    return [
        {
            "id": 1,
            "ordered_at": "2026-01-10T10:00:00Z",
            "source_id": 1,
            "status_id": 1,
            "grand_total": 1000.00,
            "manager": {"id": "19"},
            "buyer": {"id": 101, "created_at": "2026-01-10T09:00:00Z"},
            "products": [{"name": "Item 1", "quantity": 2, "offer": {"product_id": 1}}]
        },
        {
            "id": 2,
            "ordered_at": "2026-01-10T11:00:00Z",
            "source_id": 2,
            "status_id": 1,
            "grand_total": 1500.00,
            "manager": {"id": "22"},
            "buyer": {"id": 102, "created_at": "2025-12-01T09:00:00Z"},
            "products": [{"name": "Item 2", "quantity": 1, "offer": {"product_id": 2}}]
        },
        {
            "id": 3,
            "ordered_at": "2026-01-10T12:00:00Z",
            "source_id": 4,
            "status_id": 1,
            "grand_total": 2000.00,
            "manager": None,
            "buyer": {"id": 103, "created_at": "2025-11-15T09:00:00Z"},
            "products": [{"name": "Item 3", "quantity": 3, "offer": {"product_id": 3}}]
        },
        # Return order (should be excluded)
        {
            "id": 4,
            "ordered_at": "2026-01-10T13:00:00Z",
            "source_id": 1,
            "status_id": 19,  # Return status
            "grand_total": 500.00,
            "manager": {"id": "19"},
            "buyer": {"id": 104, "created_at": "2025-10-01T09:00:00Z"},
            "products": [{"name": "Item 4", "quantity": 1, "offer": {"product_id": 4}}]
        }
    ]


@pytest.fixture
def mock_api_client():
    """Mock KeyCRM API client."""
    client = MagicMock()
    client.get_orders = MagicMock()
    client.get_products = MagicMock()
    return client


@pytest.fixture
def mock_api_response(sample_orders):
    """Mock API response with pagination."""
    return {
        "data": sample_orders,
        "meta": {
            "current_page": 1,
            "last_page": 1,
            "per_page": 50,
            "total": len(sample_orders)
        }
    }
