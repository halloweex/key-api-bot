"""
Pytest configuration and shared fixtures.
"""
import asyncio
import pytest
from datetime import date, datetime
from typing import Dict, List, Any
from unittest.mock import MagicMock


@pytest.fixture(scope="function")
def event_loop():
    """Create a new event loop for each test function."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")


@pytest.fixture(autouse=True)
def _no_telegram_from_tests(monkeypatch):
    """Nothing in the suite may reach Telegram. Ever.

    `.env` holds a real BOT_TOKEN, and `bot.main.send_admin_message` falls back
    to the HTTP Bot API whenever no Application is running — which is every
    test process. So a test that deliberately fails warehouse validation, as
    the cell-guard tests must, sent a real alert to real admins:

        ⚠️ Warehouse validation failed — full retry scheduled (attempt 1/3).
        rows=2→2 (match=True), cells: 1 missing/0 orphaned, revenue=1000.00

    Two orders and a thousand hryvnia — unmistakably a fixture, delivered to a
    phone at midday. Individual tests stubbing `_send_warehouse_alert` is not
    protection; it only covers the paths someone remembered.

    Autouse and function-scoped, so a test that wants to exercise delivery can
    still patch the same name itself and see its own mock.
    """
    async def _blocked(*args, **kwargs):
        return 0

    monkeypatch.setattr(
        "core.telegram_alerts.send_admin_message_http", _blocked, raising=False,
    )


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
