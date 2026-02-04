#!/usr/bin/env python3
"""
Check specific order 33587 details.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.keycrm import KeyCRMClient
from core.models import OrderStatus


async def main():
    order_id = 33587

    print(f"\n{'='*60}")
    print(f"Checking Order ID {order_id}")
    print(f"{'='*60}")

    # Print return statuses for reference
    return_statuses = set(int(s) for s in OrderStatus.return_statuses())
    print(f"\nReturn status IDs: {sorted(return_statuses)}")

    client = KeyCRMClient()
    await client.connect()

    # Fetch the specific order
    order = await client.get_order(order_id)

    if order:
        print(f"\n{'ORDER DETAILS':^60}")
        print("-" * 60)
        print(f"ID:            {order['id']}")
        print(f"Status ID:     {order.get('status_id')}")
        print(f"Is Return:     {order.get('status_id') in return_statuses}")
        print(f"Grand Total:   ₴{float(order.get('grand_total', 0)):,.2f}")
        print(f"Source ID:     {order.get('source_id')}")
        print(f"Ordered At:    {order.get('ordered_at')}")
        print(f"Created At:    {order.get('created_at')}")
        print(f"Updated At:    {order.get('updated_at')}")

        # Get status name if available
        if order.get('status'):
            print(f"Status Name:   {order['status'].get('name', 'N/A')}")

        # Check order history if available
        history = order.get('history', [])
        if history:
            print(f"\n{'ORDER HISTORY':^60}")
            print("-" * 60)
            for h in history[-10:]:  # Last 10 history items
                print(f"  {h.get('created_at', 'N/A')}: {h.get('type', 'N/A')} - {h.get('data', {})}")
    else:
        print(f"Order {order_id} not found!")

    await client.close()

    # Now check in the API what status we have
    print(f"\n{'='*60}")
    print("Checking via Dashboard API...")
    print("-" * 60)

    import httpx
    async with httpx.AsyncClient() as http_client:
        # Get DuckDB stats to verify connection
        stats_resp = await http_client.get("http://localhost:8080/api/duckdb/stats")
        stats = stats_resp.json()
        print(f"DuckDB status: {stats.get('status')}")
        print(f"Total orders in DB: {stats.get('orders', 'N/A')}")


if __name__ == "__main__":
    asyncio.run(main())
