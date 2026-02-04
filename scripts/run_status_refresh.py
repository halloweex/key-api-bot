#!/usr/bin/env python3
"""
Run order status refresh to fix status discrepancies.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


async def main():
    from core.sync_service import get_sync_service

    print("Running order status refresh (last 30 days)...")

    sync_service = await get_sync_service()
    stats = await sync_service.refresh_order_statuses(days_back=30)

    print(f"\nResult: {stats}")
    print("\nDone! Run the daily comparison again to verify the fix.")


if __name__ == "__main__":
    asyncio.run(main())
