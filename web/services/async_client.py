"""
Async HTTP client for KeyCRM API.
Used by web dashboard for faster data fetching.
"""
import asyncio
from typing import Dict, List, Any, Optional
import httpx

from bot.config import KEYCRM_API_KEY, KEYCRM_BASE_URL


class AsyncKeyCRMClient:
    """Async HTTP client for KeyCRM API."""

    def __init__(self, api_key: str = KEYCRM_API_KEY):
        self.api_key = api_key
        self.base_url = KEYCRM_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }

    async def get_orders(
        self,
        params: Dict[str, Any],
        client: httpx.AsyncClient
    ) -> Dict[str, Any]:
        """Fetch orders with given parameters."""
        try:
            response = await client.get(
                f"{self.base_url}/order",
                params=params,
                headers=self.headers,
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e), "data": []}

    async def fetch_all_orders(
        self,
        base_params: Dict[str, Any],
        max_pages: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Fetch all orders matching params using parallel pagination.
        Returns list of all orders.
        """
        all_orders = []
        limit = base_params.get("limit", 50)

        async with httpx.AsyncClient() as client:
            # First, get page 1 to know total
            params = {**base_params, "page": 1}
            first_response = await self.get_orders(params, client)

            if first_response.get("error"):
                return []

            first_batch = first_response.get("data", [])
            all_orders.extend(first_batch)

            # If first page is not full, we're done
            if len(first_batch) < limit:
                return all_orders

            # Fetch remaining pages in parallel batches of 5
            page = 2
            while page <= max_pages:
                # Create batch of tasks
                tasks = []
                for p in range(page, min(page + 5, max_pages + 1)):
                    params = {**base_params, "page": p}
                    tasks.append(self.get_orders(params, client))

                if not tasks:
                    break

                # Execute batch in parallel
                results = await asyncio.gather(*tasks)

                done = False
                for result in results:
                    if result.get("error"):
                        continue
                    batch_data = result.get("data", [])
                    if not batch_data:
                        done = True
                        break
                    all_orders.extend(batch_data)
                    if len(batch_data) < limit:
                        done = True
                        break

                if done:
                    break
                page += 5

        return all_orders
