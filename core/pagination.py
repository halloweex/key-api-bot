"""
Unified pagination for KeyCRM API.

Provides consistent pagination behavior across bot and web packages.
"""

import logging
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

from core.exceptions import KeyCRMAPIError, KeyCRMConnectionError, KeyCRMDataError

logger = logging.getLogger(__name__)


class KeyCRMPaginator:
    """
    Paginator for KeyCRM API endpoints.

    Handles:
    - Automatic page iteration
    - Rate limiting
    - Error handling
    - Response validation

    Usage:
        client = KeyCRMClient(api_key)
        paginator = KeyCRMPaginator(client.get_orders)

        for batch in paginator.paginate(params):
            for order in batch:
                process(order)
    """

    def __init__(
        self,
        fetch_func: Callable[[Dict[str, Any]], Dict[str, Any]],
        page_size: int = 50,
        rate_limit: float = 0.3,
        max_pages: Optional[int] = None,
    ):
        """
        Initialize paginator.

        Args:
            fetch_func: Function that fetches a page (e.g., client.get_orders)
            page_size: Number of items per page
            rate_limit: Delay between requests in seconds
            max_pages: Maximum pages to fetch (None = unlimited)
        """
        self.fetch_func = fetch_func
        self.page_size = page_size
        self.rate_limit = rate_limit
        self.max_pages = max_pages

    def paginate(
        self,
        params: Dict[str, Any],
        include_empty: bool = False
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Iterate through all pages of results.

        Args:
            params: Base query parameters (page will be added automatically)
            include_empty: Whether to yield empty final page

        Yields:
            List of items from each page

        Raises:
            KeyCRMAPIError: If API returns an error
            KeyCRMConnectionError: If connection fails
            KeyCRMDataError: If response structure is invalid
        """
        params = dict(params)  # Don't modify original
        params["limit"] = self.page_size
        page = 1

        while self.max_pages is None or page <= self.max_pages:
            params["page"] = page

            try:
                response = self.fetch_func(params)
            except Exception as e:
                raise KeyCRMConnectionError(
                    f"Failed to fetch page {page}",
                    str(e)
                ) from e

            # Validate response structure
            if not isinstance(response, dict):
                raise KeyCRMDataError(
                    "Invalid response type",
                    expected="dict",
                    got=type(response).__name__
                )

            # Check for API error
            if response.get("error"):
                raise KeyCRMAPIError(
                    f"API error on page {page}",
                    response.get("error")
                )

            # Get data
            batch = response.get("data")
            if batch is None:
                raise KeyCRMDataError(
                    "Response missing 'data' field",
                    expected="list",
                    got="None"
                )

            if not isinstance(batch, list):
                raise KeyCRMDataError(
                    "Response 'data' field is not a list",
                    expected="list",
                    got=type(batch).__name__
                )

            # Yield batch
            if batch or include_empty:
                yield batch

            # Check if we've reached the end
            if len(batch) < self.page_size:
                break

            # Rate limiting
            if self.rate_limit > 0:
                time.sleep(self.rate_limit)

            page += 1

    def fetch_all(
        self,
        params: Dict[str, Any],
        flatten: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages and return combined results.

        Args:
            params: Base query parameters
            flatten: If True, return flat list. If False, return list of batches.

        Returns:
            All items from all pages
        """
        if flatten:
            items = []
            for batch in self.paginate(params):
                items.extend(batch)
            return items
        else:
            return list(self.paginate(params))

    def count(self, params: Dict[str, Any]) -> int:
        """
        Count total items across all pages.

        Note: This fetches all pages, so it's expensive.
        Use only when you need exact count.

        Args:
            params: Base query parameters

        Returns:
            Total number of items
        """
        total = 0
        for batch in self.paginate(params):
            total += len(batch)
        return total


class AsyncKeyCRMPaginator:
    """
    Async paginator for KeyCRM API.

    Uses parallel fetching for better performance.
    """

    def __init__(
        self,
        fetch_func: Callable,
        page_size: int = 50,
        batch_size: int = 5,
        max_pages: Optional[int] = None,
    ):
        """
        Initialize async paginator.

        Args:
            fetch_func: Async function that fetches a page
            page_size: Number of items per page
            batch_size: Number of pages to fetch in parallel
            max_pages: Maximum pages to fetch (None = unlimited)
        """
        self.fetch_func = fetch_func
        self.page_size = page_size
        self.batch_size = batch_size
        self.max_pages = max_pages

    async def fetch_all(
        self,
        params: Dict[str, Any],
        client: Any = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages in parallel batches.

        Args:
            params: Base query parameters
            client: HTTP client to use

        Returns:
            All items from all pages
        """
        import asyncio

        params = dict(params)
        params["limit"] = self.page_size

        all_items = []
        page = 1
        has_more = True

        while has_more and (self.max_pages is None or page <= self.max_pages):
            # Calculate batch of pages to fetch
            end_page = page + self.batch_size
            if self.max_pages:
                end_page = min(end_page, self.max_pages + 1)

            # Create tasks for parallel fetching
            tasks = []
            for p in range(page, end_page):
                page_params = {**params, "page": p}
                tasks.append(self.fetch_func(page_params, client))

            # Execute in parallel
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Async pagination error: {e}")
                break

            # Process results
            has_more = False
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Page {page + i} failed: {result}")
                    continue

                batch = result.get("data", [])
                all_items.extend(batch)

                if len(batch) >= self.page_size:
                    has_more = True

            page = end_page

        return all_items
