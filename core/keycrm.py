"""
Unified async HTTP client for KeyCRM API.

Provides a single async-first client for both bot and web services.
Supports connection pooling, parallel pagination, and proper error handling.

Features:
- Connection pooling with httpx
- Exponential backoff retry (3 attempts)
- Circuit breaker (opens after 5 failures in 60s)
- Request correlation IDs for tracing
"""
import asyncio
import os
from typing import Dict, List, Any, Optional, AsyncGenerator
from contextlib import asynccontextmanager

import httpx

from core.exceptions import KeyCRMAPIError, KeyCRMConnectionError, KeyCRMDataError
from core.models import Order, Product, Category, Buyer
from core.observability import get_logger, get_correlation_id, Timer
from core.resilience import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    RetryConfig,
    retry_with_backoff,
)

logger = get_logger(__name__)

# Configuration
KEYCRM_BASE_URL = os.getenv("KEYCRM_BASE_URL", "https://openapi.keycrm.app/v1")
KEYCRM_API_KEY = os.getenv("KEYCRM_API_KEY", "")
REQUEST_TIMEOUT = 30.0
MAX_CONCURRENT_REQUESTS = 5

# Resilience configuration
RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
    exponential_base=2.0
)

CIRCUIT_BREAKER_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    recovery_timeout=60.0,
    half_open_requests=1
)

# Global circuit breaker instance
_circuit_breaker = CircuitBreaker(config=CIRCUIT_BREAKER_CONFIG)


class KeyCRMClient:
    """
    Unified async HTTP client for KeyCRM API.

    Usage:
        async with KeyCRMClient() as client:
            orders = await client.get_orders(params)

        # Or with manual lifecycle:
        client = KeyCRMClient()
        await client.connect()
        try:
            orders = await client.get_orders(params)
        finally:
            await client.close()
    """

    def __init__(
        self,
        api_key: str = None,
        base_url: str = None,
        timeout: float = REQUEST_TIMEOUT
    ):
        """
        Initialize KeyCRM client.

        Args:
            api_key: KeyCRM API key (defaults to KEYCRM_API_KEY env var)
            base_url: API base URL (defaults to KEYCRM_BASE_URL env var)
            timeout: Request timeout in seconds
        """
        self.api_key = api_key or KEYCRM_API_KEY
        self.base_url = base_url or KEYCRM_BASE_URL
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

        if not self.api_key:
            raise ValueError("KEYCRM_API_KEY is required")

    @property
    def headers(self) -> Dict[str, str]:
        """Request headers with auth."""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def connect(self) -> None:
        """Create HTTP client with connection pooling."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers=self.headers,
                timeout=self.timeout,
                limits=httpx.Limits(
                    max_keepalive_connections=10,
                    max_connections=20,
                )
            )

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "KeyCRMClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make HTTP request to KeyCRM API with retry and circuit breaker.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (without base URL)
            params: Query parameters
            json: JSON body for POST/PUT

        Returns:
            API response as dict

        Raises:
            KeyCRMConnectionError: Network/timeout errors
            KeyCRMAPIError: API returned error response
            CircuitOpenError: Circuit breaker is open
        """
        # Check circuit breaker
        if not await _circuit_breaker.can_execute():
            raise CircuitOpenError(
                f"Circuit breaker is open, request to {endpoint} rejected"
            )

        try:
            # Use retry with exponential backoff
            result = await retry_with_backoff(
                self._do_request,
                method, endpoint, params, json,
                config=RETRY_CONFIG,
                retryable_exceptions=(KeyCRMConnectionError, httpx.RequestError),
            )
            await _circuit_breaker.record_success()
            return result

        except (KeyCRMAPIError, KeyCRMConnectionError) as e:
            await _circuit_breaker.record_failure()
            raise

    async def _do_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute a single HTTP request (called by retry wrapper)."""
        if not self._client:
            await self.connect()

        url = f"{self.base_url}/{endpoint}"

        # Add correlation ID to request headers
        request_headers = {}
        correlation_id = get_correlation_id()
        if correlation_id:
            request_headers["X-Request-ID"] = correlation_id

        try:
            with Timer(f"keycrm_{endpoint}", logger) as timer:
                response = await self._client.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    headers=request_headers if request_headers else None,
                )

            # Handle HTTP errors
            if response.status_code >= 400:
                error_text = response.text[:500]
                logger.error(
                    f"API error {response.status_code}: {error_text}",
                    extra={"endpoint": endpoint, "status_code": response.status_code}
                )
                raise KeyCRMAPIError(
                    f"API returned {response.status_code}",
                    status_code=response.status_code,
                    details=error_text
                )

            if response.content:
                return response.json()
            return {"status": "success"}

        except httpx.TimeoutException as e:
            logger.error(
                f"Request timeout: {method} {endpoint}",
                extra={"endpoint": endpoint, "timeout": self.timeout}
            )
            raise KeyCRMConnectionError(
                f"Request timeout after {self.timeout}s",
                retry_after=5
            ) from e

        except httpx.RequestError as e:
            logger.error(
                f"Request failed: {method} {endpoint} - {e}",
                extra={"endpoint": endpoint, "error": str(e)}
            )
            raise KeyCRMConnectionError(str(e)) from e

    # ═══════════════════════════════════════════════════════════════════════════
    # ORDER METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    async def get_orders(
        self,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get orders with optional filtering.

        Args:
            params: Query params (page, limit, filter, include, etc.)

        Returns:
            API response with 'data' list
        """
        return await self._request("GET", "order", params=params)

    async def get_order(
        self,
        order_id: int,
        include: str = "products,buyer"
    ) -> Dict[str, Any]:
        """Get single order by ID."""
        params = {"include": include} if include else None
        return await self._request("GET", f"order/{order_id}", params=params)

    async def create_order(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new order."""
        return await self._request("POST", "order", json=data)

    async def update_order(
        self,
        order_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an existing order."""
        return await self._request("PUT", f"order/{order_id}", json=data)

    # ═══════════════════════════════════════════════════════════════════════════
    # CUSTOMER METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    async def get_customers(
        self,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get customers with optional filtering."""
        return await self._request("GET", "buyer", params=params)

    async def get_customer(self, customer_id: int) -> Dict[str, Any]:
        """Get single customer by ID."""
        return await self._request("GET", f"buyer/{customer_id}")

    async def create_customer(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new customer."""
        return await self._request("POST", "buyer", json=data)

    async def update_customer(
        self,
        customer_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an existing customer."""
        return await self._request("PUT", f"buyer/{customer_id}", json=data)

    # ═══════════════════════════════════════════════════════════════════════════
    # PRODUCT METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    async def get_products(
        self,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get products with optional filtering."""
        return await self._request("GET", "products", params=params)

    async def get_product(self, product_id: int) -> Dict[str, Any]:
        """Get single product by ID."""
        return await self._request("GET", f"products/{product_id}")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    async def get_categories(
        self,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get product categories."""
        return await self._request("GET", "products/categories", params=params)

    # ═══════════════════════════════════════════════════════════════════════════
    # STATUS METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    async def get_statuses(self) -> Dict[str, Any]:
        """Get all order statuses."""
        return await self._request("GET", "order-status")

    # ═══════════════════════════════════════════════════════════════════════════
    # USER/MANAGER METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    async def get_users(
        self,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get users/managers from KeyCRM."""
        return await self._request("GET", "users", params=params)

    # ═══════════════════════════════════════════════════════════════════════════
    # STOCK METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    async def get_offers(
        self,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get offers (product variations) from KeyCRM.

        Each offer links offer_id to product_id, enabling proper joins
        between offer_stocks and products tables.
        """
        return await self._request("GET", "offers", params=params)

    async def get_stocks(
        self,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get offer stocks from KeyCRM."""
        return await self._request("GET", "offers/stocks", params=params)

    # ═══════════════════════════════════════════════════════════════════════════
    # SEARCH METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    async def search_orders(
        self,
        query: str,
        search_type: str = "auto",
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Search orders by ID, phone, or email.

        Args:
            query: Search query
            search_type: 'id', 'phone', 'email', or 'auto' (detect from query)
            limit: Max results

        Returns:
            API response with matching orders
        """
        params = {
            "include": "products,buyer,status",
            "limit": limit
        }

        # Auto-detect search type
        if search_type == "auto":
            if query.isdigit():
                search_type = "id"
            elif "@" in query:
                search_type = "email"
            else:
                search_type = "phone"

        # Search by ID first
        if search_type == "id":
            try:
                order = await self.get_order(int(query))
                if order and "id" in order:
                    return {"data": [order], "total": 1}
            except (KeyCRMAPIError, ValueError):
                pass
            return {"data": [], "total": 0}

        # Search by buyer info
        if search_type == "phone":
            params["filter[buyer_phone]"] = query
        elif search_type == "email":
            params["filter[buyer_email]"] = query

        return await self._request("GET", "order", params=params)

    # ═══════════════════════════════════════════════════════════════════════════
    # PAGINATION HELPERS
    # ═══════════════════════════════════════════════════════════════════════════

    async def paginate(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 50,
        max_pages: int = 100,
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Paginate through API results.

        Yields batches of items from paginated endpoint.

        Args:
            endpoint: API endpoint
            params: Base query params
            page_size: Items per page
            max_pages: Maximum pages to fetch

        Yields:
            List of items per page
        """
        params = dict(params or {})
        params["limit"] = page_size

        for page in range(1, max_pages + 1):
            params["page"] = page

            response = await self._request("GET", endpoint, params=params)
            batch = response.get("data", [])

            if not batch:
                break

            yield batch

            if len(batch) < page_size:
                break

    async def fetch_all(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 50,
        max_pages: int = 100,
        parallel_batches: int = MAX_CONCURRENT_REQUESTS,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all items from paginated endpoint using parallel requests.

        Args:
            endpoint: API endpoint
            params: Base query params
            page_size: Items per page
            max_pages: Maximum pages to fetch
            parallel_batches: Number of parallel requests

        Returns:
            List of all items
        """
        params = dict(params or {})
        params["limit"] = page_size

        all_items = []

        # Fetch first page to check if there's more
        params["page"] = 1
        first_response = await self._request("GET", endpoint, params=params)
        first_batch = first_response.get("data", [])

        if not first_batch:
            return []

        all_items.extend(first_batch)

        if len(first_batch) < page_size:
            return all_items

        # Fetch remaining pages in parallel batches
        page = 2
        while page <= max_pages:
            tasks = []
            for p in range(page, min(page + parallel_batches, max_pages + 1)):
                page_params = {**params, "page": p}
                tasks.append(self._request("GET", endpoint, params=page_params))

            if not tasks:
                break

            results = await asyncio.gather(*tasks, return_exceptions=True)

            done = False
            for result in results:
                if isinstance(result, Exception):
                    logger.warning(f"Page fetch failed: {result}")
                    continue

                batch = result.get("data", [])
                if not batch:
                    done = True
                    break

                all_items.extend(batch)

                if len(batch) < page_size:
                    done = True
                    break

            if done:
                break

            page += parallel_batches

        return all_items

    async def fetch_all_orders(
        self,
        params: Optional[Dict[str, Any]] = None,
        max_pages: int = 100,
    ) -> List[Order]:
        """
        Fetch all orders and parse into Order models.

        Args:
            params: Query params for filtering
            max_pages: Maximum pages to fetch

        Returns:
            List of Order objects
        """
        raw_orders = await self.fetch_all("order", params, max_pages=max_pages)
        return [Order.from_api(data) for data in raw_orders]

    async def fetch_all_products(
        self,
        params: Optional[Dict[str, Any]] = None,
        max_pages: int = 100,
    ) -> List[Product]:
        """
        Fetch all products and parse into Product models.

        Args:
            params: Query params for filtering
            max_pages: Maximum pages to fetch

        Returns:
            List of Product objects
        """
        raw_products = await self.fetch_all("products", params, max_pages=max_pages)
        return [Product.from_api(data) for data in raw_products]

    async def fetch_all_offers(
        self,
        params: Optional[Dict[str, Any]] = None,
        max_pages: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all offers (product variations).

        Offers link offer_id to product_id, enabling proper joins
        between offer_stocks and products tables.

        Args:
            params: Query params for filtering
            max_pages: Maximum pages to fetch

        Returns:
            List of offer dicts with id, product_id, sku
        """
        return await self.fetch_all("offers", params, max_pages=max_pages)

    async def fetch_all_stocks(
        self,
        params: Optional[Dict[str, Any]] = None,
        max_pages: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all offer stocks.

        Args:
            params: Query params for filtering
            max_pages: Maximum pages to fetch

        Returns:
            List of stock dicts with id, sku, price, purchased_price, quantity, reserve
        """
        return await self.fetch_all("offers/stocks", params, max_pages=max_pages)


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC WRAPPER (for backwards compatibility)
# ═══════════════════════════════════════════════════════════════════════════════

class SyncKeyCRMClient:
    """
    Synchronous wrapper around async KeyCRMClient.

    For use in synchronous contexts (like some bot handlers).
    Creates a new event loop for each operation if needed.
    """

    def __init__(
        self,
        api_key: str = None,
        base_url: str = None,
        timeout: float = REQUEST_TIMEOUT
    ):
        self._async_client = KeyCRMClient(api_key, base_url, timeout)

    def _run(self, coro):
        """Run coroutine in event loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # We're in an async context, use nest_asyncio or create task
            import nest_asyncio
            nest_asyncio.apply()
            return asyncio.get_event_loop().run_until_complete(coro)
        else:
            return asyncio.run(coro)

    def get_orders(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get orders synchronously."""
        async def _get():
            async with self._async_client as client:
                return await client.get_orders(params)
        return self._run(_get())

    def get_order(self, order_id: int, include: str = "products,buyer") -> Dict[str, Any]:
        """Get single order synchronously."""
        async def _get():
            async with self._async_client as client:
                return await client.get_order(order_id, include)
        return self._run(_get())

    def search_orders(
        self,
        query: str,
        search_type: str = "auto",
        limit: int = 10
    ) -> Dict[str, Any]:
        """Search orders synchronously."""
        async def _search():
            async with self._async_client as client:
                return await client.search_orders(query, search_type, limit)
        return self._run(_search())

    def get_products(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get products synchronously."""
        async def _get():
            async with self._async_client as client:
                return await client.get_products(params)
        return self._run(_get())

    def get_categories(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get categories synchronously."""
        async def _get():
            async with self._async_client as client:
                return await client.get_categories(params)
        return self._run(_get())


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_client_instance: Optional[KeyCRMClient] = None


def get_client() -> KeyCRMClient:
    """Get singleton KeyCRM client instance."""
    global _client_instance
    if _client_instance is None:
        _client_instance = KeyCRMClient()
    return _client_instance


async def get_async_client() -> KeyCRMClient:
    """Get connected singleton client for async contexts."""
    client = get_client()
    await client.connect()
    return client
