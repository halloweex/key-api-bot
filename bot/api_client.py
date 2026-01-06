"""
Pure HTTP client for KeyCRM OpenAPI.

This module contains ONLY HTTP request methods and simple API wrappers.
Business logic is handled in services.py.
"""
import requests
from typing import Dict, Optional, Any
from bot.config import KEYCRM_BASE_URL


class KeyCRMClient:
    """Pure HTTP client for KeyCRM API - no business logic."""

    def __init__(self, api_key: str, base_url: str = KEYCRM_BASE_URL):
        """
        Initialize the KeyCRM API client.

        Args:
            api_key: Your KeyCRM API key
            base_url: The base URL for the KeyCRM API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make a request to the KeyCRM API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            params: Query parameters
            data: Data to send in the request body

        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}/{endpoint}"

        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=self.headers, params=params)
            elif method.upper() == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            elif method.upper() == "PUT":
                response = requests.put(url, headers=self.headers, json=data)
            elif method.upper() == "DELETE":
                response = requests.delete(url, headers=self.headers, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()  # Raise exception for 4XX/5XX responses

            if response.content:
                return response.json()
            return {"status": "success"}

        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status code: {e.response.status_code}")
                print(f"Response body: {e.response.text}")
            return {"error": str(e)}

    # ─── Order API Methods ──────────────────────────────────────────────────

    def get_orders(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a list of orders with optional filtering.

        Args:
            params: Query parameters for filtering (page, limit, filter, include, etc.)

        Returns:
            API response containing orders data
        """
        return self._make_request("GET", "order", params=params)

    def get_order(self, order_id: int, include_products: bool = True) -> Dict[str, Any]:
        """
        Get a specific order by ID.

        Args:
            order_id: The ID of the order to retrieve
            include_products: Whether to include products information (default True)

        Returns:
            API response containing order data
        """
        params = {"include": "products,buyer"} if include_products else None
        return self._make_request("GET", f"order/{order_id}", params=params)

    def create_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new order.

        Args:
            order_data: Order data to create

        Returns:
            API response
        """
        return self._make_request("POST", "order", data=order_data)

    def update_order(self, order_id: int, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing order.

        Args:
            order_id: Order ID to update
            order_data: Updated order data

        Returns:
            API response
        """
        return self._make_request("PUT", f"order/{order_id}", data=order_data)

    # ─── Customer API Methods ───────────────────────────────────────────────

    def get_customers(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a list of customers with optional filtering.

        Args:
            params: Query parameters for filtering

        Returns:
            API response containing customers data
        """
        return self._make_request("GET", "customer", params=params)

    def get_customer(self, customer_id: int) -> Dict[str, Any]:
        """
        Get a specific customer by ID.

        Args:
            customer_id: The ID of the customer to retrieve

        Returns:
            API response containing customer data
        """
        return self._make_request("GET", f"customer/{customer_id}")

    def create_customer(self, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new customer.

        Args:
            customer_data: Customer data to create

        Returns:
            API response
        """
        return self._make_request("POST", "customer", data=customer_data)

    def update_customer(self, customer_id: int, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing customer.

        Args:
            customer_id: Customer ID to update
            customer_data: Updated customer data

        Returns:
            API response
        """
        return self._make_request("PUT", f"customer/{customer_id}", data=customer_data)

    # ─── Product API Methods ────────────────────────────────────────────────

    def get_products(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a list of products with optional filtering.

        Args:
            params: Query parameters for filtering

        Returns:
            API response containing products data
        """
        return self._make_request("GET", "product", params=params)

    def get_product(self, product_id: int) -> Dict[str, Any]:
        """
        Get a specific product by ID.

        Args:
            product_id: The ID of the product to retrieve

        Returns:
            API response containing product data
        """
        return self._make_request("GET", f"product/{product_id}")

    # ─── Status API Methods ─────────────────────────────────────────────────

    def get_statuses(self) -> Dict[str, Any]:
        """
        Get all available order statuses.

        Returns:
            API response containing statuses data
        """
        return self._make_request("GET", "status")
