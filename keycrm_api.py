import os
import requests
import json
from datetime import datetime, time, timedelta
import pytz
from collections import defaultdict
from dotenv import load_dotenv


load_dotenv()

KEYCRM_API_KEY = os.getenv("KEYCRM_API_KEY")

# Source dictionary mapping source_id to source name
source_dct = {1: 'Instagram', 2: 'Telegram', 3: 'Opencart', 4: 'Shopify'}

class KeyCRMAPI:
    """
    A Python client for the KeyCRM OpenAPI.
    """

    def __init__(self, api_key, base_url="https://openapi.keycrm.app/v1"):
        """
        Initialize the KeyCRM API client.

        Args:
            api_key (str): Your KeyCRM API key
            base_url (str): The base URL for the KeyCRM API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def _make_request(self, method, endpoint, params=None, data=None):
        """
        Make a request to the KeyCRM API.

        Args:
            method (str): HTTP method (GET, POST, PUT, DELETE)
            endpoint (str): API endpoint
            params (dict, optional): Query parameters
            data (dict, optional): Data to send in the request body

        Returns:
            dict: API response
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

            response.raise_for_status()  # Raise an exception for 4XX/5XX responses

            if response.content:
                return response.json()
            return {"status": "success"}

        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status code: {e.response.status_code}")
                print(f"Response body: {e.response.text}")
            return {"error": str(e)}

    # Orders
    def get_orders(self, params=None):
        """Get a list of orders with optional filtering"""
        return self._make_request("GET", "order", params=params)

    def get_order(self, order_id, include_products=True):
        """
        Get a specific order by ID

        Args:
            order_id: The ID of the order to retrieve
            include_products: Whether to include products information (default True)
        """
        params = {"include": "products,buyer"} if include_products else None
        return self._make_request("GET", f"order/{order_id}", params=params)

    def create_order(self, order_data):
        """Create a new order"""
        return self._make_request("POST", "order", data=order_data)

    def update_order(self, order_id, order_data):
        """Update an existing order"""
        return self._make_request("PUT", f"order/{order_id}", data=order_data)

    # Customers
    def get_customers(self, params=None):
        """Get a list of customers with optional filtering"""
        return self._make_request("GET", "customer", params=params)

    def get_customer(self, customer_id):
        """Get a specific customer by ID"""
        return self._make_request("GET", f"customer/{customer_id}")

    def create_customer(self, customer_data):
        """Create a new customer"""
        return self._make_request("POST", "customer", data=customer_data)

    def update_customer(self, customer_id, customer_data):
        """Update an existing customer"""
        return self._make_request("PUT", f"customer/{customer_id}", data=customer_data)

    # Products
    def get_products(self, params=None):
        """Get a list of products with optional filtering"""
        return self._make_request("GET", "product", params=params)

    def get_product(self, product_id):
        """Get a specific product by ID"""
        return self._make_request("GET", f"product/{product_id}")

    # Statuses
    def get_statuses(self):
        """Get all available order statuses"""
        return self._make_request("GET", "status")


    def get_sales_by_product_and_source_for_date(
            self,
            target_date,
            tz_name="Europe/Kiev",
            page_size=100):
        """
        Get sales data aggregated by product and source for a specific local-date window
        or date range, using KeyCRM's `created_between` filter.

        Args:
            target_date (date or str or tuple): The date(s) to get sales for:
                                              - datetime.date object
                                              - 'YYYY-MM-DD' string
                                              - (start_date, end_date) tuple for a date range
            tz_name (str): Timezone name to define your local midnight-to-midnight.
                           Default is "Europe/Kiev" (Kyiv timezone).
            page_size (int): Number of orders to fetch per page.

        Returns:
            tuple:
                sales_dict (dict): { source_id: { product_name: total_qty, … }, … }
                counts_dict (dict): { source_id: total_order_count, … }
                total_orders (int): total orders retrieved for that date
        """
        # Determine if we're dealing with a single date or date range
        if isinstance(target_date, tuple) and len(target_date) == 2:
            start_date, end_date = target_date
        else:
            # Single date - use the same date for start and end
            start_date = end_date = target_date

        # Normalize dates to string format if needed
        if not isinstance(start_date, str):
            start_date = start_date.strftime('%Y-%m-%d')
        if not isinstance(end_date, str):
            end_date = end_date.strftime('%Y-%m-%d')

        # Parse the strings into date objects
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Initialize container for all orders
        all_orders = []

        # Process each day in the range individually
        current = start
        while current <= end:
            # HARDCODED TIME CONVERSION: Kyiv is UTC+3
            # A full day in Kyiv (00:00 to 23:59) corresponds to:
            # Previous day 21:00 to current day 20:59 in UTC
            current_day_str = current.strftime("%Y-%m-%d")
            prev_day = current - timedelta(days=1)
            prev_day_str = prev_day.strftime("%Y-%m-%d")

            # Define the UTC time window for this Kyiv day
            utc_start_str = f"{prev_day_str} 21:00:00"
            utc_end_str = f"{current_day_str} 20:59:59"

            # Query for this day
            params = {
                "include": "products",
                "limit": page_size,
                "filter[created_between]": f"{utc_start_str}, {utc_end_str}",
            }

            # Get orders for this day
            page = 1
            while True:
                params["page"] = page
                resp = self.get_orders(params)
                if isinstance(resp, dict) and resp.get("error"):
                    break

                batch = resp.get("data", [])
                if not batch:
                    break

                all_orders.extend(batch)
                if len(batch) < page_size:
                    break
                page += 1

            # Move to next day
            current += timedelta(days=1)

        # Once we have all orders for the entire date range, perform aggregation
        sales = defaultdict(lambda: defaultdict(int))
        for order in all_orders:
            src = order.get("source_id") or "unknown"
            for prod in order.get("products", []):
                name = prod.get("name") or f"#{prod.get('id')}"
                qty = int(prod.get("quantity", 0))
                sales[src][name] += qty

        # Count orders per-source
        counts = defaultdict(int)
        for order in all_orders:
            src = order.get("source_id") or "unknown"
            counts[src] += 1

        # Return plain dicts + total
        sales_dict = {src: dict(prod_map) for src, prod_map in sales.items()}
        counts_dict = dict(counts)
        total_orders = len(all_orders)

        # Ensure we return exactly 3 values as expected
        return sales_dict, counts_dict, total_orders