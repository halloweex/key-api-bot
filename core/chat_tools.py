"""
Chat tools for LLM function calling.

Defines tools that the LLM can call to access data from DuckDB and Meilisearch.
"""
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from core.duckdb_store import get_store
from core.meilisearch_client import get_meili_client
from core.config import config
from core.observability import get_logger

logger = get_logger(__name__)

DEFAULT_TZ = ZoneInfo("Europe/Kyiv")


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL DEFINITIONS (for Anthropic API)
# ═══════════════════════════════════════════════════════════════════════════════

TOOLS = [
    {
        "name": "get_revenue_summary",
        "description": "Get revenue summary including total revenue, orders count, and average order value for a period. Use for questions like 'How are sales today?', 'What's the revenue this week?'",
        "input_schema": {
            "type": "object",
            "properties": {
                "period": {
                    "type": "string",
                    "enum": ["today", "yesterday", "week", "last_week", "month", "last_month"],
                    "description": "Time period for the summary"
                },
                "sales_type": {
                    "type": "string",
                    "enum": ["retail", "b2b", "all"],
                    "description": "Type of sales to include",
                    "default": "retail"
                }
            },
            "required": ["period"]
        }
    },
    {
        "name": "get_top_products",
        "description": "Get top selling products by revenue or quantity. Use for questions like 'What are the best sellers?', 'Top products this month?'",
        "input_schema": {
            "type": "object",
            "properties": {
                "period": {
                    "type": "string",
                    "enum": ["today", "yesterday", "week", "last_week", "month", "last_month"],
                    "description": "Time period"
                },
                "by": {
                    "type": "string",
                    "enum": ["revenue", "quantity"],
                    "description": "Sort by revenue or quantity",
                    "default": "revenue"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of products to return",
                    "default": 10
                }
            },
            "required": ["period"]
        }
    },
    {
        "name": "get_source_breakdown",
        "description": "Get sales breakdown by source (Instagram, Telegram, Shopify). Use for questions like 'Which channel sells the most?', 'Instagram vs Shopify sales?'",
        "input_schema": {
            "type": "object",
            "properties": {
                "period": {
                    "type": "string",
                    "enum": ["today", "yesterday", "week", "last_week", "month", "last_month"],
                    "description": "Time period"
                }
            },
            "required": ["period"]
        }
    },
    {
        "name": "compare_periods",
        "description": "Compare revenue between two periods. Use for questions like 'How does this week compare to last week?', 'Month over month growth?'",
        "input_schema": {
            "type": "object",
            "properties": {
                "current_period": {
                    "type": "string",
                    "enum": ["today", "week", "month"],
                    "description": "Current period to compare"
                },
                "previous_period": {
                    "type": "string",
                    "enum": ["yesterday", "last_week", "last_month"],
                    "description": "Previous period to compare against"
                }
            },
            "required": ["current_period", "previous_period"]
        }
    },
    {
        "name": "get_revenue_by_dates",
        "description": "Get revenue summary for a custom date range. Use when user asks about specific dates or non-standard periods like 'first week of January', 'sales from March 1 to March 15', etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {
                    "type": "string",
                    "description": "Start date in YYYY-MM-DD format"
                },
                "end_date": {
                    "type": "string",
                    "description": "End date in YYYY-MM-DD format"
                },
                "sales_type": {
                    "type": "string",
                    "enum": ["retail", "b2b", "all"],
                    "description": "Type of sales to include",
                    "default": "retail"
                }
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "compare_date_ranges",
        "description": "Compare revenue between two custom date ranges. Use for complex comparisons like 'compare this week with first week of last month', 'compare January to February'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "current_start": {
                    "type": "string",
                    "description": "Current period start date (YYYY-MM-DD)"
                },
                "current_end": {
                    "type": "string",
                    "description": "Current period end date (YYYY-MM-DD)"
                },
                "previous_start": {
                    "type": "string",
                    "description": "Previous period start date (YYYY-MM-DD)"
                },
                "previous_end": {
                    "type": "string",
                    "description": "Previous period end date (YYYY-MM-DD)"
                }
            },
            "required": ["current_start", "current_end", "previous_start", "previous_end"]
        }
    },
    {
        "name": "get_customer_insights",
        "description": "Get customer metrics like new vs returning customers, repeat rate. Use for questions about customer behavior.",
        "input_schema": {
            "type": "object",
            "properties": {
                "period": {
                    "type": "string",
                    "enum": ["today", "yesterday", "week", "last_week", "month", "last_month"],
                    "description": "Time period"
                }
            },
            "required": ["period"]
        }
    },
    {
        "name": "search_buyer",
        "description": "Search for a customer/buyer by name, phone, or email. Use for questions like 'Find customer Olena', 'Who is the buyer with phone +380...'",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query (name, phone, or email)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results",
                    "default": 5
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_buyer_details",
        "description": "Get detailed information about a specific buyer including order history. Use after finding a buyer with search_buyer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "buyer_id": {
                    "type": "integer",
                    "description": "Buyer ID"
                }
            },
            "required": ["buyer_id"]
        }
    },
    {
        "name": "search_order",
        "description": "Search for an order by ID or buyer name. Use for questions like 'Show order #12345', 'Find orders by Maria'",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query (order ID or buyer name)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results",
                    "default": 5
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_order_details",
        "description": "Get detailed information about a specific order including products. Use after finding an order.",
        "input_schema": {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "integer",
                    "description": "Order ID"
                }
            },
            "required": ["order_id"]
        }
    },
    {
        "name": "search_product",
        "description": "Search for a product by name, SKU, or brand. Use for questions like 'Find products with serum', 'Show COSRX products'",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query (product name, SKU, or brand)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results",
                    "default": 5
                }
            },
            "required": ["query"]
        }
    },
    # ═══════════════════════════════════════════════════════════════════════════
    # EXPENSE TRACKING TOOLS
    # ═══════════════════════════════════════════════════════════════════════════
    {
        "name": "add_expenses",
        "description": "Add business expenses. Parse natural language input like 'facebook ads 22000, salary 45000' into structured expenses. Categories: marketing (ads), salary, taxes, logistics, other. Use today's date unless specified.",
        "input_schema": {
            "type": "object",
            "properties": {
                "expenses": {
                    "type": "array",
                    "description": "List of expenses to add",
                    "items": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "Expense date in YYYY-MM-DD format (default: today)"
                            },
                            "category": {
                                "type": "string",
                                "enum": ["marketing", "salary", "taxes", "logistics", "other"],
                                "description": "Expense category"
                            },
                            "type": {
                                "type": "string",
                                "description": "Specific type (e.g., 'Facebook Ads', 'Google Ads', 'Salary', 'Nova Poshta')"
                            },
                            "amount": {
                                "type": "number",
                                "description": "Amount in UAH"
                            },
                            "note": {
                                "type": "string",
                                "description": "Optional note"
                            }
                        },
                        "required": ["category", "type", "amount"]
                    }
                }
            },
            "required": ["expenses"]
        }
    },
    {
        "name": "list_expenses",
        "description": "List recent expenses. Use for questions like 'Show my expenses', 'What did I spend this month?'",
        "input_schema": {
            "type": "object",
            "properties": {
                "period": {
                    "type": "string",
                    "enum": ["today", "yesterday", "week", "last_week", "month", "last_month"],
                    "description": "Time period"
                },
                "category": {
                    "type": "string",
                    "enum": ["marketing", "salary", "taxes", "logistics", "other"],
                    "description": "Filter by category"
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results",
                    "default": 20
                }
            }
        }
    },
    {
        "name": "delete_expense",
        "description": "Delete an expense by ID. Use after listing expenses to remove one.",
        "input_schema": {
            "type": "object",
            "properties": {
                "expense_id": {
                    "type": "integer",
                    "description": "ID of the expense to delete"
                }
            },
            "required": ["expense_id"]
        }
    },
    {
        "name": "update_expense",
        "description": "Update an existing expense. Only provided fields will be updated.",
        "input_schema": {
            "type": "object",
            "properties": {
                "expense_id": {
                    "type": "integer",
                    "description": "ID of the expense to update"
                },
                "date": {
                    "type": "string",
                    "description": "New date in YYYY-MM-DD format"
                },
                "category": {
                    "type": "string",
                    "enum": ["marketing", "salary", "taxes", "logistics", "other"],
                    "description": "New category"
                },
                "type": {
                    "type": "string",
                    "description": "New type"
                },
                "amount": {
                    "type": "number",
                    "description": "New amount"
                },
                "note": {
                    "type": "string",
                    "description": "New note"
                }
            },
            "required": ["expense_id"]
        }
    },
    {
        "name": "get_expenses_summary",
        "description": "Get summary of expenses with totals by category. Use for questions like 'How much did I spend on marketing?', 'Total expenses this month?'",
        "input_schema": {
            "type": "object",
            "properties": {
                "period": {
                    "type": "string",
                    "enum": ["today", "yesterday", "week", "last_week", "month", "last_month"],
                    "description": "Time period"
                }
            }
        }
    }
]


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _get_date_range(period: str) -> tuple[date, date]:
    """Convert period string to date range."""
    today = datetime.now(DEFAULT_TZ).date()

    if period == "today":
        return today, today
    elif period == "yesterday":
        yesterday = today - timedelta(days=1)
        return yesterday, yesterday
    elif period == "week":
        start = today - timedelta(days=today.weekday())
        return start, today
    elif period == "last_week":
        end = today - timedelta(days=today.weekday() + 1)
        start = end - timedelta(days=6)
        return start, end
    elif period == "month":
        start = today.replace(day=1)
        return start, today
    elif period == "last_month":
        first_of_month = today.replace(day=1)
        end = first_of_month - timedelta(days=1)
        start = end.replace(day=1)
        return start, end
    else:
        return today, today


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL EXECUTORS
# ═══════════════════════════════════════════════════════════════════════════════

async def execute_tool(name: str, input_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a tool and return results.

    Args:
        name: Tool name
        input_data: Tool input parameters

    Returns:
        Tool result dict
    """
    try:
        if name == "get_revenue_summary":
            return await _get_revenue_summary(
                period=input_data.get("period", "today"),
                sales_type=input_data.get("sales_type", "retail")
            )
        elif name == "get_top_products":
            return await _get_top_products(
                period=input_data.get("period", "today"),
                by=input_data.get("by", "revenue"),
                limit=input_data.get("limit", 10)
            )
        elif name == "get_source_breakdown":
            return await _get_source_breakdown(
                period=input_data.get("period", "today")
            )
        elif name == "compare_periods":
            return await _compare_periods(
                current_period=input_data.get("current_period", "week"),
                previous_period=input_data.get("previous_period", "last_week")
            )
        elif name == "get_revenue_by_dates":
            return await _get_revenue_by_dates(
                start_date=input_data.get("start_date"),
                end_date=input_data.get("end_date"),
                sales_type=input_data.get("sales_type", "retail")
            )
        elif name == "compare_date_ranges":
            return await _compare_date_ranges(
                current_start=input_data.get("current_start"),
                current_end=input_data.get("current_end"),
                previous_start=input_data.get("previous_start"),
                previous_end=input_data.get("previous_end")
            )
        elif name == "get_customer_insights":
            return await _get_customer_insights(
                period=input_data.get("period", "today")
            )
        elif name == "search_buyer":
            return await _search_buyer(
                query=input_data.get("query", ""),
                limit=input_data.get("limit", 5)
            )
        elif name == "get_buyer_details":
            return await _get_buyer_details(
                buyer_id=input_data.get("buyer_id")
            )
        elif name == "search_order":
            return await _search_order(
                query=input_data.get("query", ""),
                limit=input_data.get("limit", 5)
            )
        elif name == "get_order_details":
            return await _get_order_details(
                order_id=input_data.get("order_id")
            )
        elif name == "search_product":
            return await _search_product(
                query=input_data.get("query", ""),
                limit=input_data.get("limit", 5)
            )
        # Expense tools
        elif name == "add_expenses":
            return await _add_expenses(
                expenses=input_data.get("expenses", [])
            )
        elif name == "list_expenses":
            return await _list_expenses(
                period=input_data.get("period"),
                category=input_data.get("category"),
                limit=input_data.get("limit", 20)
            )
        elif name == "delete_expense":
            return await _delete_expense(
                expense_id=input_data.get("expense_id")
            )
        elif name == "update_expense":
            return await _update_expense(
                expense_id=input_data.get("expense_id"),
                expense_date=input_data.get("date"),
                category=input_data.get("category"),
                expense_type=input_data.get("type"),
                amount=input_data.get("amount"),
                note=input_data.get("note")
            )
        elif name == "get_expenses_summary":
            return await _get_expenses_summary(
                period=input_data.get("period")
            )
        else:
            return {"error": f"Unknown tool: {name}"}

    except Exception as e:
        logger.error(f"Tool execution error ({name}): {e}")
        return {"error": str(e)}


async def _get_revenue_summary(period: str, sales_type: str) -> Dict[str, Any]:
    """Get revenue summary for a period."""
    store = await get_store()
    start_date, end_date = _get_date_range(period)

    async with store.connection() as conn:
        sales_filter = ""
        if sales_type == "retail":
            sales_filter = "AND sales_type = 'retail'"
        elif sales_type == "b2b":
            sales_filter = "AND sales_type = 'b2b'"

        result = conn.execute(f"""
            SELECT
                COALESCE(SUM(revenue), 0) as total_revenue,
                COALESCE(SUM(orders_count), 0) as total_orders,
                COALESCE(AVG(avg_order_value), 0) as avg_order_value,
                COALESCE(SUM(returns_revenue), 0) as returns_revenue,
                COALESCE(SUM(returns_count), 0) as returns_count
            FROM gold_daily_revenue
            WHERE date BETWEEN ? AND ?
            {sales_filter}
        """, [start_date, end_date]).fetchone()

        return {
            "period": period,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_revenue": float(result[0] or 0),
            "total_orders": int(result[1] or 0),
            "avg_order_value": float(result[2] or 0),
            "returns_revenue": float(result[3] or 0),
            "returns_count": int(result[4] or 0)
        }


async def _get_top_products(period: str, by: str, limit: int) -> Dict[str, Any]:
    """Get top products by revenue or quantity."""
    store = await get_store()
    start_date, end_date = _get_date_range(period)

    async with store.connection() as conn:
        order_by = "total_revenue DESC" if by == "revenue" else "total_quantity DESC"

        results = conn.execute(f"""
            SELECT
                COALESCE(p.name, op.name) as product_name,
                p.brand,
                SUM(op.quantity) as total_quantity,
                SUM(op.quantity * op.price_sold) as total_revenue
            FROM order_products op
            JOIN silver_orders o ON op.order_id = o.id
            LEFT JOIN products p ON op.product_id = p.id
            WHERE o.order_date BETWEEN ? AND ?
                AND NOT o.is_return
                AND o.sales_type = 'retail'
            GROUP BY COALESCE(p.name, op.name), p.brand
            ORDER BY {order_by}
            LIMIT ?
        """, [start_date, end_date, limit]).fetchall()

        products = []
        for row in results:
            products.append({
                "name": row[0],
                "brand": row[1],
                "quantity": int(row[2] or 0),
                "revenue": float(row[3] or 0)
            })

        return {
            "period": period,
            "sorted_by": by,
            "products": products
        }


async def _get_source_breakdown(period: str) -> Dict[str, Any]:
    """Get sales breakdown by source."""
    store = await get_store()
    start_date, end_date = _get_date_range(period)

    async with store.connection() as conn:
        result = conn.execute("""
            SELECT
                COALESCE(SUM(instagram_revenue), 0) as instagram_revenue,
                COALESCE(SUM(instagram_orders), 0) as instagram_orders,
                COALESCE(SUM(telegram_revenue), 0) as telegram_revenue,
                COALESCE(SUM(telegram_orders), 0) as telegram_orders,
                COALESCE(SUM(shopify_revenue), 0) as shopify_revenue,
                COALESCE(SUM(shopify_orders), 0) as shopify_orders
            FROM gold_daily_revenue
            WHERE date BETWEEN ? AND ?
                AND sales_type = 'retail'
        """, [start_date, end_date]).fetchone()

        return {
            "period": period,
            "sources": [
                {
                    "name": "Instagram",
                    "revenue": float(result[0] or 0),
                    "orders": int(result[1] or 0)
                },
                {
                    "name": "Telegram",
                    "revenue": float(result[2] or 0),
                    "orders": int(result[3] or 0)
                },
                {
                    "name": "Shopify",
                    "revenue": float(result[4] or 0),
                    "orders": int(result[5] or 0)
                }
            ]
        }


async def _compare_periods(current_period: str, previous_period: str) -> Dict[str, Any]:
    """Compare revenue between two periods."""
    current = await _get_revenue_summary(current_period, "retail")
    previous = await _get_revenue_summary(previous_period, "retail")

    current_rev = current["total_revenue"]
    previous_rev = previous["total_revenue"]

    if previous_rev > 0:
        change_pct = ((current_rev - previous_rev) / previous_rev) * 100
    else:
        change_pct = 100 if current_rev > 0 else 0

    return {
        "current_period": current_period,
        "previous_period": previous_period,
        "current": current,
        "previous": previous,
        "revenue_change": current_rev - previous_rev,
        "revenue_change_percent": round(change_pct, 1),
        "orders_change": current["total_orders"] - previous["total_orders"]
    }


async def _get_revenue_by_dates(start_date: str, end_date: str, sales_type: str) -> Dict[str, Any]:
    """Get revenue summary for custom date range."""
    store = await get_store()
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    async with store.connection() as conn:
        sales_filter = ""
        if sales_type == "retail":
            sales_filter = "AND sales_type = 'retail'"
        elif sales_type == "b2b":
            sales_filter = "AND sales_type = 'b2b'"

        result = conn.execute(f"""
            SELECT
                COALESCE(SUM(revenue), 0) as total_revenue,
                COALESCE(SUM(orders_count), 0) as total_orders,
                COALESCE(AVG(avg_order_value), 0) as avg_order_value,
                COALESCE(SUM(returns_revenue), 0) as returns_revenue,
                COALESCE(SUM(returns_count), 0) as returns_count
            FROM gold_daily_revenue
            WHERE date BETWEEN ? AND ?
            {sales_filter}
        """, [start, end]).fetchone()

        return {
            "start_date": start_date,
            "end_date": end_date,
            "total_revenue": float(result[0] or 0),
            "total_orders": int(result[1] or 0),
            "avg_order_value": float(result[2] or 0),
            "returns_revenue": float(result[3] or 0),
            "returns_count": int(result[4] or 0)
        }


async def _compare_date_ranges(
    current_start: str,
    current_end: str,
    previous_start: str,
    previous_end: str
) -> Dict[str, Any]:
    """Compare revenue between two custom date ranges."""
    current = await _get_revenue_by_dates(current_start, current_end, "retail")
    previous = await _get_revenue_by_dates(previous_start, previous_end, "retail")

    current_rev = current["total_revenue"]
    previous_rev = previous["total_revenue"]

    if previous_rev > 0:
        change_pct = ((current_rev - previous_rev) / previous_rev) * 100
    else:
        change_pct = 100 if current_rev > 0 else 0

    return {
        "current_range": {"start": current_start, "end": current_end},
        "previous_range": {"start": previous_start, "end": previous_end},
        "current": current,
        "previous": previous,
        "revenue_change": current_rev - previous_rev,
        "revenue_change_percent": round(change_pct, 1),
        "orders_change": current["total_orders"] - previous["total_orders"]
    }


async def _get_customer_insights(period: str) -> Dict[str, Any]:
    """Get customer insights for a period."""
    store = await get_store()
    start_date, end_date = _get_date_range(period)

    async with store.connection() as conn:
        result = conn.execute("""
            SELECT
                COALESCE(SUM(unique_customers), 0) as total_customers,
                COALESCE(SUM(new_customers), 0) as new_customers,
                COALESCE(SUM(returning_customers), 0) as returning_customers,
                COALESCE(AVG(avg_order_value), 0) as avg_order_value
            FROM gold_daily_revenue
            WHERE date BETWEEN ? AND ?
                AND sales_type = 'retail'
        """, [start_date, end_date]).fetchone()

        total = int(result[0] or 0)
        new_customers = int(result[1] or 0)
        returning = int(result[2] or 0)
        repeat_rate = (returning / total * 100) if total > 0 else 0

        return {
            "period": period,
            "total_customers": total,
            "new_customers": new_customers,
            "returning_customers": returning,
            "repeat_rate": round(repeat_rate, 1),
            "avg_order_value": float(result[3] or 0)
        }


async def _search_buyer(query: str, limit: int) -> Dict[str, Any]:
    """Search for buyers using Meilisearch."""
    meili = get_meili_client()
    results = await meili.search_buyers(query, limit)

    return {
        "query": query,
        "count": len(results),
        "buyers": results
    }


async def _get_buyer_details(buyer_id: int) -> Dict[str, Any]:
    """Get detailed buyer information."""
    from web.services.search_service import get_search_service
    service = get_search_service()
    result = await service.get_buyer_details(buyer_id)

    if not result:
        return {"error": f"Buyer {buyer_id} not found"}
    return result


async def _search_order(query: str, limit: int) -> Dict[str, Any]:
    """Search for orders using Meilisearch."""
    meili = get_meili_client()
    results = await meili.search_orders(query, limit)

    return {
        "query": query,
        "count": len(results),
        "orders": results
    }


async def _get_order_details(order_id: int) -> Dict[str, Any]:
    """Get detailed order information."""
    from web.services.search_service import get_search_service
    service = get_search_service()
    result = await service.get_order_details(order_id)

    if not result:
        return {"error": f"Order {order_id} not found"}
    return result


async def _search_product(query: str, limit: int) -> Dict[str, Any]:
    """Search for products using Meilisearch."""
    meili = get_meili_client()
    results = await meili.search_products(query, limit)

    return {
        "query": query,
        "count": len(results),
        "products": results
    }


# ═══════════════════════════════════════════════════════════════════════════════
# EXPENSE TOOL IMPLEMENTATIONS
# ═══════════════════════════════════════════════════════════════════════════════

async def _add_expenses(expenses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Add multiple expenses."""
    from core.websocket_manager import manager, WebSocketEvent

    store = await get_store()
    added = []
    errors = []

    today = datetime.now(DEFAULT_TZ).date()

    for exp in expenses:
        try:
            # Parse date or use today
            expense_date = today
            if exp.get("date"):
                try:
                    expense_date = date.fromisoformat(exp["date"])
                except ValueError:
                    pass

            result = await store.add_expense(
                expense_date=expense_date,
                category=exp.get("category", "other"),
                expense_type=exp.get("type", "Unknown"),
                amount=float(exp.get("amount", 0)),
                currency="UAH",
                note=exp.get("note")
            )
            added.append(result)
        except Exception as e:
            errors.append({"expense": exp, "error": str(e)})

    # Broadcast to connected dashboard clients
    if added:
        await manager.broadcast(
            "dashboard",
            WebSocketEvent.EXPENSES_UPDATED,
            {"action": "added", "count": len(added)}
        )

    return {
        "added_count": len(added),
        "added": added,
        "errors": errors if errors else None
    }


async def _list_expenses(
    period: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 20
) -> Dict[str, Any]:
    """List expenses with optional filters."""
    store = await get_store()

    start_date = None
    end_date = None

    if period:
        start_date, end_date = _get_date_range(period)

    expenses = await store.list_expenses(
        start_date=start_date,
        end_date=end_date,
        category=category,
        limit=limit
    )

    # Calculate total
    total = sum(exp["amount"] for exp in expenses)

    return {
        "period": period,
        "category": category,
        "count": len(expenses),
        "total": total,
        "expenses": expenses
    }


async def _delete_expense(expense_id: int) -> Dict[str, Any]:
    """Delete an expense."""
    from core.websocket_manager import manager, WebSocketEvent

    store = await get_store()
    success = await store.delete_expense(expense_id)

    if success:
        # Broadcast to connected dashboard clients
        await manager.broadcast(
            "dashboard",
            WebSocketEvent.EXPENSES_UPDATED,
            {"action": "deleted", "expense_id": expense_id}
        )
        return {"success": True, "message": f"Expense {expense_id} deleted"}
    else:
        return {"success": False, "error": f"Expense {expense_id} not found"}


async def _update_expense(
    expense_id: int,
    expense_date: Optional[str] = None,
    category: Optional[str] = None,
    expense_type: Optional[str] = None,
    amount: Optional[float] = None,
    note: Optional[str] = None
) -> Dict[str, Any]:
    """Update an expense."""
    store = await get_store()

    # Parse date if provided
    parsed_date = None
    if expense_date:
        try:
            parsed_date = date.fromisoformat(expense_date)
        except ValueError:
            return {"error": f"Invalid date format: {expense_date}"}

    result = await store.update_expense(
        expense_id=expense_id,
        expense_date=parsed_date,
        category=category,
        expense_type=expense_type,
        amount=amount,
        note=note
    )

    if result:
        return {"success": True, "expense": result}
    else:
        return {"success": False, "error": f"Expense {expense_id} not found"}


async def _get_expenses_summary(period: Optional[str] = None) -> Dict[str, Any]:
    """Get expenses summary."""
    store = await get_store()

    start_date = None
    end_date = None

    if period:
        start_date, end_date = _get_date_range(period)

    summary = await store.get_expenses_summary(
        start_date=start_date,
        end_date=end_date
    )

    return {
        "period": period,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        **summary
    }
