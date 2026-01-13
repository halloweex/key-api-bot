"""
Domain models for KeyCRM data.

Provides type-safe dataclasses for Orders, Products, Buyers, etc.
These models serve as the single source of truth for data structures
used across both bot and web services.
"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from typing import Optional, List, Dict, Any


# ═══════════════════════════════════════════════════════════════════════════════
# ENUMS
# ═══════════════════════════════════════════════════════════════════════════════

class SourceId(IntEnum):
    """Sales source identifiers from KeyCRM."""
    INSTAGRAM = 1
    TELEGRAM = 2
    OPENCART = 3  # Deprecated, not used
    SHOPIFY = 4

    @classmethod
    def active_sources(cls) -> List["SourceId"]:
        """Get list of active sources (excluding deprecated)."""
        return [cls.INSTAGRAM, cls.TELEGRAM, cls.SHOPIFY]

    @property
    def display_name(self) -> str:
        """Human-readable source name."""
        names = {
            self.INSTAGRAM: "Instagram",
            self.TELEGRAM: "Telegram",
            self.OPENCART: "Opencart",
            self.SHOPIFY: "Shopify",
        }
        return names.get(self, f"Source {self.value}")

    @property
    def emoji(self) -> str:
        """Emoji for this source."""
        emojis = {
            self.INSTAGRAM: "📸",
            self.TELEGRAM: "✈️",
            self.OPENCART: "🌐",
            self.SHOPIFY: "🛍️",
        }
        return emojis.get(self, "📦")

    @property
    def color(self) -> str:
        """Chart color for this source."""
        colors = {
            self.INSTAGRAM: "#7C3AED",  # Purple
            self.TELEGRAM: "#2563EB",   # Blue
            self.OPENCART: "#F59E0B",   # Orange
            self.SHOPIFY: "#eb4200",    # Orange-red
        }
        return colors.get(self, "#999999")


class OrderStatus(IntEnum):
    """Order status IDs from KeyCRM."""
    # Return/Cancel statuses
    RETURNED = 19
    CANCELED = 21
    REFUNDED = 22
    REJECTED = 23

    @classmethod
    def return_statuses(cls) -> set:
        """Get set of return/cancel status IDs."""
        return {cls.RETURNED, cls.CANCELED, cls.REFUNDED, cls.REJECTED}


# ═══════════════════════════════════════════════════════════════════════════════
# DATACLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Buyer:
    """Customer/buyer from KeyCRM."""
    id: int
    created_at: Optional[datetime] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None

    @classmethod
    def from_api(cls, data: Optional[Dict[str, Any]]) -> Optional["Buyer"]:
        """Create Buyer from KeyCRM API response."""
        if not data:
            return None

        created_at = None
        if data.get("created_at"):
            try:
                created_at = datetime.fromisoformat(
                    data["created_at"].replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                pass

        return cls(
            id=data.get("id", 0),
            created_at=created_at,
            phone=data.get("phone"),
            email=data.get("email"),
            first_name=data.get("first_name"),
            last_name=data.get("last_name"),
        )

    @property
    def full_name(self) -> str:
        """Get buyer's full name."""
        parts = [self.first_name, self.last_name]
        return " ".join(p for p in parts if p) or "Unknown"

    def is_returning(self, period_start: datetime) -> bool:
        """Check if buyer existed before the given period start."""
        if not self.created_at:
            return False
        return self.created_at < period_start


@dataclass
class Manager:
    """Sales manager from KeyCRM."""
    id: int
    name: Optional[str] = None

    @classmethod
    def from_api(cls, data: Optional[Dict[str, Any]]) -> Optional["Manager"]:
        """Create Manager from KeyCRM API response."""
        if not data:
            return None
        return cls(
            id=data.get("id", 0),
            name=data.get("name"),
        )


@dataclass
class ProductOffer:
    """Product offer details (variant) from KeyCRM."""
    product_id: int
    sku: Optional[str] = None

    @classmethod
    def from_api(cls, data: Optional[Dict[str, Any]]) -> Optional["ProductOffer"]:
        """Create ProductOffer from KeyCRM API response."""
        if not data:
            return None
        return cls(
            product_id=data.get("product_id", 0),
            sku=data.get("sku"),
        )


@dataclass
class OrderProduct:
    """Product line item within an order."""
    name: str
    quantity: int
    price_sold: float
    offer: Optional[ProductOffer] = None

    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "OrderProduct":
        """Create OrderProduct from KeyCRM API response."""
        return cls(
            name=data.get("name", "Unknown"),
            quantity=int(data.get("quantity", 1)),
            price_sold=float(data.get("price_sold", 0)),
            offer=ProductOffer.from_api(data.get("offer")),
        )

    @property
    def product_id(self) -> Optional[int]:
        """Get product ID from offer."""
        return self.offer.product_id if self.offer else None

    @property
    def total(self) -> float:
        """Calculate line total."""
        return self.price_sold * self.quantity


@dataclass
class Order:
    """Order from KeyCRM."""
    id: int
    source_id: int
    status_id: int
    grand_total: float
    ordered_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    buyer: Optional[Buyer] = None
    manager: Optional[Manager] = None
    products: List[OrderProduct] = field(default_factory=list)

    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "Order":
        """Create Order from KeyCRM API response."""
        ordered_at = None
        if data.get("ordered_at"):
            try:
                ordered_at = datetime.fromisoformat(
                    data["ordered_at"].replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                pass

        created_at = None
        if data.get("created_at"):
            try:
                created_at = datetime.fromisoformat(
                    data["created_at"].replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                pass

        # Parse status_id from either direct field or nested status object
        status_id = data.get("status_id")
        if status_id is None and data.get("status"):
            status_id = data["status"].get("id")

        products = [
            OrderProduct.from_api(p)
            for p in data.get("products", [])
        ]

        return cls(
            id=data.get("id", 0),
            source_id=data.get("source_id", 0),
            status_id=status_id or 0,
            grand_total=float(data.get("grand_total", 0)),
            ordered_at=ordered_at,
            created_at=created_at,
            buyer=Buyer.from_api(data.get("buyer")),
            manager=Manager.from_api(data.get("manager")),
            products=products,
        )

    @property
    def source(self) -> Optional[SourceId]:
        """Get source as enum."""
        try:
            return SourceId(self.source_id)
        except ValueError:
            return None

    @property
    def is_return(self) -> bool:
        """Check if order is returned/canceled."""
        return self.status_id in OrderStatus.return_statuses()

    def is_within_period(self, start: datetime, end: datetime) -> bool:
        """Check if order falls within the given period."""
        if not self.ordered_at:
            return False
        return start <= self.ordered_at <= end

    def matches_manager(self, manager_ids: List[str]) -> bool:
        """Check if order's manager ID is in the allowed list."""
        if not self.manager:
            return False
        return str(self.manager.id) in manager_ids


@dataclass
class Product:
    """Product from KeyCRM catalog."""
    id: int
    name: str
    category_id: Optional[int] = None
    brand: Optional[str] = None
    sku: Optional[str] = None
    price: Optional[float] = None

    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "Product":
        """Create Product from KeyCRM API response."""
        # Extract brand from custom_fields if present
        brand = None
        custom_fields = data.get("custom_fields", [])
        if isinstance(custom_fields, list):
            for cf in custom_fields:
                if cf.get("name") == "Brand" or cf.get("uuid") == "CT_1001":
                    values = cf.get("value", [])
                    if values and isinstance(values, list):
                        brand = values[0]
                    break

        return cls(
            id=data.get("id", 0),
            name=data.get("name", "Unknown"),
            category_id=data.get("category_id"),
            brand=brand,
            sku=data.get("sku"),
            price=float(data.get("price", 0)) if data.get("price") else None,
        )


@dataclass
class Category:
    """Product category from KeyCRM."""
    id: int
    name: str
    parent_id: Optional[int] = None

    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "Category":
        """Create Category from KeyCRM API response."""
        return cls(
            id=data.get("id", 0),
            name=data.get("name", "Unknown"),
            parent_id=data.get("parent_id"),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# AGGREGATION RESULTS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SalesSourceData:
    """Aggregated sales data for a single source."""
    source_id: int
    name: str
    orders: int
    revenue: float
    color: str

    @classmethod
    def from_source(cls, source: SourceId, orders: int, revenue: float) -> "SalesSourceData":
        """Create from SourceId enum."""
        return cls(
            source_id=source.value,
            name=source.display_name,
            orders=orders,
            revenue=round(revenue, 2),
            color=source.color,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "source_id": self.source_id,
            "name": self.name,
            "orders": self.orders,
            "revenue": self.revenue,
            "color": self.color,
        }


@dataclass
class TopProduct:
    """Product in top products list."""
    name: str
    quantity: int
    revenue: float
    percentage: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "name": self.name,
            "quantity": self.quantity,
            "revenue": round(self.revenue, 2),
            "percentage": round(self.percentage, 1),
        }


@dataclass
class SummaryStats:
    """Summary statistics for a period."""
    total_orders: int
    total_revenue: float
    avg_check: float
    returns_count: int
    returns_revenue: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "total_orders": self.total_orders,
            "total_revenue": round(self.total_revenue, 2),
            "avg_check": round(self.avg_check, 2),
            "returns_count": self.returns_count,
            "returns_revenue": round(self.returns_revenue, 2),
        }


@dataclass
class CustomerInsights:
    """Customer analytics for a period."""
    new_customers: int
    returning_customers: int
    repeat_rate: float
    aov_trend: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "new_customers": self.new_customers,
            "returning_customers": self.returning_customers,
            "repeat_rate": round(self.repeat_rate, 1),
            "aov_trend": self.aov_trend,
        }
