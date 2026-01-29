"""
Pydantic response models for API endpoints.

Provides type-safe response models with automatic validation and documentation.
"""
from datetime import date, datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


# ═══════════════════════════════════════════════════════════════════════════════
# COMMON MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class CategoryResponse(BaseModel):
    """Category data."""
    id: int
    name: str


class BrandResponse(BaseModel):
    """Brand data."""
    name: str


# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════════

class DuckDBStats(BaseModel):
    """DuckDB statistics."""
    status: str
    latency_ms: Optional[float] = None
    orders: Optional[int] = None
    products: Optional[int] = None
    categories: Optional[int] = None
    managers: Optional[int] = None
    db_size_mb: Optional[float] = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(description="Service status: healthy or degraded")
    version: str = Field(description="Application version")
    uptime_seconds: int = Field(description="Uptime in seconds")
    correlation_id: Optional[str] = Field(None, description="Request correlation ID")
    duckdb: DuckDBStats


# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY STATS
# ═══════════════════════════════════════════════════════════════════════════════

class SummaryStatsResponse(BaseModel):
    """Summary statistics for a period."""
    totalOrders: int = Field(description="Total number of orders")
    totalRevenue: float = Field(description="Total revenue in UAH")
    avgCheck: float = Field(description="Average order value in UAH")
    totalReturns: int = Field(description="Number of returned orders")
    returnsRevenue: float = Field(description="Revenue from returns in UAH")
    startDate: str = Field(description="Period start date (ISO format)")
    endDate: str = Field(description="Period end date (ISO format)")


# ═══════════════════════════════════════════════════════════════════════════════
# REVENUE TREND
# ═══════════════════════════════════════════════════════════════════════════════

class ChartDataset(BaseModel):
    """Chart.js dataset."""
    label: str
    data: List[float]
    borderColor: str
    backgroundColor: str
    fill: bool = True
    tension: float = 0.3
    borderWidth: int = 2
    borderDash: Optional[List[int]] = None


class ComparisonData(BaseModel):
    """Previous period comparison data."""
    labels: List[str]
    revenue: List[float]
    orders: List[int] = []


class RevenueTrendResponse(BaseModel):
    """Revenue trend data for charts."""
    labels: List[str] = Field(description="Date labels (DD.MM format)")
    revenue: List[float] = Field(description="Daily revenue values")
    orders: List[int] = Field(description="Daily order counts")
    datasets: List[ChartDataset] = Field(description="Chart.js compatible datasets")
    comparison: Optional[ComparisonData] = Field(None, description="Previous period comparison")


# ═══════════════════════════════════════════════════════════════════════════════
# SALES BY SOURCE
# ═══════════════════════════════════════════════════════════════════════════════

class SalesBySourceResponse(BaseModel):
    """Sales breakdown by source (Instagram, Telegram, Shopify)."""
    labels: List[str] = Field(description="Source names")
    orders: List[int] = Field(description="Order counts per source")
    revenue: List[float] = Field(description="Revenue per source")
    backgroundColor: List[str] = Field(description="Colors for each source")


# ═══════════════════════════════════════════════════════════════════════════════
# TOP PRODUCTS
# ═══════════════════════════════════════════════════════════════════════════════

class TopProductsResponse(BaseModel):
    """Top products by quantity."""
    labels: List[str] = Field(description="Product names")
    data: List[int] = Field(description="Quantity sold")
    percentages: List[float] = Field(description="Percentage of total")
    backgroundColor: str = Field(description="Chart color")


# ═══════════════════════════════════════════════════════════════════════════════
# BRAND ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════

class BrandAnalyticsItem(BaseModel):
    """Brand performance data."""
    name: str
    revenue: float
    quantity: int
    orders: int


class BrandAnalyticsResponse(BaseModel):
    """Brand analytics response."""
    brands: List[BrandAnalyticsItem]


# ═══════════════════════════════════════════════════════════════════════════════
# CUSTOMER INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════

class NewVsReturning(BaseModel):
    """New vs returning customer breakdown."""
    labels: List[str]
    data: List[int]
    backgroundColor: List[str]


class AOVTrendDataset(BaseModel):
    """AOV trend chart dataset."""
    label: str
    data: List[float]
    borderColor: str
    backgroundColor: str
    fill: bool = True
    tension: float = 0.3


class AOVTrend(BaseModel):
    """Average order value trend."""
    labels: List[str]
    datasets: List[AOVTrendDataset]


class CustomerMetrics(BaseModel):
    """Customer metrics."""
    totalCustomers: int
    newCustomers: int
    returningCustomers: int
    totalOrders: int
    repeatRate: float
    averageOrderValue: float
    customerLifetimeValue: float = 0
    avgPurchaseFrequency: float = 0
    avgCustomerLifespanDays: float = 0
    purchaseFrequency: float = 0
    totalCustomersAllTime: int = 0
    repeatCustomersAllTime: int = 0
    trueRepeatRate: float = 0
    avgOrdersPerCustomer: float = 0


class CustomerInsightsResponse(BaseModel):
    """Customer insights response."""
    newVsReturning: NewVsReturning
    aovTrend: AOVTrend
    metrics: CustomerMetrics


# ═══════════════════════════════════════════════════════════════════════════════
# INVENTORY
# ═══════════════════════════════════════════════════════════════════════════════

class InventorySummary(BaseModel):
    """Inventory summary."""
    sku_count: int
    units: int
    value: float


class InventoryV2Response(BaseModel):
    """Inventory summary v2."""
    active_stock: InventorySummary
    dead_stock: InventorySummary
    low_stock_alerts: int


class DeadStockItem(BaseModel):
    """Dead stock item with recommendation."""
    offer_id: int
    sku: str
    name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    quantity: int
    stock_value: float
    days_since_sale: int
    recommended_action: str
    potential_loss: float


class RestockAlert(BaseModel):
    """Low stock alert."""
    offer_id: int
    sku: str
    name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    quantity: int
    reserve: int
    available: int
    days_since_sale: int
    alert_level: str


# ═══════════════════════════════════════════════════════════════════════════════
# GOALS
# ═══════════════════════════════════════════════════════════════════════════════

class GoalInfo(BaseModel):
    """Goal information."""
    goal: float
    is_custom: bool
    calculated_goal: Optional[float] = None
    growth_factor: float = 1.1
    adjusted_goal: Optional[float] = None
    seasonality_index: Optional[float] = None
    seasonality_confidence: Optional[str] = None


class GoalsResponse(BaseModel):
    """Goals response."""
    daily: GoalInfo
    weekly: GoalInfo
    monthly: GoalInfo


# ═══════════════════════════════════════════════════════════════════════════════
# METRICS
# ═══════════════════════════════════════════════════════════════════════════════

class TimingStats(BaseModel):
    """Timing statistics for an operation."""
    count: int
    avg_ms: float
    min_ms: float
    max_ms: float
    p50_ms: Optional[float] = None
    p95_ms: Optional[float] = None


class MetricsResponse(BaseModel):
    """Application metrics response."""
    uptime_seconds: int
    correlation_id: Optional[str] = None
    requests: Dict[str, int] = Field(default_factory=dict)
    errors: Dict[str, int] = Field(default_factory=dict)
    timing: Dict[str, TimingStats] = Field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════════════════
# BACKGROUND JOBS
# ═══════════════════════════════════════════════════════════════════════════════

class JobInfo(BaseModel):
    """Background job information."""
    id: str = Field(description="Unique job identifier")
    name: str = Field(description="Human-readable job name")
    description: str = Field(description="Job description")
    trigger: str = Field(description="Trigger type and schedule")
    next_run: Optional[str] = Field(None, description="Next scheduled run (ISO format)")
    last_run: Optional[str] = Field(None, description="Last run time (ISO format)")
    last_status: Optional[str] = Field(None, description="Last run status: success/failure")
    last_duration_ms: Optional[float] = Field(None, description="Last run duration in ms")


class JobHistoryEntry(BaseModel):
    """Job execution history entry."""
    job_id: str
    job_name: str
    started_at: str = Field(description="Start time (ISO format)")
    completed_at: Optional[str] = Field(None, description="Completion time (ISO format)")
    duration_ms: Optional[float] = None
    status: str = Field(description="Execution status: success/failure/running")
    error: Optional[str] = Field(None, description="Error message if failed")
    result: Optional[Dict[str, Any]] = Field(None, description="Job result data")


class JobsResponse(BaseModel):
    """Background jobs status response."""
    status: str = Field(description="Scheduler status: running/not_running")
    jobs: List[JobInfo] = Field(default_factory=list, description="Registered jobs")
    history: List[JobHistoryEntry] = Field(default_factory=list, description="Recent execution history")
