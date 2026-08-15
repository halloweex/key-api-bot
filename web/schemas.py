"""
Pydantic response models for API endpoints.

Provides type-safe response models with automatic validation and documentation.
"""
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


class DuckDBStats(BaseModel):
    """DuckDB statistics."""
    status: str
    latency_ms: Optional[float] = None
    orders: Optional[int] = None
    products: Optional[int] = None
    categories: Optional[int] = None
    managers: Optional[int] = None
    db_size_mb: Optional[float] = None


class DataQualityFreshness(BaseModel):
    """When a data-quality layer last produced a verdict.

    "Last success" means a run that finished without an error — a failed run
    writes a row too, so row-existence alone would have read green through
    57 consecutive reconciliation failures.
    """
    last_success_at: Optional[str] = Field(None, description="ISO timestamp of the last successful run")
    age_seconds: Optional[int] = Field(None, description="Seconds since that run; null means never succeeded")


class SyncStatus(BaseModel):
    """Background sync service status."""
    status: str = Field(description="Sync status: active, idle, or error")
    last_sync_time: Optional[str] = Field(None, description="Last sync time (ISO format)")
    seconds_since_sync: Optional[int] = Field(None, description="Seconds since last sync")
    consecutive_empty_syncs: int = Field(0, description="Number of syncs with no new orders")
    current_backoff_seconds: int = Field(300, description="Current sync interval in seconds")
    is_off_hours: bool = Field(False, description="Whether in off-hours mode (2-8 AM)")


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(description="Service status: healthy or degraded")
    version: str = Field(description="Application version")
    uptime_seconds: int = Field(description="Uptime in seconds")
    correlation_id: Optional[str] = Field(None, description="Request correlation ID")
    sync_mode: str = Field("legacy", description="Sync pipeline mode: legacy or staging")
    duckdb: DuckDBStats
    sync: Optional[SyncStatus] = Field(None, description="Background sync service status")
    data_quality: Optional[Dict[str, DataQualityFreshness]] = Field(
        None, description="Per-layer age of the last successful data-quality run"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY STATS
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


# ═══════════════════════════════════════════════════════════════════════════════
# CHAT & SEARCH
# ═══════════════════════════════════════════════════════════════════════════════

