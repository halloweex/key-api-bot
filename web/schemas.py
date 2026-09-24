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


class MirrorFreshness(BaseModel):
    """When a mirrored table last shipped successfully, and whether it is failing.

    Both are needed. The watermark only moves when there was something to ship,
    so age alone cannot separate a quiet night from a dead mirror; and a mirror
    that is switched off never raises, so a failure count alone cannot see it.
    """
    last_ok_at: Optional[str] = Field(None, description="ISO timestamp of the last successful shipment")
    age_seconds: Optional[int] = Field(None, description="Seconds since that shipment; null means never shipped")
    failures_since_ok: Optional[int] = Field(None, description="Consecutive failed attempts since the last success")
    failing: bool = Field(False, description="Whether the last attempt recorded an error")
    max_age_s: Optional[int] = Field(
        None,
        description="The age limit this table is judged by, when web declares one. "
        "Declared for the tables Postgres derives on its own signal, so the "
        "watchdog's list switches with KS_PG_DERIVE instead of being kept in "
        "a second container.",
    )


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
    duckdb: DuckDBStats
    sync: Optional[SyncStatus] = Field(None, description="Background sync service status")
    data_quality: Optional[Dict[str, DataQualityFreshness]] = Field(
        None, description="Per-layer age of the last successful data-quality run"
    )
    migrations: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Schema ledger as of connect: how many steps are applied, which are "
            "pending, and which failed. Named `migrations` rather than `schema` "
            "because Pydantic already owns that word."
        ),
    )
    alerting: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "The alerting machinery's own health: consecutive transport "
            "failures in this process and the last successful delivery. "
            "Judged by the canary — the one subsystem that had no dead-man's "
            "switch on itself."
        ),
    )
    write_chains: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Each stage-4 write chain: its KS_WRITE_* variable, the mode it "
            "writes (duckdb or postgres) and, when the value is not understood, "
            "the error — that chain's writers raise and its tables are neither "
            "shipped nor compared until it is corrected. `latched` and "
            "`latched_at` say the chain has already written Postgres and now "
            "routes there whatever the variable says (DN-06); `mismatch` is "
            "that state against a variable which disagrees — a rollback "
            "somebody believes happened has not, and only "
            "scripts/chain_copy_back.py undoes it. `unmet_precondition` names "
            "a chain's own condition for moving that does not hold (chain 6a: "
            "KS_READ_EXPENSES=postgres) — an unlatched chain then writes duckdb "
            "whatever its variable says, and a latched one writes Postgres "
            "while its readers read DuckDB."
        ),
    )
    derivation: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Who derives Postgres' Silver and Gold: `mode` is piggyback or own, "
            "and `error` names a KS_PG_DERIVE value that was not understood and "
            "fell back to piggyback. Judged by the canary, because a typo "
            "otherwise leaves one log line and an operator believing the own "
            "derivation is running. `marks_dropped_unhealed` counts the "
            "derivation marks dropped since a validated rebuild last covered "
            "them, and `last_mark_drop_age_s` is how long ago the latest was "
            "dropped while that count is above zero — both null under "
            "piggyback or when Postgres cannot be read. "
            "`signal_unreadable_ticks` counts the consecutive ticks of this "
            "process that could not read the derivation's signal; web pages on "
            "the fifth itself. Null under piggyback."
        ),
    )
    read_fallbacks: Optional[Dict[str, Dict[str, Any]]] = Field(
        None,
        description=(
            "Reads this process answered from DuckDB because Postgres (or, for "
            "cohorts, ClickHouse) failed, per surface: `count` since the "
            "process started and `last_at`. Empty is none. Counted by "
            "`core.read_fallback.fall_back`, which every such site calls; the "
            "log line beside each one carries the error (DN-20a)."
        ),
    )
    read_fallback_mode: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "`mode` is KS_READ_FALLBACK as understood at start — duckdb or "
            "off, and off is not enforced before DN-20b — and `error` names a "
            "value that was not understood and ran as duckdb; judged by the "
            "canary. `misconfigured` lists every KS_READ_* naming an engine "
            "without its address (KS_PG_DSN, KS_CH_URL): each of those reads "
            "is served by DuckDB with nothing failing to count."
        ),
    )
    mirrors: Optional[Dict[str, MirrorFreshness]] = Field(
        None,
        description=(
            "Per-table freshness of the Postgres copy of landing. Null — not an "
            "empty object — when it could not be read, so a watchdog can tell "
            "'nothing is watching' from 'nothing is wrong'."
        ),
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

