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
        "Declared for the tables Postgres derives on its own signal and for "
        "silver.order_utm, which it parses, so the watchdog's limits are "
        "web's and not kept in a second container.",
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
    status: str = Field(
        description=(
            "Service status: healthy or degraded. Healthy since step 14's "
            "S14-17, which took the DuckDB block and the DuckDB schema ledger "
            "out, the only things that degraded it; kept for the canary, "
            "Docker's healthcheck and the deploy's gate."
        ),
    )
    degraded_by: List[str] = Field(
        default_factory=list,
        description=(
            "Why `status` is degraded, one word per cause. Empty since step "
            "14's S14-17: its three causes — `duckdb`, `migrations`, "
            "`duckdb_fatal` — were DuckDB's."
        ),
    )
    version: str = Field(description="Application version")
    uptime_seconds: int = Field(description="Uptime in seconds")
    correlation_id: Optional[str] = Field(None, description="Request correlation ID")
    sync: Optional[SyncStatus] = Field(None, description="Background sync service status")
    data_quality: Optional[Dict[str, DataQualityFreshness]] = Field(
        None, description="Per-layer age of the last successful data-quality run"
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
    buyer_sync: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "The buyers step of the incremental sync, in this web process: "
            "seconds since the last attempt and the last success (floored at "
            "the process start when it has not succeeded yet, with `ever_ok` "
            "saying which), consecutive failures that are neither KeyCRM nor "
            "data errors, the CLASS of the last error — never its text, which "
            "can carry a buyer's data — the seconds left before a failed "
            "step retries, and how many buyers of the last batch Postgres "
            "would refuse and chain 4 skipped by id (`last_skipped_bad`). "
            "Also `watermark_age_s`, the age of the stamp the step writes to "
            "Postgres, which survives a restart. Null "
            "when this process has not started syncing. Judged by the canary "
            "as `buyer_sync_stalled` (WARN), and — chain 4 making this step "
            "the only writer of buyers — as `buyer_sync_stalled_chain` "
            "(CRITICAL)."
        ),
    )
    write_chains: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "One entry per registered write chain, `{chain: {sync_step?, "
            "pending?}}`. Every chain writes Postgres; since step 14's S14-11 "
            "no KS_WRITE_* is read, and the entries carry no `env`, `mode`, "
            "`latched`, `latched_at`, `mismatch`, `unmet_precondition` or "
            "`preflight` any more. `sync_step` is what the chain's half of the "
            "sync tick last did, where a Postgres failure is recorded instead "
            "of ending the tick: chain 1's (`pg_inventory_write`) offers and "
            "stocks step — consecutive failures, the last one's step and "
            "error class, and when the next attempt is allowed —, chain 3's "
            "(`pg_orders_write`) order step — failures in a row, ages, the "
            "last error's class and how many orders Postgres would refuse, "
            "judged by the canary as `orders_sync_failing` —, chain 5's "
            "managers step and chain 6's hourly products step. A report "
            "ledger's entry carries `pending`, the delivered weeks whose "
            "ledger row is still spooled (`unreadable`: the files no drain "
            "will land, whose row a human writes), judged as "
            "`report_ledger_pending`."
        ),
    )
    backups: Optional[Dict[str, Optional[float]]] = Field(
        None,
        description=(
            "Hours since the host's backup scripts last proved themselves, "
            "read from the markers they write on success only: "
            "`pitr_drill_age_h` (the weekly PITR drill), "
            "`remote_restore_age_h` (the off-site copy restored) and "
            "`pg_offsite_age_h` (a Postgres dump shipped off the host). Null "
            "where no marker exists. Judged by the canary as "
            "`backup_evidence_stale` (chain 3's flip waited on all three "
            "until step 14). Ages only — no path, host or account."
        ),
    )
    derivation: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "The Postgres derivation's own signal (`meta.derivation_signal`): "
            "`marks_dropped_unhealed` counts the derivation marks dropped "
            "since a validated rebuild last covered them, and "
            "`last_mark_drop_age_s` is how long ago the latest was dropped "
            "while that count is above zero — both null when Postgres cannot "
            "be read. `signal_unreadable_ticks` counts the consecutive ticks "
            "of this process that could not read the signal; web pages on the "
            "fifth itself. `mode` and `error` (KS_PG_DERIVE) went in step 14's "
            "S14-15."
        ),
    )
    read_fallback_mode: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "`refused` is `{surface: {count, last_at}}` for the reads refused "
            "since the process started: the engine a router reads failed, and "
            "an HTTP route answered 503 naming the surface (DN-20b). Judged by "
            "the canary as `read_refused` while one is recent. Until step "
            "14's S14-16 the block also carried `mode`, `error`, "
            "`misconfigured` and `no_engine`, and `read_fallbacks` stood "
            "beside it — the reads answered from DuckDB, none since S14-00."
        ),
    )
    # `duckdb_switch` (KS_DUCKDB as run, and every open of the DuckDB file
    # refused under off) stood here until step 14's S14-18 deleted the switch.
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

