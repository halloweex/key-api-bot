"""
Background job scheduler using APScheduler.

Manages all background tasks:
- Incremental sync (every 60 seconds)
- Full sync (weekly on Sunday at 2 AM)
- Inventory snapshot (daily at 1 AM)

There is no cache-warming job. This docstring claimed one for months and no
`_add_job` ever matched it, which is how an audit came to rate a missing cache
as the standing cause of dashboard latency. Do not re-add the line without the
job.

Features:
- Job retry with exponential backoff
- Job execution history
- Prevents job pile-up (max_instances=1)
- Graceful shutdown
"""
import asyncio
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from enum import Enum
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import (
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
    EVENT_JOB_MISSED,
    JobExecutionEvent,
)

from core.observability import get_logger, correlation_context

logger = get_logger(__name__)

# Timezone for scheduling
SCHEDULER_TIMEZONE = ZoneInfo("Europe/Kyiv")

# APScheduler drops a job as MISSED when it cannot be dispatched within this many
# seconds of its scheduled instant. The default is ONE SECOND, and it was never
# overridden: production logged 108 missed bronze_promotion runs in five hours,
# and the 05:00 reconciliation vanished every Sunday because the weekly full sync
# was still holding the heavy-job lock at the moment it was due. Nothing here is
# time-critical to the second — an invariant check running forty minutes late is
# worth infinitely more than one that never runs.
DEFAULT_MISFIRE_GRACE_SECONDS = 3600

# Six-hourly jobs used IntervalTrigger, whose next fire is computed from
# registration. _add_job re-registers on every scheduler start with
# replace_existing=True, so each deploy pushed the next run another six hours
# out — the integrity scan ran 6 times in 79 days. CronTrigger computes from the
# wall clock and is immune. Hours avoid the 03:00-04:00 DST window; do not move
# these to hour 3 or 4.
INVARIANT_CHECK_HOURS = "1,7,13,19"

# Checks whose scheduled instant we may simply not have been alive for, and how
# stale their last *successful* verdict may be at process start before we run a
# one-off catch-up. Shape: job_id -> (data_quality layer, max age s, delay s).
#
# A CronTrigger computes its next fire from the moment of registration, so a
# process that starts at 02:00:51 for a job due at 02:00:00 sets the next run a
# full day out. The job is not late — it does not exist when it is due, and
# misfire_grace_time has nothing to forgive. That is not a rare event: the host
# cron `0 2 * * 0 weekly_compact.sh` stops the containers at exactly 02:00 UTC
# every Sunday, and any deploy landing on a cron instant does the same.
#
# The delay keeps the catch-up out of the startup rush (initial sync, model
# training, first warehouse refresh) and staggers the two checks.
CATCHUP_CHECKS = {
    "dq_reconciliation": ("reconciliation", 26 * 3600, 300),
    "dq_integrity_check": ("integrity", 8 * 3600, 120),
}

# The inventory snapshot gets its own catch-up rather than a row above, because
# its liveness signal is not a data_quality layer age — it is whether
# inventory_sku_history has a row for today. See _schedule_catchup_runs.
# Staggered after the two checks there so the startup rush stays spread out.
INVENTORY_CATCHUP_DELAY_S = 420

# Which book the weekly report reads from. Retail is the business; b2b is one
# manager's nine orders a week, where a percentage move says nothing, and
# `internal` is not sales at all — staff orders and shipments to bloggers that
# carry line items and no money.
WEEKLY_REPORT_SALES_TYPE = "retail"


class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    MISSED = "missed"


@dataclass
class JobExecution:
    """Record of a job execution."""
    job_id: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: JobStatus = JobStatus.RUNNING
    duration_ms: Optional[float] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


@dataclass
class JobInfo:
    """Information about a scheduled job."""
    id: str
    name: str
    description: str
    trigger: str = ""  # Human-readable trigger description
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    last_status: Optional[JobStatus] = None
    last_duration_ms: Optional[float] = None
    run_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None


class BackgroundScheduler:
    """
    Background job scheduler with monitoring.

    Usage:
        scheduler = BackgroundScheduler()
        await scheduler.start()

        # Later...
        scheduler.shutdown()
    """

    # Jobs that consume significant DuckDB/Python memory.
    # Only one heavy job runs at a time to prevent compounding OOM.
    _HEAVY_JOBS = frozenset({
        "incremental_sync", "full_sync_weekly", "order_status_refresh",
        "revenue_prediction_train", "meilisearch_sync", "warehouse_refresh",
        "bronze_promotion",
    })

    def __init__(self):
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._job_history: Dict[str, List[JobExecution]] = {}
        self._job_info: Dict[str, JobInfo] = {}
        self._max_history = 50  # Keep last N executions per job
        self._started = False
        # Protects _job_info and _job_history: APScheduler event listeners
        # are called from its internal thread, not the asyncio event loop.
        self._state_lock = threading.Lock()
        # Serializes memory-heavy jobs to prevent OOM from compounding
        self._heavy_job_lock = asyncio.Lock()

    async def start(self) -> None:
        """Start the scheduler and register all jobs."""
        if self._started:
            logger.warning("Scheduler already started")
            return

        self._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)

        # Add job event listeners
        self._scheduler.add_listener(
            self._on_job_executed,
            EVENT_JOB_EXECUTED
        )
        self._scheduler.add_listener(
            self._on_job_error,
            EVENT_JOB_ERROR
        )
        self._scheduler.add_listener(
            self._on_job_missed,
            EVENT_JOB_MISSED
        )

        # Register jobs
        await self._register_jobs()

        # Start scheduler
        self._scheduler.start()
        self._started = True
        logger.info("Background scheduler started")

        await self._schedule_catchup_runs()

    async def _schedule_catchup_runs(self) -> None:
        """Queue a one-off run for any check whose due instant we missed.

        Only the *successful* runs count — a row written on the error path is
        not a verdict — so this reads the same freshness the canary judges on.
        One attempt per process start, never on a schedule of its own: if the
        check is broken rather than skipped, this costs one extra attempt per
        restart and no more.

        Best-effort throughout. A watchdog that can stop the scheduler from
        starting is worse than no watchdog.
        """
        from datetime import datetime, timedelta, timezone

        from apscheduler.triggers.date import DateTrigger

        try:
            from core.data_quality import fetch_last_success_ages
            from core.duckdb_store import get_store

            store = await get_store()
            async with store.connection() as conn:
                ages = fetch_last_success_ages(conn)
                # Nested: a missing inventory_sku_history must not disable the
                # data-quality catch-ups above it.
                try:
                    inventory_today = conn.execute(
                        "SELECT 1 FROM inventory_sku_history "
                        "WHERE date = CURRENT_DATE LIMIT 1"
                    ).fetchone()
                except Exception as exc:
                    logger.debug("inventory catch-up probe skipped: %s", exc)
                    inventory_today = ()  # treat as present → queue nothing
        except Exception as e:
            logger.warning(f"Catch-up check skipped: {e}")
            return

        now = datetime.now(timezone.utc)

        # The inventory snapshot is the one job whose missed run cannot be
        # repaid later: it photographs current per-SKU stock, and the API serves
        # current stock only, so the day it did not run is gone. A CronTrigger
        # at 01:00 does not fire for a process that was not alive at 01:00, and
        # misfire_grace_time has nothing to forgive — the job did not exist when
        # it was due. This cannot recover a past day; it turns "the container was
        # down at 01:00, so today is lost too" into "today was recorded late".
        #
        # Safe unconditionally: record_sku_inventory_snapshot is idempotent per
        # day and returns False when today is already there. The probe above only
        # avoids queueing a run that would certainly be a no-op.
        if inventory_today is None:
            job = self._scheduler.get_job("inventory_snapshot") if self._scheduler else None
            if job is not None:
                run_at = now + timedelta(seconds=INVENTORY_CATCHUP_DELAY_S)
                try:
                    self._scheduler.add_job(
                        job.func,
                        trigger=DateTrigger(run_date=run_at),
                        id="inventory_snapshot_catchup",
                        name=f"{job.name} (catch-up)",
                        misfire_grace_time=DEFAULT_MISFIRE_GRACE_SECONDS,
                        max_instances=1,
                        coalesce=True,
                        replace_existing=True,
                    )
                    logger.warning(
                        "No inventory snapshot for today yet — catch-up run "
                        "queued for %s. A day without one cannot be backfilled.",
                        run_at.isoformat(timespec="seconds"),
                    )
                except Exception as e:
                    logger.warning(f"Could not queue inventory catch-up: {e}")
        for job_id, (layer, max_age_s, delay_s) in CATCHUP_CHECKS.items():
            entry = ages.get(layer) or {}
            age = entry.get("age_seconds")
            # A layer that has never succeeded is exactly the case worth
            # catching up, so a null age counts as overdue, not as unknown.
            if age is not None and age <= max_age_s:
                continue

            job = self._scheduler.get_job(job_id) if self._scheduler else None
            if job is None:
                continue

            run_at = now + timedelta(seconds=delay_s)
            try:
                self._scheduler.add_job(
                    job.func,
                    trigger=DateTrigger(run_date=run_at),
                    id=f"{job_id}_catchup",
                    name=f"{job.name} (catch-up)",
                    misfire_grace_time=DEFAULT_MISFIRE_GRACE_SECONDS,
                    max_instances=1,
                    coalesce=True,
                    replace_existing=True,
                )
                logger.warning(
                    "%s last succeeded %s ago (limit %dh) — catch-up run queued for %s",
                    job_id,
                    f"{age / 3600:.1f}h" if age is not None else "never",
                    max_age_s // 3600,
                    run_at.isoformat(timespec="seconds"),
                )
            except Exception as e:
                logger.warning(f"Could not queue catch-up for {job_id}: {e}")

    async def _register_jobs(self) -> None:
        """Register all background jobs."""
        # Import here to avoid circular imports
        from core.sync_service import get_sync_service
        from core.duckdb_store import get_store

        # Job: Incremental sync (every 60 seconds)
        self._add_job(
            job_id="incremental_sync",
            name="Incremental Sync",
            description="Sync new/updated orders from KeyCRM",
            func=self._run_incremental_sync,
            trigger=IntervalTrigger(seconds=60),
            max_instances=1,
            coalesce=True,
        )

        # Job: Full sync (weekly on Sunday at 2 AM Kyiv time)
        self._add_job(
            job_id="full_sync_weekly",
            name="Weekly Full Sync",
            description="Complete resync of all data (90 days)",
            func=self._run_full_sync,
            trigger=CronTrigger(day_of_week="sun", hour=2, minute=0),
            max_instances=1,
            coalesce=True,
        )

        # Job: Order gap backfill (hourly). Detection is free — holes in our own
        # id sequence — so this costs one API call per missing order and nothing
        # at all once the holes are closed.
        self._add_job(
            job_id="order_gap_backfill",
            name="Order Gap Backfill",
            description="Re-fetch orders missing from our id sequence",
            func=self._run_order_gap_backfill,
            trigger=IntervalTrigger(hours=1),
            max_instances=1,
            coalesce=True,
        )

        # Job: Inventory snapshot (daily at 1 AM)
        self._add_job(
            job_id="inventory_snapshot",
            name="Inventory Snapshot",
            description="Record daily inventory snapshot",
            func=self._run_inventory_snapshot,
            trigger=CronTrigger(hour=1, minute=0),
            max_instances=1,
            coalesce=True,
        )

        # Job: Manager stats update (daily at 3 AM)
        self._add_job(
            job_id="manager_stats",
            name="Manager Stats",
            description="Update manager order statistics",
            func=self._run_manager_stats,
            trigger=CronTrigger(hour=3, minute=0),
            max_instances=1,
            coalesce=True,
        )

        # Job: Seasonality calculation (weekly on Monday at 4 AM)
        self._add_job(
            job_id="seasonality_calc",
            name="Seasonality Calculation",
            description="Calculate seasonality indices and goals",
            func=self._run_seasonality_calc,
            trigger=CronTrigger(day_of_week="mon", hour=4, minute=0),
            max_instances=1,
            coalesce=True,
        )

        # Job: Revenue prediction model training (2x weekly: Mon & Thu at 3:30 AM)
        # Training daily is overkill - model quality doesn't improve with daily retraining
        # and consumes unnecessary CPU. 2x weekly keeps model fresh with historical patterns.
        self._add_job(
            job_id="revenue_prediction_train",
            name="Revenue Prediction",
            description="Train LightGBM model and generate revenue forecasts",
            func=self._run_revenue_prediction,
            trigger=CronTrigger(day_of_week="mon,thu", hour=3, minute=30),
            max_instances=1,
            coalesce=True,
        )

        # Job: Order status refresh (daily at 5 AM)
        # KeyCRM doesn't update updated_at when status changes, so we need to
        # periodically re-fetch recent orders to catch status changes (like cancellations)
        self._add_job(
            job_id="order_status_refresh",
            name="Order Status Refresh",
            description="Re-fetch recent orders to catch status changes (KeyCRM workaround)",
            func=self._run_order_status_refresh,
            trigger=CronTrigger(hour=5, minute=0),
            max_instances=1,
            coalesce=True,
        )

        # Job: Meilisearch sync (every 5 minutes)
        # Sync buyers, orders, and products to Meilisearch for chat search
        self._add_job(
            job_id="meilisearch_sync",
            name="Meilisearch Sync",
            description="Sync data to Meilisearch for chat search",
            func=self._run_meilisearch_sync,
            trigger=IntervalTrigger(minutes=5),
            max_instances=1,
            coalesce=True,
        )

        # Job: DuckDB WAL checkpoint (hourly)
        # Silver full-rebuild every 2 min + Gold incremental rewrites push WAL
        # past 600MB between 6h checkpoints. Hourly keeps MVCC tombstones
        # reaped and RAM pressure lower.
        self._add_job(
            job_id="duckdb_checkpoint",
            name="DuckDB Checkpoint",
            description="Flush WAL to main database file",
            func=self._run_duckdb_checkpoint,
            trigger=IntervalTrigger(hours=1),
            max_instances=1,
            coalesce=True,
        )

        # Job: Warehouse refresh (every 2 minutes, picks up dirty flag)
        # Decoupled from sync — sync writes Bronze + sets dirty flag,
        # this job rebuilds Silver/Gold independently
        self._add_job(
            job_id="warehouse_refresh",
            name="Warehouse Refresh",
            description="Rebuild Silver/Gold layers when dirty flag is set",
            func=self._run_warehouse_refresh,
            trigger=IntervalTrigger(minutes=2),
            max_instances=1,
            coalesce=True,
        )

        # Job: Daily DuckDB backup (A9-1) at a low-traffic hour.
        # Holds the store lock briefly for a consistent CHECKPOINT+copy.
        #
        # Retention is 2, not 7. Seven daily copies of a file that is mostly
        # derived tables cost ~18 GB to defend one threat — logical corruption
        # noticed within a week — and defended nothing else, because they sit on
        # the volume they are copies of. Now that deploy/offsite_parquet.sh puts
        # a validated archive somewhere else, on-disk copies only serve fast RTO
        # on a bad write, and two is enough for that.
        #
        # The trade this makes explicit: local granularity is now 2 days, while
        # the off-site copy is weekly. Corruption discovered on day 4 is
        # recoverable from off-site, but at up to a week's resolution. If that
        # becomes uncomfortable, the fix is a more frequent off-site push, not
        # more copies on the same disk.
        self._add_job(
            job_id="db_backup",
            name="DB Backup",
            description="Consistent on-disk backup of analytics.duckdb (retains 2)",
            func=self._run_backup,
            trigger=CronTrigger(hour=4, minute=30),
            max_instances=1,
            coalesce=True,
        )

        # Job: Bronze promotion (every 2 min, staging mode only)
        # Promotes unprocessed bronze events → orders table.
        # Only active when SYNC_MODE=staging; no-ops in legacy mode.
        self._add_job(
            job_id="bronze_promotion",
            name="Bronze Promotion",
            description="Promote bronze events to orders table (staging mode)",
            func=self._run_bronze_promotion,
            trigger=IntervalTrigger(minutes=2),
            max_instances=1,
            coalesce=True,
        )

        # Job: Bronze prune (daily at 4 AM)
        # Deletes processed bronze events older than 7 days
        self._add_job(
            job_id="bronze_prune",
            name="Bronze Prune",
            description="Delete old processed bronze events (7-day retention)",
            func=self._run_bronze_prune,
            trigger=CronTrigger(hour=4, minute=0),
            max_instances=1,
            coalesce=True,
        )

        # Job: Bronze invariant check (every 6 hours)
        # Catches config drift before it accumulates into a compaction blocker.
        # The 2026-05-18 incident (4.4M rows) would have fired this within 6h
        # instead of going undetected for 30 days.
        self._add_job(
            job_id="bronze_invariant_check",
            name="Bronze Invariant Check",
            description="Verify bronze row count matches mode invariant",
            func=self._run_bronze_invariant_check,
            trigger=CronTrigger(hour=INVARIANT_CHECK_HOURS),
            max_instances=1,
            coalesce=True,
        )

        # Job: Data Quality — Layer 1 integrity (every 6h)
        # Cheap DB-only scans (PK uniqueness, FK orphans, value domains).
        # Catches schema drift and corruption before it propagates.
        self._add_job(
            job_id="dq_integrity_check",
            name="DQ: Integrity",
            description="Layer 1 internal integrity scan (PK/FK/NULL/domain)",
            func=self._run_dq_integrity,
            trigger=CronTrigger(hour=INVARIANT_CHECK_HOURS),
            max_instances=1,
            coalesce=True,
        )

        # Job: Disk capacity watchdog (every 6h)
        # Captures a (db_size, disk_pct) sample into disk_samples, then alerts
        # on the capacity thresholds (75% / 90%). The 24h DB delta is recorded
        # in the job result and the log but is NOT an alerting condition: the
        # docstring in core/disk_monitor.py explains what the percentage-growth
        # rule did here and what has to replace it.
        self._add_job(
            job_id="disk_watchdog",
            name="Disk Capacity Watchdog",
            description="Sample disk + DB size, alert on a disk capacity breach",
            func=self._run_disk_watchdog,
            trigger=CronTrigger(hour=INVARIANT_CHECK_HOURS),
            max_instances=1,
            coalesce=True,
        )

        # Job: Data Quality — Layer 2 reconciliation (daily at 05:30 Kyiv)
        # Aggregate (month, source) comparison vs KeyCRM with 2h watermark.
        # Classifies discrepancies, persists to data_quality_runs, alerts
        # admin on CRITICAL severity.
        #
        # 05:00 Kyiv is FORBIDDEN — it is 02:00 UTC, and the host cron
        # `0 2 * * 0 weekly_compact.sh` stops both containers at that exact
        # instant every Sunday. The scheduler came back at 02:00:51, after the
        # cron instant, so CronTrigger set the next fire a day out and the run
        # was lost. Not late — absent, which no misfire grace can forgive.
        # The compact takes ~85 s; half an hour of clearance is plenty.
        self._add_job(
            job_id="dq_reconciliation",
            name="DQ: Reconciliation",
            description="Layer 2 source-of-truth reconciliation vs KeyCRM (90d window)",
            func=self._run_dq_reconciliation,
            trigger=CronTrigger(hour=5, minute=30),
            max_instances=1,
            coalesce=True,
        )

        # Job: Half-written order repair (every 2 h)
        # Orders with revenue and no line items. Detection is a table scan of
        # our own data, so an idle run costs nothing; a working one costs one
        # API call per order, capped at REPAIR_BATCH_LIMIT.
        self._add_job(
            job_id="halfwritten_repair",
            name="Half-written Order Repair",
            description="Re-fetch orders that have revenue but no line items",
            func=self._run_halfwritten_repair,
            trigger=IntervalTrigger(hours=2),
            max_instances=1,
            coalesce=True,
        )

        # Job: Data Quality — daily digest (09:00 Kyiv)
        # After the 05:00 reconciliation and the 07:00 integrity scan, so it
        # reports the day's verdicts rather than yesterday's. WARN findings
        # reach a human here and nowhere else.
        self._add_job(
            job_id="dq_digest",
            name="DQ: Daily Digest",
            description="One daily message with WARN+ findings from both DQ layers",
            func=self._run_dq_digest,
            trigger=CronTrigger(hour=9, minute=0),
            max_instances=1,
            coalesce=True,
        )

        # Job: Weekly sales report (daily tick at 09:30 Kyiv, delivers once)
        #
        # Fires every day and reports the last *complete* week, going quiet on
        # the six days that week has already been delivered. A weekly
        # CronTrigger would be the obvious thing and the wrong one: it computes
        # its next fire from registration, so a container that was not alive at
        # its Monday instant — the Sunday compact stops both, and so does any
        # deploy — pushes the next run a full week out and the week is never
        # reported at all. A daily tick against a ledger turns that into a
        # Tuesday delivery. 09:30 is after the DQ digest at 09:00, so a broken
        # warehouse is heard about before its numbers are read.
        self._add_job(
            job_id="weekly_report",
            name="Weekly Sales Report",
            description="Last complete week's numbers and drivers, once a week",
            func=self._run_weekly_report,
            trigger=CronTrigger(hour=9, minute=30),
            max_instances=1,
            coalesce=True,
        )

        # Job: Reconciliation check (daily at 6 AM)
        # Legacy job — preserved for backward compatibility; the new
        # dq_reconciliation job above is the source of truth for alerts.
        self._add_job(
            job_id="reconciliation_check",
            name="Reconciliation Check",
            description="Compare DuckDB order counts with KeyCRM API",
            func=self._run_reconciliation,
            trigger=CronTrigger(hour=6, minute=0),
            max_instances=1,
            coalesce=True,
        )

        # Job: Memory monitor (every 30 minutes)
        # Reads cgroup memory stats and alerts admin via Telegram
        # when usage crosses warning/critical thresholds
        self._add_job(
            job_id="memory_monitor",
            name="Memory Monitor",
            description="Monitor container memory and alert admin when upgrade needed",
            func=self._run_memory_monitor,
            trigger=IntervalTrigger(minutes=30),
            max_instances=1,
            coalesce=True,
        )

        logger.info(f"Registered {len(self._job_info)} background jobs")

    def _add_job(
        self,
        job_id: str,
        name: str,
        description: str,
        func: Callable,
        trigger,
        max_instances: int = 1,
        coalesce: bool = True,
        misfire_grace_time: int = DEFAULT_MISFIRE_GRACE_SECONDS,
    ) -> None:
        """Add a job to the scheduler."""
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            name=name,
            misfire_grace_time=misfire_grace_time,
            max_instances=max_instances,
            coalesce=coalesce,
            replace_existing=True,
        )

        self._job_info[job_id] = JobInfo(
            id=job_id,
            name=name,
            description=description,
        )
        self._job_history[job_id] = []

        # Get next run time (may not be available until scheduler starts)
        try:
            job = self._scheduler.get_job(job_id)
            if job and hasattr(job, 'next_run_time') and job.next_run_time:
                self._job_info[job_id].next_run = job.next_run_time
        except Exception:
            pass  # next_run_time will be updated when scheduler starts

    # ═══════════════════════════════════════════════════════════════════════════
    # JOB IMPLEMENTATIONS
    # ═══════════════════════════════════════════════════════════════════════════

    async def _run_incremental_sync(self) -> Dict[str, Any]:
        """Run incremental sync job."""
        async with self._heavy_job_lock:
            with correlation_context() as corr_id:
                logger.debug("Starting incremental sync job")

                from core.sync_service import get_sync_service
                sync_service = await get_sync_service()
                stats = await sync_service.incremental_sync()

                logger.debug(
                    "Incremental sync job complete",
                    extra={"stats": stats}
                )
                return stats

    async def _run_full_sync(self) -> Dict[str, Any]:
        """Run full sync job (90 days)."""
        async with self._heavy_job_lock:
            with correlation_context() as corr_id:
                logger.info("Starting weekly full sync job")

                from core.sync_service import get_sync_service
                sync_service = await get_sync_service()
                stats = await sync_service.full_sync(days_back=365)

                logger.info(
                    "Weekly full sync job complete",
                    extra={"stats": stats}
                )
                return stats

    async def _run_inventory_snapshot(self) -> Dict[str, Any]:
        """Run inventory snapshot job."""
        with correlation_context() as corr_id:
            logger.info("Starting inventory snapshot job")

            from core.duckdb_store import get_store
            store = await get_store()

            # Refresh Layer 1
            sku_count = await store.refresh_sku_inventory_status()

            # Record Layer 2 snapshot
            recorded = await store.record_sku_inventory_snapshot()

            # Legacy snapshot
            await store.record_inventory_snapshot()

            result = {
                "sku_count": sku_count,
                "snapshot_recorded": recorded,
            }
            logger.info(
                "Inventory snapshot job complete",
                extra=result
            )
            return result

    async def _run_manager_stats(self) -> Dict[str, Any]:
        """Run manager stats update job."""
        with correlation_context() as corr_id:
            logger.info("Starting manager stats job")

            from core.duckdb_store import get_store
            store = await get_store()
            updated = await store.update_manager_stats()

            result = {"managers_updated": updated}
            logger.info(
                "Manager stats job complete",
                extra=result
            )
            return result

    async def _run_revenue_prediction(self) -> Dict[str, Any]:
        """Train revenue prediction model and generate forecasts."""
        async with self._heavy_job_lock:
            with correlation_context() as corr_id:
                logger.info("Starting revenue prediction training job")

                from core.prediction_service import get_prediction_service
                service = get_prediction_service()

                result = await service.train(sales_type="retail")

                logger.info(
                    "Revenue prediction job complete",
                    extra={"result": result}
                )
                return result

    async def _run_seasonality_calc(self) -> Dict[str, Any]:
        """Run seasonality calculation job."""
        with correlation_context() as corr_id:
            logger.info("Starting seasonality calculation job")

            from core.duckdb_store import get_store
            store = await get_store()

            # Calculate for retail
            retail_indices = await store.calculate_seasonality_indices("retail")
            retail_goals = await store.calculate_suggested_goals(sales_type="retail", growth_factor=1.10)
            await store.calculate_yoy_growth("retail")

            # Calculate for b2b
            b2b_indices = await store.calculate_seasonality_indices("b2b")
            b2b_goals = await store.calculate_suggested_goals(sales_type="b2b", growth_factor=1.10)
            await store.calculate_yoy_growth("b2b")

            result = {
                "retail_months": len(retail_indices),
                "b2b_months": len(b2b_indices),
                "retail_goals": retail_goals,
                "b2b_goals": b2b_goals,
            }
            logger.info(
                "Seasonality calculation job complete",
                extra=result
            )
            return result

    async def _run_order_status_refresh(self) -> Dict[str, Any]:
        """
        Re-fetch recent orders to catch status changes.

        KeyCRM doesn't update updated_at when order status changes (e.g., cancellations),
        so the incremental sync misses these. This job re-fetches the last 30 days
        of orders to ensure all status changes are captured.
        """
        async with self._heavy_job_lock:
            with correlation_context() as corr_id:
                logger.info("Starting order status refresh job")

                from core.sync_service import get_sync_service
                sync_service = await get_sync_service()
                stats = await sync_service.refresh_order_statuses(days_back=30)

                logger.info(
                    "Order status refresh job complete",
                    extra={"stats": stats}
                )
                return stats

    async def _run_meilisearch_sync(self) -> Dict[str, Any]:
        """Sync data to Meilisearch for chat search."""
        async with self._heavy_job_lock:
            with correlation_context() as corr_id:
                logger.debug("Starting Meilisearch sync job")

                from core.sync_service import get_sync_service
                from core.meilisearch_client import get_meili_client

                # Check if Meilisearch is available
                meili = get_meili_client()
                health = await meili.health_check()
                if health.get("status") != "available":
                    logger.debug("Meilisearch not available, skipping sync")
                    return {"skipped": True, "reason": "Meilisearch not available"}

                sync_service = await get_sync_service()
                stats = await sync_service.sync_to_meilisearch()

                logger.debug(
                    "Meilisearch sync job complete",
                    extra={"stats": stats}
                )
                return stats

    async def _run_duckdb_checkpoint(self) -> Dict[str, Any]:
        """
        Run CHECKPOINT to flush WAL to main database file.

        DuckDB uses Write-Ahead Logging (WAL) for durability. The WAL file
        can grow over time with many writes. CHECKPOINT forces all pending
        changes to be written to the main database file and resets the WAL.
        """
        with correlation_context() as corr_id:
            logger.info("Starting DuckDB checkpoint job")

            from core.duckdb_store import get_store
            store = await get_store()

            # Run checkpoint
            await store.checkpoint()

            result = {"checkpointed": True}
            logger.info("DuckDB checkpoint job complete")
            return result

    async def _run_warehouse_refresh(self) -> Dict[str, Any]:
        """Check dirty flag and rebuild Silver/Gold if needed."""
        async with self._heavy_job_lock:
            from core.duckdb_store import get_store
            store = await get_store()

            is_dirty, changed_ids = await store.consume_warehouse_dirty()
            if not is_dirty:
                return {"skipped": True, "reason": "not dirty"}

            with correlation_context() as corr_id:
                logger.info(f"Warehouse dirty — refreshing (changed_ids={'full' if changed_ids is None else len(changed_ids)})")
                result = await store.refresh_warehouse_layers(
                    trigger="dirty_flag",
                    changed_order_ids=changed_ids,
                )
                logger.info("Warehouse refresh complete")
                return result

    async def _run_backup(self) -> Dict[str, Any]:
        """Daily consistent backup of the DuckDB warehouse (A9-1)."""
        async with self._heavy_job_lock:
            from core.duckdb_store import get_store
            store = await get_store()
            with correlation_context() as corr_id:
                logger.info("Starting daily DB backup")
                result = await store.backup_database(keep=2)
                logger.info(f"DB backup job complete: {result.get('status')}")
                return result

    async def _run_reconciliation(self) -> Dict[str, Any]:
        """Run daily reconciliation check against KeyCRM API."""
        with correlation_context() as corr_id:
            logger.info("Starting reconciliation check job")

            from core.sync_service import get_sync_service
            sync_service = await get_sync_service()
            results = await sync_service.reconcile_with_api(days_back=14)

            ok = sum(1 for r in results if r["status"] == "ok")
            drift = sum(1 for r in results if r["status"] == "drift")

            result = {"checked_days": len(results), "ok": ok, "drift": drift}
            logger.info("Reconciliation check job complete", extra=result)
            return result

    # ─── Data Quality framework (Layer 1 + 2) ─────────────────────────────────

    # Alert throttling — independent timers per layer so an integrity issue
    # doesn't suppress a reconciliation alert in the same cooldown window.
    _dq_last_alert: Dict[str, float] = {}
    _DQ_ALERT_COOLDOWN_S = 86400  # 24 h

    async def _send_dq_alert_throttled(
        self, layer: str, message: str, key: Optional[str] = None,
    ) -> bool:
        """Send a Data Quality alert with a 24h cooldown per distinct problem.

        The cooldown used to be per *layer*, which meant a second, unrelated
        CRITICAL in the same layer was silently swallowed for a day by the
        first one. `key` — from `alert_fingerprint`, naming the checks and
        discrepancy classes involved — makes each problem its own bucket.

        Returns True if alert was sent, False if throttled or failed.
        """
        bucket = key or layer
        now = time.time()
        last = BackgroundScheduler._dq_last_alert.get(bucket, 0.0)
        if now - last < self._DQ_ALERT_COOLDOWN_S:
            logger.info(
                f"DQ alert ({bucket}) throttled: "
                f"last sent {int(now - last)}s ago"
            )
            return False
        try:
            from bot.main import send_admin_message
            await send_admin_message(message, key=bucket)
            BackgroundScheduler._dq_last_alert[bucket] = now
            return True
        except Exception as e:
            logger.warning(f"DQ alert send failed ({bucket}): {e}")
            return False

    async def _run_dq_integrity(self) -> Dict[str, Any]:
        """Layer-1 integrity scan. Pure DB reads, no external I/O."""
        from datetime import datetime, timezone
        from core.data_quality import (
            Severity,
            alert_fingerprint,
            check_internal_integrity,
            format_alert_message,
            overall_severity,
            persist_run,
        )
        from core.duckdb_store import get_store

        with correlation_context() as corr_id:
            started_at = datetime.now(timezone.utc)
            logger.info("DQ Layer-1 integrity scan starting")

            store = await get_store()
            error_message = None
            issues = []
            try:
                async with store.connection() as conn:
                    issues = check_internal_integrity(conn)
            except Exception as e:
                error_message = f"{type(e).__name__}: {e}"
                logger.exception("DQ integrity scan raised")

            ended_at = datetime.now(timezone.utc)
            window_day = ended_at.date()

            # Persist in a separate transaction (uses store wrapper).
            run_id = None
            try:
                async with store.connection() as conn:
                    run_id = persist_run(
                        conn,
                        started_at=started_at, ended_at=ended_at,
                        as_of=ended_at,
                        window_start=window_day, window_end=window_day,
                        layer="integrity",
                        issues=issues, discrepancies=[],
                        error_message=error_message,
                    )
            except Exception as e:
                logger.exception(f"DQ integrity persist failed: {e}")

            sev = overall_severity(issues, [])
            if sev == Severity.CRITICAL and not error_message:
                msg = format_alert_message("integrity", sev, issues, [])
                await self._send_dq_alert_throttled(
                    "integrity", msg,
                    alert_fingerprint("integrity", sev, issues, []),
                )

            result = {
                "run_id": run_id,
                "issues_count": len(issues),
                "severity": sev.value,
                "duration_ms": int((ended_at - started_at).total_seconds() * 1000),
                "error": error_message,
            }
            logger.info("DQ Layer-1 integrity scan complete", extra=result)
            return result

    # ─── Disk capacity watchdog ───────────────────────────────────────────────

    _disk_alert_last_sent: float = 0.0
    _DISK_ALERT_COOLDOWN_S = 86400  # 24h

    async def _run_disk_watchdog(self) -> Dict[str, Any]:
        """Sample disk + DB state, alert on a capacity breach.

        Independent of dq_* jobs: this is about RESOURCES (disk filling),
        not data quality. Co-located in scheduler only because the cadence
        is the same (6h).

        The 24h DB delta is measured and logged but never paged on — see
        the module docstring in core/disk_monitor.py for why the old
        percentage-growth alert was incapable of returning "OK", and what
        has to replace it. Sampling continues regardless: that history is
        what the replacement will be calibrated from.
        """
        from core.disk_monitor import (
            BOOTSTRAP_STEP_GB,
            evaluate_dir_growth,
            evaluate_disk_capacity,
            fetch_dir_sample_at_age,
            fetch_sample_at_age,
            insert_dir_samples,
            insert_sample,
            prune_old_dir_samples,
            prune_old_samples,
            sample_data_dir,
            sample_disk_state,
        )
        from core.duckdb_store import get_store

        with correlation_context() as corr_id:
            store = await get_store()
            sample = sample_disk_state(str(store.db_path))

            # The whole directory, not just the database file. The 27 GB that
            # arrived in August 2026 sat next to analytics.duckdb, so a check
            # that sampled only the file could not see it — and blamed the file.
            dir_now = sample_data_dir(str(store.db_path.parent))

            async with store.connection() as conn:
                history = fetch_sample_at_age(conn, hours=24, slack_hours=2)
                insert_sample(conn, sample)
                # Keep the table tiny: ~56 rows max (14 days x 4 samples/day).
                deleted = prune_old_samples(conn, retention_days=14)

                dir_week_ago = fetch_dir_sample_at_age(conn, hours=168, slack_hours=12)
                dir_six_ago = fetch_dir_sample_at_age(conn, hours=6, slack_hours=2)
                if dir_now:
                    insert_dir_samples(conn, dir_now)
                    prune_old_dir_samples(conn, retention_days=21)

            growth = evaluate_dir_growth(current=dir_now, baseline=dir_week_ago)
            if growth is None and dir_week_ago is None:
                # Bootstrap: no week of history yet. A step change is still a
                # step change, and a detector silent for its first seven days is
                # missing exactly when a fresh deploy is most likely to regress.
                growth = evaluate_dir_growth(
                    current=dir_now, baseline=dir_six_ago, window_hours=6,
                    warn_gb=BOOTSTRAP_STEP_GB, critical_gb=BOOTSTRAP_STEP_GB * 2,
                )

            # A heartbeat something outside this process can read. The
            # watchdog spent eleven weeks taking no samples and nothing said
            # so, because a monitor that has stopped and one with nothing to
            # report emit the same silence. No in-process check can close that
            # — it would be the same process attesting to itself — so the
            # attestation is a file, and the reader is host cron.
            try:
                from datetime import timezone as _tz
                health_dir = store.db_path.parent / "health"
                health_dir.mkdir(exist_ok=True)
                (health_dir / "watchdog_last_sample").write_text(
                    datetime.now(_tz.utc).isoformat(timespec="seconds")
                )
            except Exception as exc:
                logger.warning(f"Could not write watchdog heartbeat: {exc}")

            db_24h_ago = history["db_size_mb"] if history else None
            growth_mb_24h = (
                round(sample["db_size_mb"] - db_24h_ago, 2)
                if db_24h_ago is not None else None
            )

            alert = evaluate_disk_capacity(
                disk_pct_used=sample["disk_pct_used"],
                disk_free_gb=sample["disk_free_gb"],
                db_size_mb=sample["db_size_mb"],
            )

            # Capacity outranks growth only when it is worse: a filling disk is
            # a deadline, a growing directory is a cause. Reporting the cause
            # first is what the last incident needed and did not get.
            if growth is not None and (
                alert is None or growth.severity.rank() >= alert.severity.rank()
            ):
                from core.disk_monitor import DiskAlert
                alert = DiskAlert(
                    severity=growth.severity,
                    reason=growth.reason + (f" | {alert.reason}" if alert else ""),
                    disk_pct_used=sample["disk_pct_used"],
                    disk_free_gb=sample["disk_free_gb"],
                    db_size_mb=sample["db_size_mb"],
                )

            result = {
                "db_size_mb": sample["db_size_mb"],
                "disk_pct_used": sample["disk_pct_used"],
                "disk_free_gb": sample["disk_free_gb"],
                "db_24h_ago_mb": db_24h_ago,
                "db_growth_mb_24h": growth_mb_24h,
                "data_dir_mb": round(sum(dir_now.values()) / (1024 ** 2)) if dir_now else None,
                "data_dir_growth_gb_168h": growth.total_delta_gb if growth else None,
                "pruned_old_samples": deleted,
                "alert_fired": False,
            }

            if alert is None:
                growth_str = (
                    f"{growth_mb_24h:+,.0f} MB/24h" if growth_mb_24h is not None
                    else "no 24h baseline"
                )
                # info, not debug: a watchdog that only speaks on bad news is
                # indistinguishable from a dead one, and this one has already
                # been on the wrong side of that. Silence must mean something.
                logger.info(
                    f"Disk OK: {sample['disk_pct_used']:.1f}% used, "
                    f"{sample['disk_free_gb']:.1f} GB free, "
                    f"DB={sample['db_size_mb']:,.0f} MB ({growth_str})"
                )
                return result

            logger.warning(f"Disk watchdog: {alert.severity.value} — {alert.reason}")

            # Throttle: avoid paging admins every 6h while the breach persists.
            now = time.time()
            since = now - BackgroundScheduler._disk_alert_last_sent
            if since < self._DISK_ALERT_COOLDOWN_S:
                logger.info(
                    f"Disk alert throttled — last sent {int(since)}s ago "
                    f"(cooldown {self._DISK_ALERT_COOLDOWN_S}s)"
                )
                return result

            BackgroundScheduler._disk_alert_last_sent = now
            try:
                from bot.main import send_admin_message
                icon = "🚨" if alert.severity.value == "CRITICAL" else "⚠️"
                msg = (
                    f"{icon} *Disk watchdog: {alert.severity.value}*\n"
                    f"{alert.reason}\n\n"
                    f"DB: {alert.db_size_mb:,.0f} MB\n"
                    f"Disk: {alert.disk_pct_used:.1f}% used, "
                    f"{alert.disk_free_gb:.1f} GB free"
                )
                await send_admin_message(msg, key="disk:" + alert.severity.value)
                result["alert_fired"] = True
            except Exception as e:
                logger.warning(f"Disk alert send failed: {e}")

            return result

    async def _run_dq_reconciliation(self, window_days: int = 90) -> Dict[str, Any]:
        """Layer-2 source-of-truth reconciliation vs KeyCRM.

        Pulls (month, source) rollup for the last 90 days from both DuckDB
        and KeyCRM with a 2-hour watermark, classifies discrepancies,
        persists, and alerts on CRITICAL.
        """
        from datetime import datetime, timedelta, timezone
        from zoneinfo import ZoneInfo
        from core.data_quality import (
            Severity,
            alert_fingerprint,
            classify_discrepancies,
            classify_order_discrepancies,
            format_alert_message,
            overall_severity,
            persist_run,
        )
        from core.duckdb_store import get_store
        from core.reconciliation_io import (
            duckdb_orders_in_window,
            keycrm_orders_in_window,
            rollup_from_orders,
        )

        WATERMARK_HOURS = 2
        # The daily run covers 90 days. Older months are checked by nobody —
        # they were inside the window once, during the months this job was
        # dying on 429s. `POST /api/reconcile?days=N` widens it on demand.
        WINDOW_DAYS = max(1, int(window_days))

        with correlation_context() as corr_id:
            started_at = datetime.now(timezone.utc)
            kyiv_now = started_at.astimezone(ZoneInfo("Europe/Kyiv"))
            as_of = started_at - timedelta(hours=WATERMARK_HOURS)
            window_end = kyiv_now.date()
            window_start = window_end - timedelta(days=WINDOW_DAYS)

            logger.info(
                f"DQ Layer-2 reconciliation starting "
                f"window={window_start}..{window_end} as_of={as_of.isoformat()}"
            )

            store = await get_store()
            error_message = None
            issues: list = []
            discrepancies: list = []
            api_calls = 0

            try:
                # 1. KeyCRM orders (counts API calls). Runs first because it
                #    decides which orders are in-flight — DuckDB's updated_at is
                #    a synced copy and can only be older, so KeyCRM's cut is the
                #    wider one and both sides must honour it.
                kc_orders, api_calls, inflight_ids = await keycrm_orders_in_window(
                    window_start, window_end, watermark=as_of,
                )

                # 2. The same facts from the warehouse, minus the same orders
                async with store.connection() as conn:
                    dk_orders = duckdb_orders_in_window(
                        conn, window_start, window_end, watermark=as_of,
                        exclude_ids=inflight_ids,
                    )

                # 3. Classify (pure). Both rollups come from one function, so
                #    the two sides cannot aggregate differently. The per-order
                #    pass costs no extra API calls and catches what totals hide:
                #    offsetting errors net to zero in a monthly sum.
                dk_rollup = rollup_from_orders(dk_orders)
                kc_rollup = rollup_from_orders(kc_orders)
                discrepancies = classify_discrepancies(dk_rollup, kc_rollup)
                discrepancies += classify_order_discrepancies(dk_orders, kc_orders)
                logger.info(
                    f"DQ reconciliation: dk_cells={len(dk_rollup)} "
                    f"kc_cells={len(kc_rollup)} discrepancies={len(discrepancies)}"
                )
            except Exception as e:
                error_message = f"{type(e).__name__}: {e}"
                logger.exception("DQ reconciliation raised")

            ended_at = datetime.now(timezone.utc)

            # 4. Persist
            run_id = None
            try:
                async with store.connection() as conn:
                    run_id = persist_run(
                        conn,
                        started_at=started_at, ended_at=ended_at,
                        as_of=as_of,
                        window_start=window_start, window_end=window_end,
                        layer="reconciliation",
                        issues=issues, discrepancies=discrepancies,
                        api_calls_used=api_calls,
                        error_message=error_message,
                    )
            except Exception as e:
                logger.exception(f"DQ reconciliation persist failed: {e}")

            # 5. Repair what can only be repaired by id. A delta sync keyed on
            #    updated_at can never reach an order we do not hold, so finding
            #    them and doing nothing would mean finding them again tomorrow.
            #    Additive only — MISSING_IN_KC is deliberately not touched,
            #    because deleting on the strength of one API reading is how you
            #    turn a fetch glitch into data loss.
            #    Half-written orders used to be repaired here too. They are
            #    found by scanning our own tables, owe nothing to the comparison
            #    or its 90-day window, and cost one API call each — so they hung
            #    their fate on a job that failed 57 runs out of 68 and died at
            #    120 s before ever reaching this line. They have their own job
            #    now: `halfwritten_repair`.
            repair = None
            if not error_message:
                repairable = sorted({
                    oid
                    for d in discrepancies
                    if d.diff_class == DiscrepancyClass.MISSING_IN_DK
                    for oid in d.order_ids
                })

                if repairable:
                    try:
                        sync_service = await get_sync_service()
                        repair = await sync_service.repair_orders(repairable)
                    except Exception as e:
                        logger.exception(f"DQ repair failed: {e}")

            # 6. Alert on CRITICAL severity
            sev = overall_severity(issues, discrepancies)
            if sev == Severity.CRITICAL and not error_message:
                msg = format_alert_message(
                    "reconciliation", sev, issues, discrepancies,
                    window=(window_start, window_end),
                )
                if repair and repair.get("repaired"):
                    msg += (
                        f"\n\nRe-fetched {repair['repaired']} order(s) by id"
                        + (f", {repair['remaining']} queued for the next run"
                           if repair.get("remaining") else "")
                        + "."
                    )
                await self._send_dq_alert_throttled(
                    "reconciliation", msg,
                    alert_fingerprint("reconciliation", sev, issues, discrepancies),
                )

            result = {
                "run_id": run_id,
                "discrepancies_count": len(discrepancies),
                "severity": sev.value,
                "duration_ms": int((ended_at - started_at).total_seconds() * 1000),
                "api_calls_used": api_calls,
                "repair": repair,
                "error": error_message,
            }
            logger.info("DQ Layer-2 reconciliation complete", extra=result)
            return result

    async def _run_halfwritten_repair(self) -> Dict[str, Any]:
        """Re-fetch orders that carry revenue but no line items.

        523 of them, ₴1,422,610.30: revenue counts them because it reads
        `grand_total`, while every product, brand and category figure cannot,
        because those read line items. Only a fetch by id can fill them — a
        delta sync keyed on `updated_at` sees a complete-looking header and
        moves on.

        An order KeyCRM re-serves still empty is recorded, so the next run asks
        about something else. Recorded ids come back up for another try after
        30 days: the alternative is being permanently blind to an order whose
        line items someone eventually fixes upstream.
        """
        from core.duckdb_store import get_store
        from core.sync_service import SyncService, get_sync_service

        _EMPTY_LINE_ITEMS = """
            SELECT o.id FROM orders o
            LEFT JOIN (SELECT DISTINCT order_id FROM order_products) li
                   ON li.order_id = o.id
            WHERE li.order_id IS NULL AND o.grand_total > 0
        """

        with correlation_context():
            store = await get_store()

            async with store.connection() as conn:
                candidates = [int(r[0]) for r in conn.execute(f"""
                    {_EMPTY_LINE_ITEMS}
                      AND NOT EXISTS (
                          SELECT 1 FROM order_backfill_misses m
                          WHERE m.order_id = o.id
                            AND m.checked_at > CURRENT_TIMESTAMP - INTERVAL '30 days'
                      )
                    ORDER BY o.grand_total DESC
                    LIMIT ?
                """, [SyncService.REPAIR_BATCH_LIMIT]).fetchall()]

            if not candidates:
                logger.debug("Half-written repair: nothing to fetch")
                return {"candidates": 0, "repaired": 0, "still_empty": 0}

            sync_service = await get_sync_service()
            result = await sync_service.repair_orders(candidates)

            # Which ones KeyCRM served without line items anyway.
            ph = ",".join("?" * len(candidates))
            async with store.connection() as conn:
                still_empty = [int(r[0]) for r in conn.execute(
                    f"{_EMPTY_LINE_ITEMS} AND o.id IN ({ph})", candidates,
                ).fetchall()]
            recorded = await store.record_backfill_misses({
                oid: "re-fetched by id; KeyCRM served no line items"
                for oid in still_empty
            })

            out = {
                "candidates": len(candidates),
                "repaired": result.get("repaired", 0),
                "failed": result.get("failed", 0),
                "still_empty": recorded,
            }
            logger.info(f"Half-written repair: {out}")
            return out

    async def _run_dq_digest(self) -> Dict[str, Any]:
        """Say out loud, once a day, what the checks have been writing down.

        WARN findings are persisted on every run and pushed on none — only
        CRITICAL is alerted at the moment it happens. So two standing WARNs
        covering ₴5.6M sat in `data_quality_issues` where nobody reads them.
        This job is their surface: one message, both layers, with a delta
        against the previous run so a growing problem does not read like the
        one you already know about.

        Sends nothing when every layer is fresh and clean. Silence here means
        "nothing new to report" — proving the checks are still running is the
        canary's job, off this host and on its own clock.
        """
        from datetime import datetime, timezone
        from core.data_quality import (
            DigestSection,
            build_digest,
            fetch_latest_run,
            fetch_previous_run,
            fetch_run_diffs,
            fetch_run_issues,
            WATCHED_LAYERS,
        )
        from core.duckdb_store import get_store

        with correlation_context():
            store = await get_store()
            now = datetime.now(timezone.utc)
            sections: List[DigestSection] = []

            async with store.connection() as conn:
                for layer in WATCHED_LAYERS:
                    run = fetch_latest_run(conn, layer=layer)
                    if run is None:
                        sections.append(DigestSection(layer=layer, run=None))
                        continue

                    age_hours = None
                    if run.get("started_at"):
                        started = datetime.fromisoformat(run["started_at"])
                        age_hours = (now - started).total_seconds() / 3600

                    previous = fetch_previous_run(conn, layer, run["run_id"])
                    sections.append(DigestSection(
                        layer=layer,
                        run=run,
                        issues=fetch_run_issues(conn, run["run_id"], limit=20),
                        diffs=fetch_run_diffs(conn, run["run_id"], limit=20),
                        previous_issues=(
                            fetch_run_issues(conn, previous["run_id"], limit=20)
                            if previous else []
                        ),
                        age_hours=age_hours,
                    ))

            message = build_digest(sections)
            sent = False
            if message:
                try:
                    from bot.main import send_admin_message
                    await send_admin_message(message, key="dq:digest")
                    sent = True
                except Exception as e:
                    logger.warning(f"DQ digest send failed: {e}")

            result = {
                "layers": len(sections),
                "sent": sent,
                "quiet": message is None,
            }
            logger.info("DQ digest complete", extra=result)
            return result

    async def _run_weekly_report(self) -> Dict[str, Any]:
        """Deliver last week's numbers, once, to the admins.

        Three gates, in order of cost: already delivered, warehouse not yet
        past the week end, nothing to say. The middle one matters — the report
        is read as fact, and a week rendered while Gold is still catching up
        would understate revenue with total confidence. It is checked against
        the warehouse's own last date rather than against seven rows for this
        sales type, because a quiet type legitimately has days with no orders.

        Failing to send leaves the ledger untouched, so tomorrow's tick tries
        again rather than dropping the week.
        """
        from datetime import datetime as _datetime

        from core.config import ADMIN_USER_IDS, DASHBOARD_URL
        from core.duckdb_store import get_store
        from core.weekly_report import (
            already_sent,
            build_report,
            format_report,
            last_complete_week,
            mark_sent,
            warehouse_max_date,
        )

        sales_type = WEEKLY_REPORT_SALES_TYPE

        with correlation_context():
            store = await get_store()
            today = _datetime.now(SCHEDULER_TIMEZONE).date()
            week_start, week_end = last_complete_week(today)
            week = week_start.isoformat()

            async with store.connection() as conn:
                if already_sent(conn, week_start, sales_type):
                    logger.debug("Weekly report for %s already sent", week)
                    return {"sent": False, "week": week, "reason": "already_sent"}

                max_date = warehouse_max_date(conn)
                if max_date is None or max_date < week_end:
                    logger.info(
                        "Weekly report deferred: warehouse at %s, week ends %s",
                        max_date, week_end,
                    )
                    return {"sent": False, "week": week, "reason": "warehouse_behind"}

                report = build_report(conn, today, sales_type)

            if report.current.orders == 0:
                logger.warning("Weekly report skipped: no orders in %s", week)
                return {"sent": False, "week": week, "reason": "no_orders"}

            # Deliberately outside the connection block: store.connection()
            # holds the single-writer lock for its whole body, and Telegram is
            # allowed ten seconds per admin — longer for the card upload. Every
            # other DuckDB reader in this process would wait behind it.
            #
            # Everyone the bot has approved, plus the admins — who are always
            # on the list whether or not they ever went through approval. The
            # report is `retail` only, which is exactly what an approved user
            # already sees on the dashboard, so this widens the audience
            # without widening what is disclosed.
            #
            # One render per language, not per reader: most people share one,
            # so this is a pass or two however long the list. The grouping
            # exists so that nobody quietly gets somebody else's language.
            from core.bot_prefs import (
                default_language_for,
                group_by_language,
                read_approved_user_ids,
            )
            from core.telegram_alerts import (
                send_admin_message_http,
                send_admin_photo_http,
            )
            from core.weekly_report_image import render_weekly_card

            audience = list(dict.fromkeys(
                [int(a) for a in ADMIN_USER_IDS] + read_approved_user_ids()
            ))
            defaults = {
                uid: default_language_for(uid, ADMIN_USER_IDS) for uid in audience
            }

            delivered = 0
            with_card = 0
            for lang, recipients in group_by_language(audience, defaults).items():
                message = format_report(report, DASHBOARD_URL or None, lang)

                # The card first, with the report as its caption, so one
                # message carries both. It is drawn from the same values, so a
                # host with no fonts or a caption over Telegram's limit costs
                # the picture and nothing else.
                card = render_weekly_card(report, lang)
                sent_here = 0
                if card is not None:
                    sent_here = await send_admin_photo_http(
                        card, caption=message, chat_ids=recipients,
                        filename=f"week-{week}-{lang}.png",
                    )
                    with_card += sent_here
                if not sent_here:
                    sent_here = await send_admin_message_http(
                        message, chat_ids=recipients,
                    )
                delivered += sent_here

            # Nobody heard it, so it did not happen: leaving the ledger untouched
            # is what makes tomorrow's tick try again instead of dropping the week.
            if not delivered:
                logger.warning("Weekly report for %s reached no admin", week)
                return {"sent": False, "week": week, "reason": "not_delivered"}

            async with store.connection() as conn:
                mark_sent(
                    conn, week_start, sales_type,
                    report.current.revenue, report.current.orders,
                )

            result = {
                "sent": True,
                "week": week,
                "sales_type": sales_type,
                "revenue": round(report.current.revenue, 2),
                "orders": report.current.orders,
                "recipients": len(audience),
                "delivered": delivered,
                "card": bool(with_card),
            }
            logger.info("Weekly report sent", extra=result)
            return result

    async def _run_order_gap_backfill(self) -> Dict[str, Any]:
        """Fill holes in our order-id sequence by fetching them from KeyCRM.

        The daily reconciliation only looks at the last 90 days, so the 1 616
        orders missing from 2023–2025 would never have come up. Holes in the id
        sequence find them without spending a single API call, and the repair
        drains them a batch at a time until there is nothing left to ask for.
        """
        from core.duckdb_store import get_store
        from core.sync_service import get_sync_service

        with correlation_context():
            store = await get_store()
            gaps = await store.find_order_id_gaps(limit=200)
            if not gaps:
                logger.debug("Order gap backfill: no holes left")
                return {"gaps_found": 0, "repaired": 0}

            sync_service = await get_sync_service()
            result = await sync_service.repair_orders(gaps)

            # An id KeyCRM does not have is a hole that will never close.
            # Recording it is what lets the job finish rather than loop.
            permanent = {
                oid: reason for oid, reason in result.get("failures", {}).items()
                if "not found" in str(reason).lower()
            }
            recorded = await store.record_backfill_misses(permanent)

            out = {
                "gaps_found": len(gaps),
                "repaired": result.get("repaired", 0),
                "absent_upstream": recorded,
                "failed": result.get("failed", 0),
            }
            logger.info(f"Order gap backfill: {out}")
            return out

    # ─── Bronze Promotion & Prune ────────────────────────────────────────────

    async def _run_bronze_promotion(self) -> Dict[str, Any]:
        """Promote unprocessed bronze events → orders (staging mode only).

        In legacy mode, this is a no-op. In staging mode, this is the ONLY
        writer to the orders table — enforcing the single-writer invariant.
        After promotion, sets the warehouse dirty flag so Silver/Gold rebuild.
        """
        from core.config import config

        if not config.sync.is_staging:
            return {"skipped": True, "reason": "legacy mode"}

        async with self._heavy_job_lock:
            with correlation_context() as corr_id:
                from core.duckdb_store import get_store
                store = await get_store()

                result = await store.promote_bronze_to_orders(batch_size=2000)

                if result["promoted"] > 0:
                    logger.info(
                        f"Bronze promotion: {result['promoted']} orders promoted, "
                        f"{result['skipped']} skipped, {result['batch_event_ids']} events marked"
                    )
                    # Trigger warehouse rebuild
                    await store.mark_warehouse_dirty(None)
                else:
                    logger.debug("Bronze promotion: no unprocessed events")

                # Check for bronze backlog and alert if concerning
                stats = await store.get_bronze_stats()
                age_s = stats.get("oldest_unprocessed_age_s")
                if stats["unprocessed"] > 1000 or (age_s and age_s > 300):
                    await self._send_bronze_alert(stats)

                return result

    _bronze_invariant_last_alert: float = 0.0
    _BRONZE_INVARIANT_ALERT_COOLDOWN_S = 21600  # 6 hours — match check interval

    async def _run_bronze_invariant_check(self) -> Dict[str, Any]:
        """Assert bronze table size matches the (mode, shadow_enabled) invariant.

        Catches: prune disabled/broken, sync writing despite the opt-out,
        env-var drift between deploys, accidental staging→legacy mode flip
        without backlog cleanup. Alert sent at most once per cooldown
        window to avoid spam if the breach persists across cycles.
        """
        with correlation_context() as corr_id:
            from core.config import config
            from core.duckdb_store import get_store, evaluate_bronze_invariant

            store = await get_store()
            stats = await store.get_bronze_stats()
            mode = config.sync.mode
            shadow = bool(config.sync.legacy_bronze_shadow)

            healthy, reason = evaluate_bronze_invariant(stats, mode, shadow)

            result = {
                "healthy": healthy,
                "reason": reason,
                "mode": mode,
                "shadow_enabled": shadow,
                "total": stats["total"],
                "unprocessed": stats["unprocessed"],
            }

            if healthy:
                logger.debug(
                    f"Bronze invariant OK: mode={mode}, shadow={shadow}, "
                    f"total={stats['total']:,}"
                )
                return result

            logger.warning(f"Bronze invariant VIOLATED: {reason}")

            # Throttle Telegram alerts so a persistent breach doesn't spam.
            now = time.time()
            since_last = now - BackgroundScheduler._bronze_invariant_last_alert
            if since_last >= self._BRONZE_INVARIANT_ALERT_COOLDOWN_S:
                BackgroundScheduler._bronze_invariant_last_alert = now
                try:
                    from bot.main import send_admin_message
                    await send_admin_message(
                        "⚠️ *Bronze invariant violated*\n"
                        f"`mode={mode}`, `shadow={shadow}`\n"
                        f"total: {stats['total']:,} | unprocessed: {stats['unprocessed']:,}\n\n"
                        f"{reason}\n\n"
                        "Likely cause: prune misconfigured, or sync writing despite opt-out.",
                        key="bronze:invariant_violated",
                    )
                except Exception as e:
                    logger.warning(f"Failed to send bronze invariant alert: {e}")

            return result

    async def _run_bronze_prune(self) -> Dict[str, Any]:
        """Delete old bronze events. Retention rule is mode-aware:
        staging keeps unprocessed; legacy prunes by age only."""
        with correlation_context() as corr_id:
            from core.config import config
            from core.duckdb_store import get_store

            logger.info(f"Starting bronze prune job (mode={config.sync.mode})")
            store = await get_store()

            deleted = await store.prune_bronze_events(
                retention_days=7,
                mode=config.sync.mode,
            )

            result = {"deleted": deleted, "mode": config.sync.mode}
            if deleted > 0:
                logger.info(f"Bronze prune: deleted {deleted} old events ({config.sync.mode} mode)")
            return result

    async def _send_bronze_alert(self, stats: Dict[str, Any]) -> None:
        """Send Telegram alert when bronze backlog is concerning.

        Mode-gated by design: 'unprocessed' is only a meaningful signal in
        staging mode, where the promotion job is what flips processed_at.
        In legacy mode processed_at stays NULL by construction, so a high
        unprocessed count is a config artifact rather than an incident.

        The caller (_run_bronze_promotion) already returns early in legacy
        mode, so this guard is defence-in-depth: it makes the function safe
        to call from any future code path without re-introducing alert spam.
        """
        from core.config import config
        if not config.sync.is_staging:
            logger.debug(
                f"Bronze alert suppressed in {config.sync.mode} mode "
                f"(unprocessed={stats.get('unprocessed')})"
            )
            return

        try:
            from bot.main import send_admin_message
            unprocessed = stats["unprocessed"]
            age_s = stats.get("oldest_unprocessed_age_s")
            age_str = f"{int(age_s)}s" if age_s else "unknown"

            msg = (
                "\u26a0\ufe0f **Bronze Backlog Alert**\n"
                f"Unprocessed events: {unprocessed}\n"
                f"Oldest age: {age_str}\n\n"
                "Promotion may be falling behind. "
                "Check `/api/bronze/stats` and scheduler jobs."
            )
            await send_admin_message(msg, key="bronze:backlog")
        except Exception as e:
            logger.warning(f"Failed to send bronze alert: {e}")

    # ─── Memory Monitor ───────────────────────────────────────────────────────
    #
    # Thresholds, the working-set definition and the evaluator live in
    # core/memory_monitor.py, which explains at length why this no longer
    # judges memory.current. Short version: that number is half page cache on
    # this host, so it tracked database size rather than memory pressure.

    # Cooldowns per level to avoid alert spam (seconds)
    _MEM_ALERT_COOLDOWNS = {
        "WARN": 86400,      # 24 hours
        "CRITICAL": 21600,  # 6 hours
    }
    _mem_last_alert: Dict[str, float] = {}

    @staticmethod
    def _get_db_size_mb() -> Optional[float]:
        """Get DuckDB file size in MB."""
        import pathlib

        for path in [
            pathlib.Path("/app/data/analytics.duckdb"),
            pathlib.Path("data/analytics.duckdb"),
        ]:
            if path.exists():
                return path.stat().st_size / (1024 * 1024)
        return None

    async def _send_admin_telegram(self, message: str) -> None:
        """Send a Telegram message to all admin users."""
        import os

        bot_token = os.getenv("BOT_TOKEN", "")
        admin_str = os.getenv("ADMIN_USER_IDS", "")
        if not bot_token or not admin_str:
            logger.warning("BOT_TOKEN or ADMIN_USER_IDS not set, skipping alert")
            return

        admin_ids = [
            uid.strip() for uid in admin_str.split(",") if uid.strip().isdigit()
        ]

        import httpx

        async with httpx.AsyncClient(timeout=10) as client:
            for uid in admin_ids:
                try:
                    resp = await client.post(
                        f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        json={
                            "chat_id": uid,
                            "text": message,
                            "parse_mode": "HTML",
                        },
                    )
                    if resp.status_code != 200:
                        logger.error(f"Telegram alert to {uid} failed: {resp.text}")
                except Exception as e:
                    logger.error(f"Telegram alert to {uid} error: {e}")

    async def _run_memory_monitor(self) -> Dict[str, Any]:
        """Sample container memory, persist it, and alert on real pressure.

        Persisting is not bookkeeping. The kernel's oom_kill counter resets
        when the container is recreated, so comparing against the previous
        *stored* sample is the only way a kill that happened before a restart
        is still visible afterwards.
        """
        from core.memory_monitor import (
            evaluate_memory,
            fetch_last_sample,
            fetch_peak_working_set_mb,
            insert_sample,
            prune_old_samples,
            read_cgroup_memory,
        )

        mem = read_cgroup_memory()
        if not mem:
            return {"skipped": True, "reason": "not in a cgroup v2 container"}

        previous_oom = None
        peak_24h = None
        try:
            from core.duckdb_store import get_store
            store = await get_store()
            async with store.connection() as conn:
                last = fetch_last_sample(conn)
                previous_oom = last["oom_kills"] if last else None
                insert_sample(conn, mem)
                peak_24h = fetch_peak_working_set_mb(conn, hours=24)
                prune_old_samples(conn, retention_days=14)
        except Exception as e:
            # A memory check that cannot reach the database must still report
            # memory. Losing the OOM-across-restart comparison is the only cost.
            logger.warning(f"Memory sample not persisted: {e}")

        mb = 1024 * 1024
        alert = evaluate_memory(
            working_set_bytes=mem["working_set"],
            page_cache_bytes=mem["page_cache"],
            limit_bytes=mem["limit"],
            oom_kills=mem["oom_kills"],
            previous_oom_kills=previous_oom,
        )

        result = {
            "working_set_mb": round(mem["working_set"] / mb),
            "page_cache_mb": round(mem["page_cache"] / mb),
            "limit_mb": round(mem["limit"] / mb) if mem["limit"] else None,
            "oom_kills": mem["oom_kills"],
            "peak_working_set_mb_24h": round(peak_24h) if peak_24h else None,
            "alert_sent": None,
        }

        db_size = self._get_db_size_mb()
        if db_size:
            result["db_size_mb"] = round(db_size)

        if alert is None:
            pct = (mem["working_set"] / mem["limit"]) if mem["limit"] else 0
            # info, not debug, and it names both halves: the whole point is that
            # the big number and the number that matters are different.
            logger.info(
                f"Memory OK: working set {result['working_set_mb']:,} MB"
                + (f" of {result['limit_mb']:,} MB ({pct:.0%})" if mem["limit"] else "")
                + f", page cache {result['page_cache_mb']:,} MB (not counted)"
            )
            result["status"] = "ok"
            return result

        level = alert.severity.value
        now = datetime.now(SCHEDULER_TIMEZONE).timestamp()
        cooldown = self._MEM_ALERT_COOLDOWNS.get(level, 21600)
        last_sent = self._mem_last_alert.get(level, 0)

        logger.warning(f"Memory {level}: {alert.reason}")

        # An OOM kill is a fact about the past and is never throttled: it can
        # only be reported once per occurrence anyway, since the comparison is
        # against the previous stored sample.
        if alert.oom_kills_delta == 0 and now - last_sent < cooldown:
            result["alert_suppressed"] = level
            return result

        self._mem_last_alert[level] = now
        icon = "\U0001f4a5" if alert.oom_kills_delta else (
            "\u26a0\ufe0f" if level == "WARN" else "\U0001f6a8"
        )
        title = (
            "OOM KILL" if alert.oom_kills_delta
            else ("Memory Warning" if level == "WARN" else "MEMORY CRITICAL")
        )

        lines = [
            f"{icon} <b>{title}</b>",
            "",
            f"<b>Working set:</b> {alert.working_set_mb:,.0f} MB"
            + (f" / {alert.limit_mb:,.0f} MB" if alert.limit_mb else ""),
            f"<b>Free:</b> {alert.headroom_mb:,.0f} MB",
            f"<b>Page cache:</b> {alert.page_cache_mb:,.0f} MB (reclaimable, not counted)",
        ]
        if peak_24h:
            lines.append(f"<b>Peak 24h:</b> {peak_24h:,.0f} MB")
        if db_size:
            lines.append(f"<b>DuckDB file:</b> {db_size:,.0f} MB")
        if alert.oom_kills_delta:
            lines.append(f"<b>Processes killed:</b> {alert.oom_kills_delta}")

        lines += ["", f"<i>{alert.reason}</i>"]
        if level == "CRITICAL":
            lines += [
                "",
                "\U0001f449 Reduce <code>DUCKDB_MEMORY_LIMIT</code> or raise the "
                "container limit. Check what ran: <code>docker logs keycrm-web</code>",
            ]

        await self._send_admin_telegram("\n".join(lines))
        result["alert_sent"] = level
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # EVENT HANDLERS
    # ═══════════════════════════════════════════════════════════════════════════

    def _on_job_executed(self, event: JobExecutionEvent) -> None:
        """Handle successful job execution."""
        job_id = event.job_id
        # _state_lock: APScheduler calls listeners from its own thread,
        # not the asyncio event loop. Lock prevents races with get_jobs().
        with self._state_lock:
            if job_id not in self._job_info:
                return

            info = self._job_info[job_id]
            info.last_run = datetime.now(SCHEDULER_TIMEZONE)
            info.last_status = JobStatus.SUCCESS
            info.run_count += 1

            # Update next run time
            job = self._scheduler.get_job(job_id)
            if job and job.next_run_time:
                info.next_run = job.next_run_time

            # Record execution
            execution = JobExecution(
                job_id=job_id,
                started_at=info.last_run,
                finished_at=datetime.now(SCHEDULER_TIMEZONE),
                status=JobStatus.SUCCESS,
                result=event.retval if hasattr(event, 'retval') else None,
            )
            if execution.finished_at and execution.started_at:
                execution.duration_ms = (
                    execution.finished_at - execution.started_at
                ).total_seconds() * 1000

            self._add_execution(job_id, execution)

    def _on_job_error(self, event: JobExecutionEvent) -> None:
        """Handle job execution error."""
        job_id = event.job_id
        last_error = ""
        with self._state_lock:
            if job_id not in self._job_info:
                return

            info = self._job_info[job_id]
            info.last_run = datetime.now(SCHEDULER_TIMEZONE)
            info.last_status = JobStatus.FAILED
            info.run_count += 1
            info.error_count += 1
            info.last_error = str(event.exception) if event.exception else "Unknown error"
            last_error = info.last_error

            # Update next run time
            job = self._scheduler.get_job(job_id)
            if job and job.next_run_time:
                info.next_run = job.next_run_time

            # Record execution
            execution = JobExecution(
                job_id=job_id,
                started_at=info.last_run,
                finished_at=datetime.now(SCHEDULER_TIMEZONE),
                status=JobStatus.FAILED,
                error=info.last_error,
            )
            self._add_execution(job_id, execution)

        logger.error(
            f"Job {job_id} failed: {last_error}",
            extra={"job_id": job_id, "error": last_error}
        )

    def _on_job_missed(self, event: JobExecutionEvent) -> None:
        """Handle missed job execution."""
        job_id = event.job_id
        with self._state_lock:
            if job_id not in self._job_info:
                return

            info = self._job_info[job_id]
            info.last_status = JobStatus.MISSED

            # Record execution
            execution = JobExecution(
                job_id=job_id,
                started_at=datetime.now(SCHEDULER_TIMEZONE),
                finished_at=datetime.now(SCHEDULER_TIMEZONE),
                status=JobStatus.MISSED,
            )
            self._add_execution(job_id, execution)

        logger.warning(
            f"Job {job_id} missed scheduled execution",
            extra={"job_id": job_id}
        )

    def _add_execution(self, job_id: str, execution: JobExecution) -> None:
        """Add execution to history, keeping only last N."""
        if job_id not in self._job_history:
            self._job_history[job_id] = []

        history = self._job_history[job_id]
        history.append(execution)

        # Trim to max history
        if len(history) > self._max_history:
            self._job_history[job_id] = history[-self._max_history:]

    # ═══════════════════════════════════════════════════════════════════════════
    # PUBLIC API
    # ═══════════════════════════════════════════════════════════════════════════

    def get_jobs(self) -> List[Dict[str, Any]]:
        """Get list of all jobs with their status."""
        with self._state_lock:
            items = list(self._job_info.items())

        jobs = []
        for job_id, info in items:
            # Get trigger description from APScheduler job (outside lock — APScheduler is thread-safe)
            trigger_desc = ""
            job = self._scheduler.get_job(job_id) if self._scheduler else None
            if job and job.trigger:
                trigger_desc = str(job.trigger)

            # Ask APScheduler, don't trust the cached copy. `_add_job` reads
            # next_run_time during registration — before `scheduler.start()`,
            # when it is always None — and only the post-execution listener
            # ever refreshes it. So every job that had not yet run in this
            # process reported "next_run: null", which reads as "this will
            # never fire" for exactly the daily jobs whose silence we have
            # twice mistaken for absence.
            next_run = getattr(job, "next_run_time", None) or info.next_run

            jobs.append({
                "id": info.id,
                "name": info.name,
                "description": info.description,
                "trigger": trigger_desc,
                "next_run": next_run.isoformat() if next_run else None,
                "last_run": info.last_run.isoformat() if info.last_run else None,
                "last_status": info.last_status.value if info.last_status else None,
                "run_count": info.run_count,
                "error_count": info.error_count,
                "last_error": info.last_error,
            })
        return jobs

    def get_job_history(self, job_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get execution history for a job."""
        with self._state_lock:
            if job_id not in self._job_history:
                return []
            history = list(self._job_history[job_id][-limit:])

        return [{
            "started_at": e.started_at.isoformat() if e.started_at else None,
            "finished_at": e.finished_at.isoformat() if e.finished_at else None,
            "status": e.status.value,
            "duration_ms": e.duration_ms,
            "error": e.error,
        } for e in reversed(history)]

    async def run_job_now(self, job_id: str) -> Dict[str, Any]:
        """Manually trigger a job to run immediately."""
        if job_id not in self._job_info:
            raise ValueError(f"Unknown job: {job_id}")

        job = self._scheduler.get_job(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        # Run the job immediately
        logger.info(f"Manually triggering job: {job_id}")
        job.modify(next_run_time=datetime.now(SCHEDULER_TIMEZONE))

        return {"status": "triggered", "job_id": job_id}

    # Alias for API compatibility
    async def trigger_job(self, job_id: str) -> Dict[str, Any]:
        """Alias for run_job_now (API compatibility)."""
        return await self.run_job_now(job_id)

    def pause_job(self, job_id: str) -> None:
        """Pause a job."""
        if job_id not in self._job_info:
            raise ValueError(f"Unknown job: {job_id}")

        self._scheduler.pause_job(job_id)
        logger.info(f"Paused job: {job_id}")

    def resume_job(self, job_id: str) -> None:
        """Resume a paused job."""
        if job_id not in self._job_info:
            raise ValueError(f"Unknown job: {job_id}")

        self._scheduler.resume_job(job_id)
        logger.info(f"Resumed job: {job_id}")

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the scheduler."""
        if self._scheduler and self._started:
            self._scheduler.shutdown(wait=wait)
            self._started = False
            logger.info("Background scheduler stopped")

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._started and self._scheduler is not None


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_scheduler: Optional[BackgroundScheduler] = None


def get_scheduler() -> BackgroundScheduler:
    """Get the singleton scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler()
    return _scheduler


async def start_scheduler() -> BackgroundScheduler:
    """Start the background scheduler."""
    scheduler = get_scheduler()
    await scheduler.start()
    return scheduler


def stop_scheduler() -> None:
    """Stop the background scheduler."""
    global _scheduler
    if _scheduler:
        _scheduler.shutdown()
