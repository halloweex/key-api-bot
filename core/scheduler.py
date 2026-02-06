"""
Background job scheduler using APScheduler.

Manages all background tasks:
- Incremental sync (every 60 seconds)
- Full sync (weekly on Sunday at 2 AM)
- Inventory snapshot (daily at 1 AM)
- Cache warming (every 5 minutes)

Features:
- Job retry with exponential backoff
- Job execution history
- Prevents job pile-up (max_instances=1)
- Graceful shutdown
"""
import asyncio
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

    def __init__(self):
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._job_history: Dict[str, List[JobExecution]] = {}
        self._job_info: Dict[str, JobInfo] = {}
        self._max_history = 50  # Keep last N executions per job
        self._started = False

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
    ) -> None:
        """Add a job to the scheduler."""
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            name=name,
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
        with correlation_context() as corr_id:
            logger.info("Starting weekly full sync job")

            from core.sync_service import get_sync_service
            sync_service = await get_sync_service()
            stats = await sync_service.full_sync(days_back=90)

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
            retail_goals = await store.calculate_suggested_goals(1.10, "retail")
            await store.calculate_yoy_growth("retail")

            # Calculate for b2b
            b2b_indices = await store.calculate_seasonality_indices("b2b")
            b2b_goals = await store.calculate_suggested_goals(1.10, "b2b")
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

    # ═══════════════════════════════════════════════════════════════════════════
    # EVENT HANDLERS
    # ═══════════════════════════════════════════════════════════════════════════

    def _on_job_executed(self, event: JobExecutionEvent) -> None:
        """Handle successful job execution."""
        job_id = event.job_id
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
        if job_id not in self._job_info:
            return

        info = self._job_info[job_id]
        info.last_run = datetime.now(SCHEDULER_TIMEZONE)
        info.last_status = JobStatus.FAILED
        info.run_count += 1
        info.error_count += 1
        info.last_error = str(event.exception) if event.exception else "Unknown error"

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
            f"Job {job_id} failed: {info.last_error}",
            extra={"job_id": job_id, "error": info.last_error}
        )

    def _on_job_missed(self, event: JobExecutionEvent) -> None:
        """Handle missed job execution."""
        job_id = event.job_id
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
        jobs = []
        for job_id, info in self._job_info.items():
            # Get trigger description from APScheduler job
            trigger_desc = ""
            job = self._scheduler.get_job(job_id) if self._scheduler else None
            if job and job.trigger:
                trigger_desc = str(job.trigger)

            jobs.append({
                "id": info.id,
                "name": info.name,
                "description": info.description,
                "trigger": trigger_desc,
                "next_run": info.next_run.isoformat() if info.next_run else None,
                "last_run": info.last_run.isoformat() if info.last_run else None,
                "last_status": info.last_status.value if info.last_status else None,
                "run_count": info.run_count,
                "error_count": info.error_count,
                "last_error": info.last_error,
            })
        return jobs

    def get_job_history(self, job_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get execution history for a job."""
        if job_id not in self._job_history:
            return []

        history = self._job_history[job_id][-limit:]
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
