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
import threading
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

        # Job: Reconciliation check (daily at 6 AM)
        # Compares order counts between DuckDB and KeyCRM API for last 14 days
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

    async def _run_bronze_prune(self) -> Dict[str, Any]:
        """Delete processed bronze events older than 7 days."""
        with correlation_context() as corr_id:
            logger.info("Starting bronze prune job")

            from core.duckdb_store import get_store
            store = await get_store()

            deleted = await store.prune_bronze_events(retention_days=7)

            result = {"deleted": deleted}
            if deleted > 0:
                logger.info(f"Bronze prune: deleted {deleted} old events")
            return result

    async def _send_bronze_alert(self, stats: Dict[str, Any]) -> None:
        """Send Telegram alert when bronze backlog is concerning."""
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
            await send_admin_message(msg)
        except Exception as e:
            logger.warning(f"Failed to send bronze alert: {e}")

    # ─── Memory Monitor ───────────────────────────────────────────────────────

    # Thresholds (fraction of container memory limit)
    _MEM_WARN_THRESHOLD = 0.75   # 75% → warning (once per 24h)
    _MEM_CRITICAL_THRESHOLD = 0.90  # 90% → critical (once per 6h)

    # Cooldowns per level to avoid alert spam (seconds)
    _MEM_ALERT_COOLDOWNS = {
        "warning": 86400,   # 24 hours
        "critical": 21600,  # 6 hours
    }
    _mem_last_alert: Dict[str, float] = {}

    @staticmethod
    def _read_cgroup_memory() -> Optional[Dict[str, int]]:
        """Read memory stats from cgroup v2 filesystem.

        Returns dict with current, peak, max (bytes) or None if not in a container.
        """
        import pathlib

        try:
            cgroup = pathlib.Path("/sys/fs/cgroup")
            current = int((cgroup / "memory.current").read_text().strip())
            max_mem = (cgroup / "memory.max").read_text().strip()
            max_bytes = int(max_mem) if max_mem != "max" else None

            # Peak since container start
            peak_path = cgroup / "memory.peak"
            peak = int(peak_path.read_text().strip()) if peak_path.exists() else current

            # OOM event count
            events_path = cgroup / "memory.events"
            oom_count = 0
            if events_path.exists():
                for line in events_path.read_text().splitlines():
                    if line.startswith("oom_kill "):
                        oom_count = int(line.split()[1])

            return {
                "current": current,
                "peak": peak,
                "max": max_bytes,
                "oom_kills": oom_count,
            }
        except (FileNotFoundError, PermissionError, ValueError):
            return None

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
        """Check container memory usage and alert admin if thresholds crossed."""
        mem = self._read_cgroup_memory()
        if not mem or not mem["max"]:
            return {"skipped": True, "reason": "not in cgroup or no limit"}

        current = mem["current"]
        peak = mem["peak"]
        limit = mem["max"]
        usage_pct = current / limit
        peak_pct = peak / limit

        result = {
            "current_mb": round(current / (1024 * 1024)),
            "peak_mb": round(peak / (1024 * 1024)),
            "limit_mb": round(limit / (1024 * 1024)),
            "usage_pct": round(usage_pct * 100, 1),
            "peak_pct": round(peak_pct * 100, 1),
            "oom_kills": mem["oom_kills"],
        }

        db_size = self._get_db_size_mb()
        if db_size:
            result["db_size_mb"] = round(db_size)

        now = datetime.now(SCHEDULER_TIMEZONE).timestamp()

        # Determine alert level from current usage. memory.peak in cgroup v2
        # is monotonic for the container lifetime, so triggering on peak_pct
        # fires every 24h forever once it crosses the threshold — even if
        # current usage has dropped back to normal.
        level = None
        if usage_pct >= self._MEM_CRITICAL_THRESHOLD:
            level = "critical"
        elif usage_pct >= self._MEM_WARN_THRESHOLD:
            level = "warning"

        if level:
            cooldown = self._MEM_ALERT_COOLDOWNS[level]
            last = self._mem_last_alert.get(level, 0)

            if now - last >= cooldown:
                self._mem_last_alert[level] = now

                cur_gb = current / (1024 ** 3)
                peak_gb = peak / (1024 ** 3)
                limit_gb = limit / (1024 ** 3)
                headroom_gb = (limit - peak) / (1024 ** 3)

                icon = "\u26a0\ufe0f" if level == "warning" else "\U0001f6a8"
                title = "Memory Warning" if level == "warning" else "MEMORY CRITICAL"

                lines = [
                    f"{icon} <b>{title}</b>",
                    "",
                    f"<b>Current:</b> {cur_gb:.1f} GB / {limit_gb:.1f} GB ({usage_pct:.0%})",
                    f"<b>Peak:</b> {peak_gb:.1f} GB ({peak_pct:.0%})",
                    f"<b>Headroom:</b> {headroom_gb:.1f} GB",
                ]

                if db_size:
                    lines.append(f"<b>DuckDB:</b> {db_size:.0f} MB")

                if mem["oom_kills"] > 0:
                    lines.append(f"\U0001f4a5 <b>OOM kills:</b> {mem['oom_kills']}")

                if level == "critical":
                    lines += [
                        "",
                        "\U0001f449 <b>Action needed:</b> upgrade Hetzner instance",
                        "or reduce DuckDB <code>memory_limit</code>",
                    ]
                else:
                    lines += [
                        "",
                        "\U0001f4c8 Memory trending high — plan upgrade soon",
                    ]

                await self._send_admin_telegram("\n".join(lines))
                result["alert_sent"] = level
                logger.warning(f"Memory {level}: {usage_pct:.0%} used, peak {peak_pct:.0%}")
            else:
                result["alert_suppressed"] = level
        else:
            result["status"] = "ok"

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
