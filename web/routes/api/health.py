"""Health check, metrics, and DuckDB stats endpoints.

Authentication is enforced by ``api_gate`` at the /api include level: only
``/api/health`` is listed in ``PUBLIC_API_PATHS`` so it is reachable without
a session (Docker / nginx / uptime monitors). The detailed / stats / metrics
endpoints are gated by api_gate like every other /api/* endpoint.
"""
import asyncio
import time

from fastapi import APIRouter, Request

from core.observability import get_correlation_id, metrics, Timer
from web.config import VERSION
from web.schemas import HealthResponse, MetricsResponse
from ._deps import limiter, get_store, get_logger, START_TIME

router = APIRouter()
logger = get_logger(__name__)

# Health check stats cache (60 second TTL) with thread-safe lock
_stats_cache: dict = {"data": None, "expires_at": 0}
_stats_cache_lock = asyncio.Lock()
_STATS_CACHE_TTL = 60

# The mirror watermarks ride their own cache on the same TTL. Separate from the
# stats cache above because they come from a different database: folding them in
# would mean one Postgres hiccup blanking the DuckDB block, or the reverse.
_mirror_cache: dict = {"data": None, "expires_at": 0}
_mirror_cache_lock = asyncio.Lock()


async def _mirror_freshness() -> "dict | None":
    """Age of the last successful shipment per watched mirror table.

    Returns None — never an empty dict — when it cannot be told, because the
    canary treats a missing block as a failure (rule 3: silence is not health)
    and an empty dict would read as "asked, nothing watched".
    """
    now = time.time()
    async with _mirror_cache_lock:
        if _mirror_cache["data"] is not None and now < _mirror_cache["expires_at"]:
            return _mirror_cache["data"]
        try:
            from core import pg_derivation
            from core.mirror_reconciliation import WATCHED_MIRRORS, fetch_mirror_freshness
            from core.pg import get_pool

            # Under KS_PG_DERIVE=own the derived layers are watched too, with
            # the limit declared here: a derivation that simply stops being
            # triggered raises nothing anywhere, and only its watermarks age.
            owned = pg_derivation.owns()
            tables = WATCHED_MIRRORS + (pg_derivation.DERIVED_TABLES if owned else ())
            data = await fetch_mirror_freshness(await get_pool(), tables=tables)
            if owned:
                for table in pg_derivation.DERIVED_TABLES:
                    data[table]["max_age_s"] = pg_derivation.DERIVED_MAX_AGE_S
        except Exception as e:
            # A host with no Postgres configured raises here on every call, and
            # that is not an error worth a warning every minute.
            logger.debug(f"Mirror freshness unavailable: {e}")
            return None
        _mirror_cache["data"] = data
        _mirror_cache["expires_at"] = now + _STATS_CACHE_TTL
        return data


def _write_chains() -> dict:
    """Each write chain's KS_WRITE_* as understood now, and whether the chain
    has already written Postgres. Local state: the environment, plus the latch,
    which is a cached read of the marker files beside the DuckDB database and
    never a query — that is what lets this block answer while Postgres is down.

    Judged by the canary: a value not understood stops that chain's writers,
    and `mismatch` says the chain owns its tables in Postgres while its
    variable says otherwise (DN-06). The two copies of the latch are compared
    against each other by the daily `reconcile_operational`, which is the only
    thing that can read both."""
    from core.write_chains import chain_modes

    return chain_modes()


def _read_fallbacks() -> dict:
    """`{surface: {count, last_at}}` — every read this process answered from
    DuckDB because the engine it was sent to failed (DN-20a). Local state, no
    I/O, and no exception text: this endpoint is public, and the counts and a
    timestamp are what a reader needs to go and look at the log."""
    from core import read_fallback

    return read_fallback.counts()


def _read_fallback_mode() -> dict:
    """KS_READ_FALLBACK as this process understood it at start, the error when
    it was not understood, and every read switch naming an engine this process
    has no address for. Local state, no I/O."""
    from core import read_fallback

    return {
        "mode": read_fallback.mode(),
        "error": read_fallback.mode_error(),
        "misconfigured": read_fallback.misconfigured(),
    }


# Chain 1's answer to "may it be switched to Postgres now?" (DN-24), on the same
# TTL as the watermarks. Its own cache, because it is the one part of the
# `write_chains` block that reads Postgres: the rest is local state and must
# keep answering while Postgres cannot.
_preflight_cache: dict = {"data": None, "expires_at": 0}
_preflight_cache_lock = asyncio.Lock()

# /api/health must answer; a Postgres that neither answers nor refuses would
# otherwise hold it for the pool's connect timeout.
_PREFLIGHT_TIMEOUT_S = 5


async def _inventory_preflight() -> dict:
    """`pg_inventory_write.preflight()`, cached. Never raises: a Postgres that
    does not answer in time is an answer of its own, `ok: false`."""
    from core import pg_inventory_write

    now = time.time()
    async with _preflight_cache_lock:
        if _preflight_cache["data"] is not None and now < _preflight_cache["expires_at"]:
            return _preflight_cache["data"]
        try:
            data = await asyncio.wait_for(
                pg_inventory_write.preflight(), _PREFLIGHT_TIMEOUT_S)
        except asyncio.TimeoutError:
            data = {"ok": False, "reasons": [
                f"Postgres did not answer within {_PREFLIGHT_TIMEOUT_S} s"]}
        _preflight_cache["data"] = data
        _preflight_cache["expires_at"] = now + _STATS_CACHE_TTL
        return data


async def _inventory_sync_step() -> "dict | None":
    """What chain 1's half of the sync tick last did on the Postgres path
    (DN-24): consecutive failures, the step and error class of the last one,
    and when the next attempt is allowed. Local state, no I/O. Null when the
    sync service cannot be had, never an empty object."""
    try:
        from core.sync_service import get_sync_service

        return (await get_sync_service()).inventory_step_health()
    except Exception as e:
        logger.debug(f"Inventory sync step unavailable: {e}")
        return None


async def _write_chains_block() -> dict:
    """The `write_chains` block: every chain's local state, and under chain 1's
    entry its `preflight` — the three questions asked before
    `KS_WRITE_INVENTORY` is switched on, `ok` null once the chain already
    writes Postgres — and its `sync_step`, where a Postgres failure of the
    offers or stocks step is recorded instead of ending the tick. Neither is
    judged by the canary: the preflight is read by the person about to flip
    the chain, and a stock step that keeps failing stops `last_sync_stocks`,
    which the integrity job's chain invariants already watch."""
    from core import pg_inventory_write

    block = _write_chains()
    entry = block.get(pg_inventory_write.CHAIN)
    if isinstance(entry, dict):
        entry["preflight"] = await _inventory_preflight()
        entry["sync_step"] = await _inventory_sync_step()
    return block


def _warehouse_writer_mode() -> dict:
    """KS_WRITE_WAREHOUSE as this process understood it at start (DN-28): the
    value as read, the mode it runs, and the error when the value was not
    understood and ran as duckdb. Local state, no I/O, and nothing but the
    variable's own value in the error. Judged by the canary: in this build the
    variable switches nothing, so a typo costs nothing today — the day it
    would is the DN-29 flip, and that is not the day to learn of it."""
    from core import warehouse_cutover

    return {"mode": warehouse_cutover.mode(), "value": warehouse_cutover.value(),
            "error": warehouse_cutover.mode_error()}


def _derivation_mode() -> dict:
    """KS_PG_DERIVE as this process understood it at start. Local state, no I/O."""
    from core import pg_derivation

    return {"mode": pg_derivation.mode(), "error": pg_derivation.mode_error()}


# Marks dropped and not yet covered by a validated rebuild, on the same TTL as
# the watermarks. Its own cache rather than a field of theirs: "nothing
# dropped" is an answer here and must be cached, where the mirror block caches
# only what it could read. The stamp is cached, not the age, so the age is
# never a minute stale against the threshold it is judged by.
_marks_cache: dict = {"data": None, "expires_at": 0}
_marks_cache_lock = asyncio.Lock()
_NOT_ASKED = {"marks_dropped_unhealed": None, "last_mark_drop_age_s": None}


async def _dropped_marks() -> dict:
    """`marks_dropped_unhealed` and `last_mark_drop_age_s` for the derivation block.

    A mark that could not be raised leaves its rows landed and nothing owed, so
    the loss shows only in the signal's watermark: `failures_since_ok` counts
    the drops since a validated rebuild last covered them
    (`pg_derivation.heal_dropped_marks`), and `last_attempted_at` is when the
    latest one was recorded. The age is published only while the count is above
    zero — a heal moves the stamp too, and its age would read as a drop.

    The canary judges the pair, not the count: a drop the next rebuild will
    cover is not a finding, a drop older than any rebuild should take is.

    Both null when it is not a question — under piggyback nothing marks and
    nothing heals, so a count left over from an earlier soak would page for
    ever — and when Postgres cannot be read. A signal that has never dropped a
    mark has no row, and that is a count of 0.
    """
    from datetime import datetime, timezone

    from core import pg_derivation

    if not pg_derivation.owns():
        return dict(_NOT_ASKED)
    now = time.time()
    async with _marks_cache_lock:
        if now < _marks_cache["expires_at"]:
            count, dropped_at = _marks_cache["data"]
        else:
            try:
                from core.pg import get_pool

                pool = await get_pool()
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT failures_since_ok, last_attempted_at"
                        " FROM meta.mirror_state WHERE table_name = $1",
                        pg_derivation.SIGNAL_TABLE,
                    )
            except Exception as e:
                logger.debug(f"Dropped derivation marks unavailable: {e}")
                return dict(_NOT_ASKED)
            count = int(row["failures_since_ok"] or 0) if row else 0
            dropped_at = row["last_attempted_at"] if row and count > 0 else None
            _marks_cache["data"] = (count, dropped_at)
            _marks_cache["expires_at"] = now + _STATS_CACHE_TTL
    age = (int((datetime.now(timezone.utc) - dropped_at).total_seconds())
           if dropped_at is not None else None)
    return {"marks_dropped_unhealed": count, "last_mark_drop_age_s": age}


def _signal_reads() -> dict:
    """`signal_unreadable_ticks`: consecutive ticks of this process that could
    not read `meta.derivation_signal` (DN-05a). Local state, no I/O — the one
    number in this block that must answer while Postgres cannot, because that
    is when it is not zero. Null under piggyback, where the job that counts is
    not registered and a zero would claim a watch nobody keeps. Web pages on it
    itself (`warehouse_pg:signal_unreadable`); it is published so that the
    minutes before the page, and a page that could not be delivered, are
    visible from outside."""
    from core import pg_derivation

    if not pg_derivation.owns():
        return {"signal_unreadable_ticks": None}
    return {"signal_unreadable_ticks": pg_derivation.signal_unreadable_ticks()}


async def _derivation_block() -> dict:
    """The `derivation` block: the mode, its error and the unreadable-signal
    count, which are local state, plus the two numbers in it that have to be
    read out of Postgres."""
    return {**_derivation_mode(), **_signal_reads(), **await _dropped_marks()}


@router.get("/health", response_model=HealthResponse)
@limiter.limit("60/minute")
async def health_check(request: Request):
    """Health check endpoint for Docker/load balancer monitoring."""
    uptime_seconds = int(time.time() - START_TIME)

    now = time.time()
    async with _stats_cache_lock:
        if _stats_cache["data"] and now < _stats_cache["expires_at"]:
            duckdb_stats = _stats_cache["data"]
            duckdb_status = "connected"
            db_latency_ms = 0.0
        else:
            db_latency_ms = None
            try:
                with Timer("health_check_db") as timer:
                    store = await get_store()
                    duckdb_stats = await store.get_stats()
                duckdb_status = "connected"
                db_latency_ms = round(timer.elapsed_ms, 2)
                _stats_cache["data"] = duckdb_stats
                _stats_cache["expires_at"] = now + _STATS_CACHE_TTL
            except Exception as e:
                # /api/health is public — don't leak DuckDB exception text
                # (paths, PIDs, schema fragments) to anonymous probes.
                logger.warning(f"Health check DuckDB error: {e}")
                duckdb_stats = None
                duckdb_status = "error"

    sync_status = None
    try:
        from core.sync_service import get_sync_service
        sync_service = await get_sync_service()
        sync_stats = sync_service.get_sync_stats()

        seconds_since_sync = None
        if sync_stats.get("last_sync_time"):
            from datetime import datetime
            from core.config import DEFAULT_TZ
            last_sync = datetime.fromisoformat(sync_stats["last_sync_time"])
            seconds_since_sync = int((datetime.now(DEFAULT_TZ) - last_sync).total_seconds())

        if seconds_since_sync is None:
            status = "idle"
        elif seconds_since_sync > 900:
            status = "stale"
        else:
            status = "active"

        sync_status = {
            "status": status,
            "last_sync_time": sync_stats.get("last_sync_time"),
            "seconds_since_sync": seconds_since_sync,
            "consecutive_empty_syncs": sync_stats.get("consecutive_empty_syncs", 0),
            "current_backoff_seconds": sync_stats.get("current_backoff_seconds", 300),
            "is_off_hours": sync_stats.get("is_off_hours", False),
        }
    except Exception as e:
        logger.debug(f"Could not get sync status: {e}")

    from core.config import config as app_config

    # Data-quality freshness rides along in the cached stats; lift it to the
    # top level so the canary reads it without parsing the duckdb block.
    # Its absence is meaningful (bot/canary.py treats a missing block as a
    # failure), so never substitute an empty dict for "we could not tell".
    stats = dict(duckdb_stats or {})
    data_quality = stats.pop("data_quality", None)

    # The schema ledger. A migration that fails is retried on the next boot and
    # never recorded as applied, so it cannot be skipped past — but somebody has
    # to be able to see it without reading container logs, which is this.
    try:
        store = await get_store()
        migrations = dict(store.schema_status())
    except Exception as e:
        migrations = {"status": "unknown", "error": str(e)}

    # Same rule as the DuckDB block above, for the same reason: the ledger
    # reports a failure to *read* it as raw exception text, which on this
    # database means the file path, a pid and the user the container runs as.
    # `status` still says "unknown", which is the part the reader acts on.
    # `migrations["failed"]` is untouched — naming which migration blew up and
    # why is what the ledger is for.
    ledger_error = migrations.pop("error", None)
    if ledger_error:
        logger.warning(f"Health check schema ledger error: {ledger_error}")

    # The copy that carries the money. Its own watchdog lives in bot/canary.py,
    # out of this container — a mirror that stopped shipping used to wait for
    # the 07:30 comparison, which is a whole day of silence at the main copy.
    mirrors = await _mirror_freshness()

    # The alerting machinery watching itself: consecutive transport failures
    # in THIS process, judged by the canary from the other container. The one
    # subsystem that had no dead-man's switch — which is how the certificate
    # alert stayed undeliverable for months.
    from core.telegram_alerts import transport_health

    alerting = transport_health()

    # The alerting machinery watching itself: consecutive transport failures
    # in THIS process, judged by the canary from the other container. The one
    # subsystem that had no dead-man's switch — which is how the certificate
    # alert stayed undeliverable for months.
    from core.telegram_alerts import transport_health

    alerting = transport_health()

    return {
        "status": (
            "degraded" if not duckdb_stats or migrations.get("status") == "failed"
            else "healthy"
        ),
        "version": VERSION,
        "uptime_seconds": uptime_seconds,
        "correlation_id": get_correlation_id(),
        "duckdb": {
            "status": duckdb_status,
            "latency_ms": db_latency_ms,
            **stats
        },
        "migrations": migrations,
        "sync": sync_status,
        "data_quality": data_quality,
        "mirrors": mirrors,
        "alerting": alerting,
        "derivation": await _derivation_block(),
        "write_chains": await _write_chains_block(),
        "read_fallbacks": _read_fallbacks(),
        "read_fallback_mode": _read_fallback_mode(),
        "warehouse_writer_mode": _warehouse_writer_mode(),
    }


@router.get("/health/detailed")
@limiter.limit("30/minute")
async def detailed_health_check(request: Request):
    """Detailed health check with component-level status."""
    components = {}
    overall_status = "healthy"

    # 1. DuckDB check
    try:
        with Timer("health_duckdb") as timer:
            store = await get_store()
            duckdb_stats = await store.get_stats()
        components["duckdb"] = {
            "status": "connected",
            "latency_ms": round(timer.elapsed_ms, 2),
            **duckdb_stats,
        }
    except Exception as e:
        components["duckdb"] = {"status": "error", "error": str(e)}
        overall_status = "degraded"

    # 2. Meilisearch check
    try:
        import httpx
        import os
        meili_url = os.getenv("MEILI_URL", "http://meilisearch:7700")
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(f"{meili_url}/health")
            if response.status_code == 200:
                components["meilisearch"] = {"status": "healthy"}
            else:
                components["meilisearch"] = {"status": "unhealthy", "code": response.status_code}
    except Exception as e:
        components["meilisearch"] = {"status": "unavailable", "error": str(e)}

    # 4. WebSocket connections
    try:
        from core.websocket_manager import manager as ws_manager
        ws_stats = ws_manager.get_stats()
        components["websocket"] = {"status": "active", **ws_stats}
    except Exception as e:
        components["websocket"] = {"status": "error", "error": str(e)}

    # 5. Sync service status
    try:
        from core.sync_service import get_sync_service
        sync_service = await get_sync_service()
        sync_stats = sync_service.get_sync_stats()
        components["sync"] = {
            "status": "active" if sync_stats.get("last_sync_time") else "idle",
            **sync_stats,
        }
    except Exception as e:
        components["sync"] = {"status": "error", "error": str(e)}

    # 6. Prediction service
    try:
        from core.prediction_service import get_prediction_service
        pred_service = get_prediction_service()
        components["prediction"] = {
            "status": "ready" if pred_service.is_ready else "not_ready",
            "model_loaded": pred_service.is_ready,
        }
    except Exception as e:
        components["prediction"] = {"status": "unavailable", "error": str(e)}

    # System metrics
    uptime_seconds = int(time.time() - START_TIME)
    try:
        # Imported here rather than at the top of the handler. psutil is not in
        # requirements.txt and never has been, so a module-level import made
        # every call to this endpoint a 500 — while the block below was already
        # written to fall back to uptime alone when the metrics cannot be read.
        # The guard existed; the import stood outside it.
        import psutil

        process = psutil.Process()
        memory_info = process.memory_info()
        sys_metrics = {
            "uptime_seconds": uptime_seconds,
            "memory_mb": round(memory_info.rss / 1024 / 1024, 1),
            "memory_percent": round(process.memory_percent(), 1),
            "cpu_percent": round(process.cpu_percent(interval=0.1), 1),
            "threads": process.num_threads(),
        }
    except Exception:
        sys_metrics = {"uptime_seconds": uptime_seconds}

    return {
        "status": overall_status,
        "version": VERSION,
        "correlation_id": get_correlation_id(),
        "components": components,
        "metrics": sys_metrics,
    }


@router.get("/duckdb/stats")
@limiter.limit("60/minute")
async def get_duckdb_stats(request: Request):
    """Get DuckDB analytics store statistics."""
    try:
        store = await get_store()
        stats = await store.get_stats()
        return {"status": "connected", **stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/health/data-quality")
@limiter.limit("60/minute")
async def get_data_quality_health(request: Request):
    """Latest Data Quality run summaries (integrity + reconciliation).

    Returns the most recent row from data_quality_runs for each layer, so
    on-call can see at a glance:
      - status: PASS / WARN / CRITICAL / FAILED
      - last_run: when the watchdog last produced a verdict
      - counts: how many issues / discrepancies were found
    """
    from core.data_quality import fetch_latest_run, fetch_run_diffs, fetch_run_issues

    try:
        store = await get_store()
        async with store.connection() as conn:
            integrity = fetch_latest_run(conn, layer="integrity")
            reconciliation = fetch_latest_run(conn, layer="reconciliation")

            # Include top-N drilldown for the recon run so admins can see
            # WHICH (month, source) drifted without making a second call.
            reconciliation_diffs = []
            if reconciliation:
                reconciliation_diffs = fetch_run_diffs(
                    conn, reconciliation["run_id"], limit=20,
                )
            integrity_issues = []
            if integrity:
                integrity_issues = fetch_run_issues(
                    conn, integrity["run_id"], limit=20,
                )

            # The two layers that arrived after this endpoint was written.
            # Found by an audit standing exactly where on-call would stand: a
            # WARN verdict in the mirror-landing log line, and no way to see
            # WHICH findings without opening the database — which the
            # single-writer rule forbids from outside the process. The layer
            # holding the most comparisons must not be the one invisible here.
            mirror_landing = fetch_latest_run(conn, layer="mirror_landing")
            mirror_issues = []
            if mirror_landing:
                mirror_issues = fetch_run_issues(
                    conn, mirror_landing["run_id"], limit=20,
                )
            reconciliation_pg = fetch_latest_run(conn, layer="reconciliation_pg")
            reconciliation_pg_diffs = []
            if reconciliation_pg:
                reconciliation_pg_diffs = fetch_run_diffs(
                    conn, reconciliation_pg["run_id"], limit=20,
                )

        return {
            "integrity": {
                "last_run": integrity,
                "issues": integrity_issues,
            },
            "reconciliation": {
                "last_run": reconciliation,
                "diffs": reconciliation_diffs,
            },
            "mirror_landing": {
                "last_run": mirror_landing,
                "issues": mirror_issues,
            },
            "reconciliation_pg": {
                "last_run": reconciliation_pg,
                "diffs": reconciliation_pg_diffs,
            },
        }
    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}


@router.get("/metrics", response_model=MetricsResponse)
@limiter.limit("60/minute")
async def get_metrics_endpoint(request: Request):
    """Get application metrics."""
    return {
        "uptime_seconds": int(time.time() - START_TIME),
        "correlation_id": get_correlation_id(),
        **metrics.get_stats(),
    }
