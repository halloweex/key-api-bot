"""Health check and metrics endpoints.

Authentication is enforced by ``api_gate`` at the /api include level: only
``/api/health`` is listed in ``PUBLIC_API_PATHS`` so it is reachable without
a session (Docker / nginx / uptime monitors). The detailed / stats / metrics
endpoints are gated by api_gate like every other /api/* endpoint.
"""
import asyncio
import time

from fastapi import APIRouter, Request

from core.observability import get_correlation_id, metrics
from web.config import VERSION
from web.schemas import HealthResponse, MetricsResponse
from ._deps import limiter, get_logger, START_TIME

router = APIRouter()
logger = get_logger(__name__)

# The TTL every block below that reads a database is cached on. (A cache of
# DuckDB's row counts, `_stats_cache`, stood beside it until step 14's S14-17,
# with the `duckdb` block it fed.)
_STATS_CACHE_TTL = 60

# The mirror watermarks, on that TTL.
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
            from core import pg_derivation, pg_utm_parse
            from core.mirror_reconciliation import WATCHED_MIRRORS, fetch_mirror_freshness
            from core.pg import get_pool

            # The derived layers are watched too, with the limit declared
            # here: a derivation that simply stops being triggered raises
            # nothing anywhere, and only its watermarks age. And the UTM
            # table, whose watermark is the parse's liveness stamp (DN-19):
            # the derivation's last step, in a try of its own, so it can stop
            # while Silver and Gold stay fresh. Both unconditionally since
            # step 14's S14-15, which removed KS_PG_DERIVE and KS_UTM_PARSE
            # (`own` and `postgres` since step 13).
            tables = (WATCHED_MIRRORS + pg_derivation.DERIVED_TABLES
                      + (pg_utm_parse.UTM_TABLE,))
            data = await fetch_mirror_freshness(await get_pool(), tables=tables)
            for table in pg_derivation.DERIVED_TABLES:
                data[table]["max_age_s"] = pg_derivation.DERIVED_MAX_AGE_S
            data[pg_utm_parse.UTM_TABLE]["max_age_s"] = pg_utm_parse.MAX_AGE_S
        except Exception as e:
            # A host with no Postgres configured raises here on every call, and
            # that is not an error worth a warning every minute.
            logger.debug(f"Mirror freshness unavailable: {e}")
            return None
        _mirror_cache["data"] = data
        _mirror_cache["expires_at"] = now + _STATS_CACHE_TTL
        return data


def _buyer_sync() -> "dict | None":
    """The buyers step's state, read from the sync service already running.

    Never constructs one: a health probe must not start the thing it reports
    on, and before the first sync there is simply nothing to say — None, which
    the canary reads as "not judged" rather than as a failure. Local state
    only, so it answers while either store is down.
    """
    from core import sync_service

    service = sync_service._sync_service
    if service is None:
        return None
    try:
        return service.buyer_sync_health()
    except Exception as e:  # noqa: BLE001 — a health probe never fails on this
        logger.warning(f"buyer_sync block unavailable: {type(e).__name__}")
        return None


_buyer_watermark_cache: dict = {"age": None, "expires_at": 0.0}


async def _buyer_sync_block() -> "dict | None":
    """`_buyer_sync()`, plus `watermark_age_s`: the age of the stamp the step
    writes to Postgres on every completion (chain 4; until step 14's S14-11
    only while the chain's `mode()` said postgres). The canary's chain
    CRITICAL judges the older of the two, because the local clock is floored
    at the process start and every recreate would otherwise reset a stall and
    announce it resolved (review of PR-3). Read at most once a minute; a
    store that cannot be read publishes None, and the canary falls back to
    the local clock."""
    from core import pg_buyer_sync_read

    block = _buyer_sync()
    if block is None:
        return block
    now = time.time()
    if now >= _buyer_watermark_cache["expires_at"]:
        try:
            _buyer_watermark_cache["age"] = await pg_buyer_sync_read.watermark_age_s()
        except Exception as e:  # noqa: BLE001 — a health probe never fails on this
            logger.debug(f"buyers watermark unavailable: {type(e).__name__}")
            _buyer_watermark_cache["age"] = None
        _buyer_watermark_cache["expires_at"] = now + _STATS_CACHE_TTL
        age = _buyer_watermark_cache["age"]
    else:
        # Aged by the time since it was read, so a cached value never reads
        # younger than the stamp it came from.
        cached = _buyer_watermark_cache["age"]
        read_at = _buyer_watermark_cache["expires_at"] - _STATS_CACHE_TTL
        age = None if cached is None else cached + int(now - read_at)
    return {**block, "watermark_age_s": age}


def _write_chains() -> dict:
    """`{chain: {}}` for every registered chain — the entries the block's
    step states and spools are written into. Local state, no I/O.

    Until step 14's S14-11 each entry was the chain's KS_WRITE_* as
    understood, its mode, its latch and the mismatch between them
    (`write_chains.chain_modes()`), judged by the canary as
    `write_chain_flag_invalid`, `write_chain_flag_mismatch` and
    `write_chain_precondition_unmet`. No flag is read since: every chain
    writes Postgres, and the two copies of the latch are compared by the
    daily `reconcile_operational`, the only thing that can read both."""
    from core.write_chains import WRITE_CHAINS, chain_name

    return {chain_name(chain): {} for chain in WRITE_CHAINS}


def _duckdb_switch() -> dict:
    """KS_DUCKDB as this process understood it, the error when it was not
    understood (ran as `on`), and every open of the DuckDB file refused under
    `off` — `{site: {count, last_at}}`, a site being the `module:function`
    that asked. Local state, no I/O, no exception text: this endpoint is
    public. The canary pages a non-empty `opened_while_off` CRITICAL and
    writes the week of silence's watch from it (`core/duckdb_switch.py`)."""
    from core import duckdb_switch

    return duckdb_switch.health_block()


def _read_fallback_mode() -> dict:
    """`{"refused": {surface: {count, last_at}}}` — every read this process
    refused because its one engine failed, answered 503 naming the surface
    (`core.read_fallback.refuse`). Local state, no I/O, no exception text:
    this endpoint is public. The canary pages a recent refusal as
    `read_refused`, and reads the block by this name.

    Until step 14's S14-16 the block also carried `mode` and `error` (as
    `KS_READ_FALLBACK` was understood at start, `off` since S14-00),
    `misconfigured` (every `KS_READ_*` naming an engine with no address) and
    `no_engine` (empty since S14-07), and `read_fallbacks` stood beside it,
    the reads answered from DuckDB — always empty since S14-00. None of them
    had anything left to say: no read switch routes a read and none falls
    back, so S14-16 took them with the canary's three checks."""
    from core import read_fallback

    return {"refused": read_fallback.refusals()}


# The layer ages, out of Postgres, where chain 9 writes the journal (OD-02
# (c)), on the stats' TTL; only a successful read is cached — an error is
# answered again on the next probe rather than remembered for a minute.
_journal_ages_cache: dict = {"data": None, "expires_at": 0}
_journal_ages_lock = asyncio.Lock()
_JOURNAL_AGES_TIMEOUT_S = 5


async def _journal_ages():
    """`data_quality` for the canary: Postgres's layer ages, or None when
    Postgres cannot answer — never DuckDB's: a fallback would read "journal
    fine" to the canary at the moment the journal's writer is what failed,
    and a missing block is what the canary pages (`dq_block_missing`).
    (Until step 14's S14-10 a chain left at `duckdb` answered DuckDB's ages
    here, handed in by the caller out of the store's stats.)"""
    from core import dq_journal

    now = time.time()
    async with _journal_ages_lock:
        if _journal_ages_cache["data"] is not None and now < _journal_ages_cache["expires_at"]:
            return _journal_ages_cache["data"]
        try:
            data = await asyncio.wait_for(
                dq_journal.last_success_ages_pg(), _JOURNAL_AGES_TIMEOUT_S)
        except Exception as e:  # noqa: BLE001 — public endpoint: class only
            logger.warning(f"data-quality freshness from Postgres failed: {type(e).__name__}")
            return None
        _journal_ages_cache["data"] = data
        _journal_ages_cache["expires_at"] = now + _STATS_CACHE_TTL
        return data


# Chain 1's and chain 5's pre-flip answers (`preflight`, cached and bounded
# behind a shield, asked at once with `asyncio.gather`) stood here until step
# 14's S14-11: each said `ok: null` once its chain wrote Postgres, which since
# S14-00 is always, and they went with the chains' flags.


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


async def _orders_sync_step() -> "dict | None":
    """What chain 3's order step last did (`OrdersStepState`): failures in a
    row, ages, the error's class and how many orders Postgres would refuse.
    Local state, no I/O; null when the sync service cannot be had. The canary
    judges it as `orders_sync_failing` once the chain writes Postgres."""
    try:
        from core.sync_service import get_sync_service

        return (await get_sync_service()).orders_step_health()
    except Exception as e:
        logger.debug(f"Orders sync step unavailable: {e}")
        return None


async def _managers_sync_step() -> "dict | None":
    """What chain 5's managers step last did under the chain (failures in a
    row, the error's class, the retry window). Local state, no I/O; null when
    the sync service cannot be had."""
    try:
        from core.sync_service import get_sync_service

        return (await get_sync_service()).managers_step_health()
    except Exception as e:
        logger.debug(f"Managers sync step unavailable: {e}")
        return None


async def _write_chains_block() -> dict:
    """The `write_chains` block: `{chain: {sync_step?, pending?}}`.

    `sync_step` is what a chain's half of the sync tick last did, where a
    Postgres failure is recorded instead of ending the tick: chain 1's offers
    and stocks step, chain 3's order step — judged by the canary as
    `orders_sync_failing` —, chain 5's managers step and chain 6's hourly
    products step. `pending` is a report ledger's spool (chains 11a and 11b),
    judged as `report_ledger_pending`. Local state, no I/O beyond the spool
    directory, so the block answers while Postgres cannot.

    Until step 14's S14-11 each entry also carried `env`, `mode`, `latched`,
    `latched_at`, `mismatch` and `unmet_precondition`, and chains 1 and 5 a
    `preflight` asking Postgres; they went with the flags."""
    from core import (
        pg_catalogue_write, pg_inventory_write, pg_managers_write,
        pg_orders_write,
    )

    block = _write_chains()
    block[pg_inventory_write.CHAIN]["sync_step"] = await _inventory_sync_step()
    block[pg_orders_write.CHAIN]["sync_step"] = await _orders_sync_step()
    block[pg_managers_write.CHAIN]["sync_step"] = await _managers_sync_step()
    # Chain 6's hourly products step, in chain 1's shape: recorded instead of
    # ending the tick. Not judged by the canary — the freshness check watches
    # `last_sync_products` at 48 h.
    block[pg_catalogue_write.CHAIN]["sync_step"] = await _catalogue_sync_step()
    _pending_entries(block)
    return block


def _pending_entries(block: dict) -> None:
    """Under a chain that declares `pending()` — the report ledgers' spool,
    chains 11a and 11b — what is waiting to be recorded: the delivered weeks
    whose row is still spooled, which the canary pages as
    `report_ledger_pending`. Local files, no I/O beyond the directory. Until
    step 14's S14-10 every shadow chain's entry also carried
    `shadow_failures` (and, under KS_DUCKDB=off, `shadow_stood_down`); the
    shadow went, and both with it."""
    from core.write_chains import WRITE_CHAINS, chain_name

    for chain in WRITE_CHAINS:
        entry = block.get(chain_name(chain))
        pending = getattr(chain, "pending", None)
        if not isinstance(entry, dict) or not callable(pending):
            continue
        try:
            entry["pending"] = pending()
        except Exception as exc:  # noqa: BLE001 — published by class
            entry["pending"] = {"error_class": type(exc).__name__}


async def _catalogue_sync_step() -> "dict | None":
    """What chain 6's hourly products step last did off DuckDB. Local state,
    no I/O; null when the sync service cannot be had."""
    try:
        from core.sync_service import get_sync_service

        return (await get_sync_service()).catalogue_step_health()
    except Exception as e:
        logger.debug(f"Catalogue sync step unavailable: {e}")
        return None


# `_warehouse_writer_mode`, `_derivation_mode` and `_utm_parse_mode` stood
# here until step 14's S14-15: the `warehouse_writer_mode` and `utm_parse`
# blocks and the `derivation` block's `mode` and `error` — KS_WRITE_WAREHOUSE,
# KS_UTM_PARSE and KS_PG_DERIVE as this process understood them, constants
# since S14-00 and read by nothing since S14-15. The canary's checks of them
# went in the same change.


def _backups() -> "dict | None":
    """The ages of what the host's backup scripts last proved
    (`core.backup_evidence`) — chain 3's flip evidence once, judged by the
    canary as `backup_evidence_stale` since step 14's S14-00: the PITR drill,
    the off-site restore drill and the Postgres off-site shipment, in hours,
    null where no marker exists. Ages only: the endpoint is public. Local files, no
    database, so it answers with Postgres down; null if even that fails."""
    try:
        from core import backup_evidence

        return backup_evidence.published()
    except Exception as e:  # noqa: BLE001 — a block that cannot be read is null
        logger.debug(f"Backup evidence unavailable: {e}")
        return None


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

    Both null when Postgres cannot be read. A signal that has never dropped a
    mark has no row, and that is a count of 0. (Null under KS_PG_DERIVE=
    piggyback too, where nothing marked, until step 14's S14-15 removed the
    variable.)
    """
    from datetime import datetime, timezone

    from core import pg_derivation

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
    is when it is not zero. Web pages on it itself
    (`warehouse_pg:signal_unreadable`); it is published so that the minutes
    before the page, and a page that could not be delivered, are visible from
    outside. (Null under KS_PG_DERIVE=piggyback, where the job that counts was
    not registered, until step 14's S14-15 removed the variable.)"""
    from core import pg_derivation

    return {"signal_unreadable_ticks": pg_derivation.signal_unreadable_ticks()}


async def _derivation_block() -> dict:
    """The `derivation` block: the unreadable-signal count, which is local
    state, plus the two numbers in it that have to be read out of Postgres.
    It led with `mode` and `error` (KS_PG_DERIVE) until step 14's S14-15."""
    return {**_signal_reads(), **await _dropped_marks()}


@router.get("/health", response_model=HealthResponse)
@limiter.limit("60/minute")
async def health_check(request: Request):
    """Health check endpoint for Docker/load balancer monitoring.

    Until step 14's S14-17 the answer led with DuckDB's block — its row
    counts, its latency and `fatal` (FatalExceptions this process met, and an
    index short in the file) — and the DuckDB schema ledger, `migrations`;
    `degraded_by` named `duckdb`, `migrations` or `duckdb_fatal` from them.
    Web has run KS_DUCKDB=off since S14-00, so all three said `off`, null and
    nothing. `status` and `degraded_by` stay in the answer — `healthy` and
    `[]` — because the canary, Docker's healthcheck and the deploy's gate
    read them."""
    uptime_seconds = int(time.time() - START_TIME)

    sync_status = None
    try:
        from core.sync_service import get_sync_service
        sync_service = await get_sync_service()
        sync_stats = sync_service.get_sync_stats()

        seconds_since_sync = None
        if sync_stats.get("last_sync_time"):
            from datetime import datetime
            # The clock `last_sync_time` was stamped with. `core.config` never
            # had this name, and the ImportError, swallowed below, published
            # `sync: null` from 2026-02-12 until the type checker said so.
            from core.sync_service import DEFAULT_TZ
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

    # `data_quality` is Postgres's layer ages, from `_journal_ages()` (chain 9
    # writes the journal there), published at the top level for the canary.
    # None when Postgres cannot answer, and that absence is meaningful: the
    # canary pages `dq_block_missing` on a missing block, so never substitute
    # an empty dict for "we could not tell".
    # Every layer in it is written by a job that runs: DuckDB's
    # `reconciliation`, published marked `stood_down` since chain 3's flip,
    # left the watched layers in step 14's S14-12.
    data_quality = await _journal_ages()

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

    return {
        # `degraded_by` named what made `status` degraded, one word per
        # cause. Nothing does since step 14's S14-17: the three causes there
        # were — `duckdb`, `migrations`, `duckdb_fatal` — were DuckDB's, and
        # every other failure here has a block and a canary key of its own.
        "status": "healthy",
        "degraded_by": [],
        "version": VERSION,
        "uptime_seconds": uptime_seconds,
        "correlation_id": get_correlation_id(),
        "sync": sync_status,
        "data_quality": data_quality,
        "mirrors": mirrors,
        "alerting": alerting,
        "derivation": await _derivation_block(),
        "write_chains": await _write_chains_block(),
        "read_fallback_mode": _read_fallback_mode(),
        "duckdb_switch": _duckdb_switch(),
        "buyer_sync": await _buyer_sync_block(),
        "backups": _backups(),
    }


@router.get("/health/detailed")
@limiter.limit("30/minute")
async def detailed_health_check(request: Request):
    """Detailed health check with component-level status."""
    components = {}
    # No component degrades it since step 14's S14-17, which took DuckDB's
    # (`components.duckdb`, its row counts, the one that could) out; the
    # field stays for whoever reads it.
    overall_status = "healthy"

    # Meilisearch check
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


# `GET /api/duckdb/stats` stood here until step 14's S14-17: DuckDB's row
# counts, `{"status": "off"}` since S14-00, when web stopped opening the file.
# Nothing called it; a GET under /api/ that no route matches answers a JSON
# 404.


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
    from core import dq_journal

    try:
        # From Postgres, which writes the journal (chain 9), with no fallback.
        return await dq_journal.health_runs()
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
