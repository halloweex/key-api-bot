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
            from core import pg_derivation, pg_utm_parse
            from core.mirror_reconciliation import WATCHED_MIRRORS, fetch_mirror_freshness
            from core.pg import get_pool

            # Under KS_PG_DERIVE=own the derived layers are watched too, with
            # the limit declared here: a derivation that simply stops being
            # triggered raises nothing anywhere, and only its watermarks age.
            owned = pg_derivation.owns()
            # And under KS_UTM_PARSE=postgres (which needs own) the UTM table,
            # whose watermark is then the parse's liveness stamp rather than a
            # ship's (DN-19): the derivation's last step, in a try of its own,
            # so it can stop while Silver and Gold stay fresh. Under the
            # default the row is the ship's and is judged by the daily
            # comparison, as before.
            parsed = pg_utm_parse.parses_in_postgres()
            tables = (WATCHED_MIRRORS
                      + (pg_derivation.DERIVED_TABLES if owned else ())
                      + ((pg_utm_parse.UTM_TABLE,) if parsed else ()))
            data = await fetch_mirror_freshness(await get_pool(), tables=tables)
            if owned:
                for table in pg_derivation.DERIVED_TABLES:
                    data[table]["max_age_s"] = pg_derivation.DERIVED_MAX_AGE_S
            if parsed:
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
    """`_buyer_sync()`, plus — under chain 4 — `watermark_age_s`: the age of
    the stamp the step writes to Postgres on every completion. The canary's
    chain CRITICAL judges the older of the two, because the local clock is
    floored at the process start and every recreate would otherwise reset a
    stall and announce it resolved (review of PR-3). Read at most once a
    minute; a store that cannot be read publishes None, and the canary falls
    back to the local clock."""
    from core import pg_buyer_sync_read, pg_buyers_write

    block = _buyer_sync()
    if block is None or pg_buyers_write.mode() != "postgres":
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


def _mark_stood_down(data_quality):
    """The `data_quality` block with every layer chain 3 stood down marked
    `stood_down: true` — written by nothing by design, so the canary does not
    page its age (`pg_orders_write.stood_down_layers`). Applied per response,
    not in the 60-second stats cache, because a first write can move the
    chain between two reads. None stays None: its absence is meaningful."""
    if not isinstance(data_quality, dict):
        return data_quality
    from core.pg_orders_write import stood_down_layers

    stood = stood_down_layers()
    return {layer: ({**entry, "stood_down": True}
                    if layer in stood and isinstance(entry, dict) else entry)
            for layer, entry in data_quality.items()}


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
    has no address for. Local state, no I/O.

    `no_engine` lists the surfaces only one engine may answer under `off`
    whose switch does not name it — the cohorts at `KS_READ_COHORTS=duckdb`:
    served from DuckDB uncounted today, refused under `off`. With
    `misconfigured` it is what the canary pages as `read_routed_to_duckdb`
    (OD-07).

    Under `off`, also `refused` — `{surface: {count, last_at}}`, the reads
    answered 503 rather than from DuckDB (DN-20b). Only under `off`: nothing
    can be refused under `duckdb`, and the block keeps the shape it has
    always had there."""
    from core import read_fallback

    block = {
        "mode": read_fallback.mode(),
        "error": read_fallback.mode_error(),
        "misconfigured": read_fallback.misconfigured(),
        "no_engine": read_fallback.no_engine_routes(),
    }
    if read_fallback.refusing():
        block["refused"] = read_fallback.refusals()
    return block


# The layer ages, out of Postgres while chain 9 writes the journal there
# (OD-02 (c)). DuckDB's ages ride in the cached stats; these replace them, on
# the same TTL, and only a successful read is cached — an error is answered
# again on the next probe rather than remembered for a minute.
_journal_ages_cache: dict = {"data": None, "expires_at": 0}
_journal_ages_lock = asyncio.Lock()
_JOURNAL_AGES_TIMEOUT_S = 5


async def _journal_ages(from_duckdb):
    """`data_quality` for the canary, from the store that writes the journal.

    Under `KS_WRITE_DQ_JOURNAL=duckdb` (or unset) it is DuckDB's answer,
    untouched. Under `postgres` it is Postgres's, or None when Postgres
    cannot answer — never DuckDB's: a fallback would read "journal fine" to
    the canary at the moment the journal's writer is what failed, and a
    missing block is what the canary pages (`dq_block_missing`)."""
    from core import dq_journal

    if not dq_journal.reads_postgres():
        return from_duckdb
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


# Chain 3's pre-flip answer, on the same cache shape as chain 1's.
_orders_preflight_cache: dict = {"data": None, "expires_at": 0}
_orders_preflight_cache_lock = asyncio.Lock()


async def _orders_preflight() -> dict:
    """`pg_orders_write.preflight()`, cached and bounded. Never raises."""
    from core import pg_orders_write

    now = time.time()
    async with _orders_preflight_cache_lock:
        if (_orders_preflight_cache["data"] is not None
                and now < _orders_preflight_cache["expires_at"]):
            return _orders_preflight_cache["data"]
        try:
            data = await asyncio.wait_for(pg_orders_write.preflight(),
                                          _PREFLIGHT_TIMEOUT_S)
        except asyncio.TimeoutError:
            data = {"ok": False, "reasons": [
                f"Postgres did not answer within {_PREFLIGHT_TIMEOUT_S} s"]}
        _orders_preflight_cache["data"] = data
        _orders_preflight_cache["expires_at"] = now + _STATS_CACHE_TTL
        return data


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

    from core import pg_orders_write

    block = _write_chains()
    inventory = block.get(pg_inventory_write.CHAIN)
    # Chain 3: its preflight — every precondition by name while the flag is
    # still off — and the order step the canary judges.
    orders = block.get(pg_orders_write.CHAIN)
    # The two preflights at once, never one after the other: each is bounded
    # at `_PREFLIGHT_TIMEOUT_S`, and with Postgres hung two in a row cost
    # twice that — 10 s, the canary's whole `HEALTH_TIMEOUT_S` (the chain-3
    # review). Concurrently they cost one bound.
    preflights = await asyncio.gather(
        _inventory_preflight() if isinstance(inventory, dict) else _nothing(),
        _orders_preflight() if isinstance(orders, dict) else _nothing())
    if isinstance(inventory, dict):
        inventory["preflight"] = preflights[0]
        inventory["sync_step"] = await _inventory_sync_step()
    if isinstance(orders, dict):
        orders["preflight"] = preflights[1]
        orders["sync_step"] = await _orders_sync_step()
    # Chain 6's hourly products step off DuckDB, in chain 1's shape: recorded
    # instead of ending the tick. Not judged by the canary — the freshness
    # check watches `last_sync_products` at 48 h.
    from core import pg_catalogue_write

    entry = block.get(pg_catalogue_write.CHAIN)
    if isinstance(entry, dict):
        entry["sync_step"] = await _catalogue_sync_step()
    _shadow_entries(block)
    return block


async def _nothing() -> None:
    return None


def _shadow_entries(block: dict) -> None:
    """Under every shadow chain's entry (OD-02 (c)), `shadow_failures`: the
    DuckDB halves of its writes that failed since this process started —
    count, when, and the error class, never the text (this endpoint is
    public). Postgres held each of those rows; the daily comparison finds
    them as `shadow_missing_in_duckdb`, and this says when to look in the
    log. A chain that declares `pending()` — the report ledgers' spool —
    also publishes what is waiting to be recorded. Local state, no I/O."""
    from core import shadow_writes
    from core.write_chains import WRITE_CHAINS, chain_name, is_shadow

    for chain in WRITE_CHAINS:
        entry = block.get(chain_name(chain))
        if not isinstance(entry, dict) or not is_shadow(chain):
            continue
        entry["shadow_failures"] = shadow_writes.failures_of(chain_name(chain))
        pending = getattr(chain, "pending", None)
        if callable(pending):
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


def _warehouse_writer_mode() -> dict:
    """KS_WRITE_WAREHOUSE as this process understood it at start (DN-28,
    DN-29): the value as read, the mode it runs, the error when the value was
    not understood and ran as duckdb, and — when `postgres` was asked for and
    ran as duckdb, or when a value not understood took the way back after a
    flip (`value_understood`) — the KEYS of the preconditions unmet. Keys, not
    details: this endpoint is public, and a detail quotes the environment;
    the details are on `/api/warehouse/status` and in the log. `held` is the
    way back still owed its first validated full DuckDB tick, or the UTM parse
    that finishes after it, and `held_for_s` how long that has stood (None
    when not held) — the canary warns on a hold that outlives what a way back
    takes. `reclassify_needed` says DuckDB's UTM verdicts were found empty on
    it. Local state, no I/O. Judged by the canary."""
    from core import warehouse_cutover

    return {"mode": warehouse_cutover.mode(), "value": warehouse_cutover.value(),
            "error": warehouse_cutover.mode_error(),
            "preconditions_unmet": [u.key for u in warehouse_cutover.preconditions_unmet()],
            "held": warehouse_cutover.held(),
            "held_for_s": warehouse_cutover.held_for_s(),
            "reclassify_needed": warehouse_cutover.reclassify_needed()}


def _derivation_mode() -> dict:
    """KS_PG_DERIVE as this process understood it at start. Local state, no I/O."""
    from core import pg_derivation

    return {"mode": pg_derivation.mode(), "error": pg_derivation.mode_error()}


def _goals_history() -> dict:
    """KS_GOALS_HISTORY as a goal history read takes it (chain 7b): `mode`
    (`bridge` or `silver`), or null and the `error` every such read raises —
    the variable is read at each read, not at start, so this is asked on each
    request, with no I/O. Judged by the canary."""
    from core import pg_goals_read

    return pg_goals_read.history_state()


def _utm_parse_mode() -> dict:
    """KS_UTM_PARSE as this process understood it at start, and the error when
    it ran as `duckdb` instead of what was set — an unknown value, or
    `postgres` without KS_PG_DERIVE=own (DN-19). Local state, no I/O."""
    from core import pg_utm_parse

    return {"mode": pg_utm_parse.mode(), "error": pg_utm_parse.mode_error()}


def _backups() -> "dict | None":
    """The ages of what the host's backup scripts last proved — chain 3's
    flip evidence (`core.backup_evidence`): the PITR drill, the off-site
    restore drill and the Postgres off-site shipment, in hours, null where no
    marker exists. Ages only: the endpoint is public. Local files, no
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
    # Chain 9: from the store that writes the journal (`_journal_ages`).
    data_quality = await _journal_ages(data_quality)
    # Chain 3: the layers it stood down marked, on whichever store answered —
    # per response, after the journal's own cache.
    data_quality = _mark_stood_down(data_quality)

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
        "utm_parse": _utm_parse_mode(),
        "goals_history": _goals_history(),
        "buyer_sync": await _buyer_sync_block(),
        "backups": _backups(),
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
    from core import dq_journal

    try:
        store = await get_store()
        # The block that stood here, moved to `core.dq_journal` so it reads
        # whichever store writes the journal (chain 9) — DuckDB's one
        # connection by default, Postgres with no fallback once it moves.
        return await dq_journal.health_runs(store)
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
