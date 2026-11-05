"""
Sync service for keeping DuckDB in sync with KeyCRM API.

Handles incremental synchronization of orders, products, and categories.

Features:
- Full sync: Initial load of all historical data (in 90-day chunks)
- Incremental sync: Only fetch new/updated records
- Background sync: Periodic updates running in the background
- Observability: Correlation IDs and timing metrics
"""
import asyncio
import contextlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo

from core.keycrm import get_async_client
from core.duckdb_store import get_store, DuckDBStore
from core.exceptions import KeyCRMError, KeyCRMConnectionError, KeyCRMAPIError, KeyCRMTruncated
from core.observability import get_logger, correlation_context
from core.events import (
    events,
    SyncEvent,
    emit_sync_started,
    emit_sync_completed,
    emit_sync_failed,
    emit_orders_synced,
)
from core.meilisearch_client import get_meili_client, init_meilisearch
from core import read_fallback
from bot.config import DEFAULT_TIMEZONE

logger = get_logger(__name__)

DEFAULT_TZ = ZoneInfo(DEFAULT_TIMEZONE)

# ─── Chain 1's half of the tick, once Postgres writes it (DN-24) ─────────────
#
# Under KS_WRITE_INVENTORY=postgres the offers and stocks steps write Postgres,
# and `core.pg_inventory_write` raises by design: it is the only write, and a
# failure swallowed there is a movement lost with a success reported. But the
# tick only ever caught KeyCRM errors, so a Postgres error in these steps left
# `incremental_sync` altogether — the job marked failed, the adaptive backoff
# not updated. And a missing TEMPORARY grant fails the status rebuild on every
# attempt, so the same error would have ended one tick an hour for as long as
# the grant stayed missing: `last_sync_stocks` was stamped before the rebuild,
# so the step was not due again for an hour, and each hour the rebuild failed
# and both snapshots after it were skipped.
#
# So on that path a failure is recorded (`InventoryStepState`, published on
# /api/health), `last_sync_stocks` is left where it was — it now moves only
# once the upsert, the rebuild and both snapshots have committed, so the next
# attempt redoes the whole step — and the tick carries on. A failed offers step
# skips the stocks step: stocks read the offer→product map, and a stock
# written ahead of its offer records its movements with no product.
#
# The attempt after a failure waits `INVENTORY_RETRY_AFTER_S`. With the
# watermark held, the next tick would otherwise fetch every stock from KeyCRM
# again — a minute later, and every minute the fault lasts — on the quota the
# order sync shares.
#
# The DuckDB path is untouched, including a failure escaping it.
INVENTORY_RETRY_AFTER_S = 600

# The offers and stocks steps' cadence, as the DuckDB path spells it.
INVENTORY_STEP_EVERY_S = 3600


class InventoryStepState:
    """What chain 1's half of the tick last did on the Postgres path.

    Published on /api/health as `write_chains.pg_inventory_write.sync_step`.
    The error is its class, never its text: that endpoint is public and the
    text is a driver's message.
    """

    def __init__(self) -> None:
        self.failures_since_ok = 0
        self.last_ok_at: Optional[datetime] = None
        self.last_failure_at: Optional[datetime] = None
        self.last_failed_step: Optional[str] = None
        self.last_error: Optional[str] = None

    def succeeded(self) -> None:
        self.failures_since_ok = 0
        self.last_ok_at = datetime.now(timezone.utc)

    def failed(self, step: str, exc: BaseException) -> None:
        self.failures_since_ok += 1
        self.last_failure_at = datetime.now(timezone.utc)
        self.last_failed_step = step
        self.last_error = type(exc).__name__

    def published(self, retry_in_s: Optional[int]) -> Dict[str, Any]:
        return {
            "failures_since_ok": self.failures_since_ok,
            "last_ok_at": self.last_ok_at.isoformat() if self.last_ok_at else None,
            "last_failure_at": (self.last_failure_at.isoformat()
                                if self.last_failure_at else None),
            "last_failed_step": self.last_failed_step,
            "last_error": self.last_error,
            "retry_in_s": retry_in_s,
        }


# ─── Chain 3's half of the tick: the orders, written to Postgres ─────────────
#
# The order step writes Postgres and `core.pg_orders_write` raises by design
# (OD-13 (a)): there is no DuckDB to fall back to, and an order written nowhere
# must be loud. But the tick caught only KeyCRM errors, so a Postgres error in
# the order step would have left `incremental_sync` altogether and cost
# products, managers, buyers and the inventory steps behind it — a chain's
# fault must stop that chain and nothing else (DN-01, OD-18 (a)). So a failure
# that is not KeyCRM's is recorded (`OrdersStepState`, published on /api/health
# and judged by the canary as `orders_sync_failing`), `last_sync_orders` is
# left where it was, and the tick goes on. KeyCRM's own errors still end the
# tick, as they always have. No retry window: the watermark is read from
# Postgres before anything is fetched, so with Postgres away the step costs
# KeyCRM nothing, and the next tick's 24-hour window covers everything the
# failure held back.
#
# Until step 14's S14-09 all of this held only while chain 3 was off DuckDB
# (`_orders_off_duckdb`, a constant since S14-00), and the DuckDB path let
# the failure leave the tick; S14-09 deleted that path.


# How long the tick waits to stamp `bronze.orders` failing in Postgres, and
# for `last_sync_orders` out of it.
ORDERS_FAILURE_RECORD_TIMEOUT_S = 5

# The narrowest window `_fetch_order_window` splits a truncated listing into
# before it gives up (L1-08): 5 000 orders in an hour is not a shop this size.
ORDER_WINDOW_MIN_SPAN = timedelta(hours=1)


def _window_bound(value: str, *, end_of_day: bool) -> datetime:
    """A KeyCRM date filter's bound as a datetime. A date alone is widened to
    the whole day at the end, so a split never asks for less than the window
    it replaces (a superset is merged away by id; a subset loses orders)."""
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        day = datetime.strptime(value.strip(), "%Y-%m-%d")
        return day.replace(hour=23, minute=59, second=59) if end_of_day else day

# The step's own statement for the healthy tick after a failure (L4-03). Only
# a write of an order zeroes `failures_since_ok` (`pg_landing._WATERMARK_OK`),
# and a healthy tick that finds nothing changed writes nothing — so one failed
# step left `bronze.orders` failing, and the canary paging
# `mirror_failing:bronze.orders`, until an order next changed (overnight, hours).
# `last_ok_at` is not moved: it keeps meaning "the last write", which
# `mirror_stale` and the soak's S0 judge. `WATERMARK_OK_SQL` is the writers'
# and stays theirs.
ORDERS_FAILURE_CLEAR_SQL = """
UPDATE meta.mirror_state
   SET failures_since_ok = 0,
       last_error        = NULL,
       last_attempted_at = now()
 WHERE table_name = $1 AND failures_since_ok > 0
"""
ORDERS_WATERMARK_TIMEOUT_S = 10


class OrdersStepState:
    """What chain 3's half of the tick last did. Published on /api/health as
    `write_chains.pg_orders_write.sync_step`: ages, counts and the error's
    class — never its text, the endpoint is public."""

    def __init__(self) -> None:
        self.started_at = datetime.now(timezone.utc)
        self.consecutive_failures = 0
        self.last_ok_at: Optional[datetime] = None
        self.last_attempt_at: Optional[datetime] = None
        self.last_error_class: Optional[str] = None
        self.last_refused = 0
        # When the scheduler's tick began waiting for `_heavy_job_lock`, or
        # None while it is not waiting. The full sync, training, the backup
        # and the 05:15 refresh hold that lock, and the tick queues behind
        # them before it can reach the step; the canary must tell that wait
        # from a tick that stopped (`check_orders_sync_chain`).
        self.waiting_since: Optional[datetime] = None

    def waiting(self) -> None:
        self.waiting_since = datetime.now(timezone.utc)

    def done_waiting(self) -> None:
        self.waiting_since = None

    def attempted(self) -> None:
        self.last_attempt_at = datetime.now(timezone.utc)

    def succeeded(self, refused: int = 0) -> None:
        self.consecutive_failures = 0
        self.last_ok_at = datetime.now(timezone.utc)
        self.last_error_class = None
        self.last_refused = refused

    def failed(self, exc: BaseException) -> None:
        self.consecutive_failures += 1
        self.last_error_class = type(exc).__name__

    def published(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)

        def age(at: Optional[datetime]) -> Optional[int]:
            return None if at is None else int((now - at).total_seconds())

        return {
            "consecutive_failures": self.consecutive_failures,
            # Floored at this process's start, `BuyerSyncState`'s reason: a web
            # that has never completed the step is as stale as its uptime.
            "last_ok_age_s": age(self.last_ok_at or self.started_at),
            "ever_ok": self.last_ok_at is not None,
            "last_attempt_age_s": age(self.last_attempt_at),
            "last_error_class": self.last_error_class,
            "last_refused": self.last_refused,
            # How long the tick has waited for the heavy-job lock, or None.
            "lock_wait_s": age(self.waiting_since),
        }


# Chain 5's managers step, under `KS_WRITE_MANAGERS=postgres` (or a flag nobody
# can read): a failure is recorded (`ManagersStepState`, on /api/health as
# `write_chains.pg_managers_write.sync_step`), `last_sync_managers` is not
# moved, and the next attempt waits this long — chain 4's interval. On the
# DuckDB path a raise in the managers step used to skip the buyers, offers and
# stocks steps for the tick; under the chain it is contained (DN-01's rule: a
# chain's fault stops that chain and nothing else). The DuckDB path is
# untouched.
MANAGERS_RETRY_AFTER_S = 600

# The managers' sync cadence, as the DuckDB path spells it.
MANAGERS_STEP_EVERY_S = 86400


class ManagersStepState(InventoryStepState):
    """What chain 5's managers step last did, under the chain. Published on
    /api/health as `write_chains.pg_managers_write.sync_step` — the error's
    class, never its text: the endpoint is public."""


def _get_max_updated_at(orders: list) -> Optional[datetime]:
    """
    Extract max updated_at from a list of orders.

    Used for checkpoint - ensures we use SOURCE timestamp, not now().
    This prevents data loss when orders are updated during sync.
    """
    if not orders:
        return None

    max_updated = None
    for order in orders:
        updated_str = order.get("updated_at")
        if updated_str:
            try:
                updated = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                if max_updated is None or updated > max_updated:
                    max_updated = updated
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping unparseable updated_at '{updated_str}': {e}")

    return max_updated


# ─── Chain 6's catalogue, contained ──────────────────────────────────────────
#
# The products watermark was read at the TOP of the incremental tick, before
# orders, and the tick catches only KeyCRM errors. Under chain 6 that getter
# reads Postgres — so a Postgres outage there would stop order intake once a
# minute (DN-01's class, found designing chain 6). So the read lives in the
# hourly products step itself, bounded like the buyers' (DN-05a), and a
# failure is recorded and costs that step alone; the next attempt waits ten
# minutes, during which neither KeyCRM nor Postgres is asked anything. Until
# step 14's S14-09 that held only while chain 6 was off DuckDB
# (`_catalogue_off_duckdb`, a constant since S14-00); S14-09 deleted the
# DuckDB path, and with it the step's top-of-tick read.
CATALOGUE_RETRY_AFTER_S = 600
CATALOGUE_STEP_EVERY_S = 3600
CATALOGUE_WATERMARK_TIMEOUT_S = 10


# ─── The buyers step, contained ──────────────────────────────────────────────
#
# The buyers step runs inside the incremental tick, BEFORE offers and stocks,
# and until chain 4's preparation nothing caught what it raised except KeyCRM
# errors. So a write it could not complete — a birthday DuckDB refused, a store
# fault — escaped the tick and cost the inventory syncs behind it; and because
# a failed step never moves its watermark, the next tick sixty seconds later
# fetched the same buyers and failed the same way. Measured on a scratch store,
# not observed in production. Two things now hold:
#
# - the step cannot take anything else down: whatever it raises is recorded
#   and the tick goes on to offers and stocks;
# - a failed step waits `BUYER_RETRY_AFTER_S` before trying again, and inside
#   that window it asks neither Postgres nor KeyCRM anything — up to 500 GETs
#   a minute against a failing store is not a retry policy.
#
# Its state is published on /api/health as `buyer_sync` and judged by the
# canary (`buyer_sync_stalled`), because under chain 4 this step becomes the
# ONLY writer of buyers, and a writer that stopped would otherwise be noticed
# the next morning by a comparison.

BUYER_RETRY_AFTER_S = 600
# The getter will read Postgres once a chain owns `last_sync_buyers` (a chain's
# watermark lives in meta.chain_watermarks); a store that hangs must not hold
# the tick. Shielded where it is used — see the tick, and DN-05a.
BUYER_WATERMARK_TIMEOUT_S = 10

# Classes that mean "this data cannot be written", not "the step is broken".
# Matched by name so this module need not import either driver. They do not
# count toward `consecutive_failures`: a batch that carries such a value fails
# the same way until the value changes, and paging a person about it three
# times an hour says nothing the growing age of `last_ok` does not.
_DATA_ERROR_NAMES = frozenset({
    "ConversionException", "ConstraintException", "InvalidInputException",
    "DataError", "CharacterNotInRepertoireError", "BuyersRefused",
})


class BuyersRefused(ValueError):
    """Every buyer a batch fetched was one Postgres would refuse (chain 4 skips
    them by id rather than failing the batch). Not a success: a step that
    landed nothing must not refresh the clock the canary judges it by — with
    chain 4 there is no other writer of buyers. A data error, so it does not
    count toward `consecutive_failures`; the age still pages."""


class OrdersRefused(ValueError):
    """The order step's window landed nothing — no order written, none found
    unchanged, no expense — and the parse refused orders in it (AR-02).

    One payload the parse cannot read is refused by id and the rest of the
    window lands (L1-01). A window in which nothing could be read is not that:
    it is KeyCRM changing a field's shape for every order, or a parser defect
    that shipped, and before L1-01 it failed the step on every tick. Reported
    as a success it moved `last_sync_orders` past orders nobody wrote and kept
    `orders_sync_failing` quiet until the 05:30 reconciliation. So the step
    fails on it: the watermark holds, `consecutive_failures` counts it and
    `meta.mirror_state` marks `bronze.orders` failing, as the TypeError did.
    Chain 4's `BuyersRefused` for the same shape; unlike it, counted as a
    failure, because the order step's canary pages on the count."""


def _is_data_error(exc: BaseException) -> bool:
    return any(k.__name__ in _DATA_ERROR_NAMES for k in type(exc).__mro__)


@dataclass
class BuyerSyncState:
    """What the buyers step last did, in this process.

    `started_at` is the floor for "how long without a success": a web that has
    never completed the step since it started is as stale as its uptime, not
    unknown. Only the error's CLASS is kept — the text can carry a buyer's
    data, and this reaches a public endpoint.
    """
    started_at: datetime = field(default_factory=lambda: datetime.now(DEFAULT_TZ))
    last_attempt_at: Optional[datetime] = None
    last_ok_at: Optional[datetime] = None
    consecutive_failures: int = 0
    last_error_class: Optional[str] = None
    last_selected: int = 0
    last_written: int = 0
    unreadable_birthdays: int = 0
    # Buyers the last batch carried that Postgres would refuse, skipped by id
    # under chain 4 (`pg_buyers_write.upsert_buyers`). The count only; the ids
    # are in the log.
    last_skipped_bad: int = 0

    def succeeded(self, *, selected: int, written: int, unreadable: int = 0,
                  skipped_bad: int = 0) -> None:
        self.last_ok_at = datetime.now(DEFAULT_TZ)
        self.consecutive_failures = 0
        self.last_error_class = None
        self.last_selected = selected
        self.last_written = written
        self.unreadable_birthdays += unreadable
        self.last_skipped_bad = skipped_bad

    def failed(self, exc: BaseException) -> None:
        self.last_error_class = type(exc).__name__
        if not isinstance(exc, KeyCRMError) and not _is_data_error(exc):
            self.consecutive_failures += 1

    def published(self, retry_in_s: Optional[int]) -> Dict[str, Any]:
        """The /api/health block. Ages and counts only."""
        now = datetime.now(DEFAULT_TZ)

        def age(t: Optional[datetime]) -> Optional[int]:
            return None if t is None else int((now - t).total_seconds())

        return {
            "last_attempt_age_s": age(self.last_attempt_at),
            "last_ok_age_s": age(self.last_ok_at or self.started_at),
            "ever_ok": self.last_ok_at is not None,
            "consecutive_failures": self.consecutive_failures,
            "last_error_class": self.last_error_class,
            "last_selected": self.last_selected,
            "last_written": self.last_written,
            "unreadable_birthdays": self.unreadable_birthdays,
            "last_skipped_bad": self.last_skipped_bad,
            "retry_in_s": retry_in_s,
        }


class SyncService:
    """
    Service for syncing KeyCRM data to DuckDB.

    Features:
    - Full sync: Initial load of all historical data
    - Incremental sync: Only fetch new/updated records
    - Background sync: Periodic updates in the background
    - Adaptive sync: Exponential backoff when no new orders
    """

    # Adaptive sync configuration
    BACKOFF_BASE_SECONDS = 60       # Base interval
    BACKOFF_MAX_SECONDS = 300       # Max interval (5 minutes)
    BACKOFF_MULTIPLIER = 2          # Exponential multiplier
    OFF_HOURS_START = 23            # 11 PM Kyiv
    OFF_HOURS_END = 7               # 7 AM Kyiv
    OFF_HOURS_INTERVAL = 300        # 5 min during off-hours

    def __init__(self, store: DuckDBStore):
        self.store = store
        self._sync_task: Optional[asyncio.Task] = None
        # Track if ordered_between filter is supported
        self._use_ordered_between: Optional[bool] = None

        # Adaptive sync state
        self._consecutive_empty_syncs = 0
        self._last_sync_time: Optional[datetime] = None
        self._last_orders_found = 0
        self._current_backoff_seconds = self.BACKOFF_BASE_SECONDS

        # Chain 1 on the Postgres path (DN-24): what it last did, and when it
        # may try again after a failure (a `time.monotonic()` instant).
        self.inventory_step = InventoryStepState()
        self._inventory_retry_at = 0.0
        # Whether `meta.mirror_state` may still hold an order-step failure:
        # unknown at a start, so asked once (`_clear_orders_failure`).
        self._orders_failure_may_stand = True
        # The KeyCRM error the last offers or stocks fetch ended on, for the
        # step's published class (`sync_offers` answers None and logs it).
        self._inventory_keycrm_error: Optional[BaseException] = None
        # Chain 5's managers step, under the chain (`MANAGERS_RETRY_AFTER_S`).
        self.managers_step = ManagersStepState()
        self._managers_retry_at = 0.0
        # The buyers step, contained — see BuyerSyncState.
        self.buyer_sync_state = BuyerSyncState()
        self._buyers_retry_after: Optional[float] = None
        # Chain 3's order step, contained once it writes Postgres, and how
        # many orders its last write refused (`UpsertResult.failed`).
        self.orders_step = OrdersStepState()
        self._last_refused = 0
        # Of those, the ones the parse could not read (`UpsertResult.unparseable`).
        self._last_unparseable = 0
        # Chain 6's hourly products step off DuckDB — see CATALOGUE_RETRY_AFTER_S.
        self.catalogue_step = InventoryStepState()
        self._catalogue_retry_at = 0.0

    def _is_off_hours(self) -> bool:
        """Check if current time is during off-hours (low activity period)."""
        now = datetime.now(DEFAULT_TZ)
        hour = now.hour
        # Off-hours: 11 PM to 7 AM
        return hour >= self.OFF_HOURS_START or hour < self.OFF_HOURS_END

    def _should_skip_sync(self) -> tuple[bool, str]:
        """
        Determine if sync should be skipped based on adaptive backoff.

        Returns:
            Tuple of (should_skip, reason)
        """
        if self._last_sync_time is None:
            return False, ""

        now = datetime.now(DEFAULT_TZ)
        elapsed = (now - self._last_sync_time).total_seconds()

        # During off-hours, use longer interval
        if self._is_off_hours():
            if elapsed < self.OFF_HOURS_INTERVAL:
                remaining = int(self.OFF_HOURS_INTERVAL - elapsed)
                return True, f"off-hours mode, next sync in {remaining}s"

        # Use adaptive backoff based on consecutive empty syncs
        if elapsed < self._current_backoff_seconds:
            remaining = int(self._current_backoff_seconds - elapsed)
            return True, f"backoff active ({self._current_backoff_seconds}s interval), next in {remaining}s"

        return False, ""

    def _update_backoff(self, orders_found: int) -> None:
        """Update backoff state based on sync results."""
        if orders_found > 0:
            # Reset backoff when orders are found
            if self._consecutive_empty_syncs > 0:
                logger.info(f"Adaptive sync: backoff reset (found {orders_found} orders)")
            self._consecutive_empty_syncs = 0
            self._current_backoff_seconds = self.BACKOFF_BASE_SECONDS
        else:
            # Increase backoff exponentially
            self._consecutive_empty_syncs += 1
            new_backoff = min(
                self.BACKOFF_BASE_SECONDS * (self.BACKOFF_MULTIPLIER ** self._consecutive_empty_syncs),
                self.BACKOFF_MAX_SECONDS
            )
            if new_backoff != self._current_backoff_seconds:
                self._current_backoff_seconds = int(new_backoff)
                logger.info(
                    f"Adaptive sync: backoff increased to {self._current_backoff_seconds}s "
                    f"({self._consecutive_empty_syncs} empty syncs)"
                )

        self._last_orders_found = orders_found
        self._last_sync_time = datetime.now(DEFAULT_TZ)

    def get_sync_stats(self) -> Dict[str, Any]:
        """Get current adaptive sync statistics."""
        return {
            "consecutive_empty_syncs": self._consecutive_empty_syncs,
            "current_backoff_seconds": self._current_backoff_seconds,
            "last_orders_found": self._last_orders_found,
            "last_sync_time": self._last_sync_time.isoformat() if self._last_sync_time else None,
            "is_off_hours": self._is_off_hours(),
            "effective_interval": self.OFF_HOURS_INTERVAL if self._is_off_hours() else self._current_backoff_seconds,
        }

    async def _fetch_orders_with_date_filter(
        self,
        client,
        start_date: str,
        end_date: str,
        include: str = "products.offer,manager,buyer,expenses",
        include_updated: bool = True
    ) -> list:
        """
        Fetch orders with date filters.

        Uses BOTH created_between AND updated_between to catch:
        1. New orders (created_between)
        2. Orders with status changes (updated_between)

        Args:
            include_updated: If True, also fetch by updated_between (for incremental sync)
        """
        orders_by_id = {}

        # Fetch by created_between
        logger.debug(f"Fetching orders created between {start_date} and {end_date}")
        await self._fetch_order_window(
            client, "filter[created_between]", start_date, end_date, include,
            orders_by_id)

        created_count = len(orders_by_id)

        # Also fetch by updated_between to catch status changes
        if include_updated:
            logger.debug(f"Fetching orders updated between {start_date} and {end_date}")
            try:
                # Overwrites with latest data
                await self._fetch_order_window(
                    client, "filter[updated_between]", start_date, end_date, include,
                    orders_by_id)
            except KeyCRMTruncated:
                # Not "the filter is unsupported": the window holds more than a
                # listing can carry even split to an hour, and the step must
                # fail rather than move its watermark past what it never saw.
                raise
            except KeyCRMError as e:
                # filter[updated_between] may not be supported - fall back gracefully
                logger.warning(f"filter[updated_between] failed: {e}")

        updated_count = len(orders_by_id) - created_count
        if updated_count > 0:
            logger.info(f"Fetched {created_count} created + {updated_count} updated orders")

        return list(orders_by_id.values())

    async def _fetch_order_window(
        self, client, key: str, start: str, end: str, include: str,
        into: Dict[Any, Any],
    ) -> None:
        """One filtered order listing into `into`, by id; a listing KeyCRM
        stops at its page cap (`KeyCRMTruncated`) is fetched again as two
        halves of its window, recursively, down to `ORDER_WINDOW_MIN_SPAN`
        (L1-08). The cap is 5 000 orders (100 pages of 50); past it the
        oldest orders of the window were never fetched, and the watermark
        moved past them. Still truncated at an hour, it raises: the caller's
        step fails and holds its watermark. A truncated listing adds nothing;
        its halves add theirs, overlapping at the midpoint, merged by id."""
        params = {"include": include, key: f"{start}, {end}"}
        got: list = []
        try:
            async for batch in client.paginate("order", params=params, page_size=50):
                got.extend(batch)
        except KeyCRMTruncated:
            lo, hi = _window_bound(start, end_of_day=False), _window_bound(end, end_of_day=True)
            if hi - lo <= ORDER_WINDOW_MIN_SPAN:
                raise
            mid = lo + (hi - lo) / 2
            logger.warning("Order listing %s %s..%s truncated at the page cap; "
                           "fetching it as two halves", key, start, end)
            fmt = "%Y-%m-%d %H:%M:%S"
            await self._fetch_order_window(
                client, key, lo.strftime(fmt), mid.strftime(fmt), include, into)
            await self._fetch_order_window(
                client, key, mid.strftime(fmt), hi.strftime(fmt), include, into)
            return
        for order in got:
            into[order["id"]] = order

    async def _upsert_orders_with_expenses(
        self, orders: list, force_update: bool = False, skip_products: bool = False,
        changed_ids_out: "list[int] | None" = None,
    ) -> tuple:
        """Upsert orders and their expenses.

        Args:
            orders: List of order dicts from API
            force_update: If True, force update all orders regardless of updated_at.
                         Use for status refresh since KeyCRM doesn't update updated_at
                         when order status changes.
            skip_products: If True, skip product deletion/insertion to avoid OOM
                          during status refresh (~2000 orders worth of products).
            changed_ids_out: If given, is extended with the ids actually WRITTEN,
                          which is a subset of what was fetched. An out-list
                          rather than a wider return because six call sites and
                          a stack of mocks depend on the 2-tuple, and only the
                          incremental sync needs to know what moved.

        Returns:
            Tuple of (order_count, expense_count)

        Chain 3 (`core/pg_orders_write.py`): the orders and their expenses
        are written to Postgres in one transaction. Raises what the chain
        raises (OD-13 (a)). DuckDB's two writers, and the mirror of what they
        wrote, went with step 14's S14-09.
        """
        from core import pg_orders_write

        result, expense_count = await pg_orders_write.upsert_orders_with_expenses(
            orders, force_update=force_update, skip_products=skip_products,
        )
        if changed_ids_out is not None:
            changed_ids_out.extend(result.changed_ids)
        self._last_refused = result.failed
        # `(order id, reason)` for every order the writer refused — what the
        # repairs ledger rather than ask KeyCRM for again (L1-10). An
        # attribute, like the count, so the many mocks of this method keep
        # their signature.
        self._last_refused_ids = tuple(result.refused)
        # Of those, how many the parse could not read at all: the order step
        # fails on a window that landed nothing and holds one (`OrdersRefused`).
        self._last_unparseable = getattr(result, "unparseable", 0)
        return result.count, expense_count

    async def sync_managers(self) -> int:
        """
        Sync managers/users from KeyCRM API.

        Returns:
            Number of managers synced
        """
        logger.info("Syncing managers...")
        # Chain 5: every failure is recorded, the watermark held and the step
        # retried after `MANAGERS_RETRY_AFTER_S`, and nothing escapes into the
        # tick or the full sync. Until step 14's S14-11 that held only while
        # the chain was off DuckDB (`pg_managers_write.mode()`), and on DuckDB
        # this method raised as it always had.
        from core import pg_managers_write

        try:
            client = await get_async_client()
            managers = []

            async for batch in client.paginate("users", page_size=50):
                managers.extend(batch)

            count = await self.store.upsert_managers(managers)

            # Update manager order statistics
            await self.store.update_manager_stats()
            await self.store.set_last_sync_time("managers")

            logger.info(f"Synced {count} managers from KeyCRM")
            self.managers_step.succeeded()
            return count
        except KeyCRMConnectionError as e:
            logger.warning(f"Manager sync connection error (will retry): {e}")
            self._managers_step_failed(e)
            return 0
        except KeyCRMAPIError as e:
            logger.error(f"Manager sync API error: {e}")
            self._managers_step_failed(e)
            return 0
        except KeyCRMError as e:
            logger.error(f"Manager sync error: {e}")
            self._managers_step_failed(e)
            return 0
        except Exception as e:  # noqa: BLE001 — contained: chain 5 writes Postgres
            logger.error(
                "Manager sync failed under %s; last_sync_managers not moved, "
                "next attempt in %ss: %s: %s", pg_managers_write.WRITE_ENV,
                MANAGERS_RETRY_AFTER_S, type(e).__name__, e, exc_info=True)
            self._managers_step_failed(e)
            return 0

    def _managers_step_failed(self, exc: BaseException) -> None:
        self.managers_step.failed("managers", exc)
        self._managers_retry_at = time.monotonic() + MANAGERS_RETRY_AFTER_S

    def _managers_retry_in(self) -> Optional[int]:
        remaining = self._managers_retry_at - time.monotonic()
        return int(remaining) + 1 if remaining > 0 else None

    def managers_step_health(self) -> Dict[str, Any]:
        """The `sync_step` entry of chain 5 on /api/health."""
        return self.managers_step.published(self._managers_retry_in())

    async def _managers_step_postgres(self) -> int:
        """The incremental tick's managers step while chain 5 is off DuckDB.
        Never raises.

        The watermark getter is inside the step: under the chain it reads
        `meta.chain_watermarks`, and on a flag nobody can read it raises —
        either way a failure here used to skip the buyers, offers and stocks
        steps for the tick. Inside the retry window nothing is asked of any
        store or of KeyCRM.
        """
        if self._managers_retry_in() is not None:
            return 0
        try:
            last = await self.store.get_last_sync_time("managers")
            if not last or (datetime.now(DEFAULT_TZ) - last).total_seconds() > MANAGERS_STEP_EVERY_S:
                return await self.sync_managers()
            return 0
        except Exception as e:  # noqa: BLE001 — recorded, published, retried
            logger.error("Manager step failed before it started: %s",
                         type(e).__name__, exc_info=True)
            self._managers_step_failed(e)
            return 0

    async def sync_missing_buyers(self, limit: int = 500) -> int:
        """
        Sync buyers that are referenced in orders but not yet in buyers table.

        Fetches buyer details from KeyCRM API for orders that have buyer_id
        but no corresponding buyer record.

        Never raises but for one thing. A failure is recorded in
        `buyer_sync_state`, holds the buyers watermark, and opens the retry
        window, so the incremental tick goes on to offers and stocks and does
        not come back for ten minutes.

        The one thing is a read refused because its engine failed
        (`read_fallback.ReadUnavailable`, DN-20c): recorded the same way, and
        then passed on, because the caller is the one that can answer it — the
        tick names it and skips the step, and `POST /sync/sync-buyers`
        answers 503 naming the surface, as every route does. Answered here it
        was "Synced 0 buyers" to the one and a quiet step to the other.

        Args:
            limit: Maximum number of buyers to sync per call

        Returns:
            Number of buyers synced — 0 on any failure
        """
        from core.landing_rows import birthday_is_unreadable

        state = self.buyer_sync_state
        state.last_attempt_at = datetime.now(DEFAULT_TZ)
        # Until step 14's S14-11 a KS_WRITE_BUYERS nobody could read stopped
        # the step here, before KeyCRM was asked (`pg_buyers_write.mode()`
        # None). The flag is read by nothing now: the buyers go to Postgres.
        logger.info("Syncing missing buyers...")
        try:
            # Get buyer IDs from orders that don't have buyer records.
            #
            # A failed selection returns without moving the buyers watermark:
            # the next tick retries, `freshness_buyers` reports a stall that
            # lasts, and the offers and stocks syncs after this one in the same
            # tick still run — an unreadable selection must not cost the
            # inventory an hour. It used to propagate out of the whole tick.
            try:
                missing_ids = await self.store.get_missing_buyer_ids(limit)
            except read_fallback.ReadUnavailable as e:
                # Recorded like any failure, then passed on (docstring).
                self._buyer_step_failed(e)
                raise
            except Exception as e:  # noqa: BLE001 — logged, watermark held
                logger.error(
                    f"Buyer selection failed, buyers watermark not moved: {e}",
                    exc_info=True)
                self._buyer_step_failed(e)
                return 0

            if not missing_ids:
                logger.info("No missing buyers to sync")
                await self.store.set_last_sync_time("buyers")
                self._buyer_step_ok(selected=0, written=0)
                return 0

            # Sorted before the request and before the write, so two runs over
            # the same selection fetch and write in the same order — a failure
            # then names the same buyer twice rather than a different one.
            missing_ids = sorted(missing_ids)
            logger.info(f"Fetching {len(missing_ids)} missing buyers from KeyCRM...")
            client = await get_async_client()
            fetch_errors: list = []
            buyers = await client.fetch_buyers_by_ids(
                missing_ids, errors_out=fetch_errors)
            buyers = sorted(buyers or [], key=lambda b: b.id)
            if not buyers and fetch_errors:
                # Every fetch failed for a reason other than "no such buyer".
                # That is a KeyCRM outage, not an empty selection, and it must
                # not be stamped as a completed sync: raised as the KeyCRM
                # family it is, it holds the watermark, opens the retry window
                # and — not counted as a step failure — lets the growing age of
                # `last_ok` page after 90 minutes. A breaker that is open
                # (CircuitOpenError) is KeyCRM being unavailable too.
                first = fetch_errors[0]
                if isinstance(first, KeyCRMError):
                    raise first
                raise KeyCRMConnectionError(
                    f"{type(first).__name__}: all {len(missing_ids)} buyer "
                    "fetches failed") from first
            if fetch_errors:
                logger.warning(
                    f"Buyer fetch: {len(fetch_errors)} of {len(missing_ids)} "
                    f"failed ({type(fetch_errors[0]).__name__}); the rest are "
                    "written and the failed ids stay selected for next time")

            if buyers:
                # Mirrored to Postgres inside the store method, portion by
                # portion — the one call site every buyer writer shares. Under
                # chain 4 it writes Postgres alone, and a buyer Postgres would
                # refuse is skipped by id rather than failing the batch.
                skipped: list = []
                count = await self.store.upsert_buyers(buyers, skipped_out=skipped)
                if not count and skipped:
                    raise BuyersRefused(
                        f"all {len(skipped)} buyer(s) fetched were refused by "
                        "Postgres; their ids are in the log above")
                await self.store.set_last_sync_time("buyers")
                logger.info(f"Synced {count} buyers from KeyCRM")
                self._buyer_step_ok(
                    selected=len(missing_ids), written=count,
                    unreadable=sum(1 for b in buyers
                                   if birthday_is_unreadable(b.birthday)),
                    skipped_bad=len(skipped))
                return count

            await self.store.set_last_sync_time("buyers")
            self._buyer_step_ok(selected=len(missing_ids), written=0)
            return 0
        except KeyCRMConnectionError as e:
            logger.warning(f"Buyer sync connection error (will retry): {e}")
            self._buyer_step_failed(e)
            return 0
        except read_fallback.ReadUnavailable:
            # Already recorded where it was raised; the caller answers it.
            raise
        except KeyCRMAPIError as e:
            logger.error(f"Buyer sync API error: {e}")
            self._buyer_step_failed(e)
            return 0
        except KeyCRMError as e:
            logger.error(f"Buyer sync error: {e}")
            self._buyer_step_failed(e)
            return 0
        except Exception as e:  # noqa: BLE001 — recorded and published, never lost
            # A write that failed. Before chain 4's preparation this escaped
            # the tick and cost offers and stocks; now it holds the watermark
            # and waits out the retry window like every other failure here.
            logger.error(
                f"Buyer sync failed, buyers watermark not moved: "
                f"{type(e).__name__}", exc_info=True)
            self._buyer_step_failed(e)
            return 0

    def _buyer_step_ok(self, **counts: int) -> None:
        """Record a completed buyers step and close the retry window."""
        self.buyer_sync_state.succeeded(**counts)
        self._buyers_retry_after = None

    def _buyer_step_failed(self, exc: BaseException) -> None:
        """Record a failed buyers step and open the retry window."""
        self.buyer_sync_state.failed(exc)
        self._buyers_retry_after = time.monotonic() + BUYER_RETRY_AFTER_S

    def _buyers_retry_in(self) -> Optional[int]:
        """Seconds left in the retry window, or None outside it."""
        if self._buyers_retry_after is None:
            return None
        left = self._buyers_retry_after - time.monotonic()
        return int(left) + 1 if left > 0 else None

    def buyer_sync_health(self) -> Dict[str, Any]:
        """The `buyer_sync` block of /api/health."""
        return self.buyer_sync_state.published(self._buyers_retry_in())

    async def sync_offers(self) -> Optional[int]:
        """
        Sync offers (product variations) from KeyCRM API.

        Offers link offer_id to product_id, enabling proper joins
        between offer_stocks and products tables.

        Returns:
            Number of offers synced, or None when KeyCRM failed (logged, and
            kept in `_inventory_keycrm_error`). None, not 0: the stocks after
            it read the offer→product map, and written ahead of their offers a
            new SKU's movement is recorded with no product (L1-14).
        """
        logger.info("Syncing offers...")
        try:
            client = await get_async_client()
            offers = await client.fetch_all_offers()

            count = await self.store.upsert_offers(offers)
            await self.store.set_last_sync_time("offers")

            logger.info(f"Synced {count} offers from KeyCRM")
            return count
        except KeyCRMConnectionError as e:
            logger.warning(f"Offers sync connection error (will retry): {e}")
            self._inventory_keycrm_error = e
            return None
        except KeyCRMAPIError as e:
            logger.error(f"Offers sync API error: {e}")
            self._inventory_keycrm_error = e
            return None
        except KeyCRMError as e:
            logger.error(f"Offers sync error: {e}")
            self._inventory_keycrm_error = e
            return None

    async def sync_stocks(self) -> int:
        """
        Sync offer stocks from KeyCRM API.

        Returns:
            Number of stocks synced
        """
        count = await self._upsert_stocks_from_keycrm()
        if count is None:
            return 0
        await self.store.set_last_sync_time("stocks")

        logger.info(f"Synced {count} offer stocks from KeyCRM")
        return count

    async def _upsert_stocks_from_keycrm(self) -> Optional[int]:
        """Fetch every stock and upsert it; None when KeyCRM failed (logged).

        No watermark: `sync_stocks` moves it straight after, and the Postgres
        path of the tick only once the rebuild and the snapshots have
        committed too (DN-24). A store error propagates.
        """
        logger.info("Syncing offer stocks...")
        try:
            client = await get_async_client()
            stocks = await client.fetch_all_stocks()

            return await self.store.upsert_stocks(stocks)
        except KeyCRMConnectionError as e:
            logger.warning(f"Stock sync connection error (will retry): {e}")
            self._inventory_keycrm_error = e
            return None
        except KeyCRMAPIError as e:
            logger.error(f"Stock sync API error: {e}")
            self._inventory_keycrm_error = e
            return None
        except KeyCRMError as e:
            logger.error(f"Stock sync error: {e}")
            self._inventory_keycrm_error = e
            return None

    def _inventory_retry_in(self) -> Optional[int]:
        remaining = self._inventory_retry_at - time.monotonic()
        return int(remaining) + 1 if remaining > 0 else None

    def inventory_step_health(self) -> Dict[str, Any]:
        """The `sync_step` entry of chain 1 on /api/health."""
        return self.inventory_step.published(self._inventory_retry_in())

    def orders_step_health(self) -> Dict[str, Any]:
        """The `sync_step` entry of chain 3 on /api/health."""
        return self.orders_step.published()

    async def _clear_orders_failure(self) -> None:
        """After an order step that succeeded, idle included: `meta.mirror_state`
        stops saying `bronze.orders` is failing (`ORDERS_FAILURE_CLEAR_SQL`).

        Asked once per process (a failure recorded before a restart is still
        there) and after each failure this process recorded, never on every
        tick. Best effort and bounded the way `_record_orders_failure` is; a
        clear that did not land is asked again on the next healthy tick."""
        if not getattr(self, "_orders_failure_may_stand", True):
            return

        async def clear() -> None:
            from core.pg import get_pool
            from core.pg_landing import ORDERS_TABLE

            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute(ORDERS_FAILURE_CLEAR_SQL, ORDERS_TABLE)

        task = asyncio.ensure_future(clear())
        try:
            await asyncio.wait_for(asyncio.shield(task), ORDERS_FAILURE_RECORD_TIMEOUT_S)
            self._orders_failure_may_stand = False
        except asyncio.TimeoutError:
            logger.warning("Order step's recorded failure not cleared in "
                           "meta.mirror_state within %ss",
                           ORDERS_FAILURE_RECORD_TIMEOUT_S)
        except Exception as e:  # noqa: BLE001 — best effort, asked again next tick
            logger.debug("Order step's recorded failure not cleared: %s", e)
        finally:
            task.cancel()

    async def _record_orders_failure(self, exc: BaseException) -> None:
        """`mirror_failing`'s fast signal, which the sync's mirror gave before
        chain 3 took the tables (DN-22b): `meta.mirror_state` says
        `bronze.orders` is failing. Best effort — with Postgres down this
        fails too, quietly — and bounded behind a shield, the buyers
        watermark's DN-05a form, so a Postgres that neither answers nor
        refuses cannot hold the tick. The class only: the text is a driver's."""
        from core.pg_landing import ORDERS_TABLE, _record_failure

        self._orders_failure_may_stand = True
        task = asyncio.ensure_future(_record_failure(ORDERS_TABLE, type(exc).__name__))
        try:
            await asyncio.wait_for(asyncio.shield(task), ORDERS_FAILURE_RECORD_TIMEOUT_S)
        except asyncio.TimeoutError:
            logger.warning("Order step failure not recorded in meta.mirror_state "
                           "within %ss", ORDERS_FAILURE_RECORD_TIMEOUT_S)
        finally:
            task.cancel()

    async def _inventory_step_postgres(self, stats: Dict[str, Any]) -> None:
        """Offers then stocks, while chain 1 writes Postgres. Never raises.

        The same four writes and one event as the DuckDB path, in the same
        order, with two differences that are the point of it (DN-24): a
        failure anywhere — the watermark reads included, since under this
        path they are Postgres reads too — is recorded and ends the step, not
        the tick; and `last_sync_stocks` moves last, so a rebuild or a
        snapshot that failed is retried whole rather than an hour later.
        """
        if self._inventory_retry_in() is not None:
            return

        def due(last: Optional[datetime]) -> bool:
            return (not last or (datetime.now(DEFAULT_TZ) - last).total_seconds()
                    > INVENTORY_STEP_EVERY_S)

        step, ran, stocked = "offers", False, False
        # Which fetch KeyCRM failed, if one did. Not a success: the step used
        # to publish `last_ok_at` over a fetch that never arrived (L1-14). Not
        # a store failure either, so no hold — the fetch is what is retried on
        # the next tick, as it always was.
        keycrm_failed: Optional[str] = None
        self._inventory_keycrm_error = None
        try:
            if due(await self.store.get_last_sync_time("offers")):
                ran = True
                offers = await self.sync_offers()
                stats["offers"] = offers or 0
                if offers is None:
                    keycrm_failed = "offers"

            step = "stocks"
            # Not after a failed offers fetch: the stocks read the offer→
            # product map, and a new SKU's movement would be recorded with no
            # product.
            if keycrm_failed is None and due(await self.store.get_last_sync_time("stocks")):
                ran = stocked = True
                count = await self._upsert_stocks_from_keycrm()
                stats["stocks"] = count or 0
                await self.store.refresh_sku_inventory_status()
                await self.store.record_sku_inventory_snapshot()
                await self.store.record_inventory_snapshot()
                # Not when KeyCRM failed: the fetch is what is retried then,
                # as on the DuckDB path.
                if count is not None:
                    await self.store.set_last_sync_time("stocks")
                    logger.info(f"Synced {count} offer stocks from KeyCRM")
                else:
                    keycrm_failed = "stocks"
        except Exception as exc:  # noqa: BLE001 — recorded, published, retried
            self.inventory_step.failed(step, exc)
            self._inventory_retry_at = time.monotonic() + INVENTORY_RETRY_AFTER_S
            logger.error(
                "Inventory %s step failed on Postgres; last_sync_%s not moved, "
                "next attempt in %ss: %s", step, step, INVENTORY_RETRY_AFTER_S,
                exc, exc_info=True)
            return

        if keycrm_failed is not None:
            self.inventory_step.failed(
                keycrm_failed,
                self._inventory_keycrm_error or KeyCRMError("KeyCRM fetch failed"))
        elif ran:
            self.inventory_step.succeeded()
        if stocked:
            await events.emit(SyncEvent.INVENTORY_UPDATED, {"stocks_count": stats["stocks"]})

    async def _orders_step(self, client, stats: Dict[str, Any]) -> list:
        """Fetch the orders window, write it, move `last_sync_orders`.
        Returns the ids actually written. Raises what it meets: the tick
        decides what a failure costs (`incremental_sync`)."""
        # A Postgres read under chain 3, bounded behind a shield — the buyers
        # watermark's DN-05a form: a store that neither answers nor refuses
        # must fail the step, not hold the tick.
        read = asyncio.ensure_future(self.store.get_last_sync_time("orders"))
        try:
            last_orders_sync = await asyncio.wait_for(
                asyncio.shield(read), ORDERS_WATERMARK_TIMEOUT_S)
        finally:
            read.cancel()
        # Default to 1 hour ago if never synced — never, while the key is
        # absent from Postgres but present in DuckDB's frozen copy: chain 3
        # inherits that (`CHAIN_WATERMARK_INHERITS_DUCKDB`).
        if not last_orders_sync:
            last_orders_sync = datetime.now(DEFAULT_TZ) - timedelta(hours=1)

        # Add buffer for API delays - extended to 24 hours to catch backdated orders
        sync_from = last_orders_sync - timedelta(hours=24)
        sync_to = datetime.now(DEFAULT_TZ) + timedelta(minutes=5)

        # Sync new orders with expenses using the smart date filter helper
        # Explicitly localise to DEFAULT_TZ before formatting to survive DST transitions
        orders = await self._fetch_orders_with_date_filter(
            client,
            sync_from.astimezone(DEFAULT_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            sync_to.astimezone(DEFAULT_TZ).strftime('%Y-%m-%d %H:%M:%S'),
        )

        # Ids actually WRITTEN, not ids fetched. The sync window is the
        # trailing 24h, so `orders` is ~200 rows on every run whether or not
        # anything about them moved; marking all of them dirty cascaded to
        # their buyers' full histories (~1300 orders) and 583 distinct dates,
        # and rebuilt Gold for all of it every two minutes around the clock.
        changed_ids: list[int] = []
        if orders:
            self._last_unparseable = 0
            order_count, expense_count = await self._upsert_orders_with_expenses(
                orders, changed_ids_out=changed_ids,
            )
            # Nothing landed and the parse refused orders: a shape nothing
            # could read, not one bad order. A failure, before the watermark
            # moves (`OrdersRefused`).
            if not order_count and not expense_count and self._last_unparseable:
                raise OrdersRefused(
                    f"none of the {len(orders)} order(s) fetched landed and the "
                    f"parse could not read {self._last_unparseable} of them; "
                    "their ids are in the log above")
            stats["orders"] = order_count
            stats["expenses"] = expense_count
            # The derivation is owed by the writer itself: chain 3 raises
            # `meta.derivation_signal` in the order's own transaction. The
            # DuckDB warehouse mark that stood here, a no-op since step 13,
            # went in step 14's S14-15.
            # Use max(updated_at) from SOURCE data, not now()
            max_updated = _get_max_updated_at(orders)
            # Guard against checkpoint rollback — never go backward
            if max_updated and (not last_orders_sync or max_updated >= last_orders_sync):
                await self.store.set_last_sync_time("orders", max_updated)
            else:
                # Source returned stale timestamps; advance to sync_to instead
                await self.store.set_last_sync_time("orders", sync_to)
            logger.info(f"Incremental sync: {stats['orders']} orders, {stats['expenses']} expenses, checkpoint: {max_updated}")

            # Emit orders synced event
            await events.emit(SyncEvent.ORDERS_SYNCED, {
                "count": order_count,
                "expenses": expense_count,
                "checkpoint": max_updated.isoformat() if max_updated else None,
            })
        else:
            # No new orders — do NOT advance checkpoint.
            # The 24h buffer in sync_from already prevents re-scanning old data,
            # and advancing here risks skipping orders created during the window.
            logger.debug("No new orders in sync window, checkpoint unchanged")
        return changed_ids

    def _catalogue_retry_in(self) -> Optional[int]:
        remaining = self._catalogue_retry_at - time.monotonic()
        return int(remaining) + 1 if remaining > 0 else None

    def catalogue_step_health(self) -> Dict[str, Any]:
        """The `sync_step` entry of chain 6 on /api/health."""
        return self.catalogue_step.published(self._catalogue_retry_in())

    def _catalogue_step_failed(self, exc: BaseException) -> None:
        self.catalogue_step.failed("products", exc)
        self._catalogue_retry_at = time.monotonic() + CATALOGUE_RETRY_AFTER_S
        logger.error(
            "Catalogue products step failed off DuckDB; last_sync_products not "
            "moved, next attempt in %ss: %s: %s", CATALOGUE_RETRY_AFTER_S,
            type(exc).__name__, exc, exc_info=True)

    async def _catalogue_step_postgres(self, client, stats: Dict[str, Any]) -> None:
        """The hourly products step, chain 6's (step 14's S14-09 deleted the
        DuckDB one). Never raises a chain fault: a watermark it cannot read, a
        catalogue Postgres refuses or a Postgres that is down is recorded, and
        the tick goes on to managers, buyers and inventory. So does a KeyCRM
        error, recorded under the step without a `meta.mirror_state` stamp
        (L1-02); it used to end the tick.

        Nothing is put in `stats` but a count: the tick sums it."""
        from core.pg_landing import _record_failure

        if self._catalogue_retry_in() is not None:
            return
        try:
            # Shielded, the buyers step's form (DN-05a): a bare `wait_for`
            # waits for a cancelled Postgres read to unwind.
            read = asyncio.ensure_future(self.store.get_last_sync_time("products"))
            try:
                last = await asyncio.wait_for(
                    asyncio.shield(read), CATALOGUE_WATERMARK_TIMEOUT_S)
            finally:
                read.cancel()
        except Exception as exc:  # noqa: BLE001 — recorded, published, retried
            self._catalogue_step_failed(exc)
            return
        if last and (datetime.now(DEFAULT_TZ) - last).total_seconds() <= CATALOGUE_STEP_EVERY_S:
            return

        logger.info("Syncing products (hourly, chain 6)...")
        products = []
        try:
            async for batch in client.paginate("products", params={"include": "custom_fields"}, page_size=50):
                products.extend(batch)
        except KeyCRMError as exc:
            # KeyCRM's, contained like the other steps' (L1-02): an open
            # breaker or a page that would not decode here used to end the
            # tick, so managers, buyers and inventory never ran. Recorded under
            # the step and retried after the hold; nothing in Postgres says
            # the catalogue is failing, because nothing there failed.
            self._catalogue_step_failed(exc)
            return
        try:
            stats["products"] = await self.store.upsert_products(products)
            await self.store.set_last_sync_time("products")
        except Exception as exc:  # noqa: BLE001 — recorded, published, retried
            self._catalogue_step_failed(exc)
            # Best effort, so meta.mirror_state does not keep reading healthy
            # over a write that failed on its own data.
            await _record_failure("bronze.products", f"{type(exc).__name__}: {exc}")
            return
        # No mirror call: chain 6 owns the catalogue, and `_mirror` stands
        # down for it in any case.
        self.catalogue_step.succeeded()
        await events.emit(SyncEvent.PRODUCTS_SYNCED, {"count": stats["products"]})

    async def sync_to_meilisearch(self) -> Dict[str, int]:
        """
        Sync buyers, orders, and products to Meilisearch.

        Incremental: uses the source rows' bookkeeping stamp as a watermark. On
        the 5-minute scheduler tick, only rows touched since the last successful
        Meili sync are pushed — a no-op tick costs one MAX() query, not a full
        37K re-index. First run (no watermark) falls back to a full sync.

        Built from Postgres, under its own watermark key
        (`pg_search_index_read.WATERMARK_KEY`, `mirrored_at`'s clock) — see
        `core/pg_search_index_read.py`. One engine since step 14's S14-07:
        `KS_READ_SEARCH_INDEX` chose DuckDB's frames and its `meilisearch`
        key before, and both went with S14-07; the documents are the ones
        both engines rendered alike.

        Returns:
            Dict with counts for each synced entity
        """
        from core import pg_search_index_read as pg_index
        from core import read_fallback

        stats = {"buyers": 0, "orders": 0, "products": 0}

        try:
            meili = get_meili_client()

            # The watermark is chain 3's sync key since OD-15
            # (`pg_orders_write.CHAIN_SYNC_KEYS`): the store's getter and
            # setter keep it in `meta.chain_watermarks`.
            watermark_key = pg_index.WATERMARK_KEY
            last_sync = await self.store.get_last_sync_time(watermark_key)

            # If nothing has been stamped since the watermark, skip entirely.
            high_watermark = await pg_index.high_watermark()
            if last_sync and high_watermark and high_watermark <= last_sync:
                logger.debug("Meilisearch sync: no changes since %s, skipping", last_sync)
                return stats

            full_sync = last_sync is None
            since = None if full_sync else last_sync
            logger.info(
                "Syncing data to Meilisearch... (%s, from %s)",
                "full" if full_sync else f"incremental since {last_sync.isoformat()}",
                "postgres",
            )

            # ── Buyers ─────────────────────────────────────────────────────
            # Incremental: only buyers whose row was re-synced (profile edit)
            # OR whose order activity was re-synced (order_count changed).
            buyers_df = await pg_index.buyers_frame(since)
            if not buyers_df.empty:
                if 'created_at' in buyers_df.columns:
                    buyers_df['created_at'] = buyers_df['created_at'].apply(
                        lambda x: x.isoformat() if x else None
                    )
                stats["buyers"] = await meili.index_buyers(buyers_df.to_dict('records'))

            # ── Orders ─────────────────────────────────────────────────────
            MEILI_CHUNK = 10_000
            offset = 0
            while True:
                orders_df = await pg_index.orders_frame(since, MEILI_CHUNK, offset)

                if orders_df.empty:
                    break

                if 'ordered_at' in orders_df.columns:
                    orders_df['ordered_at'] = orders_df['ordered_at'].apply(
                        lambda x: x.isoformat() if x else None
                    )
                if 'order_date' in orders_df.columns:
                    orders_df['order_date'] = orders_df['order_date'].apply(
                        lambda x: x.isoformat() if x else None
                    )
                stats["orders"] += await meili.index_orders(orders_df.to_dict('records'))
                offset += MEILI_CHUNK

                if len(orders_df) < MEILI_CHUNK:
                    break

            # ── Products ───────────────────────────────────────────────────
            products_df = await pg_index.products_frame(since)
            if not products_df.empty:
                stats["products"] = await meili.index_products(products_df.to_dict('records'))

            logger.info(f"Meilisearch sync complete: {stats}")
            # Persist the watermark we actually covered, not wall-clock now().
            # Using now() would skip rows written between our MAX() and this line.
            if high_watermark:
                await self.store.set_last_sync_time(watermark_key, timestamp=high_watermark)
            else:
                await self.store.set_last_sync_time(watermark_key)
            return stats

        except read_fallback.ReadUnavailable:
            # DN-20c: raised to the caller, which skips the step and says so
            # — the scheduler's job in its result, the boot in its log. Kept
            # below, it read as an index with nothing to refresh.
            raise
        except Exception as e:
            logger.error(f"Meilisearch sync error: {e}")
            return stats

    async def _catalogue_full_sync_failed(
            self, stats: Dict[str, Any], entity: str, exc: BaseException) -> None:
        """Record a chain 6 write `full_sync` contained: its watermark stays
        where it was, so the freshness check sees a catalogue that did not
        land, and meta.mirror_state says so too (best effort)."""
        from core.pg_landing import _record_failure

        stats[f"{entity}_error"] = f"{type(exc).__name__}: {exc}"
        logger.error(
            "Full sync: the %s were not stored (%s); their watermark stays where "
            "it was and the sync carries on", entity, stats[f"{entity}_error"],
            exc_info=True)
        await _record_failure(f"bronze.{entity}", stats[f"{entity}_error"])

    async def full_sync(
        self, days_back: int = 730, force_update: bool = False,
    ) -> Dict[str, Any]:
        """
        Perform full sync of all data from KeyCRM.

        Args:
            days_back: Number of days of historical data to sync
            force_update: rewrite every order in the window even when its
                `updated_at` has not moved — what `force_resync` asks for,
                because KeyCRM does not bump `updated_at` on every change
                and a resync exists to overwrite what we hold.

        Returns:
            Dict with sync statistics
        """
        with correlation_context() as corr_id:
            logger.info(
                f"Starting full sync (last {days_back} days)",
                extra={"days_back": days_back, "sync_type": "full"}
            )

        stats = {"orders": 0, "products": 0, "categories": 0, "expense_types": 0, "expenses": 0, "managers": 0, "offers": 0, "stocks": 0}

        try:
            client = await get_async_client()

            # Sync managers first (needed for retail/b2b filtering)
            stats["managers"] = await self.sync_managers()

            # Fetch categories, expense_types, and products in parallel (independent data)
            logger.info("Syncing categories, expense types, and products in parallel...")

            async def fetch_categories():
                items = []
                async for batch in client.paginate("products/categories", page_size=50):
                    items.extend(batch)
                return items

            async def fetch_expense_types():
                items = []
                async for batch in client.paginate("order/expense-type", page_size=50):
                    items.extend(batch)
                return items

            async def fetch_products():
                items = []
                async for batch in client.paginate("products", params={"include": "custom_fields"}, page_size=50):
                    items.extend(batch)
                return items

            # Parallel fetch (3x faster than sequential)
            categories, expense_types, products = await asyncio.gather(
                fetch_categories(),
                fetch_expense_types(),
                fetch_products()
            )

            # Upsert to database (sequential due to DuckDB single-writer)
            try:
                stats["categories"] = await self.store.upsert_categories(categories)
                await self.store.set_last_sync_time("categories")
            except Exception as exc:
                # Chain 6, chain 6a's containment: this is the Postgres write,
                # and its fault stops the catalogue only — on a boot with no
                # orders held, aborting here would cost the whole history.
                await self._catalogue_full_sync_failed(stats, "categories", exc)
            # Chain 6 writes the catalogue to Postgres above. Until step 14's
            # S14-13 the landing mirror shipped the same payload after it
            # (`pg_landing.mirror_categories`), standing down since the chain
            # owned the table; it went with the mirror.

            try:
                stats["expense_types"] = await self.store.upsert_expense_types(expense_types)
                await self.store.set_last_sync_time("expense_types")
            except Exception as exc:
                # Chain 6a (DN-26). Both statements above go to Postgres and
                # raise when it fails. It is this chain's fault and it stops
                # this chain only (DN-01): the watermark stays where it was, so
                # the freshness check sees a dictionary that did not land, and
                # products and orders still sync. Aborting here would cost a
                # week of orders for 27 names — and on a boot with no orders
                # held, the whole history. Until step 14's S14-09 the raise
                # went out while the chain was on DuckDB.
                stats["expense_types_error"] = f"{type(exc).__name__}: {exc}"
                logger.error(
                    "Full sync: the expense-type dictionary was not stored (%s); "
                    "its watermark stays where it was and the sync carries on",
                    stats["expense_types_error"], exc_info=True,
                )
            # Under chain 6a the write above IS the Postgres write
            # (`core/pg_expense_types_write.py`). Until it, the dictionary
            # rode the hourly copy out of DuckDB, which went in step 14's
            # S14-13.

            try:
                stats["products"] = await self.store.upsert_products(products)
                await self.store.set_last_sync_time("products")
            except Exception as exc:
                await self._catalogue_full_sync_failed(stats, "products", exc)

            # Sync orders with expenses - in chunks to avoid pagination limit (100 pages × 50 = 5000 orders max)
            # IMPORTANT: Save each chunk immediately to preserve progress on timeout/crash
            logger.info("Syncing orders...")
            final_end_date = datetime.now(DEFAULT_TZ) + timedelta(days=1)
            chunk_days = 30  # Sync in 1-month chunks to limit memory usage during bulk insert
            chunk_num = 0

            current_start = datetime.now(DEFAULT_TZ) - timedelta(days=days_back)

            while current_start < final_end_date:
                chunk_num += 1
                current_end = min(current_start + timedelta(days=chunk_days), final_end_date)
                logger.info(f"Chunk {chunk_num}: Fetching orders from {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}...")

                chunk_orders = await self._fetch_orders_with_date_filter(
                    client,
                    current_start.strftime('%Y-%m-%d'),
                    current_end.strftime('%Y-%m-%d')
                )
                logger.info(f"  Chunk {chunk_num}: Got {len(chunk_orders)} orders, saving to DB...")

                # Save chunk immediately to preserve progress
                if chunk_orders:
                    order_count, expense_count = await self._upsert_orders_with_expenses(
                        chunk_orders,
                        force_update=force_update,
                    )
                    stats["orders"] += order_count
                    stats["expenses"] += expense_count
                    logger.info(f"  Chunk {chunk_num}: Saved {order_count} orders, {expense_count} expenses")
                    # A DuckDB CHECKPOINT followed each chunk under
                    # KS_DUCKDB=on until step 14's S14-18 (a WAL bug of
                    # DuckDB 1.4.x on aarch64); the chunks land in Postgres.

                current_start = current_end + timedelta(days=1)

            logger.info(f"All chunks complete. Total: {stats['orders']} orders, {stats['expenses']} expenses")

            # Sync offers (needed for proper stock-to-product linking)
            offers = await self.sync_offers()
            stats["offers"] = offers or 0

            # Sync stocks — not after a failed offers fetch, whose map they read
            stats["stocks"] = await self.sync_stocks() if offers is not None else 0

            # Refresh Layer 1: sku_inventory_status
            await self.store.refresh_sku_inventory_status()

            # Record Layer 2: daily per-SKU snapshot
            await self.store.record_sku_inventory_snapshot()

            # DuckDB's own Silver → Gold rebuild stood here until step 14's
            # S14-15; it did not run since step 13 (DN-29). Postgres derives
            # off the signal the writers raise.

            # Update sync checkpoint with latest order timestamp
            last_order_time = await self.store.get_latest_order_time()
            await self.store.set_last_sync_time("orders", last_order_time)
            logger.info(f"Full sync complete: {stats}, checkpoint: {last_order_time}")

        except KeyCRMConnectionError as e:
            logger.error(f"Full sync connection error: {e}", exc_info=True)
            raise
        except KeyCRMAPIError as e:
            logger.error(f"Full sync API error (status={e.status_code}): {e}", exc_info=True)
            raise
        except KeyCRMError as e:
            logger.error(f"Full sync error: {e}", exc_info=True)
            raise

        return stats

    async def incremental_sync(self) -> Dict[str, Any]:
        """
        Perform incremental sync - only fetch new/updated data since last sync.

        Uses adaptive backoff to reduce API calls during quiet periods:
        - Exponential backoff (60s → 120s → 240s → 300s) when no new orders
        - Extended intervals (5 min) during off-hours (11 PM - 7 AM Kyiv)
        - Instant reset to 60s when new orders are found

        Returns:
            Dict with sync statistics (includes "skipped" key if sync was skipped)
        """
        import time

        # Check if we should skip this sync cycle (adaptive backoff)
        should_skip, skip_reason = self._should_skip_sync()
        if should_skip:
            logger.info(f"Adaptive sync: skipped ({skip_reason})")
            return {"skipped": True, "reason": skip_reason}

        start_time = time.perf_counter()
        stats = {"orders": 0, "products": 0, "categories": 0, "expenses": 0, "managers": 0, "buyers": 0, "offers": 0, "stocks": 0}
        error_occurred = None

        # Emit sync started event
        await emit_sync_started("incremental")

        try:
            client = await get_async_client()

            # The order step, chain 3's: a failure that is not KeyCRM's is
            # recorded and the tick goes on (`OrdersStepState`).
            changed_ids: list[int] = []
            self.orders_step.attempted()
            self._last_refused = 0
            try:
                changed_ids = await self._orders_step(client, stats)
                self.orders_step.succeeded(refused=self._last_refused)
                await self._clear_orders_failure()
            except KeyCRMError:
                raise
            except Exception as exc:
                self.orders_step.failed(exc)
                logger.error(
                    "Order step failed under chain 3; last_sync_orders not "
                    "moved, the rest of the tick runs: %s", type(exc).__name__,
                    exc_info=True)
                await self._record_orders_failure(exc)

            # Sync products less frequently (every hour). Chain 6's step reads
            # its own watermark, and is recorded and retried, never the
            # tick's end.
            await self._catalogue_step_postgres(client, stats)

            # Sync managers daily. Chain 5's step is contained, so the buyers,
            # offers and stocks below run whatever happens here (until step
            # 14's S14-11 only while the chain was off DuckDB).
            stats["managers"] = await self._managers_step_postgres()

            # Sync missing buyers (fetch buyer details for orders that don't have them)
            #
            # Contained, and in this order on purpose: the retry window is
            # checked BEFORE the watermark getter, so a step that failed asks
            # nothing of any store for ten minutes; the getter is bounded; and
            # whatever escapes is recorded and dropped here, so offers and
            # stocks below run whatever happened. The failure is NOT put in
            # `stats`: that dict is summed in `finally`, and a string there is a
            # TypeError that would lose the tick's completion event too.
            if self._buyers_retry_in() is None:
                try:
                    # Behind a shield, the DN-05a form `_run_pg_derivation`
                    # uses: a bare `wait_for` waits for the cancelled read to
                    # unwind, and a Postgres read cut inside a query unwinds
                    # into asyncpg's cancel request to a server that may not
                    # answer — measured 150 s past a 10 s bound on a paused
                    # container. Shielded, the tick returns at the bound and
                    # the read is cancelled and left to unwind on its own.
                    read = asyncio.ensure_future(
                        self.store.get_last_sync_time("buyers"))
                    try:
                        last_buyers_sync = await asyncio.wait_for(
                            asyncio.shield(read), BUYER_WATERMARK_TIMEOUT_S)
                    finally:
                        read.cancel()
                    if not last_buyers_sync or (datetime.now(DEFAULT_TZ) - last_buyers_sync).total_seconds() > 3600:
                        stats["buyers"] = await self.sync_missing_buyers()
                except read_fallback.ReadUnavailable as e:
                    # The step recorded it and holds its watermark; the tick
                    # skips it and goes on (DN-20c). Not in `stats`, which is
                    # summed — the refusal shows on /api/health as buyer_sync's
                    # error class and in read_fallback_mode.refused.
                    read_fallback.answered(
                        "incremental_sync", e,
                        "skipped the buyers step; its watermark is held")
                except Exception as e:  # noqa: BLE001 — recorded and published
                    logger.error(
                        f"Buyer step failed before it started: {type(e).__name__}",
                        exc_info=True)
                    self._buyer_step_failed(e)

            # Chain 1 writes Postgres: a failure there is recorded and the
            # tick goes on (DN-24). See `INVENTORY_RETRY_AFTER_S`. Until step
            # 14's S14-11 a chain on DuckDB took the uncontained path here.
            await self._inventory_step_postgres(stats)

            # Silver reads only `orders`, but Gold joins products and categories,
            # THE CATALOGUE NO LONGER HAS TO ASK FOR ITSELF
            #
            # A renamed product or a re-parented category used to change Gold
            # rows with no order involved, because `gold_daily_products` was
            # the only rebuilt table that joined the catalogue — so this raised
            # a dirty flag that widened *that* layer's scope to every date, and
            # nothing else's.
            #
            # That layer is retired: three tabs and the weekly report each read
            # the order-lines level instead, and the level takes the catalogue
            # name through the same joins at read time. A rename is therefore
            # visible on the next page load rather than on the next rebuild,
            # and there is nothing left for a dirty flag to schedule.

            # Adaptive backoff follows the same number. Keyed on the inflated
            # count it never saw a quiet period, so it sat on the 60s floor
            # around the clock instead of stepping out to 300s overnight.
            self._update_backoff(len(changed_ids))

        except KeyCRMConnectionError as e:
            error_occurred = str(e)
            # On connection error, don't increase backoff (might be temporary)
            self._last_sync_time = datetime.now(DEFAULT_TZ)
            logger.warning(f"Incremental sync connection error (will retry): {e}")
        except KeyCRMAPIError as e:
            error_occurred = str(e)
            logger.error(f"Incremental sync API error: {e}", exc_info=True)
        except KeyCRMError as e:
            error_occurred = str(e)
            logger.error(f"Incremental sync error: {e}", exc_info=True)
        finally:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            total_records = sum(stats.values())
            logger.info(
                "Incremental sync completed",
                extra={"duration_ms": round(elapsed_ms, 2), "stats": stats}
            )

            # Emit sync completed or failed event
            if error_occurred:
                await emit_sync_failed("incremental", error_occurred, stats=stats, duration_ms=elapsed_ms)
            else:
                await emit_sync_completed("incremental", elapsed_ms, total_records, stats=stats)

        return stats

    # `sync_today` stood here until step 14's S14-18: today's orders, fetched
    # and written, with no caller anywhere (the stage 5 entry-point walk listed
    # it dead). The incremental sync covers today every minute.

    async def refresh_order_statuses(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Re-fetch recent orders to catch status changes.

        KeyCRM does NOT update the `updated_at` field when order status changes,
        so the incremental sync (which relies on updated_between) misses these.
        This method re-fetches orders by created_between to refresh all statuses,
        then by id the backdated orders that fetch cannot return: dated into
        the window, created before it (`_refresh_backdated_orders`).

        Args:
            days_back: Number of days to look back (default 30)

        Returns:
            Dict with sync statistics
        """
        stats = {"orders": 0, "expenses": 0, "days_back": days_back}

        try:
            client = await get_async_client()
            start_date = datetime.now(DEFAULT_TZ) - timedelta(days=days_back)
            end_date = datetime.now(DEFAULT_TZ) + timedelta(days=1)

            logger.info(f"Refreshing order statuses for last {days_back} days...")

            # Fetch by created_between ONLY (not updated_between)
            # This ensures we get ALL orders regardless of their updated_at
            orders_by_id = {}
            params = {
                "include": "products.offer,manager,buyer,expenses",
                "filter[created_between]": f"{start_date.strftime('%Y-%m-%d')}, {end_date.strftime('%Y-%m-%d')}",
            }

            async for batch in client.paginate("order", params=params, page_size=50):
                for order in batch:
                    orders_by_id[order["id"]] = order

            orders = list(orders_by_id.values())

            if orders:
                # Use force_update=True because KeyCRM doesn't update updated_at on status changes
                # Without this, orders with changed status but same updated_at won't be updated
                order_count, expense_count = await self._upsert_orders_with_expenses(
                    orders, force_update=True, skip_products=True,
                )
                stats["orders"] = order_count
                stats["expenses"] = expense_count
                logger.info(f"Status refresh: force-updated {order_count} orders, {expense_count} expenses")

                # Log return orders from API for diagnostics
                api_returns = {
                    o["id"]: o.get("status_id")
                    for o in orders
                    if o.get("status_id") in (19, 21, 22, 23)
                }
                if api_returns:
                    logger.info(
                        f"Status refresh: {len(api_returns)} return orders from API "
                        f"(sample: {dict(list(api_returns.items())[:5])})"
                    )

                # The headers written above raise Postgres' derivation signal
                # in their own transaction, and the derivation rebuilds from
                # it. (DuckDB's incremental rebuild of these ids stood here
                # until step 14's S14-15; it did not run since step 13.)
                # No read-back of Silver here. A sample of twenty return orders
                # used to be compared with DuckDB's `silver_orders`, and all it
                # produced was a log line. `dq_reconciliation` checks every
                # order's status against KeyCRM at 05:30 instead — Bronze in
                # both stores, Silver through the ClickHouse arm — and files a
                # moved status as STATUS_DRIFT where somebody reads it.

            # The orders the fetch above cannot return: dated into the window,
            # created before it. By id, before dq_reconciliation looks at 05:30.
            stats["backdated"] = await self._refresh_backdated_orders(start_date.date())

        except KeyCRMConnectionError as e:
            logger.warning(f"Status refresh connection error (will retry): {e}")
        except KeyCRMError as e:
            logger.error(f"Status refresh error: {e}")

        return stats

    async def _refresh_backdated_orders(self, since) -> Dict[str, Any]:
        """Re-fetch by id the orders dated on or after `since` but created
        before it (`find_backdated_order_ids`). Never raises: the refresh
        above has already committed, and this is one step after it.

        KeyCRM lets a B2B order's `ordered_at` sit weeks after its
        `created_at`, and does not move `updated_at` when a status changes. A
        `created_between` fetch misses such an order once its creation date
        has left the window. The weekly full sync skips it as unchanged, and
        `dq_reconciliation` repairs only orders we do not hold. So a status
        that moved on one of these was filed as STATUS_DRIFT (CRITICAL) every
        morning until its order date left the 90-day window. Until OD-10 the
        legacy 06:00 `reconciliation_check` fixed it, by padding its fetch by
        30 days. This is that repair, kept after the job was retired.

        The caller holds the heavy-job lock (the 05:15 job and the manual
        route both do), so it is not passed on: `repair_orders` does not
        re-enter it.
        """
        try:
            ids = await self.store.find_backdated_order_ids(
                since, limit=self.REPAIR_BATCH_LIMIT)
            if not ids:
                return {"found": 0}
            result = await self.repair_orders(ids)
            logger.info("Status refresh: re-fetched %d of %d backdated order(s) by id",
                        result["repaired"], len(ids))
            return {"found": len(ids), "repaired": result["repaired"],
                    "failed": result["failed"]}
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — the refresh itself is done
            logger.error("Status refresh: backdated orders were not re-fetched: %s: %s",
                         type(exc).__name__, exc, exc_info=True)
            return {"error": type(exc).__name__}

    # An order-by-order repair is one API call each, so a run is bounded and the
    # remainder is picked up on the next one rather than fired off in a burst
    # that trips the rate limiter.
    REPAIR_BATCH_LIMIT = 200

    async def repair_orders(
        self, order_ids, *, limit: int | None = None,
        lock: "asyncio.Lock | None" = None,
    ) -> Dict[str, Any]:
        """Re-fetch specific orders from KeyCRM and upsert them whole.

        The date-window syncs cannot reach these: an order absent from our copy
        is invisible to a delta sync keyed on updated_at, and one whose line
        items failed to write looks complete because the header is there. Both
        need the order pulled again by id.

        Only ever adds or corrects — nothing is deleted, so this is safe to run
        automatically against ids that a comparison against the source flagged.

        `lock` is the scheduler's heavy-job lock, held around the write and the
        dirty mark only — never around the per-id KeyCRM fetches, which would
        hold the sync up for a batch of HTTP calls. Without it a repair landing
        between a refresh's Silver commit and its validation read turned a
        correct rebuild into "validation failed" (a spurious alert and a full
        rebuild), and a repair's mirror call could overwrite a fresher order
        the sync had just written to Postgres. Not reentrant: a caller that
        already holds the lock must not pass it.
        """
        ids = list(dict.fromkeys(int(i) for i in order_ids))
        cap = self.REPAIR_BATCH_LIMIT if limit is None else limit
        attempted, fetched = ids[:cap], []
        failures: Dict[int, str] = {}

        if not attempted:
            return {"requested": 0, "attempted": 0, "repaired": 0,
                    "failed": 0, "remaining": 0, "failures": {}}

        # Postgres' clock before the first fetch (L1-12): an order some writer
        # stores after this instant is newer than the payload we are about to
        # fetch, and written under force the payload would put the older state
        # back — the 05:15 refresh's status, overwritten by a stale fetch that
        # carried the same `updated_at`. Best effort: unread, nothing is
        # skipped, and a Postgres that cannot answer fails the write anyway.
        try:
            from core.pg_orders_read import postgres_now

            fetch_began = await postgres_now()
        except Exception as exc:  # noqa: BLE001 — see above
            logger.warning("Order repair: Postgres' clock unread, every fetched "
                           "order is written: %s", type(exc).__name__)
            fetch_began = None

        client = await get_async_client()
        for order_id in attempted:
            try:
                order = await client.get_order(
                    order_id, include="products.offer,manager,buyer,expenses",
                )
                if order and order.get("id"):
                    fetched.append(order)
                else:
                    failures[order_id] = "not found in KeyCRM"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures[order_id] = f"{type(exc).__name__}: {exc}"

        repaired = 0
        rewritten: list = []
        refused: list = []
        if fetched:
            held = lock if lock is not None else contextlib.nullcontext()
            async with held:
                # Under the lock, so nothing the sync writes can land between
                # this read and the write below.
                if fetch_began is not None:
                    try:
                        from core.pg_orders_read import rewritten_since

                        rewritten = await rewritten_since(
                            [o["id"] for o in fetched], fetch_began)
                    except Exception as exc:  # noqa: BLE001 — as for the clock
                        logger.warning("Order repair: could not ask which orders "
                                       "were rewritten: %s", type(exc).__name__)
                if rewritten:
                    newer = set(rewritten)
                    fetched = [o for o in fetched if int(o["id"]) not in newer]
                # force_update: the whole point is to overwrite what we hold,
                # and KeyCRM's updated_at may well be older than our last touch.
                if fetched:
                    self._last_refused_ids = ()
                    repaired, _ = await self._upsert_orders_with_expenses(
                        fetched, force_update=True,
                    )
                    refused = list(getattr(self, "_last_refused_ids", ()))
        # A refused order is a failure the caller can ledger, so the id scan
        # moves on instead of asking KeyCRM for it every run (L1-10).
        for order_id, why in refused:
            failures[int(order_id)] = f"Postgres refused: {why}"

        result = {
            "requested": len(ids),
            "attempted": len(attempted),
            "repaired": int(repaired),
            "failed": len(failures),
            "remaining": max(0, len(ids) - len(attempted)),
            # Every failure, not a sample. The caller decides which ids are
            # permanently absent and must never be asked for again; handing it
            # ten of thirty-seven meant the rest came round every hour forever.
            "failures": failures,
            # Fetched, and not written: Postgres held a newer write of them.
            "rewritten_since_fetch": sorted(rewritten),
        }
        logger.info(
            "Order repair: %s",
            {**result, "failures": dict(list(failures.items())[:10])},
        )
        return result


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_sync_service: Optional[SyncService] = None
_sync_service_lock = asyncio.Lock()


async def get_sync_service() -> SyncService:
    """Get singleton sync service instance (coroutine-safe)."""
    global _sync_service
    async with _sync_service_lock:
        if _sync_service is None:
            store = await get_store()
            _sync_service = SyncService(store)
    return _sync_service


async def init_and_sync(full_sync_days: int = 730) -> None:
    """
    Initialize store and perform initial sync if needed.

    Called on application startup.
    """
    store = await get_store()
    # Step 13's settle of the warehouse writer stood here until step 14's
    # S14-15 (`core/warehouse_cutover.py`, deleted): under KS_DUCKDB=off it read
    # the record's frozen copy in Postgres, `app.sync_metadata`, and closed the
    # `warehouse` alert group once. The record stays there, untouched: an image
    # from before step 14 reads it.
    # The orders held where they are written — Postgres, chain 3's, whose
    # DuckDB copy never grows and on a fresh DuckDB would read as empty on
    # every boot, pulling 730 days from KeyCRM each time (`orders_held`).
    # Until step 14's S14-18 a process under KS_DUCKDB=on also counted
    # DuckDB's products for the log line below (`get_stats`); the store has
    # no DuckDB to count since.
    held = await store.orders_held()

    # If no orders, do a full sync
    if held == 0:
        logger.info("No orders held, performing initial full sync...")
        sync_service = await get_sync_service()
        await sync_service.full_sync(days_back=full_sync_days)
    else:
        logger.info(f"{held} orders held")
        # Do incremental sync
        sync_service = await get_sync_service()
        await sync_service.incremental_sync()

    # Ensure sku_inventory_status is populated (Layer 1)
    sku_count = await store.refresh_sku_inventory_status()
    if sku_count > 0:
        logger.info(f"Initialized sku_inventory_status: {sku_count} SKUs")

    # Warehouse layers (Silver/Gold) are refreshed inside incremental_sync()
    # or full_sync() when data changes. With DELETE+INSERT, tables persist across
    # restarts so existing data remains valid without a redundant second refresh.

    # Initialize Meilisearch for chat search
    try:
        if await init_meilisearch():
            sync_service = await get_sync_service()
            meili_stats = await sync_service.sync_to_meilisearch()
            logger.info(f"Meilisearch initialized: {meili_stats}")
        else:
            logger.warning("Meilisearch not available, chat search will be limited")
    except Exception as e:
        logger.warning(f"Meilisearch initialization failed: {e}")

    # Note: Background sync is now handled by APScheduler (core/scheduler.py)
    # The scheduler runs incremental_sync every 60 seconds, plus other jobs:
    # - full_sync_weekly: Sunday 2 AM
    # - inventory_snapshot: daily 1 AM
    # - manager_stats: daily 3 AM
    # - seasonality_calc: Monday 4 AM


async def force_resync(days_back: int = 730) -> dict:
    """
    Force a complete resync: re-fetch the window from KeyCRM and overwrite
    every order in it, in place.

    Use this when data discrepancies are detected between dashboard and KeyCRM.

    It used to DELETE `orders`, `order_products` and `expenses` first and refill
    them chunk by chunk. That made a 429 storm halfway through — the ordinary
    failure of a year-long walk over the API — leave the store missing months
    of facts, with nothing to restore them but the hourly gap crawl at 200 ids
    an hour, and left every DuckDB-fed page wrong for days. Rewriting in place
    with `force_update=True` reaches the same end state: each order is
    overwritten by the fetched payload, and an interrupted run can simply be
    run again. The one thing the DELETE bought — dropping orders KeyCRM has
    since deleted — is something the reconciliation deliberately refuses to do
    automatically. The purge endpoint that did it by id, in DuckDB alone, was
    retired by OD-10 (2026-09-30); removing an order from Postgres, which every
    tab reads, is chain 3's to design.

    Args:
        days_back: Number of days of historical data to sync

    Returns:
        Dict with sync statistics
    """
    logger.warning(f"Force resync requested - rewriting the last {days_back} days in place")

    sync_service = await get_sync_service()
    # Reset the filter detection flag
    sync_service._use_ordered_between = None
    stats = await sync_service.full_sync(days_back=days_back, force_update=True)

    logger.info(f"Force resync complete: {stats}")
    return stats
