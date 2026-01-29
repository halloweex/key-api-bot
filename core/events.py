"""
Event-driven architecture for sync operations.

Provides a simple publish/subscribe pattern for decoupling sync operations
from their consumers (cache invalidation, notifications, analytics, etc.).

Usage:
    from core.events import events, SyncEvent

    # Subscribe to events
    @events.on(SyncEvent.ORDERS_SYNCED)
    async def handle_orders_synced(data: dict):
        print(f"Synced {data['count']} orders")

    # Publish events
    await events.emit(SyncEvent.ORDERS_SYNCED, {"count": 100, "duration_ms": 1234})
"""
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

from core.observability import get_logger, get_correlation_id

logger = get_logger(__name__)

# Type for event handlers
EventHandler = Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]


class SyncEvent(Enum):
    """Events emitted during sync operations."""

    # Sync lifecycle
    SYNC_STARTED = "sync.started"
    SYNC_COMPLETED = "sync.completed"
    SYNC_FAILED = "sync.failed"

    # Entity-specific sync events
    ORDERS_SYNCED = "orders.synced"
    PRODUCTS_SYNCED = "products.synced"
    CATEGORIES_SYNCED = "categories.synced"
    MANAGERS_SYNCED = "managers.synced"
    EXPENSES_SYNCED = "expenses.synced"

    # Inventory events
    INVENTORY_SNAPSHOT_TAKEN = "inventory.snapshot_taken"
    INVENTORY_UPDATED = "inventory.updated"

    # Analytics events
    DAILY_STATS_UPDATED = "analytics.daily_stats_updated"
    SEASONALITY_CALCULATED = "analytics.seasonality_calculated"
    GOALS_UPDATED = "analytics.goals_updated"

    # Cache events
    CACHE_INVALIDATED = "cache.invalidated"
    CACHE_WARMED = "cache.warmed"

    # Scheduler events
    JOB_STARTED = "scheduler.job_started"
    JOB_COMPLETED = "scheduler.job_completed"
    JOB_FAILED = "scheduler.job_failed"


@dataclass
class EventMetadata:
    """Metadata attached to every event."""

    event_id: str = field(default_factory=lambda: f"{datetime.now().timestamp():.6f}")
    timestamp: datetime = field(default_factory=datetime.now)
    correlation_id: Optional[str] = field(default_factory=get_correlation_id)
    source: str = "sync_service"


@dataclass
class Event:
    """Wrapper for event data with metadata."""

    type: SyncEvent
    data: Dict[str, Any]
    metadata: EventMetadata = field(default_factory=EventMetadata)

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for logging/serialization."""
        return {
            "event_type": self.type.value,
            "data": self.data,
            "metadata": {
                "event_id": self.metadata.event_id,
                "timestamp": self.metadata.timestamp.isoformat(),
                "correlation_id": self.metadata.correlation_id,
                "source": self.metadata.source,
            },
        }


class EventBus:
    """
    Simple async event bus for publish/subscribe pattern.

    Features:
    - Async event handlers
    - Multiple handlers per event
    - Wildcard subscriptions (subscribe to all events)
    - Error isolation (one handler failure doesn't affect others)
    - Event history for debugging
    """

    def __init__(self, max_history: int = 100):
        self._handlers: Dict[SyncEvent, List[EventHandler]] = {}
        self._wildcard_handlers: List[EventHandler] = []
        self._history: List[Event] = []
        self._max_history = max_history
        self._lock = asyncio.Lock()

    def on(
        self, event_type: Union[SyncEvent, str, None] = None
    ) -> Callable[[EventHandler], EventHandler]:
        """
        Decorator to register an event handler.

        Args:
            event_type: Event type to subscribe to, or None for all events

        Usage:
            @events.on(SyncEvent.ORDERS_SYNCED)
            async def handle_orders(data: dict):
                ...

            @events.on()  # Subscribe to all events
            async def handle_all(data: dict):
                ...
        """

        def decorator(handler: EventHandler) -> EventHandler:
            if event_type is None:
                self._wildcard_handlers.append(handler)
                logger.debug(f"Registered wildcard handler: {handler.__name__}")
            else:
                if event_type not in self._handlers:
                    self._handlers[event_type] = []
                self._handlers[event_type].append(handler)
                logger.debug(
                    f"Registered handler {handler.__name__} for {event_type.value}"
                )
            return handler

        return decorator

    def subscribe(
        self, event_type: Optional[SyncEvent], handler: EventHandler
    ) -> None:
        """
        Programmatically subscribe to an event.

        Args:
            event_type: Event type to subscribe to, or None for all events
            handler: Async function to handle the event
        """
        if event_type is None:
            self._wildcard_handlers.append(handler)
        else:
            if event_type not in self._handlers:
                self._handlers[event_type] = []
            self._handlers[event_type].append(handler)

    def unsubscribe(
        self, event_type: Optional[SyncEvent], handler: EventHandler
    ) -> bool:
        """
        Unsubscribe a handler from an event.

        Returns:
            True if handler was found and removed
        """
        if event_type is None:
            if handler in self._wildcard_handlers:
                self._wildcard_handlers.remove(handler)
                return True
        else:
            if event_type in self._handlers and handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)
                return True
        return False

    async def emit(
        self,
        event_type: SyncEvent,
        data: Optional[Dict[str, Any]] = None,
        source: str = "sync_service",
    ) -> Event:
        """
        Emit an event to all subscribed handlers.

        Args:
            event_type: Type of event to emit
            data: Event payload
            source: Source of the event

        Returns:
            The emitted Event object
        """
        event = Event(
            type=event_type,
            data=data or {},
            metadata=EventMetadata(source=source),
        )

        # Store in history
        async with self._lock:
            self._history.append(event)
            if len(self._history) > self._max_history:
                self._history = self._history[-self._max_history :]

        # Get all handlers for this event
        handlers = list(self._handlers.get(event_type, []))
        handlers.extend(self._wildcard_handlers)

        if not handlers:
            logger.debug(f"No handlers for event {event_type.value}")
            return event

        logger.debug(
            f"Emitting {event_type.value} to {len(handlers)} handlers",
            extra={"event": event.to_dict()},
        )

        # Execute handlers concurrently with error isolation
        results = await asyncio.gather(
            *[self._safe_call(handler, event) for handler in handlers],
            return_exceptions=True,
        )

        # Log any errors
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                handler_name = handlers[i].__name__
                logger.error(
                    f"Handler {handler_name} failed for {event_type.value}: {result}",
                    extra={"event": event.to_dict()},
                )

        return event

    async def _safe_call(self, handler: EventHandler, event: Event) -> None:
        """Safely call a handler with error isolation."""
        try:
            await handler(event.data)
        except Exception as e:
            # Re-raise to be caught by gather
            raise

    def get_history(
        self, event_type: Optional[SyncEvent] = None, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get recent event history.

        Args:
            event_type: Filter by event type (None for all)
            limit: Maximum events to return

        Returns:
            List of event dictionaries
        """
        events = self._history
        if event_type:
            events = [e for e in events if e.type == event_type]
        return [e.to_dict() for e in events[-limit:]]

    def get_handlers(self, event_type: Optional[SyncEvent] = None) -> Dict[str, int]:
        """
        Get count of registered handlers.

        Returns:
            Dict mapping event type to handler count
        """
        if event_type:
            count = len(self._handlers.get(event_type, []))
            return {event_type.value: count}

        result = {et.value: len(handlers) for et, handlers in self._handlers.items()}
        result["*"] = len(self._wildcard_handlers)
        return result

    def clear_handlers(self) -> None:
        """Remove all handlers (useful for testing)."""
        self._handlers.clear()
        self._wildcard_handlers.clear()

    def clear_history(self) -> None:
        """Clear event history."""
        self._history.clear()


# Global event bus instance
events = EventBus()


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


async def emit_sync_started(sync_type: str, **kwargs) -> Event:
    """Emit sync started event."""
    return await events.emit(
        SyncEvent.SYNC_STARTED,
        {"sync_type": sync_type, **kwargs},
    )


async def emit_sync_completed(
    sync_type: str, duration_ms: float, records_synced: int, **kwargs
) -> Event:
    """Emit sync completed event."""
    return await events.emit(
        SyncEvent.SYNC_COMPLETED,
        {
            "sync_type": sync_type,
            "duration_ms": duration_ms,
            "records_synced": records_synced,
            **kwargs,
        },
    )


async def emit_sync_failed(sync_type: str, error: str, **kwargs) -> Event:
    """Emit sync failed event."""
    return await events.emit(
        SyncEvent.SYNC_FAILED,
        {"sync_type": sync_type, "error": error, **kwargs},
    )


async def emit_orders_synced(count: int, duration_ms: float, **kwargs) -> Event:
    """Emit orders synced event."""
    return await events.emit(
        SyncEvent.ORDERS_SYNCED,
        {"count": count, "duration_ms": duration_ms, **kwargs},
    )


async def emit_cache_invalidated(keys: List[str], reason: str, **kwargs) -> Event:
    """Emit cache invalidated event."""
    return await events.emit(
        SyncEvent.CACHE_INVALIDATED,
        {"keys": keys, "reason": reason, **kwargs},
    )
