"""
Integration tests for core/events.py

Tests the event-driven publish/subscribe system.
"""
import asyncio
import pytest
from typing import Dict, Any, List

from core.events import (
    EventBus,
    SyncEvent,
    Event,
    EventMetadata,
    events,
    emit_sync_started,
    emit_sync_completed,
    emit_sync_failed,
    emit_orders_synced,
)


class TestEventBus:
    """Tests for EventBus class."""

    def setup_method(self):
        """Create fresh event bus for each test."""
        self.bus = EventBus()

    @pytest.mark.asyncio
    async def test_emit_with_no_handlers(self):
        """Emitting event with no handlers succeeds silently."""
        event = await self.bus.emit(SyncEvent.SYNC_STARTED, {"sync_type": "test"})
        assert event.type == SyncEvent.SYNC_STARTED
        assert event.data["sync_type"] == "test"

    @pytest.mark.asyncio
    async def test_subscribe_and_receive(self):
        """Subscribed handler receives events."""
        received: List[Dict[str, Any]] = []

        @self.bus.on(SyncEvent.ORDERS_SYNCED)
        async def handler(data: dict):
            received.append(data)

        await self.bus.emit(SyncEvent.ORDERS_SYNCED, {"count": 10})

        assert len(received) == 1
        assert received[0]["count"] == 10

    @pytest.mark.asyncio
    async def test_multiple_handlers(self):
        """Multiple handlers all receive the event."""
        results = []

        @self.bus.on(SyncEvent.SYNC_COMPLETED)
        async def handler1(data: dict):
            results.append("handler1")

        @self.bus.on(SyncEvent.SYNC_COMPLETED)
        async def handler2(data: dict):
            results.append("handler2")

        await self.bus.emit(SyncEvent.SYNC_COMPLETED, {})

        assert len(results) == 2
        assert "handler1" in results
        assert "handler2" in results

    @pytest.mark.asyncio
    async def test_wildcard_handler(self):
        """Wildcard handler receives all events."""
        received = []

        @self.bus.on()  # No event type = wildcard
        async def wildcard_handler(data: dict):
            received.append(data)

        await self.bus.emit(SyncEvent.SYNC_STARTED, {"type": "start"})
        await self.bus.emit(SyncEvent.SYNC_COMPLETED, {"type": "complete"})

        assert len(received) == 2

    @pytest.mark.asyncio
    async def test_handler_isolation(self):
        """Failing handler doesn't affect other handlers."""
        results = []

        @self.bus.on(SyncEvent.ORDERS_SYNCED)
        async def failing_handler(data: dict):
            raise ValueError("Handler error")

        @self.bus.on(SyncEvent.ORDERS_SYNCED)
        async def working_handler(data: dict):
            results.append("success")

        # Should not raise, even though first handler fails
        await self.bus.emit(SyncEvent.ORDERS_SYNCED, {})

        assert len(results) == 1
        assert results[0] == "success"

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        """Can unsubscribe handlers."""
        received = []

        async def handler(data: dict):
            received.append(data)

        self.bus.subscribe(SyncEvent.SYNC_STARTED, handler)
        await self.bus.emit(SyncEvent.SYNC_STARTED, {"n": 1})
        assert len(received) == 1

        # Unsubscribe
        result = self.bus.unsubscribe(SyncEvent.SYNC_STARTED, handler)
        assert result is True

        await self.bus.emit(SyncEvent.SYNC_STARTED, {"n": 2})
        assert len(received) == 1  # No new events

    def test_get_handlers(self):
        """get_handlers returns handler counts."""

        @self.bus.on(SyncEvent.ORDERS_SYNCED)
        async def h1(data):
            pass

        @self.bus.on(SyncEvent.ORDERS_SYNCED)
        async def h2(data):
            pass

        @self.bus.on(SyncEvent.PRODUCTS_SYNCED)
        async def h3(data):
            pass

        handlers = self.bus.get_handlers()
        assert handlers[SyncEvent.ORDERS_SYNCED.value] == 2
        assert handlers[SyncEvent.PRODUCTS_SYNCED.value] == 1

    @pytest.mark.asyncio
    async def test_get_history(self):
        """get_history returns recent events."""
        await self.bus.emit(SyncEvent.SYNC_STARTED, {"n": 1})
        await self.bus.emit(SyncEvent.SYNC_COMPLETED, {"n": 2})
        await self.bus.emit(SyncEvent.SYNC_STARTED, {"n": 3})

        history = self.bus.get_history(limit=10)
        assert len(history) == 3

        # Filter by type
        starts = self.bus.get_history(event_type=SyncEvent.SYNC_STARTED)
        assert len(starts) == 2

    @pytest.mark.asyncio
    async def test_history_limit(self):
        """History respects max_history limit."""
        bus = EventBus(max_history=5)

        for i in range(10):
            await bus.emit(SyncEvent.SYNC_STARTED, {"n": i})

        history = bus.get_history()
        assert len(history) == 5
        # Should have the most recent events
        assert history[-1]["data"]["n"] == 9

    def test_clear_handlers(self):
        """clear_handlers removes all handlers."""

        @self.bus.on(SyncEvent.ORDERS_SYNCED)
        async def handler(data):
            pass

        self.bus.clear_handlers()
        handlers = self.bus.get_handlers()
        assert all(count == 0 for count in handlers.values())


class TestEvent:
    """Tests for Event class."""

    def test_event_creation(self):
        """Event is created with correct type and data."""
        event = Event(
            type=SyncEvent.ORDERS_SYNCED,
            data={"count": 100},
        )
        assert event.type == SyncEvent.ORDERS_SYNCED
        assert event.data["count"] == 100

    def test_event_to_dict(self):
        """Event serializes to dictionary."""
        event = Event(
            type=SyncEvent.SYNC_COMPLETED,
            data={"duration_ms": 1234},
        )
        d = event.to_dict()

        assert d["event_type"] == "sync.completed"
        assert d["data"]["duration_ms"] == 1234
        assert "metadata" in d
        assert "timestamp" in d["metadata"]

    def test_metadata_has_timestamp(self):
        """EventMetadata includes timestamp."""
        metadata = EventMetadata()
        assert metadata.timestamp is not None
        assert metadata.event_id is not None


class TestConvenienceFunctions:
    """Tests for convenience emit functions."""

    def setup_method(self):
        """Clear global event bus before each test."""
        events.clear_handlers()
        events.clear_history()

    @pytest.mark.asyncio
    async def test_emit_sync_started(self):
        """emit_sync_started creates correct event."""
        received = []

        @events.on(SyncEvent.SYNC_STARTED)
        async def handler(data):
            received.append(data)

        await emit_sync_started("incremental", extra_field="value")

        assert len(received) == 1
        assert received[0]["sync_type"] == "incremental"
        assert received[0]["extra_field"] == "value"

    @pytest.mark.asyncio
    async def test_emit_sync_completed(self):
        """emit_sync_completed creates correct event."""
        received = []

        @events.on(SyncEvent.SYNC_COMPLETED)
        async def handler(data):
            received.append(data)

        await emit_sync_completed("full", 5000.0, 100)

        assert len(received) == 1
        assert received[0]["sync_type"] == "full"
        assert received[0]["duration_ms"] == 5000.0
        assert received[0]["records_synced"] == 100

    @pytest.mark.asyncio
    async def test_emit_sync_failed(self):
        """emit_sync_failed creates correct event."""
        received = []

        @events.on(SyncEvent.SYNC_FAILED)
        async def handler(data):
            received.append(data)

        await emit_sync_failed("incremental", "Connection timeout")

        assert len(received) == 1
        assert received[0]["sync_type"] == "incremental"
        assert received[0]["error"] == "Connection timeout"

    @pytest.mark.asyncio
    async def test_emit_orders_synced(self):
        """emit_orders_synced creates correct event."""
        received = []

        @events.on(SyncEvent.ORDERS_SYNCED)
        async def handler(data):
            received.append(data)

        await emit_orders_synced(50, 1234.5)

        assert len(received) == 1
        assert received[0]["count"] == 50
        assert received[0]["duration_ms"] == 1234.5
