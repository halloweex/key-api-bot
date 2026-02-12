"""
Integration tests for WebSocket functionality.

Tests:
- Connection management
- Room-based subscriptions
- Message broadcasting
- Event handling
- Reconnection behavior
"""
import asyncio
import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import WebSocket
from fastapi.testclient import TestClient

# Import the modules under test
from core.websocket_manager import ConnectionManager, ConnectionInfo, WebSocketEvent


class TestConnectionManager:
    """Tests for WebSocket ConnectionManager."""

    @pytest.fixture
    def manager(self):
        """Fresh ConnectionManager instance for each test."""
        return ConnectionManager()

    @pytest.fixture
    def mock_websocket(self):
        """Create a mock WebSocket."""
        ws = AsyncMock(spec=WebSocket)
        ws.accept = AsyncMock()
        ws.send_text = AsyncMock()
        ws.close = AsyncMock()
        return ws

    @pytest.mark.asyncio
    async def test_connect_accepts_websocket(self, manager, mock_websocket):
        """Test that connect() accepts the WebSocket and adds to room."""
        conn_info = await manager.connect(mock_websocket, room="dashboard")

        mock_websocket.accept.assert_called_once()
        assert conn_info.room == "dashboard"
        assert manager.connection_count("dashboard") == 1

    @pytest.mark.asyncio
    async def test_connect_sends_welcome_message(self, manager, mock_websocket):
        """Test that connect() sends a welcome message."""
        await manager.connect(mock_websocket, room="dashboard")

        # Should have sent a "connected" event
        mock_websocket.send_text.assert_called()
        sent_message = mock_websocket.send_text.call_args[0][0]
        parsed = json.loads(sent_message)
        assert parsed["event"] == "connected"
        assert parsed["data"]["room"] == "dashboard"

    @pytest.mark.asyncio
    async def test_disconnect_removes_from_room(self, manager, mock_websocket):
        """Test that disconnect() removes connection from room."""
        conn_info = await manager.connect(mock_websocket, room="dashboard")
        assert manager.connection_count("dashboard") == 1

        await manager.disconnect(conn_info)
        assert manager.connection_count("dashboard") == 0

    @pytest.mark.asyncio
    async def test_disconnect_cleans_up_empty_rooms(self, manager, mock_websocket):
        """Test that empty rooms are cleaned up after disconnect."""
        conn_info = await manager.connect(mock_websocket, room="test_room")
        assert "test_room" in manager._rooms

        await manager.disconnect(conn_info)
        assert "test_room" not in manager._rooms

    @pytest.mark.asyncio
    async def test_broadcast_sends_to_all_in_room(self, manager):
        """Test that broadcast() sends to all connections in a room."""
        ws1 = AsyncMock(spec=WebSocket)
        ws1.accept = AsyncMock()
        ws1.send_text = AsyncMock()

        ws2 = AsyncMock(spec=WebSocket)
        ws2.accept = AsyncMock()
        ws2.send_text = AsyncMock()

        await manager.connect(ws1, room="dashboard")
        await manager.connect(ws2, room="dashboard")

        # Clear welcome message calls
        ws1.send_text.reset_mock()
        ws2.send_text.reset_mock()

        sent_count = await manager.broadcast(
            "dashboard",
            WebSocketEvent.ORDERS_SYNCED,
            {"count": 10}
        )

        assert sent_count == 2
        ws1.send_text.assert_called_once()
        ws2.send_text.assert_called_once()

        # Verify message content
        sent1 = json.loads(ws1.send_text.call_args[0][0])
        assert sent1["event"] == "orders_synced"
        assert sent1["data"]["count"] == 10

    @pytest.mark.asyncio
    async def test_broadcast_only_affects_target_room(self, manager):
        """Test that broadcast() only sends to the specified room."""
        ws_dashboard = AsyncMock(spec=WebSocket)
        ws_dashboard.accept = AsyncMock()
        ws_dashboard.send_text = AsyncMock()

        ws_admin = AsyncMock(spec=WebSocket)
        ws_admin.accept = AsyncMock()
        ws_admin.send_text = AsyncMock()

        await manager.connect(ws_dashboard, room="dashboard")
        await manager.connect(ws_admin, room="admin")

        # Clear welcome messages
        ws_dashboard.send_text.reset_mock()
        ws_admin.send_text.reset_mock()

        await manager.broadcast("dashboard", "test_event", {"data": "value"})

        ws_dashboard.send_text.assert_called_once()
        ws_admin.send_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_broadcast_handles_failed_connections(self, manager):
        """Test that broadcast() removes failed connections."""
        ws_good = AsyncMock(spec=WebSocket)
        ws_good.accept = AsyncMock()
        ws_good.send_text = AsyncMock()

        ws_bad = AsyncMock(spec=WebSocket)
        ws_bad.accept = AsyncMock()
        ws_bad.send_text = AsyncMock(side_effect=Exception("Connection closed"))

        await manager.connect(ws_good, room="dashboard")
        await manager.connect(ws_bad, room="dashboard")

        # Clear welcome messages
        ws_good.send_text.reset_mock()

        assert manager.connection_count("dashboard") == 2

        sent_count = await manager.broadcast("dashboard", "test", {})

        # Only one should have received it
        assert sent_count == 1
        # Failed connection should be removed
        assert manager.connection_count("dashboard") == 1

    @pytest.mark.asyncio
    async def test_broadcast_all_sends_to_all_rooms(self, manager):
        """Test that broadcast_all() sends to all rooms."""
        ws1 = AsyncMock(spec=WebSocket)
        ws1.accept = AsyncMock()
        ws1.send_text = AsyncMock()

        ws2 = AsyncMock(spec=WebSocket)
        ws2.accept = AsyncMock()
        ws2.send_text = AsyncMock()

        await manager.connect(ws1, room="dashboard")
        await manager.connect(ws2, room="admin")

        ws1.send_text.reset_mock()
        ws2.send_text.reset_mock()

        total_sent = await manager.broadcast_all("global_event", {"global": True})

        assert total_sent == 2
        ws1.send_text.assert_called_once()
        ws2.send_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_ping_message(self, manager, mock_websocket):
        """Test that ping messages are handled with pong response."""
        conn_info = await manager.connect(mock_websocket, room="dashboard")
        mock_websocket.send_text.reset_mock()

        await manager.handle_message(conn_info, "ping")

        # Should have sent a pong response
        mock_websocket.send_text.assert_called()
        sent = json.loads(mock_websocket.send_text.call_args[0][0])
        assert sent["event"] == "pong"

    @pytest.mark.asyncio
    async def test_handle_json_message(self, manager, mock_websocket):
        """Test that JSON messages are parsed correctly."""
        conn_info = await manager.connect(mock_websocket, room="dashboard")
        mock_websocket.send_text.reset_mock()

        # Send a JSON ping
        await manager.handle_message(conn_info, '{"action": "ping"}')

        mock_websocket.send_text.assert_called()

    @pytest.mark.asyncio
    async def test_connection_count(self, manager):
        """Test connection counting."""
        ws1 = AsyncMock(spec=WebSocket)
        ws1.accept = AsyncMock()
        ws1.send_text = AsyncMock()

        ws2 = AsyncMock(spec=WebSocket)
        ws2.accept = AsyncMock()
        ws2.send_text = AsyncMock()

        ws3 = AsyncMock(spec=WebSocket)
        ws3.accept = AsyncMock()
        ws3.send_text = AsyncMock()

        await manager.connect(ws1, room="room1")
        await manager.connect(ws2, room="room1")
        await manager.connect(ws3, room="room2")

        assert manager.connection_count("room1") == 2
        assert manager.connection_count("room2") == 1
        assert manager.connection_count() == 3  # Total

    @pytest.mark.asyncio
    async def test_get_stats(self, manager, mock_websocket):
        """Test statistics reporting."""
        await manager.connect(mock_websocket, room="dashboard")

        stats = manager.get_stats()

        assert stats["active_connections"] == 1
        assert stats["total_connections_ever"] == 1
        assert "dashboard" in stats["rooms"]
        assert stats["rooms"]["dashboard"]["count"] == 1

    @pytest.mark.asyncio
    async def test_cleanup_stale_connections(self, manager):
        """Test cleanup of stale connections."""
        ws = AsyncMock(spec=WebSocket)
        ws.accept = AsyncMock()
        ws.send_text = AsyncMock()
        ws.close = AsyncMock()

        conn_info = await manager.connect(ws, room="dashboard")

        # Artificially age the connection
        conn_info.last_activity = datetime(2020, 1, 1)

        removed = await manager.cleanup_stale_connections(max_idle_seconds=60)

        assert removed == 1
        assert manager.connection_count("dashboard") == 0


class TestWebSocketEvents:
    """Tests for WebSocket event types."""

    def test_event_values(self):
        """Test that event enum values are correct."""
        assert WebSocketEvent.ORDERS_SYNCED.value == "orders_synced"
        assert WebSocketEvent.PRODUCTS_SYNCED.value == "products_synced"
        assert WebSocketEvent.MILESTONE_REACHED.value == "milestone_reached"
        assert WebSocketEvent.CONNECTED.value == "connected"
        assert WebSocketEvent.PONG.value == "pong"


class TestConnectionInfo:
    """Tests for ConnectionInfo dataclass."""

    def test_connection_info_defaults(self):
        """Test ConnectionInfo default values."""
        ws = MagicMock(spec=WebSocket)
        info = ConnectionInfo(id=1, websocket=ws, room="test")

        assert info.id == 1
        assert info.websocket == ws
        assert info.room == "test"
        assert info.message_count == 0
        assert info.connected_at is not None
        assert info.last_activity is not None


# Skip endpoint tests if httpx is not available
try:
    import httpx

    class TestWebSocketEndpoints:
        """Integration tests for WebSocket endpoints."""

        @pytest.fixture
        def client(self):
            """Create test client for the FastAPI app."""
            from web.main import app
            return TestClient(app)

        def test_ws_stats_endpoint(self, client):
            """Test the /ws/stats endpoint returns statistics."""
            response = client.get("/ws/stats")
            assert response.status_code == 200

            data = response.json()
            assert "active_connections" in data
            assert "total_connections_ever" in data
            assert "rooms" in data

except ImportError:
    pass
