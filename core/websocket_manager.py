"""
WebSocket connection management for real-time dashboard updates.

Provides room-based subscriptions, connection pooling, and broadcast functionality
for pushing sync events to connected clients.

Usage:
    from core.websocket_manager import manager

    # In WebSocket endpoint
    await manager.connect(websocket, room="dashboard")

    # Broadcast to all dashboard clients
    await manager.broadcast("dashboard", "orders_synced", {"count": 10})
"""
import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Set

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class WebSocketEvent(Enum):
    """Events that can be sent via WebSocket."""

    # Data sync events
    ORDERS_SYNCED = "orders_synced"
    PRODUCTS_SYNCED = "products_synced"
    INVENTORY_UPDATED = "inventory_updated"
    EXPENSES_UPDATED = "expenses_updated"

    # Goal events
    GOAL_PROGRESS = "goal_progress"
    MILESTONE_REACHED = "milestone_reached"

    # System events
    SYNC_STATUS = "sync_status"
    CONNECTED = "connected"
    PONG = "pong"


@dataclass
class ConnectionInfo:
    """Information about a WebSocket connection."""

    id: int
    websocket: WebSocket
    room: str
    connected_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    message_count: int = 0


class ConnectionManager:
    """
    Manages WebSocket connections with room-based subscriptions.

    Features:
    - Multiple rooms (dashboard, admin, etc.)
    - Thread-safe connection management
    - Broadcast to all connections in a room
    - Connection statistics
    - Automatic cleanup of dead connections
    """

    def __init__(self):
        # Room -> Dict of connection_id -> ConnectionInfo
        self._rooms: Dict[str, Dict[int, ConnectionInfo]] = {}
        self._lock = asyncio.Lock()
        self._total_connections = 0
        self._total_messages_sent = 0
        self._next_connection_id = 1

    async def connect(
        self, websocket: WebSocket, room: str = "dashboard"
    ) -> ConnectionInfo:
        """
        Accept a WebSocket connection and add it to a room.

        Args:
            websocket: The WebSocket connection to accept
            room: Room name to subscribe to (default: "dashboard")

        Returns:
            ConnectionInfo for the new connection
        """
        await websocket.accept()

        async with self._lock:
            conn_id = self._next_connection_id
            self._next_connection_id += 1

            conn_info = ConnectionInfo(id=conn_id, websocket=websocket, room=room)

            if room not in self._rooms:
                self._rooms[room] = {}
            self._rooms[room][conn_id] = conn_info
            self._total_connections += 1

        logger.info(
            f"WebSocket connected to room '{room}' "
            f"(total: {self.connection_count(room)} in room, "
            f"{self.total_connections} total)"
        )

        # Send welcome message with connection info
        await self._send_to_connection(
            conn_info,
            WebSocketEvent.CONNECTED,
            {
                "room": room,
                "timestamp": datetime.now().isoformat(),
            },
        )

        return conn_info

    async def disconnect(self, conn_info: ConnectionInfo) -> None:
        """
        Remove a connection from its room.

        Args:
            conn_info: The connection to remove
        """
        async with self._lock:
            room = conn_info.room
            if room in self._rooms:
                self._rooms[room].pop(conn_info.id, None)
                # Clean up empty rooms
                if not self._rooms[room]:
                    del self._rooms[room]

        logger.info(
            f"WebSocket disconnected from room '{conn_info.room}' "
            f"(remaining: {self.connection_count(conn_info.room)} in room)"
        )

    async def broadcast(
        self, room: str, event: WebSocketEvent | str, data: Dict[str, Any]
    ) -> int:
        """
        Broadcast a message to all connections in a room.

        Args:
            room: Room to broadcast to
            event: Event type (WebSocketEvent or string)
            data: Event payload

        Returns:
            Number of connections that received the message
        """
        if isinstance(event, WebSocketEvent):
            event_name = event.value
        else:
            event_name = event

        async with self._lock:
            connections = list(self._rooms.get(room, {}).values())

        if not connections:
            logger.debug(f"No connections in room '{room}' for broadcast")
            return 0

        # Send to all connections concurrently
        tasks = [
            self._send_to_connection(conn, event_name, data)
            for conn in connections
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successful sends and handle failures
        sent_count = 0
        failed_connections = []

        for conn, result in zip(connections, results):
            if isinstance(result, Exception):
                logger.warning(f"Failed to send to connection: {result}")
                failed_connections.append(conn)
            elif result is True:
                sent_count += 1
            else:
                # result is False - send failed
                failed_connections.append(conn)

        # Clean up failed connections
        for conn in failed_connections:
            await self.disconnect(conn)

        self._total_messages_sent += sent_count
        logger.debug(
            f"Broadcast '{event_name}' to {sent_count}/{len(connections)} "
            f"connections in room '{room}'"
        )

        return sent_count

    async def broadcast_all(
        self, event: WebSocketEvent | str, data: Dict[str, Any]
    ) -> int:
        """
        Broadcast a message to all connected clients across all rooms.

        Args:
            event: Event type
            data: Event payload

        Returns:
            Total number of connections that received the message
        """
        async with self._lock:
            rooms = list(self._rooms.keys())

        total_sent = 0
        for room in rooms:
            sent = await self.broadcast(room, event, data)
            total_sent += sent

        return total_sent

    async def _send_to_connection(
        self, conn: ConnectionInfo, event: str | WebSocketEvent, data: Dict[str, Any]
    ) -> bool:
        """
        Send a message to a specific connection.

        Args:
            conn: Connection to send to
            event: Event type
            data: Event payload

        Returns:
            True if successful, False otherwise
        """
        if isinstance(event, WebSocketEvent):
            event_name = event.value
        else:
            event_name = event

        message = json.dumps(
            {
                "event": event_name,
                "data": data,
                "timestamp": datetime.now().isoformat(),
            }
        )

        try:
            await conn.websocket.send_text(message)
            conn.last_activity = datetime.now()
            conn.message_count += 1
            return True
        except Exception as e:
            logger.debug(f"Failed to send message: {e}")
            return False

    async def handle_message(
        self, conn_info: ConnectionInfo, message: str
    ) -> Optional[str]:
        """
        Handle an incoming message from a client.

        Args:
            conn_info: Connection that sent the message
            message: Raw message string

        Returns:
            Response to send back, or None
        """
        conn_info.last_activity = datetime.now()

        # Handle ping/pong for keep-alive
        if message == "ping":
            await self._send_to_connection(
                conn_info, WebSocketEvent.PONG, {"timestamp": datetime.now().isoformat()}
            )
            return None

        # Try to parse as JSON
        try:
            data = json.loads(message)
            action = data.get("action")

            if action == "subscribe":
                # Future: handle room switching
                pass
            elif action == "ping":
                await self._send_to_connection(
                    conn_info, WebSocketEvent.PONG, {}
                )

        except json.JSONDecodeError:
            logger.debug(f"Received non-JSON message: {message[:100]}")

        return None

    def connection_count(self, room: Optional[str] = None) -> int:
        """
        Get the number of active connections.

        Args:
            room: Specific room to count, or None for all rooms

        Returns:
            Number of connections
        """
        if room:
            return len(self._rooms.get(room, {}))
        return sum(len(conns) for conns in self._rooms.values())

    @property
    def total_connections(self) -> int:
        """Total number of connections ever made."""
        return self._total_connections

    @property
    def total_messages_sent(self) -> int:
        """Total number of messages sent."""
        return self._total_messages_sent

    def get_stats(self) -> Dict[str, Any]:
        """
        Get connection statistics.

        Returns:
            Dict with connection stats
        """
        rooms_info = {}
        for room, connections in self._rooms.items():
            rooms_info[room] = {
                "count": len(connections),
                "oldest_connection": min(
                    (c.connected_at for c in connections.values()), default=None
                ),
            }

        return {
            "active_connections": self.connection_count(),
            "total_connections_ever": self._total_connections,
            "total_messages_sent": self._total_messages_sent,
            "rooms": rooms_info,
        }

    async def cleanup_stale_connections(self, max_idle_seconds: int = 300) -> int:
        """
        Remove connections that have been idle too long.

        Args:
            max_idle_seconds: Max seconds of inactivity before cleanup

        Returns:
            Number of connections removed
        """
        now = datetime.now()
        stale_connections = []

        async with self._lock:
            for room, connections in self._rooms.items():
                for conn in connections.values():
                    idle_seconds = (now - conn.last_activity).total_seconds()
                    if idle_seconds > max_idle_seconds:
                        stale_connections.append(conn)

        # Disconnect stale connections outside the lock
        for conn in stale_connections:
            try:
                await conn.websocket.close()
            except Exception:
                pass
            await self.disconnect(conn)

        if stale_connections:
            logger.info(f"Cleaned up {len(stale_connections)} stale connections")

        return len(stale_connections)


# Global manager instance
manager = ConnectionManager()
