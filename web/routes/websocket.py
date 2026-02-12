"""
WebSocket routes for real-time dashboard updates.

Provides endpoints for:
- /ws/dashboard - Real-time dashboard updates (orders synced, goal progress)
- /ws/admin - Admin-only notifications (sync status, errors)
"""
import logging
from datetime import datetime

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, HTTPException

from core.websocket_manager import manager, WebSocketEvent

router = APIRouter(tags=["websocket"])
logger = logging.getLogger(__name__)


@router.websocket("/ws/dashboard")
async def dashboard_websocket(
    websocket: WebSocket,
    token: str = Query(default=None, description="Optional auth token"),
):
    """
    WebSocket endpoint for real-time dashboard updates.

    Receives events:
    - orders_synced: New orders have been synced
    - products_synced: Products catalog updated
    - goal_progress: Progress toward revenue goals
    - milestone_reached: A goal milestone was achieved
    - sync_status: Sync service status change

    Client can send:
    - "ping" for keep-alive (responds with "pong")
    - JSON: {"action": "subscribe", "room": "dashboard"}

    Connection will be closed after 5 minutes of inactivity.
    """
    # Optional: Validate token for authenticated connections
    # For now, allow anonymous connections to dashboard room

    conn_info = await manager.connect(websocket, room="dashboard")

    try:
        while True:
            # Wait for messages from client
            try:
                message = await websocket.receive_text()
                await manager.handle_message(conn_info, message)
            except WebSocketDisconnect:
                logger.debug("Client disconnected normally")
                break

    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await manager.disconnect(conn_info)


@router.websocket("/ws/admin")
async def admin_websocket(
    websocket: WebSocket,
    token: str = Query(..., description="Admin auth token required"),
):
    """
    WebSocket endpoint for admin-only notifications.

    Receives all dashboard events plus:
    - Detailed sync status
    - Error notifications
    - System health updates

    Requires authentication token.
    """
    # TODO: Validate admin token
    # For now, reject connections without token
    if not token:
        await websocket.close(code=4001, reason="Authentication required")
        return

    # Simple token validation (placeholder - should use proper auth)
    # In production, validate against session or JWT
    import os
    admin_ids = os.getenv("ADMIN_USER_IDS", "").split(",")
    # For WebSocket, we'd need a different auth mechanism
    # This is a placeholder - implement proper JWT/session validation

    conn_info = await manager.connect(websocket, room="admin")

    try:
        while True:
            try:
                message = await websocket.receive_text()
                await manager.handle_message(conn_info, message)
            except WebSocketDisconnect:
                break

    except Exception as e:
        logger.error(f"Admin WebSocket error: {e}")
    finally:
        await manager.disconnect(conn_info)


@router.get("/ws/stats")
async def get_websocket_stats():
    """
    Get WebSocket connection statistics.

    Returns active connections, room counts, and message stats.
    """
    return manager.get_stats()
