"""
WebSocket routes for real-time dashboard updates.

Provides endpoints for:
- /ws/dashboard - Real-time dashboard updates (orders synced, goal progress)
- /ws/admin - Admin-only notifications (sync status, errors)

Both endpoints authenticate using the signed ``dashboard_session`` cookie sent
on the WebSocket handshake (same-origin requests include it automatically).
This prevents Cross-Site WebSocket Hijacking and anonymous access to live
business data / admin event streams.
"""
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends

from core.websocket_manager import manager, WebSocketEvent
from web.routes.auth import get_current_user_ws, require_user

router = APIRouter(tags=["websocket"])
logger = logging.getLogger(__name__)


@router.websocket("/ws/dashboard")
async def dashboard_websocket(websocket: WebSocket):
    """
    WebSocket endpoint for real-time dashboard updates.

    Requires a valid dashboard session (cookie sent on the handshake).

    Receives events:
    - orders_synced: New orders have been synced
    - products_synced: Products catalog updated
    - goal_progress: Progress toward revenue goals
    - milestone_reached: A goal milestone was achieved
    - sync_status: Sync service status change

    Client can send:
    - "ping" for keep-alive (responds with "pong")
    - JSON: {"action": "subscribe", "room": "dashboard"}
    """
    user = await get_current_user_ws(websocket)
    if not user:
        await websocket.close(code=4001, reason="Authentication required")
        return

    conn_info = await manager.connect(websocket, room="dashboard")

    try:
        while True:
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
async def admin_websocket(websocket: WebSocket):
    """
    WebSocket endpoint for admin-only notifications.

    Requires a valid dashboard session with the ``admin`` role. Receives all
    dashboard events plus detailed sync status, error and health notifications.
    """
    user = await get_current_user_ws(websocket)
    if not user:
        await websocket.close(code=4001, reason="Authentication required")
        return
    if user.get("role") != "admin":
        await websocket.close(code=4003, reason="Admin access required")
        return

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


@router.get("/ws/stats", dependencies=[Depends(require_user)])
async def get_websocket_stats():
    """
    Get WebSocket connection statistics. Requires an authenticated session.

    Returns active connections, room counts, and message stats.
    """
    return manager.get_stats()
