"""
WebSocket routes for real-time dashboard updates.

Provides endpoints for:
- /ws/dashboard - Real-time dashboard updates (orders synced, goal progress)
- /ws/admin - Admin-only notifications (sync status, errors)

Both endpoints authenticate using the signed ``dashboard_session`` cookie sent
on the WebSocket handshake, which is what keeps anonymous callers out of live
business data and the admin event stream.

**The cookie is not what stops Cross-Site WebSocket Hijacking — it is what
makes it possible.** WebSockets are exempt from CORS, so any page on any origin
may open ``wss://<dashboard>/ws/admin`` and the browser attaches whatever
cookies its own policy allows; authenticating from a cookie alone is the
confused-deputy shape exactly. ``samesite="lax"`` does hold the line in current
browsers, but it is set for the Telegram login redirect rather than for this,
and it lives in ``web/routes/auth.py`` where nothing connects it to a socket.
So the handshake checks the ``Origin`` itself — see ``origin_allowed``.
"""
import logging
import os
from urllib.parse import urlsplit

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends

from core.websocket_manager import manager
from web.routes.auth import get_current_user_ws, require_user

router = APIRouter(tags=["websocket"])
logger = logging.getLogger(__name__)


def origin_allowed(websocket: WebSocket) -> bool:
    """May a page on this ``Origin`` open a socket to us?

    Hostnames are compared, not origin strings. Vite's dev proxy sets
    ``changeOrigin: true``, which rewrites ``Host`` to the backend
    (``localhost:8080``) while forwarding the browser's
    ``Origin: http://localhost:5173`` untouched — a port- or scheme-exact
    comparison would refuse every local development handshake. nginx passes
    both headers through unchanged, so production compares equal hostnames.

    A handshake carrying no ``Origin`` at all is allowed: browsers always send
    one here, so its absence means the caller is not a browser and therefore
    not the deputy this check exists to protect. The literal ``"null"`` an
    opaque origin sends parses to no hostname and is refused.
    """
    origin = websocket.headers.get("origin")
    if origin is None:
        return True

    origin_host = urlsplit(origin).hostname
    if not origin_host:
        return False

    allowed = set()
    host_header = websocket.headers.get("host")
    if host_header:
        # `//` so urlsplit reads a bare `example.com:443` as a network location.
        own_host = urlsplit(f"//{host_header}").hostname
        if own_host:
            allowed.add(own_host.lower())
    configured = urlsplit(os.getenv("DASHBOARD_URL", "")).hostname
    if configured:
        allowed.add(configured.lower())

    return origin_host.lower() in allowed


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
    # Ahead of authentication, so a foreign page cannot learn from the close
    # code whether the visitor is logged in.
    if not origin_allowed(websocket):
        await websocket.close(code=4002, reason="Origin not allowed")
        return

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
    if not origin_allowed(websocket):
        await websocket.close(code=4002, reason="Origin not allowed")
        return

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
