"""Shared SlowAPI rate limiter — single instance for the whole app.

Previously each route module created its own ``Limiter()``. SlowAPI enforces
``@limiter.limit`` decorators against ``app.state.limiter``; with mismatched
instances the per-route limits were never actually applied. This module is the
single source of truth — import ``limiter`` from here everywhere.
"""
from slowapi import Limiter
from starlette.requests import Request


def client_key(request: Request) -> str:
    """Rate-limit key that is correct behind the nginx reverse proxy.

    nginx sets ``X-Real-IP`` to ``$remote_addr`` (it overwrites, not appends),
    so it reflects the true client IP and cannot be spoofed by external clients.
    Without this, ``get_remote_address`` returns the nginx container IP and every
    user shares a single bucket. Falls back to the socket peer for direct hits.
    """
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


limiter = Limiter(key_func=client_key)
