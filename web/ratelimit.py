"""Shared SlowAPI limiter — one instance app-wide.

SlowAPI enforces ``@limiter.limit`` against ``app.state.limiter``; if each
route module instantiates its own ``Limiter()`` the decorators silently
no-op. Import ``limiter`` from this module everywhere.
"""
from slowapi import Limiter
from starlette.requests import Request


def client_key(request: Request) -> str:
    """Rate-limit key. Trusts ``X-Real-IP`` because this app's nginx
    overwrites it with the true client IP — safe ONLY as long as the web
    container is unreachable except via that nginx (docker-compose uses
    ``expose:`` not ``ports:``). If that ever changes, this becomes
    spoofable: bind web to loopback or validate the upstream first.
    """
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    return request.client.host if request.client else "unknown"


limiter = Limiter(key_func=client_key)
