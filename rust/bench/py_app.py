"""The Python side of the pilot's benchmark: production's app, nearly as is.

`bench.sh` serves this with uvicorn exactly as `Dockerfile.web` serves
`web.main:app` — one process, uvloop and httptools, every middleware, orjson —
and changes the two things a load test cannot pass through and the Rust pilot
does not have either:

- the rate limit (`@limiter.limit("30/minute")` on the route), off;
- the session, answered as a hardcoded admin's without a cookie. A real
  admin's cookie costs one itsdangerous signature check; anyone else's adds
  a read of their user row, which neither side pays here.

Not imported by the application or the suite.
"""
from core.permissions import ADMIN_USER_IDS
from web.ratelimit import limiter
import web.routes.auth as auth

_ADMIN = {"user_id": min(ADMIN_USER_IDS), "role": "admin"}


async def _admin_session(_cookie):
    return dict(_ADMIN)


auth._resolve_session = _admin_session
limiter.enabled = False

from web.main import app  # noqa: E402  — after the two changes above
