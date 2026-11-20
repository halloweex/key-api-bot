"""The Python side of the pilot's benchmark: production's app, as it is.

`bench.sh` and the pilot's workflow serve this with uvicorn exactly as
`Dockerfile.web` serves `web.main:app` — one process, uvloop and httptools,
every middleware, orjson. Nothing is switched off any more: the pilot checks
the session and keeps the rate limit too, so the load generator brings what
a browser would — a signed `dashboard_session` cookie, and an `X-Real-IP` of
its own on every request, so `@limiter.limit("30/minute")` is checked on each
and trips on none.

Not imported by the application or the suite.
"""
from web.main import app  # noqa: F401  — served as `rust.bench.py_app:app`
