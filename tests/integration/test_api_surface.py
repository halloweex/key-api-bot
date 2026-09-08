"""Which endpoints answer to *any* approved session, as an allowlist.

OWASP's authorization guidance opens with deny-by-default and centralised
enforcement: "treat all endpoints as protected, except truly public ones", and
put the check in one place rather than per method. This repository does both —
`api_gate` at the `/api` include, then a tab permission on the route — and
`PUBLIC_API_PATHS` already pins the endpoints reachable with **no session**.

Nothing pinned the layer between: endpoints that need a session and nothing
more. That set is not empty and should not be, but it must not grow by
accident. A read added to a module that gates per endpoint — `analytics.py`
serves the dashboard and two charts /marketing shares, so it cannot gate at the
include — inherits `api_gate` alone and is silently readable by every account,
including one narrowed to a single tab. That is the hole per-user tabs were
built to close, reopened by omission.

So: a new session-only endpoint fails here until somebody writes down why.
"""
from __future__ import annotations

import pytest

from web.main import app
from tests.routes_helper import iter_endpoints

# Reachable by any approved session, and each entry is a decision.
SESSION_ONLY = {
    # Machine callers and the page's own identity.
    "GET /api/health": "public; polled by Docker, nginx and uptime monitors",
    "POST /api/webhooks/turbosms": "public; signs itself with a shared secret",
    "GET /api/me": "who am I — the answer is about the caller only",
    "PATCH /api/me/preferences": "the caller's own language",

    # The header's filter dropdowns, on every page. Gating them per tab would
    # break the chrome of the one page a narrowed account can open, and they
    # carry catalogue names, no money.
    "GET /api/categories": "filter dropdown, catalogue names",
    "GET /api/categories/{parent_id}/children": "filter dropdown",
    "GET /api/brands": "filter dropdown",
    "GET /api/promocodes": "filter dropdown",

    # Operational readouts: row counts, uptime, check ages. No per-tab data,
    # and the canary and the digest read them. Pre-dating tabs.
    "GET /api/duckdb/stats": "row counts and date bounds",
    "GET /api/health/detailed": "component status and row counts",
    "GET /api/health/data-quality": "check results, no business figures",
    "GET /api/metrics": "uptime and request timings",
}


def _ungated() -> dict:
    """Endpoints carrying neither a tab permission nor the admin dependency."""
    out = {}
    for endpoint in iter_endpoints(app):
        if not endpoint.path.startswith("/api"):
            continue
        gated = False
        for dep in endpoint.dependencies:
            name = getattr(dep, "__name__", "")
            if name in ("check_permission", "check_any", "require_admin"):
                gated = True
                break
        if gated:
            continue
        for method in sorted(endpoint.methods - {"HEAD", "OPTIONS"}):
            out[f"{method} {endpoint.path}"] = endpoint
    return out


def test_no_endpoint_joins_the_open_surface_unannounced():
    found = set(_ungated())
    unexpected = sorted(found - set(SESSION_ONLY))
    assert not unexpected, (
        f"{unexpected} answer to any approved session and are not in the "
        f"allowlist. Either gate them on the tab their data belongs to, or "
        f"add them here with the reason they are open to everybody."
    )


def test_the_allowlist_has_no_leftovers():
    """A stale entry is a permission nobody is actually granting, and it makes
    the list stop being read."""
    found = set(_ungated())
    stale = sorted(set(SESSION_ONLY) - found)
    assert not stale, f"{stale} are gated or gone; drop them from the list"


@pytest.mark.parametrize("entry", sorted(SESSION_ONLY))
def test_every_entry_carries_a_reason(entry):
    assert SESSION_ONLY[entry].strip(), entry
