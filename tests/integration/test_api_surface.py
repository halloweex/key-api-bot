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
    "POST /api/me/send-file": (
        "reads nothing on the server; sends the caller a CSV the caller "
        "posted, to the caller's own chat — the tab gate already applied "
        "where the page fetched it"
    ),

    # The header's filter dropdowns, on every page. Gating them per tab would
    # break the chrome of the one page a narrowed account can open, and they
    # carry catalogue names, no money.
    "GET /api/categories": "filter dropdown, catalogue names",
    "GET /api/categories/{parent_id}/children": "filter dropdown",
    "GET /api/brands": "filter dropdown",
    "GET /api/promocodes": "filter dropdown",

    # The operational readouts — `GET /api/health/detailed` (component
    # status), `GET /api/health/data-quality` (check results) and
    # `GET /api/metrics` (uptime and request timings) — stood here from
    # before tabs until the owner's decision O8 (a) (2026-11-11) put each
    # behind `require_admin`; `test_the_operational_readouts_are_admin_only`
    # below holds that. (`GET /api/duckdb/stats`, DuckDB's row counts, was one
    # until step 14's S14-17 deleted it.)
}

# Admin-only by the owner's decision O8 (a), 2026-11-11.
ADMIN_ONLY_READOUTS = (
    "GET /api/health/detailed", "GET /api/health/data-quality", "GET /api/metrics",
)


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


def test_the_operational_readouts_are_admin_only():
    """The decision recorded where the open list is: each of the three carries
    `require_admin` on its own route. Mutation: drop it from any of them, and
    the readout answers to any approved session again."""
    from web.routes.auth import require_admin

    found = {f"{method} {e.path}": e for e in iter_endpoints(app)
             for method in e.methods - {"HEAD", "OPTIONS"}}
    for entry in ADMIN_ONLY_READOUTS:
        assert entry in found, entry
        assert require_admin in found[entry].dependencies, entry
        assert entry not in SESSION_ONLY, entry
