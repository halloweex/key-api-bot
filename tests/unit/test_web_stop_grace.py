"""web gets long enough to shut down before Docker kills it — for an
ordinary request. Not for a slow one, and that is stated, not hidden.

Docker's default is 10 s, and web's shutdown waits on three things in turn:

1. uvicorn draining in-flight requests. For an ordinary path that is
   `DEFAULT_REQUEST_TIMEOUT` (30 s) — and `BaseHTTPMiddleware` lets a handler
   run on past its 504, so the bound is really the handler's own runtime;
2. `close_store()`, whose executor waits for the DuckDB work a cancelled
   scheduler job left in flight — `AsyncIOExecutor.shutdown` cancels and waits
   for nothing, so this is bounded by a whole warehouse refresh, ~30 s at
   production row counts on web's one CPU;
3. `close()`'s own checkpoint, ~7 s with the 900 MB of WAL the most a writer
   can accumulate under `wal_autocheckpoint='1GB'`.

About 67 s for an ordinary request, and 120 s covers it. **A request to one
of `SLOW_ENDPOINTS` is not covered**: they get `SLOW_ENDPOINT_TIMEOUT` (300 s),
uvicorn drains them too, and three write DuckDB (`SLOW_DUCKDB_WRITERS`). One
in flight at a stop keeps web draining past the grace, and Docker kills it.
Covering them would take ~340 s of grace, and with the same margin the
ordinary case gets, a deploy — pull, stop, migrate, a 180 s health gate —
would no longer fit the ssh step's 10-minute `command_timeout`. What makes the kill survivable is `open_file`: the
WAL it leaves is checkpointed whole at the next start (see
`test_duckdb_kill_guard.py`); the grace only makes the kill rarer. dockerd
logged three forced kills in thirty days at the 10 s default. Verified on a
throwaway compose project that the setting becomes the container's
StopTimeout and that both a deploy's recreate and `docker compose stop` wait
for it.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((REPO / "docker-compose.yml").read_text())
WORKFLOW = yaml.safe_load((REPO / ".github" / "workflows" / "deploy.yml").read_text())

WAREHOUSE_REFRESH_S = 30     # measured, production row counts, cpus: 1.0
CLOSING_CHECKPOINT_S = 7     # measured, 900 MB of WAL
SSH_COMMAND_TIMEOUT_S = 600  # appleboy/ssh-action's default at the pinned commit
HEALTH_GATE_S = 180          # the deploy's `timeout 180` wait for /api/health

# The slow paths a stop can be killed in the middle of, by name: adding one is
# a decision about this gap, and CLAUDE.md and docker-compose.yml name them.
KNOWN_SLOW_ENDPOINTS = {
    "/api/duckdb/resync",
    "/api/duckdb/refresh-statuses",
    "/api/goals/recalculate",
    "/api/stocks/analysis",
    "/api/revenue/forecast/train",
    "/api/revenue/forecast/evaluate",
    "/api/revenue/forecast/tune",
    "/api/traffic/reclassify",
}
# Of those, the ones that write DuckDB — a kill there leaves a WAL.
SLOW_DUCKDB_WRITERS = {
    "/api/duckdb/resync",
    "/api/duckdb/refresh-statuses",
    "/api/traffic/reclassify",
}


def _seconds(value: str) -> int:
    parts = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", str(value).strip())
    assert parts and any(parts.groups()), f"unparsable duration {value!r}"
    h, m, s = (int(g or 0) for g in parts.groups())
    return h * 3600 + m * 60 + s


def test_web_waits_for_an_ordinary_shutdown():
    from web.middleware import DEFAULT_REQUEST_TIMEOUT

    grace = _seconds(COMPOSE["services"]["web"]["stop_grace_period"])
    ordinary = DEFAULT_REQUEST_TIMEOUT + WAREHOUSE_REFRESH_S + CLOSING_CHECKPOINT_S
    assert grace >= 120 and grace >= 1.5 * ordinary, (grace, ordinary)


def test_a_slow_endpoint_outlasts_the_grace_and_is_named():
    """The arithmetic the earlier version of this file left out: a slow
    endpoint in flight at a stop is killed, and the kill is `open_file`'s
    to absorb. If the timeout or the grace ever change so that the grace
    covers it, this fails — and the docs saying it does not must change."""
    from web.middleware import SLOW_ENDPOINT_TIMEOUT, SLOW_ENDPOINTS

    grace = _seconds(COMPOSE["services"]["web"]["stop_grace_period"])
    slow = SLOW_ENDPOINT_TIMEOUT + WAREHOUSE_REFRESH_S + CLOSING_CHECKPOINT_S
    assert slow > grace, (slow, grace)
    assert set(SLOW_ENDPOINTS) == KNOWN_SLOW_ENDPOINTS
    assert SLOW_DUCKDB_WRITERS <= KNOWN_SLOW_ENDPOINTS
    # Covering them with the ordinary case's margin would not fit a deploy:
    # the stop, then the health gate, inside the ssh step's timeout.
    deploy = next(s for job in WORKFLOW["jobs"].values() for s in job.get("steps", [])
                  if "appleboy/ssh-action" in str(s.get("uses", "")))
    timeout = _seconds(deploy.get("with", {}).get("command_timeout", f"{SSH_COMMAND_TIMEOUT_S}s"))
    assert 1.5 * slow + HEALTH_GATE_S > timeout, "a grace covering them now fits: reconsider"


def test_a_deploy_still_fits_the_ssh_step():
    grace = _seconds(COMPOSE["services"]["web"]["stop_grace_period"])
    deploy = next(s for job in WORKFLOW["jobs"].values() for s in job.get("steps", [])
                  if "appleboy/ssh-action" in str(s.get("uses", "")))
    timeout = _seconds(deploy.get("with", {}).get("command_timeout", f"{SSH_COMMAND_TIMEOUT_S}s"))
    assert grace <= 240
    assert grace + HEALTH_GATE_S < timeout, (grace, timeout)
