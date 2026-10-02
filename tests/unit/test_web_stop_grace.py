"""web gets long enough to shut down before Docker kills it.

Docker's default is 10 s, and web's shutdown waits on three things in turn:

1. uvicorn draining in-flight requests — `DEFAULT_REQUEST_TIMEOUT` (30 s),
   and `BaseHTTPMiddleware` lets a handler run on past its 504;
2. `close_store()`, whose executor waits for the DuckDB work a cancelled
   scheduler job left in flight — `AsyncIOExecutor.shutdown` cancels and waits
   for nothing, so this is bounded by a whole warehouse refresh, ~30 s at
   production row counts on web's one CPU;
3. `close()`'s own checkpoint, ~7 s with the 900 MB of WAL the most a writer
   can accumulate under `wal_autocheckpoint='1GB'`.

About 67 s at worst. A web killed before its DuckDB closes leaves a WAL; until
`open_read_write` that cost the next start its index entries (see
`test_duckdb_kill_guard.py`), and dockerd logged three forced kills in thirty
days. The upper bound keeps a deploy — pull, stop, migrate, a 180 s health
gate — inside the ssh step's 10-minute `command_timeout`. Verified on a
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


def _seconds(value: str) -> int:
    parts = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", str(value).strip())
    assert parts and any(parts.groups()), f"unparsable duration {value!r}"
    h, m, s = (int(g or 0) for g in parts.groups())
    return h * 3600 + m * 60 + s


def test_web_waits_for_its_own_shutdown():
    from web.middleware import DEFAULT_REQUEST_TIMEOUT

    grace = _seconds(COMPOSE["services"]["web"]["stop_grace_period"])
    worst = DEFAULT_REQUEST_TIMEOUT + WAREHOUSE_REFRESH_S + CLOSING_CHECKPOINT_S
    assert grace >= 120 and grace >= 1.5 * worst, (grace, worst)


def test_a_deploy_still_fits_the_ssh_step():
    grace = _seconds(COMPOSE["services"]["web"]["stop_grace_period"])
    deploy = next(s for job in WORKFLOW["jobs"].values() for s in job.get("steps", [])
                  if "appleboy/ssh-action" in str(s.get("uses", "")))
    timeout = _seconds(deploy.get("with", {}).get("command_timeout", f"{SSH_COMMAND_TIMEOUT_S}s"))
    assert grace <= 240
    assert grace + HEALTH_GATE_S < timeout, (grace, timeout)
