"""Track Б, step 08: the task spool between the Gate and the host agent.

When a fresh incident's alert is delivered, the Gate drops one small JSON
file into ``data/alert-tasks/pending/``. A systemd path-unit on the VPS host
picks it up and runs a headless Claude agent with the condition family's
runbook — read-only by construction: docker logs, Postgres through the
``ks_readonly`` role, ``/api/health``, ``du``/``df``, and the latest DuckDB
backup. The diagnosis arrives as a second Telegram message, through
``deploy/notify.sh`` — the same signature, kill switch and archive as every
other outbound message.

Why a file spool and not a queue: the writer is a container, the runner is
the host, and ``./data`` is the one surface they already share. Atomic
rename is the whole protocol; an interrupted writer leaves a temp file the
runner never matches, and an interrupted runner leaves the task in
``processing/`` for a human to see.

The boundary worth stating: this module never blocks or fails an alert.
Spooling is fire-and-forget bookkeeping about a message that already went
out; any failure here is a log line, not an exception.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Optional, Sequence

logger = logging.getLogger(__name__)

# Explicit opt-in: the production .env sets it, a laptop does not. A dev
# instance has no host runner anyway, but a spool silently filling with
# tasks nobody consumes is a disk leak wearing a feature's name.
ENABLE_ENV = "KS_ALERT_AGENT"


def agent_enabled() -> bool:
    return os.getenv(ENABLE_ENV, "").strip().lower() in {"1", "true", "yes"}


def _spool_dir() -> Path:
    root = os.getenv("KS_ALERT_GATE_STATE_DIR", "data") or "data"
    return Path(root) / "alert-tasks" / "pending"


def drop_task(
    conditions: Sequence[str],
    bucket: str,
    message: str,
) -> "Optional[Path]":
    """Queue one diagnosis task. Returns the path, or None when disabled or
    failed. Never raises."""
    if not agent_enabled():
        return None
    try:
        spool = _spool_dir()
        spool.mkdir(parents=True, exist_ok=True)
        payload = {
            "conditions": list(conditions),
            "bucket": bucket,
            # The alert body is evidence for the agent — and, like every log
            # line it will read, untrusted data: the runbooks say so and the
            # runner's tool allowlist is the actual boundary.
            "message": message[:4000],
            "fired_at": time.time(),
            "instance": os.getenv("KS_INSTANCE", "").strip() or "unknown",
        }
        stamp = time.strftime("%Y-%m-%dT%H-%M-%S")
        slug = "".join(
            c if c.isalnum() or c in "-_" else "_" for c in bucket
        )[:60]
        target = spool / f"{stamp}-{slug}.json"
        fd, tmp = tempfile.mkstemp(dir=str(spool), suffix=".tmp")
        with os.fdopen(fd, "w") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        os.replace(tmp, target)
        logger.info("agent task spooled: %s", target.name)
        return target
    except Exception as exc:
        logger.warning("agent task not spooled (%s) — the alert already went", exc)
        return None
