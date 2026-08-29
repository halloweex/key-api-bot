"""The bot watching its own 512 MB — the container nobody was watching.

The memory monitor lives in the web container and reads its own cgroup; the
bot, with a limit fourteen times smaller, had a healthcheck and no pressure
telemetry at all. This module closes that: the same evaluator, the same
thresholds, the same per-level cooldowns — different persistence, because the
bot cannot open DuckDB (single-writer lock, held by web).

What the OOM-delta rule actually needs is one previous sample, so the bot
keeps exactly that: a small JSON file on the shared volume, written
atomically. No history, no peaks — the web monitor's 24h-peak garnish is a
nicety this container's alert can live without.

Runs on the bot's own job queue every 30 minutes, raises through the same
Gate as everything else (signature, kill switch, archive, resolve, agent).
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

STATE_PATH = Path(os.getenv("KS_ALERT_GATE_STATE_DIR", "data") or "data") \
    / "memory-bot-last.json"

# Same shape as the web monitor's cooldowns, same reason: a pre-OOM signal
# on a 512 MB container is not something a daily reminder should soften.
COOLDOWNS_S = {"WARN": 86400, "CRITICAL": 21600}
_last_alert: dict = {}


def _read_state() -> Optional[dict]:
    try:
        return json.loads(STATE_PATH.read_text())
    except FileNotFoundError:
        return None
    except Exception as exc:
        logger.warning("bot memory state unreadable (%s); starting fresh", exc)
        return None


def _write_state(sample: dict) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(STATE_PATH.parent),
                                   prefix=STATE_PATH.name)
        with os.fdopen(fd, "w") as handle:
            json.dump({"oom_kills": sample["oom_kills"],
                       "working_set": sample["working_set"],
                       "at": time.time()}, handle)
        os.replace(tmp, STATE_PATH)
    except Exception as exc:
        logger.warning("bot memory state not saved (%s)", exc)


async def check_bot_memory() -> dict:
    """One sampling tick. Never raises — this rides the bot's job queue."""
    from core.memory_monitor import evaluate_memory, read_cgroup_memory

    result: dict = {"alert_sent": None, "delivered": 0}
    try:
        mem = read_cgroup_memory()
        if not mem:
            logger.debug("bot memory: no cgroup readable")
            return result

        previous = _read_state()
        alert = evaluate_memory(
            working_set_bytes=mem["working_set"],
            page_cache_bytes=mem["page_cache"],
            limit_bytes=mem["limit"],
            oom_kills=mem["oom_kills"],
            previous_oom_kills=previous["oom_kills"] if previous else None,
        )
        _write_state(mem)

        from core.alerting import raise_alert, resolve_group

        if alert is None:
            pct = (mem["working_set"] / mem["limit"]) if mem["limit"] else 0
            logger.info(
                "Bot memory OK: working set %.0f MB (%.0f%%)",
                mem["working_set"] / 1048576, pct * 100,
            )
            await resolve_group("memory:bot")
            return result

        level = alert.severity.value
        logger.warning("Bot memory %s: %s", level, alert.reason)

        # The same per-level cooldown discipline as the web monitor, and the
        # same OOM exception: a kill is reported every time it happened.
        now = time.time()
        if not alert.oom_kills_delta and \
                now - _last_alert.get(level, 0) < COOLDOWNS_S[level]:
            result["alert_sent"] = f"{level} (suppressed)"
            return result

        icon = "\U0001f4a5" if alert.oom_kills_delta else (
            "⚠️" if level == "WARN" else "\U0001f6a8")
        title = ("BOT OOM KILL" if alert.oom_kills_delta
                 else f"Bot memory {level}")
        body = (
            f"{icon} <b>{title}</b>\n\n"
            f"<b>Working set:</b> {mem['working_set'] / 1048576:,.0f} MB"
            + (f" / {mem['limit'] / 1048576:,.0f} MB" if mem["limit"] else "")
            + f"\n<b>Page cache:</b> {mem['page_cache'] / 1048576:,.0f} MB\n\n"
            f"<i>{alert.reason}</i>"
        )
        if alert.oom_kills_delta:
            delivered = await raise_alert(
                body, conditions=[], bucket=None, spool_as="memory:bot:oom",
            )
        else:
            delivered = await raise_alert(
                body, conditions=[f"memory:bot:{level}"],
                bucket=None, group="memory:bot",
                spool_as=f"memory:bot:{level}",
            )
            if delivered:
                _last_alert[level] = now
        result["alert_sent"] = level
        result["delivered"] = delivered
        return result
    except Exception as exc:
        logger.warning("bot memory check failed: %s", exc)
        return result
