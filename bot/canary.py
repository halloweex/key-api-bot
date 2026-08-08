"""
Internal canary: HTTPS health probe + TLS cert expiry watcher.

Runs from the bot process (independent of the web container). Polls the
dashboard's /api/health every 15 minutes and checks the TLS cert's notAfter
date. Alerts admins on Telegram when the dashboard goes unhealthy or the
cert is about to expire — protecting against degraded states (data drift,
sync stalls, partial failures) and against silent cert lapses like the
12-hour outage on May 4 2026.

The check logic lives here as plain async functions with no Telegram
coupling so it can be unit-tested. Wiring + alert sending lives in
bot/main.py.
"""
from __future__ import annotations

import asyncio
import logging
import socket
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# ─── Tunables ───────────────────────────────────────────────────────────────

HEALTH_TIMEOUT_S = 10.0
CERT_TIMEOUT_S = 10.0
CERT_WARN_DAYS = 14

# Cooldown so a sustained outage doesn't spam admins. Recovery messages
# bypass cooldown so admins always learn when service returns.
ALERT_COOLDOWN_S = 3600  # 1 hour

# How stale the last *successful* data-quality run may get before we say so.
# One missed cycle plus grace: the job either ran late or did not run, and
# either way nobody is checking the warehouse against KeyCRM meanwhile.
# Reconciliation is daily at 05:00 Kyiv; integrity every 6h (01/07/13/19).
# Reconciliation matters most — it is the only check that can see a wrong
# status, a wrong source, or two errors that cancel out.
DQ_MAX_AGE_S = {
    "reconciliation": 30 * 3600,  # 24h cycle + 6h grace
    "integrity": 12 * 3600,       # 6h cycle + 6h grace
}


@dataclass
class CanaryResult:
    """Outcome of a single canary cycle."""
    ok: bool
    severity: str  # "ok" | "warn" | "critical"
    failures: list[str] = field(default_factory=list)
    # Stable identifiers for the failures above, one per entry, used to
    # throttle per-problem instead of per-message: a new problem must not be
    # swallowed by an older problem's cooldown, and rendered text (which
    # carries ages and counts that change every cycle) must never be the key.
    failure_keys: list[str] = field(default_factory=list)
    health_status: Optional[str] = None  # "healthy" | "degraded" | None
    http_code: Optional[int] = None
    cert_days_remaining: Optional[int] = None
    sync_seconds_since: Optional[int] = None
    # layer -> seconds since its last successful run (None = never / unknown)
    dq_ages: dict[str, Optional[int]] = field(default_factory=dict)


# ─── Health probe ───────────────────────────────────────────────────────────

async def check_health(
    url: str,
    timeout: float = HEALTH_TIMEOUT_S,
    client: Optional[httpx.AsyncClient] = None,
) -> tuple[Optional[int], Optional[dict], Optional[str]]:
    """GET <url> and return (http_status, parsed_json, error_message).

    Either an http status or an error message is set. JSON is None if the
    response wasn't decodable.
    """
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)

    try:
        resp = await client.get(url)
        try:
            payload = resp.json()
        except Exception:
            payload = None
        return resp.status_code, payload, None
    except httpx.TimeoutException:
        return None, None, f"timeout after {timeout:.0f}s"
    except httpx.HTTPError as exc:
        return None, None, f"{type(exc).__name__}: {exc}"
    finally:
        if own_client:
            await client.aclose()


# ─── Cert expiry probe ──────────────────────────────────────────────────────

def _parse_not_after(not_after: str) -> datetime:
    """Parse the 'notAfter' string as returned by ssl.getpeercert()."""
    return datetime.strptime(not_after, "%b %d %H:%M:%S %Y %Z").replace(
        tzinfo=timezone.utc
    )


def _fetch_peer_cert(host: str, port: int, timeout: float) -> dict:
    """Synchronous TLS handshake to read the peer cert. Runs in a thread."""
    ctx = ssl.create_default_context()
    with socket.create_connection((host, port), timeout=timeout) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as ssock:
            return ssock.getpeercert()


async def check_cert_expiry(
    host: str,
    port: int = 443,
    timeout: float = CERT_TIMEOUT_S,
    now: Optional[datetime] = None,
) -> tuple[Optional[int], Optional[str]]:
    """Return (days_remaining, error_message). Exactly one is None."""
    try:
        cert = await asyncio.to_thread(_fetch_peer_cert, host, port, timeout)
    except (socket.timeout, TimeoutError):
        return None, f"TLS connect timeout after {timeout:.0f}s"
    except (ssl.SSLError, OSError) as exc:
        return None, f"{type(exc).__name__}: {exc}"

    not_after_raw = cert.get("notAfter")
    if not not_after_raw:
        return None, "cert has no notAfter field"

    try:
        not_after = _parse_not_after(not_after_raw)
    except (ValueError, TypeError) as exc:
        return None, f"unparseable notAfter {not_after_raw!r}: {exc}"

    reference = now or datetime.now(timezone.utc)
    delta = not_after - reference
    return int(delta.total_seconds() // 86400), None


# ─── Data-quality run-age check ─────────────────────────────────────────────

def _format_age(seconds: int) -> str:
    """Human-readable age: '3h', '2d 4h'."""
    hours = seconds // 3600
    if hours < 48:
        return f"{hours}h"
    return f"{hours // 24}d {hours % 24}h"


def check_dq_freshness(
    payload: Optional[dict],
    max_age_s: Optional[dict[str, int]] = None,
) -> tuple[list[tuple[str, str]], dict[str, Optional[int]]]:
    """Judge the `data_quality` block of /api/health.

    Returns (failures, ages) where failures is a list of (key, message).

    **Absence is a failure.** The health endpoint omits the block when the
    query behind it fails, and a layer that has never succeeded reports a
    null age. Both read green to a naive `if age > threshold` check — which
    is exactly how a job that stopped producing verdicts stayed invisible.
    """
    thresholds = max_age_s if max_age_s is not None else DQ_MAX_AGE_S
    failures: list[tuple[str, str]] = []
    ages: dict[str, Optional[int]] = {}

    block = (payload or {}).get("data_quality")
    if not isinstance(block, dict):
        # Older web build, or the freshness query itself failed. Either way
        # nothing is watching the watchers.
        return [("dq_block_missing", "health payload has no data_quality block")], ages

    for layer, limit in thresholds.items():
        entry = block.get(layer)
        if not isinstance(entry, dict):
            failures.append(
                (f"dq_missing:{layer}", f"data quality: no {layer} freshness reported")
            )
            ages[layer] = None
            continue

        age = entry.get("age_seconds")
        ages[layer] = age
        if age is None:
            failures.append(
                (f"dq_never:{layer}", f"data quality: {layer} has never run successfully")
            )
        elif age > limit:
            failures.append((
                f"dq_stale:{layer}",
                f"data quality: last successful {layer} run was "
                f"{_format_age(int(age))} ago (>{_format_age(limit)})",
            ))

    return failures, ages


# ─── Orchestration ──────────────────────────────────────────────────────────

async def run_canary(
    dashboard_url: str,
    *,
    cert_warn_days: int = CERT_WARN_DAYS,
    client: Optional[httpx.AsyncClient] = None,
    now: Optional[datetime] = None,
) -> CanaryResult:
    """Run health + cert checks against `dashboard_url` and summarize.

    Both checks run in parallel — the cert check shouldn't be blocked by a
    slow health response.
    """
    parsed = urlparse(dashboard_url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    health_url = dashboard_url.rstrip("/") + "/api/health"

    health_task = asyncio.create_task(check_health(health_url, client=client))
    cert_task: Optional[asyncio.Task] = None
    if parsed.scheme == "https" and host:
        cert_task = asyncio.create_task(check_cert_expiry(host, port, now=now))

    http_code, payload, http_err = await health_task
    cert_days, cert_err = (None, None)
    if cert_task is not None:
        cert_days, cert_err = await cert_task

    failures: list[str] = []
    failure_keys: list[str] = []
    severity = "ok"

    def fail(key: str, message: str) -> None:
        failure_keys.append(key)
        failures.append(message)

    if http_err:
        fail("health_unreachable", f"health request failed: {http_err}")
        severity = "critical"
    elif http_code != 200:
        fail("health_http", f"health returned HTTP {http_code}")
        severity = "critical"

    health_status = None
    sync_seconds = None
    dq_ages: dict[str, Optional[int]] = {}
    if payload:
        health_status = payload.get("status")
        sync_block = payload.get("sync") or {}
        sync_seconds = sync_block.get("seconds_since_sync")
        if health_status and health_status != "healthy":
            fail("health_status", f"status={health_status}")
            severity = "critical"

        # Only judge freshness when the endpoint answered at all — an
        # unreachable dashboard is already reported above, and piling a
        # "no data_quality block" line on top adds noise, not information.
        dq_failures, dq_ages = check_dq_freshness(payload)
        for key, message in dq_failures:
            fail(key, message)
        if dq_failures and severity == "ok":
            # A blind checker is degraded observability, not an outage:
            # the dashboard still serves. Loud enough to be delivered,
            # quiet enough not to read as a site-down page.
            severity = "warn"

    if cert_err:
        fail("cert_unreachable", f"cert check failed: {cert_err}")
        if severity == "ok":
            severity = "warn"
    elif cert_days is not None and cert_days < cert_warn_days:
        fail("cert_expiring", f"cert expires in {cert_days}d (<{cert_warn_days})")
        # Cert about to expire is critical even if health is otherwise OK —
        # silent expiry is what burned us last time.
        severity = "critical"

    return CanaryResult(
        ok=(severity == "ok"),
        severity=severity,
        failures=failures,
        failure_keys=failure_keys,
        health_status=health_status,
        http_code=http_code,
        cert_days_remaining=cert_days,
        sync_seconds_since=sync_seconds,
        dq_ages=dq_ages,
    )


# ─── Alert formatting + dedup state machine ─────────────────────────────────

def format_alert(result: CanaryResult, dashboard_url: str) -> str:
    """Build a Telegram HTML message for a failing result."""
    icon = "\U0001f6a8" if result.severity == "critical" else "⚠️"
    title = "Dashboard CRITICAL" if result.severity == "critical" else "Dashboard Warning"

    lines = [
        f"{icon} <b>{title}</b>",
        f"<a href=\"{dashboard_url}\">{dashboard_url}</a>",
        "",
    ]
    for failure in result.failures:
        lines.append(f"• {failure}")

    extras: list[str] = []
    if result.http_code is not None:
        extras.append(f"http={result.http_code}")
    if result.health_status:
        extras.append(f"status={result.health_status}")
    if result.cert_days_remaining is not None:
        extras.append(f"cert_days={result.cert_days_remaining}")
    if result.sync_seconds_since is not None:
        extras.append(f"sync_age={result.sync_seconds_since}s")
    for layer, age in result.dq_ages.items():
        extras.append(f"{layer}_age=" + (_format_age(int(age)) if age is not None else "never"))
    if extras:
        lines.append("")
        lines.append("<i>" + " · ".join(extras) + "</i>")
    return "\n".join(lines)


def format_recovery(result: CanaryResult, dashboard_url: str) -> str:
    """Telegram message announcing return to healthy."""
    extras: list[str] = []
    if result.cert_days_remaining is not None:
        extras.append(f"cert {result.cert_days_remaining}d")
    if result.sync_seconds_since is not None:
        extras.append(f"sync {result.sync_seconds_since}s ago")
    suffix = f" ({', '.join(extras)})" if extras else ""
    return (
        "✅ <b>Dashboard recovered</b>\n"
        f"<a href=\"{dashboard_url}\">{dashboard_url}</a>{suffix}"
    )


class CanaryState:
    """Tracks failures per problem to dedupe alerts and emit recovery notices.

    Throttling is keyed on `CanaryResult.failure_keys` — stable identifiers
    like `dq_stale:reconciliation` — and never on the rendered message, which
    carries ages and counts that change on every cycle and would defeat the
    cooldown. Keying per problem also means a new problem alerts immediately
    instead of waiting out an unrelated problem's cooldown.

    Kept as a small object so tests can construct independent instances and
    the bot can hold one shared instance across job runs.
    """

    def __init__(self, cooldown_s: float = ALERT_COOLDOWN_S):
        self.cooldown_s = cooldown_s
        # key -> timestamp of the last alert sent for that key
        self._alerted_at: dict[str, float] = {}

    def decide(
        self, result: CanaryResult, *, now: Optional[float] = None
    ) -> Optional[str]:
        """Return 'alert', 'recovery', or None depending on state transitions."""
        ts = now if now is not None else time.monotonic()
        if result.ok:
            if self._alerted_at:
                self._alerted_at.clear()
                return "recovery"
            return None

        # A result with no keys still has to alert — fall back to one bucket
        # rather than silently dropping it.
        keys = result.failure_keys or ["unkeyed"]

        # Forget problems that have resolved, so their return alerts at once
        # instead of inheriting a cooldown from the last time they happened.
        for stale_key in set(self._alerted_at) - set(keys):
            del self._alerted_at[stale_key]

        due = [
            k for k in keys
            if k not in self._alerted_at
            or (ts - self._alerted_at[k]) >= self.cooldown_s
        ]
        if due:
            # A key missing from the map is due by definition, so recording
            # the due ones records every currently-failing problem.
            for k in due:
                self._alerted_at[k] = ts
            return "alert"
        # Suppressed (every current problem alerted within the cooldown).
        return None
