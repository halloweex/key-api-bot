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


@dataclass
class CanaryResult:
    """Outcome of a single canary cycle."""
    ok: bool
    severity: str  # "ok" | "warn" | "critical"
    failures: list[str] = field(default_factory=list)
    health_status: Optional[str] = None  # "healthy" | "degraded" | None
    http_code: Optional[int] = None
    cert_days_remaining: Optional[int] = None
    sync_seconds_since: Optional[int] = None


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
    severity = "ok"

    if http_err:
        failures.append(f"health request failed: {http_err}")
        severity = "critical"
    elif http_code != 200:
        failures.append(f"health returned HTTP {http_code}")
        severity = "critical"

    health_status = None
    sync_seconds = None
    if payload:
        health_status = payload.get("status")
        sync_block = payload.get("sync") or {}
        sync_seconds = sync_block.get("seconds_since_sync")
        if health_status and health_status != "healthy":
            failures.append(f"status={health_status}")
            severity = "critical"

    if cert_err:
        failures.append(f"cert check failed: {cert_err}")
        if severity == "ok":
            severity = "warn"
    elif cert_days is not None and cert_days < cert_warn_days:
        failures.append(f"cert expires in {cert_days}d (<{cert_warn_days})")
        # Cert about to expire is critical even if health is otherwise OK —
        # silent expiry is what burned us last time.
        severity = "critical"

    return CanaryResult(
        ok=(severity == "ok"),
        severity=severity,
        failures=failures,
        health_status=health_status,
        http_code=http_code,
        cert_days_remaining=cert_days,
        sync_seconds_since=sync_seconds,
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
    """Tracks last-failure to dedupe alerts and emit recovery notices.

    Kept as a small object so tests can construct independent instances and
    the bot can hold one shared instance across job runs.
    """

    def __init__(self, cooldown_s: float = ALERT_COOLDOWN_S):
        self.cooldown_s = cooldown_s
        self._last_alert_ts: float = 0.0
        self._failing: bool = False

    def decide(
        self, result: CanaryResult, *, now: Optional[float] = None
    ) -> Optional[str]:
        """Return 'alert', 'recovery', or None depending on state transitions."""
        ts = now if now is not None else time.monotonic()
        if result.ok:
            if self._failing:
                self._failing = False
                self._last_alert_ts = 0.0
                return "recovery"
            return None

        # Failing — alert if first failure or cooldown elapsed.
        if not self._failing or (ts - self._last_alert_ts) >= self.cooldown_s:
            self._failing = True
            self._last_alert_ts = ts
            return "alert"
        # Suppressed (still failing within cooldown).
        return None
