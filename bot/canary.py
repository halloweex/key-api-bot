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
import html
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

# The alert policy lives in core/alerting.py's Gate since step 07 —
# CanaryState, this module's own throttle for its first four months, is gone.

# How stale the last *successful* data-quality run may get before we say so.
# One missed cycle plus grace: the job either ran late or did not run, and
# either way nobody is checking the warehouse against KeyCRM meanwhile.
# Reconciliation is daily at 05:00 Kyiv; integrity every 6h (01/07/13/19).
# Reconciliation matters most — it is the only check that can see a wrong
# status, a wrong source, or two errors that cancel out.
DQ_MAX_AGE_S = {
    "reconciliation": 30 * 3600,  # 24h cycle + 6h grace
    "integrity": 12 * 3600,       # 6h cycle + 6h grace
    # Daily at 07:30 Kyiv, same shape as reconciliation. Added 28.08 after the
    # layer grew the step-2/5/6 comparisons (buyers, витрина, two engines'
    # Gold, the archive) — a сверка that quietly stops running would now hide
    # six copies at once. The old worry — the first probe firing 90 s after a
    # restart, before the catch-up run finishes — only bites when the layer is
    # ALREADY past 30h at restart, i.e. after a genuine day-long outage, and
    # one page on the way out of that is the canary doing its job.
    "mirror_landing": 30 * 3600,  # 24h cycle + 6h grace
}

# How stale the Postgres copy of a landing table may get before we say so.
#
# The hole this closes: until the 07:30 comparison, a broken orders mirror was
# silent — a full day in which the copy that carries the money could be dead
# with nobody told.
#
# **The number is measured, not chosen.** `mirror_orders` deliberately refuses
# to move the watermark when there was nothing to ship ("moving it here would
# date-stamp a shipment that did not happen"), so the threshold has to clear
# the longest *legitimate* silence. On production, order writes stop at about
# 01:00 Kyiv, resume for the 05:15 status refresh — measured 29.08: 1 458
# orders written in that one hour — and then again with traffic around 07:00.
# That is a real ~4h15m quiet window, ~6h if the 05:15 job is ever skipped.
# Eight hours is that plus grace.
#
# A generous age limit is affordable only because it is not the primary
# signal: `failures_since_ok` catches an actively failing mirror on the next
# sync tick, minutes after it breaks. The age is the backstop for the failure
# mode that raises nothing — a mirror switched off, or a sync that stopped
# feeding it.
MIRROR_MAX_AGE_S = {
    "bronze.orders": 8 * 3600,
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
    # mirrored table -> seconds since its last successful shipment
    mirror_ages: dict[str, Optional[int]] = field(default_factory=dict)


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
        return [("dq_block_missing", "нет блока data_quality в health")], ages

    for layer, limit in thresholds.items():
        entry = block.get(layer)
        if not isinstance(entry, dict):
            failures.append(
                (f"dq_missing:{layer}", f"{layer}: свежесть не сообщается")
            )
            ages[layer] = None
            continue

        age = entry.get("age_seconds")
        ages[layer] = age
        if age is None:
            failures.append(
                (f"dq_never:{layer}", f"{layer}: ни одного успешного прогона")
            )
        elif age > limit:
            failures.append((
                f"dq_stale:{layer}",
                f"{layer}: молчит {_format_age(int(age))}",
            ))

    return failures, ages


def check_mirror_freshness(
    payload: Optional[dict],
    max_age_s: Optional[dict[str, int]] = None,
) -> tuple[list[tuple[str, str]], dict[str, Optional[int]]]:
    """Judge the `mirrors` block of /api/health.

    Same shape and same rule as `check_dq_freshness`: **absence is a failure**.
    The endpoint publishes null rather than an empty object when it could not
    read the watermarks, so "nobody is watching the main copy" cannot arrive
    looking like "nothing is wrong".

    Two independent verdicts per table, and the order matters — a mirror that
    is failing says so on the next sync tick, long before its age crosses
    anything, so the failure count is checked first and reported on its own.
    """
    thresholds = max_age_s if max_age_s is not None else MIRROR_MAX_AGE_S
    failures: list[tuple[str, str]] = []
    ages: dict[str, Optional[int]] = {}

    block = (payload or {}).get("mirrors")
    if not isinstance(block, dict):
        return [("mirror_block_missing",
                 "нет блока mirrors в health")], ages

    for table, limit in thresholds.items():
        entry = block.get(table)
        if not isinstance(entry, dict):
            failures.append(
                (f"mirror_missing:{table}", f"зеркало {table}: свежесть не сообщается")
            )
            ages[table] = None
            continue

        age = entry.get("age_seconds")
        ages[table] = age

        # The mirror is actively erroring. This is the fast signal and it is
        # worth saying even when the age is still inside its limit, because it
        # is the difference between "it broke minutes ago" and waiting 8 hours
        # to find out.
        failed = entry.get("failures_since_ok") or 0
        if entry.get("failing") or failed:
            failures.append((
                f"mirror_failing:{table}",
                f"зеркало {table}: падает, {failed}× подряд",
            ))

        if age is None:
            failures.append(
                (f"mirror_never:{table}", f"зеркало {table}: ещё не уезжало")
            )
        elif age > limit:
            failures.append((
                f"mirror_stale:{table}",
                f"зеркало {table}: не уезжало {_format_age(int(age))}",
            ))

    return failures, ages


# How many transport failures in a row before the canary says the alerting
# itself is broken. One failure is a Telegram hiccup the next send retries;
# three consecutive means nothing is reaching anyone — and nothing else in
# the system would ever say so, which is how the certificate alert stayed
# undeliverable for months.
ALERTING_MAX_CONSECUTIVE_FAILURES = 3


def check_alerting_health(
    payload: Optional[dict],
) -> "list[tuple[str, str]]":
    """Judge the `alerting` block of /api/health. Absence is a failure —
    rule 3, same as the data_quality and mirrors blocks."""
    block = (payload or {}).get("alerting")
    if not isinstance(block, dict):
        return [("alerting_block_missing",
                 "нет блока alerting в health")]
    failures = int(block.get("consecutive_transport_failures") or 0)
    if failures >= ALERTING_MAX_CONSECUTIVE_FAILURES:
        return [(
            "alerting_transport_failing",
            f"алертинг: {failures} отказов доставки подряд — тревоги могут не доходить",
        )]
    return []


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
        fail("health_unreachable", f"дашборд не отвечает: {http_err}")
        severity = "critical"
    elif http_code != 200:
        fail("health_http", f"дашборд вернул HTTP {http_code}")
        severity = "critical"

    health_status = None
    sync_seconds = None
    dq_ages: dict[str, Optional[int]] = {}
    mirror_ages: dict[str, Optional[int]] = {}
    if payload:
        health_status = payload.get("status")
        sync_block = payload.get("sync") or {}
        sync_seconds = sync_block.get("seconds_since_sync")
        if health_status and health_status != "healthy":
            fail("health_status", f"приложение деградировало: {health_status}")
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

        # Same judgement, same reason it is only made when the endpoint
        # answered. The copy of landing in Postgres is the store the whole
        # migration is being carried to; its own comparison runs once a day,
        # so without this a broken mirror waits until 07:30 to be noticed.
        mirror_failures, mirror_ages = check_mirror_freshness(payload)
        for key, message in mirror_failures:
            fail(key, message)
        if mirror_failures and severity == "ok":
            severity = "warn"

        # The alerting watching itself — the web process reports its own
        # transport health; this container judges it. Warn, not critical:
        # the site serves, but the verdict channel may be mute.
        alerting_failures = check_alerting_health(payload)
        for key, message in alerting_failures:
            fail(key, message)
        if alerting_failures and severity == "ok":
            severity = "warn"

    if cert_err:
        fail("cert_unreachable", f"TLS не проверился: {cert_err}")
        if severity == "ok":
            severity = "warn"
    elif cert_days is not None and cert_days < cert_warn_days:
        fail("cert_expiring", f"сертификат истекает через {cert_days}д")
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
        mirror_ages=mirror_ages,
    )


# ─── Alert formatting + dedup state machine ─────────────────────────────────

# What the reader is supposed to *do*, per failing key. Rule 1 of the alerts
# charter: a page that does not name a lever is a page that trains people to
# swipe. Ordered by how much the answer differs — the first matching key wins,
# because "the dashboard is unreachable" outranks "a layer is stale" when both
# are true.
_ACTIONS: tuple[tuple[str, str], ...] = (
    ("health_unreachable",
     "curl /api/health с VPS — отделить приложение от nginx/TLS. "
     "Рестарт web — последний рычаг"),
    ("health_http", "Смотри лог web по упавшему запросу — сеть ни при чём"),
    ("health_status", "/api/health скажет, что деградировало; миграцию рестарт не чинит"),
    ("cert_expiring", "Проверь certbot на хосте — автопродление сломалось"),
    ("cert_unreachable", "TLS не отвечает: nginx или сеть, не приложение"),
    ("mirror_", "Смотри meta.mirror_state и лог web; зеркало шлёт само"),
    ("dq_", "Проверь джобы в /api/jobs — склад сейчас никто не сверяет"),
    ("alerting_", "Отказы доставки Telegram подряд — проверь лог web"),
)

def _what_to_do(result: CanaryResult) -> Optional[str]:
    """The single most useful lever for this result, or None."""
    for prefix, action in _ACTIONS:
        if any(k.startswith(prefix) for k in result.failure_keys):
            return action
    return None


def format_alert(result: CanaryResult, dashboard_url: str) -> str:
    """Build a Telegram HTML message for a failing result."""
    icon = "\U0001f6a8" if result.severity == "critical" else "⚠️"
    title = "Дашборд лежит" if result.severity == "critical" else "Дашборд: тревога"

    lines = [f"{icon} <b>{title}</b>"]
    for failure in result.failures:
        # Escaped because failure text is data, not markup: the cert line
        # carries a literal `(<14)` and httpx exception strings can carry
        # anything. Under parse_mode=HTML an unescaped `<` is a Telegram 400
        # — which is how the certificate alert, the alert this module was
        # written for, went undeliverable without anyone knowing.
        lines.append(f"• {html.escape(failure)}")

    action = _what_to_do(result)
    if action:
        lines.append(f"→ {action}")

    # Две-три ключевые цифры, не приборная панель: детальные возрасты живут
    # в /api/health, а страница обязана читаться за три секунды.
    extras: list[str] = []
    if result.cert_days_remaining is not None:
        extras.append(f"cert {result.cert_days_remaining}д")
    if result.http_code is not None and result.http_code != 200:
        extras.append(f"http {result.http_code}")
    if extras:
        lines.append("<i>" + html.escape(" · ".join(extras)) + "</i>")
    return "\n".join(lines)
