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
from typing import Optional, Sequence
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# ─── Tunables ───────────────────────────────────────────────────────────────

HEALTH_TIMEOUT_S = 10.0
CERT_TIMEOUT_S = 10.0
CERT_WARN_DAYS = 14

# The alert policy lives in core/alerting.py's Gate since step 07 —
# CanaryState, this module's own throttle for its first four months, is gone.

# Keys that must fail two consecutive probes before they page. The 05:15
# status refresh blocks web's event loop for ~4.5 minutes daily (the UTM
# parse over 32k orders), and a probe landing in that window is a
# self-healing blip: page + diagnosis + resolve for a condition that needs
# no human — measured on the first real night. A true outage still pages
# on the next probe (≤15 min later), and UptimeRobot watches from outside
# on its own clock. Everything else — cert, dq, mirrors, alerting — pages
# on the first probe as before: none of those flap with the event loop.
FLAKY_PROBE_KEYS = frozenset(
    {"health_unreachable", "health_http", "health_status"}
)


def defer_flaky(
    keys: "list[str]", previous: "set[str]",
) -> "tuple[bool, set[str]]":
    """(defer_this_tick, new_previous).

    Defer only when EVERY current failure is a flaky-probe key seen for the
    first time — a mixed result (cert, dq, mirrors alongside) pages at once,
    and a health failure already seen last tick is confirmed.
    """
    current = set(keys)
    fresh_flaky = {
        k for k in current if k in FLAKY_PROBE_KEYS and k not in previous
    }
    defer = bool(current) and current == fresh_flaky
    return defer, current

# How stale the last *successful* data-quality run may get before we say so.
# One missed cycle plus grace: the job either ran late or did not run, and
# either way nobody is checking the warehouse against KeyCRM meanwhile.
# Reconciliation is daily at 05:30 Kyiv; integrity every 6h (01/07/13/19).
# Reconciliation matters most — it is the only check that can see a wrong
# status, a wrong source, or two errors that cancel out.
DQ_MAX_AGE_S = {
    # `reconciliation`, the 05:30 job's DuckDB half, was paged here at 30 h
    # until chain 3's flip stood it down (published `stood_down`, unjudged);
    # step 14's S14-12 deleted the half and the layer with it.
    #
    # The same 05:30 job's Postgres half, compared against the KeyCRM
    # snapshot — the digest's limit too. It is a layer of its own so that a
    # Postgres half which stops cannot hide behind a fresh sibling, and until
    # DN-21 only the 09:00 digest would say so.
    # It is meant to be the comparison against the source that is left once
    # DuckDB stops being fed. It is not independent of DuckDB yet: the job
    # runs it only after the DuckDB extraction succeeded, inside the same try,
    # and journals it in DuckDB's data_quality_runs, which is where the age
    # /api/health publishes comes from — until chain 9 (KS_WRITE_DQ_JOURNAL,
    # OD-02 (c)) moves the journal to Postgres, when the age comes from there;
    # the job's dependence on the DuckDB extraction stays. So a DuckDB failure
    # silences it too,
    # and since DN-21 that pages under this key as well as `reconciliation`.
    # Decoupling it belongs with step 13.
    # DN-21's opt-in rested on what the stage-4 soak checked on 18.09: a
    # successful run on each of the 14 days before that, none CRITICAL. That
    # counted calendar days, not the silence this limit measures; the check
    # asks both now (deploy/daily_soak/20_reconciliation_pg_history.sql).
    # A web with no Postgres configured never writes the layer
    # (`_reconcile_postgres` returns None — silence, not a clean run), so such
    # a host now pages `dq_never:reconciliation_pg`; production web always
    # carries `KS_PG_DSN`.
    "reconciliation_pg": 30 * 3600,  # 24h cycle + 6h grace
    # The third arm of the same 05:30 job: ClickHouse against the same
    # snapshot, its own layer for the reason above. It was published and
    # digested but not paged on while ClickHouse was optional; OD-08 (a)
    # (2026-09-30) made it required for the parallel period, and after step 13
    # ClickHouse is the only engine that recomputes anything independently.
    # Same limit, same dependence on the DuckDB extraction as reconciliation_pg,
    # and the same consequence of a web with nowhere to compare: without
    # KS_CH_URL `_reconcile_clickhouse` returns None, no run is written, and
    # the host pages `dq_never:reconciliation_ch` — which is what "required"
    # means. A copy too old to reconcile (over 3 h) is gated, not blamed —
    # and not a success either: it compared nothing, so it is written with
    # `error_message` set beside its `ch_reconcile_pending` and does not reset
    # this age. It was once written as a success, and a ClickHouse that stayed
    # down then paged here at most once: the next morning's gated run reset
    # the age and the page was announced resolved with ClickHouse still down.
    # So any morning without a comparison — an arm that raised, a stale copy
    # — pages here 30 h after the last one that compared. After a restart the
    # dq_reconciliation catch-up reads this layer's age too
    # (`CATCHUP_SIBLING_LAYERS`), so the first probe pages only a layer
    # already past 30 h; a catch-up against a copy still stale is a failed
    # run as well, so it cannot hold the page back — it costs one KeyCRM
    # fetch per restart while ClickHouse stays down.
    "reconciliation_ch": 30 * 3600,  # 24h cycle + 6h grace
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

# The most the canary will accept from a limit web *declares* for a table (the
# layers Postgres derives on its own signal, `max_age_s` in the mirrors block).
# Web declares those because only web knows its KS_PG_DERIVE, and until DN-05c
# the canary adopted whatever it was told: one wrong constant in web —
# `90 * 60 * 60` where `90 * 60` was meant, nearly four days — would have
# switched the derived-table page off without a word, and the canary is the
# watchdog meant to stand outside web's mistakes. So it judges
# min(declared, this).
#
# Three hours, because it is twice what web declares today (90 minutes: the
# 60-minute heartbeat, the 10-minute floor, a tick and grace) and so costs
# nothing now, and because a derivation that has not run for three hours has
# already left /summary, /marketing and /traffic behind by more than anyone
# would read as current. A tighter declaration is honoured as it stands.
# Live paging change: a declaration above three hours now pages at three.
DECLARED_MAX_AGE_CEILING_S = 3 * 3600


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
    # `web_uptime_s` and `duckdb_silent` stood here until step 14's S14-18:
    # what the week of silence's watch (`watch:duckdb_switch`) was written
    # from on every probe. The switch is deleted, its row stays in
    # `app.alert_series` as history, and nothing writes it.
    # Keys this probe could not judge, because it read no block to judge them
    # by: the resolve keeps them firing (see unjudged_keys).
    unjudged_keys: list[str] = field(default_factory=list)
    # `duckdb_fatal` stood here until step 14's S14-17: "index" or "other"
    # when `health_status` failed for a DuckDB FATAL alone, which set the
    # page's severity, title and lever. Web publishes no FATAL since then.


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
    """Human age: "3h", "2d 4h"."""
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
        return [("dq_block_missing", "no data_quality block in health")], ages

    for layer, limit in thresholds.items():
        entry = block.get(layer)
        if not isinstance(entry, dict):
            failures.append(
                (f"dq_missing:{layer}", f"{layer}: no freshness reported")
            )
            ages[layer] = None
            continue

        age = entry.get("age_seconds")
        ages[layer] = age
        if age is None:
            failures.append(
                (f"dq_never:{layer}", f"{layer}: never ran successfully")
            )
        elif age > limit:
            failures.append((
                f"dq_stale:{layer}",
                f"{layer}: silent for {_format_age(int(age))}",
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
    thresholds = dict(max_age_s if max_age_s is not None else MIRROR_MAX_AGE_S)
    failures: list[tuple[str, str]] = []
    ages: dict[str, Optional[int]] = {}

    block = (payload or {}).get("mirrors")
    if not isinstance(block, dict):
        return [("mirror_block_missing",
                 "no mirrors block in health")], ages

    # Tables web declares a limit for — the layers Postgres derives on its own
    # signal. Declared there because only web knows its KS_PG_DERIVE; a
    # threshold kept here would page on every deploy that has not switched.
    # Never looser than DECLARED_MAX_AGE_CEILING_S, whatever web says.
    for table, entry in block.items():
        if isinstance(entry, dict) and isinstance(entry.get("max_age_s"), int):
            thresholds.setdefault(
                table, min(entry["max_age_s"], DECLARED_MAX_AGE_CEILING_S))

    for table, limit in thresholds.items():
        entry = block.get(table)
        if not isinstance(entry, dict):
            failures.append(
                (f"mirror_missing:{table}", f"mirror {table}: no freshness reported")
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
                f"mirror {table}: failing, {failed}× in a row",
            ))

        if age is None:
            failures.append(
                (f"mirror_never:{table}", f"mirror {table}: never shipped")
            )
        elif age > limit:
            failures.append((
                f"mirror_stale:{table}",
                f"mirror {table}: not shipped for {_format_age(int(age))}",
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
                 "no alerting block in health")]
    failures = int(block.get("consecutive_transport_failures") or 0)
    if failures >= ALERTING_MAX_CONSECUTIVE_FAILURES:
        return [(
            "alerting_transport_failing",
            f"alerting: {failures} delivery failures in a row — pages may reach nobody",
        )]
    return []


# `check_write_chains`, `check_write_chain_latch` and
# `check_write_chain_precondition` stood here until step 14's S14-11: a
# KS_WRITE_* web did not understand (`write_chain_flag_invalid`, CRITICAL), a
# chain latched against its flag (`write_chain_flag_mismatch`, WARN) and a
# chain's own condition for moving unmet (`write_chain_precondition_unmet`,
# WARN). Web reads no KS_WRITE_* since, and its `write_chains` block publishes
# no `error`, `mismatch` or `unmet_precondition` to judge.


def check_report_ledger_pending(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """A report that went out and whose ledger row is still spooled (chains
    11a/11b, OD-16 (a)): `write_chains.<chain>.pending.count` above zero.

    Warn: nothing is lost and nothing will be sent twice — the gate counts a
    spooled week as sent, and the report's next daily tick drains the spool
    into Postgres. What it says is that Postgres refused three times after a
    delivery, and that the local disk is now the only record of the week until
    the drain. Judged from the published block alone.
    """
    block = (payload or {}).get("write_chains")
    if not isinstance(block, dict):
        return []
    parts = []
    for name, state in sorted(block.items()):
        pending = state.get("pending") if isinstance(state, dict) else None
        if isinstance(pending, dict) and pending.get("count"):
            weeks = ", ".join(pending.get("weeks") or ()) or f"{pending['count']} row(s)"
            parts.append(f"{name}: {weeks}")
    if not parts:
        return []
    return [("report_ledger_pending", "report ledgers spooled: " + "; ".join(parts))]


# The buyers step, published by web as `buyer_sync` (chain 4, PR-1). The step
# runs at most hourly by its watermark, so a success older than an hour and a
# half means one hourly run has already been missed; three consecutive
# failures that are neither KeyCRM nor data errors mean the retry is not going
# to heal it. Warn in both cases: nothing is lost while buyers still land in
# DuckDB and ship to Postgres behind them. Once chain 4 makes this step the
# ONLY writer of buyers, a stall there is a CRITICAL of its own
# (`check_buyer_sync_chain`).
BUYER_SYNC_STALE_S = 90 * 60
BUYER_SYNC_FAILURES = 3

# Chain 4 writes the buyers in Postgres alone (`core/pg_buyers_write.py`), so a
# buyers step that has not succeeded is new customers landing nowhere: no
# mirror behind it, no second writer, and the SMS audience and the search
# index read what it writes. Three hours is two missed hourly runs past the
# WARN above — long enough that a KeyCRM hiccup has had its retries, short
# enough that a day's ~19 new buyers are not a morning's surprise.
# (`BUYER_CHAIN`, the chain's `write_chains` entry this was gated on, went in
# step 14's S14-11: the chain writes Postgres, a constant.)
BUYER_SYNC_CHAIN_STALE_S = 3 * 60 * 60


def check_buyer_sync(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the `buyer_sync` block: a buyers step that stopped succeeding.

    An absent or null block is not judged — an older web publishes none, and a
    web that has not started syncing yet has nothing to say. The age is
    floored at web's start, so a process that has never completed the step is
    as stale as its uptime rather than unknown.
    """
    block = (payload or {}).get("buyer_sync")
    if not isinstance(block, dict):
        return []
    failures = _number(block.get("consecutive_failures")) or 0
    age = _number(block.get("last_ok_age_s"))
    if failures >= BUYER_SYNC_FAILURES:
        return [("buyer_sync_stalled",
                 f"buyer sync: {failures} failures in a row"
                 f" ({block.get('last_error_class') or 'unknown'})")]
    if age is not None and age > BUYER_SYNC_STALE_S:
        # The step is attempted at least hourly, and every ten minutes after a
        # failure. An attempt that is missing or as old as the success means
        # the tick never REACHED the step — it stops earlier, at the orders
        # fetch or write — and blaming the buyers step would send the reader
        # to the wrong log line. Minutes, not `_format_age`: that truncates to
        # hours, and "1h" read against a 90-minute threshold.
        attempt = _number(block.get("last_attempt_age_s"))
        if attempt is None or attempt > BUYER_SYNC_STALE_S:
            return [("buyer_sync_stalled",
                     f"buyer sync: step not reached for {age // 60} min — "
                     "the incremental tick stops before it (orders fetch/write)")]
        return [("buyer_sync_stalled",
                 f"buyer sync: no success for {age // 60} min"
                 f" ({block.get('last_error_class') or 'no error recorded'})")]
    return []


def check_buyer_sync_chain(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the buyers step as the ONLY writer of buyers: CRITICAL.

    Unconditional since step 14's S14-11: chain 4 writes Postgres, a constant,
    so whenever the probe read a `buyer_sync` block it is judged here. Until
    then only when the same payload said the chain wrote Postgres (the
    registry's `mode` under `write_chains.pg_buyers_write`); a web on DuckDB,
    or one publishing no chain entry, was judged by `check_buyer_sync` alone.

    The age is the OLDER of two clocks. `last_ok_age_s` is the process's own
    and is floored at web's start, so on its own every recreate reset a stall
    — a writer dead across deploys under three hours apart never paged, and
    each recreate announced the page resolved. `watermark_age_s` is the stamp
    the step writes to Postgres on every completion and survives the restart;
    when it cannot be read the local clock is all there is (review of PR-3).
    """
    block = (payload or {}).get("buyer_sync")
    if not isinstance(block, dict):
        return []
    ages = [a for a in (_number(block.get("watermark_age_s")),
                        _number(block.get("last_ok_age_s"))) if a is not None]
    if not ages or max(ages) <= BUYER_SYNC_CHAIN_STALE_S:
        return []
    age = max(ages)
    # The WARN's distinction, kept: with no attempt as recent as the stale
    # bound the tick never reached the step, and the error class would send
    # the reader to a buyers log line that was never written.
    attempt = _number(block.get("last_attempt_age_s"))
    if attempt is None or attempt > BUYER_SYNC_STALE_S:
        return [("buyer_sync_stalled_chain",
                 f"buyer sync: step not reached for {age // 60} min — the "
                 "incremental tick stops before it — and chain 4 makes it the "
                 "only writer of buyers")]
    return [("buyer_sync_stalled_chain",
             f"buyer sync: no success for {age // 60} min, and chain 4 makes it "
             f"the only writer of buyers ({block.get('last_error_class') or 'no error recorded'})")]


# Chain 3 writes the orders in Postgres alone (`core/pg_orders_write.py`,
# OD-13 (a)): an order step that fails is orders landing nowhere, with no
# DuckDB copy behind it. Web publishes the step's own state under the chain's
# `write_chains` entry as `sync_step`, judged whenever it is there (until step
# 14's S14-11 only when the same payload said the chain wrote Postgres). Three failures in a row, no success for 15
# minutes, or no attempt for 20 (the tick never reached the step) page: the
# incremental tick runs at most five minutes apart, so each bound is past
# anything the adaptive backoff can produce. Spelled rather than imported —
# nothing under `bot/` may import `core.pg*`, and a test pins the name.
ORDERS_CHAIN = "pg_orders_write"
ORDERS_SYNC_FAILURES = 3
ORDERS_SYNC_STALE_S = 15 * 60
ORDERS_SYNC_UNREACHED_S = 20 * 60
# Except while the tick waits for the scheduler's heavy-job lock
# (`lock_wait_s`): the Sunday full sync, training, the backup and the 05:15
# refresh hold it, and the tick queues behind them before it can reach the
# step. Nobody has measured how long they hold it, so the wait is not given a
# number of its own to fit inside: the clocks are judged as they stood when the
# wait began — a step already stale before it still pages — and the wait
# itself pages past chain 4's bound for its own step, the point past which
# whatever holds the lock is stuck (the chain-3 review).
ORDERS_SYNC_LOCK_WAIT_MAX_S = 90 * 60


def check_orders_sync_chain(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the order step as the only writer of orders: CRITICAL.

    The class of the last error, never its text — the block is public.
    Unconditional since step 14's S14-11: a web publishing a step for the
    chain is judged whatever else its entry says. One publishing none is not
    judged here — `unjudged_keys` holds the page instead. Until then an entry
    whose `mode` was not postgres was not judged: its orders landed in DuckDB
    and the mirror's own watch covered them."""
    chain = ((payload or {}).get("write_chains") or {}).get(ORDERS_CHAIN)
    if not isinstance(chain, dict):
        return []
    step = chain.get("sync_step")
    if not isinstance(step, dict):
        return []
    failures = _number(step.get("consecutive_failures")) or 0
    ok_age = _number(step.get("last_ok_age_s"))
    attempt = _number(step.get("last_attempt_age_s"))
    error = step.get("last_error_class") or "no error recorded"
    if failures >= ORDERS_SYNC_FAILURES:
        return [("orders_sync_failing",
                 f"order sync: {failures} failures in a row ({error}) — chain 3 "
                 "makes it the only writer of orders")]
    wait = _number(step.get("lock_wait_s"))
    if wait is not None:
        if wait > ORDERS_SYNC_LOCK_WAIT_MAX_S:
            return [("orders_sync_failing",
                     f"order sync: waiting {wait // 60} min for the heavy-job "
                     "lock — whatever holds it is stuck — and chain 3 makes it "
                     "the only writer of orders")]
        # The wait excuses its own length and nothing before it.
        ok_age = None if ok_age is None else max(0, ok_age - wait)
        attempt = None if attempt is None else max(0, attempt - wait)
    if attempt is None or attempt > ORDERS_SYNC_UNREACHED_S:
        if ok_age is not None and ok_age > ORDERS_SYNC_UNREACHED_S:
            return [("orders_sync_failing",
                     f"order sync: step not reached for {ok_age // 60} min — the "
                     "incremental tick is not running — and chain 3 makes it the "
                     "only writer of orders")]
        return []
    if ok_age is not None and ok_age > ORDERS_SYNC_STALE_S:
        return [("orders_sync_failing",
                 f"order sync: no success for {ok_age // 60} min ({error}), and "
                 "chain 3 makes it the only writer of orders")]
    return []


# How long a dropped derivation mark may stand before it says the heal is not
# happening. A drop owes nothing — the rows landed, only the signal did not —
# so in a quiet hour the rebuild that covers it is the hourly heartbeat. This is
# `core.pg_derivation.HEARTBEAT` (60 min) plus fifteen for the ten-minute floor,
# the one-minute tick and the heal's own 30 s margin. Written out rather than
# imported: nothing under `bot/` may import `core.pg*`
# (`tests/unit/test_pg_bot_state.py`), and a test pins the two together.
DERIVATION_HEAL_WITHIN_S = 75 * 60

# Marks failing this many times in a row page whatever their age. A mark that
# fails on every landing keeps the latest drop minutes old, so the age alone
# would never see the one failure that is not a blip.
DERIVATION_MARKS_PERSISTENT = 10


def _number(value: object) -> Optional[int]:
    """A published number, or None for null, a bool or anything else."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)


def check_derivation_marks(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the dropped derivation marks: page only where healing is not happening.

    A dropped mark is cleared by the next validated rebuild, so a count above
    zero is usually a drop already on its way out, and paging it would train
    people to swipe. Two shapes are not:

    - **the latest drop is older than a heal can take**
      (`DERIVATION_HEAL_WITHIN_S`): a validated rebuild should have covered it
      by now and has not, so the heal or the derivation itself is broken;
    - **the count has reached `DERIVATION_MARKS_PERSISTENT`**: marks fail on
      every landing, which keeps the latest drop young enough that the first
      rule would never fire.

    Null is never judged — an older web, piggyback, or Postgres unreadable, and
    the derivation's own watermarks already page on the last of those. So with
    no count nothing fires, and with no age only the persistent count can.
    """
    block = (payload or {}).get("derivation")
    if not isinstance(block, dict):
        return []
    count = _number(block.get("marks_dropped_unhealed"))
    if count is None or count <= 0:
        return []
    if count >= DERIVATION_MARKS_PERSISTENT:
        return [("derivation_marks_failing",
                 f"derivation: marks keep failing — {count} in a row")]
    age = _number(block.get("last_mark_drop_age_s"))
    if age is not None and age > DERIVATION_HEAL_WITHIN_S:
        return [("derivation_marks_failing",
                 f"derivation: dropped marks not rebuilt over for "
                 f"{_format_age(age)} — {count}")]
    return []


# `check_derivation_mode` (`derivation_mode_invalid`, a KS_PG_DERIVE web did
# not understand) stood here until step 14's S14-15 removed the variable and
# the `derivation` block's `mode` and `error` with it.


# ─── Reads refused with a 503 ───────────────────────────────────────────────
#
# A router whose engine fails refuses the read, and web counts the refusal per
# surface under `read_fallback_mode.refused` (core/read_fallback.py). Until
# step 14's S14-16 three more checks stood here, over the reads web answered
# from DuckDB: `read_fallback_mode_invalid` (a KS_READ_FALLBACK web did not
# understand), `read_fallback_used` (a read whose engine failed, answered from
# DuckDB: the `read_fallbacks` block) and `read_routed_to_duckdb` (a read
# switch routing to DuckDB uncounted: `misconfigured`, `no_engine`), with the
# `watch:read_fallbacks` row the week before KS_READ_FALLBACK=off was judged
# by (OD-07). No read falls back since S14-00 and no switch routes one since
# S14-07, so S14-16 took the checks with the blocks; the row stays in
# `app.alert_series` as history, and the names are not reused (rule 15).

# The first words of the page's line, which a reader greps the journal by.
READ_REFUSED_LINE = "reads refused with a 503: "

# Surfaces named in one line; the rest are counted. Three details, the format rule.
READ_SURFACES_SHOWN = 3

# How recent a refusal must be to page. Two probe intervals: a refusal is seen by
# the next probe and the one after it, and a page stands only while reads are
# still being refused.
READ_REFUSED_RECENT_S = 30 * 60


def _surfaces(entries: dict) -> str:
    """`dashboard ×2 (last …); traffic ×1 (last …)` — sorted, at most
    `READ_SURFACES_SHOWN`, the rest counted. Tolerant of any shape inside:
    this is a message, and a malformed entry must not cost the page."""
    parts = []
    for surface in sorted(entries, key=str):
        entry = entries[surface] if isinstance(entries[surface], dict) else {}
        count = _number(entry.get("count"))
        last = entry.get("last_at") or "unknown"
        parts.append(f"{surface} ×{'?' if count is None else count} (last {last})")
    shown = parts[:READ_SURFACES_SHOWN]
    if len(parts) > len(shown):
        shown.append(f"+{len(parts) - len(shown)} more")
    return "; ".join(shown)


# `duckdb_fatal_only` stood here until step 14's S14-17: `(kind, message)`
# when web said it was degraded for a DuckDB FATAL and nothing else
# (`degraded_by == ["duckdb_fatal"]`), which named the page "DuckDB index
# short" (CRITICAL, an index short in the file) or paged it WARN (any other
# FATAL, which the reconnect answered past). Web has not opened DuckDB since
# S14-00, and since S14-17 publishes neither `duckdb.fatal` nor that cause.


# Which block of /api/health each payload-judged family is read from (L4-02).
# A probe that read the payload without one of these blocks — or read no
# payload at all — cannot say a page judged from it cleared, so every
# delivered key of the block's families is held (`unjudged_keys`). Until then
# only the keys named below were held: a blip the canary defers (no payload),
# or a payload with `data_quality` null, announced a standing
# `dq_stale:mirror_landing` resolved, and the next probe paged it again as a
# new incident. The `*_block_missing` keys are failures of their own and are
# not listed. `tests/unit/test_canary_blind_probe_holds.py` holds the table
# to the check functions.
BLOCK_KEY_PREFIXES: "tuple[tuple[str, tuple[str, ...]], ...]" = (
    ("data_quality", ("dq_missing:", "dq_never:", "dq_stale:")),
    ("mirrors", ("mirror_missing:", "mirror_never:", "mirror_stale:",
                 "mirror_failing:")),
    ("alerting", ("alerting_transport_failing",)),
    ("derivation", ("derivation_marks_",)),
    ("write_chains", ("report_ledger_pending",)),
)

# The keys a probe judges from its own answer, never from the payload: the
# request's outcome and the TLS probe. A blind probe still judges these.
JUDGED_WITHOUT_PAYLOAD = frozenset({
    "health_unreachable", "health_http", "cert_expiring", "cert_unreachable",
})


def _delivered_canary_keys() -> "list[str]":
    """The canary's delivered, unresolved conditions in this process's Gate.
    Never raises: a probe must not fail over what it holds."""
    try:
        from core.alerting import delivered_conditions

        return sorted(k for k, group in delivered_conditions().items()
                      if group == "canary")
    except Exception:  # noqa: BLE001
        return []


def unjudged_keys(payload: Optional[dict],
                  delivered: "Sequence[str]" = ()) -> "list[str]":
    """The keys this probe could not judge, because it read no block to
    judge them by — web did not answer, or answered without the block.

    The resolve must keep them firing: `resolve_group` clears every
    delivered key a probe did not report, and a probe that read nothing
    cannot say a condition cleared. A standing page read blind by a
    first-time blip the canary holds back (`defer_flaky` — the 05:15 freeze,
    an nginx reload, a 10 s timeout) would be announced resolved and paged
    again as a new incident by the next probe, agent and all. Only these
    keys: `read_refused` (OD-07's; its siblings `read_fallback_used` and
    `read_routed_to_duckdb` went in step 14's S14-16), and the buyers
    step's: every other payload-derived key keeps today's behaviour. (The
    week of silence's tripwire, `duckdb_opened_while_off`, and off's
    held-back verdict, `duckdb_off_held_back`, were held the same way until
    step 14's S14-18 deleted the switch and its block.)

    The buyers step's two keys are held the same way, when the probe read no
    `buyer_sync` block: that step is the only writer of buyers, and a stall
    outlives the 05:15 freeze and every deploy recreate. Read blind, the page
    was announced resolved and paged again as a new incident each time —
    through the WARN beside it as much as through the CRITICAL, since both
    fire for one stall (review of chain 4's merge with OD-07). Until step
    14's S14-11 chain 4's CRITICAL was also held when the probe read no
    `write_chains` entry for the chain, which gated it.

    Chain 3's `orders_sync_failing` the same way, when the probe read no
    `sync_step` under the chain's entry: the order step is the only writer of
    orders, and the Postgres hang that fails it hangs this endpoint too
    (batch-E review). Until S14-11 an entry whose `mode` said duckdb was
    judged, and cleared it; no entry says a mode since.

    And `backup_evidence_stale` (step 14's S14-00) when the probe read no
    `backups` block: a stale drill stands until the next weekly run.

    And, of the `delivered` canary keys (L4-02): every one judged from the
    payload when the probe read none, and every one of a block's families
    (`BLOCK_KEY_PREFIXES`) when the payload carried that block as anything
    but an object."""
    blind = not payload
    payload = payload or {}
    keys = []
    if not isinstance(payload.get("read_fallback_mode"), dict):
        keys.append("read_refused")
    chains = payload.get("write_chains")
    if not isinstance(payload.get("buyer_sync"), dict):
        keys += ["buyer_sync_stalled", "buyer_sync_stalled_chain"]
    # Chain 3's CRITICAL, held the way chain 4's is: the order step is the
    # only writer of orders, and a hung Postgres — the likeliest cause of the
    # page — hangs /api/health too, which is exactly a blind probe. Held
    # whenever the probe read no step to judge (batch-E review; by `sync_step`
    # alone since step 14's S14-11).
    orders = chains.get(ORDERS_CHAIN) if isinstance(chains, dict) else None
    if not (isinstance(orders, dict)
            and isinstance(orders.get("sync_step"), dict)):
        keys.append("orders_sync_failing")
    # The backup evidence (S14-00): the drills run weekly, so a stale marker
    # stands for days, and a probe that read no `backups` block cannot say a
    # drill has run since.
    if not isinstance(payload.get("backups"), dict):
        keys.append("backup_evidence_stale")
    for key in delivered:
        if blind:
            held = key not in JUDGED_WITHOUT_PAYLOAD
        else:
            held = any(key.startswith(prefixes)
                       for block, prefixes in BLOCK_KEY_PREFIXES
                       if not isinstance(payload.get(block), dict))
        if held and key not in keys:
            keys.append(key)
    return keys


def _parse_instant(value: object) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def check_read_refusals(
    payload: Optional[dict], now: Optional[datetime] = None,
) -> "list[tuple[str, str]]":
    """Judge `read_fallback_mode.refused`: reads answered 503 because the
    engine behind them failed (DN-20b).

    A refused read is a page somebody could not open, and nothing else says
    so when the engine behind it is up for everything but one query. Warn
    while one is recent (`READ_REFUSED_RECENT_S`), so the page stands only
    while reads are being refused: a refusal left no wrong number behind to
    explain. A timestamp that cannot be read counts as recent. An absent
    block, or one with no `refused`, is nothing to judge.
    """
    block = (payload or {}).get("read_fallback_mode")
    refused = block.get("refused") if isinstance(block, dict) else None
    if not isinstance(refused, dict) or not refused:
        return []
    reference = now or datetime.now(timezone.utc)
    recent = {}
    for surface, entry in refused.items():
        last = _parse_instant(entry.get("last_at") if isinstance(entry, dict) else None)
        if last is None or (reference - last).total_seconds() <= READ_REFUSED_RECENT_S:
            recent[surface] = entry
    if not recent:
        return []
    return [("read_refused", READ_REFUSED_LINE + _surfaces(recent))]


# Step 13's switch was judged here until step 14's S14-15 deleted it
# (`core/warehouse_cutover.py`) with the `warehouse_writer_mode` and
# `utm_parse` blocks of /api/health: `warehouse_mode_invalid`,
# `warehouse_preconditions_unmet`, `warehouse_way_back_refused`,
# `warehouse_hold_stuck` and `utm_parse_mode_invalid`. A step-14 web
# publishes none of them, and the names are not reused (rule 15).


# ─── The week of silence (OD-17 (a)), until step 14's S14-18 ────────────────
#
# Web refused every open of the DuckDB file under `KS_DUCKDB=off`, counted it
# and published the sites as `duckdb_switch.opened_while_off`; this module
# paged that block (`duckdb_opened_while_off`, CRITICAL), a value of
# `KS_DUCKDB` web did not understand (`duckdb_mode_invalid`, WARN) and an
# image from before step 14 held back on `on` (`duckdb_off_held_back`,
# CRITICAL), and wrote the week's watch (`watch:duckdb_switch` in
# `app.alert_series`) from every probe that read the block. S14-18 deleted the
# switch and the block: nothing in any process opens the file. The names are
# not reused (rule 15), and the watch's row stays as history.


# `(field of /api/health's backups block, what it proves, the script that
# writes its marker, which limit)`. The limits are `core/backup_evidence.py`'s,
# read there, so the canary and the host never disagree about a marker.
BACKUP_EVIDENCE: "tuple[tuple[str, str, str, str], ...]" = (
    ("pitr_drill_age_h", "PITR drill", "deploy/pg_pitr_drill.sh", "drill"),
    ("remote_restore_age_h", "off-site restore drill",
     "deploy/pg_restore_drill.sh --from-remote", "drill"),
    ("pg_offsite_age_h", "Postgres off-site copy", "deploy/pg_offsite.sh", "offsite"),
)


def check_backup_evidence(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge `/api/health`'s `backups` block: a backup marker past its limit.

    The host's PITR drill, its off-site restore drill and the Postgres
    off-site shipment each leave a marker in `data/` on success only
    (`core/backup_evidence.py`), and web publishes their ages in hours.
    Until step 14's S14-00 the two drills were judged by chain 3's
    precondition alone, which paged `write_chain_precondition_unmet`; with
    that gone a drill that quietly stopped running would be seen by nobody,
    since `deploy/offsite_check.sh` reads the shipment's marker alone. Warn:
    nothing is broken now, but the evidence that Postgres can be got back is
    getting old. A null age is a marker that does not exist — the script has
    never passed on this host. An absent block, or one that is not an
    object, is not judged: an older web, or web unable to read the
    directory, which says nothing about the drills."""
    block = (payload or {}).get("backups")
    if not isinstance(block, dict):
        return []
    from core.backup_evidence import DRILL_MAX_AGE_H, OFFSITE_MAX_AGE_H

    limits = {"drill": DRILL_MAX_AGE_H, "offsite": OFFSITE_MAX_AGE_H}
    stale: list[str] = []
    for name, what, script, kind in BACKUP_EVIDENCE:
        if name not in block:
            continue
        age = block.get(name)
        limit = limits[kind]
        if age is None:
            stale.append(f"{what} never passed ({script})")
        elif isinstance(age, (int, float)) and age > limit:
            stale.append(f"{what} {age:.1f} h old, over its {limit} h limit ({script})")
    if not stale:
        return []
    return [("backup_evidence_stale", "Backup evidence stale: " + "; ".join(stale))]


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
    mirror_ages: dict[str, Optional[int]] = {}
    if payload:
        health_status = payload.get("status")
        sync_block = payload.get("sync") or {}
        sync_seconds = sync_block.get("seconds_since_sync")
        if health_status and health_status != "healthy":
            # Web publishes `healthy` since step 14's S14-17; a web that says
            # otherwise is one this canary does not know, and is paged as it
            # always was. The DuckDB FATAL alone (`duckdb_fatal_only`) had a
            # page of its own until then.
            causes = payload.get("degraded_by")
            named = (f" ({', '.join(map(str, causes))})"
                     if isinstance(causes, list) and causes else "")
            fail("health_status", f"status={health_status}{named}")
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

        # A read refused with a 503, recently. Warn: an outage somebody saw,
        # which is what a refusal chooses over a wrong number.
        refused_failures = check_read_refusals(payload, now)
        for key, message in refused_failures:
            fail(key, message)
        if refused_failures and severity == "ok":
            severity = "warn"

        # Derivation marks dropped and demonstrably not being healed. Warn:
        # every row landed, and what is owed is a rebuild.
        marks_failures = check_derivation_marks(payload)
        for key, message in marks_failures:
            fail(key, message)
        if marks_failures and severity == "ok":
            severity = "warn"

        # A backup marker past its limit: warn. The drills prove Postgres can
        # be got back, and nothing else watches the two of them (S14-00).
        backup_failures = check_backup_evidence(payload)
        for key, message in backup_failures:
            fail(key, message)
        if backup_failures and severity == "ok":
            severity = "warn"
        # A delivered report whose ledger row is spooled: warn.
        ledger_failures = check_report_ledger_pending(payload)
        for key, message in ledger_failures:
            fail(key, message)
        if ledger_failures and severity == "ok":
            severity = "warn"
        # The buyers step: warn. Its own reasons are in check_buyer_sync.
        buyer_failures = check_buyer_sync(payload)
        for key, message in buyer_failures:
            fail(key, message)
        if buyer_failures and severity == "ok":
            severity = "warn"
        # And under chain 4, where it is the only writer of buyers: page.
        chain_buyer_failures = check_buyer_sync_chain(payload)
        for key, message in chain_buyer_failures:
            fail(key, message)
        if chain_buyer_failures:
            severity = "critical"
        # Chain 3's order step, the only writer of orders under it: page.
        orders_failures = check_orders_sync_chain(payload)
        for key, message in orders_failures:
            fail(key, message)
        if orders_failures:
            severity = "critical"

    if cert_err:
        fail("cert_unreachable", f"cert check failed: {cert_err}")
        if severity == "ok":
            severity = "warn"
    elif cert_days is not None and cert_days < cert_warn_days:
        fail("cert_expiring", f"cert expires in {cert_days}d")
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
        unjudged_keys=unjudged_keys(payload, delivered=_delivered_canary_keys()),
    )


# ─── Alert formatting + dedup state machine ─────────────────────────────────

# What the reader is supposed to *do*, per failing key. Rule 1 of the alerts
# charter: a page that does not name a lever is a page that trains people to
# swipe. Ordered by how much the answer differs — the first matching key wins,
# because "the dashboard is unreachable" outranks "a layer is stale" when both
# are true.
_ACTIONS: tuple[tuple[str, str], ...] = (
    ("health_unreachable",
     "curl /api/health from the VPS — app vs nginx/TLS. Restart is the last lever"),
    ("health_http", "Read web's log for the failing request — not a network issue"),
    ("health_status", "/api/health's degraded_by names what degraded; read web's log for it"),
    ("cert_expiring", "Check certbot on the host — auto-renew broke"),
    ("cert_unreachable", "TLS handshake fails: nginx or the network, not the app"),
    ("mirror_",
     "Read meta.mirror_state's last_error: bronze.orders is chain 3's order step (write_chains.pg_orders_write.sync_step names the error class), the rest the derivation; the writer's next success clears it"),
    ("dq_", "Check /api/jobs — nothing is verifying the warehouse meanwhile"),
    ("alerting_", "Consecutive Telegram delivery failures — check web's log"),
    ("derivation_marks_",
     "Read meta.derivation_signal's last_error in meta.mirror_state, then meta.derivation_runs"),
    ("orders_sync_failing",
     "Chain 3: nothing else writes orders. write_chains.pg_orders_write.sync_step names the error class (a long lock_wait_s: /api/jobs shows the heavy job); Postgres back, the 24 h window refills"),
    ("buyer_sync_stalled_chain",
     "Chain 4: nothing else writes buyers. 'step not reached' means the tick stops first: grep 'Incremental sync'; else buyer_sync names the error class, grep 'Buyer'"),
    ("buyer_sync_",
     "grep web's log for 'Buyer' and 'Incremental sync'; 'step not reached' means the whole tick stops"),
    ("backup_evidence_stale",
     "On the host, run the script the line names; it writes its marker in data/ "
     "on success only, and the next probe clears this"),
    ("report_ledger_pending",
     "Nothing to resend: the next daily tick drains data/report-ledger-pending into the ledger, under either flag; grep -i 'report ledger' in web's log"),
    # Last: when an engine is down its own key names the cause, and a refusal
    # is what that cause cost the pages.
    ("read_refused",
     "grep web's log for 'read refused': those pages answered 503. Fix the engine it "
     "names; clears 30 min after the last"),
)

# `_FATAL_ACTIONS` stood here until step 14's S14-17: the lever of a
# `health_status` page for a DuckDB FATAL alone — rebuild the indexes by a
# compaction for `index`, grep web's log for `other`. It went with the FATAL,
# and the compaction itself in S14-18.


def _what_to_do(result: CanaryResult) -> Optional[str]:
    """The single most useful lever for this result, or None."""
    for prefix, action in _ACTIONS:
        if any(k.startswith(prefix) for k in result.failure_keys):
            return action
    return None


# "DOWN" only when it is: the three keys that mean web did not answer, or
# answered that it is not healthy. Every other CRITICAL — a certificate about
# to expire, a write chain whose flag web cannot read, a warehouse switch held
# back — is a site that serves, and a title saying it does not sends the reader
# to the wrong lever before they reach the right one. A result with no keys is
# read as an outage, as every CRITICAL was before keys existed.
_OUTAGE_KEYS: tuple[str, ...] = ("health_unreachable", "health_http", "health_status")

# A CRITICAL that is not an outage, named for what it is. First match wins.
# Empty since step 14's S14-18: its two entries were the DuckDB switch's,
# "DuckDB switch held back" and "DuckDB opened while off". Every other
# CRITICAL that is not an outage reads "Dashboard critical".
_CRITICAL_TITLES: tuple[tuple[str, str], ...] = ()


def _title(result: CanaryResult) -> str:
    if result.severity != "critical":
        return "Dashboard warning"
    keys = result.failure_keys
    # A `health_status` for a DuckDB FATAL alone was web answering that its
    # store was damaged, not web down, titled "DuckDB index short" for an
    # index, until step 14's S14-17 took the FATAL out of the payload.
    outage = [key for key in keys if key in _OUTAGE_KEYS]
    if not keys or outage:
        return "Dashboard DOWN"
    for prefix, title in _CRITICAL_TITLES:
        if any(key.startswith(prefix) for key in keys):
            return title
    return "Dashboard critical"


def format_alert(result: CanaryResult, dashboard_url: str) -> str:
    """Build a Telegram HTML message for a failing result."""
    icon = "\U0001f6a8" if result.severity == "critical" else "⚠️"
    title = _title(result)

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
        extras.append(f"cert {result.cert_days_remaining}d")
    if result.http_code is not None and result.http_code != 200:
        extras.append(f"http {result.http_code}")
    if extras:
        lines.append("<i>" + html.escape(" · ".join(extras)) + "</i>")
    return "\n".join(lines)
