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
    "reconciliation": 30 * 3600,  # 24h cycle + 6h grace
    # The same 05:30 job's Postgres half, compared against the same KeyCRM
    # snapshot, and so the same limit — the digest's too. It is a layer of its
    # own so that a Postgres half which stops cannot hide behind a fresh DuckDB
    # one, and until DN-21 only the 09:00 digest would say so.
    # It is meant to be the comparison against the source that is left once
    # DuckDB stops being fed. It is not independent of DuckDB yet: the job
    # runs it only after the DuckDB extraction succeeded, inside the same try,
    # and journals it in DuckDB's data_quality_runs, which is where the age
    # /api/health publishes comes from. So a DuckDB failure silences it too,
    # and since DN-21 that pages under this key as well as `reconciliation`.
    # Decoupling it belongs with step 13.
    # DN-21's opt-in rested on what the stage-4 soak checked on 18.09: a
    # successful run on each of the 14 days before that, none CRITICAL. That
    # counted calendar days, not the silence this limit measures; the check
    # asks both now (deploy/stage4_soak/20_reconciliation_pg_history.sql).
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
    # What this probe read for the watch over reads served from DuckDB
    # (OD-07): True none, False one, None no block read. See read_fallbacks_clean.
    read_fallbacks_clean: Optional[bool] = None
    # How long the web process this probe read had been running, from
    # `uptime_seconds`; None when no payload said. The watch uses it to tell
    # the process it read last time from a new one (see record_watch).
    web_uptime_s: Optional[float] = None
    # Keys this probe could not judge, because it read no block to judge them
    # by: the resolve keeps them firing (see unjudged_keys).
    unjudged_keys: list[str] = field(default_factory=list)


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


def check_write_chains(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the `write_chains` block: a KS_WRITE_* value web did not understand.

    Critical, unlike the derivation mode: that chain's writers raise on every
    call — an expense typed on the form fails, a stock sync stops — and its
    tables are neither shipped nor compared until the value is fixed. An absent
    block is not a failure; an older web publishes none.
    """
    block = (payload or {}).get("write_chains")
    if not isinstance(block, dict):
        return []
    bad = {name: state.get("error") for name, state in block.items()
           if isinstance(state, dict) and state.get("error")}
    if not bad:
        return []
    return [("write_chain_flag_invalid",
             "write chains: " + "; ".join(f"{n}: {e}" for n, e in sorted(bad.items())))]


def check_write_chain_latch(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the latch against the flag: a chain that already writes Postgres
    while its KS_WRITE_* says otherwise (DN-06, owner decision OD-19 (a)).

    Warn, not critical: nothing is failing. The writes go where the rows are,
    which is the safe answer and the deliberate one — but somebody has edited
    a variable expecting a rollback that did not happen, and the shipper is
    stamping those tables failing meanwhile. The only way back is
    `scripts/chain_copy_back.py`.

    Judged from the published block alone, and not from the marker files —
    which this container *can* see, since `docker-compose.yml` mounts the same
    `./data` into both. It must not: the question is where **web's** writes are
    going, web is the process that answers it, and a bot reading the directory
    itself would be a second opinion about a decision it never takes part in.
    Nothing under `bot/` may import `core.pg*` either, so the Postgres copy is
    out of reach here in any case.
    """
    block = (payload or {}).get("write_chains")
    if not isinstance(block, dict):
        return []
    owned = {name: state.get("latched_at") or "an unknown time"
             for name, state in block.items()
             if isinstance(state, dict) and state.get("mismatch")}
    if not owned:
        return []
    return [("write_chain_flag_mismatch",
             "write chains own Postgres against their flag: "
             + "; ".join(f"{n} since {t}" for n, t in sorted(owned.items())))]


def check_write_chain_precondition(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge each chain's own condition for moving (`unmet_precondition`).

    Two states, one warning. Unlatched, the chain is HELD on DuckDB although
    its KS_WRITE_* says postgres: safe — every consumer reads the same
    "duckdb" — but somebody believes the chain moved. Latched, it keeps
    writing Postgres (OD-19 (a)) while the condition that made that readable
    has gone, so what it writes is not what the page shows. Warn, not page:
    nothing is lost in either, and the lever is one variable. Judged from the
    published block alone, `check_write_chain_latch`'s reason.
    """
    block = (payload or {}).get("write_chains")
    if not isinstance(block, dict):
        return []
    parts = []
    for name, state in sorted(block.items()):
        if not isinstance(state, dict) or not state.get("unmet_precondition"):
            continue
        env = state.get("env") or "its KS_WRITE_*"
        where = (f"{name} writes Postgres" if state.get("mode") == "postgres"
                 else f"{name} held on DuckDB despite {env}=postgres")
        parts.append(f"{where}: {state['unmet_precondition']}")
    if not parts:
        return []
    return [("write_chain_precondition_unmet", "write chains: " + "; ".join(parts))]


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
# enough that a day's ~19 new buyers are not a morning's surprise. Spelled
# rather than imported: nothing under `bot/` may import `core.pg*`, and a test
# pins the name to the chain's.
BUYER_CHAIN = "pg_buyers_write"
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

    Only when the same payload says chain 4 writes Postgres — the registry's
    `mode`, which a latch sets whatever the flag says — so a web still on
    DuckDB, or one publishing no chain block, is judged by `check_buyer_sync`
    alone.

    The age is the OLDER of two clocks. `last_ok_age_s` is the process's own
    and is floored at web's start, so on its own every recreate reset a stall
    — a writer dead across deploys under three hours apart never paged, and
    each recreate announced the page resolved. `watermark_age_s` is the stamp
    the step writes to Postgres on every completion and survives the restart;
    when it cannot be read the local clock is all there is (review of PR-3).
    """
    block = (payload or {}).get("buyer_sync")
    chain = ((payload or {}).get("write_chains") or {}).get(BUYER_CHAIN)
    if not isinstance(block, dict) or not isinstance(chain, dict):
        return []
    if chain.get("mode") != "postgres":
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


def check_derivation_mode(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the `derivation` block: a KS_PG_DERIVE value web did not understand.

    Not a liveness signal, so an absent block is not a failure — a web image
    older than the block publishes none. What it catches is a typo: web falls
    back to piggyback, and without this the only trace is one log line while
    whoever set the variable believes the own derivation is running.
    """
    block = (payload or {}).get("derivation")
    if isinstance(block, dict) and block.get("error"):
        return [("derivation_mode_invalid",
                 f"derivation: {block['error']}")]
    return []


def check_read_fallback_mode(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the `read_fallback_mode` block: a KS_READ_FALLBACK web did not
    understand (DN-20a).

    Warn, as for the derivation mode: web runs as `duckdb`, which is what it
    did before the variable existed, so nothing is failing — but whoever set
    it believes fallbacks are refused, and after step 13 that belief is the
    difference between an error page and frozen numbers. It must not stop web
    instead: web is the only process that syncs orders. An absent block is
    not a failure; an older web publishes none.
    """
    block = (payload or {}).get("read_fallback_mode")
    if isinstance(block, dict) and block.get("error"):
        return [("read_fallback_mode_invalid",
                 f"read fallback: {block['error']}")]
    return []


# ─── Reads answered from DuckDB (OD-07) ─────────────────────────────────────
#
# KS_READ_FALLBACK=off may be set only after a covered, clean week with no read
# answered from DuckDB (owner decision OD-07 (a), 2026-09-30). The counters that
# say so live in web's process and `/api/health` publishes them as
# `read_fallbacks`; every deploy recreates web, which empties them and takes its
# log with it, so "a clean week" could not be measured across a deploy by
# grepping. The canary is the process that outlives web's, so the evidence is
# made here and journaled in Postgres: a page whenever the block is non-empty,
# and a watch row saying since when every probe read it empty.
#
# A counter is not the only way a read reaches DuckDB. A read switch naming an
# engine web has no address for (`read_fallback_mode.misconfigured`), or the
# cohorts' switch not naming ClickHouse (`no_engine`), serves DuckDB on every
# request and counts nothing — and `off` refuses every such read. So under
# `duckdb` those page too, as `read_routed_to_duckdb`, and the watch reads them
# as not clean.

# The first words of the pages' lines. The soak check (deploy/stage4_soak/
# 22_f1_read_fallbacks.sql) reads the line back out of a page by the words the
# two share; a test holds the two equal.
READ_FALLBACK_LINE = "reads served from DuckDB: "
READ_ROUTED_LINE = "reads served from DuckDB uncounted: "
READ_REFUSED_LINE = "reads refused with a 503: "

# The row the canary keeps in `app.alert_series` for its watch over the block
# (`core.alert_archive.record_watch`). An event-kind row, never a condition:
# nothing resolves it, escalates it or digests it.
READ_FALLBACK_WATCH_KEY = "watch:read_fallbacks"

# The longest a web process may go unread before it is replaced, and the watch
# still hold. The counters cover a process from its start, so a probe that reads
# the SAME process as the last one reads everything since, however long the bot
# was away; only a process that ended between two probes loses what it counted
# after the last. That tail is at most the time from the last probe to the new
# process's start (`uptime_seconds`, core.alert_archive): up to one probe
# interval plus the stop-to-start of a deploy, or of the Sunday compaction —
# neither the bot's own restart nor web's startup before it answers counts.
# The soak check also calls the watch dead when nobody has written it for this
# long, and a test holds the two equal.
READ_FALLBACK_WATCH_GAP_S = 35 * 60

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


def check_read_fallbacks(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the `read_fallbacks` block: any read this web process answered
    from DuckDB because the engine it was sent to failed (DN-20a).

    Warn, and on the block being non-empty, not on a count moving: the
    counters are per process and only a restart empties them, so the page
    stands until web restarts — a fallback the soak before KS_READ_FALLBACK=off
    has to explain (OD-07) should not clear itself because nobody looked. The
    key is the condition alone; surfaces and counts ride the message.

    A refusal under `off` is not a fallback — it served nothing from DuckDB —
    and is published elsewhere (`read_fallback_mode.refused`), judged by
    `check_read_refusals`. An absent block is not a failure; an older web
    publishes none.
    """
    block = (payload or {}).get("read_fallbacks")
    if not isinstance(block, dict) or not block:
        return []
    return [("read_fallback_used", READ_FALLBACK_LINE + _surfaces(block))]


def _routed_to_duckdb(payload: Optional[dict]) -> "list[str]":
    """Every read web routes to DuckDB with nothing to count, as web names
    them: `read_fallback_mode.misconfigured` (a switch naming an engine with
    no address) and `no_engine` (the cohorts' switch not naming ClickHouse).

    Under `duckdb` only. Under `off` those reads are refused, not served —
    each refusal is counted and paged as `read_refused` — so they are not
    reads from DuckDB. An absent or malformed list is none; an older web
    publishes no `no_engine`."""
    block = (payload or {}).get("read_fallback_mode")
    if not isinstance(block, dict) or block.get("mode") == "off":
        return []
    lines = []
    for field in ("misconfigured", "no_engine"):
        entries = block.get(field)
        if isinstance(entries, list):
            lines.extend(str(entry) for entry in entries if entry)
    return lines


def check_read_routes(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the reads web serves from DuckDB by configuration: every one of
    them, on every request, with nothing counted (`_routed_to_duckdb`).

    Warn, its own key: a fallback is an engine that failed and a log line to
    read; this is a switch whose address is missing — or the cohorts' switch
    not naming ClickHouse, their only engine under `off` — and the lever is
    `.env`. Both are what the week before KS_READ_FALLBACK=off must not
    contain (OD-07), since `off` would answer every such read with a 503. It
    stands until web restarts with the address, the one moment web reads
    the environment again."""
    lines = _routed_to_duckdb(payload)
    if not lines:
        return []
    shown = lines[:READ_SURFACES_SHOWN]
    if len(lines) > len(shown):
        shown.append(f"+{len(lines) - len(shown)} more")
    return [("read_routed_to_duckdb", READ_ROUTED_LINE + "; ".join(shown))]


def read_fallbacks_clean(payload: Optional[dict]) -> Optional[bool]:
    """What this probe can say for the watch: True when no read was served
    from DuckDB, False when one was — a counted fallback, or a read the
    configuration routes there uncounted (`_routed_to_duckdb`) — and None
    when it read no `read_fallbacks` block at all (web did not answer, or is
    older than the block), which says nothing either way and is not
    written."""
    block = (payload or {}).get("read_fallbacks")
    if not isinstance(block, dict):
        return None
    return not block and not _routed_to_duckdb(payload)


def unjudged_keys(payload: Optional[dict]) -> "list[str]":
    """The OD-07 keys this probe could not judge, because it read no block to
    judge them by — web did not answer, or answered without the block.

    The resolve must keep them firing: `resolve_group` clears every
    delivered key a probe did not report, and a probe that read nothing
    cannot say a condition cleared. `read_fallback_used` stands for days by
    design, until web restarts, so a first-time blip the canary holds back
    (`defer_flaky` — the 05:15 freeze, an nginx reload, a 10 s timeout)
    would announce it resolved and the next probe page it again as a new
    incident, agent and all. Only these keys, and the buyers step's: every
    other payload-derived key keeps today's behaviour.

    The buyers step's two keys are held the same way: both when the probe read
    no `buyer_sync` block, and chain 4's CRITICAL when it read no entry for the
    chain either. Under chain 4 that step is the only writer of buyers, and a
    stall outlives the 05:15 freeze and every deploy recreate. Read blind, the
    page was announced resolved and paged again as a new incident each time —
    through the WARN beside it as much as through the CRITICAL, since both
    fire for one stall (review of chain 4's merge with OD-07)."""
    payload = payload or {}
    keys = []
    if not isinstance(payload.get("read_fallbacks"), dict):
        keys.append("read_fallback_used")
    if not isinstance(payload.get("read_fallback_mode"), dict):
        keys += ["read_routed_to_duckdb", "read_refused"]
    chains = payload.get("write_chains")
    if not isinstance(payload.get("buyer_sync"), dict):
        keys += ["buyer_sync_stalled", "buyer_sync_stalled_chain"]
    elif not (isinstance(chains, dict) and isinstance(chains.get(BUYER_CHAIN), dict)):
        keys.append("buyer_sync_stalled_chain")
    return keys


def web_uptime_s(payload: Optional[dict]) -> Optional[float]:
    """`uptime_seconds` as a number, or None: how long the web process this
    probe read had been running. A bool, a negative or anything else is
    None — the watch then judges the gap alone, as it did before."""
    value = (payload or {}).get("uptime_seconds")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if value >= 0 else None


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
    """Judge `read_fallback_mode.refused`: reads answered 503 under
    KS_READ_FALLBACK=off (DN-20b).

    Its own key, never `read_fallback_used`: a refusal served nothing from
    DuckDB, which is the whole point of `off`, and counting it as a fallback
    would fail the very soak that licenses the flip. But a refused read is a
    page somebody could not open, and nothing else says so when the engine
    behind it is up for everything but one query. Warn while one is recent
    (`READ_REFUSED_RECENT_S`), so the page stands only while reads are being
    refused — unlike a fallback, a refusal left no wrong number behind to
    explain. A timestamp that cannot be read counts as recent. Published under
    `off`, and under `duckdb` only for a read of a table a write chain owns,
    which has no fallback in either mode (`read_fallback.chain_refusal`).
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


def check_warehouse_writer_mode(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the `warehouse_writer_mode` block: a KS_WRITE_WAREHOUSE web did
    not understand (DN-28).

    Warn, the read-fallback mode's reason: web runs as `duckdb`, so before a
    flip nothing is failing — but whoever set `postgress` believes DuckDB has
    stopped. After a flip the same typo is the way back (a full DuckDB
    rebuild, the checks held), and web then also publishes `value_understood`
    under `preconditions_unmet`, which `check_warehouse_preconditions` pages
    with the way-back lever. An absent block is not a failure; an older web
    publishes none.
    """
    block = (payload or {}).get("warehouse_writer_mode")
    if isinstance(block, dict) and block.get("error"):
        return [("warehouse_mode_invalid",
                 f"warehouse writer: {block['error']}")]
    return []


def check_warehouse_preconditions(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the `warehouse_writer_mode` block: KS_WRITE_WAREHOUSE=postgres
    with a precondition of the switch unmet (DN-29).

    Pages, unlike the typo: web runs as duckdb (OD-09 (b) — never a raise, it
    is the only syncer), so the numbers keep coming — but somebody deployed
    the switch that retires DuckDB's derivation, the owner's decision behind
    it has not taken effect, and the flip is the moment to hear that, not the
    next audit. And after a flip it is the way back itself: a full DuckDB
    rebuild, a new `since` on the next start under postgres, the warehouse
    group closed again — which is why the lever says so first. So is a value
    web did not understand, after a flip (`value_understood`): the same way
    back, and the message names the value as web read it. The keys name what
    to do; the details are on the admin status page. An absent block or field
    is not a failure; an older web publishes none."""
    block = (payload or {}).get("warehouse_writer_mode")
    if not isinstance(block, dict):
        return []
    unmet = block.get("preconditions_unmet")
    if not isinstance(unmet, list) or not unmet:
        return []
    value = block.get("value")
    value = value if isinstance(value, str) and value else "postgres"
    return [("warehouse_preconditions_unmet",
             f"KS_WRITE_WAREHOUSE={value} ran as duckdb, unmet: "
             + ", ".join(str(key) for key in unmet))]


# How long the way back from KS_WRITE_WAREHOUSE=postgres may hold the DuckDB
# checks down before we say so. It ends in its first full DuckDB tick — a
# rebuild and a parse, minutes even after a compaction emptied the verdicts —
# so two hours is not a slow way back: it is a UTM parse raising tick after
# tick, or a full tick whose parse raised with no dirty tick after it.
WAREHOUSE_HOLD_WARN_S = 2 * 3600


def check_warehouse_hold(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the `warehouse_writer_mode` block: the way back's hold standing
    past `WAREHOUSE_HOLD_WARN_S` (DN-29).

    Warn: web serves and DuckDB derives — what is down is the five DuckDB
    checks over Silver, Gold and UTM and the three mirror comparisons, which
    the hold keeps down until a full tick validates and a UTM parse finishes.
    Nothing else says so: the tick logs a failing parse at WARNING and still
    reports success. An absent block or field is not a failure; an older web
    publishes none."""
    block = (payload or {}).get("warehouse_writer_mode")
    if not isinstance(block, dict) or block.get("held") is not True:
        return []
    age = block.get("held_for_s")
    if not isinstance(age, int) or isinstance(age, bool) or age < WAREHOUSE_HOLD_WARN_S:
        return []
    return [("warehouse_hold_stuck",
             f"warehouse way back: the DuckDB checks over Silver, Gold and UTM "
             f"held down for {_format_age(age)}")]


def check_utm_parse_mode(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the `utm_parse` block: a KS_UTM_PARSE web ran as `duckdb` instead
    of what was set — a value it did not understand, or `postgres` without
    KS_PG_DERIVE=own (DN-19).

    Warn, the derivation mode's reason: web runs as `duckdb`, which is what it
    did before the variable existed, so /traffic is fed exactly as before —
    but whoever set it believes Postgres parses the table, and a soak believed
    to be running is not. It must not stop web instead: web is the only
    process that syncs orders. An absent block is not a failure; an older web
    publishes none.
    """
    block = (payload or {}).get("utm_parse")
    if isinstance(block, dict) and block.get("error"):
        return [("utm_parse_mode_invalid",
                 f"UTM parse: {block['error']}")]
    return []


def check_goals_history_mode(payload: Optional[dict]) -> "list[tuple[str, str]]":
    """Judge the `goals_history` block: a KS_GOALS_HISTORY web does not
    understand (chain 7b).

    Critical, unlike the siblings above, and for `write_chain_flag_invalid`'s
    reason: those run as duckdb and nothing fails, while this one raises at
    every goal history read on purpose — counting a different set of orders
    in silence is the worse failure — so the dashboard's goal widget and
    every `/goals/*` page answer 500 and the Monday job fails, and none of
    those pages anybody by itself. An absent block is not a failure; an older
    web publishes none.
    """
    block = (payload or {}).get("goals_history")
    if isinstance(block, dict) and block.get("error"):
        return [("goals_history_mode_invalid",
                 f"goal history: {block['error']}")]
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
        fail("health_unreachable", f"health request failed: {http_err}")
        severity = "critical"
    elif http_code != 200:
        fail("health_http", f"health returned HTTP {http_code}")
        severity = "critical"

    health_status = None
    sync_seconds = None
    dq_ages: dict[str, Optional[int]] = {}
    mirror_ages: dict[str, Optional[int]] = {}
    read_fallbacks_seen: Optional[bool] = None
    uptime_seen: Optional[float] = None
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

        # A derivation setting web could not read. Warn: nothing is broken —
        # piggyback is today's behaviour — but a soak believed to be running
        # is not.
        derivation_failures = check_derivation_mode(payload)
        for key, message in derivation_failures:
            fail(key, message)
        if derivation_failures and severity == "ok":
            severity = "warn"

        # A KS_READ_FALLBACK web could not read. Warn, the derivation mode's
        # reason: today's behaviour is running, and somebody believes it is not.
        fallback_mode_failures = check_read_fallback_mode(payload)
        for key, message in fallback_mode_failures:
            fail(key, message)
        if fallback_mode_failures and severity == "ok":
            severity = "warn"

        # A read answered from DuckDB in this web process (OD-07). Warn: the
        # page served, with numbers from the store it was leaving.
        fallback_failures = check_read_fallbacks(payload)
        for key, message in fallback_failures:
            fail(key, message)
        if fallback_failures and severity == "ok":
            severity = "warn"
        read_fallbacks_seen = read_fallbacks_clean(payload)
        uptime_seen = web_uptime_s(payload)

        # A read the configuration serves from DuckDB, uncounted (OD-07).
        # Warn: the page served, and `off` would refuse it.
        routed_failures = check_read_routes(payload)
        for key, message in routed_failures:
            fail(key, message)
        if routed_failures and severity == "ok":
            severity = "warn"

        # A read refused with a 503 under KS_READ_FALLBACK=off, recently.
        # Warn: an outage somebody saw, which is what `off` chose over a
        # wrong number — and a different signal from a fallback.
        refused_failures = check_read_refusals(payload, now)
        for key, message in refused_failures:
            fail(key, message)
        if refused_failures and severity == "ok":
            severity = "warn"

        # A KS_WRITE_WAREHOUSE web could not read. Warn, for the same reason:
        # duckdb is running, and somebody believes something else is.
        warehouse_mode_failures = check_warehouse_writer_mode(payload)
        for key, message in warehouse_mode_failures:
            fail(key, message)
        if warehouse_mode_failures and severity == "ok":
            severity = "warn"

        # KS_WRITE_WAREHOUSE=postgres held back by an unmet precondition.
        # Pages: the switch somebody deployed did not happen (OD-09 (b)).
        warehouse_unmet = check_warehouse_preconditions(payload)
        for key, message in warehouse_unmet:
            fail(key, message)
        if warehouse_unmet:
            severity = "critical"

        # The way back holding the DuckDB checks down for hours. Warn: web
        # serves and DuckDB derives, and the checks are what is missing.
        hold_failures = check_warehouse_hold(payload)
        for key, message in hold_failures:
            fail(key, message)
        if hold_failures and severity == "ok":
            severity = "warn"

        # A KS_UTM_PARSE web ran as duckdb instead. Warn, for the same reason:
        # the copy /traffic has always had is still shipped.
        utm_mode_failures = check_utm_parse_mode(payload)
        for key, message in utm_mode_failures:
            fail(key, message)
        if utm_mode_failures and severity == "ok":
            severity = "warn"

        # A KS_GOALS_HISTORY web does not understand: every goal history read
        # raises, so this pages rather than warns.
        goals_history_failures = check_goals_history_mode(payload)
        for key, message in goals_history_failures:
            fail(key, message)
        if goals_history_failures:
            severity = "critical"

        # Derivation marks dropped and demonstrably not being healed. Warn:
        # every row landed, and what is owed is a rebuild.
        marks_failures = check_derivation_marks(payload)
        for key, message in marks_failures:
            fail(key, message)
        if marks_failures and severity == "ok":
            severity = "warn"

        # A write chain's flag web did not understand: that chain's writers
        # raise on every call, so this pages rather than warns.
        chain_failures = check_write_chains(payload)
        for key, message in chain_failures:
            fail(key, message)
        if chain_failures:
            severity = "critical"

        # A chain that has already written Postgres against a flag that says
        # otherwise. Warn: the writes are going to the right store, and what
        # is wrong is that somebody believes they are not.
        latch_failures = check_write_chain_latch(payload)
        for key, message in latch_failures:
            fail(key, message)
        if latch_failures and severity == "ok":
            severity = "warn"

        # A chain's own condition for moving that does not hold — held on
        # DuckDB against its flag, or latched while its readers are not.
        precondition_failures = check_write_chain_precondition(payload)
        for key, message in precondition_failures:
            fail(key, message)
        if precondition_failures and severity == "ok":
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
        read_fallbacks_clean=read_fallbacks_seen,
        web_uptime_s=uptime_seen,
        unjudged_keys=unjudged_keys(payload),
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
    ("health_status", "/api/health names what degraded; a restart won't fix a migration"),
    ("cert_expiring", "Check certbot on the host — auto-renew broke"),
    ("cert_unreachable", "TLS handshake fails: nginx or the network, not the app"),
    ("warehouse_preconditions_unmet",
     "After a flip this IS the way back (full DuckDB rebuild). Meet each cutover.unmet on /api/warehouse/status or unset KS_WRITE_WAREHOUSE; recreate web"),
    ("mirror_", "Check meta.mirror_state and web's log; the mirror re-ships itself"),
    ("dq_", "Check /api/jobs — nothing is verifying the warehouse meanwhile"),
    ("alerting_", "Consecutive Telegram delivery failures — check web's log"),
    ("derivation_marks_",
     "Read meta.derivation_signal's last_error in meta.mirror_state, then meta.derivation_runs"),
    ("buyer_sync_stalled_chain",
     "Chain 4: nothing else writes buyers. 'step not reached' means the tick stops first: grep 'Incremental sync'; else buyer_sync names the error class, grep 'Buyer'"),
    ("buyer_sync_",
     "grep web's log for 'Buyer' and 'Incremental sync'; 'step not reached' means the whole tick stops"),
    ("write_chain_flag_mismatch",
     "Set the named KS_WRITE_* back to postgres, or run scripts/chain_copy_back.py to hand the tables back"),
    ("read_fallback_mode_invalid",
     "Set KS_READ_FALLBACK to duckdb or off in .env, then recreate web"),
    ("warehouse_mode_invalid",
     "Set KS_WRITE_WAREHOUSE to duckdb or postgres in .env, then recreate web"),
    ("warehouse_hold_stuck",
     "grep web's log for 'UTM layer refresh failed'; then POST /api/warehouse/refresh — a full tick whose parse finishes ends it"),
    ("write_chain_precondition_unmet",
     "Set the read flag the message names to postgres, then docker compose up -d web"),
    ("utm_parse_mode_invalid",
     "Set KS_UTM_PARSE to duckdb, or to postgres with KS_PG_DERIVE=own, in .env; then recreate web"),
    ("goals_history_mode_invalid",
     "Set KS_GOALS_HISTORY to bridge or silver (or remove it) in .env, then recreate web; every goal read fails until then"),
    # Last: when an engine is down its own key names the cause, and a fallback
    # or a refusal is what that cause cost the pages. A route is a cause of
    # its own, and its lever is `.env`, so it goes first of the three.
    ("read_routed_to_duckdb",
     "Give web the address the line names in .env (cohorts: KS_READ_COHORTS="
     "clickhouse and KS_CH_URL), then up -d web; off would 503 these reads"),
    ("read_fallback_used",
     "grep web's log for 'falling back to DuckDB' at each last; explain it before "
     "KS_READ_FALLBACK=off. Clears when web restarts"),
    ("read_refused",
     "grep web's log for 'read refused': those pages answered 503. Fix the engine it "
     "names; clears 30 min after the last"),
)

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
_CRITICAL_TITLES: tuple[tuple[str, str], ...] = (
    ("warehouse_preconditions_unmet", "Warehouse switch held back"),
)


def _title(result: CanaryResult) -> str:
    if result.severity != "critical":
        return "Dashboard warning"
    keys = result.failure_keys
    if not keys or any(key in _OUTAGE_KEYS for key in keys):
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
