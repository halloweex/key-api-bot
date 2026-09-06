"""The durable half of the alerting machinery — step 03.

Writes the archive (`app.alert_series` + `app.alert_events`, revision 0012)
that turns "alert X was sent" from a docker-log line with a 250 MB rotation
into a queryable fact: the digest's «узаконено» tail, the escalator's timers
(step 05) and П4's acknowledgements all read from here.

Three properties, each a decision the adversarial review forced:

**Fire-and-forget, ~1 s budget, never on the critical path.** The warehouse
validator awaits its alert inline on a 2-minute job; a slow Postgres must
cost a missing archive row, never a stalled refresh. Callers get a Task they
are free to ignore.

**No revision gate.** Every other Postgres consumer calls `require_revision`
and fails closed on a mismatch — correct for rebuilds, fatal here: the
deploy window between `migrate` and the container swap is exactly when
rebuilds start failing and alerts start firing, and a version gate on the
alerting store would turn a deploy-order mistake into deaf alerting. This
module tolerates absence instead: missing DSN, missing tables, unreachable
server — one WARNING per stand-down, then quiet until the process restarts
or a write succeeds.

**Never raises.** An archive that could take an alert down with it would
invert its purpose.
"""
from __future__ import annotations

import asyncio
import json as _json
import logging
import os
from datetime import datetime
from typing import Iterable, Optional, Sequence

logger = logging.getLogger(__name__)

# One write's budget. Generous against the measured ~0.5 ms asyncpg call and
# irrelevant to callers, who never await the task on their own path.
WRITE_TIMEOUT_S = 1.0

# Set after a failed write so a dead Postgres logs one warning per streak,
# not one per suppressed-alert tick. Cleared by the next successful write.
_standing_down = False


def _instance() -> str:
    from core.telegram_alerts import instance_name

    return instance_name()


async def _write_fired(
    conditions: Sequence[str],
    message: str,
    delivered: int,
    swallowed: int,
    evidence: "Optional[dict]" = None,
) -> None:
    global _standing_down
    from core.alerting import Kind, spec_for
    from core.pg import get_pool

    pool = await get_pool()
    instance = _instance()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for i, key in enumerate(conditions):
                kind = spec_for(key).kind
                state = "firing" if kind is Kind.CONDITION else "event"
                await conn.execute(
                    """
                    INSERT INTO app.alert_series
                        (condition_key, kind, state, first_fired_at,
                         last_fired_at, fired_count, suppressed_count, instance)
                    VALUES ($1, $2, $3, now(), now(), 1, $4, $5)
                    ON CONFLICT (condition_key) DO UPDATE SET
                        last_fired_at    = now(),
                        fired_count      = app.alert_series.fired_count + 1,
                        suppressed_count = app.alert_series.suppressed_count
                                           + EXCLUDED.suppressed_count,
                        state = CASE WHEN app.alert_series.kind = 'condition'
                                     THEN 'firing'
                                     ELSE app.alert_series.state END,
                        resolved_at = CASE WHEN app.alert_series.kind = 'condition'
                                           THEN NULL
                                           ELSE app.alert_series.resolved_at END,
                        instance   = EXCLUDED.instance,
                        updated_at = now()
                    """,
                    key, kind.value, state, swallowed if i == 0 else 0, instance,
                )
            # Serialised here rather than handed over as a dict: the pool
            # registers no JSONB codec, so the cast is what makes this a
            # document instead of a quoted string.
            blob = _json.dumps(evidence, ensure_ascii=False) if evidence else None
            await conn.executemany(
                """
                INSERT INTO app.alert_events
                    (condition_key, event_type, instance, delivered_to,
                     message, context)
                VALUES ($1, 'fired', $2, $3, $4, $5::jsonb)
                """,
                [
                    # The body rides the first condition's row; siblings share
                    # the moment by (at, instance) without duplicating ~4 KB.
                    # The evidence rides with it for the same reason.
                    (key, instance, delivered,
                     message if i == 0 else None,
                     blob if i == 0 else None)
                    for i, key in enumerate(conditions)
                ],
            )
    if _standing_down:
        _standing_down = False
        logger.info("alert archive: writes succeeding again")


async def _write_resolved(keys, message, delivered) -> None:
    global _standing_down
    from core.pg import get_pool

    pool = await get_pool()
    instance = _instance()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE app.alert_series
                   SET state = 'resolved', resolved_at = now(),
                       updated_at = now()
                 WHERE condition_key = ANY($1::text[])
                   AND kind = 'condition'
                """,
                list(keys),
            )
            await conn.executemany(
                """
                INSERT INTO app.alert_events
                    (condition_key, event_type, instance, delivered_to, message)
                VALUES ($1, 'resolved', $2, $3, $4)
                """,
                [
                    (key, instance, delivered, message if i == 0 else None)
                    for i, key in enumerate(keys)
                ],
            )
    if _standing_down:
        _standing_down = False
        logger.info("alert archive: writes succeeding again")


def _spawn(coro) -> "Optional[asyncio.Task]":
    """Wrap an archive write in the standard armour: budget, one warning per
    streak, never raises, never blocks the caller."""

    async def _task() -> None:
        global _standing_down
        try:
            await asyncio.wait_for(coro, timeout=WRITE_TIMEOUT_S)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not _standing_down:
                _standing_down = True
                logger.warning(
                    "alert archive standing down (%s: %s) — alerts still "
                    "deliver, the ledger misses rows until this clears",
                    type(exc).__name__, exc,
                )

    try:
        if not os.getenv("KS_PG_DSN", "").strip():
            coro.close()
            return None
        return asyncio.get_running_loop().create_task(_task())
    except RuntimeError:
        coro.close()
        return None


async def _write_escalated(keys, message, delivered) -> None:
    global _standing_down
    from core.pg import get_pool

    pool = await get_pool()
    instance = _instance()
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO app.alert_events
                (condition_key, event_type, instance, delivered_to, message)
            VALUES ($1, 'escalated', $2, $3, $4)
            """,
            [
                (key, instance, delivered, message if i == 0 else None)
                for i, key in enumerate(keys)
            ],
        )
    if _standing_down:
        _standing_down = False
        logger.info("alert archive: writes succeeding again")


# ─── The digest's tail (step 06) ────────────────────────────────────────────
#
# The digest's *delta* stays on data_quality_issues in DuckDB — the review's
# finding 24: a ledger-based delta answers a different question from a
# different store, and a metric defined twice drifts. What the archive
# contributes is the part only it knows: which conditions stand firing right
# now, for how long, what was escalated, and — once П4 lands — how many are
# acknowledged. The tail may never summon a digest on its own; it rides one
# that news already earned.

_TAIL_FIRING_LIMIT = 10


def render_digest_tail(
    firing: "Sequence[tuple]",
    resolved_24h: int,
    acknowledged: int,
    now: "Optional[datetime]" = None,
) -> "Optional[str]":
    """Pure renderer. `firing` rows: (key, first_fired_at, fired_count,
    escalated: bool). Returns None when there is nothing to say — an empty
    ledger block every morning would be the digest teaching readers to skip
    its tail."""
    from datetime import datetime as _dt, timezone as _tz

    now = now or _dt.now(_tz.utc)
    lines = []
    if firing:
        shown = list(firing)[:_TAIL_FIRING_LIMIT]
        rendered = []
        for key, first, count, escalated in shown:
            if first.tzinfo is None:
                first = first.replace(tzinfo=_tz.utc)
            hours = int((now - first).total_seconds() // 3600)
            age = f"{hours}h" if hours < 48 else f"{hours // 24}d"
            mark = " · escalated" if escalated else ""
            rendered.append(f"• {key} — {age}, ×{count}{mark}")
        lines.append("⏳ Firing:")
        lines.extend(rendered)
        if len(firing) > _TAIL_FIRING_LIMIT:
            lines.append(f"  …+{len(firing) - _TAIL_FIRING_LIMIT} more")
    if resolved_24h:
        lines.append(f"✅ Resolved in 24h: {resolved_24h}")
    if acknowledged:
        lines.append(f"📌 Acknowledged: {acknowledged}")
    if not lines:
        return None
    return "── Alert ledger ──\n" + "\n".join(lines)


async def fetch_digest_tail() -> "Optional[str]":
    """Read the ledger and render the tail. None on any failure — the digest
    goes out without its tail rather than not at all, and this read must
    never summon or sink the message it rides."""
    if not os.getenv("KS_PG_DSN", "").strip():
        return None
    try:
        from core.pg import get_pool

        pool = await get_pool()

        async def _read():
            async with pool.acquire() as conn:
                firing = await conn.fetch(
                    """
                    SELECT s.condition_key, s.first_fired_at, s.fired_count,
                           EXISTS (
                               SELECT 1 FROM app.alert_events e
                               WHERE e.condition_key = s.condition_key
                                 AND e.event_type = 'escalated'
                                 AND e.at > s.first_fired_at
                           ) AS escalated
                    FROM app.alert_series s
                    WHERE s.kind = 'condition' AND s.state = 'firing'
                      AND s.acknowledged_at IS NULL
                    ORDER BY s.first_fired_at
                    """
                )
                resolved = await conn.fetchval(
                    """
                    SELECT count(DISTINCT condition_key)
                    FROM app.alert_events
                    WHERE event_type = 'resolved'
                      AND at > now() - interval '24 hours'
                    """
                )
                acked = await conn.fetchval(
                    """
                    SELECT count(*) FROM app.alert_series
                    WHERE acknowledged_at IS NOT NULL AND state = 'firing'
                    """
                )
                return firing, int(resolved or 0), int(acked or 0)

        firing, resolved, acked = await asyncio.wait_for(
            _read(), timeout=2.0,
        )
        rows = [
            (r["condition_key"], r["first_fired_at"],
             int(r["fired_count"]), bool(r["escalated"]))
            for r in firing
        ]
        return render_digest_tail(rows, resolved, acked)
    except Exception as exc:
        logger.warning("digest tail unavailable (%s) — digest rides without it", exc)
        return None


def record_escalated(
    keys: Sequence[str], *, delivered: int, message: str,
) -> "Optional[asyncio.Task]":
    """Archive an escalation. The series stays firing — an escalation is a
    louder repeat about it, not a state change."""
    if not keys:
        return None
    return _spawn(_write_escalated(list(keys), message, delivered))


def record_resolved(
    keys: Sequence[str], *, delivered: int, message: str,
) -> "Optional[asyncio.Task]":
    """Archive a resolution: close the series, add the event rows."""
    if not keys:
        return None
    return _spawn(_write_resolved(list(keys), message, delivered))


def record_fired(
    conditions: Iterable[str],
    *,
    message: str,
    delivered: int,
    swallowed: int = 0,
    evidence: "Optional[dict]" = None,
) -> "Optional[asyncio.Task]":
    """Archive a delivered alert. Returns the background task, or None.

    Synchronous on purpose: callers are mid-alert and must not await
    anything. The task carries its own timeout and swallows its own failures.
    """
    keys = [k for k in conditions]
    if not keys:
        return None

    async def _task() -> None:
        global _standing_down
        try:
            await asyncio.wait_for(
                _write_fired(keys, message, delivered, swallowed, evidence),
                timeout=WRITE_TIMEOUT_S,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not _standing_down:
                _standing_down = True
                logger.warning(
                    "alert archive standing down (%s: %s) — alerts still "
                    "deliver, the ledger misses rows until this clears",
                    type(exc).__name__, exc,
                )

    try:
        if not os.getenv("KS_PG_DSN", "").strip():
            # A host with no Postgres runs the whole app unarchived — the
            # laptop case. Quietly: this is configuration, not a failure.
            return None
        return asyncio.get_running_loop().create_task(_task())
    except RuntimeError:
        # No running loop (sync test context) — the archive is best-effort.
        return None
