"""Step 05: the escalator — pressure on what nobody resolved or acknowledged.

Lives in core/ but runs in the **bot**, on the canary's existing 15-minute
tick, reading only the archive. The owner of this judgement was the open
question the adversarial review refused to let slide: a web-side job dies
with web — exactly the case the canary exists for — so the process that
already watches from outside is the one that judges standing conditions too.

The policy, shaped by the noise measurements:

- A condition must be **firing, unacknowledged, and standing six hours**
  past the start of its current cycle. Six hours is the gap the Gate's
  design leaves open: the loud hour ends, the daily reminder is 23 hours
  away, and a CRITICAL nobody has touched should not wait that long to say
  "still here".
- **One escalation per firing cycle**, not per day: the Gate's daily
  reminder already covers "still standing" for as long as the emitter keeps
  raising. The cycle boundary is the last resolution — a condition that
  cleared and returned earns a fresh escalation, a condition that just
  stands does not earn a stream of them.
- **Only while web is reachable.** A dead web means every series in the
  archive goes stale at once, and the canary is already paging the outage
  itself; one escalation per stale layer per cycle on top of that page is
  the storm the review predicted. The caller passes what the canary just
  measured.

Honesty note, recorded here because the message does not carry it: with one
Telegram channel this is a louder repeat, not an escalation tier. Whether a
second tier exists (a call, a second chat, UptimeRobot paging) is the
owner's open question; this module gains a second transport the day it is
answered.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Sequence

logger = logging.getLogger(__name__)

# Standing this long unresolved and unacknowledged earns the one loud repeat.
ESCALATE_AFTER_S = 6 * 3600

_DUE_QUERY = """
WITH last_resolved AS (
    SELECT condition_key, max(at) AS at
    FROM app.alert_events
    WHERE event_type = 'resolved'
    GROUP BY condition_key
),
cycles AS (
    SELECT s.condition_key,
           s.fired_count,
           GREATEST(s.first_fired_at, COALESCE(lr.at, s.first_fired_at))
               AS cycle_start
    FROM app.alert_series s
    LEFT JOIN last_resolved lr USING (condition_key)
    WHERE s.kind = 'condition'
      AND s.state = 'firing'
      AND s.acknowledged_at IS NULL
)
SELECT c.condition_key, c.fired_count, c.cycle_start
FROM cycles c
WHERE c.cycle_start <= now() - make_interval(secs => $1)
  AND NOT EXISTS (
      SELECT 1 FROM app.alert_events e
      WHERE e.condition_key = c.condition_key
        AND e.event_type = 'escalated'
        AND e.at > c.cycle_start
  )
ORDER BY c.cycle_start
"""


@dataclass(frozen=True)
class DueCondition:
    condition_key: str
    fired_count: int
    cycle_start: datetime


def format_escalation(
    due: Sequence[DueCondition], now: Optional[datetime] = None,
) -> str:
    now = now or datetime.now(timezone.utc)
    lines = []
    for row in due:
        start = row.cycle_start
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        hours = int((now - start).total_seconds() // 3600)
        lines.append(
            f"• {row.condition_key} — {hours}ч, срабатываний ×{row.fired_count}"
        )
    return "⏳ <b>Висит без реакции:</b>\n" + "\n".join(lines)


async def _query_due(seconds: float) -> List[DueCondition]:
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch(_DUE_QUERY, float(seconds))
    return [
        DueCondition(
            condition_key=r["condition_key"],
            fired_count=int(r["fired_count"]),
            cycle_start=r["cycle_start"],
        )
        for r in records
    ]


async def escalate_due(
    *,
    web_alive: bool,
    _due: "Optional[Sequence[DueCondition]]" = None,
) -> int:
    """Judge and, if due, send the one loud repeat. Returns admins reached.

    `web_alive` is what the canary's probe just measured: False stands the
    escalator down for this cycle, because the outage is already its own
    page and every archive series is stale together during one.

    Never raises — this runs on the canary's tick and must never take the
    canary down with it.
    """
    if not web_alive:
        return 0
    if not os.getenv("KS_PG_DSN", "").strip():
        return 0
    try:
        due = list(_due) if _due is not None else await _query_due(
            ESCALATE_AFTER_S,
        )
        if not due:
            return 0

        text = format_escalation(due)

        from bot.main import send_admin_message

        delivered = await send_admin_message(text, pre_throttled=True)

        # Recorded whatever the delivery outcome: an attempted escalation is
        # the fact that stops the next fifteen-minute tick from re-judging
        # the same cycle — a kill-switched dev instance must not try again
        # every cycle forever.
        from core.alert_archive import record_escalated

        record_escalated(
            [d.condition_key for d in due], delivered=delivered, message=text,
        )
        logger.info(
            "Escalated %d standing condition(s), delivered to %d",
            len(due), delivered,
        )
        return delivered
    except Exception as exc:
        logger.warning("Escalator cycle failed: %s", exc)
        return 0
