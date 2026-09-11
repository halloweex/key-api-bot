"""The button on an alert: a named action, approved by a human, run by code.

The diagnostician reads logs, and log content is written by things we do not
control. Its safety model is therefore the allowlist — it holds no command that
writes, and `ks_readonly` cannot write even if a log line asks it to nicely.
That is the property this module must not spend.

So **the agent never names a command.** An alert offers a `key`, the key is a
member of `ACTIONS` below, and what the key *does* lives in code that a human
reviewed. A log line can at worst make the system offer a button that already
existed. It cannot invent one, and it cannot pass an argument that becomes a
command: the only free-form part of a request is the subject, and the executor
matches that against a known set too.

WHY THE LEDGER AND NOT A QUEUE TABLE

The tap arrives at the *bot* and the work happens in *web* — two containers.
Wiring them over HTTP would mean a new internal credential, which is a new way
in; both already share Postgres. And the natural place for "somebody asked for
X on alert Y" is the log of what alerts did: `app.alert_events` already carries
a JSONB `context` and an autoincrementing id, so a request is one row and its
outcome is another that points back at it. The audit is not a feature added
afterwards, it is the storage.

WHAT A BUTTON MAY BE

Rule 5 of the alerts charter still holds: an alert does not trigger a repair.
This does not change that — nothing here runs on its own. A person reads the
diagnosis and decides, and the delay between the alert and the tap is the
point, not a cost.

An action qualifies when it is **safe to run twice**, **cannot destroy the only
copy of anything**, and **is something an operator would otherwise do by hand
from a keyboard at 03:00**. `dq_recheck` is the first because it is the
commonest of those: half the diagnoses this system has produced end in "wait
for the next run and see whether the same ids come back", and the next run is
hours away.

Deliberately absent, and each for its own reason: anything that writes to a
mirrored table (Reconciliation A reports and does not repair — a "repair" there
would write the only record of a stock change from the only other record of
it), anything that deletes (`docker volume prune` was recommended by the agent
once and would have taken a named volume with it), and the weekly compact,
which stops both containers.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Tuple


@dataclass(frozen=True)
class AlertAction:
    """One thing a human may authorise from an alert."""
    key: str
    # The button's face. Short: Telegram gives an inline button about 30
    # characters before it truncates, and a truncated verb is a guess.
    label: str
    # What the operator is agreeing to, answered into the tap. It says what
    # will happen, not that it succeeded — the work is still ahead of it.
    ack: str
    # Subjects this action accepts. The tap carries one, and it is the only
    # caller-supplied part of a request, so it is matched rather than trusted.
    subjects: Tuple[str, ...]


# DQ layers a recheck may name. Taken from the scheduler's own job ids, which
# is what makes the subject a lookup rather than a string handed to a runner.
DQ_LAYERS: Tuple[str, ...] = (
    "integrity",
    "reconciliation",
    "mirror_landing",
)

ACTIONS: Dict[str, AlertAction] = {
    "dq_recheck": AlertAction(
        key="dq_recheck",
        label="🔄 Перепроверить сейчас",
        ack="Проверка поставлена в очередь — результат придёт отдельным сообщением",
        subjects=DQ_LAYERS,
    ),
}

# Layer → the scheduler job that re-runs it. A second lookup, not a string
# built from the subject: `trigger_job` refuses an id its registry does not
# hold, and this keeps the subject a key in a table written here rather than
# something concatenated into an identifier.
DQ_JOB_FOR_LAYER: Dict[str, str] = {
    "integrity": "dq_integrity_check",
    "reconciliation": "dq_reconciliation",
    "mirror_landing": "dq_mirror_landing",
}


def job_for(action: str, subject: str) -> Optional[str]:
    """The scheduler job this request means, or None if it means none."""
    if action != "dq_recheck":
        return None
    return DQ_JOB_FOR_LAYER.get(subject)


# `afix:<action>:<subject>`. Colon, matching `atab:` next door and for the same
# reason: neighbouring handlers split ids on `_`, and several action keys and
# every layer name contain one.
CALLBACK_PREFIX = "afix"
CALLBACK_PATTERN = r"^afix:[a-z_]+:[a-z_]+$"

# Telegram's own ceiling on callback_data. Checked rather than assumed, because
# the failure is silent: an over-long payload is rejected when the *keyboard is
# sent*, which would cost the whole alert rather than the button.
CALLBACK_MAX_BYTES = 64

REQUESTED = "action_requested"
DONE = "action_done"


def callback_for(action: str, subject: str) -> str:
    """The callback_data for one button, or raise.

    Raises rather than returning None: a caller building a keyboard has already
    decided to offer this, and a silently dropped button is a button an
    operator looks for at 03:00 and does not find.
    """
    spec = ACTIONS.get(action)
    if spec is None:
        raise ValueError(f"unknown alert action {action!r}")
    if subject not in spec.subjects:
        raise ValueError(f"{action!r} does not accept subject {subject!r}")
    data = f"{CALLBACK_PREFIX}:{action}:{subject}"
    if len(data.encode("utf-8")) > CALLBACK_MAX_BYTES:
        raise ValueError(f"callback_data too long: {data!r}")
    return data


def parse_callback(data: str) -> Optional[Tuple[AlertAction, str]]:
    """`afix:dq_recheck:mirror_landing` → (spec, subject), or None.

    Strict on purpose. This is the point where a string that arrived from
    outside becomes a decision to run code, so every part of it is matched
    against something declared here: the shape, the action, and the subject.
    Anything else is None, and the caller says "не знаю такого действия"
    rather than guessing what was meant.
    """
    if not isinstance(data, str) or not re.match(CALLBACK_PATTERN, data):
        return None
    _, action, subject = data.split(":")
    spec = ACTIONS.get(action)
    if spec is None or subject not in spec.subjects:
        return None
    return spec, subject


def buttons_for(actions: Sequence[str], subject: str) -> list:
    """[(label, callback_data)] for the keyboard, skipping what does not apply.

    Skips rather than raises here — an emitter may offer an action generally
    while a particular subject is outside it, and losing one button is better
    than losing the alert that carries it.
    """
    out = []
    for action in actions:
        try:
            out.append((ACTIONS[action].label, callback_for(action, subject)))
        except (KeyError, ValueError):
            continue
    return out


# ─── The ledger side ─────────────────────────────────────────────────────────


async def request(conn, *, action: str, subject: str, condition_key: str,
                  by_user_id: int, instance: str) -> int:
    """Record that a human asked for this. Returns the request id.

    Written by the bot, in the bot's own pool. The row *is* the queue: there is
    no separate state to fall out of step with it, and `id` is the handle the
    outcome points back at.
    """
    return await conn.fetchval(
        """
        INSERT INTO app.alert_events
            (condition_key, event_type, instance, context)
        VALUES ($1, $2, $3, $4::jsonb)
        RETURNING id
        """,
        condition_key, REQUESTED, instance,
        _json({"action": action, "subject": subject, "by": by_user_id}),
    )


async def pending(conn, *, limit: int = 5) -> list:
    """Requests with no outcome yet, oldest first.

    The anti-join is what makes this a queue without a state column. One web
    container consumes it, and the outcome lands in the same transaction as the
    work, so a claim cannot be lost between the two.

    Bounded because a burst of taps must not turn one tick into a long job.
    """
    rows = await conn.fetch(
        """
        SELECT r.id, r.condition_key, r.context
        FROM app.alert_events r
        WHERE r.event_type = $1
          AND NOT EXISTS (
              SELECT 1 FROM app.alert_events d
              WHERE d.event_type = $2
                AND (d.context->>'request_id')::bigint = r.id
          )
        ORDER BY r.id
        LIMIT $3
        """,
        REQUESTED, DONE, limit,
    )
    return [dict(r) for r in rows]


async def complete(conn, *, request_id: int, condition_key: str,
                   instance: str, ok: bool, detail: str) -> None:
    """Record the outcome, which is also what takes the request off the queue."""
    await conn.execute(
        """
        INSERT INTO app.alert_events
            (condition_key, event_type, instance, message, context)
        VALUES ($1, $2, $3, $4, $5::jsonb)
        """,
        condition_key, DONE, instance, detail[:2000],
        _json({"request_id": int(request_id), "ok": bool(ok)}),
    )


def _json(payload: Dict[str, Any]) -> str:
    """asyncpg registers no JSONB codec here, so the cast needs a string —
    handing it a dict stores a quoted string that reads like a document in
    psql and is not one."""
    import json

    return json.dumps(payload, ensure_ascii=False)
