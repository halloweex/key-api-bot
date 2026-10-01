"""A durable record of every rollback lever pulled (OD-17 (a)).

WHY IT EXISTS

The parallel period runs thirty days from the last write flag, and the week of
silence seven days from `KS_DUCKDB=off`; both restart when "any rollback lever
is used". Until this module nothing durable said that one had been. The only
lever that gives a chain back to DuckDB is `scripts/chain_copy_back.py`, and
what it leaves behind is an absence: `release_chain` deletes the chain's
`owner:` rows from `meta.chain_watermarks` and unlinks the marker, and the
script's own report goes to a `--rm` container's output and dies with it. An
absence cannot be dated, and a soak check that has to prove a lever was *not*
pulled in thirty days cannot read one.

WHERE IT GOES, AND WHY THERE

`app.alert_events`, as one row with `event_type = 'lever_used'` under the key
`lever:<name>`. That table is already the ledger of what operators did beside
what alerts said — `core/alert_actions.py` records every approved button there
for the same reason — it lives in Postgres alone, carries a JSONB `context`,
has no CHECK on `event_type` (revision 0012 leaves it unconstrained on
purpose), and is read by nothing that would mistake this row for an alert: the
escalator, the digest's tail and the acknowledgements all ask for `fired`,
`escalated` or `resolved`. So no revision, and no new table.

INSIDE THE CALLER'S TRANSACTION

`record` takes a connection and writes on it, with no armour: the release of a
latch and its record commit together or neither does. A release whose record
could not be written must not commit, or the period's clock would run on over
the one event that should have stopped it. `core/alert_archive.py` writes
fire-and-forget because a page must never wait on its ledger; this is the
opposite contract, and the reason it is a module of its own.

The soak's lever check (`deploy/stage4_soak/50_p1_rollback_levers.sql`) reads
these rows by `LEVER_PREFIX` and `LEVER_USED`, and a test holds the file to
the constants here.
"""
from __future__ import annotations

import json
import logging
import os
import socket
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

LEVER_USED = "lever_used"
LEVER_PREFIX = "lever:"

# The levers recorded today. `chain_copy_back` is the only lever that gives a
# chain back to DuckDB; the outcome says how far it got.
CHAIN_COPY_BACK = "chain_copy_back"
RELEASED = "released"
COMMITTED_NOT_RELEASED = "committed_not_released"

_INSERT = (
    "INSERT INTO app.alert_events (condition_key, event_type, instance, message, context) "
    "VALUES ($1, $2, $3, $4, $5::jsonb)"
)


def lever_key(lever: str) -> str:
    return f"{LEVER_PREFIX}{lever}"


def _instance() -> str:
    """`KS_INSTANCE`, as the alert journal stamps its rows."""
    return os.getenv("KS_INSTANCE", "").strip() or socket.gethostname()


async def record(conn, lever: str, *, subject: str, outcome: str,
                 detail: Optional[Dict[str, Any]] = None) -> None:
    """Write one `lever_used` row on `conn`, in whatever transaction it holds.

    Raises whatever the driver raises: a lever whose use cannot be recorded
    must not complete silently."""
    context = {"subject": subject, "outcome": outcome, **(detail or {})}
    await conn.execute(
        _INSERT, lever_key(lever), LEVER_USED, _instance(),
        f"{lever}: {subject} {outcome}", json.dumps(context, default=str),
    )
    logger.warning("Rollback lever recorded: %s %s %s (OD-17: the parallel "
                   "period and the week of silence start again)",
                   lever, subject, outcome)
