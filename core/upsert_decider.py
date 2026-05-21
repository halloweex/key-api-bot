"""Pure decider for the upsert_orders write-amplification fix.

Decouples the "should we touch this row?" decision from the surrounding
I/O so it can be unit-tested exhaustively without spinning up DuckDB.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional


def should_update_order(
    existing_updated_at: Optional[datetime],
    incoming_updated_at: Optional[datetime],
    *,
    force: bool = False,
) -> bool:
    """Return True iff we should issue an UPDATE for this order.

    The fix this enables: incremental_sync runs every minute and pulls a
    24-hour rolling window. For 278 orders × 1440 syncs/day that meant
    400K identity UPDATEs and 1M+ MVCC tombstones per day on production.
    We can avoid almost all of that by skipping rows whose KeyCRM
    ``updated_at`` hasn't advanced past the stored value.

    Semantics:

    - ``force=True``        — always UPDATE. Required for the status-refresh
      path because KeyCRM does NOT bump ``updated_at`` when only the order
      status changes (a known KeyCRM behaviour). Callers who detect status
      drift out-of-band must pass force.

    - existing is None      — UPDATE. We don't have a stored timestamp to
      compare against (legacy row pre-migration), so be safe and write.

    - incoming is None      — UPDATE. The KeyCRM payload didn't include
      ``updated_at``. Better to overwrite than risk silently keeping stale
      data.

    - strictly newer        — UPDATE. KeyCRM has moved on; we follow.

    - same or older         — SKIP. Identity write; nothing changed. This
      is where the amplification reduction comes from.

    Args:
        existing_updated_at: stored ``orders.updated_at`` from DuckDB.
        incoming_updated_at: ``updated_at`` field from the KeyCRM payload.
        force: bypass all checks; behaves like the old "always UPDATE" path.

    Returns:
        True = issue UPDATE. False = skip (and skip line-item DELETE+INSERT).
    """
    if force:
        return True
    if existing_updated_at is None or incoming_updated_at is None:
        return True
    return incoming_updated_at > existing_updated_at
