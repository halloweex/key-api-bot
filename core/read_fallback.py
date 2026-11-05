"""A read whose engine failed is refused, counted in one place.

Every router reads one engine — Postgres, or ClickHouse for the cohorts —
and has no other to answer from. When that engine fails, the router calls
`refuse(surface, exc)` from the handler around its read and raises what it
is handed: `ReadUnavailable`, which `web/main.py` turns, from any route, into
one 503 naming the surface. A 503 is an outage somebody can see; the frontend
already reads it as "temporarily unavailable" and retries it
(`ApiErrorState`). The cause stays in web's log, beside the refusal.

Each refusal is counted per surface (`refusals()`), and `/api/health`
publishes the counts as `read_fallback_mode.refused` — the block keeps its
name because the canary judges it there, paging a recent refusal as
`read_refused` (bot/canary.py). The line it is logged with says "read
refused", which is what that page's lever greps for.

WHAT NO HTTP REQUEST WAITS FOR ANSWERS ITSELF (DN-20c)

Everything else that can receive a refusal gives it an answer of its own:
the two weekly reports defer with no ledger row; the Monday goals job asks
every read it makes before it writes anything, so it defers whole; training
is skipped and the previous model stands; the search index and the buyers
step skip their step for the tick with the watermark held, and the tick
goes on; the boot contains each of those; and the assistant's tools return a
named `data_unavailable` result. A job says so in its result through
`answered`, below, so `/api/jobs` shows a refusal rather than a quiet run —
all but the incremental sync, whose stats are summed and carry no strings:
its buyers step records the refusal and passes it on, the tick names it at
the call with `answered` and skips the step, and it shows in the log, in
`buyer_sync`'s error class and in `refusals()`. The consumers are derived,
not remembered — `tests/unit/test_read_fallback_consumers.py` walks up from
every refusal to the entry points nothing in the repository calls, then back
down through the `try` each call sits in, and pins where every consumer's
refusal stops.

WHAT THIS MODULE WAS, UNTIL STEP 14'S S14-16

It counted every read that fell back to DuckDB (DN-20a) — fourteen routers
answered from DuckDB when their engine failed, each in its own words — and
published the counts as `read_fallbacks`. `KS_READ_FALLBACK=off` (DN-20b)
made `fall_back` refuse instead, as production has since 2026-10-09;
`no_address` refused a read switch naming an engine with no address, and
`no_engine` the cohorts with no ClickHouse; `misconfigured` and `no_engine`
published those configurations at start, and the canary paged them as
`read_fallback_used`, `read_routed_to_duckdb` and
`read_fallback_mode_invalid`. Since S14-07 no router has a DuckDB tail and
no read switch routes a read, so none of that had a caller: S14-16 deleted
it with the health fields and the canary's three checks, and
`KS_READ_FALLBACK` is not read. The condition names are not reused (rule 15
of the plan). What is left is the refusal, its count and its answers.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Dict

logger = logging.getLogger(__name__)

_lock = threading.Lock()
# `{surface: {count, last_at}}`, the reads refused since this process started.
_refused: Dict[str, Dict[str, object]] = {}


class ReadUnavailable(Exception):
    """A read whose one engine failed, refused.

    Raised from the handler around a router's read with what `refuse`
    returns, and by nothing else. `web/main.py` maps it to a 503 carrying
    `surface`, so a handler between a route and a router must let it
    through — the walk in `tests/unit/test_read_fallback_sites.py` requires
    every handler around a router to say so, and
    `GoalsMixin._get_ml_forecast_total` is the one that had to.
    """

    def __init__(self, surface: str):
        super().__init__(f"{surface}: the read cannot be answered")
        self.surface = surface


def _tally(surface: str) -> int:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with _lock:
        entry = _refused.setdefault(surface, {"count": 0, "last_at": None})
        entry["count"] = int(entry["count"]) + 1
        entry["last_at"] = now
        return int(entry["count"])


def refuse(surface: str, exc: BaseException) -> ReadUnavailable:
    """A read on `surface` failed in its one engine: count it, say so, and
    hand back the `ReadUnavailable` the caller raises.

    The routers call this from the handler around their read, as `raise
    read_fallback.refuse(surface, exc)`. Counted with the refusals
    (`read_fallback_mode.refused` on `/api/health`), logged at ERROR with the
    cause beside it, and caused by `exc` — what `raise ReadUnavailable(surface)
    from exc` makes: setting the cause suppresses the context, as `from` does
    — so `web/main.py` answers the 503 naming the surface. Returned rather
    than raised: the handler says `raise` itself, which is what the walks in
    `tests/unit/test_read_fallback_sites.py` read as a decision.

    `surface` is the tab or consumer a reader would name (`dashboard`,
    `goals`, `cohorts`…), a literal, because it is a key in `/api/health`.

    The line says "read refused", the phrase `read_refused`'s lever greps,
    and never "falling back to DuckDB": nothing fell back. Until step 14's
    S14-16 it named `KS_READ_FALLBACK=off` as what refused; nothing reads
    that variable since, and a reader of the log is not sent to it.
    """
    count = _tally(surface)
    logger.error(
        "read refused: %s — its engine failed, and no other store may answer "
        "it (%d refused since start): %s", surface, count, exc,
        exc_info=exc if isinstance(exc, BaseException) else None,
    )
    refusal = ReadUnavailable(surface)
    refusal.__cause__ = exc
    return refusal


READ_UNAVAILABLE = "read_unavailable"


def answered(consumer: str, exc: ReadUnavailable, answer: str) -> Dict[str, str]:
    """`consumer` received a refusal the 503 handler will not answer — no
    HTTP request is waiting for it, or (the assistant) the request is a
    conversation and the answer goes inside it — and answered it itself
    (DN-20c). Returns the fields its result carries.

    `consumer` names the caller as a reader of the job list would —
    `weekly_report`, `revenue_prediction` — and `answer` says in words what
    it did instead: deferred, kept the previous model, skipped the step. The
    refusal itself was logged at ERROR and counted where it was raised
    (`read_fallback_mode.refused`), so this line says only what became of
    it, at WARNING. The returned `{"reason", "surface"}` goes into the
    consumer's own result, so `/api/jobs` history shows a refusal rather than
    a quiet run.
    """
    logger.warning("refused read answered by %s (%s): %s",
                   consumer, exc.surface, answer)
    return {"reason": READ_UNAVAILABLE, "surface": exc.surface}


def refusals() -> Dict[str, Dict[str, object]]:
    """`{surface: {count, last_at}}` — reads refused since this process
    started. A copy."""
    with _lock:
        return {surface: dict(entry) for surface, entry in _refused.items()}


def reset_counts() -> None:
    """Forget every count. Tests only; a process never forgets its own."""
    with _lock:
        _refused.clear()
