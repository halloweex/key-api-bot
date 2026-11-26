"""The watch on the Rust pilot (`ks-rust-api`, rust/README.md): is it there,
and how much of what it was asked did it hand to the Python app for a reason
of its own.

A line in the 09:00 digest, never a page — the owner's decision of
2026-11-25. So every finding is a WARN on the integrity layer, and the
integrity job leaves them out of what it pages with (`is_watch_finding`). A
pilot that is down or hands everything over costs nobody an answer: nginx
gives the request to web on the pilot's 501 and on the 502 it makes itself
(nginx/rust/proxy.conf). What this watches is whether the pilot still does
its share.

Asked by the integrity job (01, 07, 13 and 19 Kyiv) where `KS_RUST_API_URL`
says where the pilot is — `http://ks-rust-api:8090` in docker-compose.yml,
on the `default` network web shares with it. Unset, nothing is asked and
nothing is filed: a host without the pilot runs unchanged. It reads
`/readyz` and the pilot's own `/stats`, whose window is the last 24 hours
(`rust/crates/ks-api/src/stats.rs`), so each run sees the day before it and
the morning's run, the one the digest reports, sees the night too.

Hand-overs by design are not counted against the pilot: a route it does not
port (`route`), the trend's filters it leaves to Python (`line_level_filter`,
`promocode_filter`, `source_without_gold_channel`) and input the page never
sends (`input`). The share counts the rest — `FAILURE_REASONS`: the schema at
another revision, Postgres, the pool or the session's stores not answering,
a statement its own role stopped.

The names are new (rule 15): the canary's `rust_api_*` keys, which never
left the pull request, are not reused.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

URL_ENV = "KS_RUST_API_URL"
TIMEOUT_S = 10.0
TABLE = "ks-rust-api"

UNREACHABLE = "rust_pilot_unreachable"
NOT_READY = "rust_pilot_not_ready"
UNWATCHED = "rust_pilot_unwatched"
HANDOVER_SHARE = "rust_pilot_handover_share"
CONDITIONS = (UNREACHABLE, NOT_READY, UNWATCHED, HANDOVER_SHARE)

# What clears each, for core/alerting.py's REGISTRY.
CLEARS: Dict[str, str] = {
    UNREACHABLE: "an integrity run reaches ks-rust-api again",
    NOT_READY: "ks-rust-api's /readyz answers 200 at an integrity run",
    UNWATCHED: "an integrity run reads ks-rust-api's /stats again",
    HANDOVER_SHARE: "the day's share of hand-overs falls under the limit",
}

# The words the digest prints beside each name (data_quality.HUMAN_CHECK_NAMES).
HUMAN_NAMES: Dict[str, str] = {
    UNREACHABLE: "the Rust pilot did not answer",
    NOT_READY: "the Rust pilot not ready",
    UNWATCHED: "the Rust pilot's counts not read",
    HANDOVER_SHARE: "the Rust pilot handed requests to Python",
}

FAILURE_REASONS = ("revision", "database", "pool", "session", "timeout")
# Above this share of the window's requests, with at least the minimum asked:
# one failed read in a quiet day is not a finding.
SHARE_MAX = 0.05
MIN_REQUESTS = 20


@dataclass(frozen=True)
class Answer:
    """One GET: its status and parsed body, or why there was none."""
    status: Optional[int] = None
    payload: Any = None
    error: Optional[str] = None


@dataclass(frozen=True)
class Facts:
    """What one integrity run read. `url` empty: not configured, nothing asked."""
    url: str = ""
    readyz: Answer = Answer()
    stats: Answer = Answer()


async def _get(client: Any, url: str) -> Answer:
    import httpx

    try:
        response = await client.get(url)
    except httpx.TimeoutException:
        return Answer(error=f"timeout after {TIMEOUT_S:.0f}s")
    except httpx.HTTPError as exc:
        return Answer(error=f"{type(exc).__name__}: {exc}")
    try:
        payload = response.json()
    except ValueError:
        payload = None
    return Answer(status=response.status_code, payload=payload)


async def read_facts(client: Any = None) -> Facts:
    """The pilot's `/readyz` and `/stats`, where `KS_RUST_API_URL` is set.
    Never raises: a failure is a fact the judgement names."""
    url = os.environ.get(URL_ENV, "").strip().rstrip("/")
    if not url:
        return Facts()
    import httpx

    own = client is None
    if own:
        client = httpx.AsyncClient(timeout=TIMEOUT_S)
    try:
        readyz = await _get(client, url + "/readyz")
        stats = await _get(client, url + "/stats") if readyz.error is None else Answer()
    except Exception as exc:  # noqa: BLE001 — anything else is the pilot not answering
        return Facts(url=url, readyz=Answer(error=f"{type(exc).__name__}: {exc}"))
    finally:
        if own:
            await client.aclose()
    return Facts(url=url, readyz=readyz, stats=stats)


def judge(facts: Facts) -> list:
    """WARN findings for the digest, or none."""
    from core.data_quality import IntegrityIssue, Severity

    def issue(name: str, count: int, description: str) -> Any:
        return IntegrityIssue(check_name=name, table_name=TABLE, severity=Severity.WARN,
                              count=count, description=description)

    if not facts.url:
        return []
    ready = facts.readyz
    if ready.error is not None:
        return [issue(UNREACHABLE, 1, f"ks-rust-api did not answer /readyz: {ready.error}. "
                                      "nginx hands its routes to web meanwhile")]
    if ready.status != 200:
        reason = (ready.payload or {}).get("reason") if isinstance(ready.payload, dict) else None
        return [issue(NOT_READY, 1, f"ks-rust-api not ready ({reason or f'HTTP {ready.status}'}): "
                                    "it hands every request to Python")]
    stats = facts.stats
    window = stats.payload.get("window") if (
        stats.status == 200 and isinstance(stats.payload, dict)) else None
    requests = window.get("requests") if isinstance(window, dict) else None
    handed = window.get("handed_over") if isinstance(window, dict) else None
    if not isinstance(requests, int) or not isinstance(handed, dict):
        why = stats.error or f"HTTP {stats.status}"
        return [issue(UNWATCHED, 1, f"ks-rust-api's /stats could not be read ({why}): "
                                    "the share it hands to Python is not judged")]
    failed = {r: n for r, n in handed.items()
              if r in FAILURE_REASONS and isinstance(n, int) and n > 0}
    total = sum(failed.values())
    if requests < MIN_REQUESTS or total <= SHARE_MAX * requests:
        return []
    hours = max(1, round((stats.payload.get("window_s") or 0) / 3600))
    reasons = ", ".join(f"{r} {n}" for r, n in sorted(failed.items()))
    return [issue(HANDOVER_SHARE, total,
                  f"ks-rust-api handed {total} of {requests} requests to Python "
                  f"in the last {hours} h for reasons of its own ({reasons})")]


def is_watch_finding(issue: Any) -> bool:
    """Whether `issue` is one `judge` files: digested, never paged."""
    return getattr(issue, "check_name", None) in CONDITIONS
