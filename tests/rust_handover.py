"""The pilot's 501 hand-over, as the harnesses count it.

A 501 from Rust is neither a difference nor an identical answer: nginx hands
the request to Python (`nginx/rust/proxy.conf`), and the person reads
Python's own answer. The harnesses count it apart, by the reason the 501
names in its `branch` (`rust/crates/ks-api/src/handover.rs`), and a phase of
requests the page sends may hand nothing over that it was not allowed to.
"""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple


class HandedOver(str):
    """The reason Rust's 501 names: the request went to Python, whose answer
    the person reads. No difference, and not identical either."""


def handover(rs) -> Optional[HandedOver]:
    """The reason Rust handed the request to Python — its 501's `branch` —
    or None when it answered itself."""
    if rs.status_code != 501:
        return None
    try:
        reason = rs.json().get("branch")
    except ValueError:
        reason = None
    return HandedOver(reason or "unnamed")


def handed_over(rows: Sequence[Tuple[str, int, Optional[str]]]) -> List[str]:
    """The requests among `rows` — (request, status, what was found) — that
    Rust handed to Python."""
    return [request for request, _, found in rows if isinstance(found, HandedOver)]
