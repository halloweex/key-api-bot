"""The alert boundary of the data-quality jobs, recorded.

A job pages through `core.alerting.raise_alert` and announces what cleared
through `core.alerting.resolve_group`, telling it what is still firing. Both
are replaced here by recorders, so a test reads what the job sent — the
page's conditions and evidence — and what it held firing, and never how the
scheduler's own methods were called. The scheduler imports both names from
`core.alerting` at the call, so patching the module is what it meets.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator, List, Optional, Set
from unittest.mock import patch


@dataclass
class Page:
    group: Optional[str]
    conditions: List[str]
    evidence: Optional[dict]


@dataclass
class Resolution:
    group: str
    still_firing: Set[str]


@dataclass
class AlertBoundary:
    pages: List[Page] = field(default_factory=list)
    resolutions: List[Resolution] = field(default_factory=list)

    def held(self, group: str) -> Optional[Set[str]]:
        """What the group's one resolution kept firing, or None when the job
        announced nothing for the group at all."""
        found = [r.still_firing for r in self.resolutions if r.group == group]
        assert len(found) <= 1, found
        return found[0] if found else None


@contextmanager
def alert_boundary(reached: int = 2) -> Iterator[AlertBoundary]:
    """Record every page and every resolution of the block; a page reaches
    `reached` admins."""
    record = AlertBoundary()

    async def raise_alert(text, *, conditions, bucket, group=None, evidence=None, **_kw):
        record.pages.append(Page(group, list(conditions), evidence))
        return reached

    async def resolve_group(group, still_firing=(), **_kw):
        record.resolutions.append(Resolution(group, set(still_firing)))
        return 0

    with patch("core.alerting.raise_alert", new=raise_alert), \
         patch("core.alerting.resolve_group", new=resolve_group):
        yield record
