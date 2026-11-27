"""The daily mirror_landing job, each check stubbed, read at its boundaries.

Every check is replaced by a stub that returns its findings or raises; the
journal's door records the run it is handed; pages and resolutions are read
at `core.alerting` (`tests/helpers/recon_chains_alerts.py`). Everything in
between — the job, `_send_dq_alert_throttled`, `_resolve_dq_layer` — runs as
production runs it.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Set
from unittest.mock import AsyncMock, patch

from tests.helpers.mirror_landing import CHECKS_BY_MODULE
from tests.helpers.recon_chains_alerts import alert_boundary

GROUP = "dq:mirror_landing"

# Every check, in the order the job asks them.
ALL_CHECKS = [name for names in CHECKS_BY_MODULE.values() for name in names]


@dataclass
class MirrorRun:
    persisted: Dict[str, Any]      # what the run was journalled with
    pages: List[List[str]]         # each page's conditions
    held: Optional[Set[str]]       # what the layer's resolution kept firing; None: no resolution
    result: Dict[str, Any]         # what the job returned
    checks: Dict[str, AsyncMock]   # each check's stub, by name


async def run_mirror_landing(
    outcomes: Mapping[str, Any], *, hook: Optional[Callable[[], None]] = None,
) -> MirrorRun:
    """Run the real job with every check stubbed to `outcomes[name]`: a list
    of findings, or an exception to raise (a check not named finds nothing).
    `hook` runs after the stubs are in place and before the job."""
    from core.scheduler import BackgroundScheduler

    persisted: Dict[str, Any] = {}

    async def journal_run(**kwargs):
        persisted.update(kwargs)
        return 1

    patches = [
        patch("core.mirror_reconciliation.configured", return_value=True),
        patch("core.dq_journal.journal_run", new=journal_run),
    ]
    checks: Dict[str, AsyncMock] = {}
    for module, names in CHECKS_BY_MODULE.items():
        for name in names:
            outcome = outcomes.get(name, [])
            checks[name] = (AsyncMock(side_effect=outcome)
                            if isinstance(outcome, BaseException)
                            else AsyncMock(return_value=list(outcome)))
            patches.append(patch(f"{module}.{name}", new=checks[name]))

    scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
    with alert_boundary() as alerts:
        for p in patches:
            p.start()
        try:
            if hook is not None:
                hook()
            result = await scheduler._run_dq_mirror_landing()
        finally:
            for p in reversed(patches):
                p.stop()
    return MirrorRun(
        persisted=persisted,
        pages=[page.conditions for page in alerts.pages if page.group == GROUP],
        held=alerts.held(GROUP),
        result=result,
        checks=checks,
    )
