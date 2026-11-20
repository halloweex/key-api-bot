"""The daily mirror_landing job, each check replaced by a stub."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch


# Every check, in the order the job runs it, named as it is imported.
CHECKS_BY_MODULE = {
    "core.mirror_reconciliation": [
        "reconcile_order_utm_completeness", "pg_gold_internal_check",
        "reconcile_order_versions",
        "reconcile_buyer_completeness",
    ],
    "core.ch_silver": ["reconcile_clickhouse"],
    "core.ch_history": ["reconcile_ch_history"],
}


async def _run(outcomes, mocks=None, held=None, hook=None, returned=None):
    """Run the real job with every check stubbed to `outcomes[name]`.

    An outcome is a list of issues, or an Exception to raise. Returns what the
    job persisted, what it paged, and whether it announced a recovery. Pass a
    dict as `mocks` to get each check's stub back by name, to ask how the job
    called it, a list as `held` to collect the conditions each resolution
    was told it could not re-examine (`unverified`), and a callable as `hook`
    to break something after the stubs are in place and before the job runs,
    and a list as `returned` to get what the job returned.
    """
    from core.scheduler import BackgroundScheduler

    persisted, paged, resolved = {}, [], []

    # What the job hands the journal's door. Until step 14's S14-10 this
    # module routed chain 9 back to DuckDB and caught DuckDB's `persist_run`
    # behind the door instead.
    async def _journal_run(**kw):
        persisted.update(kw)
        return 1

    patches = [
        patch("core.mirror_reconciliation.configured", return_value=True),
        patch("core.dq_journal.journal_run", new=_journal_run),
        patch("core.data_quality.machine_attempts_note", return_value=None),
        patch("core.data_quality.evidence_for_agent", return_value={}),
    ]
    for module, names in CHECKS_BY_MODULE.items():
        for name in names:
            outcome = outcomes.get(name, [])
            mock = (AsyncMock(side_effect=outcome) if isinstance(outcome, Exception)
                    else AsyncMock(return_value=list(outcome)))
            if mocks is not None:
                mocks[name] = mock
            patches.append(patch(f"{module}.{name}", new=mock))

    scheduler = BackgroundScheduler.__new__(BackgroundScheduler)

    async def _page(layer, message, key=None, conditions=(), evidence=None):
        paged.append(list(conditions))
        return True

    async def _resolve(layer, issues, error_message, unverified=()):
        # The real method returns early on error_message; record whether it
        # would have announced anything.
        if held is not None:
            held.append(list(unverified))
        if not error_message:
            resolved.append([i.check_name for i in issues])

    scheduler._send_dq_alert_throttled = _page
    scheduler._resolve_dq_layer = _resolve

    for p_ in patches:
        p_.start()
    try:
        if hook is not None:
            hook()
        result = await scheduler._run_dq_mirror_landing()
        if returned is not None:
            returned.append(result)
    finally:
        for p_ in reversed(patches):
            p_.stop()
    return persisted, paged, resolved
