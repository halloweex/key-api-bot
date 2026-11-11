"""`/api/health/data-quality` shows the layers that are written (L4-06).

Its body named its layers by hand: it went on showing `reconciliation` —
DuckDB's arm of the 05:30 job, deleted in step 14's S14-12, so its last run
was weeks old — and never showed `reconciliation_ch`. It is built from
`data_quality.WATCHED_LAYERS` now: issues for the integrity and landing
checks, diffs for both reconciliation arms.
"""
from __future__ import annotations

import pytest

from core import dq_journal
from core.data_quality import WATCHED_LAYERS
from tests.integration.test_dq_journal_pg import DSN, NOW, _kwargs, pool  # noqa: F401

pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")


@pytest.mark.asyncio
async def test_every_watched_layer_and_only_those(pool):  # noqa: F811
    """Mutation: name the layers by hand again — `reconciliation` comes back
    and `reconciliation_ch` goes."""
    for layer in ("integrity", "mirror_landing", "reconciliation_pg",
                  "reconciliation_ch", "reconciliation"):
        await dq_journal.journal_run(**_kwargs(layer=layer))

    body = await dq_journal.health_runs()

    assert set(body) == set(WATCHED_LAYERS)
    assert "reconciliation" not in body
    for layer in ("reconciliation_pg", "reconciliation_ch"):
        assert set(body[layer]) == {"last_run", "diffs"}
        assert body[layer]["last_run"]["layer"] == layer
        assert [d["diff_class"] for d in body[layer]["diffs"]] == ["MISSING_IN_DK"]
    for layer in ("integrity", "mirror_landing"):
        assert set(body[layer]) == {"last_run", "issues"}
        assert [i["check_name"] for i in body[layer]["issues"]] == ["pk_uniqueness_orders"]


@pytest.mark.asyncio
async def test_a_layer_never_run_is_none_with_nothing_under_it(pool):  # noqa: F811
    body = await dq_journal.health_runs()
    assert body["reconciliation_ch"] == {"last_run": None, "diffs": []}
    assert body["integrity"] == {"last_run": None, "issues": []}
