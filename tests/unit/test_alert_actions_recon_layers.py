"""The reconciliation pages carry their "recheck now" button again (L4-07).

`dq_recheck` declared `integrity`, `reconciliation` and `mirror_landing`; the
05:30 job's layers have been `reconciliation_pg` and `reconciliation_ch`
since step 14's S14-12, and `buttons_for` drops a subject it does not know,
so their pages went out with no button. Both map to `dq_reconciliation` now,
and `reconciliation` stays so a tap on an older message still resolves.
"""
from __future__ import annotations

import re

import pytest

from core.alert_actions import (
    CALLBACK_PATTERN, buttons_for, callback_for, job_for, parse_callback,
)


@pytest.mark.parametrize("layer", ["reconciliation_pg", "reconciliation_ch"])
def test_a_reconciliation_page_has_its_button(layer):
    """Mutation: drop the layer from `DQ_LAYERS` — buttons_for gives []."""
    [(label, data)] = buttons_for(("dq_recheck",), layer)
    assert "Перепроверить" in label
    assert re.match(CALLBACK_PATTERN, data)
    spec, subject = parse_callback(data)
    assert spec.key == "dq_recheck" and subject == layer
    assert job_for("dq_recheck", layer) == "dq_reconciliation"


def test_an_old_tap_still_resolves():
    data = callback_for("dq_recheck", "reconciliation")
    assert parse_callback(data)[1] == "reconciliation"
    assert job_for("dq_recheck", "reconciliation") == "dq_reconciliation"


def test_every_watched_layer_has_a_job():
    from core.data_quality import WATCHED_LAYERS

    for layer in WATCHED_LAYERS:
        assert job_for("dq_recheck", layer), layer
