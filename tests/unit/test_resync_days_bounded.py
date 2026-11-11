"""The resync's window is bounded before it takes anything (L3-10).

`POST /api/sync/resync?days=1000000000` took the heavy lock, ran the
catalogue sync and then answered 500 with the error's text, the date
arithmetic having overflowed. `days` is held to 1..`MAX_RANGE_DAYS` by
FastAPI now. (It was asked under both names of the door until the final
stage removed `/api/duckdb/resync`.)
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from core.filters import MAX_RANGE_DAYS
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture


@pytest.mark.parametrize("path", ["/api/sync/resync"])
@pytest.mark.parametrize("days", [0, MAX_RANGE_DAYS + 1, 10 ** 9])
def test_a_window_past_the_bound_is_a_422_and_takes_nothing(admin_client, path, days):  # noqa: F811
    with patch("core.sync_service.force_resync", new=AsyncMock()) as resync:
        response = admin_client.post(f"{path}?days={days}")
    assert response.status_code == 422, response.text
    resync.assert_not_awaited()
