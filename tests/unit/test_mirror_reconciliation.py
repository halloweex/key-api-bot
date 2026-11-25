"""What is left of `core.mirror_reconciliation` that needs no Postgres: the
switch every check of the `mirror_landing` layer asks first, and the layer's
wiring.

`compare_table` was pinned here, on the catalogue's specs, for as long as a
comparison ran it: Reconciliation A until step 14's S14-12, then
`reconcile_bot_state` (`data/bot.db` against `app.*`). That one stood down
under `KS_BOT_STORE=postgres` from 2026-08-27 and was deleted on 2026-11-19
with the copy it compared, and `compare_table` and its tests with it. The
checks that remain read Postgres alone, and their findings are run on a real
database in `tests/integration/`.
"""
from __future__ import annotations

import pytest

from core.data_quality import (
    DIGEST_MAX_AGE_HOURS,
    WATCHED_LAYERS,
)
from core.mirror_reconciliation import (
    MIRROR_LAYER,
    configured,
)


class TestTheKillSwitch:
    def test_configured_is_false_without_a_dsn(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert configured() is False
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        assert configured() is True


class TestThePostgresAloneChecksAskForAPostgres:
    """Step 14's S14-13 re-gated the four checks that asked
    `pg_landing.enabled()` — the landing mirror, deleted there — on
    `configured()`: they read Postgres alone, so a Postgres is what they
    need. Without `KS_PG_DSN` each answers nothing and asks nothing (a
    morning's run on a laptop must not persist a failed layer); with it, each
    reaches Postgres whatever `KS_MIRROR_LANDING` says. Three are left:
    `reconcile_bot_state` went on 2026-11-19. Mutation: put
    `if not pg_landing.enabled()` back in any of them, and the first test
    fails for it."""

    CHECKS = (
        "reconcile_order_utm_completeness",
        "reconcile_buyer_completeness",
        "reconcile_order_versions",
    )

    @staticmethod
    def _call(name):
        import core.mirror_reconciliation as mr

        return getattr(mr, name)()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", CHECKS)
    async def test_without_a_postgres_it_asks_nothing(self, name, monkeypatch):
        from unittest.mock import AsyncMock

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        asked = AsyncMock(side_effect=AssertionError("reached Postgres"))
        monkeypatch.setattr("core.pg.get_pool", asked)
        assert await self._call(name) == []
        asked.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mirror", ["0", "off", "garbage", ""])
    @pytest.mark.parametrize("name", CHECKS)
    async def test_with_one_it_asks_whatever_the_mirror_flag_says(
            self, name, mirror, monkeypatch):
        from unittest.mock import AsyncMock

        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        monkeypatch.setenv("KS_MIRROR_LANDING", mirror)
        asked = AsyncMock(side_effect=RuntimeError("reached Postgres"))
        monkeypatch.setattr("core.pg.get_pool", asked)
        with pytest.raises(RuntimeError, match="reached Postgres"):
            await self._call(name)
        asked.assert_awaited_once()


class TestWiring:
    def test_the_layer_is_watched(self):
        """Absent from WATCHED_LAYERS, the catch-up re-queues it every restart."""
        assert MIRROR_LAYER in WATCHED_LAYERS
        assert MIRROR_LAYER in DIGEST_MAX_AGE_HOURS

    def test_the_job_is_registered_daily_and_clear_of_the_sunday_cron(self):
        from core.scheduler import CATCHUP_CHECKS

        assert CATCHUP_CHECKS["dq_mirror_landing"][0] == MIRROR_LAYER
