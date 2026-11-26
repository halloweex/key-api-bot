"""A failed read of chain 7a's stored goals is a counted 503 and a page.

`GET /api/goals` reads chain 7a's `app.revenue_goals` first. Until step 14's
S14-07 a failure there was let through raw, an uncounted 500 that paged
nothing; since then `_goals_run` refuses it like every other goals read: the
counted 503 naming `goals`, published under `read_fallback_mode.refused`, and
the canary's `read_refused` page when it is the only read that fails.

That every route of `/expenses`, `/inventory`, `/goals` and the cohorts
answers 503 naming its surface when its engine fails, and the goal page's
writes too, is the sweep's in `tests/unit/test_read_fallback_http.py`
(`TestEveryRoute`); it replaced the walks of these tabs' modules that stood
here until 2026-11. The expenses' history gate is
`tests/unit/test_pg_expenses_read.py`'s, over Postgres.
"""
from __future__ import annotations


class TestChain7aAFailedReadPages:
    """An accepted change in failure mode (the review of S14-07, decided):
    a failed read of chain 7a's `app.revenue_goals` — `GET /api/goals`, the
    smart goal's goals block — was let through raw before S14-07, an
    uncounted 500 that paged nothing; it is the counted 503 naming `goals`
    now, published under `read_fallback_mode.refused`, and the canary pages
    `read_refused` (WARN) when it is the only read that fails. The same
    change S14-05 made for /marketing's goal line and S14-06 for the returns
    list. Mutation: read the chain's table raw again."""

    def test_only_the_stored_goals_failing_is_a_counted_503_and_a_page(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        from bot import canary
        from core import pg_goals_read

        async def fetch(sql, params):
            if "app.revenue_goals" in sql:
                raise OSError('relation "app.revenue_goals" does not exist')
            return []

        monkeypatch.setattr(pg_goals_read, "fetch", fetch)

        response = admin_client.get("/api/goals")
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "goals"

        health = admin_client.get("/api/health").json()
        assert health["read_fallback_mode"]["refused"]["goals"]["count"] == 1
        assert "read_fallbacks" not in health
        [(key, message)] = canary.check_read_refusals(health)
        assert key == "read_refused"
        assert message.startswith(canary.READ_REFUSED_LINE + "goals ×1")
