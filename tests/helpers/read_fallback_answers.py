"""The functions that answer a read refusal with a named answer, as the
consumers' guard lists them.
"""
from __future__ import annotations

from typing import Set


_TOOLS = "core/chat_tools.py:execute_tool:named"


ANSWERS = {
    "core/scheduler.py:BackgroundScheduler._run_seasonality_calc": {
        "core/scheduler.py:BackgroundScheduler._run_seasonality_calc:named"},
    "core/scheduler.py:BackgroundScheduler._run_traffic_report": {
        "core/scheduler.py:BackgroundScheduler._run_traffic_report:named"},
    "core/scheduler.py:BackgroundScheduler._run_month_goal_freeze": {
        "core/scheduler.py:BackgroundScheduler._run_month_goal_freeze:named"},
}


# The named answers step 14's S14-07 left with no refusal to answer: the
# readers behind them stopped asking a switch (`no_address`), and with no
# fallback they fail with their own error, which these handlers never named.
# The plan keeps them through S14-16 ("they must stay alive": the
# scheduler's, the sync's and the forecast's here; the chat's tools still
# answer one, in `UNSWEPT_STOPS`), so they are pinned: the HTTP rule that a
# handler naming the refusal must raise it exempts exactly these, and only
# while the entries below alone reach them.
KEPT_ANSWERS = {
    "core/scheduler.py:BackgroundScheduler._run_weekly_report",
    "core/scheduler.py:BackgroundScheduler._run_revenue_prediction",
    "core/scheduler.py:BackgroundScheduler._run_meilisearch_sync",
    "core/sync_service.py:SyncService.incremental_sync",
}


# The routes the sweep does not run, and where a refusal stops on each before
# the 503 handler — empty means it reaches the handler. The assistant's stop
# at its tools, by design (`IN_BAND_ROUTES`). `POST /sync/sync-buyers`
# reaches the handler: the buyers step passes a refusal on and the route lets
# it through (chain 4's PR-3). Until then the step's `except Exception`
# answered it, and the route said "Buyer sync failed" with a 500.
UNSWEPT_STOPS = {
    "DELETE /api/goals/{period_type}": set(),
    "GET /api/chat/stream": {_TOOLS},
    "POST /api/chat": {_TOOLS},
    "POST /api/goals": set(),
    "POST /api/goals/recalculate": set(),
}


def named_answer_functions() -> Set[str]:
    """`file:function` of every named answer in `ANSWERS` and
    `UNSWEPT_STOPS`, and of those kept with nothing to answer
    (`KEPT_ANSWERS`)."""
    return {entry.rsplit(":", 1)[0]
            for pins in (ANSWERS, UNSWEPT_STOPS)
            for stops in pins.values() for entry in stops
            if entry.endswith(":named")} | KEPT_ANSWERS
