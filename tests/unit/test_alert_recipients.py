"""Alerts reach admins and nobody else — pinned, per the owner's audit ask.

The recipient universe: ADMIN_USER_IDS gets alerts; approved users get
exactly two business messages (the weekly report and the milestone
broadcast), each documented and deliberate; individual users get replies to
their own actions. Nothing else may widen an audience, and this test is what
makes widening one a decision instead of an accident.
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _calls_with_chat_ids(path):
    """(func_name, lineno) for every call passing a chat_ids= kwarg."""
    out = []
    tree = ast.parse((ROOT / path).read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "chat_ids":
                    name = getattr(node.func, "attr",
                                   getattr(node.func, "id", "?"))
                    out.append((name, node.lineno))
    return out


class TestAlertsGoToAdminsOnly:
    def test_the_only_recipient_override_is_the_weekly_report(self):
        """chat_ids= re-aims the HTTP senders; the weekly report is the one
        legitimate user (admins + approved, business content, its own
        audience rules in CLAUDE.md). A second override site is somebody
        widening an alert audience and must land here first.

        Three of them, and all three are the same message: the weekly
        report's delivery ladder sends the rich form, falls back to the card
        with the report as its caption, then to plain text. Adding a rung
        landed here, which is the guard working — the audience did not
        change, only the number of ways it is written to.
        """
        hits = []
        for path in ("core/scheduler.py", "core/duckdb_store.py",
                     "core/alerting.py", "core/alert_escalator.py",
                     "core/alert_archive.py", "core/prediction_service.py",
                     "bot/main.py", "bot/canary.py", "bot/memory_watch.py",
                     "web/routes/api/webhooks.py"):
            hits += [(path, *c) for c in _calls_with_chat_ids(path)]
        assert len(hits) == 3, hits
        assert {name for _, name, _ in hits} == {
            "send_rich_message_http",
            "send_admin_photo_http",
            "send_admin_message_http",
        }, hits
        assert all(p == "core/scheduler.py" for p, _, _ in hits), hits
        src = (ROOT / "core/scheduler.py").read_text().splitlines()
        for _, _, line in hits:
            context = "\n".join(src[line - 40:line])
            assert "weekly" in context.lower() or "report" in context.lower(), (
                f"chat_ids= at scheduler:{line} is not the weekly report"
            )

    def test_no_alert_module_reads_the_authorized_users_list(self):
        """The approved-users audience belongs to business messaging in the
        bot handlers and the weekly report; an alert module importing it
        would be one refactor away from paging two dozen phones."""
        for path in ("core/alerting.py", "core/alert_escalator.py",
                     "core/alert_archive.py", "core/telegram_alerts.py",
                     "bot/canary.py"):
            src = (ROOT / path).read_text()
            assert "authorized_users" not in src, path
            assert "read_approved_user_ids" not in src, path


class TestNoiseGuards:
    def test_test_buckets_never_summon_the_agent(self, monkeypatch, tmp_path):
        from core.alert_agent_spool import drop_task

        monkeypatch.setenv("KS_ALERT_AGENT", "1")
        monkeypatch.setenv("KS_ALERT_GATE_STATE_DIR", str(tmp_path))
        assert drop_task(["disk:WARN"], "test:format", "x") is None
        assert drop_task(["disk:WARN"], "deploy-test:x", "x") is None
        assert drop_task(["disk:WARN"], "disk:WARN", "x") is not None


class TestFlakyProbeDeferral:
    """The 05:15 freeze window: a first-probe health blip is confirmed by
    the next probe or forgotten — measured on the first real night, where a
    self-healing 4.5-minute block cost a page, a diagnosis and a resolve."""

    def _defer(self, keys, prev):
        from bot.canary import defer_flaky

        return defer_flaky(keys, prev)

    def test_a_fresh_health_blip_is_deferred(self):
        defer, prev = self._defer(["health_unreachable"], set())
        assert defer is True
        assert prev == {"health_unreachable"}

    def test_a_confirmed_health_failure_pages(self):
        defer, _ = self._defer(["health_unreachable"], {"health_unreachable"})
        assert defer is False

    def test_everything_else_pages_on_the_first_probe(self):
        for key in ("cert_expiring", "dq_stale:integrity",
                    "mirror_failing:bronze.orders",
                    "alerting_transport_failing"):
            defer, _ = self._defer([key], set())
            assert defer is False, key

    def test_a_mixed_result_pages_at_once(self):
        """A health blip beside a cert problem is not a blip."""
        defer, _ = self._defer(
            ["health_unreachable", "cert_expiring"], set())
        assert defer is False

    def test_a_clean_tick_forgets_the_history(self):
        defer, prev = self._defer([], {"health_unreachable"})
        assert defer is False and prev == set()
        # …so an outage after a clean tick defers once again: by design,
        # every *new* incident gets its confirmation probe.
        defer, _ = self._defer(["health_unreachable"], prev)
        assert defer is True
