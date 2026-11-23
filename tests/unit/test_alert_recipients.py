"""Alerts reach admins and nobody else — pinned, per the owner's audit ask.

The recipient universe: ADMIN_USER_IDS gets alerts; approved users get
exactly two business messages (the weekly report and the milestone
broadcast), each documented and deliberate; the traffic report and the
Monday question about promotions go to the admins and a list a human typed;
individual users get replies to their own actions. Nothing else may widen an
audience, and this test is what makes widening one a decision instead of an
accident.
"""
import ast
import textwrap
from pathlib import Path

import pytest

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


def _binders(fn: ast.AST, name: str):
    """Every node inside `fn` that binds `name`: an assignment of any kind
    (plain, augmented, annotated, walrus), a loop, `with`, comprehension or
    `del` target, an import, an argument, an `except … as`, a `global` or
    `nonlocal`, a nested definition, a pattern capture."""
    for node in ast.walk(fn):
        if isinstance(node, ast.Name) and node.id == name and isinstance(
                node.ctx, (ast.Store, ast.Del)):
            yield node
        elif isinstance(node, ast.alias) and (node.asname or node.name) == name:
            yield node
        elif isinstance(node, ast.arg) and node.arg == name:
            yield node
        elif isinstance(node, ast.ExceptHandler) and node.name == name:
            yield node
        elif isinstance(node, (ast.Global, ast.Nonlocal)) and name in node.names:
            yield node
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) \
                and node is not fn and node.name == name:
            yield node
        elif isinstance(node, (ast.MatchAs, ast.MatchStar)) and node.name == name:
            yield node
        elif isinstance(node, ast.MatchMapping) and node.rest == name:
            yield node


def _assert_written_to_its_audience_alone(fn: ast.AST) -> None:
    """Dataflow, not text: `readers` is bound once, by `readers =
    audience(...)`, and every `chat_ids=` in the function is the second loop
    variable of a `for … in group_by_language(readers, …).items()` that holds
    the call — bound nowhere else — with no `**` splat beside it."""
    parents = {child: node for node in ast.walk(fn) for child in ast.iter_child_nodes(node)}

    readers = list(_binders(fn, "readers"))
    assert len(readers) == 1, (
        f"`readers` is bound {len(readers)} times in {getattr(fn, 'name', fn)}: "
        "only `readers = audience(...)` may")
    assign = parents.get(readers[0])
    assert isinstance(assign, ast.Assign) and assign.targets == [readers[0]], (
        "`readers` is not bound by a plain `readers = …`")
    assert isinstance(assign.value, ast.Call) and isinstance(assign.value.func, ast.Name) \
        and assign.value.func.id == "audience", "`readers` is not `audience(...)`"

    sends = [node for node in ast.walk(fn) if isinstance(node, ast.Call)
             and any(kw.arg == "chat_ids" for kw in node.keywords)]
    assert sends, "no send with chat_ids= found"
    for call in ast.walk(fn):
        if isinstance(call, ast.Call):
            assert all(kw.arg is not None for kw in call.keywords), (
                f"a `**` splat at line {call.lineno} could carry chat_ids")
    for call in sends:
        (value,) = [kw.value for kw in call.keywords if kw.arg == "chat_ids"]
        assert isinstance(value, ast.Name), f"chat_ids= at line {call.lineno} is not a name"
        bound = list(_binders(fn, value.id))
        assert len(bound) == 1, f"`{value.id}` is bound {len(bound)} times"
        target = parents.get(bound[0])
        loop = parents.get(target) if isinstance(target, ast.Tuple) else None
        assert isinstance(loop, (ast.For, ast.AsyncFor)) and loop.target is target \
            and len(target.elts) == 2 and target.elts[1] is bound[0], (
                f"`{value.id}` is not the value of a `for key, value in …` loop")
        iterated = loop.iter
        assert isinstance(iterated, ast.Call) and isinstance(iterated.func, ast.Attribute) \
            and iterated.func.attr == "items" and not iterated.args, (
                "the loop is not over a mapping's `.items()`")
        buckets = iterated.func.value
        assert isinstance(buckets, ast.Call) and isinstance(buckets.func, ast.Name) \
            and buckets.func.id == "group_by_language" and buckets.args \
            and isinstance(buckets.args[0], ast.Name) and buckets.args[0].id == "readers", (
                "the loop is not over `group_by_language(readers, …)`")
        assert any(node is call for stmt in loop.body for node in ast.walk(stmt)), (
            f"the send at line {call.lineno} is outside the loop binding `{value.id}`")


_AUDIENCE = "readers = audience(ADMIN_USER_IDS)\n"
_SEND_LOOP = """\
for lang, recipients in group_by_language(readers, defaults).items():
    if lang:
        await send_admin_message_http('x', chat_ids=recipients)
"""
# What a widening looks like with the line of text the old guard looked for
# still in place.
_WIDENED = {
    "augmented": _AUDIENCE + "readers += approved_users()\n" + _SEND_LOOP,
    "rebound": _AUDIENCE + "readers = readers + approved_users()\n" + _SEND_LOOP,
    "walrus": _AUDIENCE + "if (readers := approved_users()):\n    pass\n" + _SEND_LOOP,
    "loop_target": _AUDIENCE + "for readers in [approved_users()]:\n    pass\n" + _SEND_LOOP,
    "chat_ids_elsewhere": _AUDIENCE + _SEND_LOOP.replace(
        "chat_ids=recipients", "chat_ids=approved_users()"),
    "other_buckets": _AUDIENCE + _SEND_LOOP.replace(
        "group_by_language(readers, defaults)", "group_by_language(approved_users(), defaults)"),
    "rebound_in_the_loop": _AUDIENCE + _SEND_LOOP.replace(
        "        await send", "        recipients = approved_users()\n        await send"),
    "send_after_the_loop": _AUDIENCE + _SEND_LOOP
    + "await send_admin_message_http('x', chat_ids=recipients)\n",
    "the_keys": _AUDIENCE + _SEND_LOOP.replace("for lang, recipients in", "for recipients, lang in"),
    "a_splat": _AUDIENCE + _SEND_LOOP.replace("chat_ids=recipients", "chat_ids=recipients, **extra"),
}


def _job(body: str) -> ast.AsyncFunctionDef:
    """`run_weekly_ask` with `body`, as the guard reads it."""
    source = "async def run_weekly_ask():\n" + textwrap.indent(body, "    ")
    (fn,) = ast.parse(source).body
    assert isinstance(fn, ast.AsyncFunctionDef)
    return fn


class TestAlertsGoToAdminsOnly:
    def test_the_only_recipient_overrides_are_the_reports_and_the_promotions_question(self):
        """chat_ids= re-aims the HTTP senders; the weekly report is the one
        legitimate user (admins + approved, business content, its own
        audience rules in CLAUDE.md). A second override site is somebody
        widening an alert audience and must land here first.

        Five of them, across two reports. The weekly sales report's ladder is
        three — the rich form, the card with the report as its caption, then
        plain text. The weekly traffic report's is two.

        The traffic report's two **can widen past the admins**, and that is
        the one place in this repository where an override does. It used to
        be a strict subset — the admin list split by language so nobody was
        sent somebody else's — and `KS_TRAFFIC_REPORT_RECIPIENTS` was added
        on 2026-09-08 so a curator can be added without a deploy. The guard
        does not forbid that; it forbids it happening quietly. What makes it
        acceptable is that the widening is an explicit list of Telegram ids
        typed by a human, empty by default, and applied by one function
        (`core.traffic_report.audience`) that no alert path calls —
        never a rule inferred from dashboard permissions, which is how
        "everyone who can open the tab" would have become seventeen of
        eighteen accounts. `tests/unit/test_traffic_report.py` pins its
        behaviour; the audience decision itself is in CLAUDE.md.

        All of it landed here first, which is the guard working.

        The sixth is the Monday question about promotions
        (`core/promo_asks.py`, revision 0039), and it widens past the admins
        the way the traffic report does and for its reason:
        `KS_PROMO_ASK_RECIPIENTS` is an explicit list of Telegram ids typed
        by a human, empty by default, applied by one function
        (`core.promo_asks.audience`) that no alert path calls — never "who
        holds the marketing tab" — and only to those the bot approves. It
        carries a question and two buttons, no figures, and an answer is
        accepted only from an approved recipient. What is checked here is the
        dataflow (`_assert_written_to_its_audience_alone`): every `chat_ids=`
        in the job is a bucket of `audience(...)`'s answer, so a send cannot
        be widened beside a line that still reads right.
        `tests/unit/test_promo_asks.py` pins `audience` itself.
        """
        hits = []
        for path in ("core/scheduler.py", "core/store.py",
                     "core/alerting.py", "core/alert_escalator.py",
                     "core/alert_archive.py", "core/prediction_service.py",
                     "core/promo_asks.py",
                     "bot/main.py", "bot/canary.py", "bot/memory_watch.py",
                     "web/routes/api/webhooks.py"):
            hits += [(path, *c) for c in _calls_with_chat_ids(path)]
        assert len(hits) == 6, hits
        assert {name for _, name, _ in hits} == {
            "send_rich_message_http",
            "send_admin_photo_http",
            "send_admin_message_http",
        }, hits
        reports = [hit for hit in hits if hit[0] == "core/scheduler.py"]
        assert len(reports) == 5, hits
        src = (ROOT / "core/scheduler.py").read_text().splitlines()
        for _, _, line in reports:
            context = "\n".join(src[line - 40:line])
            assert "weekly" in context.lower() or "report" in context.lower(), (
                f"chat_ids= at scheduler:{line} is not the weekly report"
            )

        (question,) = [hit for hit in hits if hit[0] == "core/promo_asks.py"]
        assert question[1] == "send_admin_message_http", question
        tree = ast.parse((ROOT / "core/promo_asks.py").read_text())
        (sender,) = [fn for fn in ast.walk(tree)
                     if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef))
                     and fn.lineno <= question[2] <= fn.end_lineno]
        assert sender.name == "run_weekly_ask"
        _assert_written_to_its_audience_alone(sender)

    @pytest.mark.parametrize("body", list(_WIDENED.values()), ids=list(_WIDENED))
    def test_the_dataflow_guard_refuses_a_widened_question(self, body):
        """The guard above reads where `chat_ids` comes from, not whether a
        line of text is present: each body keeps `readers =
        audience(ADMIN_USER_IDS)` and widens the send anyway."""
        with pytest.raises(AssertionError):
            _assert_written_to_its_audience_alone(_job(body))

    def test_the_dataflow_guard_takes_the_shape_it_guards(self):
        _assert_written_to_its_audience_alone(_job(_AUDIENCE + _SEND_LOOP))

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
