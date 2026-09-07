"""Per-user tab access: the rule, the presets, and the bot's checklist.

What is being pinned here is one sentence — **the tab set decides what is
visible, the role decides what may be done inside it** — and the three states
the stored column carries: `None` for "as the role", a list for "exactly
these", and an empty list for "none of them", which is a decision somebody can
actually take from the admin page and must not be folded into the first.
"""
from __future__ import annotations

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from core import permissions as perm


# ─── the rule ────────────────────────────────────────────────────────────────


class TestTheOverrideNarrowsAndWidens:
    def test_none_changes_nothing(self):
        """Every account meant this before the column existed, and the 24 rows
        in production still do. If this ever stops holding, a deploy silently
        re-decides everybody's access."""
        base = perm.get_permissions_for_role("viewer")
        assert perm.apply_feature_override(base, None) == base

    def test_a_set_hides_every_tab_it_does_not_name(self):
        base = perm.get_permissions_for_role("viewer")
        narrowed = perm.apply_feature_override(base, ["traffic"])

        assert narrowed["traffic"]["view"] is True
        for key in perm.TAB_FEATURE_KEYS:
            if key != "traffic":
                assert narrowed[key]["view"] is False, key

    def test_it_can_open_a_tab_the_role_does_not_have(self):
        """This is how "give this person margin" works without making them an
        admin — and it is the half a pure intersection could not express."""
        base = perm.get_permissions_for_role("viewer")
        assert base["margin"]["view"] is False
        assert perm.apply_feature_override(base, ["margin"])["margin"]["view"] is True

    def test_it_never_grants_an_action(self):
        """Ticking `sms` for a viewer gives the roster sizes. It must not give
        the CSV of names and phone numbers, nor the send — those are `sms`
        edit, and no checkbox reaches an action."""
        base = perm.get_permissions_for_role("viewer")
        granted = perm.apply_feature_override(base, ["sms"])
        assert granted["sms"]["view"] is True
        assert granted["sms"]["edit"] is False

    def test_edit_survives_where_the_role_has_it(self):
        base = perm.get_permissions_for_role("admin")
        granted = perm.apply_feature_override(base, ["expenses"])
        assert granted["expenses"] == {"view": True, "edit": True, "delete": True}

    def test_a_hidden_tab_loses_its_actions_too(self):
        base = perm.get_permissions_for_role("admin")
        hidden = perm.apply_feature_override(base, ["traffic"])
        assert hidden["expenses"] == {"view": False, "edit": False, "delete": False}

    def test_features_that_are_not_tabs_are_untouched(self):
        """`user_management` is the admin pages, gated on the role. A tab set
        that could switch it off — or on — would make the role check
        advisory."""
        base = perm.get_permissions_for_role("admin")
        narrowed = perm.apply_feature_override(base, ["traffic"])
        for key in ("user_management", "analytics", "customers"):
            assert narrowed[key] == base[key], key

    def test_an_empty_set_is_no_tabs_at_all(self):
        base = perm.get_permissions_for_role("admin")
        nothing = perm.apply_feature_override(base, [])
        assert not any(nothing[key]["view"] for key in perm.TAB_FEATURE_KEYS)


# ─── the column ──────────────────────────────────────────────────────────────


class TestTheStoredValue:
    @pytest.mark.parametrize("raw,expected", [
        (None, None),                       # as the role
        ("", []),                           # deliberately nothing
        ("traffic", ["traffic"]),
        ("sms,traffic", ["traffic", "sms"]),   # returned in sidebar order
        ("traffic,traffic", ["traffic"]),
        ("traffic,not_a_tab", ["traffic"]),
    ])
    def test_parsing(self, raw, expected):
        assert perm.parse_features(raw) == expected

    def test_none_and_empty_survive_a_round_trip(self):
        """The distinction is the whole point of the column, and it has to hold
        through both directions or an admin's "no tabs" becomes "as the role"
        on the next read."""
        assert perm.serialize_features(None) is None
        assert perm.serialize_features([]) == ""
        assert perm.parse_features(perm.serialize_features(None)) is None
        assert perm.parse_features(perm.serialize_features([])) == []

    def test_unknown_keys_never_reach_the_column(self):
        assert perm.serialize_features(["traffic", "../etc"]) == "traffic"


# ─── the presets ─────────────────────────────────────────────────────────────


class TestThePresets:
    def test_every_preset_names_only_real_tabs(self):
        for name, features in perm.ACCESS_PRESETS.items():
            assert set(features) <= set(perm.TAB_FEATURE_KEYS), name

    def test_the_default_is_what_a_viewer_used_to_see(self):
        """`standard` is what an approval grants when the admin taps nothing,
        so it has to equal the access an approved account had the day before
        per-user tabs existed: every page a viewer's role opened.

        Computed from `ROLE_PERMISSIONS` rather than restated, so that changing
        the viewer role and forgetting the preset fails here instead of
        silently widening or narrowing every future approval."""
        viewer = perm.get_permissions_for_role("viewer")
        expected = [k for k in perm.TAB_FEATURE_KEYS if viewer[k]["view"]]
        assert list(perm.ACCESS_PRESETS[perm.DEFAULT_PRESET]) == expected

    def test_traffic_only_is_one_tab(self):
        assert perm.preset_features("traffic_only") == ["traffic"]

    def test_an_unknown_preset_is_none_rather_than_empty(self):
        """Empty means "no tabs" and would silently lock an account out; the
        caller has to be able to tell a typo from a decision."""
        assert perm.preset_features("nope") is None

    def test_full_is_every_tab(self):
        assert list(perm.ACCESS_PRESETS["full"]) == list(perm.TAB_FEATURE_KEYS)


class TestTheTabListItself:
    def test_admin_pages_are_not_grantable(self):
        assert perm.Feature.USER_MANAGEMENT.value not in perm.TAB_FEATURE_KEYS

    def test_every_tab_has_a_role_default_for_every_role(self):
        """A missing entry reads as denied, so a tab added to the enum and not
        to the matrix would be invisible to every DB-backed role — including
        admins. That is `seed_default_permissions`' lesson, one level up."""
        for role, features in perm.ROLE_PERMISSIONS.items():
            for tab in perm.TAB_FEATURES:
                assert tab in features, (role, tab)

    def test_every_tab_is_described(self):
        described = {f["key"] for f in perm.get_all_features() if f.get("tab")}
        assert described == set(perm.TAB_FEATURE_KEYS)

    def test_margin_is_the_admin_role_only_by_default(self):
        """It was `require_admin` before it was a permission. Nobody gained it
        the day that changed, and this is what says so."""
        for role in ("viewer", "editor", "marketer"):
            assert perm.get_permissions_for_role(role)["margin"]["view"] is False
        assert perm.get_permissions_for_role("admin")["margin"]["view"] is True


# ─── the bot's checklist ─────────────────────────────────────────────────────


class _FakeDashboard:
    """The port's dashboard aggregate, in memory."""

    def __init__(self, features=None, exists=True):
        self.features = features
        self.exists = exists
        self.granted = []

    def available(self):
        return True

    def get(self, user_id):
        if not self.exists:
            return None
        return {"user_id": user_id, "role": "viewer", "status": "approved",
                "allowed_features": self.features}

    def grant(self, user_id, admin_id, features=None, username=None,
              first_name=None, last_name=None):
        self.granted.append((user_id, list(features or [])))
        self.features = list(features or [])
        return True

    def set_features(self, user_id, features, admin_id):
        self.features = None if features is None else list(features)
        return True


@pytest.fixture
def handlers():
    from bot import handlers_legacy

    return handlers_legacy


class TestTheKeyboard:
    def test_it_draws_every_tab_and_every_preset(self, handlers):
        markup = handlers._tabs_keyboard(42, ["traffic"], "en")
        labels = [b.text for row in markup.inline_keyboard for b in row]
        data = [b.callback_data for row in markup.inline_keyboard for b in row]

        for key in perm.TAB_FEATURE_KEYS:
            assert any(d == f"atab:42:{key}" for d in data), key
        for name in perm.ACCESS_PRESETS:
            assert f"apre:42:{name}" in data, name
        assert "adone:42" in data
        assert sum(1 for text in labels if text.startswith("✅")) == 1

    def test_callback_data_fits_telegram(self, handlers):
        """64 bytes is the hard limit, and a tab key is not a number."""
        markup = handlers._tabs_keyboard(9_999_999_999, None, "en")
        for row in markup.inline_keyboard:
            for button in row:
                assert len(button.callback_data.encode()) <= 64, button.callback_data

    def test_ids_are_split_on_a_colon(self, handlers):
        """The neighbouring auth handlers read ids with `split('_')[-1]`, and
        a key like `user_management` would be read as the id. The separator is
        what keeps the two families apart."""
        markup = handlers._tabs_keyboard(7, ["traffic"], "en")
        for row in markup.inline_keyboard:
            for button in row:
                assert int(button.callback_data.split(":")[1]) == 7

    def test_the_summary_names_the_empty_set(self, handlers):
        assert handlers._tabs_summary([], "en") == "no tabs — this account sees nothing"
        assert "Traffic" in handlers._tabs_summary(["traffic"], "en")


@pytest.mark.asyncio
class TestTogglingATab:
    async def _run(self, handlers, monkeypatch, dashboard, data):
        query = SimpleNamespace(
            data=data,
            answer=AsyncMock(),
            edit_message_text=AsyncMock(),
        )
        update = SimpleNamespace(
            callback_query=query,
            effective_user=SimpleNamespace(id=1, language_code="en"),
        )
        monkeypatch.setattr(handlers, "is_admin", lambda uid: True)
        monkeypatch.setattr(handlers, "_lang", lambda _u: "en")
        monkeypatch.setattr(handlers.database, "get_user_auth_status", lambda uid: {})
        monkeypatch.setattr(
            handlers.database, "get_dashboard_access", dashboard.get)
        monkeypatch.setattr(
            handlers.database, "set_dashboard_features",
            lambda uid, features, admin: dashboard.set_features(uid, features, admin))
        return query, update

    async def test_a_tap_adds_one_tab(self, handlers, monkeypatch):
        dashboard = _FakeDashboard(["traffic"])
        query, update = await self._run(
            handlers, monkeypatch, dashboard, "atab:42:reports")

        await handlers.auth_toggle_tab(update, MagicMock())
        assert dashboard.features == ["traffic", "reports"]

    async def test_a_second_tap_removes_it(self, handlers, monkeypatch):
        dashboard = _FakeDashboard(["traffic", "reports"])
        query, update = await self._run(
            handlers, monkeypatch, dashboard, "atab:42:reports")

        await handlers.auth_toggle_tab(update, MagicMock())
        assert dashboard.features == ["traffic"]

    async def test_the_first_tap_starts_from_the_default_not_from_nothing(
        self, handlers, monkeypatch,
    ):
        """With no override stored, the first tick must not silently revoke
        every other tab the account was already seeing."""
        dashboard = _FakeDashboard(None)
        query, update = await self._run(
            handlers, monkeypatch, dashboard, "atab:42:margin")

        await handlers.auth_toggle_tab(update, MagicMock())
        expected = perm.normalize_features(
            [*perm.ACCESS_PRESETS[perm.DEFAULT_PRESET], "margin"])
        assert dashboard.features == expected

    async def test_a_preset_replaces_the_set(self, handlers, monkeypatch):
        dashboard = _FakeDashboard(["dashboard", "reports"])
        query, update = await self._run(
            handlers, monkeypatch, dashboard, "apre:42:traffic_only")

        await handlers.auth_apply_preset(update, MagicMock())
        assert dashboard.features == ["traffic"]

    async def test_done_puts_the_keyboard_away_and_saves_nothing(
        self, handlers, monkeypatch,
    ):
        dashboard = _FakeDashboard(["traffic"])
        query, update = await self._run(handlers, monkeypatch, dashboard, "adone:42")

        await handlers.auth_tabs_done(update, MagicMock())
        text = query.edit_message_text.await_args.args[0]
        assert "Traffic" in text
        assert query.edit_message_text.await_args.kwargs.get("reply_markup") is None

    async def test_a_non_admin_changes_nothing(self, handlers, monkeypatch):
        dashboard = _FakeDashboard(["traffic"])
        query, update = await self._run(
            handlers, monkeypatch, dashboard, "atab:42:margin")
        monkeypatch.setattr(handlers, "is_admin", lambda uid: False)

        await handlers.auth_toggle_tab(update, MagicMock())
        assert dashboard.features == ["traffic"]
        query.edit_message_text.assert_not_awaited()


# ─── the statements, against a real engine ───────────────────────────────────


@pytest.mark.asyncio
class TestTheSharedSqlRuns:
    """`core/dashboard_access.py` holds one SQL text for two containers.

    The bot runs it against Postgres, which no test here can reach; DuckDB is
    the engine available, and it is the same statement with the same two holes
    filled in. What this catches is the half that is not about permissions at
    all — `ON CONFLICT DO UPDATE` referring to the existing row, `RETURNING`,
    the parameter order — which is exactly the class of mistake that would
    otherwise surface as an admin tapping Approve and getting nothing.
    """

    async def _store(self, tmp_path):
        from core.duckdb_store import DuckDBStore

        store = DuckDBStore(db_path=tmp_path / "users.duckdb")
        await store.connect()
        return store

    async def test_a_grant_creates_an_approved_row_with_its_tabs(self, tmp_path):
        from core.dashboard_access import GRANT_ACCESS, SELECT_ACCESS, access_row

        store = await self._store(tmp_path)
        try:
            await store._users_run(
                GRANT_ACCESS,
                [7, "tester", "Test", None, "viewer", "traffic", 1],
                mode="none",
            )
            row = access_row(
                await store._users_run(SELECT_ACCESS, [7], mode="one"))
            assert row["status"] == "approved"
            assert row["allowed_features"] == ["traffic"]
        finally:
            await store.close()

    async def test_a_second_grant_keeps_the_role_and_the_name(self, tmp_path):
        """Approving is not a statement about what somebody *is*: re-approving
        an admin whose request row came back to pending must not demote them,
        and Telegram dropping a field must not blank the stored name."""
        from core.dashboard_access import GRANT_ACCESS, SELECT_ACCESS, access_row

        store = await self._store(tmp_path)
        try:
            await store._users_run(
                GRANT_ACCESS, [7, "tester", "Test", None, "viewer", None, 1],
                mode="none")
            await store.update_user_role(7, "admin", changed_by=1)
            await store._users_run(
                GRANT_ACCESS, [7, None, None, None, "viewer", "traffic", 1],
                mode="none")

            row = access_row(
                await store._users_run(SELECT_ACCESS, [7], mode="one"))
            assert row["role"] == "admin"
            assert row["username"] == "tester"
            assert row["allowed_features"] == ["traffic"]
        finally:
            await store.close()

    async def test_set_features_round_trips_all_three_states(self, tmp_path):
        store = await self._store(tmp_path)
        try:
            await store.create_user(7, username="tester", status="approved")

            assert await store.set_user_features(7, ["traffic"], changed_by=1)
            assert (await store.get_user(7))["allowed_features"] == ["traffic"]

            assert await store.set_user_features(7, [], changed_by=1)
            assert (await store.get_user(7))["allowed_features"] == []

            assert await store.set_user_features(7, None, changed_by=1)
            assert (await store.get_user(7))["allowed_features"] is None
        finally:
            await store.close()

    async def test_setting_the_tabs_of_a_missing_user_says_so(self, tmp_path):
        store = await self._store(tmp_path)
        try:
            assert await store.set_user_features(404, ["traffic"], changed_by=1) is False
        finally:
            await store.close()


class TestTheHandlersAreReachable:
    def test_every_tab_callback_is_registered(self):
        """A handler nobody routed to is a button that does nothing, which is
        exactly how a keyboard fails silently."""
        import bot.main as main

        source = inspect.getsource(main)
        for name, pattern in (
            ("auth_toggle_tab", "atab:"),
            ("auth_apply_preset", "apre:"),
            ("auth_tabs_done", "adone:"),
        ):
            assert f"handlers.{name}" in source, name
            assert pattern in source, pattern


# ─── the push side ───────────────────────────────────────────────────────────


class _Socket:
    def __init__(self):
        self.sent = []

    async def accept(self):
        return None

    async def send_text(self, text):
        self.sent.append(text)


@pytest.mark.asyncio
class TestBroadcastsRespectTheTabSet:
    """The REST surface is gated per tab; the push surface was not.

    Nothing leaked when this was written — the dashboard room carries counts
    (`orders_synced`, `products_synced`, `inventory_updated {stocks_count}`)
    and `goal_progress`, the one event whose payload is revenue, has no emitter
    anywhere. The scope exists so that the day somebody writes that emitter,
    the answer is already made.
    """

    async def _manager(self):
        from core.websocket_manager import ConnectionManager

        return ConnectionManager()

    async def test_an_unscoped_event_reaches_everybody(self):
        manager = await self._manager()
        narrow = _Socket()
        await manager.connect(narrow, room="dashboard", features={"traffic"})

        sent = await manager.broadcast("dashboard", "orders_synced", {"count": 3})
        assert sent == 1, "a sync counter must still refresh a traffic-only page"

    async def test_a_scoped_event_skips_a_connection_without_the_tab(self):
        manager = await self._manager()
        narrow = _Socket()
        await manager.connect(narrow, room="dashboard", features={"traffic"})

        sent = await manager.broadcast(
            "dashboard", "goal_progress", {"revenue": 1_000_000},
            feature="dashboard",
        )
        assert sent == 0
        assert not any("goal_progress" in message for message in narrow.sent)

    async def test_a_scoped_event_reaches_a_connection_with_it(self):
        manager = await self._manager()
        wide = _Socket()
        await manager.connect(wide, room="dashboard", features={"dashboard", "traffic"})

        sent = await manager.broadcast(
            "dashboard", "goal_progress", {"revenue": 1_000_000},
            feature="dashboard",
        )
        assert sent == 1

    async def test_an_unresolved_connection_is_not_silently_starved(self):
        """`features=None` means the route could not resolve them — an admin
        socket, or a failure. It must not turn into "sees nothing", which would
        make a live-update feature fail closed and invisibly."""
        manager = await self._manager()
        unknown = _Socket()
        await manager.connect(unknown, room="dashboard", features=None)

        assert await manager.broadcast(
            "dashboard", "goal_progress", {}, feature="dashboard",
        ) == 1


class TestTheRevenueEventIsScoped:
    def test_goal_progress_names_its_tab(self):
        """The audit's finding, pinned: a broadcast carrying money must say
        which tab it belongs to."""
        import inspect
        import web.main as main

        source = inspect.getsource(main._register_event_handlers)
        block = source[source.index("GOAL_PROGRESS"):]
        assert 'feature="dashboard"' in block[:400]
