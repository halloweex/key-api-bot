"""The boundary between an untrusted diagnosis and code that runs.

Every test here defends one sentence: the agent names a key, never a command.
"""
import pytest

from core.alert_actions import (
    ACTIONS,
    CALLBACK_MAX_BYTES,
    DQ_LAYERS,
    buttons_for,
    callback_for,
    parse_callback,
)


class TestTheRegistryIsTheBoundary:
    def test_every_action_declares_its_subjects(self):
        """An action that accepted anything would be a command with an
        argument, which is the thing this design exists to avoid."""
        for key, spec in ACTIONS.items():
            assert spec.key == key
            assert spec.subjects, f"{key} accepts any subject"
            assert spec.label.strip()
            assert spec.ack.strip()

    def test_nothing_here_deletes_or_repairs(self):
        """Rule 5 of the charter, and the reasons written in the module: a
        mirrored table's 'repair' writes the only record of a change from the
        only other record of it, and the compact stops both containers."""
        forbidden = ("prune", "delete", "drop", "compact", "repair", "rm")
        for key, spec in ACTIONS.items():
            assert not any(w in key for w in forbidden), key

    def test_callback_data_fits_telegrams_ceiling(self):
        """Over-long callback_data is rejected when the keyboard is sent, so
        the cost is the whole alert, not the button."""
        for key, spec in ACTIONS.items():
            for subject in spec.subjects:
                data = callback_for(key, subject)
                assert len(data.encode()) <= CALLBACK_MAX_BYTES, data


class TestParsingRefusesWhatItDoesNotKnow:
    def test_a_known_pair_round_trips(self):
        spec, subject = parse_callback("afix:dq_recheck:mirror_landing")
        assert spec.key == "dq_recheck"
        assert subject == "mirror_landing"

    @pytest.mark.parametrize("data", [
        "afix:dq_recheck:not_a_layer",      # subject not declared
        "afix:rm_rf:mirror_landing",        # action not in the registry
        "afix:dq_recheck",                  # wrong shape
        "afix:dq_recheck:a:b",              # extra segment
        "atab:dq_recheck:mirror_landing",   # someone else's prefix
        "afix:dq_recheck:mirror landing",   # space
        "afix:dq_recheck:mirror_landing; DROP TABLE",
        "",
        None,
    ])
    def test_anything_else_is_refused(self, data):
        assert parse_callback(data) is None

    def test_the_subject_is_matched_not_trusted(self):
        """The subject is the only caller-supplied part of a request. It is
        compared against a declared set, never passed through."""
        for layer in DQ_LAYERS:
            assert parse_callback(f"afix:dq_recheck:{layer}") is not None
        assert parse_callback("afix:dq_recheck:../../etc") is None


class TestBuildingButtons:
    def test_it_offers_what_applies(self):
        out = buttons_for(["dq_recheck"], "mirror_landing")
        assert len(out) == 1
        label, data = out[0]
        assert label == ACTIONS["dq_recheck"].label
        assert data == "afix:dq_recheck:mirror_landing"

    def test_an_action_that_does_not_fit_the_subject_is_skipped(self):
        """Losing one button beats losing the alert carrying it."""
        assert buttons_for(["dq_recheck"], "some_other_layer") == []

    def test_an_unknown_action_is_skipped_not_raised(self):
        assert buttons_for(["no_such_action"], "mirror_landing") == []

    def test_callback_for_raises_where_buttons_for_skips(self):
        """The direct caller has already decided to offer it, so a silent drop
        there would be a button an operator looks for and cannot find."""
        with pytest.raises(ValueError):
            callback_for("dq_recheck", "some_other_layer")
        with pytest.raises(ValueError):
            callback_for("no_such_action", "mirror_landing")


class TestTheSubjectResolvesToAJobNotAString:
    def test_every_declared_layer_has_a_job(self):
        """A subject the button offers and the executor cannot resolve is a
        tap that answers 'queued' and then does nothing."""
        from core.alert_actions import ACTIONS, DQ_JOB_FOR_LAYER, job_for
        for subject in ACTIONS["dq_recheck"].subjects:
            assert job_for("dq_recheck", subject) == DQ_JOB_FOR_LAYER[subject]

    def test_the_two_tables_do_not_drift(self):
        from core.alert_actions import ACTIONS, DQ_JOB_FOR_LAYER
        assert set(ACTIONS["dq_recheck"].subjects) == set(DQ_JOB_FOR_LAYER)

    def test_an_unknown_pair_resolves_to_nothing(self):
        from core.alert_actions import job_for
        assert job_for("dq_recheck", "not_a_layer") is None
        assert job_for("something_else", "mirror_landing") is None

    def test_the_job_ids_exist_in_the_scheduler(self):
        """The registry is the whitelist — `trigger_job` raises on an id it
        does not hold, so a typo here would surface only on a tap."""
        import inspect
        from core.alert_actions import DQ_JOB_FOR_LAYER
        import core.scheduler as sch
        src = inspect.getsource(sch)
        for job_id in DQ_JOB_FOR_LAYER.values():
            assert f'job_id="{job_id}"' in src, job_id


class TestTheHandlerIsTheAuthorisationBoundary:
    """`parse_callback` decides what is *sayable*; the handler decides who."""

    def _query(self, data, user_id):
        from unittest.mock import AsyncMock, MagicMock
        query = MagicMock()
        query.data = data
        query.from_user = MagicMock(id=user_id)
        query.answer = AsyncMock()
        query.edit_message_reply_markup = AsyncMock()
        update = MagicMock()
        update.callback_query = query
        return update, query

    @pytest.mark.asyncio
    async def test_a_non_admin_is_refused_and_nothing_is_recorded(self, monkeypatch):
        """Alerts reach admins only, so this is somebody forwarding a message
        and tapping. It must be answered, not silently dropped."""
        import bot.handlers_legacy as h
        from unittest.mock import MagicMock
        monkeypatch.setattr("bot.config.is_admin", lambda uid: False)
        store = MagicMock()
        monkeypatch.setattr("core.bot_store.get_bot_store", lambda: store)

        update, query = self._query("afix:dq_recheck:mirror_landing", 111)
        await h.alert_action_callback(update, None)

        query.answer.assert_awaited_once()
        assert query.answer.await_args.kwargs.get("show_alert") is True
        store.alert_actions.request.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_unknown_action_records_nothing(self, monkeypatch):
        """An old message whose action was retired, or something that did not
        come from us. Neither should run."""
        import bot.handlers_legacy as h
        from unittest.mock import MagicMock
        monkeypatch.setattr("bot.config.is_admin", lambda uid: True)
        store = MagicMock()
        monkeypatch.setattr("core.bot_store.get_bot_store", lambda: store)

        update, query = self._query("afix:rm_rf:mirror_landing", 777)
        await h.alert_action_callback(update, None)

        query.answer.assert_awaited_once()
        store.alert_actions.request.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_admin_records_the_request_and_the_button_goes(self, monkeypatch):
        import bot.handlers_legacy as h
        from unittest.mock import MagicMock
        monkeypatch.setattr("bot.config.is_admin", lambda uid: True)
        store = MagicMock()
        store.alert_actions.available.return_value = True
        store.alert_actions.request.return_value = 42
        monkeypatch.setattr("core.bot_store.get_bot_store", lambda: store)

        update, query = self._query("afix:dq_recheck:mirror_landing", 777)
        await h.alert_action_callback(update, None)

        store.alert_actions.request.assert_called_once()
        kwargs = store.alert_actions.request.call_args.kwargs
        assert kwargs["action"] == "dq_recheck"
        assert kwargs["subject"] == "mirror_landing"
        assert kwargs["by_user_id"] == 777
        # Taken away once used: a second tap looks to the operator exactly
        # like the first and would queue work they did not intend.
        query.edit_message_reply_markup.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sqlite_says_so_instead_of_recording_into_the_void(self, monkeypatch):
        """The row has to be visible to the other container."""
        import bot.handlers_legacy as h
        from unittest.mock import MagicMock
        monkeypatch.setattr("bot.config.is_admin", lambda uid: True)
        store = MagicMock()
        store.alert_actions.available.return_value = False
        monkeypatch.setattr("core.bot_store.get_bot_store", lambda: store)

        update, query = self._query("afix:dq_recheck:mirror_landing", 777)
        await h.alert_action_callback(update, None)

        store.alert_actions.request.assert_not_called()
        assert query.answer.await_args.kwargs.get("show_alert") is True
