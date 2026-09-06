"""The admin's Approve/Deny buttons answer the request they were shown for."""
import inspect

from bot import handlers_legacy as h


def test_the_two_buttons_pass_the_expected_status():
    assert 'expected_status="pending"' in inspect.getsource(h.auth_approve_user)
    assert 'expected_status="pending"' in inspect.getsource(h.auth_deny_user)


def test_the_refused_verdict_names_its_cause():
    for fn in (h.auth_approve_user, h.auth_deny_user):
        assert "admin.already_decided" in inspect.getsource(fn)


def test_the_person_is_told_in_their_own_language():
    src = inspect.getsource(h.auth_deny_user)
    assert "t(ACCESS_FROZEN_MESSAGE, target_lang)" in src
    assert "t(ACCESS_DENIED_MESSAGE, target_lang)" in src
    assert "ACCESS_DENIED_MESSAGE + " not in src, "the raw i18n key used to be sent"


def test_the_list_refresh_no_longer_reaches_for_a_missing_update():
    src = inspect.getsource(h.show_updated_user_list)
    assert "_lang(update)" not in src
    assert "lang" in inspect.signature(h.show_updated_user_list).parameters
    for caller in (h.admin_revoke_user, h.admin_unfreeze_user):
        assert "show_updated_user_list(query, admin.id, _lang(update))" in inspect.getsource(caller)
