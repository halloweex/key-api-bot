"""The role/feature matrix, and the seeding that carries it into the database.

Two failures this file exists to catch:

* a role that does not answer for every feature — a missing entry reads as
  "denied", which is the safe direction but an invisible one;
* a new feature that never reaches `role_permissions`. Seeding used to bail
  out the moment the table held a single row, so every feature added after the
  first deploy was denied to every DB-backed role, admins included.
"""
import pytest

from core.permissions import (
    Action,
    Feature,
    ROLE_PERMISSIONS,
    Role,
    can,
    get_all_features,
    get_all_roles,
    get_permissions_for_role,
)


class TestMatrixCoverage:
    def test_every_role_answers_for_every_feature(self):
        for role, features in ROLE_PERMISSIONS.items():
            missing = {f for f in Feature} - set(features)
            assert not missing, f"{role.value} says nothing about {missing}"

    def test_every_role_is_in_the_matrix(self):
        assert set(ROLE_PERMISSIONS) == set(Role)

    def test_features_and_roles_are_listed_for_the_admin_page(self):
        assert {f["key"] for f in get_all_features()} == {f.value for f in Feature}
        assert {r["key"] for r in get_all_roles()} == {r.value for r in Role}


class TestSmsIsAnAreaAndAnAction:
    """`sms` used to be the whole point of the `marketer` role — and that role
    was the modelling error: an *area* wearing a *level's* clothes. It carried
    a viewer's depth plus `sms` edit, so "a marketer who may only read campaign
    results" could not be said at all.

    The two questions are separate now. Whether somebody reaches the SMS page
    is their **tab set**; whether they may send from it is their **level**.
    """

    @pytest.mark.parametrize("level", [Role.VIEWER, Role.EDITOR, Role.ADMIN])
    def test_the_matrix_is_depth_only(self, level):
        """Every level may *view* every feature it is asked about — the matrix
        no longer decides areas, so it cannot say "not for you" about one."""
        assert can(level, Feature.SMS, "view")

    def test_only_editor_and_above_may_send(self):
        assert not can(Role.VIEWER, Feature.SMS, "edit")
        assert can(Role.EDITOR, Feature.SMS, "edit")
        assert can(Role.ADMIN, Feature.SMS, "edit")

    def test_the_area_is_what_actually_opens_the_page(self):
        """A level alone opens nothing: without the tab the page is closed,
        whatever the depth. And with the tab, the depth decides sending —
        which is the pair the old role could not express."""
        from core.permissions import ACCESS_PRESETS, apply_feature_override

        for level in (Role.VIEWER, Role.EDITOR):
            narrowed = apply_feature_override(
                get_permissions_for_role(level), ["traffic"], level)
            assert narrowed[Feature.SMS.value]["view"] is False
            assert narrowed[Feature.SMS.value]["edit"] is False

        marketer_tabs = ACCESS_PRESETS["marketer"]
        reader = apply_feature_override(
            get_permissions_for_role(Role.VIEWER), marketer_tabs, Role.VIEWER)
        sender = apply_feature_override(
            get_permissions_for_role(Role.EDITOR), marketer_tabs, Role.EDITOR)

        assert reader[Feature.SMS.value] == {
            "view": True, "edit": False, "delete": False}
        assert sender[Feature.SMS.value]["edit"] is True

    def test_the_default_set_does_not_include_sms(self):
        """Nobody gains the roster by the matrix becoming uniform: an account
        with no tab set of its own holds its level's default, and SMS is not in
        it below admin."""
        from core.permissions import apply_feature_override

        for level in (Role.VIEWER, Role.EDITOR):
            inherited = apply_feature_override(
                get_permissions_for_role(level), None, level)
            assert inherited[Feature.SMS.value]["view"] is False

    def test_no_level_below_admin_manages_users(self):
        for level in (Role.VIEWER, Role.EDITOR):
            for action in Action:
                assert not can(level, Feature.USER_MANAGEMENT, action)
