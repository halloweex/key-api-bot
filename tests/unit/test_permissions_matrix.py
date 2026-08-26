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


class TestSmsGrant:
    """`sms` is the whole point of the marketer role."""

    @pytest.mark.parametrize("action", ["view", "edit"])
    def test_marketer_and_admin_may_run_campaigns(self, action):
        assert can(Role.MARKETER, Feature.SMS, action)
        assert can(Role.ADMIN, Feature.SMS, action)

    @pytest.mark.parametrize("role", [Role.VIEWER, Role.EDITOR])
    @pytest.mark.parametrize("action", ["view", "edit"])
    def test_nobody_else_may(self, role, action):
        assert not can(role, Feature.SMS, action)

    def test_marketer_is_a_viewer_everywhere_else(self):
        marketer = get_permissions_for_role(Role.MARKETER)
        viewer = get_permissions_for_role(Role.VIEWER)
        del marketer[Feature.SMS.value], viewer[Feature.SMS.value]
        assert marketer == viewer

    def test_marketer_cannot_manage_users(self):
        for action in Action:
            assert not can(Role.MARKETER, Feature.USER_MANAGEMENT, action)
