"""A signed-in visitor with a tab set, and the store that answers their row."""
import time

import pytest

from core.permissions import invalidate_permissions_cache
from web.routes.auth import create_session_data, SESSION_COOKIE, session_serializer


PLAIN_ID = 555_000_444


def _cookie(user_id: int, role: str) -> dict:
    data = create_session_data(
        {
            "id": str(user_id),
            "first_name": "Test",
            "last_name": "User",
            "username": "tester",
            "auth_date": str(int(time.time())),
        },
        role=role,
    )
    return {"Cookie": f"{SESSION_COOKIE}={session_serializer.dumps(data)}"}


class _FakeStore:
    """Enough of the store for `_resolve_session` and one traffic read."""

    def __init__(self, role: str, allowed_features):
        self._role = role
        self._features = allowed_features

    async def get_user(self, uid):
        return {
            "status": "approved",
            "role": self._role,
            "allowed_features": self._features,
        }

    async def get_traffic_analytics(self, **kwargs):
        return {"summary": {}}

    async def set_user_features(self, user_id, features, changed_by):
        self.written = (user_id, features, changed_by)
        return user_id != 404


@pytest.fixture(autouse=True)
def _fresh_permissions():
    # The role matrix is cached process-wide; a stale copy would make these
    # pass or fail depending on test order.
    invalidate_permissions_cache()
    yield
    invalidate_permissions_cache()


# Each route module does `from ._deps import get_store` at import time, so it
# holds its own reference: patching `_deps` alone leaves every handler talking
# to the real store. Named here rather than discovered per test, because the
# failure mode is a green test that opened a database.
_STORE_HOLDERS = (
    "core.store.get_store",
    "web.routes.api._deps.get_store",
    "web.routes.api.traffic.get_store",
    "web.routes.api.users.get_store",
    "web.routes.api.analytics.get_store",
)


def _install_store(monkeypatch, store) -> None:
    async def _fake_get_store():
        return store

    for target in _STORE_HOLDERS:
        monkeypatch.setattr(target, _fake_get_store)
