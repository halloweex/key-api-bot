"""The suite is entitled to no secrets, and must hold none.

This exists because of one CI run — the first ever executed on this repository,
on the pull request that added CI. It failed at collection: `web/routes/auth.py`
raises at import when no session signing key is present, and five integration
modules import it. The suite had passed for as long as anyone had checked, on
machines where a gitignored `.env` quietly supplied one.

Sixteen more tests then failed on `KEYCRM_API_KEY`, which they need only because
`KeyCRMClient.__init__` refuses to be built without it — the transport is mocked
in every one of them.

`tests/conftest.py` now supplies all three at import, before any test module is
loaded. What this file pins is the *consequence* rather than the mechanism: a
test process holds credentials that cannot reach anything real. Remove the
conftest block and this fails immediately on a developer's machine, where `.env`
would supply the genuine ones — which is a better place to find out than a
production incident.
"""
from __future__ import annotations

import os

FAKE = "not-a-real-secret"


class TestNoRealCredentialIsInReach:
    def test_sessions_are_signed_with_a_test_key(self):
        """A test must not be able to mint a session valid against production."""
        from core.config import config

        assert FAKE in config.web.secret_key

    def test_the_bot_token_is_a_test_token(self):
        """`send_admin_message` falls back to the HTTP Bot API whenever no
        Application is running, which is every test process. The autouse guard
        in conftest blocks the request; this removes the credential as well."""
        from core.config import config

        assert FAKE in config.bot.token

    def test_the_keycrm_key_is_a_test_key(self):
        """The `external` marker keeps the live API out of a default run. This
        makes a stray non-external test that reaches for the network fail on
        authentication instead of succeeding against production."""
        assert FAKE in os.environ["KEYCRM_API_KEY"]


class TestTheEnvironmentIsSetBeforeAnythingReadsIt:
    def test_conftest_assigns_rather_than_defaults(self):
        """`load_dotenv()` does not override variables already present, so an
        assignment in conftest wins over `.env`. `setdefault` would lose to it,
        and the suite would go back to signing with the production key on every
        developer machine."""
        from pathlib import Path

        conftest = (Path(__file__).resolve().parents[1] / "conftest.py").read_text()
        assert "os.environ.setdefault" not in conftest
        for name in ("DASHBOARD_SECRET_KEY", "BOT_TOKEN", "KEYCRM_API_KEY"):
            assert f'os.environ["{name}"]' in conftest

    def test_the_block_precedes_the_pytest_import(self):
        """conftest is imported before any test module, but the assignment has
        to precede anything that imports `core.config` — its dataclasses read
        the environment once, at construction."""
        from pathlib import Path

        conftest = (Path(__file__).resolve().parents[1] / "conftest.py").read_text()
        assert conftest.index('os.environ["DASHBOARD_SECRET_KEY"]') < conftest.index(
            "import pytest"
        )
