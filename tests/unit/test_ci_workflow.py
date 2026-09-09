"""The pull-request check, as facts about a file.

Until this workflow existed `deploy.yml` was the only one in the repository and
it triggers on push to `main`. A pull request was verified by **nothing**: the
suite ran on the author's machine and nowhere else, so a forgotten `pytest`
reached production with production as the first thing to notice it.

What is worth pinning here is not that a YAML file exists — it is the four
properties that decide whether a green check means anything, each of which is
one careless edit away from being false.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
CI_PATH = REPO / ".github" / "workflows" / "ci.yml"
CI = yaml.safe_load(CI_PATH.read_text())
CI_TEXT = CI_PATH.read_text()

# PyYAML resolves the bare key `on:` to the boolean True — it is a YAML 1.1
# truthy word. Every workflow file in existence hits this.
TRIGGERS = CI[True] if True in CI else CI["on"]

STEPS = CI["jobs"]["tests"]["steps"]


class TestItRunsWhenItMatters:
    def test_it_runs_on_every_pull_request(self):
        """The whole point. `deploy.yml` fires after the merge, which is too
        late to be a check on the thing being merged."""
        assert "pull_request" in TRIGGERS

    def test_it_runs_on_main_too(self):
        """A direct push to `main` deploys. It should also be verified."""
        assert TRIGGERS["push"]["branches"] == ["main"]

    def test_it_has_no_paths_filter(self):
        """`deploy.yml` ignores `tests/**`, and rightly — a test change cannot
        alter what runs in a container. Copying that filter here would skip the
        suite on the pull requests that are *nothing but* suite.

        Asserted on the parsed structure, not the text: the workflow's own
        comment explains why the filter is absent and therefore contains the
        word. Three tests in this repository have now been written the wrong
        way round for exactly that reason."""
        for trigger in TRIGGERS.values():
            for key in ("paths", "paths-ignore"):
                assert key not in (trigger or {})

    def test_a_run_on_main_is_never_cancelled(self):
        """Superseding a run on a branch loses nothing. Superseding one on
        `main` leaves the branch that deploys with no verdict at all."""
        assert CI["concurrency"]["cancel-in-progress"] == (
            "${{ github.ref != 'refs/heads/main' }}"
        )


class TestGreenMeansWhatItSaysOnALaptop:
    def _run_step(self):
        return next(s for s in STEPS if s.get("name") == "Run the suite")

    def test_it_invokes_pytest_with_no_marker_or_path_of_its_own(self):
        """`pytest.ini` owns `testpaths`, `asyncio_mode` and the deselections.
        A `-m` or a path here would make the check and the laptop disagree
        about what passing means, and the laptop is where it gets debugged.

        Asserted on the selection flags rather than on the whole string: the
        command also reports its skips and tees the output, and neither
        changes which tests run.
        """
        run = self._run_step()["run"]
        assert "pytest -q" in run
        assert "-m " not in run
        assert not re.search(r"pytest[^|]*\btests/", run), "a path would narrow the run"
        assert "-k " not in run
        assert "--deselect" not in run

    def test_it_does_not_reach_the_live_api_or_the_production_database(self):
        """`external` is deselected by `pytest.ini` and must stay that way:
        those tests read the live KeyCRM API and the production DuckDB file.
        Nothing in CI may opt into them.

        The blanket "no step sets `env`" this used to assert stopped being
        available when the throwaway PostgreSQL arrived. The guarantee it
        stood for is narrower and is asserted directly: no secret reaches
        this workflow, and the only variable any step sets points at a
        database on the runner itself.
        """
        assert "-m " not in self._run_step()["run"]
        assert "secrets." not in CI_TEXT

        for step in STEPS:
            for name, value in (step.get("env") or {}).items():
                assert name == "KS_PG_DSN", f"{name} is not the throwaway DSN"
                assert "127.0.0.1" in value, f"{name} must not leave the runner"


class TestTheStoreTestsActuallyRun:
    """The differential tests skip themselves without `KS_PG_DSN`, and it was
    unset here — 60 checks that never ran on any pull request, including all
    20 proving the /traffic port answers the same in both engines. A check
    that does not run is not a check, and `deploy/gate_with_stores.sh` is a
    thing a person remembers rather than a thing that happens.
    """

    def _step(self, name):
        return next(s for s in STEPS if s.get("name") == name)

    def test_a_postgres_is_started_from_the_repository_s_own_initdb(self):
        """Those scripts create the roles and schemas the migrations need, so
        a bare image would fail at the first `CREATE TABLE`. They are also why
        this is a `docker run` after checkout and not a `services:` block:
        service containers start before the checkout that would supply them."""
        run = self._step("Start a throwaway PostgreSQL")["run"]
        assert "postgres/initdb:/docker-entrypoint-initdb.d" in run

    def test_the_postgres_version_is_the_one_everything_else_uses(self):
        """A check on a different major from production and from the gate is
        a check about a database nobody runs."""
        run = self._step("Start a throwaway PostgreSQL")["run"]
        image = re.search(r"postgres:\d+\.\d+-alpine", run)
        assert image, "the postgres image is no longer pinned"

        gate = (REPO / "deploy" / "gate_with_stores.sh").read_text()
        compose = (REPO / "docker-compose.yml").read_text()
        assert image.group(0) in gate
        assert image.group(0) in compose

    def test_the_gate_takes_its_volumes_with_its_containers(self):
        """`docker rm` without `-v` orphans the anonymous volume each store
        container mints, so the gate leaked two per run. Measured on the VPS
        2026-09-09: 173 dangling volumes, 7.13 GB — enough to take the disk
        watchdog from WARN to CRITICAL, which is how it was found.

        Asserted on the flag rather than on the comment beside it: a grep for
        the reason passes while the behaviour regresses."""
        gate = (REPO / "deploy" / "gate_with_stores.sh").read_text()
        removals = re.findall(r"docker rm[^\n]*", gate)
        assert removals, "the gate no longer removes its containers"
        for line in removals:
            flags = line.split('"')[0]
            assert "-v" in flags, (
                f"{line.strip()!r} removes containers without -v; every run "
                "then leaves the stores' anonymous volumes on the host"
            )

    def test_the_migrations_are_applied_before_the_suite(self):
        """`require_revision` refuses a schema that is behind or ahead, so an
        unmigrated database would fail every store test rather than skip
        them — noisily, but for the wrong reason."""
        names = [s.get("name") for s in STEPS]
        assert names.index("Apply the migrations") < names.index("Run the suite")
        assert "alembic upgrade head" in self._step("Apply the migrations")["run"]

    def test_the_suite_is_given_the_dsn(self):
        assert "KS_PG_DSN" in (self._step("Run the suite").get("env") or {})

    def test_a_postgres_skip_fails_the_job(self):
        """Otherwise this whole arrangement can stop working — a renamed
        container, a migration that did not apply — and the only sign would
        be a skip count in a log, which is exactly the state it replaced."""
        run = self._step("No store test may have skipped")["run"]
        assert "needs a live PostgreSQL" in run
        assert "exit 1" in run

    def test_the_reason_string_is_the_one_the_tests_actually_use(self):
        """The check above greps for a sentence written in another file. If
        that sentence is reworded, the grep silently matches nothing and the
        job goes green on zero store tests."""
        marker = "needs a live PostgreSQL"
        users = [
            p for p in (REPO / "tests").rglob("test_*.py")
            if marker in p.read_text()
        ]
        assert users, f"no test skips with {marker!r} — the CI grep is dead"


class TestItRunsTheRuntimesPython:
    def test_the_version_matches_the_production_image(self):
        """A suite passing on a Python production does not run is a green check
        that means nothing."""
        setup = next(
            s for s in STEPS if str(s.get("uses", "")).startswith("actions/setup-python")
        )
        declared = str(setup["with"]["python-version"])

        dockerfile = (REPO / "Dockerfile.web").read_text()
        match = re.search(r"FROM python:(\d+\.\d+)-slim", dockerfile)
        assert match, "Dockerfile.web no longer pins a python:X.Y-slim base"
        assert declared == match.group(1)

    def test_it_installs_the_locked_set(self):
        """Not `requirements-dev.txt`, which would re-resolve: a check running
        against a different version set from the one being deployed is a check
        about nothing. See tests/unit/test_dependency_locks.py."""
        install = next(s for s in STEPS if s.get("name") == "Install dependencies")
        assert "requirements-dev.lock" in install["run"]
        assert "requirements-dev.txt" not in install["run"]
