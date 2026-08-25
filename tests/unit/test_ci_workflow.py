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

    def test_it_invokes_pytest_with_no_marker_of_its_own(self):
        """`pytest.ini` owns `testpaths`, `asyncio_mode` and the deselections.
        A `-m` or a path here would make the check and the laptop disagree
        about what passing means, and the laptop is where it gets debugged."""
        run = self._run_step()["run"].strip()
        assert run == "pytest -q"

    def test_it_does_not_reach_the_live_api_or_the_production_database(self):
        """`external` is deselected by `pytest.ini` and must stay that way:
        those tests read the live KeyCRM API and the production DuckDB file.
        Nothing in CI may opt into them."""
        run = self._run_step()["run"]
        assert "-m " not in run
        # No credentials reach this workflow, so the external suite could not
        # run even if something asked it to.
        assert all("env" not in step for step in STEPS)
        assert "secrets." not in CI_TEXT


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

    def test_it_installs_the_dev_requirements(self):
        """`requirements-dev.txt` pulls in `requirements.txt` through `-r`, and
        carries alembic and PyYAML — which two test modules read as data."""
        install = next(s for s in STEPS if s.get("name") == "Install dependencies")
        assert "requirements-dev.txt" in install["run"]
