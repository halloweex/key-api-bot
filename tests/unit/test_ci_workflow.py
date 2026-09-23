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

# Everything in this repository that can start or stop a container. The
# runbooks under deploy/agent/ are prose for the diagnostician, not scripts.
SHELL_AND_WORKFLOW_FILES = [
    p
    for p in (
        list((REPO / "deploy").rglob("*.sh"))
        + list((REPO / "scripts").glob("*.sh"))
        + list((REPO / ".github" / "workflows").glob("*.yml"))
    )
    if "runbooks" not in p.parts
]


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
        this workflow, and the only variables any step sets are the two
        logins to a database on the runner itself.
        """
        assert "-m " not in self._run_step()["run"]
        assert "secrets." not in CI_TEXT

        for step in STEPS:
            for name, value in (step.get("env") or {}).items():
                assert name in ("KS_PG_DSN", "KS_PG_READONLY_DSN"), \
                    f"{name} is not a throwaway DSN"
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

    def test_every_container_removal_takes_its_volumes_with_it(self):
        """`docker rm` without `-v` orphans the anonymous volume an image with
        a `VOLUME` line mints on every `docker run`. Measured on the VPS
        2026-09-09: 173 dangling volumes, 7.13 GB — enough to take the disk
        watchdog from WARN to CRITICAL, which is how it was found.

        **This walks the repository rather than naming the gate**, because the
        first version of this test named `gate_with_stores.sh` alone and
        `quick_gate.sh` — same shape, same `postgres:17.2-alpine`, run far more
        often — kept leaking one 49 MB volume per invocation behind a green
        suite. Measured 2026-09-14: 65 dangling volumes, 3.52 GB, and the
        diagnostic agent found it before this test did.

        `-v` is safe to require everywhere and that is what makes the rule flat
        rather than a list of exceptions: it removes **only** anonymous
        volumes. A named volume and a bind mount are untouched, so a script
        that mounts its own directory over the image's `VOLUME` — the PITR
        drill, the weekly compact — loses nothing by carrying the flag, and
        gains the bound the day somebody drops the mount.

        Asserted on the flag rather than on the comment beside it: a grep for
        the reason passes while the behaviour regresses."""
        removals = []
        for path in sorted(SHELL_AND_WORKFLOW_FILES):
            for line in path.read_text().splitlines():
                if "docker rm" not in line or line.lstrip().startswith("#"):
                    continue
                removals.append((path, line))

        assert len(removals) >= 6, (
            "the removals this walk is meant to cover have moved or gone; "
            f"found {len(removals)}"
        )
        for path, line in removals:
            # Searched, not anchored: in a workflow the command is the value
            # of a `run:` key, so the line does not start with it.
            flags = re.search(r"docker rm((?:\s+-[A-Za-z]+)*)", line)
            assert flags and "v" in flags.group(1), (
                f"{path.relative_to(REPO)}: {line.strip()!r} removes a "
                "container without -v; every run then leaves its anonymous "
                "volumes on the host"
            )

    def test_every_script_that_builds_an_image_bounds_the_cache(self):
        """Whoever fills the cache caps it — and the set is derived, not named.

        This assertion used to read one file, `gate_with_stores.sh`. Its
        sibling `quick_gate.sh` builds the same image from the same Dockerfile,
        is the one run by hand while iterating on a test, and capped nothing;
        between 2026-09-14 and 09-17 the cache went 1.6 GB / 90 entries to
        2.5 GB / 752 while this test was green. That is the third time a guard
        that named its subject guarded only the subject somebody was thinking
        of — after the mirror specs and after `docker rm -v` in this very file.

        So the subjects come from behaviour: any tracked script that shells out
        to `docker build` owns a cache and must bound it. Adding a third gate
        without the cap now fails here.
        """
        builders = []
        for path in SHELL_AND_WORKFLOW_FILES:
            body = path.read_text()
            if re.search(r"(?:^|[^-\w])docker build\b", body):
                builders.append((path, body))

        assert builders, (
            "no script runs `docker build` any more — has the gate moved?"
        )

        for path, body in builders:
            rel = path.relative_to(REPO)
            assert "builder prune -f --max-used-space" in body, (
                f"{rel} runs `docker build` and never caps the cache it "
                f"fills. Every invocation then adds layers the host keeps "
                f"for good."
            )
            # Not the emptying form, which is what makes the next alert: the
            # disk watchdog differences at a fixed 168h lag, so a cache
            # emptied today reads as +2 GB of growth a week later when it
            # returns to its natural size. The 2026-09-13 WARN was made that
            # way by a prune on 09-09.
            assert not re.search(r"builder prune -f\s*(\||;|$)", body, re.M), (
                f"{rel}: an unbounded `builder prune -f` empties the cache "
                f"instead of capping it"
            )

    def test_the_two_gates_agree_on_how_big_the_cache_may_get(self):
        """One cache, one bound. Two gates that disagreed would each undo the
        other's judgement on whichever ran last."""
        caps = {}
        for path in SHELL_AND_WORKFLOW_FILES:
            body = path.read_text()
            for cap in re.findall(r"--max-used-space\s+\"?\$\{(\w+):-([^}]+)\}",
                                  body):
                caps[path.name] = cap

        assert len(caps) >= 2, f"expected both gates to carry a cap, got {caps}"
        assert len(set(caps.values())) == 1, (
            f"the gates disagree about the cache bound: {caps}"
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

    def test_the_dry_run_s_roles_are_provisioned(self):
        """The reclassify dry run's tests need two roles `ks_app` cannot
        make: a `ks_readonly` login, handed to the suite as its DSN, and a
        NOINHERIT member of `ks_app`. Named here from the test module's own
        constant, so renaming one side without the other fails here and not
        as a skip a person has to notice."""
        start = self._step("Start a throwaway PostgreSQL")["run"]
        suite_env = self._step("Run the suite").get("env") or {}
        assert "ALTER ROLE ks_readonly WITH PASSWORD 'ci-only'" in start
        assert suite_env.get("KS_PG_READONLY_DSN", "").startswith(
            "postgresql://ks_readonly:ci-only@127.0.0.1:")

        module = REPO / "tests" / "integration" / "test_utm_reclassify_dryrun_pg.py"
        role = re.search(r'^NOINHERIT_ROLE = "(\w+)"$', module.read_text(), re.M)
        assert role, "the dry run's test no longer names its NOINHERIT role"
        assert f"CREATE ROLE {role.group(1)} NOLOGIN NOINHERIT" in start
        assert f"GRANT ks_app TO {role.group(1)}" in start

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


class TestTheGatesTestTheCodeInFrontOfThem:
    """The gate ran the branch's tests against the *published* image.

    `gate_with_stores.sh` defaulted to `halloweex/keycrm-web:latest` and mounted
    the repository's `tests/` over it, so a branch's tests met the previous
    release's `core/`. On 2026-09-12 that surfaced as a collection error,
    because the change under test added a new module the published image did
    not carry — luck, not design: a change that only edits an existing module
    would have gone green while testing code that was never in the branch.

    That is the same class of defect as the one the `KS_PG_DSN` block above
    exists to prevent — a check that quietly checks the wrong thing — and it
    sat in the script that decides whether something ships.

    Asserted on the image name and the build command rather than on the
    comments beside them: a grep for the reason passes while the behaviour
    regresses.
    """

    GATES = ("gate_with_stores.sh", "quick_gate.sh")

    def _script(self, name):
        return (REPO / "deploy" / name).read_text()

    def _default_image(self, text):
        m = re.search(r'IMAGE="\$\{IMAGE:-([^}]+)\}"', text)
        assert m, "the script no longer has a defaulted IMAGE"
        return m.group(1)

    def test_neither_gate_defaults_to_an_image_from_a_registry(self):
        """A name with a namespace in it is pulled, not built — which is
        exactly how the gate came to test a release instead of a branch."""
        for name in self.GATES:
            image = self._default_image(self._script(name))
            assert "/" not in image, (
                f"{name} defaults to {image!r}, which names a registry image; "
                f"the gate would then test what is published, not what is here"
            )

    def test_each_gate_builds_the_image_it_then_runs(self):
        for name in self.GATES:
            text = self._script(name)
            build = re.search(r'docker build[^\n]*-t "\$IMAGE"[^\n]*', text)
            assert build, f"{name} never builds $IMAGE"
            assert "Dockerfile.web" in build.group(0), (
                f"{name} builds $IMAGE from something other than Dockerfile.web"
            )

    def test_the_build_happens_before_anything_runs_that_image(self):
        """Ordering is the whole property: a build after the suite proves
        nothing about the suite that just ran."""
        for name in self.GATES:
            text = self._script(name)
            build_at = text.index('docker build')
            runs = [m.start() for m in re.finditer(r'--entrypoint sh "\$IMAGE"', text)]
            assert runs, f"{name} never runs $IMAGE"
            assert build_at < min(runs), (
                f"{name} runs the image before building it"
            )

    def test_skipping_the_build_checks_the_image_is_actually_there(self):
        """`SKIP_BUILD=1` against an image that does not exist should say so,
        not fail later inside a container command."""
        for name in self.GATES:
            text = self._script(name)
            assert "SKIP_BUILD" in text, f"{name} has no way to skip the build"
            assert 'docker image inspect "$IMAGE"' in text, (
                f"{name} skips the build without checking the image exists"
            )
