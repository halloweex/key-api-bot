"""The migration runner's wiring, as facts about two files.

The container itself was exercised against a real server and is not
reproducible here: `postgres:17.2-alpine` with the real
`postgres/initdb/*.sql`, brought up through this very `docker-compose.yml`.
Ordering held on a cold volume and on a non-empty one whose schemas had been
dropped; the revision applied, re-ran as a no-op, and a missing `KS_PG_DSN`,
a wrong password and an unreachable host each exited 1 with the reason on
stderr.

What is worth pinning in the suite is the wiring those runs depended on,
because every line of it is load-bearing and none of it is obvious from
reading the service in isolation:

* Alembic must stay out of the application images, or rule 11's "declare the
  version, fail closed" becomes a convention instead of a constraint.
* `migrate` must wait for `postgres-bootstrap`, not merely for a healthy
  server — the race is invisible on a fresh volume and real on the one case
  that service exists for.
* `bot` and `web` must wait for it to *finish*. That line used to say the
  opposite — "nothing may depend on it yet" — and named the condition for
  changing: the step that first reads Postgres. Both read it now, and both
  already failed without it, the bot loudly and `web` silently.
"""
from __future__ import annotations

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((REPO / "docker-compose.yml").read_text())
WORKFLOW = yaml.safe_load((REPO / ".github" / "workflows" / "deploy.yml").read_text())
SERVICES = COMPOSE["services"]


class TestAlembicStaysOutOfTheApplicationImages:
    def test_the_runtime_requirements_do_not_carry_it(self):
        """If the tool sat in the app image, `alembic.command.upgrade()` would
        be one import away from startup — and this system's history is a list
        of self-healing that turned a one-off fault into a loop."""
        runtime = (REPO / "requirements.txt").read_text().lower()
        assert "alembic" not in runtime

    def test_the_dev_requirements_do(self):
        dev = (REPO / "requirements-dev.txt").read_text().lower()
        assert "alembic" in dev

    def test_the_migrate_image_is_its_own(self):
        assert SERVICES["migrate"]["image"].endswith("keycrm-migrate:latest")
        assert SERVICES["migrate"]["image"] != SERVICES["web"]["image"]

    def test_its_dockerfile_does_not_install_the_application(self):
        """A slim base and three wheels. Copying requirements.txt in here
        would quietly recreate the web image with a different command."""
        df = (REPO / "Dockerfile.migrate").read_text()
        assert "requirements.txt" not in df
        assert "alembic" in df and "asyncpg" in df

    def test_it_copies_pg_without_the_core_package_init(self):
        """`core/pg.py` alone makes `core` a namespace package, so importing
        it does not drag in config, validators, pagination and httpx. Copying
        `core/` wholesale would."""
        df = (REPO / "Dockerfile.migrate").read_text()
        assert "COPY core/pg.py" in df
        assert "COPY core/ " not in df and "COPY core ." not in df


class TestTheOrderingIsDeclared:
    def test_it_waits_for_a_healthy_server(self):
        assert SERVICES["migrate"]["depends_on"]["postgres"]["condition"] == (
            "service_healthy"
        )

    def test_it_waits_for_the_schemas_to_exist(self):
        """The race that a fresh volume hides: `initdb` runs inside the
        entrypoint before Postgres reports healthy, so on a new database the
        schemas are always there first. On a non-empty data directory
        `initdb` is skipped — which is the entire reason
        `postgres-bootstrap` exists — and both services would start the
        moment the server went healthy."""
        assert SERVICES["migrate"]["depends_on"]["postgres-bootstrap"][
            "condition"
        ] == "service_completed_successfully"

    def test_it_is_a_one_off_and_says_so(self):
        """`Exited (0)` is this service's healthy steady state, so a restart
        policy would turn a finished migration into a crash loop."""
        assert SERVICES["migrate"]["restart"] == "no"

    def test_the_readers_wait_for_it(self):
        """This assertion used to be its own opposite, and said why: "nothing
        depends on it **yet** — that changes in the step that first reads
        Postgres". That step arrived and kept arriving. `web` now answers
        /summary, the user list, expenses and traffic out of Postgres, and the
        bot keeps its whole state there.

        What the old wording protected against was a failed migration taking
        down a dashboard that did not need the database. Both services need it
        now, and both already fail without it — the bot refuses to start on a
        revision mismatch, and `web` starts and then cannot authenticate
        anybody, which is the quieter and worse of the two. Observed on the
        0023 deploy: one bot crash and restart per migration, in the log as a
        RuntimeError naming both revisions.

        So they wait, and a failed migration stops them in one place instead of
        a restart loop and a login page that refuses everybody."""
        for name in ("bot", "web"):
            depends = SERVICES[name].get("depends_on") or {}
            assert "migrate" in depends, (
                f"{name} does not wait for migrate — every migration costs it "
                f"a crash, or a window where it serves 401 to everyone"
            )
            assert depends["migrate"]["condition"] == "service_completed_successfully", (
                f"{name} waits for the wrong thing: what it needs is the "
                f"migration finished, not the container started"
            )

    def test_it_stays_a_one_shot_so_the_condition_can_mean_that(self):
        """`service_completed_successfully` is only meaningful because the
        container exits. A restart policy here would leave the dependents
        waiting for something that never completes."""
        assert SERVICES["migrate"].get("restart") in ("no", False), \
            SERVICES["migrate"].get("restart")


class TestItRunsAsTheRoleThatOwnsTheTables:
    def test_the_dsn_names_ks_app(self):
        """Migrating as `postgres` leaves tables the application cannot
        alter."""
        dsn = SERVICES["migrate"]["environment"]["KS_PG_DSN"]
        assert dsn.startswith("postgresql://ks_app:")
        assert "@postgres:5432/ks" in dsn

    def test_the_password_comes_from_the_same_place_as_everyone_elses(self):
        dsn = SERVICES["migrate"]["environment"]["KS_PG_DSN"]
        assert "${KS_APP_PASSWORD" in dsn, "one home for the credential"

    def test_it_is_not_published_to_the_host(self):
        """Charter rule 12, which the migration runner has no reason to be an
        exception to."""
        assert "ports" not in SERVICES["migrate"]


class TestTheDeployReadsTheExitCode:
    """The failure this file was one PR late in noticing.

    `docker compose up -d` starts a one-off container and returns without
    waiting for it or looking at what it exited with — verified on a
    throwaway compose project: a service exiting 1 still leaves `up -d` at 0.
    So a revision that failed would deploy green, which is precisely the
    301-redirect gate's disease, in the same pipeline, three PRs after that
    one was fixed.
    """

    @staticmethod
    def _script() -> str:
        step = next(
            s for s in WORKFLOW["jobs"]["deploy"]["steps"]
            if "script" in (s.get("with") or {})
        )
        return step["with"]["script"]

    def test_it_waits_for_the_migration_container(self):
        assert "docker wait ks-migrate" in self._script()

    def test_a_non_zero_exit_fails_the_run(self):
        script = self._script()
        assert 'MIGRATE_RC" != "0"' in script
        assert "exit 1" in script

    def test_a_missing_container_is_a_failure_too(self):
        """The service is declared, so `up -d` was supposed to produce it.
        Absent means something is wrong, not that there was nothing to do."""
        assert "echo missing" in self._script()

    def test_the_logs_are_printed_before_giving_up(self):
        """An exit code alone sends whoever is on call to the host to find
        out what it said."""
        assert "docker logs" in self._script()

    def test_the_gate_runs_before_the_health_check(self):
        """No point asking whether the app serves when its schema did not
        apply — and the health endpoint would answer 200 either way."""
        script = self._script()
        assert script.index("docker wait ks-migrate") < script.index(
            "ksanalytics.duckdns.org/api/health"
        )


class TestTheImageIsActuallyBuilt:
    def test_the_workflow_builds_and_pushes_it(self):
        """A compose file naming an image nothing publishes is a deploy that
        fails on `docker compose pull`, at the worst moment."""
        steps = WORKFLOW["jobs"]["build"]["steps"]
        files = [
            s.get("with", {}).get("file")
            for s in steps
            if s.get("with", {}).get("file")
        ]
        assert "./Dockerfile.migrate" in files

    def test_it_is_tagged_like_the_others(self):
        steps = WORKFLOW["jobs"]["build"]["steps"]
        step = next(
            s for s in steps
            if s.get("with", {}).get("file") == "./Dockerfile.migrate"
        )
        tags = step["with"]["tags"]
        assert "keycrm-migrate:latest" in tags
        assert "keycrm-migrate:${{ env.VERSION }}" in tags
        assert step["with"]["push"] is True
