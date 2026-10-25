"""The switch that sends a route to the Rust pilot, as facts about five files.

What these hold is the promise that makes merging the switch safe: until
nginx/rust/routes.conf names a location, nothing anybody is answered changes.
The service starts in a profile the Python deploy never names, nginx reaches it
by a name resolved per request, so nginx starts with or without it, and the
include that would route to it is empty. When a route is listed, the pilot
reads as `ks_readonly`, answers from an image built without the test clock,
and is handed back to web on exactly two codes.

The behaviour itself was rehearsed with real containers — nginx 1.31, the
pilot built from Dockerfile.rust, uvicorn serving `web.main.app`, one Postgres
— and is described in the pull request; these pin the wiring it relied on.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import List

import yaml

REPO = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((REPO / "docker-compose.yml").read_text())
SERVICE = COMPOSE["services"]["rust-api"]
NGINX_SERVICE = COMPOSE["services"]["nginx"]
WORKFLOW = yaml.safe_load((REPO / ".github" / "workflows" / "deploy.yml").read_text())
JOBS = WORKFLOW["jobs"]
NGINX_CONF = (REPO / "nginx" / "nginx.conf").read_text()
RUST_DIR = REPO / "nginx" / "rust"
ROUTES = (RUST_DIR / "routes.conf").read_text()
SERVER = (RUST_DIR / "server.conf").read_text()
PROXY = (RUST_DIR / "proxy.conf").read_text()
DOCKERFILE = (REPO / "Dockerfile.rust").read_text()
RUST_SRC = REPO / "rust" / "crates" / "ks-api" / "src"


def _directives(text: str) -> str:
    """The configuration with its comments and blank lines taken out."""
    lines = (line.split("#", 1)[0].strip() for line in text.splitlines())
    return "\n".join(line for line in lines if line)


def _block(text: str, opener: str) -> str:
    """The body of the first `opener { ... }`, braces matched."""
    start = text.index(opener)
    depth, i = 0, text.index("{", start)
    for j in range(i, len(text)):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                return text[i + 1:j]
    raise AssertionError(f"{opener!r} is never closed")


def _headers(block: str) -> List[str]:
    return sorted(re.findall(r"^\s*(proxy_set_header .+;)", block, re.MULTILINE))


def _rust_deploy_script() -> str:
    return JOBS["deploy-rust"]["steps"][0]["with"]["script"]


def _python_deploy_script() -> str:
    step = next(s for s in JOBS["deploy"]["steps"] if "script" in (s.get("with") or {}))
    return step["with"]["script"]


class TestNothingIsSwitchedByDefault:
    def test_routes_conf_names_no_route(self):
        """The one promise the rest depends on: as merged, this file routes
        nothing, so every request still goes to web through nginx.conf."""
        assert _directives(ROUTES) == "", _directives(ROUTES)

    def test_the_line_it_documents_is_the_one_proxy_conf_serves(self):
        """The commented example is what an operator copies when switching."""
        assert "location = /api/revenue/trend { include /etc/nginx/rust/proxy.conf; }" in ROUTES

    def test_the_always_included_part_routes_nothing(self):
        """server.conf is included whatever routes.conf says, so a location
        with a path in it would be a route switched by default. It declares
        the named fallback and nothing else."""
        assert re.findall(r"^\s*location\s+(\S+)", _directives(SERVER), re.MULTILINE) == [
            "@rust_fallback_to_web"
        ]


class TestNginxStartsWithOrWithoutThePilot:
    def test_the_include_sits_in_the_https_server(self):
        bodies = [_block(NGINX_CONF[m.start():], "server")
                  for m in re.finditer(r"\bserver\s*\{", NGINX_CONF)]
        tls = [b for b in bodies if "listen 443 ssl;" in b]
        assert len(tls) == 1
        assert "include /etc/nginx/rust/server.conf;" in _directives(tls[0])

    def test_the_directory_is_mounted_not_its_files(self):
        """A single-file bind mount is bound to the inode it saw at creation,
        and `git reset --hard` replaces the file: nginx would reload the old
        routes and report success (deploy/postgres-move-runbook.md)."""
        volumes = NGINX_SERVICE["volumes"]
        assert "./nginx/rust:/etc/nginx/rust:ro" in volumes
        assert not [v for v in volumes if v.startswith("./nginx/rust/")], volumes

    def test_the_pilot_is_reached_by_a_variable_never_an_upstream_block(self):
        """An `upstream` block resolves its names once, at load, and fails the
        load when one does not resolve — and the pilot's container is absent
        whenever its profile is not started. The 2026-08-23 outage is why
        nginx.conf has no upstream block either."""
        for name, text in (("nginx.conf", NGINX_CONF), ("server.conf", SERVER),
                           ("proxy.conf", PROXY), ("routes.conf", ROUTES)):
            assert not re.search(r"^\s*upstream\s", _directives(text), re.MULTILINE), name
        assert "set $rust_upstream ks-rust-api;" in _directives(SERVER)
        assert "proxy_pass http://$rust_upstream:8090;" in _directives(PROXY)

    def test_nginx_does_not_wait_for_the_pilot(self):
        assert "rust-api" not in (NGINX_SERVICE.get("depends_on") or {})


class TestTheFallback:
    def test_only_501_and_502_are_handed_back(self):
        """501 is the pilot's own "the Python app serves it"; 502 is nginx's
        when the pilot did not answer. Every other answer is the pilot's to
        give — handing back its 500s would hide the disagreements the live
        comparison looks for."""
        directives = _directives(PROXY)
        assert "proxy_intercept_errors on;" in directives
        assert re.findall(r"^error_page .+;$", directives, re.MULTILINE) == [
            "error_page 501 502 = @rust_fallback_to_web;"
        ]

    def test_a_switched_route_is_proxied_as_the_api_location_proxies(self):
        """The pilot's location and the fallback both send web's headers and
        add `location /api/`'s `expires`: a client cannot tell who answered."""
        api = _block(NGINX_CONF, "location /api/")
        fallback = _block(SERVER, "location @rust_fallback_to_web")
        assert _headers(PROXY) == _headers(api) == _headers(fallback)
        for text in (api, fallback, PROXY):
            assert "expires 60s;" in _directives(text)
        assert "proxy_pass http://$web_upstream:8080;" in _directives(fallback)

    def test_both_halves_log_which_upstream_answered(self):
        log_format = re.search(r"log_format timed_upstream (.+?);", NGINX_CONF, re.DOTALL).group(1)
        assert "$upstream_addr" in log_format and "$upstream_status" in log_format
        for text in (PROXY, _block(SERVER, "location @rust_fallback_to_web")):
            assert "access_log /var/log/nginx/access.log timed_upstream;" in _directives(text)


class TestTheService:
    def test_it_sits_in_a_profile_the_python_deploy_never_names(self):
        """A plain `docker compose pull` / `up -d` neither pulls its image nor
        starts it, so the pilot cannot fail a Python deploy."""
        assert SERVICE["profiles"] == ["rust"]
        assert "--profile" not in _python_deploy_script()

    def test_it_publishes_no_port(self):
        """Charter rule 12. nginx reaches it inside the project's network."""
        assert "ports" not in SERVICE

    def test_it_is_on_the_internal_network_only(self):
        assert SERVICE["networks"] == ["default"]

    def test_it_reads_as_the_read_only_role(self):
        """`ks_readonly` holds SELECT and nothing else, so "the pilot writes
        nothing" is a grant, not only a property of its code."""
        env = SERVICE["environment"]
        assert env["KS_PG_DSN"].startswith("postgresql://ks_readonly:${KS_READONLY_PASSWORD")
        assert env["KS_PG_DSN"].endswith("@postgres:5432/ks")
        assert not [k for k, v in env.items() if "ks_app" in str(v)]

    def test_it_takes_no_env_file(self):
        """Named variables only: `.env` whole would hand it every secret."""
        assert "env_file" not in SERVICE
        assert set(SERVICE["environment"]) == {
            "KS_PG_DSN", "KS_PG_POOL_MAX", "KS_USER_STORE", "KS_BOT_STORE",
            "DASHBOARD_SECRET_KEY", "KS_HARDCODED_ADMIN_IDS",
        }

    def test_the_session_key_is_the_one_web_signs_with(self):
        """`config.web.secret_key or config.bot.token` (web/routes/auth.py):
        compose's `:-` takes the default when the variable is unset or empty,
        as `or` does."""
        assert SERVICE["environment"]["DASHBOARD_SECRET_KEY"] == "${DASHBOARD_SECRET_KEY:-${BOT_TOKEN:-}}"
        assert "config.web.secret_key or config.bot.token" in (REPO / "web" / "routes" / "auth.py").read_text()

    def test_the_admins_come_from_the_same_env(self):
        """The pilot refuses to start unless they hash to core.permissions'
        set, so a disagreement is loud at start rather than a session judged
        differently."""
        assert SERVICE["environment"]["KS_HARDCODED_ADMIN_IDS"] == "${ADMIN_USER_IDS:-}"
        assert "their hash is not the contract's" in (RUST_SRC / "main.rs").read_text()

    def test_its_memory_is_capped_without_swap(self):
        """The host is shared with other projects."""
        assert SERVICE["mem_limit"] == SERVICE["memswap_limit"]
        assert re.fullmatch(r"\d+m", SERVICE["mem_limit"]) and int(SERVICE["mem_limit"][:-1]) <= 256

    def test_its_healthcheck_asks_readiness(self):
        """Rule 13: a check that does the system's work. `/readyz` reads the
        schema revision as ks_readonly; the image has no shell, so the binary
        itself asks it."""
        assert SERVICE["healthcheck"]["test"] == ["CMD", "/usr/local/bin/ks-api", "healthcheck"]
        assert 'probe(target, "/readyz")' in (RUST_SRC / "main.rs").read_text()
        assert '.route("/readyz", get(readyz))' in (RUST_SRC / "http.rs").read_text()

    def test_the_image_is_the_one_the_deploy_pushes(self):
        tags = JOBS["build-rust"]["steps"][-1]["with"]["tags"]
        assert SERVICE["image"] == "halloweex/keycrm-rust-api:latest"
        assert "${{ secrets.DOCKER_USERNAME }}/keycrm-rust-api:latest" in tags

    def test_it_listens_where_nginx_dials(self):
        last_stage = DOCKERFILE.rsplit("\nFROM ", 1)[1]
        assert "ENV KS_PILOT_LISTEN=0.0.0.0:8090" in last_stage
        assert SERVICE["expose"] == ["8090"]


class TestTheImageHasNoTestClock:
    def test_the_last_stage_is_the_service_built_without_the_feature(self):
        """A plain build — what the deploy does — produces the last stage. It
        copies the binary `build-api` made, and that build names no feature:
        `X-Pilot-Now` is compiled out of what production runs."""
        stages = re.split(r"^FROM ", DOCKERFILE, flags=re.MULTILINE)[1:]
        last = stages[-1]
        assert " AS " not in last.splitlines()[0], "the service must be the unnamed last stage"
        assert "COPY --from=build-api /out/ks-api /usr/local/bin/ks-api" in last
        build_api = next(s for s in stages if s.splitlines()[0].endswith(" AS build-api"))
        assert "cargo build --release --locked -p ks-api" in build_api
        assert "test-clock" not in build_api and "--features" not in build_api

    def test_the_deploy_builds_that_stage(self):
        build = JOBS["build-rust"]["steps"][-1]["with"]
        assert build["file"] == "./Dockerfile.rust"
        assert build["context"] == "rust"
        assert "target" not in build
        assert build["platforms"] == "linux/arm64"


class TestTheDeploySplitsTheHalves:
    def test_the_python_half_waits_for_its_flag(self):
        for job in ("build", "deploy"):
            assert JOBS[job]["if"] == "needs.changes.outputs.python == 'true'", job
        assert "changes" in JOBS["deploy"]["needs"]

    def test_the_rust_image_is_built_only_when_it_changed(self):
        assert JOBS["build-rust"]["if"] == "needs.changes.outputs.rust_image == 'true'"
        assert JOBS["build-rust"]["runs-on"] == "ubuntu-24.04-arm"

    def test_the_rust_deploy_runs_after_the_python_one_and_survives_its_skip(self):
        """`!failure()` and not the default `success()`: a skipped Python half
        is not a failure, a failed one is."""
        job = JOBS["deploy-rust"]
        assert job["needs"] == ["changes", "build-rust", "deploy"]
        assert job["if"].startswith("!cancelled() && !failure() && ")
        assert job["if"].endswith("needs.changes.outputs.rust_deploy == 'true'")

    def test_the_rust_deploy_touches_its_service_and_reloads_nginx(self):
        script = _rust_deploy_script()
        for line in ('git reset --hard "$GH_SHA"',
                     "docker compose --profile rust pull rust-api",
                     "docker compose --profile rust up -d --no-deps rust-api",
                     "docker exec keycrm-nginx nginx -t",
                     "docker exec keycrm-nginx nginx -s reload"):
            assert line in script, line
        assert script.index("nginx -t") < script.index("nginx -s reload")
        assert "= healthy ]" in script, "started is not ready"

    def test_each_half_is_measured_from_its_own_last_deploy(self):
        """Measured from the previous push, a run cancelled while pending would
        leave its changes undeployed: the newer run would not see them."""
        run = JOBS["changes"]["steps"][-1]["run"]
        assert "last_deployed deploy)" in run and "last_deployed deploy-rust)" in run
        assert ".github/scripts/changed_components.py" in run
        assert (REPO / ".github" / "scripts" / "changed_components.py").is_file()

    def test_one_deploy_at_a_time_still(self):
        assert WORKFLOW["concurrency"] == {"group": "deploy-production", "cancel-in-progress": False}
