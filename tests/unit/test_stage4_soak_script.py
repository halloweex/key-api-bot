"""deploy/stage4_soak.sh, run against a fake `docker` (DN-03).

The SQL files are tested against a real Postgres in
`tests/integration/test_stage4_soak_sql.py`. What is left is the script's own
contract, and it is the part a daily reader acts on: the exit code, the S0 log
grep, what a check that did not run turns into, and the promises the header
makes — every session read-only, `postgres` only where a file asks for it, and
the web container's environment asked for by name, never listed.

The fake answers the way the real commands do: `inspect` prints the running
state, `printenv NAME` exits 1 for an unset name, and `psql` reads the file on
stdin. It records every call, which is how the promises are checked. A run
costs about a second, so the healthy run is shared and flag cases are paired.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "deploy" / "stage4_soak.sh"
SQL_DIR = REPO / "deploy" / "stage4_soak"
FILES = sorted(SQL_DIR.glob("*.sql"))

pytestmark = pytest.mark.skipif(
    not (shutil.which("bash") and shutil.which("awk")), reason="needs bash and awk")

FAKE_DOCKER = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_DOCKER_CALLS"
case "$1" in
    inspect)
        case "${FAKE_WEB:-running}" in
            missing) exit 1 ;;
            running) echo true ;;
            *) echo false ;;
        esac
        exit 0 ;;
    logs)
        printf '%s\n' "${FAKE_LOGS:-INFO all quiet}"
        exit 0 ;;
    exec)
        if [ "$2" = "keycrm-web" ]; then
            case "$4" in
                KS_WRITE_INVENTORY) v="${FAKE_KS_WRITE_INVENTORY-__unset__}" ;;
                KS_DQ_PG_WAREHOUSE) v="${FAKE_KS_DQ_PG_WAREHOUSE-__unset__}" ;;
                *) v="__unset__" ;;
            esac
            [ "$v" = "__unset__" ] && exit 1
            printf '%s\n' "$v"
            exit 0
        fi
        sql="$(cat)"
        check="$(printf '%s\n' "$sql" | sed -n "s/^SELECT '\([^']*\)'::text AS \"check\",.*/\1/p" | head -n 1)"
        if [ "$check" = "${FAKE_PSQL_ERROR:-}" ]; then
            echo 'ERROR:  permission denied for sequence' >&2
            exit 3
        fi
        verdict=PASS
        [ "$check" = "${FAKE_FAIL:-}" ] && verdict=FAIL
        [ "$check" = "${FAKE_UNKNOWN:-}" ] && verdict=UNKNOWN
        printf '%s|%s|detail for %s | with a bar\n' "$check" "$verdict" "$check"
        exit 0 ;;
esac
exit 2
"""


def _labels():
    """The check label each file returns, read the way the fake reads it."""
    out = {}
    for path in FILES:
        m = re.search(r"""^SELECT '([^']*)'::text AS "check",""",
                      path.read_text(encoding="utf-8"), flags=re.M)
        assert m, f"{path.name} has no `SELECT '<label>'::text AS \"check\",` line"
        out[path.stem] = m.group(1)
    return out


LABELS = _labels()


class Run:
    def __init__(self, done, calls):
        self.code = done.returncode
        self.out = done.stdout + done.stderr
        self.rows = {}
        for line in done.stdout.splitlines():
            m = re.match(r"^(.+?)\s{2,}(PASS|FAIL|UNKNOWN)\s{2,}(.*)$", line)
            if m:
                self.rows[m.group(1)] = (m.group(2), m.group(3))
        self.calls = calls
        self.psql = [c for c in calls if c.startswith("exec") and " psql " in c]


def _run(workdir: Path, **fake: str) -> Run:
    bin_dir = workdir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    docker = bin_dir / "docker"
    docker.write_text(FAKE_DOCKER)
    docker.chmod(0o755)
    calls = workdir / "calls.log"
    calls.write_text("")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FAKE_", "SOAK_"))}
    env.update({"PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
                "FAKE_DOCKER_CALLS": str(calls), **fake})
    done = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True,
                          text=True, timeout=120)
    return Run(done, calls.read_text().splitlines())


@pytest.fixture(scope="module")
def healthy(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("healthy"))


class TestExitCode:
    def test_all_pass_is_zero(self, healthy):
        assert healthy.code == 0, healthy.out
        assert set(healthy.rows) == set(LABELS.values()) | {"S0 order intake (log)"}
        assert {v for v, _ in healthy.rows.values()} == {"PASS"}
        assert f"{len(healthy.rows)} PASS, 0 FAIL, 0 UNKNOWN" in healthy.out

    def test_any_fail_is_one(self, tmp_path):
        run = _run(tmp_path, FAKE_FAIL="D4 unmarked writes")
        assert run.code == 1, run.out
        assert run.rows["D4 unmarked writes"][0] == "FAIL"

    def test_unknown_without_fail_is_two(self, tmp_path):
        run = _run(tmp_path, FAKE_UNKNOWN="D8 mirror_landing (derived)")
        assert run.code == 2, run.out
        assert run.rows["D8 mirror_landing (derived)"][0] == "UNKNOWN"

    def test_fail_outranks_unknown(self, tmp_path):
        run = _run(tmp_path, FAKE_FAIL="D1 owed rebuild",
                   FAKE_UNKNOWN="D8 mirror_landing (derived)")
        assert run.code == 1, run.out

    def test_a_query_that_did_not_run_is_unknown_not_pass(self, tmp_path):
        """Named by its file, since it never got as far as naming itself."""
        run = _run(tmp_path, FAKE_PSQL_ERROR="D2 derivation journal")
        assert run.code == 2, run.out
        verdict, detail = run.rows["03_d2_derivation_journal"]
        assert verdict == "UNKNOWN"
        assert "did not run" in detail and "permission denied" in detail
        assert "D2 derivation journal" not in run.rows


class TestS0LogHalf:
    @pytest.mark.parametrize("line", [
        "ERROR core.scheduler: Job incremental_sync failed: RuntimeError",
        "RuntimeError: KS_WRITE_EXPENSES='postgress' is not understood; "
        "expected 'duckdb' or 'postgres'",
    ])
    def test_a_matching_line_fails(self, tmp_path, line):
        run = _run(tmp_path, FAKE_LOGS=f"INFO fine\n{line}\nINFO fine")
        assert run.code == 1, run.out
        verdict, detail = run.rows["S0 order intake (log)"]
        assert verdict == "FAIL" and "1 line(s)" in detail

    def test_a_stopped_web_container_fails_and_its_flags_are_unknown(self, tmp_path):
        run = _run(tmp_path, FAKE_WEB="exited")
        assert run.code == 1, run.out
        assert run.rows["S0 order intake (log)"][0] == "FAIL"
        assert not [c for c in run.calls if c.startswith("logs")]
        assert all("inventory_on=unknown " in c and "dq_pg_warehouse_on=unknown" in c
                   for c in run.psql)

    def test_no_web_container_at_all_is_unknown(self, tmp_path):
        run = _run(tmp_path, FAKE_WEB="missing")
        assert run.rows["S0 order intake (log)"][0] == "UNKNOWN"
        assert run.code == 2, run.out


class TestWhatTheScriptPromises:
    def test_every_file_runs_once_in_a_read_only_session(self, healthy):
        assert len(healthy.psql) == len(FILES)
        for call in healthy.psql:
            assert "PGOPTIONS=-c default_transaction_read_only=on" in call, call
            assert "ON_ERROR_STOP=1" in call, call

    def test_postgres_only_where_a_file_asks(self, healthy):
        users = [re.search(r"-U (\S+)", c).group(1) for c in healthy.psql]
        asking = sum(1 for p in FILES if p.read_text(encoding="utf-8").splitlines()[0]
                     == "-- soak:run-as postgres")
        assert asking == 2
        assert users.count("postgres") == asking
        assert users.count("ks_readonly") == len(FILES) - asking

    def test_the_environment_is_asked_by_name_and_never_listed(self, healthy):
        """The web container's environment holds every secret on the host."""
        web = [c for c in healthy.calls if c.startswith("exec keycrm-web")]
        assert web, healthy.calls
        assert all(re.fullmatch(r"exec keycrm-web printenv KS_[A-Z_]+", c) for c in web), web
        assert not [c for c in healthy.calls if "Config.Env" in c]

    def test_unset_flags_are_zero(self, healthy):
        assert all("inventory_on=0 " in c and "dq_pg_warehouse_on=0" in c for c in healthy.psql)

    def test_set_flags_are_read_the_way_the_application_reads_them(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_INVENTORY=" Postgres ", FAKE_KS_DQ_PG_WAREHOUSE="on")
        assert all("inventory_on=1 " in c and "dq_pg_warehouse_on=1" in c for c in run.psql)

    def test_values_nobody_understands_are_invalid(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_INVENTORY="postgress", FAKE_KS_DQ_PG_WAREHOUSE="yes")
        assert all("inventory_on=invalid " in c and "dq_pg_warehouse_on=invalid" in c
                   for c in run.psql)

    def test_a_bar_inside_a_detail_survives(self, healthy):
        assert healthy.rows["D1 owed rebuild"][1] == "detail for D1 owed rebuild | with a bar"

    def test_it_lives_in_no_image(self):
        for dockerfile in REPO.glob("Dockerfile*"):
            copies = re.findall(r"^COPY\s+(?:--\S+\s+)?(\S+)", dockerfile.read_text(), flags=re.M)
            assert not [c for c in copies if c.rstrip("/") in {"deploy", "."}], (
                dockerfile.name, copies)
