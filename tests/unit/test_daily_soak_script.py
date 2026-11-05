"""deploy/daily_soak.sh, run against a fake `docker` (DN-03).

The SQL files are tested against a real Postgres in
`tests/integration/test_daily_soak_sql.py`. What is left is the script's own
contract, and it is the part a daily reader acts on: the exit code, the S0 log
grep, what a check that did not run turns into, and the promises the header
makes — every session read-only, `ks_app` only where a file asks for it, and
the web container's environment and its files never asked at all.

It was deploy/stage4_soak.sh, which read five KS_WRITE_* flags, three buyer
readers and KS_DUCKDB by exact name and five latch markers from the web
container, and passed what it found to the checks. Step 14's S14-20 renamed
it and took every one of those gates out: every chain writes Postgres since
S14-00 whatever its flag or its marker says, and a step-14 web runs `off` on
any KS_DUCKDB. The tests of those readings went with them.

The fake answers the way the real commands do: `inspect` prints the running
state and `psql` reads the file on stdin. It records every call, which is how
the promises are checked. A run costs about a second, so the healthy run is
shared.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "deploy" / "daily_soak.sh"
SQL_DIR = REPO / "deploy" / "daily_soak"
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
        # Nothing is asked of the web container since S14-20: no flag, no
        # marker. Answered the way an unset name would be, and recorded.
        case " $* " in *" keycrm-web "*) exit 1 ;; esac
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
                "FAKE_DOCKER_CALLS": str(calls),
                # The file-hash record P1 reads: none, unless a test makes
                # one. Never the host's /root/duckdb-silence.
                "DUCKDB_SILENCE_STATE_DIR": str(workdir / "silence"), **fake})
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
    # The KS_WRITE_* typo line ("is not understood; expected") matched too,
    # until step 14's S14-11 stopped reading the flags and S14-20 the grep.
    @pytest.mark.parametrize("line", [
        "ERROR core.scheduler: Job incremental_sync failed: RuntimeError",
    ])
    def test_a_matching_line_fails(self, tmp_path, line):
        run = _run(tmp_path, FAKE_LOGS=f"INFO fine\n{line}\nINFO fine")
        assert run.code == 1, run.out
        verdict, detail = run.rows["S0 order intake (log)"]
        assert verdict == "FAIL" and "1 line(s)" in detail

    def test_a_stopped_web_container_fails(self, tmp_path):
        run = _run(tmp_path, FAKE_WEB="exited")
        assert run.code == 1, run.out
        assert run.rows["S0 order intake (log)"][0] == "FAIL"
        assert not [c for c in run.calls if c.startswith("logs")]
        # The checks run all the same, and none of them is told about web.
        assert len(run.psql) == len(FILES)

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

    def test_the_owner_only_where_a_file_asks_and_never_the_superuser(self, healthy):
        users = [re.search(r"-U (\S+)", c).group(1) for c in healthy.psql]
        asking = sum(1 for p in FILES if p.read_text(encoding="utf-8").splitlines()[0]
                     == "-- soak:run-as ks_app")
        assert asking == 2
        assert users.count("ks_app") == asking
        assert "postgres" not in users
        assert users.count("ks_readonly") == len(FILES) - asking

    def test_the_web_container_is_never_asked(self, healthy):
        """Not its environment — it holds every secret on the host — and not
        its files. The script asked five flags, three readers and KS_DUCKDB
        by name and five latch markers until S14-20. Mutation: put a
        `docker exec keycrm-web printenv` or a `test -f` back."""
        assert not [c for c in healthy.calls if "keycrm-web" in c
                    and not c.startswith(("inspect", "logs"))], healthy.calls
        assert not [c for c in healthy.calls if "Config.Env" in c]
        code = [ln for ln in SCRIPT.read_text().splitlines()
                if not ln.lstrip().startswith("#")]
        assert not [ln for ln in code if "printenv" in ln or "test -f" in ln
                    or "write-chain-owners" in ln], code

    def test_no_gate_is_passed_to_the_checks(self, healthy):
        """The flags', the latches' and KS_DUCKDB's variables went with the
        gates, and so did the flip-day times. Mutation: pass `-v
        orders_on=...` again."""
        gone = ("inventory_on", "inventory_flip_at", "buyers_on", "buyers_flip_at",
                "managers_on", "managers_flip_at", "orders_on", "orders_flip_at",
                "dq_journal_direct", "duckdb_off", "dq_pg_warehouse_on")
        for call in healthy.psql:
            assert not [name for name in gone if f" {name}=" in f" {call}"], call

    def test_a_flip_time_in_the_environment_changes_nothing(self, tmp_path, healthy):
        """The flip-day knobs read nothing any more: the same sessions with
        them set. Mutation: read SOAK_ORDERS_FLIP_AT again."""
        run = _run(tmp_path, SOAK_INVENTORY_FLIP_AT="2026-10-01 10:00+03",
                   SOAK_BUYERS_FLIP_AT="2026-10-01 10:30+03",
                   SOAK_MANAGERS_FLIP_AT="2026-10-08 10:30+03",
                   SOAK_ORDERS_FLIP_AT="2026-10-12 10:30+03")
        assert run.psql == healthy.psql
        assert "flip" not in run.out.lower()

    def test_the_override_floor_is_passed(self, tmp_path, healthy):
        """B3's one knob, kept: the fewest human overrides there may be."""
        assert all("buyers_override_floor= " in c for c in healthy.psql), healthy.psql
        run = _run(tmp_path, SOAK_BUYERS_OVERRIDE_FLOOR="3")
        assert all("buyers_override_floor=3 " in c for c in run.psql), run.psql
        assert "override floor 3" in run.out

    def test_a_bar_inside_a_detail_survives(self, healthy):
        assert healthy.rows["D1 owed rebuild"][1] == "detail for D1 owed rebuild | with a bar"

    def test_it_lives_in_no_image(self):
        for dockerfile in REPO.glob("Dockerfile*"):
            copies = re.findall(r"^COPY\s+(?:--\S+\s+)?(\S+)", dockerfile.read_text(), flags=re.M)
            assert not [c for c in copies if c.rstrip("/") in {"deploy", "."}], (
                dockerfile.name, copies)


def test_the_order_checks_are_in_the_report(healthy):
    """O2–O5, each once, in the order a reader works down them. (O1, the
    copies stood down, went in step 14's S14-13 with the copies.)"""
    labels = [label for label in healthy.rows if label.startswith("O")]
    assert labels == ["O2 orders watermark", "O3 order versions",
                      "O4 half-written orders", "O5 orders integrity"], labels


# `TestChain4`, `TestChain5`, `TestChain3` and `TestChain9` held the states the
# script read for chains 4, 5, 3 and 9 — 0, 1, held, pending, invalid,
# unknown, the latch outranking the flag — until step 14's S14-20 deleted the
# readings: every chain writes Postgres whatever its flag or its marker says.


class TestTheDuckDBFile:
    """The DuckDB file (P1, OD-17 (a)): the file's hash record read with
    `--status` — which hashes nothing and writes nothing. P2–P4 and F1 went
    at step 14's S14-02, and with P4 the report's one reason to read `.env`;
    KS_DUCKDB, which P1 was gated on, went at S14-20."""

    def test_no_record(self, healthy):
        assert all("duckdb_file_last=none " in c and "duckdb_file_since= " in c
                   for c in healthy.psql), healthy.psql
        assert "DuckDB file record: none" in healthy.out

    def test_the_retired_checks_and_their_inputs_are_gone(self, healthy):
        """The parallel period's declared start and `.env`'s reading went with
        P3 and P4: nothing passes them, and nothing reads `.env` — a file whose
        other lines are secrets — for a check that no longer exists. Mutation:
        put the `.env` reader back, or a `-v parallel_from=`."""
        assert not any("parallel_from=" in c or "duckdb_off_env=" in c
                       or "duckdb_env_changed_at=" in c for c in healthy.psql), healthy.psql
        code = [ln for ln in SCRIPT.read_text().splitlines()
                if not ln.lstrip().startswith("#")]
        assert not [ln for ln in code
                    if any(word in ln for word in ("duckdb_env.sh", "ks_duckdb_env_read",
                                                   "SOAK_ENV_FILE", "SOAK_PARALLEL_FROM"))], code
        labels = set(LABELS.values())
        assert not {label for label in labels if label.split()[0] in ("P2", "P3", "P4", "F1")}, labels

    def test_the_record_is_read_and_left_as_it_was(self, tmp_path):
        """The hourly cron's record, made by the real script, then read by the
        report: every field passed through, and the record byte for byte as
        it was. Mutation: call the check without `--status` in the report —
        it would hash the file and write the record."""
        db = tmp_path / "analytics.duckdb"
        db.write_bytes(b"duck")
        state = tmp_path / "silence"
        base = {k: v for k, v in os.environ.items() if not k.startswith("DUCKDB_")}
        made = subprocess.run(
            ["bash", str(REPO / "deploy" / "duckdb_silence_check.sh")],
            env={**base, "DUCKDB_FILE": str(db), "DUCKDB_SILENCE_STATE_DIR": str(state),
                 "DUCKDB_SILENCE_NOW": "1900000000"},
            capture_output=True, text=True, timeout=60)
        assert made.returncode == 2, made.stdout + made.stderr
        before = {p.name: p.read_bytes() for p in state.iterdir()}
        db.write_bytes(b"changed")  # a record-mode run would now say CHANGED

        run = _run(tmp_path, DUCKDB_SILENCE_STATE_DIR=str(state), DUCKDB_FILE=str(db))
        assert all("duckdb_file_last=BASELINE " in c
                   and "duckdb_file_since=2030-03-17T17:46:40Z " in c
                   and "duckdb_file_since_reason=baseline " in c
                   and "duckdb_file_checked_at=2030-03-17T17:46:40Z" in c
                   for c in run.psql), run.psql
        assert {p.name: p.read_bytes() for p in state.iterdir()} == before

    def test_a_missing_episode_reaches_the_checks(self, tmp_path):
        """The review of 02.10's sequence, by the real script: baseline, the
        file moved away for a check, moved back. The record reads UNCHANGED
        since the baseline; `missing_at` is what tells P1. Mutation:
        drop the `missing_at=*` case from `silence_record`."""
        db = tmp_path / "analytics.duckdb"
        db.write_bytes(b"duck")
        state = tmp_path / "silence"
        base = {k: v for k, v in os.environ.items() if not k.startswith("DUCKDB_")}
        env = {**base, "DUCKDB_FILE": str(db), "DUCKDB_SILENCE_STATE_DIR": str(state)}
        check = ["bash", str(REPO / "deploy" / "duckdb_silence_check.sh")]
        for now, move in ((1900000000, None), (1900003600, "away"), (1900007200, "back")):
            if move == "away":
                db.rename(tmp_path / "away")
            elif move == "back":
                (tmp_path / "away").rename(db)
            subprocess.run(check, env={**env, "DUCKDB_SILENCE_NOW": str(now)},
                           capture_output=True, text=True, timeout=60)
        run = _run(tmp_path, DUCKDB_SILENCE_STATE_DIR=str(state), DUCKDB_FILE=str(db))
        assert all("duckdb_file_last=UNCHANGED " in c
                   and "duckdb_file_since_reason=baseline " in c
                   and "duckdb_file_missing_at=2030-03-17T18:46:40Z" in c
                   for c in run.psql), run.psql
        assert "last missing 2030-03-17T18:46:40Z" in run.out

    def test_an_unreadable_record_is_an_error_not_a_pass(self, tmp_path):
        state = tmp_path / "silence"
        state.mkdir()
        (state / "state").write_text("nonsense=1\n")
        run = _run(tmp_path, DUCKDB_SILENCE_STATE_DIR=str(state))
        assert all("duckdb_file_last=error " in c for c in run.psql)

    def test_the_check_is_only_ever_asked_for_its_status(self):
        calls = re.findall(r'"\$SILENCE_CHECK"[^\n]*', SCRIPT.read_text())
        assert calls == ['"$SILENCE_CHECK" --status 2>&1 || true)"'], calls


def test_every_variable_a_check_reads_is_passed():
    """Walked over the checks, not listed: a `:'name'` a file reads that the
    script does not pass reaches the server as psql's literal text, and the
    check judges that. And nothing else is passed: a `-v` no check reads is
    an input left behind by a check that went (S14-02 took P2–P4's three),
    computed every morning for nobody. Every read is the quoted form, so
    the walk sees them all. Mutation: drop `-v duckdb_file_last=...` from
    the script, or put back `-v parallel_from=...`."""
    passed = set(re.findall(r"-v (\w+)=", SCRIPT.read_text())) - {"ON_ERROR_STOP"}
    read = set()
    for path in FILES:
        sql = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))
        read |= set(re.findall(r":'(\w+)'", sql))
        # A bare `:name` or `:"name"` would be read too, and the walk above
        # would not see it; `::type` casts are not variables.
        assert not re.findall(r"(?<![:\w]):(?:\"\w+\"|[A-Za-z_]\w*)", sql), path.name
    assert read <= passed, sorted(read - passed)
    assert passed <= read, sorted(passed - read)


# A soak file is named in prose far from where it lives — a module docstring
# telling the reader which check reads its rows, a canary comment naming the
# check that judges its page — and a renumbering leaves those pointing at
# nothing. Found once: three such names survived a renumbering in the change
# that added P1–P4. Walked, not listed: every tree a reader would look in.
_SOAK_NAME = re.compile(r"\b\d{2}_[a-z0-9]+_[a-z0-9_]+\.sql\b")
_PROSE_TREES = ("core", "bot", "web", "scripts", "deploy", "tests")


def test_every_soak_file_named_anywhere_exists():
    """Mutation: put P4's old number back in the canary's comment (52 for 53,
    `p3` for `p4`), a file that does not exist, and this fails naming the
    file and the name."""
    sources = [REPO / ".claude" / "CLAUDE.md"]
    for tree in _PROSE_TREES:
        sources += [p for p in (REPO / tree).rglob("*")
                    if p.is_file() and p.suffix in {".py", ".sh", ".sql", ".md", ".yml"}
                    and "__pycache__" not in p.parts]
    existing = {p.name for p in FILES}
    dangling = sorted(
        f"{path.relative_to(REPO)}: {name}"
        for path in sources if path.exists()
        for name in set(_SOAK_NAME.findall(path.read_text(errors="replace")))
        if name not in existing)
    assert not dangling, dangling
