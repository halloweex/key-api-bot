"""scripts/weekly_compact.sh, run end to end against a fake `docker`.

The wrapper runs as root from host cron at 02:00 on Sundays, with web and bot
stopped by the time it reaches the part tested here. `set -u` turns a typo in a
variable name into an exit halfway through, and a missing marker in the
sidecar's output must never become an abort. So it is run, not read.

The sidecar logs these tests feed it are the real output of the real
compaction phases, captured in-process — the only way the section headers the
wrapper cuts on cannot drift away from the ones the script prints. The one
exception is the older image, whose output is quoted from the code it replaced.

Only the four path constants are rewritten, each asserted to occur exactly
once, and `docker`, `sleep`, `curl` and notify.sh are fakes on a private PATH
and in a private compose directory. Nothing reaches a server. `rm` is the real
one behind a recorder, so a run can be asked whether it deleted anything at
all. The compose directory holds the real deploy/duckdb_env.sh, and a `.env`
only when a test writes one — which is how the week of silence's stand-down
(stage 5, PR-10) is driven.
"""
from __future__ import annotations

import contextlib
import io
import os
import re
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

from tests.unit import test_compact_sequences as seq

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "weekly_compact.sh"

pytestmark = pytest.mark.skipif(
    not (shutil.which("bash") and shutil.which("sed")), reason="needs bash and sed")

# Removing the container takes its log, as the real `docker rm` does, so a copy
# taken after the removal finds nothing — which is what the order of the
# wrapper's steps is tested against. The cleanup before `run` has no container
# to remove. `ps` says Up for as long as FAKE_PS says so: never, by default.
FAKE_DOCKER = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_DOCKER_CALLS"
case "$1" in
    compose) exit 0 ;;
    run) touch "$FAKE_LOGS.started"; echo fake-container-id; exit 0 ;;
    rm)
        if [ -e "$FAKE_LOGS.started" ]; then rm -f "$FAKE_LOGS" "$FAKE_LOGS.started"; fi
        exit 0 ;;
    ps) if [ -n "${FAKE_PS:-}" ]; then echo "$FAKE_PS"; fi; exit 0 ;;
    inspect) echo "${FAKE_EXIT:-0}"; exit 0 ;;
    logs)
        if [ ! -e "$FAKE_LOGS" ]; then
            echo "Error response from daemon: No such container: $2" >&2; exit 1
        fi
        if [ "${3:-}" = "--tail" ]; then tail -n "$4" "$FAKE_LOGS"; else cat "$FAKE_LOGS"; fi
        exit 0 ;;
esac
exit 2
"""

# The real `rm`, recorded: every deletion the wrapper makes goes through it,
# so an empty record is a run that deleted nothing.
FAKE_RM = f"""#!/usr/bin/env bash
[ -n "${{FAKE_RM_CALLS:-}}" ] && printf '%s\\n' "$*" >> "$FAKE_RM_CALLS"
exec {shutil.which("rm")} "$@"
"""

FAKE_NOTIFY = r"""notify() {
    printf '%s\n' "$1" >> "$FAKE_NOTIFY"
}
"""

# What origin/main's compact_duckdb.py printed around its restore: a section
# header it shares with today's, lines in a different shape, no sequence check.
OLDER_IMAGE_LOG = (
    "[02:00:39] \x1b[32mOK\x1b[0m    stock_movements: 57,090 rows\n"
    "[02:00:40] INFO  \nRestoring sequences:\n"
    "[02:00:40] \x1b[32mOK\x1b[0m    warehouse_refresh_seq: advanced 75,454 to hand out 75455 next\n"
    "[02:00:40] \x1b[32mOK\x1b[0m    reconciliation_seq: advanced 2,614 to hand out 2615 next\n"
    "[02:00:40] INFO  \nFlushing WAL...\n"
    "[02:00:41] INFO  \nDerived table placeholders:\n"
    "[02:00:41] \x1b[32mOK\x1b[0m  \nALL VALIDATIONS PASSED\n"
)


# A column name shaped like a DSN with its password: DuckDB quotes the name in
# the Binder Error, so the real failure output carries a credential to mask.
LEAKY_COLUMN = "postgresql://ks_app:s3cret-pw@ks-postgres:5432/ks"


def _real_compaction_log(tmp_path_factory, *, fault: str = "") -> str:
    """Run the real phases on a production-shaped source and return stdout.

    `fault` is "" for a healthy run, "lagging" for a restore that moves no
    sequence (phase 3 refuses), "import" for a source column the schema does
    not have (phase 2 aborts before restoring), "disk" for a preflight refusal.
    """
    base = tmp_path_factory.mktemp("sidecar")
    data = base / "data"
    data.mkdir()
    buf = io.StringIO()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(seq.compact, "DATA_DIR", data)
        mp.setattr(seq.compact, "SOURCE_DB", data / "analytics.duckdb")
        mp.setattr(seq.compact, "NEW_DB", data / "analytics_clean.duckdb")
        mp.setattr(seq.compact, "EXPORT_DIR", data / "export_parquet")
        mp.setattr(seq.compact, "MANIFEST_PATH", data / "export_parquet" / "_manifest.json")
        mp.setattr("core.duckdb_store.DB_DIR", data)
        seq._build_source(seq.compact.SOURCE_DB, seq.PRODUCTION_SHAPE)
        if fault == "lagging":
            mp.setattr(seq.duckdb_sequences, "advance_to", lambda *a, **k: 0)
        elif fault == "import":
            conn = duckdb.connect(str(seq.compact.SOURCE_DB))
            conn.execute(f'ALTER TABLE warehouse_refreshes ADD COLUMN "{LEAKY_COLUMN}" VARCHAR')
            conn.execute(f"UPDATE warehouse_refreshes SET \"{LEAKY_COLUMN}\" = 'x'")
            conn.close()
        elif fault == "disk":
            mp.setattr(seq.compact.shutil, "disk_usage", lambda _: (10 * 2**30, 10 * 2**30, 0))
        with contextlib.redirect_stdout(buf):
            try:
                if fault == "disk":
                    seq.compact.preflight()
                seq._compact()
            except SystemExit:
                assert fault, "the healthy compaction exited"
    shutil.rmtree(base)
    return buf.getvalue()


@pytest.fixture(scope="module")
def healthy_log(tmp_path_factory):
    out = _real_compaction_log(tmp_path_factory)
    assert "ALL VALIDATIONS PASSED" in out
    return out


@pytest.fixture(scope="module")
def refused_log(tmp_path_factory):
    out = _real_compaction_log(tmp_path_factory, fault="lagging")
    assert "DO NOT SWAP" in out
    return out


@pytest.fixture(scope="module")
def import_failure_log(tmp_path_factory):
    out = _real_compaction_log(tmp_path_factory, fault="import")
    assert "ABORTING" in out and "Restoring sequences" not in out
    return out


@pytest.fixture(scope="module")
def preflight_refusal_log(tmp_path_factory):
    out = _real_compaction_log(tmp_path_factory, fault="disk")
    assert "Need at least" in out and "PHASE 1" not in out
    return out


class Run:
    def __init__(self, done, root: Path):
        self.code = done.returncode
        self.out = done.stdout + done.stderr
        self.log = (root / "compact.log").read_text() if (root / "compact.log").exists() else ""
        notified = root / "notify.txt"
        self.notify = notified.read_text() if notified.exists() else ""
        self.calls = (root / "calls.log").read_text().splitlines()
        self.rm = (root / "rm.log").read_text().splitlines()


@pytest.fixture(scope="module")
def fake_bin(tmp_path_factory):
    """Written once: every run reads its state from FAKE_* variables. A fresh
    executable per run cost about a second each on macOS, where the first exec
    of a new file is scanned."""
    bin_dir = tmp_path_factory.mktemp("bin")
    for name, body in (("docker", FAKE_DOCKER),
                       ("rm", FAKE_RM),
                       ("sleep", "#!/usr/bin/env bash\nexit 0\n"),
                       ("curl", "#!/usr/bin/env bash\nexit 0\n")):
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    return bin_dir


@pytest.fixture
def wrapper(tmp_path, fake_bin):
    return lambda sidecar_log, exit_code=0, ps="": _run(
        tmp_path, fake_bin, sidecar_log, exit_code, ps)


def _run(root: Path, bin_dir: Path, sidecar_log: str, exit_code: int, ps: str,
         *, env_text: "str | None" = None, env_lib: bool = True) -> Run:
    data = root / "data"
    compose = root / "compose"
    for d in (data, compose / "deploy"):
        d.mkdir(parents=True, exist_ok=True)
    (data / "analytics.duckdb").write_bytes(b"\0" * 4096)
    (compose / "deploy" / "notify.sh").write_text(FAKE_NOTIFY)
    if env_lib:
        shutil.copy(REPO / "deploy" / "duckdb_env.sh", compose / "deploy" / "duckdb_env.sh")
    if env_text is not None:
        (compose / ".env").write_text(env_text)
    (root / "sidecar.log").write_text(sidecar_log)
    (root / "sidecar.log.started").unlink(missing_ok=True)
    (root / "calls.log").write_text("")
    (root / "rm.log").write_text("")

    text = SCRIPT.read_text()
    for old, new in (
        ('DATA_DIR="/opt/key-api-bot/data"', f'DATA_DIR="{data}"'),
        ('COMPOSE_DIR="/opt/key-api-bot"', f'COMPOSE_DIR="{compose}"'),
        ('LOG="/var/log/keycrm-compact.log"', f'LOG="{root / "compact.log"}"'),
        ('HEALTH_URL="https://ksanalytics.duckdns.org/api/health"',
         'HEALTH_URL="http://127.0.0.1:9/health"'),
    ):
        assert text.count(old) == 1, f"the wrapper no longer says {old!r}"
        text = text.replace(old, new)
    script = root / "weekly_compact.sh"
    script.write_text(text)

    env = {k: v for k, v in os.environ.items() if not k.startswith("FAKE_")}
    env.update({
        "PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
        "FAKE_DOCKER_CALLS": str(root / "calls.log"),
        "FAKE_RM_CALLS": str(root / "rm.log"),
        "FAKE_LOGS": str(root / "sidecar.log"),
        "FAKE_NOTIFY": str(root / "notify.txt"),
        "FAKE_EXIT": str(exit_code),
        "FAKE_PS": ps,
    })
    done = subprocess.run(["bash", str(script)], env=env, capture_output=True,
                          text=True, timeout=60)
    return Run(done, root)


def _sequence_lines(text: str) -> list:
    return [line for line in text.splitlines() if re.search(r"sequence [a-z_]+:", line)]


class TestASuccessfulCompact:
    def test_the_restore_and_its_read_back_reach_the_host_log(self, wrapper, healthy_log):
        run = wrapper(healthy_log)

        assert run.code == 0, run.out
        assert "=== WEEKLY COMPACT END ===" in run.log
        assert "Restoring sequences:" in run.log
        assert "Sequence check (reopened read-only):" in run.log
        lines = _sequence_lines(run.log)
        # Six restored, six read back — every line the sidecar printed, and
        # nothing from the rest of its output.
        assert len(lines) == 12, run.log
        assert any("sequence warehouse_refresh_seq: next 75,454 (warehouse_refreshes.id "
                   "MAX 75,453" in line for line in lines)
        assert any("sequence reconciliation_seq: next 2,614 > reconciliation_log.id "
                   "MAX 2,597" in line for line in lines)
        assert "\x1b" not in run.log
        assert "stock_movements: 57,090 rows" not in run.log
        assert run.notify.startswith("✅ Weekly compact:")

    def test_the_copy_is_taken_as_soon_as_the_sidecar_exits(self, wrapper, healthy_log):
        """Before the verdict and the swap checks, each of which can abort and
        remove the container, and long before the final `docker rm -v`."""
        run = wrapper(healthy_log)

        copied = run.log.index("Sequences, as the sidecar printed them:")
        assert copied < run.log.index("Auto-swap OK")
        assert copied < run.log.index("Starting services...")
        final_rm = max(i for i, c in enumerate(run.calls) if c == "rm -v duckdb-compact")
        assert run.calls.index("logs duckdb-compact") < final_rm

    def test_an_older_image_leaves_what_it_printed_and_does_not_abort(self, wrapper):
        run = wrapper(OLDER_IMAGE_LOG)

        assert run.code == 0, run.out
        assert "warehouse_refresh_seq: advanced 75,454 to hand out 75455 next" in run.log
        assert "Sequence check" not in run.log
        assert "ABORT" not in run.log
        assert run.notify.startswith("✅ Weekly compact:")

    def test_no_sequence_section_at_all_is_said_and_is_not_an_abort(self, wrapper):
        run = wrapper("[02:00:41] OK  \nALL VALIDATIONS PASSED\n")

        assert run.code == 0, run.out
        assert ("Sequences: the sidecar printed no restore section (an image older "
                "than this wrapper?)") in run.log
        assert run.notify.startswith("✅ Weekly compact:")


class TestWhatAMissingRestoreSectionMeans:
    """Only a sidecar that got past the restore and printed none of it says
    anything about the image. Every other run without one stopped first, and
    blaming image drift there sent the reader after the wrong fault."""

    @pytest.mark.parametrize("sidecar_log, exit_code", [
        ("[02:00:41] OK  \nALL VALIDATIONS PASSED\n", 0),
        ("[02:00:40] INFO  \nFlushing WAL...\n"
         "[02:00:41] ERROR    orders: 10 expected 11\n", 1),
    ], ids=["validated", "flushed"])
    def test_past_the_restore_it_is_an_older_image(self, wrapper, sidecar_log, exit_code):
        run = wrapper(sidecar_log, exit_code=exit_code)

        assert "an image older than this wrapper?" in run.log
        assert "did not get as far as the restore" not in run.log

    @pytest.mark.parametrize("fixture", ["import_failure_log", "preflight_refusal_log"])
    def test_before_the_restore_it_says_the_sidecar_stopped_first(
        self, wrapper, request, fixture
    ):
        run = wrapper(request.getfixturevalue(fixture), exit_code=1)

        assert run.code == 1, run.out
        assert ("Sequences: none restored — the sidecar did not get as far as the "
                "restore") in run.log
        assert "older than this wrapper" not in run.log


class TestARefusedCompact:
    def test_the_alert_says_which_sequence_and_why(self, wrapper, refused_log):
        run = wrapper(refused_log, exit_code=1)

        assert run.code == 1, run.out
        first = run.notify.splitlines()[0]
        assert first.startswith("🚨 Compact aborted: compact failed (exit 1): FAIL: "
                                "sequence warehouse_refresh_seq: hands out 1 but "
                                "warehouse_refreshes.id MAX is 75,453")
        reason = first.split("(exit 1): ", 1)[1]
        assert len(reason) <= 300
        # The failed read-back is on the host too, not only in the alert.
        assert any("hands out 1 but warehouse_refreshes.id MAX is 75,453" in line
                   for line in _sequence_lines(run.log))
        assert "\x1b" not in run.log
        assert "rm -f -v duckdb-compact" in run.calls

    def test_without_the_summary_line_the_last_named_failure_is_quoted(
        self, wrapper, refused_log
    ):
        """An image without the FAIL line ends on "DO NOT SWAP", whose ERROR tag
        sits alone on the line above it. Quoting that tag said nothing."""
        older = "\n".join(line for line in refused_log.splitlines() if "FAIL: " not in line)
        run = wrapper(older + "\n", exit_code=1)

        assert run.notify.splitlines()[0] == (
            "🚨 Compact aborted: compact failed (exit 1): sequence "
            "seq_buyer_contacts_id: hands out 1 but buyer_contacts.id MAX is 34,460")

    def test_a_failed_import_names_the_table_and_is_masked(self, wrapper, import_failure_log):
        """The abort a column or table dropped from the schema produces. Its
        last line used to be "ABORTING — clean DB is incomplete", and the alert
        quoted exactly that. DuckDB's message quotes the column it refused, and
        a column can be named like anything — a DSN with its password here."""
        run = wrapper(import_failure_log, exit_code=1)

        assert run.code == 1, run.out
        first = run.notify.splitlines()[0]
        assert first.startswith(
            "🚨 Compact aborted: compact failed (exit 1): FAIL: warehouse_refreshes: "
            'Binder Error: Table "warehouse_refreshes" does not have a column with name '
            '"postgresql://***@ks-postgres:5432/ks"'), first
        reason = first.split("(exit 1): ", 1)[1]
        assert len(reason) <= 300
        assert "s3cret-pw" not in run.notify

    def test_nothing_quotable_leaves_the_alert_as_it_was(self, wrapper):
        run = wrapper("[02:00:05] INFO  killed\n", exit_code=137)

        assert run.code == 1, run.out
        assert run.notify.splitlines()[0] == "🚨 Compact aborted: compact failed (exit 137)"

    def test_the_quoted_line_is_bounded_and_carries_no_secret(self, wrapper):
        noisy = (
            "[02:00:05] \x1b[31mERROR\x1b[0m  pool failed for "
            "postgresql://ks_app:s3cret-pw@ks-postgres:5432/ks with "
            "123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawQ and $$); DROP TABLE x; `id` "
            + "x" * 600 + "\n"
        )
        run = wrapper(noisy, exit_code=1)

        reason = run.notify.splitlines()[0].split("(exit 1): ", 1)[1]
        assert len(reason) <= 300
        assert "s3cret-pw" not in reason and "postgresql://***@ks-postgres" in reason
        assert "AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawQ" not in reason
        assert "$" not in reason and "`" not in reason and "\x1b" not in reason
        assert reason.isascii() and reason.startswith("pool failed for postgresql://")

    def test_a_run_that_exits_zero_without_passing_says_why(self, wrapper, refused_log):
        run = wrapper(refused_log, exit_code=0)

        assert run.code == 1, run.out
        assert run.notify.splitlines()[0].startswith(
            "🚨 Compact aborted: compact validation did not pass: FAIL: sequence ")


class TestATimedOutCompact:
    """`ps` keeps saying Up until the wrapper gives up after TIMEOUT_SEC. The
    branch removes the container, and the fake `rm` takes its log with it."""

    STALLED = "Up 31 minutes"

    def test_what_the_sidecar_printed_is_copied_before_it_is_removed(
        self, wrapper, healthy_log
    ):
        """A sidecar that stalls after its restore and read-back — in the final
        CHECKPOINT, or the swap — has already printed what SEQ-3 was about."""
        run = wrapper(healthy_log, ps=self.STALLED)

        assert run.code == 1, run.out
        assert run.notify.splitlines()[0] == "🚨 Compact aborted: compact timeout after 1800s"
        assert "Sequences, as the sidecar printed them:" in run.log
        # The restore sits further up than the last 30 lines, so only the
        # section copy can have brought it.
        assert ("sequence warehouse_refresh_seq: next 75,454 (warehouse_refreshes.id "
                "MAX 75,453") in run.log
        assert "Compact still running after 1800s. Last log:" in run.log
        assert "ALL VALIDATIONS PASSED" in run.log  # the --tail 30
        launched = next(i for i, c in enumerate(run.calls) if c.startswith("run "))
        removed = run.calls.index("rm -f -v duckdb-compact", launched)
        read = [i for i, c in enumerate(run.calls) if c.startswith("logs duckdb-compact")]
        assert read and max(read) < removed

    def test_the_alert_says_why_when_the_sidecar_did(self, wrapper, refused_log):
        run = wrapper(refused_log, ps=self.STALLED)

        assert run.code == 1, run.out
        assert run.notify.splitlines()[0].startswith(
            "🚨 Compact aborted: compact timeout after 1800s: FAIL: sequence "
            "warehouse_refresh_seq: hands out 1 but warehouse_refreshes.id MAX is 75,453")


class TestTheWeekOfSilence:
    """Stage 5, PR-10. With `KS_DUCKDB=off` in `.env` the sidecar is refused by
    the switch before it opens the file — and the wrapper used to learn that
    last: the previous compaction's backup and the export deleted, web, bot
    and Meilisearch stopped, then the refusal, a minute of downtime and a
    "Compact aborted" page for a compaction the week had retired. Now it
    stands down first, and the proof is the run itself: no `docker` call, no
    `rm`, nothing on disk moved, no page."""

    LEFTOVERS = ("analytics.duckdb.old", "analytics_clean.duckdb",
                 "analytics_clean.duckdb.wal", "export_parquet/orders.parquet")

    @pytest.fixture
    def run_with_env(self, tmp_path, fake_bin, healthy_log):
        def run(env_text, **kw):
            data = tmp_path / "data"
            for rel in self.LEFTOVERS:
                (data / rel).parent.mkdir(parents=True, exist_ok=True)
                (data / rel).write_text(rel)
            result = _run(tmp_path, fake_bin, healthy_log, 0, "", env_text=env_text, **kw)
            return result, data
        return run

    def _untouched(self, run, data):
        assert run.calls == [], f"docker was called: {run.calls}"
        assert run.rm == [], f"something was deleted: {run.rm}"
        for rel in self.LEFTOVERS:
            assert (data / rel).read_text() == rel, rel
        assert (data / "analytics.duckdb").read_bytes() == b"\0" * 4096

    @pytest.mark.parametrize("env_text", [
        "KS_DUCKDB=off\n",
        "ADMIN_USER_IDS=1\nKS_DUCKDB=OFF \r\n",
        "KS_DUCKDB=on\nKS_DUCKDB=off\n",
    ], ids=["off", "upper-crlf", "last-line-wins"])
    def test_off_stops_nothing_deletes_nothing_and_pages_nobody(self, run_with_env, env_text):
        """Mutation: move the stand-down below `rm -f …analytics.duckdb.old`
        or below `docker compose stop` — the record is no longer empty."""
        run, data = run_with_env(env_text)
        assert run.code == 0, run.out
        self._untouched(run, data)
        assert run.notify == ""
        assert "KS_DUCKDB=off in .env: no compaction" in run.log
        assert "remove weekly_compact.sh from root's crontab" in run.log
        # The hold is named beside it: a web held back on `on` writes the file
        # this leaves uncompacted.
        assert "duckdb_switch.held_back" in run.log
        assert "WEEKLY COMPACT START" not in run.log

    def test_off_written_with_quotes_stands_down_too_and_says_so(self, run_with_env):
        """`docker run --env-file` keeps the quotes, so the sidecar would read
        a value the switch does not understand and run on — a compaction in
        the week the file must not change, while compose, which strips them,
        runs web off. Standing down is the harmless direction. Mutation: stand
        down on `KS_DUCKDB_ENV = off` (the docker reading) alone."""
        run, data = run_with_env('KS_DUCKDB="off"\n')
        assert run.code == 0, run.out
        self._untouched(run, data)
        assert "written with quotes" in run.log and "write KS_DUCKDB=off" in run.log

    @pytest.mark.parametrize("env_text", [None, "", "KS_DUCKDB=on\n", "KS_DUCKDB=\n",
                                          "KS_DUCKDB=off\nKS_DUCKDB=on\n",
                                          "# KS_DUCKDB=off\n"],
                             ids=["no-env", "empty", "on", "blank", "on-last", "comment"])
    def test_on_compacts_as_it_always_did(self, run_with_env, env_text):
        """Nothing changes under `on`: the same calls in the same order as the
        healthy run without a `.env`, which every other test here drives."""
        run, _ = run_with_env(env_text)
        assert run.code == 0, run.out
        assert run.calls[:2] == ["rm -f -v duckdb-compact",
                                 "compose stop --timeout 30 web bot meilisearch"], run.calls
        assert any(c.startswith("run -d --name duckdb-compact") for c in run.calls)
        # And the recorder sees deletions, so its silence under `off` is one.
        assert any(c.endswith("analytics.duckdb.old") for c in run.rm), run.rm
        assert run.notify.startswith("✅ Weekly compact:")
        assert "KS_DUCKDB" not in run.log

    def test_a_value_the_switch_does_not_understand_runs_on_and_says_so(self, run_with_env):
        """Web and the sidecar both run `on` over a typo (OD-09), so the
        compaction does too — with one line, because whoever typed `of`
        believes the week has started."""
        run, _ = run_with_env("KS_DUCKDB=of\n")
        assert run.code == 0, run.out
        assert run.notify.startswith("✅ Weekly compact:")
        assert "KS_DUCKDB=of in .env is not a value the switch understands" in run.log

    def test_without_the_reader_it_stops_before_anything(self, run_with_env):
        """A checkout without deploy/duckdb_env.sh cannot say whether `.env`
        is off, so it neither stops nor deletes on the chance that it is."""
        run, data = run_with_env("KS_DUCKDB=off\n", env_lib=False)
        assert run.code == 1, run.out
        self._untouched(run, data)
        assert run.notify.startswith("🚨 Compact aborted: deploy/duckdb_env.sh is missing")
