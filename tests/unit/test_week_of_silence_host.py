"""The host's half of the week of silence (stage 5, PR-10), actually run.

Three things the week changes on the host, and each is a script run from
root's crontab with nobody watching it:

* `deploy/duckdb_env.sh` — the one reader of `KS_DUCKDB` in `.env` for the
  host-cron scripts. Every stand-down below rests on it, so its readings are
  pinned here form by form, against what compose (web's environment) and
  `docker run --env-file` (a sidecar's) were measured to make of each: a
  script stands down when either reads off, so a quoted `off`, one with an
  inline comment, an indented one, `export KS_DUCKDB=off` and
  `KS_DUCKDB = off` all stand it down.
* `deploy/daily_offsite.sh` — under `off` its sidecar is refused (a "Daily
  snapshot FAILED" page the first night) and web registers no db_backup, so
  every later night refuses a backup past 30 h. It now stands down before its
  cleanup trap exists: nothing read, linked, started, shipped or deleted.
* `deploy/duckdb_silence_check.sh --crontab` — the week's crontab change,
  printed and never installed. The lines it prints are the ones each
  script's own header documents, compared here so the two cannot drift.

The scripts run in a scratch repository root against fakes on PATH that
record every call — `docker`, `chown`, and the real `rm` and `ln` behind a
recorder — which is how "nothing was deleted" is a fact about a run and not
a reading of the code. The Sunday compaction's stand-down is proved the same
way in `test_weekly_compact_wrapper.py`, and offsite_check's frozen branch
and the .env that now rides with every Postgres shipment in
`test_pg_offsite_scripts.py`.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DEPLOY = REPO / "deploy"

pytestmark = pytest.mark.skipif(
    not (shutil.which("bash") and shutil.which("sed")), reason="needs bash and sed")


def _recorder(name: str, real: "str | None") -> str:
    """A fake that records its arguments and then runs the real command —
    or, without one, succeeds doing nothing."""
    tail = f'exec {real} "$@"' if real else "exit 0"
    return (f"#!/usr/bin/env bash\n"
            f"printf '%s %s\\n' {name} \"$*\" >> \"$FAKE_CALLS\"\n{tail}\n")


# `docker run --entrypoint id … -u` asks the image who it runs as; every other
# docker call succeeds and does nothing — except the snapshot sidecar under
# FAKE_SIDECAR_REFUSES, which exits 1 the way the switch inside it refuses a
# sidecar started from a `.env` that says off.
FAKE_DOCKER = r"""#!/usr/bin/env bash
printf 'docker %s\n' "$*" >> "$FAKE_CALLS"
case " $* " in
    *" --entrypoint id "*) echo 999 ;;
    *" python /app/deploy/snapshot_export.py"*) [ -n "${FAKE_SIDECAR_REFUSES:-}" ] && exit 1 ;;
esac
exit 0
"""

# GNU `stat -c` on a laptop whose stat is BSD's: the script runs on Linux and
# says `stat -c %Y`; this lets the on-path be driven here as well as in CI.
FAKE_STAT = f"""#!/usr/bin/env bash
real={shutil.which("stat")}
if "$real" -c %Y / >/dev/null 2>&1; then exec "$real" "$@"; fi
if [ "$1" = "-c" ]; then
    fmt="$2"; shift 2
    case "$fmt" in %Y) fmt=%m ;; %s) fmt=%z ;; esac
    exec "$real" -f "$fmt" "$@"
fi
exec "$real" "$@"
"""

# The Parquet push it ends with: recorded, never run — its own tests are its own.
FAKE_PUSH = """#!/usr/bin/env bash
printf 'offsite_parquet.sh %s\\n' "${BACKUP_EXPORT_DIR:-}" >> "$FAKE_CALLS"
exit 0
"""


class Box:
    """A scratch repository root holding daily_offsite.sh and what it sources."""

    def __init__(self, root: Path, bin_dir: Path):
        self.root = root
        self.bin = bin_dir
        (root / "deploy").mkdir(parents=True)
        for name in ("daily_offsite.sh", "duckdb_env.sh", "notify.sh"):
            shutil.copy(DEPLOY / name, root / "deploy" / name)
        push = root / "deploy" / "offsite_parquet.sh"
        push.write_text(FAKE_PUSH)
        push.chmod(0o755)
        self.backups = root / "data" / "backups"
        self.backups.mkdir(parents=True)
        self.calls = root / "calls.log"

    def backup(self, age_h: float = 1) -> Path:
        path = self.backups / "analytics-20261014-013000.duckdb"
        path.write_bytes(b"DUCK" * 64)
        when = time.time() - age_h * 3600
        os.utime(path, (when, when))
        return path

    def run(self, env_text: str, **extra: str):
        # The kill switch in every .env, so notify() returns before it can
        # reach Telegram or the alert archive, and says so on stderr.
        (self.root / ".env").write_text("KS_ALERTS_DISABLED=1\n" + env_text)
        self.calls.write_text("")
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("FAKE_", "BACKUP_", "KS_"))}
        env.update({"PATH": f"{self.bin}{os.pathsep}{os.environ.get('PATH', '')}",
                    "FAKE_CALLS": str(self.calls), **extra})
        done = subprocess.run(["bash", str(self.root / "deploy" / "daily_offsite.sh")],
                              env=env, capture_output=True, text=True, timeout=60)
        return done.returncode, done.stdout + done.stderr, self.calls.read_text().splitlines()


@pytest.fixture(scope="module")
def fake_bin(tmp_path_factory):
    bin_dir = tmp_path_factory.mktemp("bin")
    for name, body in (("docker", FAKE_DOCKER),
                       ("chown", _recorder("chown", None)),
                       ("rm", _recorder("rm", shutil.which("rm"))),
                       ("ln", _recorder("ln", shutil.which("ln"))),
                       ("stat", FAKE_STAT)):
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    return bin_dir


@pytest.fixture
def box(tmp_path, fake_bin):
    return Box(tmp_path / "repo", fake_bin)


# ── the reader ────────────────────────────────────────────────────────────────

# Every way of writing the line this module knows, and what each of the two
# parsers that read `.env` makes of it, then read by the switch's own rule
# (`strip().lower()`; empty is `on`, anything but on/off is `invalid`):
#
#   docker — `docker run --env-file .env`, which starts the compaction's and
#            the nightly snapshot's sidecars (and the restore drill's).
#            `refused`: it rejects the whole file ("poorly formatted
#            environment") and starts nothing.
#   web    — compose's `env_file:`, which is web's environment, so what web
#            runs.
#
# MEASURED, not reasoned: each row was written to an env file and read by
# docker compose v2.23.3 (`config --format json`, the service's
# `environment.KS_DUCKDB`) and by engine 24.0.7 (`docker run --rm --env-file`
# printing `os.environ.get("KS_DUCKDB")` from python:3.14-slim), 2026-10-15.
# Re-measure before editing a row: the review that added half of them found
# the reader's guesses wrong for four forms compose reads as off.
#
# deploy/stage4_soak.sh's P4 reads the file through the same reader; its
# test (test_stage4_soak_script.TestStage5.test_env_reads_as_the_host_scripts_
# read_it) feeds the soak rows of this table and holds it to this reading.
ENV_FORMS = [
    # (id, text, docker, web)
    ("off", "KS_DUCKDB=off\n", "off", "off"),
    ("upper-crlf", "KS_DUCKDB=OFF \r\n", "off", "off"),
    ("double-quoted", 'KS_DUCKDB="off"\n', "invalid", "off"),
    ("single-quoted", "KS_DUCKDB='Off'\n", "invalid", "off"),
    ("on", "KS_DUCKDB=on\n", "on", "on"),
    ("blank", "KS_DUCKDB=\n", "on", "on"),
    ("absent", "ADMIN_USER_IDS=1,2\n", "on", "on"),
    ("typo", "KS_DUCKDB=of\n", "invalid", "invalid"),
    ("on-last", "KS_DUCKDB=off\nKS_DUCKDB=on\n", "on", "on"),
    ("off-last", "KS_DUCKDB=on\nKS_DUCKDB=off\n", "off", "off"),
    ("comment", "# KS_DUCKDB=off\n", "on", "on"),
    ("indented-comment", "  # KS_DUCKDB=off\n", "on", "on"),
    ("other-key", "KS_DUCKDB_X=off\n", "on", "on"),
    ("other-key-2", "KS_DUCKDBX=off\n", "on", "on"),
    ("prefixed-key", "XKS_DUCKDB=off\n", "on", "on"),
    ("inline-comment", "KS_DUCKDB=off # week of silence\n", "invalid", "off"),
    ("inline-comment-crlf", "KS_DUCKDB=off # c\r\n", "invalid", "off"),
    ("inline-comment-padded", "KS_DUCKDB=  OFF  # c\n", "invalid", "off"),
    ("inline-comment-on", "KS_DUCKDB=on # c\n", "invalid", "on"),
    ("hash-no-space", "KS_DUCKDB=off#x\n", "invalid", "invalid"),
    ("tab-hash", "KS_DUCKDB=off\t# tab\n", "invalid", "invalid"),
    ("quoted-then-comment", 'KS_DUCKDB="off" # c\n', "invalid", "off"),
    ("single-quoted-then-comment", "KS_DUCKDB='off' # c\n", "invalid", "off"),
    ("comment-inside-quotes", 'KS_DUCKDB="off # c"\n', "invalid", "invalid"),
    ("quoted-padded", 'KS_DUCKDB=" off "\n', "invalid", "off"),
    ("quoted-empty", 'KS_DUCKDB=""\n', "invalid", "on"),
    ("quoted-on", 'KS_DUCKDB="on"\n', "invalid", "on"),
    ("leading-spaces", "  KS_DUCKDB=off\n", "off", "off"),
    ("leading-tab", "\tKS_DUCKDB=off\n", "off", "off"),
    ("space-after-equals", "KS_DUCKDB= off\n", "off", "off"),
    ("trailing-spaces", "KS_DUCKDB=off   \n", "off", "off"),
    ("indented-last", "KS_DUCKDB=on\n  KS_DUCKDB=off # c\n", "invalid", "off"),
    ("export", "export KS_DUCKDB=off\n", "refused", "off"),
    ("export-crlf", "export KS_DUCKDB=off\r\n", "refused", "off"),
    ("export-two-spaces", "export  KS_DUCKDB=off\n", "refused", "off"),
    ("export-indented", "  export KS_DUCKDB=off\n", "refused", "off"),
    ("export-quoted", 'export KS_DUCKDB="off"\n', "refused", "off"),
    ("export-typo", "export KS_DUCKDB=of\n", "refused", "invalid"),
    ("spaces-around-equals", "KS_DUCKDB = off\n", "refused", "off"),
    ("space-before-equals", "KS_DUCKDB =off\n", "refused", "off"),
    ("colon-space", "KS_DUCKDB: off\n", "refused", "off"),
    ("colon", "KS_DUCKDB:off\n", "on", "off"),
    ("export-first", "export KS_DUCKDB=on\nKS_DUCKDB=off\n", "refused", "off"),
    ("export-last-on", "KS_DUCKDB=off\nexport KS_DUCKDB=on\n", "refused", "on"),
    ("colon-last-off", "KS_DUCKDB=on\nKS_DUCKDB:off\n", "on", "off"),
    ("colon-last-on", "KS_DUCKDB=off\nKS_DUCKDB:on\n", "off", "on"),
    ("commented-last", "KS_DUCKDB=off\n# KS_DUCKDB=on\n", "off", "off"),
    ("blank-last", "KS_DUCKDB=off\nKS_DUCKDB=\n", "on", "on"),
]

READ = r"""
set -Eeuo pipefail
source "$1"
ks_duckdb_env_read "$2"
printf '%s|%s|%s|%s|%s\n' "$KS_DUCKDB_ENV" "$KS_DUCKDB_ENV_WEB" \
    "$KS_DUCKDB_ENV_MEANT_OFF" "$KS_DUCKDB_ENV_READ" "$KS_DUCKDB_ENV_VALUE"
if ks_duckdb_stands_down; then echo DOWN; else echo RUNS; fi
ks_duckdb_env_note
"""


def _read(tmp_path: Path, text: "str | None"):
    env_file = tmp_path / "env"
    if text is not None:
        env_file.write_bytes(text.encode())
    done = subprocess.run(["bash", "-c", READ, "_", str(DEPLOY / "duckdb_env.sh"),
                           str(env_file)], capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stderr
    fields, verdict, note = done.stdout.split("\n", 2)
    return fields.split("|"), verdict, note


class TestTheReader:
    @pytest.mark.parametrize("text, docker, web", [row[1:] for row in ENV_FORMS],
                             ids=[row[0] for row in ENV_FORMS])
    def test_it_reads_the_file_the_way_docker_and_compose_do(self, tmp_path, text,
                                                             docker, web):
        """`KS_DUCKDB_ENV` is what `docker run --env-file` hands a sidecar,
        `KS_DUCKDB_ENV_WEB` what compose hands web, and a host script stands
        down when either says off: web runs off, or the sidecar would be
        refused after the script had stopped and deleted what it stops and
        deletes. The review of PR-10 drove the old reader — `^KS_DUCKDB=`,
        the last line, trim, unquote — through four forms compose reads as
        off (an inline comment, leading whitespace, `export`, spaces around
        `=`), and on each it ran: the compaction stopped web, bot and
        Meilisearch and deleted the last compaction's backup during the week.
        Mutation: drop the comment cut, the `export` strip or the leading
        trim from the web reading, or take the first line."""
        (env, web_reads, meant, read, _), verdict, _ = _read(tmp_path, text)
        assert (env, web_reads, read) == (docker, web, "1")
        down = "off" in (docker, web)
        assert (meant, verdict) == (("1", "DOWN") if down else ("0", "RUNS"))

    def test_an_unreadable_file_is_today(self, tmp_path):
        """No `.env` to read is not a claim that the week has started: the
        script does what it did before stage 5, and fails where it always
        failed."""
        (env, web, meant, read, _), verdict, _ = _read(tmp_path, None)
        assert (env, web, meant, read, verdict) == ("on", "on", "0", "0", "RUNS")

    def test_the_note_names_the_hold_and_what_the_line_costs(self, tmp_path):
        """A host script reads `.env`, never web's verdict, so its stand-down
        says what a web held back on `on` means (HELD_BACK_NOTE's sentence),
        and a line docker reads otherwise than compose what that costs —
        each with the one line to write instead."""
        _, _, note = _read(tmp_path, "KS_DUCKDB=off\n")
        assert "duckdb_switch.held_back" in note and "KS_DUCKDB=on" in note
        assert "quotes" not in note and "refuses" not in note
        _, _, note = _read(tmp_path, "  KS_DUCKDB=off\n")
        assert "quotes" not in note and "refuses" not in note
        for text in ('KS_DUCKDB="off"\n', "KS_DUCKDB=off # week of silence\n"):
            _, _, note = _read(tmp_path, text)
            assert "written with quotes or a comment" in note, note
            assert "a sidecar would" in note and "run on" in note, note
            assert "write KS_DUCKDB=off as a line of its own" in note, note
        for text in ("export KS_DUCKDB=off\n", "KS_DUCKDB = off\n"):
            _, _, note = _read(tmp_path, text)
            assert "docker run --env-file refuses" in note, note
            assert "the restore drill" in note, note
            assert "write KS_DUCKDB=off as a line of its own" in note, note
        _, _, note = _read(tmp_path, "KS_DUCKDB:off\n")
        assert "docker run --env-file skips" in note and "run on" in note, note
        _, _, note = _read(tmp_path, "KS_DUCKDB=off\nKS_DUCKDB:on\n")
        assert "web runs on" in note and "would be refused" in note, note

    def test_a_value_is_quoted_back_bounded_and_printable(self, tmp_path):
        """The value is quoted in log lines; `.env` is not a file to echo.
        Mutation: drop the `cut -c1-40`."""
        (_, _, _, _, value), _, _ = _read(tmp_path, "KS_DUCKDB=" + "x" * 300 + "\x1b[31m\n")
        assert len(value) <= 40 and "\x1b" not in value

    def test_it_reads_one_key_and_prints_nothing_else(self, tmp_path):
        """By exact name, never a listing: the other lines of `.env` are the
        host's secrets. Mutation: `grep KS_` for the anchored name."""
        secret = "BOT_TOKEN=123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawQ\n"
        for other in ("KS_DUCKDB_FAKE=off\n", "export BOT_TOKEN=x\n"):
            fields, verdict, note = _read(tmp_path, secret + other)
            assert "AAHdq" not in "|".join(fields) + verdict + note
        text = (DEPLOY / "duckdb_env.sh").read_text()
        code = [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]
        greps = [ln.strip() for ln in code if "grep" in ln]
        assert greps == [
            """lines="$(grep -E '^[[:space:]]*(export[[:space:]]+)?KS_DUCKDB[[:space:]]*[=:]' \\"""
        ], greps
        assert not any(re.search(r"\b(printenv|env|export|set)\b\s*($|\|)", ln) for ln in code)


# ── the nightly snapshot ──────────────────────────────────────────────────────

class TestTheNightlySnapshotStandsDown:
    def test_off_reads_links_starts_ships_and_deletes_nothing(self, box):
        """Every command the snapshot would run is recorded, and under `off`
        there is none — not even its cleanup trap's `rm -rf`, which is why the
        stand-down sits above it. Mutation: move the stand-down below `trap
        cleanup EXIT`, or below the `ln`."""
        box.backup()
        leftover = box.root / "data" / ".snapshot_tmp" / "leftover"
        leftover.parent.mkdir(parents=True)
        leftover.write_text("a run's scratch, not this one's to delete")
        code, out, calls = box.run("KS_DUCKDB=off\n")
        assert code == 0, out
        assert calls == [], calls
        assert leftover.exists()
        assert "KS_DUCKDB=off in .env: no DuckDB snapshot tonight" in out
        assert "remove daily_offsite.sh from root's crontab" in out
        assert "deploy/pg_offsite.sh" in out
        assert "duckdb_switch.held_back" in out

    def test_off_pages_nobody(self, box):
        """The first night under `off` its sidecar was refused and the ERR
        trap paged "Daily snapshot FAILED"; every night after, the newest
        backup was past 30 h (web registers no db_backup under off). Now a
        fresh backup — the first night's shape — is not even looked at.
        Mutation: drop the stand-down — the sidecar is refused, the ERR trap
        notifies, and the run exits 1."""
        box.backup()
        code, out, _ = box.run("KS_DUCKDB=off\n", FAKE_SIDECAR_REFUSES="1")
        assert code == 0, out
        assert "notify suppressed" not in out
        assert "Daily snapshot FAILED" not in out

    def test_off_with_quotes_stands_down_and_says_why(self, box):
        box.backup()
        code, out, calls = box.run("KS_DUCKDB=\"off\"\n")
        assert code == 0, out
        assert calls == [] and "written with quotes" in out

    @pytest.mark.parametrize("env_text", [
        "KS_DUCKDB=off  # week of silence\n", "  KS_DUCKDB=off\n",
        "export KS_DUCKDB=off\n", "KS_DUCKDB = off\n",
    ], ids=["inline-comment", "leading-space", "export", "spaces-around-equals"])
    def test_every_off_compose_reads_stands_it_down(self, box, env_text):
        """Each of these is off to compose, so web runs off. The review of
        PR-10 ran the snapshot with the leading-space line and the sidecar
        refusing, and it paged "Daily snapshot FAILED"; the comment line
        would have shipped the file the week keeps still, its sidecar handed
        `off # …` and running on. Mutation: read the line the way the first
        reader did, `^KS_DUCKDB=` and nothing else."""
        box.backup()
        code, out, calls = box.run(env_text, FAKE_SIDECAR_REFUSES="1")
        assert code == 0, out
        assert calls == [], calls
        assert "KS_DUCKDB=off in .env: no DuckDB snapshot tonight" in out
        assert "Daily snapshot FAILED" not in out and "notify suppressed" not in out

    @pytest.mark.parametrize("env_text", ["", "KS_DUCKDB=on\n", "KS_DUCKDB=off\nKS_DUCKDB=on\n"],
                             ids=["unset", "on", "on-last"])
    def test_on_snapshots_and_ships_as_it_always_did(self, box, env_text):
        """Nothing changes under `on`: the backup is linked, the image asked
        who it runs as, the sidecar started with `.env`, the push called, and
        the scratch directory removed. Mutation: stand down on anything but
        a line that says off."""
        backup = box.backup()
        code, out, calls = box.run(env_text)
        assert code == 0, out
        kinds = [c.split()[0] for c in calls]
        assert kinds[0] == "rm" and "ln" in kinds and "offsite_parquet.sh" in kinds, calls
        assert any(c.startswith("docker run --rm --memory=6500m") and "--env-file .env" in c
                   for c in calls), calls
        assert calls[-1].startswith("rm -rf data/.snapshot_tmp"), calls
        assert "KS_DUCKDB" not in out
        assert backup.exists()

    def test_a_typo_runs_on_with_a_line(self, box):
        box.backup()
        code, out, calls = box.run("KS_DUCKDB=of\n")
        assert code == 0, out
        assert any(c.startswith("offsite_parquet.sh") for c in calls), calls
        assert "KS_DUCKDB=of in .env is not a value the switch understands" in out

    def test_on_still_refuses_a_stale_backup(self, box):
        """The on-path's own guard, unchanged: a backup older than a day means
        db_backup stopped. It is also what every night of a week under `off`
        would have ended on, which is why `off` must never reach it."""
        box.backup(age_h=50)
        code, out, calls = box.run("KS_DUCKDB=on\n")
        assert code == 1, out
        assert "db_backup has stopped" in out
        assert not any(c.startswith("docker") for c in calls), calls


# ── the runbook ───────────────────────────────────────────────────────────────

def _runbook() -> str:
    text = (REPO / ".claude" / "CLAUDE.md").read_text(encoding="utf-8")
    start = text.index("### Starting the week of silence")
    return text[start:text.index("\n### ", start + 1)]


class TestTheRunbook:
    """CLAUDE.md, "Starting the week of silence": the one runbook, read by a
    person on the host. What it names has to be there to run."""

    def test_every_tool_it_names_exists(self):
        """Mutation: rename a script without the runbook."""
        named = set(re.findall(r"(?:deploy|scripts)/[\w./-]+\.(?:sh|py)", _runbook()))
        assert {"deploy/duckdb_silence_check.sh", "deploy/ark_ship.sh",
                "deploy/pg_offsite.sh", "deploy/offsite_check.sh",
                "scripts/weekly_compact.sh", "deploy/daily_offsite.sh",
                "deploy/duckdb_env.sh", "deploy/stage4_soak.sh",
                "deploy/duckdb_table_fates_check.py"} <= named, named
        for path in named:
            assert (REPO / path).is_file(), path

    def test_it_changes_the_crontab_from_the_print_only(self):
        """The runbook never spells a crontab line of its own: the lines are
        the ones `--crontab` prints, which a test holds to the scripts'
        headers. Mutation: write `0 2 * * 0` into the runbook."""
        text = _runbook()
        assert "--crontab" in text
        assert not re.search(r"[0-9*]+ [0-9*]+ \* \* [0-9*]", text)

    def test_the_env_copy_is_the_documented_one(self):
        """`.env` is copied before it is edited, mode 600, to the directory
        CLAUDE.md names for every copy of it — never the repository root."""
        text = _runbook()
        assert "install -m 600 /opt/key-api-bot/.env\n   /root/env-backups/key-api-bot/" in text
        assert "never by printing the file" in text

    @staticmethod
    def _item(heading: str, number: int) -> str:
        """Item `number` of the numbered list under the bold `heading`, its
        lines joined, so a phrase the text wraps is still one phrase."""
        text = _runbook()
        block = text[text.index(f"**{heading}**"):]
        start = block.index(f"\n{number}. ") + 1
        end = re.search(r"\n(\d+\. |\*\*|\n)", block[start:])
        item = block[start:start + end.start()] if end else block[start:]
        return " ".join(item.split())

    def test_precondition_four_asks_what_the_verdict_asks(self):
        """It is read so the start is not the first time anybody asks
        core/duckdb_off.py's questions, so it asks those and no others. The
        verdict wants every chain writing Postgres, by latch or by flag, and
        a latch — marker and owner rows — only of the chains that keep a value
        in DuckDB's `sync_metadata`: a flagged chain with nothing written
        latches on its first write, mid-week, and counting it once held a
        start back. The first runbook asked for a marker per chain, which six
        chains can rightly lack. Mutation: name another chain as needing a
        latch, or go back to "a marker per chain"."""
        from core.duckdb_off import chains_keeping_sync_metadata
        from core.write_chains import WRITE_CHAINS, chain_name

        item = self._item("Preconditions", 4)
        assert "a marker per chain" not in item
        assert "`mode` `postgres`" in item and "by its latch or by its flag" in item
        latched = set(re.findall(r"`(pg_\w+_write)`", item))
        assert latched == {chain_name(c) for c in chains_keeping_sync_metadata()}, latched
        assert latched < {chain_name(c) for c in WRITE_CHAINS}

    def test_web_waits_for_the_freeze_not_the_upload(self):
        """deploy/ark_ship.sh prints SOURCE RELEASED once nothing after it
        reads the file — test_ark_ship_script holds that line before anything
        leaves the host — and under off web never opens the file, so steps
        4-6 may begin there. The first runbook kept web and the bot stopped
        until ARK SHIPPED AND VERIFIED: the dashboard down and orders waiting
        through the upload, the download, the decrypt and L0-L2 of the
        downloaded copy, the longest outage of the procedure, for nothing —
        while the script said the opposite. The crontab print and the
        snapshot's header said step 4 waited for the Ark off-site. Mutation:
        put "until `ARK SHIPPED AND VERIFIED`" back into step 3."""
        step3 = self._item("Starting the week", 3)
        assert "SOURCE RELEASED" in step3 and "ARK SHIPPED AND VERIFIED" in step3
        assert "until `ARK SHIPPED AND VERIFIED`" not in step3
        assert "steps 4-6" in step3.replace("–", "-")
        for n in (4, 5, 6):
            assert "ARK SHIPPED" not in self._item("Starting the week", n), n
        header = " ".join(re.sub(r"^\s*#[\s#]*", "", ln)
                          for ln in (DEPLOY / "ark_ship.sh").read_text().splitlines())
        assert "the week's later steps need not wait for the upload" in header
        printed = subprocess.run(["bash", str(DEPLOY / "duckdb_silence_check.sh"), "--crontab"],
                                 capture_output=True, text=True, timeout=30)
        assert printed.returncode == 0, printed.stderr
        assert "once the second Ark is off-site" not in printed.stdout
        assert "SOURCE RELEASED" in printed.stdout
        snapshot = " ".join((DEPLOY / "daily_offsite.sh").read_text().split())
        assert "once the second Ark is off-site" not in snapshot


# ── the restore drill ─────────────────────────────────────────────────────────

FAKE_SFTP = r"""#!/usr/bin/env bash
printf 'sftp %s\n' "$*" >> "$FAKE_CALLS"
batch="$(cat)"
case "$batch" in
    *"ls -1"*) printf '%s\n' "${FAKE_ARCHIVE##*/}" ;;
    *"get "*) set -- $(printf '%s\n' "$batch" | grep '^get '); cp "$FAKE_ARCHIVE" "$3" ;;
esac
exit 0
"""

FAKE_RECORDING_DOCKER = """#!/usr/bin/env bash
printf 'docker %s\\n' "$*" >> "$FAKE_CALLS"
exit 0
"""

# GNU `date -u -d "<when>" +%s` on a laptop whose date is BSD's: the drill
# runs on Linux, and its archive-age line is half of what is asserted here.
FAKE_GNU_DATE = f"""#!/usr/bin/env bash
if [ "$1" = "-u" ] && [ "$2" = "-d" ]; then
    exec {sys.executable} -c 'import calendar, datetime, sys
print(calendar.timegm(datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S").timetuple()))' "$3"
fi
exec {shutil.which("date")} "$@"
"""


class TestTheRestoreDrill:
    """deploy/restore-test.sh, the drill that proves the off-site Parquet
    archive rebuilds the warehouse, run in a scratch repository root: a fake
    `sftp` serves one archive, a fake `docker` records the rebuild it would
    start. Its sidecar is started with `--env-file .env`, which says off for
    the whole week of silence — and a restored `.env` says what the system
    ran as when it was taken — so under off the switch refused the drill
    before it imported a row, and the one command proving the archive
    rebuilds stopped working for the week (review of PR-10)."""

    @pytest.fixture
    def drill(self, tmp_path):
        root = tmp_path / "repo"
        (root / "deploy").mkdir(parents=True)
        for name in ("restore-test.sh", "duckdb_env.sh", "restore_from_export.py"):
            shutil.copy(DEPLOY / name, root / "deploy" / name)
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        for name, body in (("sftp", FAKE_SFTP), ("docker", FAKE_RECORDING_DOCKER),
                           ("date", FAKE_GNU_DATE)):
            (bin_dir / name).write_text(body)
            (bin_dir / name).chmod(0o755)
        calls = tmp_path / "calls.log"

        def run(env_text: str, age_h: int = 2):
            import datetime
            import tarfile

            taken = (datetime.datetime.now(datetime.timezone.utc)
                     - datetime.timedelta(hours=age_h)).strftime("%Y%m%d-%H%M%S")
            remote = tmp_path / "remote"
            remote.mkdir(exist_ok=True)
            archive = remote / f"ks-warehouse-{taken}.tar"
            src = tmp_path / "src" / "export_parquet"
            src.mkdir(parents=True, exist_ok=True)
            (src / "_manifest.json").write_text("{}")
            (src / "orders.parquet").write_bytes(b"PAR1")
            with tarfile.open(archive, "w") as tar:
                tar.add(src, arcname="export_parquet")
            (root / ".env").write_text("KS_ALERTS_DISABLED=1\n" + env_text)
            calls.write_text("")
            env = {k: v for k, v in os.environ.items()
                   if not k.startswith(("FAKE_", "BACKUP_", "KS_"))}
            env.update({"PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
                        "FAKE_CALLS": str(calls), "FAKE_ARCHIVE": str(archive),
                        "BACKUP_REMOTE": "u1@storage.invalid"})
            done = subprocess.run(["bash", str(root / "deploy" / "restore-test.sh")],
                                  env=env, capture_output=True, text=True, timeout=60)
            return done.returncode, done.stdout + done.stderr, calls.read_text().splitlines()

        return run

    @pytest.mark.parametrize("env_text", ["KS_DUCKDB=off\n", "", "KS_DUCKDB=on\n"],
                             ids=["off", "unset", "on"])
    def test_the_rebuild_runs_on_whatever_env_says(self, drill, env_text):
        """`-e KS_DUCKDB=on` beside `--env-file .env`: docker gives `-e` the
        last word (measured, engine 24.0.7), and the sidecar sees only its
        scratch directory, never the live file. Under on it changes nothing.
        Mutation: drop the `-e KS_DUCKDB=on`."""
        code, out, calls = drill(env_text)
        assert code == 0, out
        [rebuild] = [c for c in calls if c.startswith("docker run")]
        assert "--env-file .env" in rebuild and "-e KS_DUCKDB=on" in rebuild, rebuild
        assert "python /app/deploy/restore_from_export.py" in rebuild
        assert "/opt/key-api-bot/data" not in rebuild and ":/app/data " in rebuild
        assert "PASS — the off-site archive rebuilt a validated warehouse." in out

    def test_under_off_an_old_archive_is_frozen_not_stale(self, drill):
        """The week stops the nightly archive, so its newest copy ages from
        the start: the plan's "frozen since the week began", not a warning
        asking whether the compaction still runs. Mutation: judge the age
        under off as under on."""
        code, out, _ = drill("KS_DUCKDB=off\n", age_h=200)
        assert code == 0, out
        assert "WARNING: newest off-site archive" not in out
        assert "frozen since the week of silence began" in out, out
        code, out, _ = drill("KS_DUCKDB=on\n", age_h=200)
        assert "WARNING: newest off-site archive is over" in out, out

    def test_a_file_docker_refuses_stops_before_the_download(self, drill):
        """`export KS_DUCKDB=off` or a space before the `=`: docker refuses
        the whole file and no `-e` can start the rebuild from it. Say so
        before fetching an archive the drill cannot use."""
        code, out, calls = drill("export KS_DUCKDB=off\n")
        assert code == 1, out
        assert calls == [], calls
        assert "docker run --env-file refuses" in out and "KS_DUCKDB=off" in out

    def test_the_real_restore_carries_it_too(self):
        """The disaster restore starts the same rebuild with a `.env`
        decrypted from `env-<stamp>.gpg`, which during the week says off.
        Its command lives in restore_from_export.py's docstring, which the
        runbook for a real restore points at. Mutation: drop the `-e` there."""
        doc = (DEPLOY / "restore_from_export.py").read_text()
        command = re.search(r"docker run [^\n]*\\\n[^\n]*", doc).group(0)
        assert "--env-file .env" in command and "-e KS_DUCKDB=on" in command, command
        setup = (REPO / "docs" / "offsite_backup_setup.md").read_text(encoding="utf-8")
        real = setup[setup.index("## Restoring for real"):]
        assert "-e KS_DUCKDB=on" in real and "restore_from_export.py" in real
