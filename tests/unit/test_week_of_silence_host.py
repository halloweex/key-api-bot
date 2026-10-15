"""The host's half of the week of silence (stage 5, PR-10), actually run.

Three things the week changes on the host, and each is a script run from
root's crontab with nobody watching it:

* `deploy/duckdb_env.sh` — the one reader of `KS_DUCKDB` in `.env` for the
  host-cron scripts. Every stand-down below rests on it, so its readings are
  pinned here line by line: the last line wins, quotes are kept the way
  `docker run --env-file` keeps them, and a quoted `off` still stands a
  script down.
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

READ = r"""
set -Eeuo pipefail
source "$1"
ks_duckdb_env_read "$2"
printf '%s|%s|%s|%s\n' "$KS_DUCKDB_ENV" "$KS_DUCKDB_ENV_MEANT_OFF" \
    "$KS_DUCKDB_ENV_READ" "$KS_DUCKDB_ENV_VALUE"
if ks_duckdb_stands_down; then echo DOWN; else echo RUNS; fi
ks_duckdb_env_note
"""


def _read(tmp_path: Path, text: "str | None"):
    env_file = tmp_path / "env"
    if text is not None:
        env_file.write_text(text)
    done = subprocess.run(["bash", "-c", READ, "_", str(DEPLOY / "duckdb_env.sh"),
                           str(env_file)], capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stderr
    fields, verdict, note = done.stdout.split("\n", 2)
    return fields.split("|"), verdict, note


class TestTheReader:
    @pytest.mark.parametrize("text, docker_reads, meant_off", [
        ("KS_DUCKDB=off\n", "off", "1"),
        ("KS_DUCKDB=OFF \r\n", "off", "1"),
        ('KS_DUCKDB="off"\n', "invalid", "1"),
        ("KS_DUCKDB='Off'\n", "invalid", "1"),
        ("KS_DUCKDB=on\n", "on", "0"),
        ("KS_DUCKDB=\n", "on", "0"),
        ("ADMIN_USER_IDS=1,2\n", "on", "0"),
        ("KS_DUCKDB=of\n", "invalid", "0"),
        ("KS_DUCKDB=off\nKS_DUCKDB=on\n", "on", "0"),
        ("KS_DUCKDB=on\nKS_DUCKDB=off\n", "off", "1"),
        ("# KS_DUCKDB=off\n", "on", "0"),
        ("KS_DUCKDB_X=off\n", "on", "0"),
    ], ids=["off", "upper-crlf", "double-quoted", "single-quoted", "on", "blank",
            "absent", "typo", "on-last", "off-last", "comment", "other-key"])
    def test_it_reads_the_file_the_way_the_sidecars_do(self, tmp_path, text,
                                                       docker_reads, meant_off):
        """`KS_DUCKDB_ENV` is deploy/stage4_soak.sh's reading — the last line,
        quotes kept, as `docker run --env-file` hands the value to a sidecar
        — and `KS_DUCKDB_ENV_MEANT_OFF` is what its writer meant, which is what
        a host script stands down on. Mutation: `head -n 1` for `tail -n 1`,
        or strip the quotes before the docker reading."""
        (env, meant, read, _), verdict, _ = _read(tmp_path, text)
        assert (env, meant, read) == (docker_reads, meant_off, "1")
        assert verdict == ("DOWN" if meant_off == "1" else "RUNS")

    def test_an_unreadable_file_is_today(self, tmp_path):
        """No `.env` to read is not a claim that the week has started: the
        script does what it did before stage 5, and fails where it always
        failed."""
        (env, meant, read, _), verdict, _ = _read(tmp_path, None)
        assert (env, meant, read, verdict) == ("on", "0", "0", "RUNS")

    def test_the_note_names_the_hold_and_the_quotes(self, tmp_path):
        """A host script reads `.env`, never web's verdict, so its stand-down
        says what a web held back on `on` means (HELD_BACK_NOTE's sentence),
        and a quoted value what the quotes cost."""
        _, _, note = _read(tmp_path, "KS_DUCKDB=off\n")
        assert "duckdb_switch.held_back" in note and "KS_DUCKDB=on" in note
        assert "quotes" not in note
        _, _, note = _read(tmp_path, 'KS_DUCKDB="off"\n')
        assert "written with quotes" in note and "write KS_DUCKDB=off" in note

    def test_a_value_is_quoted_back_bounded_and_printable(self, tmp_path):
        """The value is quoted in log lines; `.env` is not a file to echo.
        Mutation: drop the `cut -c1-40`."""
        (_, _, _, value), _, _ = _read(tmp_path, "KS_DUCKDB=" + "x" * 300 + "\x1b[31m\n")
        assert len(value) <= 40 and "\x1b" not in value

    def test_it_reads_one_key_and_prints_nothing_else(self, tmp_path):
        """By exact name, never a listing: the other lines of `.env` are the
        host's secrets. Mutation: `grep KS_` for `grep '^KS_DUCKDB='`."""
        secret = "BOT_TOKEN=123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawQ\n"
        fields, verdict, note = _read(tmp_path, secret + "KS_DUCKDB_FAKE=off\n")
        assert "AAHdq" not in "|".join(fields) + verdict + note
        text = (DEPLOY / "duckdb_env.sh").read_text()
        code = [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]
        greps = [ln for ln in code if "grep" in ln]
        assert greps == ["""    line="$(grep -E '^KS_DUCKDB=' "$file" 2>/dev/null | tail -n 1 || true)\""""], greps
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
