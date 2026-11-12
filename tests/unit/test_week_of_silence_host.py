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
* `deploy/daily_offsite.sh` — the nightly snapshot, which stood down under
  `off` before its cleanup trap existed. Its cron line was retired on
  2026-10-16, and step 14's S14-18 deleted it with the Sunday compaction
  (`scripts/weekly_compact.sh`) and both their stand-down tests.
* `deploy/duckdb_silence_check.sh` — the hourly hash of the file, whose
  `--crontab` printed the week's crontab change and never installed it. The
  final stage (2026-11-12) took its line out of the crontab, deleted the file
  it hashed, and deleted the script with its tests.

What is left here: the reader, the history of the week in CLAUDE.md (its
runbook until step 14's S14-20), and the restore drill, which runs in a
scratch repository root against fakes on PATH that record every call.
offsite_check's judgement of the DuckDB copy went at the final stage; that
it is gone, and the .env that rides with every Postgres shipment, are in
`test_pg_offsite_scripts.py`.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.repo_tree import needs_checkout

REPO = Path(__file__).resolve().parents[2]
DEPLOY = REPO / "deploy"

pytestmark = pytest.mark.skipif(
    not (shutil.which("bash") and shutil.which("sed")), reason="needs bash and sed")


# `_recorder`, the snapshot's `FAKE_DOCKER`, `FAKE_STAT`, `FAKE_PUSH`, `Box`
# and its fixtures stood here until step 14's S14-18 deleted
# `deploy/daily_offsite.sh`, the script they ran.

# The image the archive tools run in since step 14 (S14-02).
PINNED = "halloweex/keycrm-web:rollback-step14"

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
# deploy/stage4_soak.sh's P4 read the file through the same reader until step
# 14's S14-02 retired it with the week it judged.
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
"""


def _read(tmp_path: Path, text: "str | None"):
    env_file = tmp_path / "env"
    if text is not None:
        env_file.write_bytes(text.encode())
    done = subprocess.run(["bash", "-c", READ, "_", str(DEPLOY / "duckdb_env.sh"),
                           str(env_file)], capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stderr
    fields, verdict, rest = (done.stdout.split("\n", 2) + [""])[:3]
    # Whatever else was printed: `ks_duckdb_env_note`'s sentence until S14-20
    # deleted it, nothing since.
    return fields.split("|"), verdict, rest


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

    # `test_the_note_names_the_hold_and_what_the_line_costs` held
    # `ks_duckdb_env_note`'s sentence, until step 14's S14-20 deleted the
    # function: its callers went in S14-18.

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

# `TestTheNightlySnapshotStandsDown` stood here until step 14's S14-18
# deleted `deploy/daily_offsite.sh`: under `off` it stood down before its
# cleanup trap, read, linked, started, shipped and deleted nothing, and paged
# nobody; under `on` it refused an image that was not the pinned one and a
# backup past 30 h.


# ── the history ───────────────────────────────────────────────────────────────

def _history() -> str:
    """CLAUDE.md's "The week of silence, and its way back": what is left of
    the runbook "Starting the week of silence", compressed into the history
    of the DuckDB exit by step 14's S14-20."""
    text = (REPO / ".claude" / "CLAUDE.md").read_text(encoding="utf-8")
    start = text.index("### The week of silence, and its way back")
    return text[start:text.index("\n### ", start + 1)]


@needs_checkout
class TestTheHistory:
    """The week's runbook was a person's procedure for the host, and it is
    history since S14-20. What it still tells a person on the host has to be
    true: the scripts it names exist or are said to be gone, the crontab is
    changed from the print and never from the text, and on a step-14 image
    there is no way back to make. The runbook's own steps — the start, the
    preconditions, the Ark's SOURCE RELEASED, the `.env` copy — went with it
    (their tests are in S14-20's list)."""

    # Named by the history: two deleted in step 14's S14-18 with their cron
    # lines long retired, and the file's hourly hash at the final stage with
    # its line; the history says so.
    DELETED = frozenset({"scripts/weekly_compact.sh", "deploy/daily_offsite.sh",
                         "deploy/duckdb_silence_check.sh"})

    def test_every_tool_it_names_exists_or_is_said_gone(self):
        """Mutation: rename a script without the history — or delete one
        without the history saying it is gone."""
        text = _history()
        named = set(re.findall(r"(?:deploy|scripts)/[\w./-]+\.(?:sh|py)", text))
        assert {"deploy/duckdb_silence_check.sh", "deploy/duckdb_env.sh",
                "deploy/offsite_check.sh", "deploy/restore-test.sh"} <= named, named
        assert self.DELETED <= named, named
        for path in named - self.DELETED:
            assert (REPO / path).is_file(), path
        for path in self.DELETED:
            assert not (REPO / path).exists(), path
        joined = " ".join(text.split())
        assert "deleted in step 14's S14-18" in joined, joined
        assert ("its line came out of the crontab, the file was deleted, and the "
                "script went from this checkout") in joined, joined
        assert "2026-11-12" in joined and "`/root/duckdb-silence`" in joined, joined

    def test_it_changes_the_crontab_from_the_print_only(self):
        """The history never spells a crontab line of its own: the lines were
        the ones `--crontab` printed, held to the scripts' headers until the
        final stage deleted the print with the silence check. A line spelled
        here would be one nobody holds. Mutation: write `0 2 * * 0` into the
        section."""
        text = _history()
        assert "--crontab" in text
        assert not re.search(r"[0-9*]+ [0-9*]+ \* \* [0-9*]", text)

    def test_the_way_back_on_a_step_14_image_is_the_image_rollback(self):
        """Since step 14's S14-00 web runs off whatever `.env` says, so
        `KS_DUCKDB=on` changes nothing in web and only re-arms the host
        scripts that read `.env`. The history says so, and names the rollback
        section that exists. Mutation: drop the paragraph, or rename the
        Deployment section without the history."""
        joined = " ".join(_history().split())
        assert "On a step-14 image there is no way back to make" in joined, joined
        assert "leave `.env` at `KS_DUCKDB=off`" in joined
        assert "do not re-add those three cron lines" in joined
        assert "a step-14 web runs `off` whatever `.env` says" in joined
        section = "Before any rollback to an image from before step 14"
        assert f'"{section}"' in joined
        whole = (REPO / ".claude" / "CLAUDE.md").read_text(encoding="utf-8")
        assert f"\n### {section}\n" in whole

    def test_the_way_back_names_the_snapshots_image(self):
        """On an image from before step 14, the nightly snapshot's cron line
        put back has no default image since S14-02: without the variable in
        deploy/backup.env, or without the rollback-step14 tag on the host,
        every night refuses (exit 2) and pages "Daily snapshot FAILED". The
        variable is the one `deploy/daily_offsite.sh` read (a literal since
        S14-18 deleted the script, read at 50cc367b). Mutation: drop the
        sentence."""
        joined = " ".join(_history().split())
        for words in ("`BACKUP_RESTORE_IMAGE`", "`deploy/backup.env`", "`rollback-step14`",
                      "Daily snapshot FAILED"):
            assert words in joined, (words, joined)

    def test_the_file_is_said_gone_and_off_is_what_a_rollback_runs(self):
        """The final stage deleted the file: the history says an image from
        before step 14 runs without it only under `off`, and that `on` would
        need a warehouse rebuilt from the Ark first. (The print it pointed a
        person at, `--crontab`, went with the silence check.) Mutation: drop
        either sentence."""
        joined = " ".join(_history().split())
        assert ("an image from before step 14 runs without the file only under "
                "it") in joined, joined
        assert "there is no file for `on` to open" in joined, joined
        assert "`deploy/restore_from_export.py`" in joined, joined


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
case " $* " in
    *" image inspect "*) [ -n "${FAKE_NO_IMAGE:-}" ] && exit 1 ;;
    *"import duckdb"*)
        if [ "${FAKE_DUCKDB:-1.5.5}" = absent ]; then
            echo "ModuleNotFoundError: No module named 'duckdb'" >&2
            exit 1
        fi
        echo "${FAKE_DUCKDB:-1.5.5}" ;;
esac
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
        for name in ("restore-test.sh", "duckdb_env.sh", "restore_from_export.py",
                     "archive_image.sh"):
            shutil.copy(DEPLOY / name, root / "deploy" / name)
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        for name, body in (("sftp", FAKE_SFTP), ("docker", FAKE_RECORDING_DOCKER),
                           ("date", FAKE_GNU_DATE)):
            (bin_dir / name).write_text(body)
            (bin_dir / name).chmod(0o755)
        calls = tmp_path / "calls.log"

        def run(env_text: str, age_h: int = 2, **extra: str):
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
                        "BACKUP_REMOTE": "u1@storage.invalid",
                        "BACKUP_RESTORE_IMAGE": PINNED, **extra})
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
        [rebuild] = [c for c in calls if c.startswith("docker run")
                     and "restore_from_export.py" in c]
        assert "--env-file .env" in rebuild and "-e KS_DUCKDB=on" in rebuild, rebuild
        assert "--pull never" in rebuild and f" {PINNED} " in rebuild, rebuild
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

    def test_the_image_is_asked_before_the_archive_is_fetched(self, drill):
        """Step 14 (S14-02): the pinned image, present, with DuckDB 1.5.5 —
        asked first, so a drill that could rebuild nothing fetches nothing.
        Mutation: probe after the download."""
        code, out, calls = drill("KS_DUCKDB=off\n")
        assert code == 0, out
        assert calls[0] == f"docker image inspect {PINNED}", calls
        assert "import duckdb" in calls[1] and "--network none" in calls[1], calls
        assert calls[2].startswith("sftp "), calls

    @pytest.mark.parametrize("image, extra, words", [
        ("", {}, "BACKUP_RESTORE_IMAGE is not set"),
        ("halloweex/keycrm-web:latest", {}, "moves with every deploy"),
        (PINNED, {"FAKE_NO_IMAGE": "1"}, "this does not pull"),
        (PINNED, {"FAKE_DUCKDB": "absent"}, "cannot import duckdb"),
        (PINNED, {"FAKE_DUCKDB": "1.4.3"}, "carries DuckDB 1.4.3, not 1.5.5"),
    ], ids=["unset", "latest", "no-image", "no-duckdb", "another-version"])
    def test_a_drill_without_the_pinned_image_refuses(self, drill, image, extra, words):
        """It used to default to :latest, which after step 14's S14-19 has no
        DuckDB to rebuild with. Exit 2 — refused, not failed — naming the tag
        to use, and nothing fetched. Mutation: put the default back."""
        code, out, calls = drill("KS_DUCKDB=off\n", BACKUP_RESTORE_IMAGE=image, **extra)
        assert code == 2, out
        assert words in out and "rollback-step14" in out, out
        assert not [c for c in calls if c.startswith("sftp")], calls
        assert not [c for c in calls if "restore_from_export.py" in c], calls

    @needs_checkout
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
