"""deploy/gpg_safe.sh, run: a home of its own per call, and a second ask only
when gpg found no agent.

On 2026-10-10 the Postgres off-site copy stopped at `encrypt` with
"problem with the agent: No agent running": a root SSH login had started
systemd's user manager, which holds the sockets of root's default gpg home,
eleven seconds before cron's gpg went looking for an agent. These tests run the
wrapper against a fake gpg and gpgconf on PATH, which record every call, and
under the same `set -Eeuo pipefail` and ERR trap the backup scripts use: the
wrapper must not set off the caller's trap on a call it recovered, and must
hand back gpg's own failure when it did not.

`test_real_gpg_round_trips` proves the wrapper with a real gpg where one is
installed (CI's ubuntu runner ships GnuPG).
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LIB = REPO / "deploy" / "gpg_safe.sh"

FAKE_GPG = r"""#!/usr/bin/env bash
printf '%s|%s\n' "$GNUPGHOME" "$*" >> "$FAKE_CALLS"
n=$(grep -c . "$FAKE_CALLS")
case "${FAKE_MODE:-ok}" in
    ok) ;;
    no-agent-once) [ "$n" -eq 1 ] && { echo "gpg: problem with the agent: No agent running" >&2; exit 2; } ;;
    no-agent) echo "gpg: error running '/usr/bin/gpg-agent': exit status 2" >&2; exit 2 ;;
    bad-passphrase) echo "gpg: decryption failed: Bad session key" >&2; exit 2 ;;
esac
echo "gpg: attempt $n done" >&2
exit 0
"""

FAKE_GPGCONF = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_GPGCONF_CALLS"
exit 0
"""


def _run(tmp_path, mode, *, caller_home="/should/not/be/used"):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for name, body in (("gpg", FAKE_GPG), ("gpgconf", FAKE_GPGCONF)):
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    calls, conf = tmp_path / "calls", tmp_path / "gpgconf"
    calls.write_text("")
    conf.write_text("")
    # The caller's shape: errexit, errtrace and an ERR trap that names itself.
    script = f"""
set -Eeuo pipefail
trap 'echo "ERR trap fired rc=$?"' ERR
source {LIB}
rc=0
gpg_safe --batch --yes --passphrase-file /dev/null -o out in || rc=$?
echo "gpg_safe rc=$rc"
gpg_safe --batch -d in
echo "reached the end"
"""
    env = {**os.environ,
           "PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
           "FAKE_CALLS": str(calls), "FAKE_GPGCONF_CALLS": str(conf),
           "FAKE_MODE": mode, "GPG_SAFE_DELAYS": "0 0",
           "GNUPGHOME": caller_home}
    before = set(Path("/tmp").glob("ks-gnupg.*"))
    done = subprocess.run(["bash", "-c", script], env=env, capture_output=True,
                          text=True, timeout=30)
    leftover = set(Path("/tmp").glob("ks-gnupg.*")) - before
    lines = [ln for ln in calls.read_text().splitlines() if ln]
    homes = [ln.split("|", 1)[0] for ln in lines]
    args = [ln.split("|", 1)[1] for ln in lines]
    return done, homes, args, conf.read_text().splitlines(), leftover


def test_a_healthy_call_runs_once_in_a_fresh_home_and_leaves_nothing(tmp_path):
    done, homes, args, conf, leftover = _run(tmp_path, "ok")
    out = done.stdout + done.stderr
    assert done.returncode == 0, out
    assert "gpg_safe rc=0" in out and "reached the end" in out
    assert "ERR trap fired" not in out
    assert len(args) == 2
    # its own home, not the caller's, and a different one per call
    assert all(h.startswith("/tmp/ks-gnupg.") for h in homes), homes
    assert len(set(homes)) == 2
    # the caller's arguments, untouched, behind --no-keyring
    assert args[0] == "--no-keyring --batch --yes --passphrase-file /dev/null -o out in"
    assert not leftover, leftover
    # every home's agent is stopped on the way out
    assert sum("--kill all" in c for c in conf) == 2, conf


def test_a_call_that_found_no_agent_is_asked_again(tmp_path):
    done, homes, args, conf, leftover = _run(tmp_path, "no-agent-once")
    out = done.stdout + done.stderr
    assert done.returncode == 0, out
    assert "gpg_safe rc=0" in out
    assert "No agent running" in out, "the first attempt's words still reach the log"
    assert "asking again" in out
    assert "ERR trap fired" not in out, "a recovered call must not set off the caller's trap"
    assert len(args) == 3  # refused, asked again, then the second call
    assert homes[0] == homes[1], "the second ask reuses the call's own home"
    assert any(c.startswith(f"--homedir {homes[0]} --kill gpg-agent") for c in conf), conf
    assert not leftover, leftover


def test_an_agent_that_never_comes_back_fails_after_three_asks(tmp_path):
    done, homes, args, conf, leftover = _run(tmp_path, "no-agent")
    out = done.stdout + done.stderr
    assert "gpg_safe rc=2" in out, out
    assert len([a for a in args if "-o out in" in a]) == 3
    # the second call fails too; under errexit the caller's trap names it and
    # the script stops there, as it would on a bare gpg
    assert "ERR trap fired rc=2" in out, out
    assert "reached the end" not in out
    assert done.returncode != 0
    assert not leftover, leftover


def test_any_other_failure_is_not_asked_again(tmp_path):
    """A wrong passphrase also exits 2; only gpg's words tell the two apart."""
    done, homes, args, conf, leftover = _run(tmp_path, "bad-passphrase")
    out = done.stdout + done.stderr
    assert "gpg_safe rc=2" in out, out
    assert "asking again" not in out
    assert len([a for a in args if "-o out in" in a]) == 1
    assert "Bad session key" in out
    assert not leftover, leftover


def test_the_default_delays_are_bounded():
    """Two second asks, thirteen seconds in all: the night's job waits a
    moment for a session to finish, never long enough to run into the next
    job on the host."""
    text = LIB.read_text()
    assert 'GPG_SAFE_DELAYS="${GPG_SAFE_DELAYS:-3 10}"' in text


@pytest.mark.skipif(shutil.which("gpg") is None, reason="no real gpg on this machine")
def test_real_gpg_round_trips(tmp_path):
    plain = tmp_path / "plain"
    plain.write_bytes(os.urandom(50_000))
    passfile = tmp_path / "pf"
    passfile.write_text("not-the-real-one\n")
    passfile.chmod(0o600)
    script = f"""
set -Eeuo pipefail
source {LIB}
gpg_safe --symmetric --cipher-algo AES256 --batch --yes \\
    --passphrase-file {passfile} -o {tmp_path}/c.gpg {plain}
gpg_safe --batch --yes --quiet --passphrase-file {passfile} \\
    -o {tmp_path}/back -d {tmp_path}/c.gpg
"""
    before = set(Path("/tmp").glob("ks-gnupg.*"))
    done = subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                          timeout=60, env={**os.environ, "HOME": str(tmp_path)})
    assert done.returncode == 0, done.stdout + done.stderr
    assert (tmp_path / "back").read_bytes() == plain.read_bytes()
    assert not (tmp_path / "c.gpg").read_bytes().startswith(plain.read_bytes()[:64])
    assert set(Path("/tmp").glob("ks-gnupg.*")) == before
    # nothing about a new keybox in the log of a symmetric job
    assert "keybox" not in done.stderr, done.stderr


# A command the wrapper runs besides gpg that fails. `sleep`, `rm` and `cat`
# are external commands, so a wrapper of the same name on PATH stands in for
# them; the two that touch files fail only on the wrapper's own home.
FAKE_SLEEP = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_SLEEPS"
case "$1" in ''|*[!0-9.]*) echo "sleep: invalid time interval '$1'" >&2; exit 1 ;; esac
exit 0
"""
FAILS_ON_HOME = r"""#!/usr/bin/env bash
case "$*" in *ks-gnupg*) echo "{name}: injected failure" >&2; exit 1 ;; esac
exec {real} "$@"
"""


def _run_with(tmp_path, mode, prelude="", failing=()):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    fakes = {"gpg": FAKE_GPG, "gpgconf": FAKE_GPGCONF, "sleep": FAKE_SLEEP}
    for name in failing:
        fakes[name] = FAILS_ON_HOME.format(name=name, real=shutil.which(name))
    for name, body in fakes.items():
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    calls, sleeps = tmp_path / "calls", tmp_path / "sleeps"
    calls.write_text("")
    sleeps.write_text("")
    script = f"""
set -Eeuo pipefail
trap 'echo "ERR trap fired rc=$?"' ERR
source {LIB}
{prelude}
gpg_safe --batch -o out in
echo "gpg_safe ok"
"""
    env = {**os.environ,
           "PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
           "FAKE_CALLS": str(calls), "FAKE_GPGCONF_CALLS": str(tmp_path / "conf"),
           "FAKE_SLEEPS": str(sleeps), "FAKE_MODE": mode}
    env.pop("GPG_SAFE_DELAYS", None)
    done = subprocess.run(["bash", "-c", script], env=env, capture_output=True,
                          text=True, timeout=30)
    return (done, done.stdout + done.stderr,
            [ln for ln in calls.read_text().splitlines() if ln],
            [ln for ln in sleeps.read_text().splitlines() if ln])


def test_unset_after_sourcing_keeps_the_defaults(tmp_path):
    """Under `set -u` an unset GPG_SAFE_DELAYS used to end the shell with no
    ERR trap — so no alert — and leave the home behind."""
    done, out, calls, sleeps = _run_with(tmp_path, "no-agent-once",
                                         prelude="unset GPG_SAFE_DELAYS")
    assert "gpg_safe ok" in out and "ERR trap fired" not in out, out
    assert len(calls) == 2 and sleeps == ["3"], (calls, sleeps)


def test_a_malformed_delay_costs_only_the_wait(tmp_path):
    done, out, calls, sleeps = _run_with(tmp_path, "no-agent-once",
                                         prelude='GPG_SAFE_DELAYS="x y"')
    assert "gpg_safe ok" in out and "ERR trap fired" not in out, out
    assert len(calls) == 2 and sleeps == ["x"], (calls, sleeps)


@pytest.mark.parametrize("tool", ["rm", "cat"])
def test_a_failing_helper_never_stands_in_for_gpg(tmp_path, tool):
    """gpg succeeded; a failed rm of the home or cat of its stderr must not
    fail the call, or the caller's trap names a step gpg completed."""
    done, out, calls, sleeps = _run_with(tmp_path, "ok", failing=(tool,))
    assert "gpg_safe ok" in out and "ERR trap fired" not in out, out
    assert done.returncode == 0, out
