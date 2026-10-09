"""deploy/step13_rehearsal.sh runs beside production, so its safety is read
out of the script itself, not taken on trust.

The rehearsal shares the host's Docker daemon, disk and memory with the live
stack for over an hour. Every property below is one that, broken, would touch
production: a docker command aimed at a name that is not the rehearsal's, a
write into the deployed tree, a container without a memory cap, a network with
a route out (KeyCRM's quota, Telegram), a cleanup that leaves volumes behind or
deletes the wrong directory. Each is asserted on the parsed commands — never on
the comments beside them, which can say the right thing while the code
regresses.

`tests/unit/test_ci_workflow.py` already walks every `deploy/**/*.sh` for a
`docker rm` without `-v` and for a `docker build` without the cache cap, and
`tests/unit/test_gate_host_lock.py` for the host lock; the rehearsal is held to
both by those walks, not by copies of them here.
"""
from __future__ import annotations

import re
import shlex
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "deploy" / "step13_rehearsal.sh"
TEXT = SCRIPT.read_text(encoding="utf-8")

# The variables that name a path inside the repository — on the host, inside
# the deployed tree. Mounted only read-only, never a write's target.
REPO_VARS = ("REPO", "SELF_DIR", "HELPER_DIR", "SQL_DIR", "PROD_ENV")
NAME_VARS = ("REH_NET", "REH_PG", "REH_CH", "REH_KEYCRM", "REH_WEB", "REH_SEED",
             "REH_PROBE", "REH_MIGRATE")
CONTAINER_VARS = tuple(v for v in NAME_VARS if v != "REH_NET")
# Helpers whose first argument is the container they act on.
CONTAINER_HELPERS = ("start_web", "running", "oom_killed", "wait_health", "save_log", "stop_web",
                     "state_of", "wait_stopped", "kill_web", "start_stopped")


def _code_lines() -> List[Tuple[int, str]]:
    """(line number, text) with whole-line comments dropped."""
    return [(n, line) for n, line in enumerate(TEXT.splitlines(), start=1)
            if line.strip() and not line.lstrip().startswith("#")]


def _statements() -> List[Tuple[int, str]]:
    """Logical lines: backslash continuations joined, whole-line comments
    dropped, a trailing ` # ...` comment cut."""
    out: List[Tuple[int, str]] = []
    buf, start = "", None
    for n, line in _code_lines():
        if start is None:
            start = n
        if line.rstrip().endswith("\\"):
            buf += line.rstrip()[:-1] + " "
            continue
        joined = (buf + line).strip()
        joined = re.sub(r"\s+#\s[^\"']*$", "", joined)
        out.append((start, joined))
        buf, start = "", None
    return out


STATEMENTS = _statements()


def _assignments() -> Dict[str, str]:
    found: Dict[str, str] = {}
    for _n, stmt in STATEMENTS:
        m = re.match(r"^([A-Z_][A-Z0-9_]*)=(.+)$", stmt)
        if m and m.group(1) not in found:
            value = m.group(2)
            if len(value) > 1 and value[0] == value[-1] == '"':
                value = value[1:-1]
            found[m.group(1)] = value
    return found


ASSIGN = _assignments()


_PUNCT = ";&|<>()"


def _words(fragment: str) -> List[str]:
    """The words of one command, up to the first redirection, pipe, list
    operator or closing parenthesis — quotes honoured, as the shell reads
    them. A fragment cut out of `"$(docker ...)"` ends in an unclosed quote;
    the words before it are kept."""
    lex = shlex.shlex(fragment, posix=True, punctuation_chars=_PUNCT)
    lex.whitespace_split = True
    words: List[str] = []
    try:
        for token in lex:
            if token and set(token) <= set(_PUNCT):
                break
            words.append(token)
    except ValueError:
        pass
    return words


def _docker_commands() -> List[Tuple[int, str, List[str]]]:
    """Every docker invocation: (line, subcommand, the words after it)."""
    cmds = []
    for n, stmt in STATEMENTS:
        for m in re.finditer(r"(?:^|[\s;(|&`$\"])docker\s+([a-z-]+)", stmt):
            words = _words(stmt[m.end():])
            cmds.append((n, m.group(1), words))
    return cmds


DOCKER = _docker_commands()


def _runs() -> List[Tuple[int, List[str]]]:
    """Every `docker run`, as the words after `run`."""
    return [(n, words) for n, sub, words in DOCKER if sub == "run"]


def _mib(value: str) -> int:
    value = value.strip('"')
    if value.startswith("$"):
        value = ASSIGN[value.strip("${}")]
    m = re.fullmatch(r"(\d+)([mg])", value)
    assert m, f"a memory cap the test cannot read: {value!r}"
    return int(m.group(1)) * (1024 if m.group(2) == "g" else 1)


def _flag(words: List[str], name: str) -> Optional[str]:
    for i, word in enumerate(words):
        if word == name and i + 1 < len(words):
            return words[i + 1]
        if word.startswith(name + "="):
            return word.split("=", 1)[1]
    return None


# ─── 1. Names: the rehearsal's, and nothing else ─────────────────────────────

def test_every_name_variable_is_a_reh_literal():
    for var in NAME_VARS:
        assert var in ASSIGN, f"{var} is never assigned"
        assert ASSIGN[var].startswith("reh-"), f"{var}={ASSIGN[var]!r} is not a reh- name"


def test_no_production_or_gate_name_appears_in_the_code():
    forbidden = re.compile(
        r"keycrm-web\b|keycrm-bot|keycrm-migrate\b(?!:)|ks-postgres|ks-clickhouse|ks-migrate"
        r"|ks-tg-bot|ks-data-platform|ks-data\b|gate-pg|gate-ch|quick-pg|\bks-gate\b(?!\.lock)")
    for n, line in _code_lines():
        # The registry image names are images, not containers: allowed only
        # as the defaults the script runs, never pulls.
        line = line.replace("halloweex/keycrm-web:latest", "").replace(
            "halloweex/keycrm-migrate:latest", "")
        assert not forbidden.search(line), f"line {n}: {line.strip()}"


def test_compose_is_never_addressed():
    for n, line in _code_lines():
        assert not re.search(r"docker[ -]compose", line), f"line {n} addresses a compose project"


def test_every_container_is_named_as_one_of_the_rehearsals():
    """No container goes unnamed, not even a `--rm` one-off of a second:
    Docker would name it something like `eager_bassi`, which is not reh-*,
    and the host's `docker ps` is shared with production while it runs."""
    for n, words in _runs():
        name = _flag(words, "--name")
        assert name is not None, f"line {n}: a container without --name"
        assert name in {f"${v}" for v in CONTAINER_VARS} | {"$name"}, f"line {n}: --name {name}"


def _function_lines(name: str) -> range:
    lines = TEXT.splitlines()
    start = next(i for i, l in enumerate(lines, start=1) if l.startswith(f"{name}() {{"))
    end = next(i for i, l in enumerate(lines, start=1) if i > start and l == "}")
    return range(start, end + 1)


def test_every_container_command_targets_a_rehearsal_container():
    allowed = {f"${v}" for v in CONTAINER_VARS} | {"$1", "$name"}
    takes_value = {"-f", "--format", "-e", "-t", "--since", "--until", "-u", "--time", "-s"}
    # The one look at containers that are not the rehearsal's: Z0's
    # `docker inspect` of what `docker ps` listed, inside others() alone.
    others = _function_lines("others")
    checked = 0
    for n, sub, words in DOCKER:
        if sub == "inspect" and n in others and "$ids" in words:
            continue
        if sub not in {"exec", "kill", "stop", "start", "rm", "logs", "inspect", "wait",
                       "cp", "restart", "pause", "unpause"}:
            continue
        targets, skip = [], False
        for word in words:
            if skip:
                skip = False
                continue
            if word in takes_value:
                skip = True
                continue
            if word.startswith("-"):
                continue
            targets.append(word)
            if sub in {"exec", "logs", "inspect"}:
                break    # the rest is the command, or nothing
        assert targets, f"line {n}: docker {sub} with no target"
        for target in targets:
            if target.startswith((">", "2>")) or target in {"||", "true"}:
                break
            assert target in allowed, f"line {n}: docker {sub} aims at {target!r}"
            checked += 1
    assert checked >= 20, f"the walk found only {checked} targeted docker commands"


def test_the_look_at_other_containers_only_reads_and_sees_restarts():
    """others() is the one function aimed at containers that are not the
    rehearsal's — production's among them — so it may list and inspect,
    nothing else. And it must record what a restart changes: an id and a
    name stay the same across a restart policy's restart, so a live web the
    kernel killed would otherwise read as 'unchanged'."""
    lines = _function_lines("others")
    subs = [(n, sub) for n, sub, _w in DOCKER if n in lines]
    assert subs and {sub for _n, sub in subs} == {"ps", "inspect"}, subs
    body = _function_body("others")
    for field in ("{{.State.StartedAt}}", "{{.RestartCount}}", "{{.State.OOMKilled}}"):
        assert field in body, f"others() does not record {field}"
    calls = [n for n, stmt in STATEMENTS if re.search(r"(^|[\s;(|&!])others(\s|$)", stmt)
             and not stmt.startswith("others()")]
    assert len(calls) == 2, f"others() is called {len(calls)} times, not before and after"


def test_helpers_that_take_a_container_are_handed_a_rehearsal_one():
    calls = 0
    for n, stmt in STATEMENTS:
        for helper in CONTAINER_HELPERS:
            for m in re.finditer(rf"(?:^|[\s;(|&!])({helper})\s+(?=\S)", stmt):
                if stmt.lstrip().startswith(f"{helper}()"):
                    continue
                args = _words(stmt[m.end():])
                arg = args[0] if args else ""
                assert arg in {f"${v}" for v in CONTAINER_VARS} | {"$name", "$1"}, (
                    f"line {n}: {helper} {arg}")
                calls += 1
    assert calls >= 10


def test_the_network_is_internal_and_the_only_one():
    creates = [(n, w) for n, sub, w in DOCKER if sub == "network" and w and w[0] == "create"]
    assert creates, "reh-net is never created"
    for n, words in creates:
        assert "--internal" in words, f"line {n}: a network with a route out"
        assert words[-1] == "$REH_NET", f"line {n}: creates {words[-1]}"
    for n, sub, words in DOCKER:
        if sub == "network" and words and words[0] in {"rm", "inspect", "connect", "disconnect"}:
            assert words[-1] == "$REH_NET" or "$REH_NET" in words, f"line {n}: network {words}"


def test_a_built_image_is_a_reh_one():
    for n, sub, words in DOCKER:
        if sub == "build":
            tag = _flag(words, "-t")
            assert tag and tag.startswith("reh-"), f"line {n}: builds {tag!r}"


# ─── 2. The lock before anything ─────────────────────────────────────────────

def test_the_host_lock_is_taken_before_any_container_or_cleanup():
    lines = TEXT.splitlines()

    def first(pattern: str) -> Optional[int]:
        for i, line in enumerate(lines):
            if not line.lstrip().startswith("#") and re.search(pattern, line):
                return i
        return None

    lock = first(r"^exec 9>/tmp/ks-gate\.lock$")
    flock = first(r"flock -n 9\b")
    remove = first(r"^remove_all$")
    run = first(r"\bdocker run\b")
    assert lock is not None and flock is not None and flock > lock
    assert remove is not None and remove > flock, "the leading cleanup runs before the lock"
    assert run is not None and run > flock, "a container could start before the lock"
    # Refused without flock on the host; only --local may go on without it.
    assert re.search(r'elif \[ "\$LOCAL" = 1 \]; then\n\s+say "no flock', TEXT)
    assert 'die "flock is required on the host"' in TEXT


# ─── 3. Cleanup ──────────────────────────────────────────────────────────────

def _function_body(name: str) -> str:
    m = re.search(rf"^{name}\(\) \{{\n(.*?)^\}}", TEXT, re.S | re.M)
    assert m, f"no function {name}"
    return m.group(1)


def test_the_exit_trap_removes_every_container_with_its_volumes():
    assert re.search(r"^trap cleanup EXIT$", TEXT, re.M)
    assert re.search(r"^trap 'exit 143' TERM INT HUP$", TEXT, re.M), (
        "a SIGTERM (the memory watchdog's) must leave through the EXIT trap")
    cleanup = _function_body("cleanup")
    assert re.search(r"^\s+remove_all$", cleanup, re.M)
    body = _function_body("remove_all")
    rm = re.search(r"docker rm -f -v ((?:\"\$[A-Z_]+\"\s*)+)", body)
    assert rm, "remove_all does not docker rm -f -v"
    removed = set(re.findall(r"\$([A-Z_]+)", rm.group(1)))
    assert removed == set(CONTAINER_VARS)
    assert 'docker network rm "$REH_NET"' in body


def test_the_one_directory_it_deletes_is_guarded():
    body = _function_body("remove_all")
    lines = [l.strip() for l in body.splitlines()]
    assert lines.index("safe_root") < lines.index('rm -rf "$REH_ROOT"')
    guard = _function_body("safe_root")
    assert "*/reh-step13) ;;" in guard
    assert '"$REPO"/*|/opt/key-api-bot/*' in guard
    rms = [s for _n, s in STATEMENTS if re.search(r"(^|\s)rm\s+-[a-z]*r", s)]
    for stmt in rms:
        # The one other: the evidence a failed judge kept, by its exact glob
        # beside the report (test_a_run_removes_the_evidence_an_earlier_judge_kept…).
        if 'rm -rf -- "$kept"' in stmt:
            assert ('for kept in "$REPORT_DIR"/step13-rehearsal-*-evidence; do\n'
                    '    if [ -d "$kept" ]; then rm -rf -- "$kept"; fi\n'
                    'done\n') in SCRIPT.read_text(encoding="utf-8"), stmt
            continue
        assert re.search(r'rm -rf "\$(REH_ROOT|DATA_DIR" "\$KEYCRM_DIR)"', stmt), stmt


def test_keep_says_what_it_leaves_and_cleanup_only_removes():
    cleanup = _function_body("cleanup")
    assert "customer names and phone numbers" in cleanup
    assert "--cleanup-only" in cleanup
    assert re.search(r'if \[ "\$CLEANUP_ONLY" = 1 \]; then\n\s+KEEP=0', TEXT)


# ─── 4. Nothing reaches a person or KeyCRM ───────────────────────────────────

def _web_env() -> str:
    m = re.search(r"^WEB_ENV=\((.*?)^\)", TEXT, re.S | re.M)
    assert m
    return m.group(1)


def test_reh_web_cannot_alert_anyone_or_call_keycrm():
    env = _web_env()
    assert "-e KS_ALERTS_DISABLED=1" in env
    assert '-e "KEYCRM_BASE_URL=http://$REH_KEYCRM:8080/v1"' in env
    assert "-e KEYCRM_API_KEY=reh-step13-stub-key-not-keycrm" in env
    assert "-e KS_INSTANCE=reh-step13" in env
    assert "-e ADMIN_USER_IDS=1" in env
    for n, line in _code_lines():
        assert "BOT_TOKEN" not in line, f"line {n} mentions BOT_TOKEN"
        assert "--env-file" not in line, f"line {n} passes an env file"
        assert "TELEGRAM" not in line.upper() or "telegram" in line.lower() and "#" in line


def test_every_web_container_starts_through_the_one_environment():
    starts = [w for _n, w in _runs() if "${WEB_ENV[@]}" in w]
    assert len(starts) == 1, "reh-web must start in exactly one place"
    assert _flag(starts[0], "--name") == "$name"
    image_runs = [w for _n, w in _runs() if "$REH_IMAGE" in w and "--entrypoint" not in w]
    assert image_runs == starts, "the image's own command runs only as reh-web"


# ─── 5. Read-only towards production ─────────────────────────────────────────

_REPO_VAR = re.compile(r"^\$\{?(" + "|".join(REPO_VARS) + r")(?![A-Za-z0-9_])")


def _is_repo_path(word: str) -> bool:
    word = word.strip('"')
    return word.startswith("/opt/key-api-bot") or bool(_REPO_VAR.match(word))


def test_repo_variables_are_derived_from_the_script_location():
    assert ASSIGN["SELF_DIR"].startswith("$(cd")
    assert ASSIGN["REPO"] == '$(cd "$SELF_DIR/.." && pwd)'
    for var in ("HELPER_DIR", "SQL_DIR", "PROD_ENV"):
        assert ASSIGN[var].startswith(("$SELF_DIR", "$HELPER_DIR", "$REPO")), var


def test_every_mount_of_the_tree_is_read_only():
    mounts = 0
    for n, words in _runs():
        for i, word in enumerate(words):
            if word in {"-v", "--volume"} and i + 1 < len(words):
                spec = words[i + 1]
                if _is_repo_path(spec):
                    assert spec.endswith(":ro"), f"line {n}: {spec} is mounted writable"
                    mounts += 1
            assert not word.startswith("--mount"), f"line {n}: --mount escapes this walk"
    assert mounts >= 8


def test_nothing_writes_into_the_tree():
    writers = re.compile(
        r"(?:^|[\s;(|&])(cp|mv|install|rm|mkdir|chown|chmod|touch|ln|truncate|tee)\s+([^;|&)]*)")
    for n, stmt in STATEMENTS:
        for m in re.finditer(r"(?<![<0-9&])>>?\s*(\"?[^\s;|&)]+)", stmt):
            assert not _is_repo_path(m.group(1)), f"line {n}: writes {m.group(1)}"
        for m in writers.finditer(stmt):
            cmd, args = m.group(1), [w for w in _words(m.group(2)) if not w.startswith("-")]
            if cmd in {"cp", "install", "ln"}:
                args = args[-1:]          # the destination; a source is only read
            for arg in args:
                assert not _is_repo_path(arg), f"line {n}: {cmd} {arg}"
        assert not re.search(r"sed\s+-i", stmt), f"line {n}: an in-place edit"


def test_production_env_is_read_by_exact_name_only():
    uses = [(n, s) for n, s in STATEMENTS if "PROD_ENV" in s or ".env" in s]
    for n, stmt in uses:
        ok = (stmt == 'PROD_ENV="$REPO/.env"'
              or re.fullmatch(r'elif \[ -f "\$PROD_ENV" \]; then', stmt)
              or re.search(r'grep -m1 "\^\$\{name\}=" "\$PROD_ENV"', stmt))
        assert ok, f"line {n} reads production's .env otherwise: {stmt}"
    reader = [s for _n, s in uses if "grep -m1" in s]
    assert len(reader) == 1
    # And `name` there comes from the image's own chain list, not a glob.
    assert re.search(r"for name in \$\(image_names chains\); do", TEXT)


def test_the_live_duckdb_file_is_never_read():
    for n, line in _code_lines():
        assert "data/analytics.duckdb" not in line.replace("/app/data/analytics.duckdb", ""), (
            f"line {n}: the live DuckDB file, whose copy is torn")
    assert re.search(r'ls -1t "\$REPO"/data/backups/analytics-\*\.duckdb', TEXT)
    assert re.search(r'ls -1t "\$REPO"/backups/postgres/ks-\*\.dump', TEXT)


# ─── 6. Memory ───────────────────────────────────────────────────────────────

CAPS = {"$REH_WEB": 1536, "$name": 1536, "$REH_PG": 512, "$REH_CH": 1536}


def test_every_container_has_a_hard_memory_cap():
    runs = _runs()
    assert len(runs) >= 12
    for n, words in runs:
        memory, swap = _flag(words, "--memory"), _flag(words, "--memory-swap")
        assert memory and swap, f"line {n}: docker run without --memory and --memory-swap"
        assert _mib(memory) == _mib(swap), f"line {n}: swap beyond the cap"
        name = _flag(words, "--name")
        if name in CAPS:
            assert _mib(memory) <= CAPS[name], f"line {n}: {name} capped at {memory}"
        else:
            assert _mib(memory) <= 1024, f"line {n}: a one-off capped at {memory}"


def test_duckdb_is_held_under_the_web_cap():
    env = _web_env()
    m = re.search(r"-e DUCKDB_MEMORY_LIMIT=(\d+)MB", env)
    assert m, "DUCKDB_MEMORY_LIMIT is not set"
    assert int(m.group(1)) < _mib(ASSIGN["WEB_MEM"]) * 0.6


def test_the_start_guard_and_the_watchdog_cover_the_caps():
    total = sum(_mib(ASSIGN[v]) for v in ("WEB_MEM", "PG_MEM", "CH_MEM", "STUB_MEM"))
    assert int(ASSIGN["CAPS_MIB"]) == total
    guard = _function_body("memory_guard")
    assert "CAPS_MIB + MEM_HEADROOM_MIB" in guard
    watch = _function_body("watch_memory")
    assert 'kill -TERM "$MAIN_PID"' in watch and "MEM_FLOOR_MIB" in watch


# ─── 7. Every container: never pulled, logs bounded, no port, no restart ────

def test_every_container_is_fenced():
    for n, words in _runs():
        assert _flag(words, "--pull") == "never", f"line {n}: may pull an image"
        assert any(w.startswith("max-size=") for w in words), f"line {n}: unbounded log"
        for bad in ("-p", "--publish", "-P", "--publish-all", "--privileged"):
            assert bad not in words, f"line {n}: {bad}"
        restart = _flag(words, "--restart")
        assert restart in (None, "no"), f"line {n}: --restart {restart}"
        network = _flag(words, "--network")
        assert network in ("$REH_NET", "none"), f"line {n}: --network {network}"
        assert "--network" in words, f"line {n}: on the default bridge"
        assert not any(w.startswith("-v") and "docker.sock" in w for w in words)


def test_a_long_running_container_never_restarts_on_its_own():
    for n, words in _runs():
        if "-d" in words:
            assert _flag(words, "--restart") == "no", f"line {n}: a detached container without --restart no"


@pytest.mark.parametrize("flag", ["--local", "--keep", "--cleanup-only", "--any-hour", "--build"])
def test_the_documented_flags_exist(flag):
    assert re.search(rf"^\s+{re.escape(flag)}\) ", TEXT, re.M)
    assert flag in TEXT.split("set -Eeuo pipefail")[0], f"{flag} is not documented in the header"


# ─── 8. The kernel picks the rehearsal, not the live web ─────────────────────

def test_every_container_is_first_in_line_for_the_oom_killer():
    """The caps bound what the rehearsal may take; they do not make it the
    victim when the host itself runs short. The kernel kills the largest
    badness, about its RSS — the live web (DuckDB at 4 GB, a 7 g cap) and
    never a 1.5 g rehearsal container, at the default adjustment of 0.
    Reproduced in one 400 m container: a 'live' process at 220 MB beside a
    'rehearsal' one growing to 250 MB — at 0 the live one was killed, at
    1000 the rehearsal was."""
    runs = _runs()
    for n, words in runs:
        assert _flag(words, "--oom-score-adj") == "1000", f"line {n}: not first in line"


# ─── 9. The watchdog, the lock and the disk, by running them ────────────────
#
# Each of these runs the script's own functions — cut out of the script, never
# copied — in a bash with `docker`, `df` and `du` stubbed, so that what is
# asserted is what the shell does, not what the text says.

def _function_text(name: str) -> str:
    m = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", TEXT, re.S | re.M)
    assert m, f"no function {name}"
    return m.group(0)


def _launch_line() -> str:
    lines = [l.strip() for l in TEXT.splitlines()
             if re.match(r"\s*watch_memory\b.*&$", l) and not l.lstrip().startswith("#")]
    assert len(lines) == 1, lines
    return lines[0]


DOCKER_STUB = """#!/bin/bash
# Records every call. `exec` stands in for a foreground command that ends when
# its container goes: it waits for a `kill` or an `rm`, and gives up at 30 s.
echo "$*" >> "$STUB_LOG"
case "$1" in
    kill) touch "$STUB_DIR/gone" ;;
    rm) sleep "${STUB_RM_SLEEP:-0}"; touch "$STUB_DIR/gone" ;;
    exec) for _ in $(seq 1 300); do [ -e "$STUB_DIR/gone" ] && exit 137; sleep 0.1; done ;;
esac
exit 0
"""

DF_STUB = """#!/bin/bash
echo "Filesystem 1024-blocks Used Available Capacity Mounted on"
echo "/dev/stub $((DF_USED + DF_AVAIL)) $DF_USED $DF_AVAIL 0% /"
"""

DU_STUB = """#!/bin/bash
case "$2" in
    *analytics*) echo "$DU_BACKUP_KB	$2" ;;
    *) echo "$DU_DUMP_KB	$2" ;;
esac
"""


def _stubs(tmp_path: Path) -> Dict[str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("docker", DOCKER_STUB), ("df", DF_STUB), ("du", DU_STUB)):
        path = bin_dir / name
        path.write_text(body)
        path.chmod(0o755)
    (tmp_path / "meminfo").write_text("MemAvailable:   8000000 kB\n")
    (tmp_path / "reh-step13").mkdir()
    (tmp_path / "docker-root").mkdir()
    import os

    return {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "STUB_LOG": str(tmp_path / "docker.log"), "STUB_DIR": str(tmp_path),
            "DF_USED": "1000000", "DF_AVAIL": "9000000"}


def _harness(tmp_path: Path, body: str) -> Path:
    names = "\n".join(f"{v}={ASSIGN[v]}" for v in NAME_VARS)
    functions = "".join(_function_text(f) for f in (
        "safe_root", "kill_all", "remove_all", "cleanup", "disk_used_pct",
        "disk_guard", "watch_memory"))
    script = tmp_path / "harness.sh"
    script.write_text(f"""set -Eeuo pipefail
{names}
MEM_FLOOR_MIB={ASSIGN["MEM_FLOOR_MIB"]}
DISK_CEIL_PCT={ASSIGN["DISK_CEIL_PCT"]}
WATCH_INTERVAL_S=1
MEMINFO={tmp_path}/meminfo
REPORT_DIR={tmp_path}
DOCKER_ROOT={tmp_path}/docker-root
REH_ROOT={tmp_path}/reh-step13
REPO=/nonexistent-repository
KEEP=0
BUILT=0
WATCHDOG_PID=""
MAIN_PID=$$
say() {{ printf '[reh] %s\\n' "$*" >&2; }}
die() {{ say "ABORT: $*"; exit 3; }}
{functions}
{body}
""")
    return script


def _calls(tmp_path: Path) -> List[str]:
    try:
        return (tmp_path / "docker.log").read_text().splitlines()
    except FileNotFoundError:
        return []


@pytest.mark.parametrize("pressure", ["memory", "disk"])
def test_the_watchdog_frees_the_host_itself_without_waiting_for_the_shell(tmp_path, pressure):
    """The shell runs a trapped TERM only when its foreground command returns —
    a `docker exec` bounded at 25 min, `pg_restore` at nothing. A watchdog
    that only signals the shell left the containers holding 3.6 GB all that
    time: reproduced, the teardown ran when the foreground command did, at
    +12 s of a 12 s stand-in. So it kills the containers first, and the
    command waiting on them returns."""
    import subprocess
    import time

    env = _stubs(tmp_path)
    if pressure == "memory":
        (tmp_path / "meminfo").write_text("MemAvailable:    512000 kB\n")
    else:
        env.update(DF_USED="7400000", DF_AVAIL="2600000")     # 74 %
    script = _harness(tmp_path, f"""
trap cleanup EXIT
trap 'exit 143' TERM INT HUP
{_launch_line()}
WATCHDOG_PID=$!
docker exec "$REH_WEB" python /reh/probe.py trigger-wait dq_mirror_landing --timeout 1500
echo "the shell went on" >> "$STUB_LOG"
""")
    started = time.monotonic()
    proc = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True,
                          timeout=60)
    elapsed = time.monotonic() - started
    calls = _calls(tmp_path)
    assert proc.returncode == 143, (proc.returncode, proc.stderr)
    assert elapsed < 10, f"the teardown waited {elapsed:.1f} s for the foreground command"
    kills = [c for c in calls if c.startswith("kill ")]
    assert kills, calls
    assert set(kills[0].split()[1:]) == {ASSIGN[v] for v in CONTAINER_VARS}
    assert calls.index(kills[0]) < next(i for i, c in enumerate(calls) if c.startswith("rm -f -v"))
    assert "the shell went on" not in calls


def test_a_killed_shell_leaves_neither_the_lock_nor_its_containers(tmp_path):
    """SIGKILL — `kill -9`, the kernel's OOM killer — runs no EXIT trap. The
    watchdog was forked after the lock was taken and inherited fd 9, so it
    held the host lock for as long as it looped, which was forever:
    reproduced, the lock was still held after the shell was gone, every gate
    then waits in `flock 9`, and `--cleanup-only` refuses. Launched without
    fd 9, it notices the shell is gone and removes what the shell started."""
    import fcntl
    import os
    import signal
    import subprocess
    import sys
    import time

    env = _stubs(tmp_path)
    lock = tmp_path / "ks-gate.lock"
    script = _harness(tmp_path, f"""
exec 9>{lock}
{sys.executable} -c 'import fcntl; fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)'
{_launch_line()}
echo $! > {tmp_path}/watchdog.pid
docker exec "$REH_PG" pg_restore -U postgres -d ks
""")

    def held() -> bool:
        fd = os.open(lock, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return False
        except BlockingIOError:
            return True
        finally:
            os.close(fd)

    proc = subprocess.Popen(["bash", str(script)], env=env, start_new_session=True,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 10
        while not (tmp_path / "watchdog.pid").exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        watchdog = int((tmp_path / "watchdog.pid").read_text())
        assert held(), "the harness never took the lock"
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        deadline = time.monotonic() + 10
        while held() and time.monotonic() < deadline:
            time.sleep(0.1)
        assert not held(), "the lock outlived the shell"
        while time.monotonic() < deadline:
            try:
                os.kill(watchdog, 0)
            except ProcessLookupError:
                break
            time.sleep(0.1)
        else:
            pytest.fail("the watchdog outlived the shell")
        removed = [c for c in _calls(tmp_path) if c.startswith("rm -f -v ")]
        assert removed and set(removed[0].split()[3:]) == {ASSIGN[v] for v in CONTAINER_VARS}
        assert not (tmp_path / "reh-step13").exists(), "the copies were left behind"
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def test_the_watchdog_never_holds_the_lock_itself(tmp_path):
    """Exiting with the shell is not enough on its own: the watchdog's last
    act is a `docker rm` that a busy daemon can hold for minutes, and a child
    holding fd 9 all that time is a gate waiting all that time. Here the
    `rm` hangs and the shell's own foreground command does not carry the
    lock (the real one ends when its container is removed — the test above),
    so the only process that could still hold it is the watchdog."""
    import fcntl
    import os
    import signal
    import subprocess
    import sys
    import time

    env = {**_stubs(tmp_path), "STUB_RM_SLEEP": "20"}
    lock = tmp_path / "ks-gate.lock"
    script = _harness(tmp_path, f"""
exec 9>{lock}
{sys.executable} -c 'import fcntl; fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)'
{_launch_line()}
echo $! > {tmp_path}/watchdog.pid
sleep 60 9>&-
""")
    proc = subprocess.Popen(["bash", str(script)], env=env, start_new_session=True,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 10
        while not (tmp_path / "watchdog.pid").exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        watchdog = int((tmp_path / "watchdog.pid").read_text())
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        deadline = time.monotonic() + 8
        while not (tmp_path / "docker.log").exists() and time.monotonic() < deadline:
            time.sleep(0.1)       # the watchdog is inside its hanging `docker rm`
        os.kill(watchdog, 0)      # and alive
        fd = os.open(lock, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pytest.fail("the watchdog holds the host lock")
        finally:
            os.close(fd)
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def test_the_watchdog_is_launched_without_the_lock_and_lives_only_with_the_shell():
    assert _launch_line() == "watch_memory 9>&- &"
    body = _function_body("watch_memory")
    assert 'while kill -0 "$MAIN_PID" 2>/dev/null; do' in body
    assert body.index("kill_all") < body.index('kill -TERM "$MAIN_PID"')


KB = 1024
GB = 1024 * 1024


@pytest.mark.parametrize("used_pct, expect", [(69, 3), (63, 3), (60, 0), (40, 0)])
def test_the_disk_guard_counts_the_copies_it_is_about_to_make(tmp_path, used_pct, expect):
    """It used to compare today's use against 70 % and add nothing: a host at
    69 % passed, and a 2.6 GB backup, its growth in P8 and a restored dump
    end the run past the live monitor's 75 % WARN — production's admins paged
    about the rehearsal's own files. Reproduced with the reviewer's numbers:
    80 GB at 69 % passed."""
    import subprocess

    env = _stubs(tmp_path)
    total = 80 * GB
    used = total * used_pct // 100
    env.update(DF_USED=str(used), DF_AVAIL=str(total - used),
               DU_BACKUP_KB=str(int(2.6 * GB)), DU_DUMP_KB=str(int(0.4 * GB)))
    script = _harness(tmp_path, f'disk_guard {tmp_path}/analytics-x.duckdb {tmp_path}/ks-x.dump')
    proc = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True,
                          timeout=30)
    assert proc.returncode == expect, proc.stderr
    if expect:
        assert "would end at" in proc.stderr


def test_the_disk_ceiling_sits_under_the_live_monitors_warning():
    m = re.search(r"^WARN_DISK_PCT = ([\d.]+)$",
                  (REPO / "core" / "disk_monitor.py").read_text(), re.M)
    assert m and int(ASSIGN["DISK_CEIL_PCT"]) < float(m.group(1))
    assert 'disk_guard "$BACKUP" "$DUMP"' in TEXT


# ─── 10. --local on the host, --keep, a second signal ────────────────────────

def test_local_is_refused_on_the_host_before_anything_runs(tmp_path):
    """--local skips the window, MemAvailable and the watchdog, and may build
    an uncapped image: on the host that is 3.6 GB beside the live web with
    nothing watching. Refused before the lock and before any docker call."""
    import os
    import subprocess

    env = _stubs(tmp_path)
    fake_id = tmp_path / "bin" / "id"
    fake_id.write_text('#!/bin/bash\n[ "$1" = -u ] && echo 0 || /usr/bin/id "$@"\n')
    fake_id.chmod(0o755)
    env["TMPDIR"] = str(tmp_path)
    proc = subprocess.run(["bash", str(SCRIPT), "--local"], env=env, capture_output=True,
                          text=True, timeout=30)
    assert proc.returncode == 3, proc.stderr
    assert "--local is for a laptop" in proc.stderr
    assert _calls(tmp_path) == [], "a docker command ran first"
    lines = TEXT.splitlines()
    refusal = next(i for i, l in enumerate(lines) if "--local is for a laptop" in l)
    lock = lines.index("exec 9>/tmp/ks-gate.lock")
    assert refusal < lock
    assert os.path.exists(str(SCRIPT))


def test_keep_names_every_copy_it_leaves():
    """The stopped reh-pg keeps its anonymous volume, a full restore of
    production's Postgres — buyers and phone numbers — and reh-ch its Silver:
    seen after a local --keep run. The warning named the directory alone."""
    cleanup = _function_body("cleanup")
    message = next(l for l in cleanup.splitlines() if "--keep:" in l)
    for word in ("$REH_ROOT", "$REH_PG", "$REH_CH", "anonymous volumes",
                 "customer names and phone numbers", "--cleanup-only"):
        assert word in message, word


def test_cleanup_is_not_cut_short_by_a_second_signal():
    cleanup = [l.strip() for l in _function_body("cleanup").splitlines()
               if l.strip() and not l.strip().startswith("#")]
    assert cleanup[0] == "local rc=$?"
    assert cleanup[1] == "trap '' TERM INT HUP"


# ─── 11. A stop, a kill and a start are what the container shows ────────────
#
# P3, P6 and D1 judge the container's own state after each stop and start,
# never what the script meant to do. These run the script's functions against
# a `docker` that keeps one container's state — running, exit code, OOM, when
# it started and finished — and changes it the way Docker does: `kill` and
# `stop` end it (at once, or a few inspects later, as Docker records an exit
# after `docker kill` returns), `start` on a running container changes
# nothing.

STATE_STUB = r'''#!{python}
import json, os, sys
d = os.environ["STUB_DIR"]
path = os.path.join(d, "state.json")
with open(os.path.join(d, "docker.log"), "a") as log:
    log.write(" ".join(sys.argv[1:]) + "\n")
st = json.load(open(path))

def now():
    st["clock"] = st.get("clock", 0) + 1
    return "2026-10-02T09:%02d:%02d.25Z" % divmod(st["clock"], 60)

def end(code, oom=False):
    st.update(running=False, exit_code=code, oom_killed=oom, finished_at=now())

cmd, args = sys.argv[1], sys.argv[2:]
if cmd == "inspect":
    left = st.get("stops_after_inspects")
    if st["running"] and isinstance(left, int):
        st["stops_after_inspects"] = left - 1
        if left - 1 <= 0:
            st.pop("stops_after_inspects")
            end(st.get("exit_on_stop", 137))
    fmt = args[args.index("-f") + 1]
    for key, value in (("{{.State.Running}}", str(st["running"]).lower()),
                       ("{{.State.ExitCode}}", str(st["exit_code"])),
                       ("{{.State.OOMKilled}}", str(st["oom_killed"]).lower()),
                       ("{{.State.StartedAt}}", st["started_at"]),
                       ("{{.State.FinishedAt}}", st["finished_at"]),
                       ("{{.RestartCount}}", "0")):
        fmt = fmt.replace(key, value)
    print(fmt)
elif cmd == "kill" and st["running"]:
    how = st.get("on_kill", "kill")
    if how == "kill":
        end(137)
    elif how == "late":
        st.update(stops_after_inspects=3, exit_on_stop=137)
    elif how == "oom":
        end(137, oom=True)
elif cmd == "stop" and st["running"]:
    end(0 if st.get("on_stop", "graceful") == "graceful" else 137)
elif cmd == "start" and not st["running"]:
    st.update(running=True, oom_killed=False, started_at=now())
json.dump(st, open(path, "w"))
'''

RUNNING = {"running": True, "exit_code": 0, "oom_killed": False,
           "started_at": "2026-10-02T08:59:00.5Z", "finished_at": "0001-01-01T00:00:00Z"}
STATE_FUNCTIONS = ("running", "state_of", "wait_stopped", "sigkilled", "json_field", "kill_web",
                   "start_stopped", "stop_web", "save_log", "log_count", "restart_record",
                   "p3_record", "window_run", "max_int")


def _state_run(tmp_path: Path, body: str, state: Optional[dict] = None, **env_over):
    """Runs `body` after the script's own state functions, against a
    `docker` that keeps one container's state. Returns (process, docker
    calls, final state)."""
    import json
    import os
    import subprocess
    import sys

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    stub = bin_dir / "docker"
    stub.write_text(STATE_STUB.replace("{python}", sys.executable))
    stub.chmod(0o755)
    (tmp_path / "state.json").write_text(json.dumps(state or RUNNING))
    for d in ("ev", "logs"):
        (tmp_path / d).mkdir(exist_ok=True)
    (tmp_path / "logs" / "flip.log").write_text("")
    names = "\n".join(f"{v}={ASSIGN[v]}" for v in NAME_VARS)
    functions = "".join(_function_text(f) for f in STATE_FUNCTIONS)
    script = tmp_path / "state.sh"
    script.write_text(f"""set -Eeuo pipefail
{names}
STOP_GRACE_S={ASSIGN["STOP_GRACE_S"]}
KILL_WAIT_S=2
STEP_TIMEOUT=5
EV={tmp_path}/ev
LOG_DIR={tmp_path}/logs
RESOLVED_SQL="SELECT 1"
say() {{ printf '[reh] %s\\n' "$*" >&2; }}
pgq() {{ echo 1; }}
{functions}
{body}
""")
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "STUB_DIR": str(tmp_path),
           **env_over}
    proc = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True,
                          timeout=60)
    calls = (tmp_path / "docker.log").read_text().splitlines() \
        if (tmp_path / "docker.log").exists() else []
    return proc, calls, json.loads((tmp_path / "state.json").read_text())


def _probe_module():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "reh_probe_for_script", REPO / "deploy" / "step13_rehearsal" / "probe.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PROBE = _probe_module()


@pytest.mark.parametrize("state, killed", [
    ({"running": False, "exit_code": 137, "oom_killed": False}, True),
    ({"running": False, "exit_code": 0, "oom_killed": False}, False),
    ({"running": False, "exit_code": 137, "oom_killed": True}, False),
    ({"running": True, "exit_code": 0, "oom_killed": False}, False),
    (None, False),
])
def test_sigkilled_is_a_sigkill_and_nothing_else(tmp_path, state, killed):
    """Kills: "sigkilled ignores the exit code", "…ignores the OOM killer"."""
    import json

    (tmp_path / "s.json").write_text(json.dumps(state))
    proc, _calls, _st = _state_run(tmp_path, f"sigkilled {tmp_path}/s.json")
    assert (proc.returncode == 0) is killed, proc.stderr


def test_wait_stopped_waits_for_the_recorded_exit_and_gives_up(tmp_path):
    """`docker kill` returns before Docker records the exit; a state read at
    once still says running.
    Kills: "wait_stopped never waits"."""
    proc, calls, st = _state_run(tmp_path, 'wait_stopped "$REH_WEB" 10',
                                 {**RUNNING, "stops_after_inspects": 3})
    assert proc.returncode == 0 and st["running"] is False, proc.stderr
    assert sum(c.startswith("inspect") for c in calls) >= 3
    proc, _calls, st = _state_run(tmp_path / "x" if (tmp_path / "x").mkdir() is None else tmp_path,
                                  'wait_stopped "$REH_WEB" 1')
    assert proc.returncode == 1 and st["running"] is True


@pytest.mark.parametrize("on_kill, state, landed, kind", [
    ("kill", RUNNING, True, "kill"),
    ("late", RUNNING, True, "kill"),
    ("oom", RUNNING, False, "oom"),
    ("ignore", RUNNING, False, "running"),
    ("kill", {**RUNNING, "running": False, "exit_code": 137,
              "finished_at": "2026-10-02T08:59:30.5Z"}, False, "not_running"),
])
def test_kill_web_lands_only_what_the_container_shows(tmp_path, on_kill, state, landed, kind):
    """The kill P3 and D1 judge is the one in this file, read with the
    probe's own `stop_kind`.
    Kills: "KILLED is set without the container's word", "the state is
    read before Docker has recorded the exit"."""
    import json

    proc, calls, _st = _state_run(
        tmp_path, f'kill_web "$REH_WEB" {tmp_path}/ev/kill.json',
        {**state, "on_kill": on_kill})
    assert (proc.returncode == 0) is landed, proc.stderr
    recorded = json.loads((tmp_path / "ev" / "kill.json").read_text())
    assert recorded["asked"] == "kill"
    assert PROBE.stop_kind(recorded) == kind, recorded
    assert any(c.startswith("kill ") for c in calls)


@pytest.mark.parametrize("state, starts", [
    (RUNNING, False),
    ({**RUNNING, "running": False, "exit_code": 137}, True),
])
def test_start_stopped_starts_only_what_stopped(tmp_path, state, starts):
    """Kills: "docker start runs on a container that never stopped"."""
    proc, calls, st = _state_run(tmp_path, 'start_stopped "$REH_WEB"', state)
    assert proc.returncode == 0 and st["running"] is True, proc.stderr
    assert any(c.startswith("start ") for c in calls) is starts, calls


@pytest.mark.parametrize("on_stop, kind", [("graceful", "graceful"), ("expired", "grace_expired")])
def test_stop_web_records_how_the_stop_ended(tmp_path, on_stop, kind):
    """A stop that outruns its grace ends exit 137, a kill's state: told
    apart only by what was asked, which the file carries.
    Kills: "a stop's state carries no `asked`", "a graceful stop is given
    a grace other than production's"."""
    import json

    proc, calls, _st = _state_run(
        tmp_path, f'stop_web "$REH_WEB" flip {tmp_path}/ev/stop.json', {**RUNNING, "on_stop": on_stop})
    assert proc.returncode == 0, proc.stderr
    assert f"stop -t {ASSIGN['STOP_GRACE_S']} {ASSIGN['REH_WEB']}" in calls
    recorded = json.loads((tmp_path / "ev" / "stop.json").read_text())
    assert recorded["grace_s"] == int(ASSIGN["STOP_GRACE_S"]) and recorded["was_running"] is True
    assert PROBE.stop_kind(recorded) == kind


def test_restart_record_writes_only_a_restart_the_container_shows(tmp_path):
    """The record P6 counts carries both states, and is not written when
    there is no restart to record: a `kill` record once followed a kill
    that was never sent and a `docker start` that did nothing.
    Kills: "the record is written whatever the stop showed", "the record
    drops the stop", "…drops the start"."""
    import json

    killed = {"running": False, "exit_code": 137, "oom_killed": False, "asked": "kill",
              "was_running": True, "started_at": "2026-10-02T08:59:00.5Z",
              "finished_at": "2026-10-02T09:00:00.25Z"}
    (tmp_path / "snap.json").write_text(json.dumps({"health_code": 200}))
    (tmp_path / "ev" / "kill.json").parent.mkdir(exist_ok=True)
    (tmp_path / "ev" / "kill.json").write_text(json.dumps(killed))
    proc, _calls, _st = _state_run(
        tmp_path, f'restart_record {tmp_path}/snap.json {tmp_path}/ev/r.json {tmp_path}/ev/kill.json',
        {**RUNNING, "started_at": "2026-10-02T09:00:03.5Z"})
    assert proc.returncode == 0, proc.stderr
    record = json.loads((tmp_path / "ev" / "r.json").read_text())
    assert record["stop"] == killed and record["start"]["running"] is True
    assert record["snapshot"] == {"health_code": 200}
    assert PROBE.restart_shown(record) == ("kill", None)

    for n, (stop, now) in enumerate([
            (None, RUNNING),                                   # nothing was killed
            ({**killed, "running": True}, RUNNING),            # it never stopped
            (killed, {**RUNNING, "running": False})]):         # it does not run again
        (tmp_path / "ev" / "kill.json").write_text(json.dumps(stop))
        (tmp_path / "ev" / "r.json").unlink(missing_ok=True)
        proc, _calls, _st = _state_run(
            tmp_path, f'restart_record {tmp_path}/snap.json {tmp_path}/ev/r.json {tmp_path}/ev/kill.json', now)
        assert proc.returncode == 0, proc.stderr
        assert not (tmp_path / "ev" / "r.json").exists(), (n, stop, now)


def test_p3_record_carries_the_kills_state(tmp_path):
    """Kills: "p3.json drops the kill's state"."""
    import json

    killed = {"running": False, "exit_code": 137, "oom_killed": False, "asked": "kill",
              "was_running": True}
    (tmp_path / "ev").mkdir(exist_ok=True)
    (tmp_path / "ev" / "kill.json").write_text(json.dumps(killed))
    proc, _calls, _st = _state_run(
        tmp_path, f"p3_record {tmp_path}/ev/p3.json {tmp_path}/ev/kill.json 41 40 4 "
                  "'{\"requested\": 41, \"built\": 40, \"rows_above\": 0}' '' 41")
    assert proc.returncode == 0, proc.stderr
    rec = json.loads((tmp_path / "ev" / "p3.json").read_text())
    assert rec["stop"] == killed and rec["seen_blocked"] is True
    assert (rec["requested"], rec["built"], rec["first_after_restart"]) == (41, 40, None)
    assert PROBE.stop_kind(rec["stop"]) == "kill"


WPROBE_STUB = r'''
wprobe() {
    echo "wprobe $*" >> "$EV/wprobe.log"
    case "$1" in
        dq-last) [ -n "${DQ_LAST:-}" ] || return 1; echo "{\"run_id\": $DQ_LAST}" ;;
        trigger-wait) [ "${TRIGGER_OK:-1}" = 1 ] || { echo '{"done": false}'; return 1; }; echo '{"done": true}' ;;
        wait-dq) echo "{\"run_id\": $WAIT_DQ, \"ended_at\": \"x\"}" ;;
    esac
}
'''


@pytest.mark.parametrize("env, floor, expect, after", [
    # The reviewer's case: the live read fails, the trigger does not run,
    # and the newest ended run is F4's #6, at F5s's checkpoint.
    ({"TRIGGER_OK": "0", "WAIT_DQ": "6"}, 6, "", None),
    # The trigger ran but the newest run is still the checkpoint's.
    ({"WAIT_DQ": "6"}, 6, "", "6"),
    # A failed trigger is no run of ours, whatever comes along.
    ({"TRIGGER_OK": "0", "WAIT_DQ": "9"}, 6, "", None),
    # The live read is above the checkpoint's floor: the wait starts there.
    ({"DQ_LAST": "7", "WAIT_DQ": "8"}, 6, "8", "7"),
    ({"WAIT_DQ": "9"}, 6, "9", "6"),
])
def test_window_run_names_only_a_run_written_after_the_checkpoint(tmp_path, env, floor,
                                                                  expect, after):
    """Kills: "the floor is the live read alone", "trigger-wait's exit is
    ignored", "a run at the floor is taken"."""
    proc, _calls, _st = _state_run(tmp_path, WPROBE_STUB + f"window_run {floor}", **env)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == expect, (proc.stdout, proc.stderr)
    log = (tmp_path / "ev" / "wprobe.log").read_text()
    waits = re.findall(r"wait-dq integrity --after (\d+)", log)
    assert waits == ([after] if after else []), log


def test_max_int_takes_only_whole_numbers():
    import subprocess

    out = subprocess.run(["bash", "-c", _function_text("max_int")
                          + 'max_int "" null 5 12 x7 3; max_int'],
                         capture_output=True, text=True).stdout.split()
    assert out == ["12", "0"]


def test_the_window_floor_is_every_run_the_checkpoint_holds():
    """F5w's floor reads F5s's DuckDB read, not only the live API: the read
    that failed once and let F4's run through."""
    stmt = next(s for _n, s in STATEMENTS if s.startswith("WIN_RUN="))
    assert 'json_field "$EV/d_pre.json" max_dq_run_id' in stmt
    assert '"$INT_RUN" "$ML_RUN"' in stmt


def _enclosing_ifs(line: int) -> List[str]:
    """The `if` conditions the statement starting at `line` sits under,
    outermost first, read off the parsed statements: `if …; then` opens one,
    `elif …; then` and `else` replace the innermost, `fi` closes it. A
    one-line `if …; fi` opens nothing."""
    stack: List[str] = []
    for n, stmt in STATEMENTS:
        if n >= line:
            break
        s = stmt.strip()
        if re.match(r"if\s", s) and s.endswith("; then"):
            stack.append(s)
        elif re.match(r"elif\s", s) and s.endswith("; then"):
            stack[-1] = s
        elif s == "else":
            stack[-1] = f"else of {stack[-1]}"
        elif s == "fi":
            stack.pop()
    return stack


def test_the_if_walk_is_balanced():
    """The walk below must see every `if` closed, or what it says sits
    under KILLED is read off a stack that drifted."""
    assert _enclosing_ifs(len(TEXT.splitlines()) + 1) == []
    assert any(s == 'if [ "$KILLED" = 1 ]; then' for _n, s in STATEMENTS)


def test_what_means_something_only_after_a_kill_sits_under_killed():
    """F6's evidence of a kill — P6's restart record after it, the journal
    read after it, the first derivation after the restart, the end's DELETE
    of D1's window — is written under `KILLED=1` alone, and KILLED is set
    only where `kill_web` succeeded inside the blocked derivation: the
    container showed the rehearsal's SIGKILL. The script once wrote the
    `kill` restart record inside `if wait_health`; with no TRUNCATE seen,
    nothing killed and `docker start` a no-op, P6 passed "3 restarts
    (graceful,kill,graceful)" on it.
    Kills: "the kill record is written whatever F6 did", "KILLED is set
    without kill_web", "kill_web runs without a blocked TRUNCATE"."""
    under_killed = ('if [ "$KILLED" = 1 ]; then', 'elif [ "$KILLED" = 1 ]; then')
    targets = {
        "the restart record after the kill":
            lambda s: s.startswith("restart_record ") and "p6_restart2.json" in s,
        "the journal read after the kill": lambda s: s.startswith("AFTER_KILL="),
        "the first derivation after the restart":
            lambda s: s.startswith("FIRST=") and "poll_pg" in s,
        "the end's DELETE of the window": lambda s: " window-delete " in s,
    }
    for what, match in targets.items():
        found = [n for n, s in STATEMENTS if match(s)]
        assert found, f"{what}: not in the script"
        for n in found:
            ifs = _enclosing_ifs(n)
            assert ifs and ifs[-1] in under_killed, (what, n, ifs)
    # Nothing else names P6's second record.
    assert [n for n, s in STATEMENTS if "p6_restart2.json" in s] == [
        n for n, s in STATEMENTS if targets["the restart record after the kill"](s)]
    assert [s for _n, s in STATEMENTS if re.match(r"KILLED=", s)] == ["KILLED=0", "KILLED=1"]
    one = next(n for n, s in STATEMENTS if s == "KILLED=1")
    assert _enclosing_ifs(one)[-1] == 'if kill_web "$REH_WEB" "$EV/f6_kill_state.json"; then'
    for n, s in STATEMENTS:
        if re.match(r"(if )?kill_web ", s):
            assert 'if [ -n "$BLOCKED" ]; then' in _enclosing_ifs(n), (n, s)


# ─── 12. Every call the script makes is one the probe accepts ────────────────

_PROBE_HELPERS = ("wprobe", "probe_offline", "probe_keycrm_dir")


def _shell_arrays() -> Dict[str, List[str]]:
    found: Dict[str, List[str]] = {}
    for _n, stmt in STATEMENTS:
        for m in re.finditer(r"\b([A-Z_][A-Z0-9_]*)\+?=\(([^)]*)\)", stmt):
            found.setdefault(m.group(1), []).extend(_words(m.group(2)))
    return found


def _expand(fragment: str, arrays: Dict[str, List[str]], positional: Optional[str]) -> List[str]:
    """The words of one call with every expansion stood in for: arrays by
    what they are ever given, `${X:+…}` by its words, `${X:-d}` by `d`, and
    any other variable by `1` — a value every int and str argument takes."""
    def array(m):
        return " ".join(shlex.quote(w) for w in arrays.get(m.group(1), []))

    fragment = re.split(r"\s\d*[<>]|\s\|\|?\s|\s&&\s|;", fragment)[0]
    fragment = re.sub(r'\$\{([A-Z_][A-Z0-9_]*)\[@\]\+"\$\{\1\[@\]\}"\}', array, fragment)
    fragment = re.sub(r'"\$\{([A-Z_][A-Z0-9_]*)\[@\]\}"', array, fragment)
    fragment = re.sub(r"\$\{[A-Za-z_]\w*:\+([^}]*)\}", r"\1", fragment)
    fragment = re.sub(r"\$\{[A-Za-z_]\w*:-([^}]*)\}", lambda m: m.group(1) or "1", fragment)
    if positional is not None:
        fragment = fragment.replace('"$1"', positional)
    fragment = re.sub(r"\$\{?[A-Za-z_]\w*\}?", "1", fragment)
    return _words(fragment)


def _probe_calls() -> List[Tuple[int, List[str]]]:
    arrays = _shell_arrays()
    image_names = _function_lines("image_names")
    lists = sorted(set(re.findall(r"image_names (\w+)", TEXT)))
    calls: List[Tuple[int, List[str]]] = []
    for n, stmt in STATEMENTS:
        for m in re.finditer(r"/reh/probe\.py\s+", stmt):
            rest = stmt[m.end():]
            if rest.startswith('"$@"'):
                continue
            for positional in (lists if n in image_names else [None]):
                calls.append((n, _expand(rest, arrays, positional)))
        for helper in _PROBE_HELPERS:
            if stmt.startswith(f"{helper}()"):
                continue
            for m in re.finditer(rf"(?:^|[\s;(|&!`$\"]){helper}\s+", stmt):
                calls.append((n, _expand(stmt[m.end():], arrays, None)))
    return calls


def test_every_probe_call_parses_under_the_probes_own_cli():
    """The script once passed `duckdb-facts --window-order/--window-run` and
    called `window-delete`, neither of which the probe defined: exit 2,
    swallowed by `|| true`, an empty `d1.json`, and P1, P6 failed and D1,
    P2 unknown on a run that had nothing wrong with it. Every call is parsed
    here with the probe's own parser.
    Kills: "a flag or a subcommand the script uses is missing from the
    probe"."""
    import contextlib
    import io

    calls = _probe_calls()
    bad = []
    for n, words in calls:
        with contextlib.redirect_stderr(io.StringIO()) as err:
            try:
                PROBE.build_parser().parse_args(words)
            except SystemExit:
                bad.append(f"line {n}: probe.py {' '.join(words)}: {err.getvalue().strip()[-200:]}")
    assert not bad, "\n".join(bad)
    used = {words[0] for _n, words in calls if words}
    assert {"duckdb-facts", "window-delete", "fixture", "wait-dq", "trigger-wait", "dq-last",
            "snapshot", "canary", "readers", "judge", "seed-gate", "derive-latches",
            "wait-health", "api", "keycrm-url"} <= used, used
    facts = [w for _n, w in calls if w and w[0] == "duckdb-facts"]
    assert any("--window-run" in w and "--window-order" in w for w in facts)
    assert any("--run" in w for w in facts)


def test_the_cli_walk_sees_a_flag_the_probe_lacks():
    """The walk above must be able to fail: a flag no subcommand defines."""
    import contextlib
    import io

    with contextlib.redirect_stderr(io.StringIO()), pytest.raises(SystemExit):
        PROBE.build_parser().parse_args(_expand('duckdb-facts --db x --window-col "$A"', {}, None))


# ─── 13. Production's versions and production's grace ───────────────────────

def _compose():
    import yaml

    return yaml.safe_load((REPO / "docker-compose.yml").read_text())


def test_the_store_images_are_productions():
    """The dump is restored into the server that wrote it: docker-compose's
    postgres, and the ClickHouse the stores' gate pins as production's. A
    drift here restores production's dump into a different server and every
    other test still passes.
    Kills: "PG_IMAGE or CH_IMAGE moves off production's"."""
    assert ASSIGN["PG_IMAGE"] == _compose()["services"]["postgres"]["image"]
    gate = (REPO / "deploy" / "gate_with_stores.sh").read_text()
    pinned = set(re.findall(r"clickhouse/clickhouse-server:[\w.\-]+", gate))
    assert pinned == {ASSIGN["CH_IMAGE"]}, pinned
    for n, line in _code_lines():
        if re.match(r"(PG|CH)_IMAGE=", line):
            continue
        assert not re.search(r"postgres:\d|clickhouse-server:", line), (
            f"line {n}: a store image named outside PG_IMAGE/CH_IMAGE")


def _seconds(value: str) -> int:
    total = 0
    for amount, unit in re.findall(r"(\d+)(h|ms|m|s)", str(value)):
        total += int(amount) * {"h": 3600, "m": 60, "s": 1, "ms": 0}[unit]
    return total


def test_a_graceful_stop_gets_exactly_productions_grace():
    """A deploy's `docker compose up -d` stops web with compose's grace —
    its `stop_grace_period`, 10 s when it sets none — and nothing in the
    workflow passes another. Every graceful stop of reh-web gets the same,
    so the checkpoint a phase calls a deploy's is one, and a stop that
    outruns it is the kill a deploy would make.
    Kills: "stop_web's grace drifts from production's"."""
    web = _compose()["services"]["web"]
    expected = _seconds(web["stop_grace_period"]) if "stop_grace_period" in web else 10
    assert int(ASSIGN["STOP_GRACE_S"]) == expected
    deploy = (REPO / ".github" / "workflows" / "deploy.yml").read_text()
    ups = re.findall(r"docker compose (?:up|stop|restart|down)[^\n]*", deploy)
    assert ups and not any(re.search(r"\s(-t|--timeout)\b", u) for u in ups), ups
    assert 'docker stop -t "$STOP_GRACE_S" "$1"' in _function_body("stop_web")
    stops = [(n, w) for n, sub, w in DOCKER if sub == "stop" and "$REH_WEB" in w]
    assert all(n in _function_lines("cleanup") for n, _w in stops), stops


# ─── 14. The flip the script acts on is the flip the judges read ─────────────

def _env_words(words: List[str]) -> Dict[str, str]:
    """`-e NAME=value` pairs out of a command's words, in order."""
    env: Dict[str, str] = {}
    for i, word in enumerate(words[:-1]):
        if word == "-e" and "=" in words[i + 1]:
            name, value = words[i + 1].split("=", 1)
            env[name] = value
    return env


def _phase_env(extra: List[str], monkeypatch) -> Dict[str, str]:
    """reh-web's environment for one `start_web "$REH_WEB" ...`: the list,
    the readers the image names (`probe.py readers`, as the script asks),
    and the phase's own `-e` flags. Shell variables stand for themselves —
    each is a non-empty value at run time."""
    monkeypatch.setattr(PROBE, "APP_DIR", str(REPO))
    env = _env_words(_words(_web_env()))
    env.update({name: "postgres" for name in PROBE.readers()["readers"]})
    env.update(_env_words(extra))
    return env


def _web_starts() -> Dict[str, List[str]]:
    starts = [_words(s[len("start_web "):]) for _n, s in STATEMENTS
              if s.startswith('start_web "$REH_WEB"')]
    by_flags = {" ".join(w[1:]): w[1:] for w in starts}
    assert set(by_flags) == {
        "-e KS_WRITE_WAREHOUSE=postgres",                              # phase 0
        "-e KS_WRITE_WAREHOUSE=postgres -e KS_READ_FALLBACK=off",      # F1
        "-e KS_READ_FALLBACK=off",                                     # B
    }, by_flags
    return by_flags


def test_the_rehearsal_sets_every_switch_the_cutover_names(monkeypatch):
    """F1 must be the flip with every precondition, and phase 0 every one
    but `read_fallback_off` — exactly what P1 and P7 demand. Run through
    this tree's own `evaluate_preconditions`, with every fact a Postgres
    read supplies taken as met, so a precondition main adds and the list
    lacks fails here and not an hour into a run: chain 7b's
    `KS_GOALS_HISTORY=silver` did exactly that — `no flip: mode=duckdb
    unmet=[goals_bridge]`.
    Kills: "WEB_ENV drops a switch the cutover requires"."""
    from core import warehouse_cutover as wc

    facts = wc.Facts(revision="r", required_revision="r", expenses_backfilled=True,
                     bridge_owners={}, open_retired={}, od10_doors=())
    starts = _web_starts()
    f1 = _phase_env(starts["-e KS_WRITE_WAREHOUSE=postgres -e KS_READ_FALLBACK=off"], monkeypatch)
    assert [u.key for u in wc.evaluate_preconditions(f1, facts)] == []
    phase0 = _phase_env(starts["-e KS_WRITE_WAREHOUSE=postgres"], monkeypatch)
    assert [u.key for u in wc.evaluate_preconditions(phase0, facts)] == ["read_fallback_off"]
    assert wc.evaluate_preconditions(
        {**f1, "KS_GOALS_HISTORY": "bridge"}, facts)[0].key == "goals_bridge"


def test_the_flip_is_read_the_way_p1_reads_it():
    """The script runs F2 to B only on a flip, and decides it with the
    probe's `writer-mode` over the same snapshot P1 judges — never a grep:
    the snapshot's `utm_parse` block reads `"mode": "postgres"` before any
    flip, and a grep for that ran every phase after F1 over a process the
    table then called unflipped.
    Kills: "FLIPPED is set off a grep of the snapshot"."""
    sets = [n for n, s in STATEMENTS if s == "FLIPPED=1"]
    assert len(sets) == 1, sets
    assert any("/reh/probe.py writer-mode" in s and '"$EV/f1_snapshot.json"' in s
               for s in _enclosing_ifs(sets[0])), _enclosing_ifs(sets[0])
    for n, s in STATEMENTS:
        assert not ("grep" in s and '"mode"' in s), f"line {n} greps a mode: {s}"


@pytest.mark.parametrize("snapshot, out, code", [
    # Before a flip: the UTM parse is postgres, the writer is not.
    ({"health_code": 200, "health": {"warehouse_writer_mode": {"mode": "duckdb"},
                                     "utm_parse": {"mode": "postgres"},
                                     "derivation": {"mode": "own"}}}, "duckdb", 1),
    ({"health_code": 200, "health": {"warehouse_writer_mode": {"mode": "postgres"},
                                     "utm_parse": {"mode": "postgres"}}}, "postgres", 0),
    (None, "", 1),
    ("not json", "", 1),
    ([1, 2], "", 1),
])
def test_writer_mode_reads_only_the_writers_mode(snapshot, out, code):
    import json
    import subprocess
    import sys

    text = snapshot if isinstance(snapshot, str) else ("" if snapshot is None else json.dumps(snapshot))
    proc = subprocess.run([sys.executable, str(REPO / "deploy" / "step13_rehearsal" / "probe.py"),
                           "writer-mode"], input=text, capture_output=True, text=True, timeout=60)
    assert (proc.stdout.strip(), proc.returncode) == (out, code), proc.stderr


# ─── 15. Every state file reaches the record and the judge it is for ─────────
#
# The judges read the container's state after each stop out of files the
# script names by hand, and a mutation review found the wiring unpinned:
# p3.json fed F7's stop instead of F6's kill, restart 1's record fed the
# kill, F7's stop recording nothing, d1_window.json written without its run,
# and --keep's skip removed — each left every test green. Three stops of
# reh-web recorded no state at all, so one that outran production's grace
# before the way back was a SIGKILL nothing said.

def _ev_name(word: str) -> Optional[str]:
    m = re.fullmatch(r"\$\{?EV\}?/(.+)", word)
    return m.group(1) if m else None


def _state_events() -> List[Tuple[int, str, List[str]]]:
    """In script order: every stop and kill of reh-web with the state file
    it writes, every start of it, and every record a stop's state is fed
    into — `(line, kind, files)`."""
    events = []
    for n, stmt in STATEMENTS:
        s = re.sub(r"^(if|elif)\s+", "", stmt.strip())
        if re.match(r"^\w+\(\)\s*\{", s):
            continue
        words = _words(s)
        if not words:
            continue
        head, args = words[0], words[1:]
        if head == "stop_web" and args[:1] == ["$REH_WEB"]:
            events.append((n, "stop", [_ev_name(a) for a in args[2:3]]))
        elif head == "kill_web" and args[:1] == ["$REH_WEB"]:
            events.append((n, "kill", [_ev_name(a) for a in args[1:2]]))
        elif head == "start_stopped" and args[:1] == ["$REH_WEB"]:
            events.append((n, "start", []))
        elif head == "start_web" and args[:1] == ["$REH_WEB"]:
            events.append((n, "start_web", args[1:]))
        elif head == "restart_record":
            events.append((n, "record", [_ev_name(a) for a in args[1:3]]))
        elif head == "p3_record":
            events.append((n, "p3", [_ev_name(a) for a in args[0:2]]))
    return events


def test_every_stop_of_reh_web_records_its_state():
    """`stop_web "$REH_WEB"` without a state file is a stop whose kind
    nothing can read: the stop before the way back, the way back's own,
    and the one after a run that did not flip were that.
    Kills: "a stop of reh-web records no state" (F7's included), "two stops
    share one file"."""
    stops = [(n, files) for n, kind, files in _state_events() if kind in ("stop", "kill")]
    assert len(stops) >= 7, stops
    missing = [n for n, files in stops if not files or not files[0]]
    assert not missing, f"stops of reh-web with no state file, lines {missing}"
    names = [files[0] for _n, files in stops]
    assert len(set(names)) == len(names), names
    assert all(name.endswith("_state.json") for name in names), names


def _fed_stop(events, at: int) -> Optional[str]:
    """The state file of the last stop before the last start before line `at`."""
    start = max((n for n, kind, _f in events if kind == "start" and n < at), default=None)
    if start is None:
        return None
    stops = [f[0] for n, kind, f in events if kind in ("stop", "kill") and n < start]
    return stops[-1] if stops else None


def test_each_stop_state_feeds_the_record_and_the_judge_it_belongs_to(tmp_path):
    """Each restart record carries the state of the stop just before its
    start, P3 the state of the kill, and each judge reads the stop its phase
    made: D1 F7's and phase 0's, P8 the two around the way back — and D1
    those two as well, since its DELETE comes after them. Checked by
    the script's order of statements and then by `assemble` itself, over
    files that name themselves.
    Kills: "p3.json is fed another stop than the kill", "a restart record is
    fed another stop than its own", "a judge reads another phase's stop"."""
    import json

    events = _state_events()
    records = [(n, f) for n, kind, f in events if kind == "record"]
    assert [f[0] for _n, f in records] == ["p6_restart1.json", "p6_restart2.json",
                                           "p6_restart3.json"], records
    for n, (out, fed) in records:
        assert fed == _fed_stop(events, n), (n, out, fed, _fed_stop(events, n))
    kills = [(n, f[0]) for n, kind, f in events if kind == "kill"]
    assert len(kills) == 1, kills
    p3 = [(n, f) for n, kind, f in events if kind == "p3"]
    assert p3 and all(f == ["p3.json", kills[0][1]] and n > kills[0][0] for n, f in p3), p3
    assert dict((f[0], f[1]) for _n, f in records)["p6_restart2.json"] == kills[0][1]

    stops = [(n, f[0]) for n, kind, f in events if kind in ("stop", "kill")]
    starts = {" ".join(f): n for n, kind, f in events if kind == "start_web"}
    f1 = starts["-e KS_WRITE_WAREHOUSE=postgres -e KS_READ_FALLBACK=off"]
    way_back = starts["-e KS_READ_FALLBACK=off"]
    expect = {
        ("D1", "p0_stop"): [s for n, s in stops if n < f1][-1],
        ("D1", "f7_stop"): dict((f[0], f[1]) for _n, f in records)["p6_restart3.json"],
        ("P8", "pre_stop"): [s for n, s in stops if n < way_back][-1],
        ("P8", "stop"): [s for n, s in stops if n > way_back][0],
        # The end's DELETE comes after the way back: D1 reads the same two.
        ("D1", "b_pre_stop"): [s for n, s in stops if n < way_back][-1],
        ("D1", "b_stop"): [s for n, s in stops if n > way_back][0],
    }
    assert expect[("D1", "p0_stop")] == "p0_stop_state.json"
    for _n, name in stops:
        (tmp_path / name).write_text(json.dumps({"marker": name}))
    ev = PROBE.assemble(tmp_path)
    for (judge, key), name in expect.items():
        assert (ev[judge].get(key) or {}).get("marker") == name, (judge, key, ev[judge].get(key))
    (tmp_path / "p3.json").write_text(json.dumps(
        {"seen_blocked": True, "stop": {"marker": kills[0][1]}}))
    ev = PROBE.assemble(tmp_path)
    assert ev["D1"]["kill_stop"] == ev["P6"]["kill_stop"] == {"marker": kills[0][1]}


def _evidence_writes() -> List[str]:
    """Every evidence file the script writes, `$k` and the like as `*`."""
    out = []
    for _n, stmt in STATEMENTS:
        for m in re.finditer(r'(?:>|\btee|\bcp\s+"[^"]*")\s+"\$EV/([^"]+)"', stmt):
            out.append(m.group(1))
    for _n, kind, files in _state_events():
        if kind in ("stop", "kill"):
            out.extend(f for f in files[:1] if f)
        elif kind == "record":
            out.extend(f for f in files[:1] if f)
        elif kind == "p3":
            out.extend(f for f in files[:1] if f)
    return [re.sub(r"\$\{?\w+\}?", "*", name) for name in out]


def _assemble_reads() -> List[str]:
    import ast

    tree = ast.parse((REPO / "deploy" / "step13_rehearsal" / "probe.py").read_text())
    fn = next(node for node in ast.walk(tree)
              if isinstance(node, ast.FunctionDef) and node.name == "assemble")
    return sorted({node.value for node in ast.walk(fn)
                   if isinstance(node, ast.Constant) and isinstance(node.value, str)
                   and re.fullmatch(r"[\w*]+\.(json|log)", node.value)})


def test_every_evidence_file_the_judges_read_is_one_the_script_writes():
    """A judge reading a file nobody writes judges None, which reads as
    "the container's state was not read" — what F7's stop recording no
    state would have looked like, a run of UNKNOWNs nobody could trace.
    `frozen_samples.json` is the one file a caller supplies instead of the
    snapshots, and the script never does.
    Kills: "a state file a judge reads is no longer written"."""
    import fnmatch

    writes = _evidence_writes()
    reads = [r for r in _assemble_reads() if r != "frozen_samples.json"]
    assert {"f7_stop_state.json", "p0_stop_state.json", "b_pre_stop_state.json",
            "b_stop_state.json", "p3.json", "d1_window.json", "d1_delete.json"} <= set(reads)
    unwritten = [r for r in reads
                 if not any(fnmatch.fnmatch(w, r) or fnmatch.fnmatch(r, w) for w in writes)]
    assert not unwritten, unwritten


def _statement_writing(name: str) -> str:
    found = [s for _n, s in STATEMENTS if f'> "$EV/{name}"' in s]
    assert len(found) == 1, found
    return found[0]


@pytest.mark.parametrize("win_run, expect", [("41", 41), ("", None)])
def test_d1_window_carries_the_run_the_order_and_the_status(tmp_path, win_run, expect):
    """The ids D1 asks of, as the script writes them and `assemble` reads
    them: F5w's run (null when it wrote none, which D1 reads as UNKNOWN),
    F6's order and its fourth version's status.
    Kills: "d1_window.json is written without the window's run"."""
    import os
    import subprocess

    stmt = _statement_writing("d1_window.json")
    env = {**os.environ, "EV": str(tmp_path), "WIN_RUN": win_run, "FIX_ID": "900500",
           "V4_STATUS": "9"}
    proc = subprocess.run(["bash", "-c", f"set -Eeuo pipefail\n{stmt}"], env=env,
                          capture_output=True, text=True, timeout=30)
    assert proc.returncode == 0, proc.stderr
    assert PROBE.assemble(tmp_path)["D1"]["window_ids"] == {
        "run_id": expect, "order_id": 900500, "order_status": 9}


def _block_with(needle: str) -> str:
    """The `if [ "$KEEP" = 1 ]; then … fi` holding `needle`, as written."""
    lines = TEXT.splitlines()
    at = next(i for i, l in enumerate(lines) if needle in l)
    start = max(i for i in range(at) if lines[i].strip() == 'if [ "$KEEP" = 1 ]; then')
    depth = 0
    for i in range(start, len(lines)):
        s = lines[i].strip()
        if re.match(r"if\s", s) and s.endswith("; then"):
            depth += 1
        elif s == "fi":
            depth -= 1
            if depth == 0:
                return "\n".join(lines[start:i + 1])
    raise AssertionError("unbalanced block")


@pytest.mark.parametrize("keep, killed, deletes", [
    ("1", "1", False),   # --keep: the copy stays as it is
    ("0", "1", True),    # a kill to ask about, a copy about to go
    ("0", "0", False),   # nothing killed: nothing to ask
])
def test_keep_leaves_the_copy_undeleted(tmp_path, keep, killed, deletes):
    """`--keep` promises the copy left in place, and D1 UNKNOWN for it: the
    end's DELETEs change the copy and end in DuckDB's FATAL on a loss.
    Run as the script runs it, against a `probe_offline` that records.
    Kills: "--keep runs the window's DELETEs", "the DELETEs run without a
    kill"."""
    import json
    import os
    import subprocess

    block = _block_with(" window-delete ")
    body = f"""set -Eeuo pipefail
probe_offline() {{ echo "$*" >> "$EV/calls"; echo '{{"deletes": []}}'; }}
{block}
"""
    env = {**os.environ, "EV": str(tmp_path), "LOG_DIR": str(tmp_path), "KEEP": keep,
           "KILLED": killed, "FIX_ID": "900500", "WIN_RUN": "41"}
    proc = subprocess.run(["bash", "-c", body], env=env, capture_output=True, text=True,
                          timeout=30)
    assert proc.returncode == 0, proc.stderr
    calls = (tmp_path / "calls").read_text().splitlines() if (tmp_path / "calls").exists() else []
    assert bool(calls) is deletes, calls
    if deletes:
        assert calls[0].startswith("window-delete ") and "--window-order 900500" in calls[0]
    done = tmp_path / "d1_delete.json"
    if keep == "1":
        record = json.loads(done.read_text())
        assert record == {"skipped": "--keep leaves the copy as it is"}
        verdict, detail = PROBE.judge_d1({"flipped": True, "seen_blocked": True,
                                          "kill_stop": {"running": False, "exit_code": 137,
                                                        "oom_killed": False, "asked": "kill",
                                                        "was_running": True},
                                          "pre": {"indexes": {}}, "post": {"indexes": {}},
                                          "delete": record})
        assert verdict == "UNKNOWN" and "--keep" in detail, detail
    elif not deletes:
        assert not done.exists()


def _judge_section() -> str:
    """The verdict step's judge call and what it keeps when the judge fails."""
    text = SCRIPT.read_text(encoding="utf-8")
    start = text.index("JUDGE_RC=0\n")
    end = text.index("\nrender() {", start)
    return text[start:end]


@pytest.mark.parametrize("stdout, stderr, rc, kept, reason", [
    ("", "Traceback (most recent call last):\nKeyError: 'kill_stop'\n", 1, True,
     "exit 1: KeyError: 'kill_stop'"),
    ("", "", 137, True, "exit 137: killed"),
    ("P1 x|PASS|ok\n", "ValueError: half way\n", 1, True, "ValueError: half way"),
    ("P1 x|PASS|ok\n", "", 0, False, None),
])
def test_a_failed_judge_keeps_its_reason_and_its_evidence(tmp_path, stdout, stderr, rc,
                                                          kept, reason):
    """2026-10-08: the judge failed on the host, the row said "see
    probe.err", and the cleanup deleted that file with the run's directory —
    an hour of rehearsal with nothing to read. The reason now goes into the
    row, the evidence and stderr are kept beside the report (root's alone),
    and the row prints the command that judges them again. A judge that
    answered keeps nothing. Kills: "drop the cp of the evidence", "the row
    without the reason", "keep on success"."""
    import os
    import stat
    import subprocess

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(
        "#!/bin/sh\n"
        f"printf '%s' '{stdout}'\n"
        f"printf '%s' \"$JUDGE_STDERR\" >&2\n"
        f"exit {rc}\n")
    (bin_dir / "docker").chmod(0o755)
    ev, logs, report_dir = tmp_path / "ev", tmp_path / "logs", tmp_path / "root"
    for d in (ev, logs, report_dir):
        d.mkdir()
    (ev / "p3.json").write_text('{"seen_blocked": true}')
    body = f"""set -Eeuo pipefail
REH_PROBE=reh-probe; REH_IMAGE=img:prod; HELPER_DIR=/opt/x/deploy/step13_rehearsal
FLOOR_S=120; STAMP=20261008-101600
EV={ev}; LOG_DIR={logs}; REPORT_DIR={report_dir}
{_judge_section()}
printf '%s\\n' "$ROWS"
"""
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "JUDGE_STDERR": stderr}
    proc = subprocess.run(["bash", "-c", body], env=env, capture_output=True, text=True,
                          timeout=30)
    assert proc.returncode == 0, proc.stderr
    rows = proc.stdout
    keep_dir = report_dir / "step13-rehearsal-20261008-101600-evidence"
    assert keep_dir.is_dir() is kept, rows
    if not kept:
        assert rows.strip() == "P1 x|PASS|ok"
        return
    assert reason in rows, rows
    assert f"kept in {keep_dir}" in rows and "the probe's judge with --evidence /ev and --floor 120" in rows, rows
    assert (keep_dir / "p3.json").read_text() == '{"seen_blocked": true}'
    assert stat.S_IMODE(keep_dir.stat().st_mode) == 0o700
    if stdout:
        assert rows.startswith("P1 x|PASS|ok\nJUDGE|UNKNOWN|"), rows
    else:
        assert rows.startswith("ALL|UNKNOWN|the judge did not run"), rows
    for line in rows.splitlines():
        assert line.count("|") == 2, f"the reason broke the table: {line}"


def test_the_judge_can_read_root_s_evidence():
    """2026-10-08 and 09.10: both rehearsals ran every phase and lost the
    verdict to `PermissionError: '/ev/f1_snapshot.json'` — $EV is root's and
    700, the image runs as appuser. The judge runs as root, still with no
    network and the evidence read-only, and the kept run's command says so.
    Kills: "drop --user 0", "the re-judge line without it"."""
    (judge,) = [words for _n, words in _runs() if "judge" in words]
    assert _flag(judge, "--user") == "0"
    assert _flag(judge, "--network") == "none"
    assert '"$EV:/ev:ro"' in judge or "$EV:/ev:ro" in judge
    assert "as --user 0" in _judge_section()


def test_a_run_removes_the_evidence_an_earlier_judge_kept_and_nothing_else(tmp_path):
    """One kept directory at most: the next run's start removes it, and only
    it — not the reports, not the run log. Kills: "the sweep removed"."""
    import os
    import subprocess

    text = SCRIPT.read_text(encoding="utf-8")
    start = text.index('for kept in "$REPORT_DIR"/step13-rehearsal-*-evidence; do')
    loop = text[start:text.index("done\n", start) + len("done\n")]
    (tmp_path / "step13-rehearsal-20261008-101600-evidence").mkdir()
    (tmp_path / "step13-rehearsal-20261008-101600-evidence" / "p3.json").write_text("{}")
    (tmp_path / "step13-rehearsal-20261008-101600.txt").write_text("table")
    (tmp_path / "step13-rehearsal-run-20261008.log").write_text("log")
    proc = subprocess.run(["bash", "-c", f"set -Eeuo pipefail\nREPORT_DIR={tmp_path}\n{loop}"],
                          env=dict(os.environ), capture_output=True, text=True, timeout=30)
    assert proc.returncode == 0, proc.stderr
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "step13-rehearsal-20261008-101600.txt", "step13-rehearsal-run-20261008.log"]


def test_the_sweep_survives_when_there_is_nothing_to_sweep(tmp_path):
    """An unmatched glob is the literal pattern; under `set -e` the sweep
    must not end the run before it began."""
    import os
    import subprocess

    text = SCRIPT.read_text(encoding="utf-8")
    start = text.index('for kept in "$REPORT_DIR"/step13-rehearsal-*-evidence; do')
    loop = text[start:text.index("done\n", start) + len("done\n")]
    proc = subprocess.run(["bash", "-c", f"set -Eeuo pipefail\nREPORT_DIR={tmp_path}\n{loop}echo after"],
                          env=dict(os.environ), capture_output=True, text=True, timeout=30)
    assert proc.returncode == 0 and proc.stdout.strip() == "after", proc.stderr
