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
CONTAINER_HELPERS = ("start_web", "running", "oom_killed", "wait_health", "save_log", "stop_web")


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
