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


def test_every_container_command_targets_a_rehearsal_container():
    allowed = {f"${v}" for v in CONTAINER_VARS} | {"$1", "$name"}
    takes_value = {"-f", "--format", "-e", "-t", "--since", "--until", "-u", "--time", "-s"}
    checked = 0
    for n, sub, words in DOCKER:
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
