"""The /api request log nginx keeps on the host, and the bound that keeps it small.

nginx writes one JSON line per HTTPS request under /api/ into a directory on
the host (``nginx/nginx.conf``, ``ks_api``), so the lines survive a recreate of
the container, which ``docker logs`` does not. nginx cannot rotate a file, so
the bound is a service of its own, ``api-log`` in docker-compose.yml, running
``nginx/api-log/rotate.sh`` every five minutes.

The pieces live in three files and come apart silently: a log path nobody
rotates grows for ever, a format whose first field moved leaves the script
unable to cut by day, and a location with an ``access_log`` of its own drops
its requests from this log without a word. So these tests hold the pieces to
each other, read the format the way a reader of the log will, and run the
script itself.

What they cannot do is run nginx: there is none here. How the format behaves
(query string cut off, a 502 keeping its own path, plain HTTP and non-API
requests left out) was checked against nginx:alpine by hand before this
landed. What is held here is the text that behaviour comes from.
"""
from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
NGINX_DIR = REPO / "nginx"
NGINX = NGINX_DIR / "nginx.conf"
ROTATE = NGINX_DIR / "api-log" / "rotate.sh"
COMPOSE_FILE = REPO / "docker-compose.yml"

# nginx writes these unquoted, so each must be a number on every request.
# With `escape=json` a variable that is not set is written as nothing at all,
# which would leave `"x":,` behind: only variables nginx sets on every request
# may stand bare.
ALWAYS_NUMERIC = {"status", "body_bytes_sent", "request_time"}

# What the log must never carry. The address and the user agent say who; the
# query string is what they typed (a search for a phone number); $request is
# the request line with its query; $uri is the path after `error_page` has
# rewritten it, so every request answered by the 502 page would read
# /error.html.
FORBIDDEN = {
    "remote_addr", "realip_remote_addr", "http_x_forwarded_for",
    "proxy_add_x_forwarded_for", "http_user_agent", "http_cookie",
    "http_referer", "remote_user", "request", "request_uri", "args",
    "query_string", "uri", "document_uri",
}


def _conf() -> str:
    assert NGINX.exists(), (
        f"{NGINX} is not here. The runtime image does not carry `nginx/`, so "
        f"`deploy/gate_with_stores.sh` has to mount it."
    )
    return NGINX.read_text(encoding="utf-8")


def _services() -> dict:
    return yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))["services"]


def _api_access_log() -> tuple[str, str]:
    """(path, condition) of the one `access_log` that writes `ks_api`."""
    found = re.findall(
        r"^\s*access_log\s+(\S+)\s+ks_api\s+if=\$(\w+);", _conf(), re.MULTILINE
    )
    assert len(found) == 1, f"expected one `access_log … ks_api if=…`, found {found}"
    return found[0]


def _log_format(name: str) -> str:
    match = re.search(
        rf"log_format\s+{name}\s+escape=json\s+((?:'[^']*'\s*)+);", _conf()
    )
    assert match, f"no `log_format {name} escape=json` in nginx.conf"
    return "".join(re.findall(r"'([^']*)'", match.group(1)))


def _script_default(name: str) -> str:
    match = re.search(
        rf'^{name}="\$\{{KS_API_LOG_{name}:-([^}}]+)\}}"', ROTATE.read_text(), re.MULTILINE
    )
    assert match, f"rotate.sh declares no default for {name}"
    return match.group(1)


def _blocks(text: str) -> list[list[str]]:
    """Each block's own directives, whitespace folded: the file's top level
    (the block it is included into) and every `{ … }`, nested ones apart.
    Quotes are honoured, since the log format itself is full of braces."""
    text = re.sub(r"^\s*#.*$|\s#.*$", "", text, flags=re.MULTILINE)
    stack: list[list[str]] = [[]]
    blocks: list[list[str]] = []
    statement, quote = "", ""
    for ch in text:
        if quote:
            statement += ch
            quote = "" if ch == quote else quote
        elif ch in "'\"":
            statement, quote = statement + ch, ch
        elif ch == "{":
            stack.append([])
            statement = ""
        elif ch == "}":
            blocks.append(stack.pop())
            statement = ""
        elif ch == ";":
            stack[-1].append(" ".join(statement.split()))
            statement = ""
        else:
            statement += ch
    blocks.append(stack.pop())
    return blocks


def _host_side(volumes: list[str], container_dir: str) -> str | None:
    for volume in volumes:
        host, _, rest = volume.partition(":")
        if rest.split(":")[0] == container_dir:
            return host
    return None


class TestTheBoundWatchesTheFileNginxWrites:
    def test_nginx_writes_into_a_directory_on_the_host(self):
        path, _ = _api_access_log()
        host = _host_side(_services()["nginx"]["volumes"], os.path.dirname(path))
        assert host, f"nothing is mounted where nginx writes {path}"
        assert host.startswith("/"), (
            f"{host} is not a host path. Inside the container the lines go with "
            f"every recreate, which is what this log exists to outlive."
        )

    def test_the_bound_mounts_the_same_directory_at_the_same_place(self):
        path, _ = _api_access_log()
        log_dir = os.path.dirname(path)
        services = _services()
        assert "api-log" in services, "the log has no bound: no `api-log` service"
        bound = services["api-log"]
        assert _host_side(bound["volumes"], log_dir) == _host_side(
            services["nginx"]["volumes"], log_dir
        ), "the bound rotates a directory nginx does not write"
        assert _script_default("DIR") == log_dir
        assert os.path.basename(path) == "api.log", "rotate.sh cuts `$DIR/api.log`"

    def test_the_script_runs_on_a_loop_that_outlives_a_failed_pass(self):
        bound = _services()["api-log"]
        assert bound["restart"] == "unless-stopped"
        # nginx's image stops with SIGQUIT, which the shell outlives: without
        # these two every stop waited out Docker's ten seconds.
        assert bound["init"] is True
        assert bound["stop_signal"] == "SIGTERM"
        loop = " ".join(bound["command"])
        assert "sh /opt/api-log/rotate.sh ||" in loop, (
            "a pass that fails must not end the loop, and with it the bound"
        )
        assert "sleep 300" in loop

    def test_the_script_is_mounted_as_a_directory(self):
        """A single-file mount keeps the inode it was given, and `git reset
        --hard` writes a new one: the service would go on running the script
        from before the deploy. A directory mount sees the new file, and the
        loop reads it afresh on every pass."""
        volumes = _services()["api-log"]["volumes"]
        assert "./nginx/api-log:/opt/api-log:ro" in volumes


class TestTheBound:
    def test_it_is_finite_and_covers_a_month(self):
        """62 cuts of at most 32 MiB. A month is the floor: a lever run by hand
        once a month must show up, and a Python route is kept for a month
        after its switch to Rust, with its traffic in view."""
        keep = int(_script_default("KEEP"))
        max_bytes = int(_script_default("MAX_BYTES"))
        assert keep >= 31
        assert keep * max_bytes <= 4 * 2**30, "the bound outgrew what the disk can give"


class TestTheFormat:
    def test_the_day_comes_first(self):
        """rotate.sh reads the day of the oldest line from its first 16 bytes."""
        assert _log_format("ks_api").startswith('{"t":"$time_iso8601"')

    def test_every_line_is_json_even_with_nothing_upstream(self):
        fmt = _log_format("ks_api")
        bare = set(re.findall(r'(?<!")\$(\w+)(?!")', fmt))
        assert bare <= ALWAYS_NUMERIC, f"written unquoted but not always a number: {bare - ALWAYS_NUMERIC}"
        for sample in ("", 'a "quoted" \\ value\n'):
            def fill(match: re.Match[str], sample: str = sample) -> str:
                if match.group(1) in ALWAYS_NUMERIC:
                    return "200"
                return json.dumps(sample)[1:-1]  # what escape=json writes

            line = re.sub(r"\$(\w+)", fill, fmt)
            assert isinstance(json.loads(line), dict), line

    def test_it_says_nothing_about_who_or_what_they_typed(self):
        used = set(re.findall(r"\$(\w+)", _log_format("ks_api")))
        assert not used & FORBIDDEN, f"the log would carry {used & FORBIDDEN}"

    def test_the_path_is_cut_out_of_the_request_line_before_the_query(self):
        match = re.search(
            r'map\s+"\$scheme \$request_uri"\s+\$ks_api_path\s*\{([^}]*)\}', _conf()
        )
        assert match, "no map cutting $ks_api_path out of $request_uri"
        body = match.group(1)
        regex = re.search(r'"~([^"]+)"\s+\$1;', body)
        assert regex, "the map no longer answers with its capture"
        assert re.search(r'default\s+"";', body), "a non-API request must map to nothing"
        assert _api_access_log()[1] == "ks_api_path", "logged only when the map matched"
        pattern = re.compile(regex.group(1))
        cases = {
            "https /api/search?q=%2B380671234567": "/api/search",
            "https /api/summary": "/api/summary",
            "https /api/stocks/abc/A?x=1": "/api/stocks/abc/A",
            "http /api/summary": None,  # the 301 to HTTPS
            "https /": None,
            "https /static-v2/assets/x.js": None,
            "https /apix": None,
        }
        for source, expected in cases.items():
            got = pattern.match(source)
            assert (got.group(1) if got else None) == expected, source

    def test_no_block_takes_its_requests_out_of_the_log(self):
        """A block that declares any `access_log` inherits none from above. One
        in a location, or in a file included into one, would leave that
        block's requests out of this log with nothing to say so. So wherever an
        `access_log` is declared, this one is declared beside it."""
        path, condition = _api_access_log()
        ours = f"access_log {path} ks_api if=${condition}"
        for conf in sorted(NGINX_DIR.rglob("*.conf")):
            for block in _blocks(conf.read_text(encoding="utf-8")):
                logs = [d for d in block if d.split()[:1] == ["access_log"]]
                assert not logs or ours in logs, (
                    f"{conf.relative_to(REPO)}: a block declares {logs} without "
                    f"`{ours};`, so the /api requests it serves drop out of the log"
                )


def _line(day: str) -> str:
    return '{"t":"%sT10:00:00+00:00","m":"GET","p":"/api/summary","s":200}\n' % day


@pytest.mark.skipif(
    not (shutil.which("sh") and shutil.which("gzip")), reason="needs sh and gzip"
)
class TestRotate:
    TODAY = "20261030"
    NOW = "20261030T000301Z"

    def _run(self, log_dir: Path, *, keep: int = 3, max_bytes: int = 10_000) -> None:
        subprocess.run(
            ["sh", str(ROTATE)],
            env={
                "PATH": os.environ["PATH"],
                "KS_API_LOG_DIR": str(log_dir),
                "KS_API_LOG_KEEP": str(keep),
                "KS_API_LOG_MAX_BYTES": str(max_bytes),
                "KS_API_LOG_TODAY": self.TODAY,
                "KS_API_LOG_NOW": self.NOW,
            },
            check=True,
            timeout=30,
        )

    @staticmethod
    def _cuts(log_dir: Path) -> list[str]:
        return sorted(p.name for p in log_dir.glob("api-*"))

    def test_todays_file_is_left_alone(self, tmp_path):
        (tmp_path / "api.log").write_text(_line("2026-10-30") * 3)
        self._run(tmp_path)
        assert (tmp_path / "api.log").read_text() == _line("2026-10-30") * 3
        assert self._cuts(tmp_path) == []

    def test_a_new_day_cuts_the_old_one_whole(self, tmp_path):
        log = tmp_path / "api.log"
        old = _line("2026-10-29") * 5
        log.write_text(old)
        inode = log.stat().st_ino
        self._run(tmp_path)
        assert self._cuts(tmp_path) == [f"api-{self.NOW}.log.gz"]
        assert gzip.decompress((tmp_path / f"api-{self.NOW}.log.gz").read_bytes()).decode() == old
        assert log.read_text() == ""
        assert log.stat().st_ino == inode, (
            "the file nginx holds open must stay the same file: truncated, never renamed"
        )

    def test_past_the_size_it_cuts_the_same_day(self, tmp_path):
        (tmp_path / "api.log").write_text(_line("2026-10-30") * 200)
        self._run(tmp_path, max_bytes=1_000)
        assert self._cuts(tmp_path) == [f"api-{self.NOW}.log.gz"]
        assert (tmp_path / "api.log").read_text() == ""

    def test_a_head_it_cannot_read_cuts_by_size_alone(self, tmp_path):
        """Read as "not today", an unreadable first line would cut on every
        pass and spend the whole window in a few hours."""
        log = tmp_path / "api.log"
        log.write_text('{"m":"GET","t":"2026-10-01"}\n')
        self._run(tmp_path)
        assert self._cuts(tmp_path) == []
        log.write_text('{"m":"GET","t":"2026-10-01"}\n' * 500)
        self._run(tmp_path, max_bytes=1_000)
        assert self._cuts(tmp_path) == [f"api-{self.NOW}.log.gz"]

    def test_only_the_newest_cuts_survive(self, tmp_path):
        for day in range(1, 6):
            (tmp_path / f"api-2026100{day}T000301Z.log.gz").write_bytes(gzip.compress(b"x\n"))
        (tmp_path / "api.log").write_text(_line("2026-10-29"))
        self._run(tmp_path, keep=3)
        assert self._cuts(tmp_path) == [
            "api-20261004T000301Z.log.gz",
            "api-20261005T000301Z.log.gz",
            f"api-{self.NOW}.log.gz",
        ]

    def test_a_pass_stopped_half_way_loses_nothing(self, tmp_path):
        """A partial copy goes (api.log still has every line of it); a cut that
        was made and never compressed is compressed."""
        (tmp_path / "api.log").write_text(_line("2026-10-30"))
        (tmp_path / "api-20261029T000301Z.log.tmp").write_text("partial")
        (tmp_path / "api-20261028T000301Z.log").write_text(_line("2026-10-27"))
        self._run(tmp_path)
        assert self._cuts(tmp_path) == ["api-20261028T000301Z.log.gz"]
        assert gzip.decompress(
            (tmp_path / "api-20261028T000301Z.log.gz").read_bytes()
        ).decode() == _line("2026-10-27")
        assert (tmp_path / "api.log").read_text() == _line("2026-10-30")

    def test_it_keeps_the_directory_to_root(self, tmp_path):
        log = tmp_path / "api.log"
        log.write_text(_line("2026-10-29"))
        log.chmod(0o644)
        tmp_path.chmod(0o755)
        self._run(tmp_path)
        assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o750
        assert stat.S_IMODE(log.stat().st_mode) == 0o640
        assert stat.S_IMODE((tmp_path / f"api-{self.NOW}.log.gz").stat().st_mode) == 0o640

    def test_no_directory_is_not_a_failure(self, tmp_path):
        """Before nginx's first start under the mount there is nothing to bound."""
        self._run(tmp_path / "missing")
