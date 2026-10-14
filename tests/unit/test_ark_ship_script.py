"""deploy/ark_ship.sh, the second Ark's shipment, actually run.

PR-11 of the stage-5 plan, and its checklist line: the Ark is taken from the
static file, shipped off-site, its checksum verified after download, and its
restore verified at level 2 on the downloaded copy. Each of those is a
sentence about what happens to bytes, so each is checked by running the
script, not by reading it.

The script is copied into a scratch repository root and run against fakes on
PATH, as tests/unit/test_pg_offsite_scripts.py runs the Postgres shipper —
and with that file's `sftp`, `gpg` and `flock`, because the transport is the
same library. `docker` is the one fake of its own, and it is not much of a
fake: it maps the container paths back to the host and runs the real
deploy/ark_freeze.py with this interpreter's DuckDB. So every freeze here is a
real freeze of a real DuckDB file, and every L0-L2 verification is real —
including the one on the copy fetched back from the scratch Storage Box.

What a fake cannot prove is that real gpg round-trips the archive;
`test_real_gpg_end_to_end` does, where gpg is installed — CI's ubuntu runner,
not most laptops and not the gate's slim image.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from tests.unit.test_pg_offsite_scripts import FAKE_FLOCK, FAKE_GPG, FAKE_SFTP

duckdb = pytest.importorskip("duckdb")

REPO = Path(__file__).resolve().parents[2]
DEPLOY = REPO / "deploy"
SCRIPT = DEPLOY / "ark_ship.sh"

pytestmark = pytest.mark.skipif(
    not all(shutil.which(t) for t in ("bash", "awk", "tar", "gzip", "diff", "cmp")),
    reason="needs bash, awk, tar, gzip, diff and cmp")

STAMP_RE = re.compile(r"^\d{8}T\d{6}Z$")
PHONE = "0501234567"
FIRST_ARK = "ark-20260823T212918Z.tar.gz"
OLDER = ["20260901T000000Z", "20260902T000000Z", "20260903T000000Z"]

# Runs the real ark_freeze.py with this interpreter, the container's paths
# mapped back to the host's, under the umask a real container starts with. It
# records every call, and three knobs stand in for what a real container can
# do to the run: no image on the host, a writer touching the source while it
# is frozen, and a verification that fails on one path only — the downloaded
# copy's, which nothing else can make fail without breaking a check that runs
# before it.
FAKE_DOCKER = r'''#!{python}
import os, subprocess, sys

args = sys.argv[1:]
with open(os.environ["FAKE_DOCKER_CALLS"], "a") as fh:
    fh.write(" ".join(args) + "\n")
if args[:2] == ["image", "inspect"]:
    sys.exit(1 if os.environ.get("FAKE_DOCKER_NO_IMAGE") else 0)
if not args or args[0] != "run":
    sys.exit(0)

VALUED = {"--pull", "--network", "--user", "--oom-score-adj", "--memory",
          "--memory-swap", "--log-opt", "--entrypoint", "-v", "-e", "-w", "--name"}
mounts, entry, i = [], None, 1
while i < len(args):
    a = args[i]
    if a in VALUED:
        if a == "-v":
            src, dst = args[i + 1].split(":")[:2]
            mounts.append((src, dst))
        if a == "--entrypoint":
            entry = args[i + 1]
        i += 2
        continue
    if a.startswith("-"):
        i += 1
        continue
    break
cmd = ([entry] if entry else []) + args[i + 1:]

def host(p):
    for src, dst in sorted(mounts, key=lambda m: -len(m[1])):
        if p == dst or p.startswith(dst + "/"):
            return src + p[len(dst):]
    return p

cmd = [host(c) for c in cmd]
if cmd and cmd[0] in ("python", "python3"):
    cmd[0] = sys.executable
if "--verify" in cmd:
    under = os.environ.get("FAKE_DOCKER_FAIL_VERIFY_UNDER")
    if under and under in cmd[cmd.index("--verify") + 1]:
        print("fake docker: verification refused", file=sys.stderr)
        sys.exit(1)
# A container starts with the runtime's umask, 0022, and never the caller's:
# the script's own `umask 077` does not reach anything the freeze writes.
os.umask(0o022)
rc = subprocess.call(cmd)
if os.environ.get("FAKE_DOCKER_TOUCH_SOURCE") and "--source" in cmd:
    with open(cmd[cmd.index("--source") + 1], "ab") as fh:
        fh.write(b"\0a writer was here")
sys.exit(rc)
'''


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _warehouse(path: Path) -> None:
    """A small DuckDB file with every kind of object the Ark has to carry: a
    sequence a default reads, constraints, an index, and a view over a view."""
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SEQUENCE seq_orders START 1")
    conn.execute("""CREATE TABLE orders (
        id INTEGER PRIMARY KEY, phone VARCHAR NOT NULL,
        grand_total DECIMAL(12, 2) DEFAULT 0,
        n INTEGER DEFAULT nextval('seq_orders'))""")
    conn.execute(f"INSERT INTO orders (id, phone, grand_total) VALUES "
                 f"(1, '{PHONE}', 1200.50), (2, '0679999999', 300)")
    conn.execute("CREATE TABLE sync_metadata (key VARCHAR PRIMARY KEY, value VARCHAR)")
    conn.execute("INSERT INTO sync_metadata VALUES ('last_sync_orders', '2026-10-27')")
    conn.execute("CREATE INDEX idx_orders_phone ON orders (phone)")
    conn.execute("CREATE VIEW v_orders AS SELECT id, grand_total FROM orders")
    conn.execute("CREATE VIEW v_revenue AS SELECT SUM(grand_total) AS r FROM v_orders")
    conn.close()


class Run:
    def __init__(self, done, world):
        self.code = done.returncode
        self.out = done.stdout + done.stderr
        self.world = world

    def _calls(self, name):
        path = self.world.root / f"calls-{name}.log"
        return path.read_text().splitlines() if path.exists() else []

    @property
    def docker(self):
        return self._calls("docker")

    @property
    def docker_runs(self):
        return [c for c in self.docker if c.startswith("run ")]

    @property
    def sftp(self):
        return self._calls("sftp")

    @property
    def gpg(self):
        return self._calls("gpg")


def _make_world(tmp_path, *, real_gpg=False):
    # Resolved, because the script resolves every path it is given (`pwd -P`)
    # and on macOS the temporary directory is a symlink into /private.
    tmp_path = tmp_path.resolve()
    root = tmp_path / "repo"
    (root / "deploy").mkdir(parents=True)
    for name in ("ark_ship.sh", "pg_offsite_lib.sh", "ark_freeze.py"):
        shutil.copy(DEPLOY / name, root / "deploy" / name)
    (root / "data").mkdir()
    source = root / "data" / "analytics.duckdb"
    _warehouse(source)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fakes = {"sftp": FAKE_SFTP, "flock": FAKE_FLOCK,
             "docker": FAKE_DOCKER.replace("{python}", sys.executable)}
    if not real_gpg:
        fakes["gpg"] = FAKE_GPG
    for name, body in fakes.items():
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)

    remote = tmp_path / "storagebox"
    remote.mkdir()
    passfile = tmp_path / "passfile"
    passfile.write_text("not-the-real-one\n")
    (tmp_path / "key").write_text("")
    arks = tmp_path / "arks"
    (root / "deploy" / "backup.env").write_text(
        f"BACKUP_REMOTE=u1-sub1@box.example\n"
        f"BACKUP_SSH_KEY={tmp_path}/key\n"
        f"BACKUP_ENV_PASSFILE={passfile}\n"
        f"BACKUP_REMOTE_DIR=key-api-bot\n"
        f"BACKUP_ARK_DIR={arks}\n"
    )

    class World:
        def __init__(self):
            self.tmp = tmp_path
            self.root = root
            self.source = source
            self.arks = arks
            self.passfile = passfile
            self.bin = bin_dir
            self.remote_root = remote
            self.remote = remote / "key-api-bot" / "ark"
            self.pg_remote = remote / "key-api-bot" / "postgres"

        def run(self, *args, cwd=None, **env_extra):
            env = {k: v for k, v in os.environ.items()
                   if not k.startswith(("FAKE_", "BACKUP_", "KS_"))}
            env.update({
                "PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
                "HOME": str(tmp_path),
                "FAKE_REMOTE_ROOT": str(remote),
                "FAKE_SFTP_CALLS": str(root / "calls-sftp.log"),
                "FAKE_GPG_CALLS": str(root / "calls-gpg.log"),
                "FAKE_DOCKER_CALLS": str(root / "calls-docker.log"),
            })
            env.update({k: str(v) for k, v in env_extra.items()})
            for name in ("sftp", "gpg", "docker"):
                (root / f"calls-{name}.log").unlink(missing_ok=True)
            done = subprocess.run(
                ["bash", str(root / "deploy" / "ark_ship.sh"), *map(str, args)],
                cwd=cwd or root, env=env, capture_output=True, text=True, timeout=300)
            return Run(done, self)

        def ark_dirs(self):
            if not self.arks.exists():
                return []
            return sorted(p for p in self.arks.iterdir()
                          if p.is_dir() and STAMP_RE.match(p.name))

        def shipped(self):
            if not self.remote.exists():
                return []
            return sorted(p.name for p in self.remote.iterdir())

        def freeze_here(self):
            """An Ark frozen in-process, so a test knows its stamp before the
            script runs — the fake remote corrupts uploads by final name."""
            return _load_ark_freeze().freeze(self.source, self.arks)

        def seed_remote(self, *stamps, extras=(), manifest=True):
            self.remote.mkdir(parents=True, exist_ok=True)
            for stamp in stamps:
                (self.remote / f"ark-{stamp}.tar.gz.gpg").write_text("old")
                if manifest:
                    (self.remote / f"ark-{stamp}.sha256").write_text("old")
            for extra in extras:
                (self.remote / extra).write_text("not ours")

    return World()


@pytest.fixture
def world(tmp_path):
    return _make_world(tmp_path)


@pytest.fixture(scope="module")
def healthy(tmp_path_factory):
    """One healthy shipment, read by every test that only asks what it did:
    each run is three interpreters importing DuckDB, and nine of them would
    be the same run."""
    world = _make_world(tmp_path_factory.mktemp("healthy"))
    run = world.run("--source", "data/analytics.duckdb")
    assert run.code == 0, run.out
    return world, run


# ── the healthy run ───────────────────────────────────────────────────────────

class TestTheShipment:
    def test_a_healthy_run_freezes_ships_and_verifies(self, healthy):
        world, run = healthy
        assert "ARK SHIPPED AND VERIFIED" in run.out

        [ark] = world.ark_dirs()
        names = world.shipped()
        assert names == [f"ark-{ark.name}.sha256", f"ark-{ark.name}.tar.gz.gpg"], names
        # The manifest is sha256sum's own format, of the ciphertext — what
        # travels, and what can be checked without the passphrase.
        manifest = (world.remote / f"ark-{ark.name}.sha256").read_text()
        cipher = world.remote / f"ark-{ark.name}.tar.gz.gpg"
        assert manifest == f"{_sha256(cipher)}  ark-{ark.name}.tar.gz.gpg\n"
        assert cipher.read_bytes().startswith(b"FAKEGPG\n")
        # The local Ark is a whole one, and it is where it was told to go.
        for rel in ("manifest.json", "schema.sql", "RESTORE.md",
                    "analytics.duckdb.frozen", "tables/orders.parquet",
                    "views/v_revenue.parquet"):
            assert (ark / rel).is_file(), rel

    def test_the_byte_copy_is_the_source_and_the_run_says_its_hash(self, healthy):
        """The week of silence hashes the same file from the next hour on;
        the Ark's source hash is what its first record should read."""
        world, run = healthy
        want = _sha256(world.source)
        [ark] = world.ark_dirs()
        assert _sha256(ark / "analytics.duckdb.frozen") == want
        assert re.search(rf"SOURCE RELEASED \S+: nothing below reads .* \(sha256 {want}\)",
                         run.out), run.out
        assert f"sha256 {want}" in run.out.split("ARK SHIPPED AND VERIFIED")[1]

    def test_the_source_is_released_before_anything_leaves_the_host(self, healthy):
        """Steps 4-6 of the week may start at that line, so nothing after it
        may read the source: the freeze and both hashes come before it."""
        world, run = healthy
        released = run.out.index("SOURCE RELEASED")
        assert released < run.out.index("── pack and encrypt")
        freezes = [c for c in run.docker_runs if "--source" in c]
        assert len(freezes) == 1, run.docker_runs

    def test_the_freeze_sees_the_file_alone_read_only_and_no_network(self, healthy):
        world, run = healthy
        [freeze] = [c for c in run.docker_runs if "--source" in c]
        assert f"-v {world.source}:/src/analytics.duckdb:ro" in freeze, freeze
        assert f"{world.root / 'data'}:" not in freeze, "the data directory is mounted"
        for call in run.docker_runs:
            assert "--network none" in call and "--pull never" in call, call
            assert "--env-file" not in call and " -e " not in call, call

    def test_the_downloaded_copy_is_verified_with_the_verifier_it_carries(self, healthy):
        """The plan's bar: L2 on the DOWNLOADED copy — not the one on this
        disk — run with the ark_freeze.py the archive carries, which is what a
        stranger holding only the archive will have."""
        world, run = healthy
        verifies = [c for c in run.docker_runs if "--verify" in c]
        assert len(verifies) == 2, run.docker_runs
        [ark] = world.ark_dirs()
        local, downloaded = verifies
        assert f"-v {ark}:/ark:ro" in local
        mounted = re.search(r"-v (\S+):/ark:ro", downloaded).group(1)
        script = re.search(r"-v (\S+):/ark_freeze.py:ro", downloaded).group(1)
        assert "/.ship." in mounted and mounted.endswith(f"/x/{ark.name}"), downloaded
        assert script.endswith("/x/ark_freeze.py"), downloaded
        # Real verifications ran, all three levels, twice.
        assert run.out.count("L2 schema+parquet: 2 tables replayed, 0 mismatched") == 2, run.out
        assert run.out.count("VERIFY OK") == 2, run.out

    def test_the_archive_carries_the_ark_and_its_verifier(self, healthy):
        """Decrypted by hand, as an operator in five years would."""
        world, run = healthy
        [ark] = world.ark_dirs()
        cipher = world.remote / f"ark-{ark.name}.tar.gz.gpg"
        by_hand = Path(tempfile.mkdtemp(dir=world.tmp))
        plain = by_hand / "back.tar.gz"
        plain.write_bytes(cipher.read_bytes()[len(b"FAKEGPG\n"):])
        out = by_hand / "unpacked"
        out.mkdir()
        subprocess.run(["tar", "-C", str(out), "-xzf", str(plain)], check=True)
        assert sorted(p.name for p in out.iterdir()) == [ark.name, "ark_freeze.py"]
        assert (out / "ark_freeze.py").read_bytes() == (DEPLOY / "ark_freeze.py").read_bytes()
        assert _sha256(out / ark.name / "analytics.duckdb.frozen") == _sha256(world.source)

    def test_nothing_is_left_behind_but_the_ark(self, healthy):
        """The scratch space holds a decrypted tarball and an unpacked copy of
        every phone number; it is gone when the run is."""
        world, run = healthy
        assert sorted(p.name for p in world.arks.iterdir()) == sorted(
            [d.name for d in world.ark_dirs()] + [".ark_ship.lock"]), list(world.arks.iterdir())
        assert sorted(p.name for p in (world.root / "data").iterdir()) == ["analytics.duckdb"]

    def test_the_ark_is_private_whatever_its_root(self, world):
        """The Ark is plaintext. The freeze runs in a container, whose umask is
        0022 whatever the script's is, so an Ark root that already existed
        open to everyone — anything but a root this script created — used to
        hold a 0755 Ark of 0644 files. The Ark is closed now; the root itself
        is the operator's choice and may be a directory others use, so it is
        left as it was."""
        world.arks.mkdir()
        world.arks.chmod(0o755)
        run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 0, run.out
        [ark] = world.ark_dirs()
        assert ark.stat().st_mode & 0o777 == 0o700, oct(ark.stat().st_mode)
        opened = [f"{p.relative_to(ark)} {oct(p.stat().st_mode & 0o777)}"
                  for p in ark.rglob("*") if p.stat().st_mode & 0o077]
        assert not opened, opened
        assert (ark / "tables" / "orders.parquet").stat().st_mode & 0o777 == 0o600
        assert world.arks.stat().st_mode & 0o777 == 0o755, "it changed the operator's root"

    def test_a_relative_source_is_the_callers_and_not_the_repositorys(self, world):
        """The script moves to the repository root; a path typed elsewhere
        must still mean what it meant where it was typed."""
        elsewhere = world.tmp / "elsewhere"
        elsewhere.mkdir()
        shutil.copy(world.source, elsewhere / "analytics-copy.duckdb")
        run = world.run("--source", "analytics-copy.duckdb", cwd=elsewhere)
        assert run.code == 0, run.out
        [freeze] = [c for c in run.docker_runs if "--source" in c]
        assert f"-v {elsewhere.resolve()}/analytics-copy.duckdb:/src/" in freeze, freeze


class TestEncryption:
    def test_nothing_reaches_the_remote_that_did_not_go_through_gpg(self, healthy):
        """OD-S5-6: the Ark carries every customer's phone number. The fake
        gpg leaves the bytes readable behind its marker, which is what lets
        this test see that only the marked file and its manifest went up."""
        world, run = healthy
        for path in world.remote_root.rglob("*"):
            if path.is_file():
                if path.name.endswith(".gpg"):
                    assert path.read_bytes().startswith(b"FAKEGPG\n"), path
                else:
                    assert path.name.endswith(".sha256"), path
                    assert PHONE not in path.read_text()
        [encrypt] = [c for c in run.gpg if "--symmetric" in c]
        assert re.search(r"/\.ship\.\w+/ark-\d{8}T\d{6}Z\.tar\.gz$", encrypt), encrypt

    def test_the_passphrase_travels_as_a_path_and_never_as_a_value(self, healthy):
        world, run = healthy
        secret = world.passfile.read_text().strip()
        assert len(run.gpg) == 2, run.gpg     # one encryption, one decryption
        for call in run.gpg:
            assert f"--passphrase-file {world.passfile}" in call
            assert secret not in call

    def test_it_refuses_to_ship_without_a_passphrase_file(self, world):
        (world.root / "deploy" / "backup.env").write_text(
            f"BACKUP_REMOTE=u1-sub1@box.example\nBACKUP_ARK_DIR={world.arks}\n")
        run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 78, run.out
        assert "not shipped in the clear" in run.out
        assert not run.docker_runs and not run.sftp
        assert not world.ark_dirs(), "an Ark was frozen for a shipment that cannot happen"

    def test_it_is_unconfigured_rather_than_failed_without_a_remote(self, world):
        (world.root / "deploy" / "backup.env").write_text(
            f"BACKUP_ENV_PASSFILE={world.passfile}\nBACKUP_ARK_DIR={world.arks}\n")
        run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 78, run.out
        assert "BACKUP_REMOTE is not set" in run.out
        assert not run.docker_runs and not run.sftp


# ── what it verifies after the download ───────────────────────────────────────

class TestTheVerificationAfterDownload:
    @pytest.mark.parametrize("victim,expected,step", [
        ("tar.gz.gpg", "sha256 mismatch", "verify the copy off-site"),
        ("sha256", "manifest that came back", "record the shipment"),
    ])
    def test_a_file_that_arrived_damaged_refuses(self, world, victim, expected, step):
        """The upload reports success and the file that landed is not the file
        that was sent — indistinguishable from a good night without the
        read-back. One file at a time, or the manifest check would hide
        whether the ciphertext is compared at all."""
        ark = world.freeze_here()
        world.seed_remote(*OLDER)
        before = world.shipped()
        run = world.run("--ark", ark, BACKUP_ARK_RETAIN=1,
                        FAKE_SFTP_CORRUPT=f"ark-{ark.name}.{victim}")
        assert run.code == 1, run.out
        assert expected in run.out
        assert f"FAILED at: {step}" in run.out
        assert "ARK SHIPPED" not in run.out
        assert set(before) <= set(world.shipped()), "it pruned after a failed verification"
        # No record off-site claims this Ark was verified: a damaged manifest
        # is taken back down, not left for retention to count.
        assert f"ark-{ark.name}.sha256" not in world.shipped(), world.shipped()
        # The local Ark is not the problem: it is kept, with the way to finish.
        assert world.ark_dirs() == [ark]
        assert f"--ark {ark}" in run.out

    @pytest.mark.parametrize("mode,expected", [
        ("fail", "will not decrypt"),
        ("garble", "decrypts to something other than"),
    ])
    def test_a_copy_that_does_not_open_into_the_ark_is_not_a_copy(self, world, mode, expected):
        """A sha256 of ciphertext is satisfied by a file encrypted under a
        passphrase nobody holds any more."""
        run = world.run("--source", "data/analytics.duckdb", FAKE_GPG_DECRYPT=mode)
        assert run.code == 1, run.out
        assert expected in run.out
        assert "ARK SHIPPED" not in run.out
        [ark] = world.ark_dirs()
        assert f"ark-{ark.name}.sha256" not in world.shipped(), \
            "a copy that does not open is recorded as a verified shipment"

    def test_an_unpacked_copy_that_differs_from_the_ark_refuses(self, world):
        """The last comparison before the verification: file for file against
        the local Ark. A `tar` that quietly brings back something else stands
        in for every way the archive could not be the Ark."""
        real_tar = shutil.which("tar")
        (world.bin / "tar").write_text(
            "#!/usr/bin/env bash\n"
            f'"{real_tar}" "$@" || exit $?\n'
            'case " $* " in *" -xzf "*)\n'
            '  dir=""; prev=""; for a in "$@"; do [ "$prev" = -C ] && dir="$a"; prev="$a"; done\n'
            '  for s in "$dir"/*/schema.sql; do echo "-- edited" >> "$s"; done ;;\n'
            "esac\n")
        (world.bin / "tar").chmod(0o755)
        run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 1, run.out
        assert "is not the Ark" in run.out
        assert not [c for c in run.docker_runs if "/.ship." in c], \
            "it verified a copy it already knew was different"

    def test_a_downloaded_copy_that_does_not_verify_fails_the_run(self, world):
        """Mutation: drop the last verification, or its `|| fail` — the run
        would then report a copy nobody replayed."""
        world.seed_remote(*OLDER)
        run = world.run("--source", "data/analytics.duckdb", BACKUP_ARK_RETAIN=1,
                        FAKE_DOCKER_FAIL_VERIFY_UNDER="/.ship.")
        assert run.code == 1, run.out
        assert "FAILED at: verify the downloaded copy" in run.out
        assert "downloaded copy did not verify" in run.out
        assert all((world.remote / f"ark-{s}.sha256").exists() for s in OLDER), \
            "it pruned after the downloaded copy failed"
        [ark] = world.ark_dirs()
        assert f"ark-{ark.name}.sha256" not in world.shipped(), \
            "a copy that did not verify is recorded as a verified shipment"


# ── what it refuses before anything exists ────────────────────────────────────

class TestRefusals:
    def test_no_arguments_does_nothing(self, world):
        """There is no default action: a cron line or a stray call without
        arguments prints the usage and touches nothing."""
        run = world.run()
        assert run.code == 2, run.out
        assert "usage:" in run.out
        assert not run.docker and not run.sftp and not run.gpg
        assert not world.arks.exists()

    def test_both_modes_at_once_is_a_usage_error(self, world):
        run = world.run("--source", "data/analytics.duckdb", "--ark", world.tmp)
        assert run.code == 2 and "usage:" in run.out
        assert not run.docker

    def test_a_source_with_a_wal_beside_it_is_not_static(self, world):
        """Held open, or closed uncleanly: the byte copy would not carry what
        the log holds, and the Parquet and the copy would disagree."""
        (world.root / "data" / "analytics.duckdb.wal").write_bytes(b"log")
        run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 2, run.out
        assert "analytics.duckdb.wal exists" in run.out
        assert not run.docker_runs and not run.sftp
        assert not world.ark_dirs()

    def test_a_source_that_moves_under_the_freeze_is_not_an_ark(self, world):
        """Static is proved, not assumed: hashed before, hashed after, and the
        byte copy equal to both. A writer during the freeze leaves an Ark that
        is a picture of no moment at all; it is deleted, nothing is shipped."""
        run = world.run("--source", "data/analytics.duckdb", FAKE_DOCKER_TOUCH_SOURCE=1)
        assert run.code == 1, run.out
        assert "is not static" in run.out
        assert "SOURCE RELEASED" not in run.out
        assert not world.ark_dirs(), "the half-made Ark was left behind"
        assert not run.sftp and not run.gpg

    def test_a_freeze_that_fails_leaves_no_half_made_ark(self, world):
        """ark_freeze.py makes the Ark's directory before it opens the file,
        so a file DuckDB cannot open leaves a stamp with nothing in it — a
        plaintext directory that `--ark` would later be offered."""
        world.source.write_bytes(b"not a DuckDB file at all" * 100)
        run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 1, run.out
        assert "could not freeze" in run.out and "removed the half-made" in run.out
        assert not world.ark_dirs()
        assert not run.sftp and not run.gpg

    @pytest.mark.parametrize("where", ["out", "env"])
    def test_an_ark_under_data_is_refused(self, world, where):
        """The disk watchdog books growth under ./data as `other` and pages
        at +0.75 GB a week; an Ark is a permanent step of that size."""
        target = world.root / "data" / "ark"
        if where == "out":
            run = world.run("--source", "data/analytics.duckdb", "--out", "data/ark")
        else:
            env_file = world.root / "deploy" / "backup.env"
            env_file.write_text(env_file.read_text().replace(
                f"BACKUP_ARK_DIR={world.arks}", "BACKUP_ARK_DIR=data/ark"))
            run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 2, run.out
        assert "under ./data" in run.out
        assert not target.exists()
        assert not run.docker_runs

    @pytest.mark.parametrize("where,value", [
        ("out", "ark"), ("env", "ark"), ("out", "."), ("out", "deploy/ark"),
    ])
    def test_an_ark_inside_the_repository_is_refused(self, world, where, value):
        """The repository is public, and the Ark is plaintext: every customer's
        phone number, in a byte copy and in Parquet. `.gitignore` covers
        data/ and nothing an Ark is made of, so an Ark anywhere else in the
        working tree is one `git add .` from being published — the class of
        the twelve `.env.bak*` files and the 5 550 scrubbed phone numbers.
        A relative BACKUP_ARK_DIR resolves against the repository, which is
        how this happens without anybody typing the path."""
        def tree():
            return sorted(p.relative_to(world.root).as_posix()
                          for p in world.root.rglob("*")
                          if not p.name.startswith("calls-"))

        before = tree()
        if where == "out":
            run = world.run("--source", "data/analytics.duckdb", "--out", value)
        else:
            env_file = world.root / "deploy" / "backup.env"
            env_file.write_text(env_file.read_text().replace(
                f"BACKUP_ARK_DIR={world.arks}", f"BACKUP_ARK_DIR={value}"))
            run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 2, run.out
        assert "inside the repository" in run.out
        assert not run.docker_runs and not run.sftp
        assert tree() == before, "it wrote into the repository's working tree"

    def test_the_default_is_outside_data_and_the_freezer_agrees(self):
        """Two places spell the default and a test holds them equal; neither
        may sit under the repository's data/."""
        ark_freeze = _load_ark_freeze()
        text = SCRIPT.read_text()
        assert str(ark_freeze.DEFAULT_OUT) == "/root/ark/key-api-bot"
        assert f'${{BACKUP_ARK_DIR:-{ark_freeze.DEFAULT_OUT}}}' in text
        assert "data" not in ark_freeze.DEFAULT_OUT.parts

    def test_too_little_disk_is_refused_before_the_freeze(self, world):
        (world.bin / "df").write_text(
            "#!/usr/bin/env bash\n"
            "echo 'Filesystem 1024-blocks Used Available Capacity Mounted on'\n"
            "echo '/dev/x 100 99 1 99% /'\n")
        (world.bin / "df").chmod(0o755)
        run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 2, run.out
        assert "MB needed" in run.out
        assert not run.docker_runs

    def test_without_the_image_it_does_not_pull(self, world):
        run = world.run("--source", "data/analytics.duckdb", FAKE_DOCKER_NO_IMAGE=1)
        assert run.code == 2, run.out
        assert "this does not pull" in run.out
        assert not run.docker_runs

    def test_a_held_lock_refuses(self, world):
        run = world.run("--source", "data/analytics.duckdb", FAKE_FLOCK_HELD=1)
        assert run.code == 2, run.out
        assert "holds" in run.out and ".ark_ship.lock" in run.out
        assert not run.docker_runs and not run.sftp

    @pytest.mark.parametrize("remote_dir", ["key-api-bot/postgres", "key-api-bot", "key-api-bot/"])
    def test_the_ark_never_shares_another_familys_directory(self, world, remote_dir):
        """Separate directories are what keep every family's retention blind
        to the others' files, whatever a glob is edited into."""
        run = world.run("--source", "data/analytics.duckdb", BACKUP_ARK_REMOTE_DIR=remote_dir)
        assert run.code == 2, run.out
        assert "another family's directory" in run.out
        assert not run.docker_runs


# ── shipping an Ark that already exists ───────────────────────────────────────

class TestShippingAnExistingArk:
    def test_a_failed_shipment_finishes_without_a_second_freeze(self, world):
        first = world.run("--source", "data/analytics.duckdb", FAKE_GPG_DECRYPT="fail")
        assert first.code == 1, first.out
        [ark] = world.ark_dirs()
        world.source.write_bytes(b"web started again; the file moved on")

        run = world.run("--ark", ark)
        assert run.code == 0, run.out
        assert not [c for c in run.docker_runs if "--source" in c], "it froze again"
        assert world.ark_dirs() == [ark]
        assert f"ark-{ark.name}.tar.gz.gpg" in world.shipped()
        assert "SOURCE RELEASED" not in run.out

    def test_a_local_ark_that_does_not_verify_is_not_shipped(self, world):
        first = world.run("--source", "data/analytics.duckdb")
        assert first.code == 0, first.out
        [ark] = world.ark_dirs()
        shutil.rmtree(world.remote)
        with (ark / "tables" / "orders.parquet").open("ab") as fh:
            fh.write(b"bitrot")

        run = world.run("--ark", ark)
        assert run.code == 1, run.out
        assert "did not verify" in run.out
        assert "size differs: tables/orders.parquet" in run.out
        assert not world.remote.exists() or not world.shipped()
        assert not run.gpg

    def test_an_ark_already_recorded_off_site_is_verified_and_never_sent_again(self, world):
        """gpg's output differs on every run, and the transport deletes the
        remote file before renaming its replacement into place: a second
        upload of a shipped Ark destroyed the verified copy before the new
        one was verified, and a damaged second upload left none at all.
        `--ark` is what the script tells an operator to run after any
        failure, a prune's included, so it must be safe on a shipped Ark."""
        first = world.run("--source", "data/analytics.duckdb")
        assert first.code == 0, first.out
        [ark] = world.ark_dirs()
        cipher = world.remote / f"ark-{ark.name}.tar.gz.gpg"
        manifest = world.remote / f"ark-{ark.name}.sha256"
        sent = cipher.read_bytes(), manifest.read_bytes()

        # Anything put now would land damaged.
        run = world.run("--ark", ark, FAKE_SFTP_CORRUPT=cipher.name)
        assert run.code == 0, run.out
        assert "ARK SHIPPED AND VERIFIED" in run.out
        assert "not sent again" in run.out
        assert not [c for c in run.sftp
                    if c.lstrip("-").split()[0] in ("put", "rename", "rm")], run.sftp
        assert (cipher.read_bytes(), manifest.read_bytes()) == sent
        # Fetched back and verified as hard as a fresh shipment: decrypted,
        # compared with the local Ark, and L0-L2 on the downloaded copy.
        assert not [c for c in run.gpg if "--symmetric" in c], run.gpg
        assert len([c for c in run.gpg if " -d " in f" {c} "]) == 1, run.gpg
        assert "VERIFY OK" in run.out.split("verify the downloaded copy")[1], run.out

    def test_a_recorded_copy_that_does_not_verify_is_not_replaced(self, world):
        """The other half: a recorded copy that no longer verifies is not sent
        over either. Replacing it is a decision about the only record of a
        verified shipment, so the run stops and says how to take it."""
        first = world.run("--source", "data/analytics.duckdb")
        assert first.code == 0, first.out
        [ark] = world.ark_dirs()
        cipher = world.remote / f"ark-{ark.name}.tar.gz.gpg"
        cipher.write_bytes(cipher.read_bytes()[:-5])

        run = world.run("--ark", ark)
        assert run.code == 1, run.out
        assert "sha256 mismatch" in run.out and "ARK SHIPPED" not in run.out
        assert "is not replaced" in run.out
        assert f"ark-{ark.name}.sha256" in run.out   # what to remove, by name
        assert not [c for c in run.sftp
                    if c.lstrip("-").split()[0] in ("put", "rename", "rm")], run.sftp
        assert f"ark-{ark.name}.sha256" in world.shipped()

    @pytest.mark.parametrize("make", ["not-a-stamp", "no-manifest"])
    def test_a_directory_that_is_not_an_ark_is_refused(self, world, make):
        if make == "not-a-stamp":
            target = world.tmp / "my-ark"
            target.mkdir()
            (target / "manifest.json").write_text("{}")
            expected = "not an Ark"
        else:
            target = world.tmp / "20261027T061500Z"
            target.mkdir()
            expected = "no manifest.json"
        run = world.run("--ark", target)
        assert run.code == 2, run.out
        assert expected in run.out
        assert not run.docker_runs


# ── retention off-site ────────────────────────────────────────────────────────

class TestRetention:
    def test_it_keeps_two_of_its_own_and_never_touches_the_first_ark(self, world):
        """OD-S5-6 keeps two. The first Ark went up by hand as a plain
        .tar.gz before OD-01; it is not a name this script mints, so it is
        neither counted nor deleted — and neither is anybody's note."""
        world.seed_remote(*OLDER, extras=(FIRST_ARK, "notes.txt"))
        run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 0, run.out
        [ark] = world.ark_dirs()
        names = world.shipped()
        assert names == sorted([
            FIRST_ARK, "notes.txt",
            f"ark-{OLDER[2]}.sha256", f"ark-{OLDER[2]}.tar.gz.gpg",
            f"ark-{ark.name}.sha256", f"ark-{ark.name}.tar.gz.gpg",
        ]), names
        assert "held      2 of 2" in run.out
        assert FIRST_ARK in run.out.split("ARK SHIPPED")[1], "the summary hides the first Ark"

    def test_it_writes_nowhere_but_its_own_directory(self, world):
        world.pg_remote.mkdir(parents=True)
        (world.pg_remote / "ks-20260918-074000.sha256").write_text("dump manifest")
        (world.remote_root / "key-api-bot" / "ks-warehouse-20260101-000000.tar").write_text("x")
        run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 0, run.out
        assert sorted(p.name for p in world.pg_remote.iterdir()) == ["ks-20260918-074000.sha256"]
        assert sorted(p.name for p in (world.remote_root / "key-api-bot").iterdir()) == [
            "ark", "ks-warehouse-20260101-000000.tar", "postgres"]
        for line in run.sftp:
            if line.lstrip("-").split()[0] in ("put", "rm", "rename"):
                assert "key-api-bot/ark/" in line, line

    def test_its_own_debris_is_swept_and_nobody_elses(self, world):
        world.seed_remote(*OLDER[:1], extras=(
            f"ark-{OLDER[1]}.tar.gz.gpg.part",     # an upload that never came back
            f"{FIRST_ARK}.part",                   # not a name this script mints
        ))
        world.seed_remote(OLDER[2], manifest=False)  # a ciphertext whose manifest never landed
        run = world.run("--source", "data/analytics.duckdb", BACKUP_ARK_RETAIN=5)
        assert run.code == 0, run.out
        names = world.shipped()
        assert f"ark-{OLDER[1]}.tar.gz.gpg.part" not in names
        assert f"ark-{OLDER[2]}.tar.gz.gpg" not in names
        assert f"{FIRST_ARK}.part" in names
        assert f"ark-{OLDER[0]}.tar.gz.gpg" in names

    @pytest.mark.parametrize("value", ["0", "two", ""])
    def test_a_retention_nobody_can_read_deletes_nothing(self, world, value):
        world.seed_remote(*OLDER)
        run = world.run("--source", "data/analytics.duckdb", BACKUP_ARK_RETAIN=value)
        if value == "":
            # Empty means unset, and unset means the owner's two.
            assert run.code == 0, run.out
            assert "held      2 of 2" in run.out
            return
        assert run.code == 0, run.out
        assert "nothing pruned" in run.out
        assert all((world.remote / f"ark-{s}.sha256").exists() for s in OLDER)

    def test_a_shipment_that_failed_verification_is_not_one_of_the_two(self, world):
        """OD-S5-6 keeps two Arks, and a copy that failed its read-back is not
        an Ark. It used to keep its manifest — which went up before the read-
        back — and so took one of the two places and pushed a verified Ark
        out, while the summary said `held 2 of 2`."""
        # Renamed as they are made: two freezes in one second share a stamp.
        older = world.freeze_here()
        older = older.rename(older.with_name("20200101T000000Z"))
        broken = world.freeze_here()
        broken = broken.rename(broken.with_name("20200102T000000Z"))

        a = world.run("--ark", older)
        assert a.code == 0, a.out
        b = world.run("--ark", broken, FAKE_SFTP_CORRUPT=f"ark-{broken.name}.tar.gz.gpg")
        assert b.code == 1 and "sha256 mismatch" in b.out, b.out
        c = world.run("--source", "data/analytics.duckdb")
        assert c.code == 0, c.out
        newest = (set(world.ark_dirs()) - {older, broken}).pop()

        assert world.shipped() == sorted([
            f"ark-{older.name}.sha256", f"ark-{older.name}.tar.gz.gpg",
            f"ark-{newest.name}.sha256", f"ark-{newest.name}.tar.gz.gpg",
        ]), world.shipped()
        assert f"swept: ark-{broken.name}.tar.gz.gpg" in c.out
        assert "held      2 of 2" in c.out

    def test_a_prune_that_cannot_delete_does_not_fail_a_verified_shipment(self, world):
        """The Ark is up and verified when the prune runs, so a deletion that
        fails is reported and stands down — it does not turn the run into
        FAILED with advice to ship again."""
        real = world.bin / "sftp-real"
        (world.bin / "sftp").rename(real)
        # Every batch made of nothing but `-rm` lines — the sweep and the
        # prune — dies the way sftp does when the connection drops.
        (world.bin / "sftp").write_text(
            "#!/usr/bin/env bash\n"
            'batch="$(cat)"\n'
            "if ! printf '%s\\n' \"$batch\" | grep -qvE '^-rm '; then exit 255; fi\n"
            f"printf '%s\\n' \"$batch\" | \"{real}\" \"$@\"\n")
        (world.bin / "sftp").chmod(0o755)
        world.seed_remote(*OLDER, extras=(f"ark-{OLDER[0]}.tar.gz.gpg.part",))

        run = world.run("--source", "data/analytics.duckdb")
        assert run.code == 0, run.out
        assert "ARK SHIPPED AND VERIFIED" in run.out
        assert "FAILED" not in run.out and "--ark" not in run.out
        assert run.out.index("VERIFY OK") < run.out.index("── prune off-site")
        [stuck] = re.findall(r"^  not pruned (.*)$", run.out, re.M)
        for stamp in OLDER[:2]:
            assert f"could not delete ark-{stamp}" in run.out
            assert f"ark-{stamp}.sha256" in world.shipped()
            assert f"ark-{stamp}" in stuck.split(), stuck
        # The summary is reached with its own debris still there now, and
        # must not call it somebody else's.
        assert f"could not delete ark-{OLDER[0]}.tar.gz.gpg.part" in run.out
        assert "also here" not in run.out, run.out

    def test_a_newer_ark_already_off_site_stops_the_prune(self, world):
        """Anchored to the Ark this run verified: if it is not among the
        newest N, the listing is not what it seems, and nothing is deleted."""
        world.seed_remote("29991231T235959Z", *OLDER)
        run = world.run("--source", "data/analytics.duckdb", BACKUP_ARK_RETAIN=1)
        assert run.code == 0, run.out
        assert "not pruning" in run.out
        assert all((world.remote / f"ark-{s}.sha256").exists() for s in OLDER)


# ── nothing runs it but a person ──────────────────────────────────────────────

# What may run the tool, as opposed to what may talk about it. A runbook or a
# comment naming it is documentation; a code line, a workflow step or a cron
# entry naming it is a caller.
_CALLERS_EXEMPT = {
    "deploy/ark_ship.sh": "the tool itself",
    "deploy/ark_freeze.py": "names it in the refusal that sends a person to it",
}


def _code_strings(path: Path):
    """What could invoke something: shell, workflow and compose lines that are
    not comments, and in Python every string and name that is not a docstring
    — a comment is not in the tree at all."""
    text = path.read_text(errors="replace")
    if path.suffix != ".py":
        return [line for line in text.splitlines() if not line.lstrip().startswith("#")]
    import ast

    tree = ast.parse(text)
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                docstrings.add(id(body[0].value))
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and id(node) not in docstrings:
            out.append(node.value)
        elif isinstance(node, ast.Name):
            out.append(node.id)
    return out


def test_nothing_in_the_repository_schedules_or_calls_it():
    """A host tool a person runs at the start of the week of silence. No
    scheduler job, cron line, workflow, compose service or other script may
    invoke it. Mutation: register it as a job in core/scheduler.py, or add a
    step running it to a workflow — either names it in code."""
    callers = []
    sources = []
    for tree in ("core", "web", "bot", "scripts", "deploy", ".github"):
        for here, dirs, files in os.walk(REPO / tree):
            dirs[:] = [d for d in dirs
                       if d not in {"node_modules", "__pycache__", "static-v2", "dist"}]
            sources += [Path(here) / f for f in files
                        if Path(f).suffix in {".py", ".sh", ".yml", ".yaml"}]
    sources += [p for p in REPO.glob("docker-compose*.yml")]
    sources += [p for p in REPO.glob("Dockerfile*")]
    for path in sources:
        rel = path.relative_to(REPO).as_posix()
        if rel in _CALLERS_EXEMPT:
            continue
        if any("ark_ship" in line for line in _code_strings(path)):
            callers.append(rel)
    assert not callers, callers
    # The walk reaches what it claims to: the scheduler and the workflows.
    walked = {p.relative_to(REPO).as_posix() for p in sources}
    assert "core/scheduler.py" in walked and any(w.startswith(".github/") for w in walked)
    for exempt in _CALLERS_EXEMPT:
        assert (REPO / exempt).exists(), f"an exemption outlived its file: {exempt}"


def test_the_documents_say_the_second_arks_are_kept_by_count():
    """OD-S5-6 keeps two of deploy/ark_ship.sh's Arks off-site, by count, in
    the directory the first Ark went to. The runbook and the freezer's
    docstring are where somebody reads how long an Ark lives, and after the
    pruning existed both still said nothing there is pruned, or that the Ark
    is never rotated. Each now names the knob that prunes and the decision
    behind it, and the sentences that said otherwise are gone."""
    assert re.search(r'^RETAIN="\$\{BACKUP_ARK_RETAIN:-2\}"$', SCRIPT.read_text(), re.M)

    def flat(text):
        return " ".join(text.split())

    runbook = flat((DEPLOY / "step05-preflight-runbook.md").read_text())
    docstring = flat(_load_ark_freeze().__doc__)
    for name, text in (("the runbook", runbook), ("ark_freeze.py", docstring)):
        assert "BACKUP_ARK_RETAIN" in text and "OD-S5-6" in text, name
    for stale in ("Nothing there is pruned today",
                  "Nothing prunes it, here or there",
                  "once in the deploy that removes DuckDB",
                  "never rotated: once before the first landing write"):
        assert stale not in runbook, stale
    assert "twice in the whole migration** and never rotated" not in docstring


def test_the_help_says_where_the_ark_goes_and_touches_nothing(world):
    run = world.run("--help")
    assert run.code == 0
    assert "/root/ark/key-api-bot" in run.out and "Never under ./data" in run.out
    assert not run.docker and not run.sftp


# ── ark_freeze.py ─────────────────────────────────────────────────────────────

def _load_ark_freeze():
    import importlib.util

    spec = importlib.util.spec_from_file_location("ark_freeze_under_test", DEPLOY / "ark_freeze.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestTheFreezer:
    def test_an_ark_is_never_written_into_an_existing_one(self, tmp_path, monkeypatch):
        """Two freezes in one second share a stamp; the second must not write
        a manifest over files from another moment — or into an Ark that is
        already shipped and immutable."""
        ark_freeze = _load_ark_freeze()
        source = tmp_path / "analytics.duckdb"
        _warehouse(source)
        out = tmp_path / "arks"

        class Frozen(ark_freeze.datetime):
            @classmethod
            def now(cls, tz=None):
                return ark_freeze.datetime(2026, 10, 27, 6, 15, 0, tzinfo=tz)

        monkeypatch.setattr(ark_freeze, "datetime", Frozen)
        first = ark_freeze.freeze(source, out)
        before = (first / "manifest.json").read_bytes()
        with pytest.raises(SystemExit) as stop:
            ark_freeze.freeze(source, out)
        assert stop.value.code == 1
        assert (first / "manifest.json").read_bytes() == before
        assert ark_freeze.verify(first) == 0

    def test_inside_a_container_the_default_must_be_a_mount(self, tmp_path, monkeypatch):
        """The default is a host path. Inside a container it is the
        container's own layer unless something is mounted on the way down,
        and an Ark written there is gone at `--rm` — after the freeze has
        reported it complete."""
        ark_freeze = _load_ark_freeze()
        marker = tmp_path / ".dockerenv"
        monkeypatch.setattr(ark_freeze, "CONTAINER_MARKERS", (marker,))
        out = Path("/root/ark/key-api-bot")

        assert not ark_freeze.leaves_with_the_container(out), "not in a container"
        marker.write_text("")
        monkeypatch.setattr(ark_freeze.os.path, "ismount", lambda p: False)
        assert ark_freeze.leaves_with_the_container(out)
        mounted = {"/root/ark"}
        monkeypatch.setattr(ark_freeze.os.path, "ismount", lambda p: str(p) in mounted)
        assert not ark_freeze.leaves_with_the_container(out)

    @pytest.mark.parametrize("bad,rows", [
        ("the wrong shape", "SELECT * FROM (VALUES (1), (2)) t(id)"),
        ("a key the PRIMARY KEY refuses",
         "SELECT * FROM (VALUES (1, 'a', 1.00, 1), (1, 'b', 2.00, 2)) t(id, phone, grand_total, n)"),
        ("a NULL in a NOT NULL column",
         "SELECT * FROM (VALUES (1, NULL::VARCHAR, 1.00, 1), (2, NULL, 2.00, 2)) t(id, phone, grand_total, n)"),
    ])
    def test_l2_restores_every_table_rather_than_counting_its_parquet(
            self, tmp_path, capsys, bad, rows):
        """L2 is the restore RESTORE.md tells a stranger to make: schema.sql
        into an empty engine, then each table's Parquet INSERTed into its
        table. Counting the Parquet passed a file that cannot be loaded —
        with the right number of rows, its manifest entry refreshed so L0
        holds, and the byte copy untouched so L1 holds."""
        ark_freeze = _load_ark_freeze()
        source = tmp_path / "analytics.duckdb"
        _warehouse(source)
        ark = ark_freeze.freeze(source, tmp_path / "arks")
        assert ark_freeze.verify(ark) == 0

        pq = ark / "tables" / "orders.parquet"
        pq.unlink()
        duckdb.connect().execute(f"COPY ({rows}) TO '{pq}' (FORMAT PARQUET)")
        manifest = json.loads((ark / "manifest.json").read_text())
        assert manifest["tables"]["orders"] == 2
        manifest["files"]["tables/orders.parquet"] = {
            "bytes": pq.stat().st_size, "sha256": _sha256(pq)}
        (ark / "manifest.json").write_text(json.dumps(manifest, indent=2))
        capsys.readouterr()

        assert ark_freeze.verify(ark) == 1, bad
        out = capsys.readouterr().out
        assert re.search(r"L0 files: \d+ checked, 0 bad", out), out
        assert "L1 frozen db: 2 tables, 0 mismatched" in out
        assert re.search(r"ERROR .*orders.* does not restore", out), out

    def test_l2_loads_a_foreign_keys_parent_before_its_child(self, tmp_path):
        """Each table is restored into an engine of its own, so the largest
        table bounds the memory and not the whole warehouse; a table that
        references another gets its parent loaded first, or the restore
        would refuse what a full one accepts."""
        ark_freeze = _load_ark_freeze()
        source = tmp_path / "fk.duckdb"
        conn = duckdb.connect(str(source))
        conn.execute("CREATE TABLE a_parent (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE b_child (id INTEGER, "
                     "parent_id INTEGER REFERENCES a_parent (id))")
        conn.execute("INSERT INTO a_parent VALUES (1), (2)")
        conn.execute("INSERT INTO b_child VALUES (10, 1), (11, 2)")
        conn.close()
        ark = ark_freeze.freeze(source, tmp_path / "arks")
        assert ark_freeze.verify(ark) == 0

    def test_the_container_guard_refuses_the_default_and_not_a_choice(self, tmp_path, monkeypatch):
        ark_freeze = _load_ark_freeze()
        source = tmp_path / "analytics.duckdb"
        _warehouse(source)
        monkeypatch.setattr(ark_freeze, "leaves_with_the_container", lambda out: True)
        froze = []
        monkeypatch.setattr(ark_freeze, "freeze", lambda s, o: froze.append(o))

        monkeypatch.setattr(sys, "argv", ["ark_freeze.py", "--source", str(source)])
        with pytest.raises(SystemExit) as stop:
            ark_freeze.main()
        assert stop.value.code == 2 and not froze

        monkeypatch.setattr(sys, "argv", ["ark_freeze.py", "--source", str(source),
                                          "--out", str(tmp_path / "chosen")])
        ark_freeze.main()
        assert froze == [tmp_path / "chosen"]


@pytest.mark.skipif(shutil.which("gpg") is None, reason="no real gpg on this machine")
def test_real_gpg_end_to_end(tmp_path):
    """The whole shipment with a real gpg: the archive really is encrypted,
    really opens with the passphrase file, and the downloaded copy still
    replays at L2. The keyring lives in a short directory of its own — the
    run's HOME is a pytest path, and gpg-agent's socket has a length limit."""
    world = _make_world(tmp_path, real_gpg=True)
    gnupg = tempfile.mkdtemp(prefix="arkg")
    try:
        run = world.run("--source", "data/analytics.duckdb", GNUPGHOME=gnupg)
    finally:
        if shutil.which("gpgconf"):
            subprocess.run(["gpgconf", "--kill", "gpg-agent"], capture_output=True,
                           env={**os.environ, "GNUPGHOME": gnupg})
        shutil.rmtree(gnupg, ignore_errors=True)
    assert run.code == 0, run.out
    [ark] = world.ark_dirs()
    cipher = (world.remote / f"ark-{ark.name}.tar.gz.gpg").read_bytes()
    assert not cipher.startswith(b"\x1f\x8b"), "the remote holds a plain gzip"
    assert "VERIFY OK" in run.out.split("verify the downloaded copy")[1], run.out
