"""deploy/pg_offsite.sh, offsite_check.sh and the remote drill, actually run.

The parsing guards next door say the scripts are *shaped* right. They cannot
say that a sha mismatch is refused, that the freshness marker is written only
after the bytes came back, or that retention deletes its own oldest and
nothing else — and those are the three sentences DN-09 is made of. A backup
whose failure modes have only been read is the thing this whole item exists to
stop believing in.

So each script is copied into a scratch repository root, given a directory to
treat as the Storage Box, and run against fakes on PATH: `sftp` speaks the
batch mini-language against that directory, `gpg` prefixes a marker line
instead of encrypting, `flock` answers held or free, and `docker` reports row
counts. Every fake records its calls, which is how the promises are checked.

The one thing a fake cannot prove is that real gpg round-trips, so
`test_real_gpg_round_trips` runs when a real one is installed and skips when
it is not — which is most laptops. **Not CI**: `ci.yml` runs on
ubuntu-latest, which ships GnuPG, so that one call is proven on every pull
request and this is a skip only where somebody is reading the output.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DEPLOY = REPO / "deploy"

pytestmark = pytest.mark.skipif(
    not (shutil.which("bash") and shutil.which("awk")), reason="needs bash and awk")

STAMP = "20260918-074000"
OLDER = ["20260101-000000", "20260102-000000", "20260103-000000"]

# The remote, as sftp sees it. Only the verbs the scripts use, and a leading
# '-' is sftp's own "ignore this failing" prefix — the scripts rely on it for
# mkdir of a directory that exists and rm of a .part that does not.
FAKE_SFTP = r"""#!/usr/bin/env bash
root="$FAKE_REMOTE_ROOT"
cwd="$root"
while IFS= read -r line; do
    [ -n "$line" ] || continue
    printf '%s\n' "$line" >> "$FAKE_SFTP_CALLS"
    soft=0
    case "$line" in -*) soft=1; line="${line#-}" ;; esac
    # shellcheck disable=SC2086
    set -- $line
    cmd="$1"; shift
    ok=0
    case "$cmd" in
        mkdir)  mkdir -p "$root/$1" && ok=1 ;;
        rm)     if [ -f "$root/$1" ]; then rm -f "$root/$1" && ok=1; fi ;;
        put)    mkdir -p "$(dirname "$root/$2")"
                # FAKE_SFTP_CORRUPT truncates on the way up: `all`, or the
                # final name of one file. One file at a time matters — with
                # everything truncated the manifest comparison fires first and
                # the per-file sha256 is never reached, which is exactly how
                # the first version of this test passed against a script that
                # had stopped comparing hashes.
                tgt="$(basename "$2")"; tgt="${tgt%.part}"
                if [ "${FAKE_SFTP_CORRUPT:-}" = all ] || [ "${FAKE_SFTP_CORRUPT:-}" = "$tgt" ]; then
                    head -c 3 "$1" > "$root/$2" && ok=1
                else
                    cp "$1" "$root/$2" && ok=1
                fi ;;
        get)    if [ -f "$root/$1" ]; then cp "$root/$1" "$2" && ok=1; fi ;;
        rename) if [ -f "$root/$1" ]; then mv "$root/$1" "$root/$2" && ok=1; fi ;;
        cd)     cwd="$root/$1"; [ -d "$cwd" ] && ok=1 ;;
        ls)     ls -1 "$cwd" 2>/dev/null; ok=1 ;;
    esac
    if [ "$ok" != 1 ] && [ "$soft" != 1 ]; then exit 1; fi
done
exit 0
"""

# Encryption is a marker line, so the round trip is still a round trip and a
# truncated ciphertext still fails to "decrypt". FAKE_GPG_DECRYPT stands in for
# the two ways a passphrase file can betray the archive: `fail` is the rotated
# or truncated one, which is what the nightly read-back could never see while
# it only hashed ciphertext; `garble` is a round trip that succeeds and returns
# something else.
FAKE_GPG = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_GPG_CALLS"
out=""; in=""; pf=""; mode=enc
while [ $# -gt 0 ]; do
    case "$1" in
        -o) out="$2"; shift 2 ;;
        -d|--decrypt) mode=dec; shift ;;
        --passphrase-file) pf="$2"; shift 2 ;;
        --cipher-algo) shift 2 ;;
        -*) shift ;;
        *) in="$1"; shift ;;
    esac
done
[ -r "$pf" ] || { echo "gpg: no passphrase file" >&2; exit 2; }
if [ "$mode" = enc ]; then
    { printf 'FAKEGPG\n'; cat "$in"; } > "$out"
else
    [ "${FAKE_GPG_DECRYPT:-}" = fail ] && { echo "gpg: decryption failed" >&2; exit 2; }
    head -n 1 "$in" | grep -qx FAKEGPG || { echo "gpg: decryption failed" >&2; exit 2; }
    tail -n +2 "$in" > "$out"
    [ "${FAKE_GPG_DECRYPT:-}" = garble ] && printf 'extra\n' >> "$out"
fi
exit 0
"""

FAKE_FLOCK = """#!/usr/bin/env bash
[ -n "${FAKE_FLOCK_HELD:-}" ] && exit 1
exit 0
"""

FAKE_DOCKER = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_DOCKER_CALLS"
case "$1" in
    run) echo fakecontainerid; exit 0 ;;
    rm)  exit 0 ;;
    exec)
        shift
        while [ $# -gt 0 ]; do
            case "$1" in -i|-t|-it) shift ;; -e) shift 2 ;; *) break ;; esac
        done
        cname="$1"; shift
        case "$1" in
            pg_isready) exit 0 ;;
            pg_restore) cat >/dev/null; exit 0 ;;
            psql)
                q=""
                while [ $# -gt 0 ]; do [ "$1" = "-tAc" ] && q="$2"; shift; done
                if [ -z "$q" ]; then cat >/dev/null; exit 0; fi
                tbl="${q##*FROM }"
                key="$(printf '%s' "$tbl" | tr '.a-z' '_A-Z')"
                if [ "$cname" = ks-postgres ]; then var="FAKE_LIVE_$key"; else var="FAKE_RESTORED_$key"; fi
                eval "v=\${$var-}"
                [ -n "$v" ] || exit 1
                printf '%s\n' "$v"
                exit 0 ;;
        esac
        exit 0 ;;
esac
exit 0
"""

FAKES = {"sftp": FAKE_SFTP, "gpg": FAKE_GPG, "flock": FAKE_FLOCK, "docker": FAKE_DOCKER}


class Run:
    def __init__(self, done, root):
        self.code = done.returncode
        self.out = done.stdout + done.stderr
        self.root = root

    @property
    def sftp(self):
        path = self.root / "calls-sftp.log"
        return path.read_text().splitlines() if path.exists() else []

    @property
    def gpg(self):
        path = self.root / "calls-gpg.log"
        return path.read_text().splitlines() if path.exists() else []

    @property
    def docker(self):
        path = self.root / "calls-docker.log"
        return path.read_text().splitlines() if path.exists() else []


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def world(tmp_path):
    """A scratch repository root, a scratch Storage Box, and the fakes."""
    root = tmp_path / "repo"
    (root / "deploy").mkdir(parents=True)
    for name in ("pg_offsite.sh", "pg_offsite_lib.sh", "notify.sh",
                 "offsite_check.sh", "pg_restore_drill.sh"):
        shutil.copy(DEPLOY / name, root / "deploy" / name)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in FAKES.items():
        target = bin_dir / name
        target.write_text(body)
        target.chmod(0o755)

    remote = tmp_path / "storagebox"
    remote.mkdir()
    passfile = tmp_path / "passfile"
    passfile.write_text("not-the-real-one\n")

    # The kill switch, so notify() returns before it can reach Telegram or the
    # alert archive. tests/conftest.py's fixture does not cover a subprocess.
    (root / ".env").write_text("KS_ALERTS_DISABLED=1\nADMIN_USER_IDS=1,2\n")
    (root / "deploy" / "backup.env").write_text(
        f"BACKUP_REMOTE=u1-sub1@box.example\n"
        f"BACKUP_SSH_KEY={tmp_path}/key\n"
        f"BACKUP_ENV_PASSFILE={passfile}\n"
        f"BACKUP_PG_REMOTE_DIR=key-api-bot/postgres\n"
    )
    (tmp_path / "key").write_text("")

    dumps = root / "backups" / "postgres"
    dumps.mkdir(parents=True)

    class World:
        def __init__(self):
            self.root = root
            self.remote_root = remote
            self.remote = remote / "key-api-bot" / "postgres"
            self.dumps = dumps
            self.passfile = passfile
            self.marker = root / "data" / ".pg_offsite_last_ok"
            self.log = root / "data" / "logs" / "pg_offsite.log"

        def dump(self, stamp=STAMP, body="DUMPBYTES", roles="ROLES", age_h=0):
            (dumps / f"ks-{stamp}.dump").write_text(body)
            if roles is not None:
                (dumps / f"roles-{stamp}.sql").write_text(roles)
            when = time.time() - age_h * 3600
            for suffix in (".dump", ".sql"):
                name = ("ks-" if suffix == ".dump" else "roles-") + stamp + suffix
                if (dumps / name).exists():
                    os.utime(dumps / name, (when, when))

        def seed_remote(self, *stamps, extras=(), manifest=True):
            self.remote.mkdir(parents=True, exist_ok=True)
            for stamp in stamps:
                (self.remote / f"ks-{stamp}.dump.gpg").write_text("old")
                (self.remote / f"roles-{stamp}.sql.gpg").write_text("old")
                if manifest:
                    (self.remote / f"ks-{stamp}.sha256").write_text("old")
            for extra in extras:
                (self.remote / extra).write_text("not ours")

        def run(self, script="pg_offsite.sh", args=(), **env_extra):
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
            done = subprocess.run(
                ["bash", str(root / "deploy" / script), *args],
                env=env, capture_output=True, text=True, timeout=120)
            return Run(done, root)

    return World()


# ── the shipment ──────────────────────────────────────────────────────────────

class TestTheShipment:
    def test_a_healthy_run_ships_three_files_and_writes_the_marker(self, world):
        world.dump()
        run = world.run()
        assert run.code == 0, run.out

        names = sorted(p.name for p in world.remote.iterdir())
        assert names == [f"ks-{STAMP}.dump.gpg", f"ks-{STAMP}.sha256",
                         f"roles-{STAMP}.sql.gpg"], run.out
        assert world.marker.exists()
        # The manifest hashes the ciphertext, which is what travels and
        # therefore what a verification can compare without the passphrase.
        manifest = (world.remote / f"ks-{STAMP}.sha256").read_text()
        assert _sha256(world.remote / f"ks-{STAMP}.dump.gpg") in manifest
        assert "FAKEGPG" in (world.remote / f"ks-{STAMP}.dump.gpg").read_text()

    def test_nothing_reaches_the_remote_in_the_clear(self, world):
        """The dump carries ~6 000 phone numbers; OD-01 made this mandatory."""
        world.dump(body="0501234567 Ivanenko")
        assert world.run().code == 0
        for path in world.remote.iterdir():
            if path.suffix == ".gpg":
                assert path.read_text().startswith("FAKEGPG")
        assert not any("0501234567" in p.read_text()
                       for p in world.remote.iterdir() if p.suffix != ".gpg")

    def test_the_passphrase_travels_as_a_path_and_never_as_a_value(self, world):
        world.dump()
        run = world.run()
        gpg_calls = [c for c in run.gpg if c]
        assert gpg_calls
        secret = world.passfile.read_text().strip()
        for call in gpg_calls:
            assert "--passphrase-file" in call
            assert secret not in call

    @pytest.mark.parametrize("victim,expected", [
        (f"ks-{STAMP}.dump.gpg", "sha256 mismatch"),
        (f"roles-{STAMP}.sql.gpg", "sha256 mismatch"),
        (f"ks-{STAMP}.sha256", "manifest that came back"),
    ])
    def test_a_file_that_arrived_damaged_refuses_and_leaves_no_marker(
            self, world, victim, expected):
        """The realistic failure: the upload reports success and the file that
        landed is not the file that was sent. Without the read-back this is
        indistinguishable from a good night — and each of the three files has
        to be checked, because a manifest that is itself damaged would
        otherwise excuse whatever it claims about the other two."""
        world.dump()
        run = world.run(FAKE_SFTP_CORRUPT=victim)
        assert run.code != 0, run.out
        assert expected in run.out
        assert not world.marker.exists(), "a marker was written for a copy that did not verify"

    @pytest.mark.parametrize("mode,expected", [
        ("fail", "will not decrypt"),
        ("garble", "decrypts to something other than"),
    ])
    def test_a_copy_that_does_not_open_is_not_a_copy(self, world, mode, expected):
        """A sha256 of ciphertext is satisfied by a file encrypted under a
        passphrase nobody holds any more.

        The realistic route there is $ENV_PASSFILE being rotated, truncated or
        restored from another host: every shipment after that verifies, the
        marker moves and the 36 h alarm stays quiet for ever, while the
        escrowed passphrase opens nothing. So the run fetches its own
        ciphertext back, decrypts it, and compares the plaintext with the dump
        on this disk."""
        world.dump()
        run = world.run(FAKE_GPG_DECRYPT=mode)
        assert run.code != 0, run.out
        assert expected in run.out
        assert not world.marker.exists(), "a marker was written for a copy nothing can open"

    def test_the_healthy_run_opens_the_copy_it_shipped(self, world):
        """Two encryptions and one decryption: the shipment is not finished
        until the bytes off-site have been read back *and* opened."""
        world.dump()
        run = world.run()
        assert run.code == 0, run.out
        assert sum(1 for c in run.gpg if "--symmetric" in c) == 2, run.gpg
        assert sum(1 for c in run.gpg if "-d" in c.split()) == 1, run.gpg
        assert "opens with" in run.out

    def test_an_empty_dump_is_not_a_backup(self, world):
        world.dump(body="")
        run = world.run()
        assert run.code != 0
        assert "is empty" in run.out
        assert not world.marker.exists()
        assert not any(world.remote_root.rglob("*.gpg"))

    def test_a_roles_file_older_than_the_dump_refuses(self, world):
        """pg_backup.sh deletes whichever half came out empty, so the newest
        roles file can be days older than the newest dump — and a restore
        given last week's roles is a cluster nobody can log in to read."""
        world.dump(stamp=OLDER[0], age_h=2)
        world.dump(stamp=STAMP, roles=None)
        run = world.run()
        assert run.code != 0
        assert "older than the newest dump" in run.out

    def test_a_stale_dump_is_not_shipped_under_a_fresh_name(self, world):
        world.dump(age_h=40)
        run = world.run()
        assert run.code != 0
        assert "pg_backup has stopped" in run.out
        assert not world.marker.exists()

    def test_it_refuses_to_ship_an_older_cluster_than_the_one_off_site(self, world):
        """A host restored from an old backups/ directory would otherwise push
        yesterday's cluster and count-retention would age the good copies out."""
        world.dump(stamp=OLDER[0])
        world.seed_remote(STAMP)
        run = world.run()
        assert run.code != 0
        assert "refusing to ship an older cluster" in run.out

    def test_debris_newer_than_today_does_not_look_like_a_newer_cluster(self, world):
        """The refusal above reads the remote *before* anything is uploaded or
        swept, so it is where the anchored listing has to hold on its own.

        An interrupted upload leaves `ks-<stamp>.dump.gpg.part`; under the
        unanchored `grep -o` that name matched as a copy, and a leftover from
        later the same day made the shipper refuse to ship at all — the host
        would have stopped backing up because of its own litter."""
        world.dump()
        world.remote.mkdir(parents=True, exist_ok=True)
        (world.remote / "ks-20260919-000000.dump.gpg.part").write_text("truncated")
        run = world.run()
        assert run.code == 0, run.out
        assert "refusing to ship an older cluster" not in run.out
        assert (world.remote / f"ks-{STAMP}.dump.gpg").exists()

    def test_re_running_the_same_day_finishes(self, world):
        """Idempotent on purpose: a run that failed at the verification must be
        able to be repeated, so the refusal above is on a *newer* stamp and
        not on any difference."""
        world.dump()
        assert world.run().code == 0
        second = world.run()
        assert second.code == 0, second.out
        assert (world.remote / f"ks-{STAMP}.dump.gpg").exists()

    def test_it_is_unconfigured_rather_than_failed_without_a_remote(self, world):
        world.dump()
        (world.root / "deploy" / "backup.env").write_text(
            f"BACKUP_ENV_PASSFILE={world.passfile}\n")
        run = world.run()
        assert run.code == 78, run.out
        assert "BACKUP_REMOTE is not set" in run.out

    def test_it_refuses_to_ship_without_a_passphrase_file(self, world):
        world.dump()
        (world.root / "deploy" / "backup.env").write_text(
            "BACKUP_REMOTE=u1-sub1@box.example\n")
        run = world.run()
        assert run.code == 78, run.out
        assert "not shipped in the clear" in run.out
        assert not any(world.remote_root.rglob("*.gpg"))


class TestRetention:
    def test_it_deletes_its_own_oldest_beyond_n_and_nothing_else(self, world):
        world.dump()
        world.seed_remote(*OLDER, extras=("ks-warehouse-20260101-000000.tar",
                                          "notes.txt"))
        run = world.run(BACKUP_PG_RETAIN=2)
        assert run.code == 0, run.out

        stamps = sorted(p.name[3:-9] for p in world.remote.glob("ks-*.dump.gpg"))
        assert stamps == [OLDER[2], STAMP], run.out
        for stamp in OLDER[:2]:
            assert not (world.remote / f"roles-{stamp}.sql.gpg").exists()
            assert not (world.remote / f"ks-{stamp}.sha256").exists()
        # Files this script did not mint are not its to delete, whatever their
        # age: the Parquet archive's own naming lives one directory up and a
        # stray note is somebody's.
        assert (world.remote / "ks-warehouse-20260101-000000.tar").exists()
        assert (world.remote / "notes.txt").exists()

    def test_the_debris_of_an_interrupted_upload_is_not_a_copy(self, world):
        """The `.part` files a quota failure leaves behind.

        `remote_put` uploads under `.part` and renames, so a night that ran
        out of space leaves `ks-<stamp>.dump.gpg.part` on the box. Retention
        listed the remote with an unanchored `grep -o`, which matched the
        stamp *inside* that name and counted each leftover as a shipped copy:
        with five of them and BACKUP_PG_RETAIN=4, four genuine copies were
        deleted to make room for files that do not exist. Reproduced before
        the listing was anchored — exactly one real dump survived."""
        world.dump()
        world.seed_remote(*OLDER)
        phantoms = [f"2026060{n}-000000" for n in range(1, 6)]
        for stamp in phantoms:
            (world.remote / f"ks-{stamp}.dump.gpg.part").write_text("truncated")
        run = world.run(BACKUP_PG_RETAIN=4)
        assert run.code == 0, run.out

        stamps = sorted(p.name[3:-9] for p in world.remote.glob("ks-*.dump.gpg"))
        assert stamps == [*OLDER, STAMP], run.out
        # And the litter is gone, swept by the run that would otherwise have
        # left it: the bound belongs to whoever creates the file.
        assert not list(world.remote.glob("*.part")), run.out

    def test_a_ciphertext_whose_manifest_never_landed_is_not_a_copy(self, world):
        """The manifest is uploaded last, so its absence means the shipment
        did not finish — there is no sha256 to verify that dump against and
        the drill cannot use it. It must neither hold a retention slot nor
        stay on the box for ever."""
        world.dump()
        world.seed_remote(*OLDER)
        world.seed_remote("20260604-000000", manifest=False)
        run = world.run(BACKUP_PG_RETAIN=4)
        assert run.code == 0, run.out

        stamps = sorted(p.name[3:-9] for p in world.remote.glob("ks-*.dump.gpg"))
        assert stamps == [*OLDER, STAMP], run.out
        assert not (world.remote / "roles-20260604-000000.sql.gpg").exists()

    def test_an_unreadable_retention_still_sweeps_the_debris(self, world):
        """The sweep runs before RETAIN is parsed. A typo in backup.env stands
        the count retention down — correctly, it is not an answer to "how many
        copies" — but a `.part` is litter under every reading of that value."""
        world.dump()
        (world.remote).mkdir(parents=True, exist_ok=True)
        (world.remote / f"ks-20260604-000000.dump.gpg.part").write_text("x")
        run = world.run(BACKUP_PG_RETAIN="fourteen")
        assert run.code == 0, run.out
        assert "nothing pruned" in run.out
        assert not list(world.remote.glob("*.part")), run.out

    @pytest.mark.parametrize("value", ["0", "fourteen"])
    def test_a_retention_nobody_can_read_deletes_nothing(self, world, value):
        """The way a human reaches the edge of this: a typo in backup.env.
        Neither "keep zero" nor an unparseable word is an answer to "how many
        copies do we keep", so the shipment stands and the deletion stands
        down — rather than the one setting that empties the off-site archive.

        Found by running the script on a Mac: `head -n 0` prints nothing on
        GNU coreutils and is an error on BSD, so the same typo was a crash on
        one platform and a request to delete everything on the other."""
        world.dump()
        world.seed_remote(*OLDER)
        run = world.run(BACKUP_PG_RETAIN=value)
        assert run.code == 0, run.out
        assert "nothing pruned" in run.out
        assert len(list(world.remote.glob("ks-*.dump.gpg"))) == 4
        assert world.marker.exists(), "the shipment itself was fine"


class TestOneAtATime:
    def test_a_held_lock_stands_down_quietly(self, world):
        world.dump()
        run = world.run(FAKE_FLOCK_HELD="1")
        assert run.code == 0, run.out
        assert "leaving it to finish" in run.out
        assert not world.marker.exists()
        assert not run.sftp, "it talked to the remote while another run held the lock"


class TestTheLogIsBounded:
    def test_the_log_is_trimmed_by_the_run_that_writes_it(self, world):
        """The bound belongs to whoever creates the file — six things on this
        host grew without one and every one was found by a watchdog."""
        world.dump()
        for _ in range(3):
            assert world.run(BACKUP_PG_OFFSITE_LOG_LINES=4).code == 0
        assert world.log.exists()
        assert len(world.log.read_text().splitlines()) <= 4

    def test_a_failed_run_still_says_why_in_the_log(self, world):
        world.dump(body="")
        assert world.run().code != 0
        assert "is empty" in world.log.read_text()


# ── the alarm ─────────────────────────────────────────────────────────────────

class TestOffsiteCheck:
    def _instruments(self, world, offsite_h=1, watchdog_h=1, pg_h=None):
        for rel, age in (("data/.offsite_last_ok", offsite_h),
                         ("data/health/watchdog_last_sample", watchdog_h),
                         ("data/.pg_offsite_last_ok", pg_h)):
            if age is None:
                continue
            path = world.root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("2026-09-18T07:40:00Z\n")
            when = time.time() - age * 3600
            os.utime(path, (when, when))

    def test_all_three_fresh_is_quiet_and_says_so(self, world):
        self._instruments(world, pg_h=1)
        run = world.run("offsite_check.sh")
        assert run.code == 0, run.out
        assert "instruments ok" in run.out and "postgres 1h ago" in run.out

    def test_a_stale_postgres_marker_exits_non_zero(self, world):
        self._instruments(world, pg_h=40)
        run = world.run("offsite_check.sh")
        assert run.code == 1, run.out
        assert "newest Postgres copy off-site is 40h old" in run.out

    def test_a_missing_postgres_marker_is_never_not_fine(self, world):
        self._instruments(world, pg_h=None)
        run = world.run("offsite_check.sh")
        assert run.code == 1, run.out
        assert "no Postgres copy has ever left this host" in run.out

    def test_one_stopped_instrument_does_not_hide_another(self, world):
        """The regression this shape exists to prevent. On a host where the
        07:40 cron has not been installed yet the PG marker is absent every
        morning; under the old fail-fast form that would have exited before
        the disk-watchdog check ever ran, silencing it indefinitely."""
        self._instruments(world, offsite_h=100, watchdog_h=100, pg_h=None)
        run = world.run("offsite_check.sh")
        assert run.code == 1
        assert "off-site copy is 100h old" in run.out
        assert "no Postgres copy has ever left this host" in run.out
        assert "disk watchdog last sampled" in run.out


# ── the drill ─────────────────────────────────────────────────────────────────

LIVE = {
    "FAKE_LIVE_APP_ORDER_VERSIONS": 52500,
    "FAKE_LIVE_APP_MANUAL_EXPENSES": 0,
    "FAKE_LIVE_APP_STOCK_MOVEMENTS": 50500,
    "FAKE_LIVE_META_CHAIN_WATERMARKS": 4,
}
RESTORED = {
    "FAKE_RESTORED_APP_ORDER_VERSIONS": 52400,
    "FAKE_RESTORED_APP_MANUAL_EXPENSES": 0,
    "FAKE_RESTORED_APP_STOCK_MOVEMENTS": 50400,
    "FAKE_RESTORED_META_CHAIN_WATERMARKS": 4,
}


class TestTheRemoteDrill:
    def _shipped(self, world):
        world.dump()
        assert world.run().code == 0

    def test_it_restores_the_shipped_copy_and_agrees_with_live(self, world):
        self._shipped(world)
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **LIVE, **RESTORED)
        assert run.code == 0, run.out
        assert "PASS" in run.out
        assert "sha256 matches" in run.out
        # The container it started is removed with its anonymous volume.
        assert any(c.startswith("rm -f -v ks-pg-remote-drill-") for c in run.docker), run.docker
        # Live is read through the SELECT-only role and never through ks_app.
        live_reads = [c for c in run.docker if "ks-postgres" in c]
        assert live_reads and all("-U ks_readonly" in c for c in live_reads), live_reads

    def test_a_corrupted_remote_copy_is_caught_before_it_is_restored(self, world):
        self._shipped(world)
        target = world.remote / f"ks-{STAMP}.dump.gpg"
        target.write_text(target.read_text() + "tampered")
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **LIVE, **RESTORED)
        assert run.code != 0
        assert "sha256 mismatch" in run.out
        assert not any(c.startswith("run ") for c in run.docker), \
            "it started a cluster from bytes it had not verified"

    def test_a_restored_count_above_live_is_the_alarming_direction(self, world):
        """These tables only ever gain rows, so more in the dump than in live
        does not mean the dump is ahead — it means live has lost rows."""
        self._shipped(world)
        live = dict(LIVE, FAKE_LIVE_APP_ORDER_VERSIONS=52000)
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **live, **RESTORED)
        assert run.code != 0
        assert "ROWS MISSING FROM LIVE" in run.out

    def test_a_shortfall_beyond_the_margin_fails(self, world):
        self._shipped(world)
        restored = dict(RESTORED, FAKE_RESTORED_APP_STOCK_MOVEMENTS=10)
        run = world.run("pg_restore_drill.sh", ["--from-remote"],
                        **LIVE, **restored, KS_DRILL_MARGIN_ROWS=2000,
                        KS_DRILL_MARGIN_PCT=2)
        assert run.code != 0
        assert "GAP TOO LARGE" in run.out

    def test_a_day_of_lag_is_not_a_failure(self, world):
        """The rule is restored <= live within a margin, never equality: the
        dump is taken at 06:50 and the drill runs later, so a copy that
        matched exactly would be the surprising outcome."""
        self._shipped(world)
        restored = dict(RESTORED, FAKE_RESTORED_APP_ORDER_VERSIONS=52400)
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **LIVE, **restored)
        assert run.code == 0, run.out

    def test_the_one_table_that_may_shrink_may_also_be_ahead(self, world):
        """`app.manual_expenses:either`, the drill's one non-obvious rule.

        The /expenses form deletes rows, so between the 06:50 dump and the
        drill this table can legitimately lose some and the restored copy can
        hold MORE than live. For the other three — two append-only, one that
        only gains keys — that same shape means live has lost rows. It held
        zero rows in production when this was written, so every existing case
        compared 0 against 0 and the relaxation was a claim the suite could
        not defend: simplifying it to `grows` left all of them green."""
        self._shipped(world)
        live = dict(LIVE, FAKE_LIVE_APP_MANUAL_EXPENSES=1400)
        restored = dict(RESTORED, FAKE_RESTORED_APP_MANUAL_EXPENSES=1500)
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **live, **restored)
        assert run.code == 0, run.out
        assert "ROWS MISSING FROM LIVE" not in run.out

        # Same numbers on an append-only table, and it is a finding.
        live = dict(LIVE, FAKE_LIVE_APP_STOCK_MOVEMENTS=1400)
        restored = dict(RESTORED, FAKE_RESTORED_APP_STOCK_MOVEMENTS=1500)
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **live, **restored)
        assert run.code != 0, run.out
        assert "ROWS MISSING FROM LIVE" in run.out

    def test_a_table_that_may_shrink_still_has_a_margin(self, world):
        """`either` relaxes the direction, not the size: a restored copy far
        above live is a dump from another cluster, not a few deleted rows."""
        self._shipped(world)
        live = dict(LIVE, FAKE_LIVE_APP_MANUAL_EXPENSES=1400)
        restored = dict(RESTORED, FAKE_RESTORED_APP_MANUAL_EXPENSES=9000)
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **live, **restored)
        assert run.code != 0, run.out
        assert "GAP TOO LARGE" in run.out

    def test_a_failed_drill_reaches_a_human(self, world):
        """It printed FAIL: to root's local mail and nothing read it, while
        its sibling deploy/pg_pitr_drill.sh has alerted since it was written.
        The kill switch is on in this fixture, so what is asserted is that
        notify() was reached at all — with it off, this is a Telegram
        message to the admins."""
        self._shipped(world)
        target = world.remote / f"ks-{STAMP}.dump.gpg"
        target.write_text(target.read_text() + "tampered")
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **LIVE, **RESTORED)
        assert run.code != 0
        assert "notify suppressed" in run.out, run.out
        assert "did not restore" in run.out

    def test_quiet_is_for_the_keyboard_and_not_for_cron(self, world):
        """Developing pg_pitr_drill.sh sent admins two "drill failed" messages
        on 2026-08-31 for a fault that was being fixed as they arrived. Same
        flag, same reason; cron runs without it."""
        self._shipped(world)
        target = world.remote / f"ks-{STAMP}.dump.gpg"
        target.write_text(target.read_text() + "tampered")
        run = world.run("pg_restore_drill.sh", ["--from-remote", "--quiet"],
                        **LIVE, **RESTORED)
        assert run.code != 0
        assert "notify suppressed" not in run.out, run.out

    def test_the_local_drill_does_not_borrow_the_remote_drill_s_alert(self, world):
        """Arming `fail` unconditionally would have made the other drill page
        under a key that names the off-site copy, which it never touches."""
        run = world.run("pg_restore_drill.sh", [], **LIVE, **RESTORED)
        assert run.code != 0, run.out
        assert "notify suppressed" not in run.out, run.out

    def test_an_empty_table_says_it_proves_nothing(self, world):
        """app.manual_expenses held zero rows in production when this was
        written, so 0 == 0 passes and must not be read as evidence."""
        self._shipped(world)
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **LIVE, **RESTORED)
        assert "proves nothing yet" in run.out

    def test_it_drills_the_newest_real_copy_past_any_debris(self, world):
        """The drill takes `remote_stamps | head -1` and never sweeps, so this
        is the other place the anchored listing carries the weight alone.

        Under the unanchored listing a `.part` newer than the newest complete
        shipment became the copy under test, and the drill died fetching a
        manifest that was never written — an instrument reporting on the
        state of its own input rather than on the backup."""
        self._shipped(world)
        (world.remote / "ks-20260919-000000.dump.gpg.part").write_text("truncated")
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **LIVE, **RESTORED)
        assert run.code == 0, run.out
        assert STAMP in run.out and "20260919-000000" not in run.out, run.out

    def test_it_refuses_when_nothing_has_been_shipped(self, world):
        run = world.run("pg_restore_drill.sh", ["--from-remote"], **LIVE, **RESTORED)
        assert run.code != 0
        assert "nothing shipped" in run.out


@pytest.mark.skipif(shutil.which("gpg") is None, reason="no real gpg on this machine")
def test_real_gpg_round_trips(tmp_path):
    """The fakes prove the plumbing; this proves the one call that carries the
    secret is spelled in a way a real gpg accepts, symmetrically, from a file
    and with no tty."""
    passfile = tmp_path / "pass"
    passfile.write_text("a-passphrase\n")
    plain = tmp_path / "plain"
    plain.write_text("0501234567 Ivanenko\n")
    enc, back = tmp_path / "plain.gpg", tmp_path / "back"

    subprocess.run(["gpg", "--symmetric", "--cipher-algo", "AES256", "--batch",
                    "--yes", "--passphrase-file", str(passfile), "-o", str(enc),
                    str(plain)], check=True, capture_output=True)
    assert b"0501234567" not in enc.read_bytes()
    subprocess.run(["gpg", "--batch", "--yes", "--quiet", "--passphrase-file",
                    str(passfile), "-o", str(back), "-d", str(enc)],
                   check=True, capture_output=True)
    assert back.read_text() == plain.read_text()
