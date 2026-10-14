#!/usr/bin/env bash
# gpg_safe: every gpg the backup scripts run, in a home of its own, asked again
# when it finds no agent.
#
# On 2026-10-10 at 07:40 UTC, deploy/pg_offsite.sh failed at `encrypt`:
#   gpg: error running '/usr/bin/gpg-agent': exit status 2
#   gpg: problem with the agent: No agent running
# Eleven seconds earlier, a root SSH login had started systemd's user manager
# for uid 0. That manager listens on gpg-agent's sockets in /run/user/0/gnupg,
# which is the socket directory of gpg's DEFAULT home. Cron's gpg asked that
# socket for an agent while the manager was coming up or going down, got none,
# then tried to start its own and could not take the name systemd held. The
# same run succeeded twice by hand that evening and has every night since. The
# cost was one night's off-site copy. Any cron job here that runs gpg as root
# shares the race; the ones that ship or verify the copies are the ones that
# matter.
#
# Two things close it.
#
# * A home of its own, created per call with mktemp and removed after, moves
#   the agent's socket. It goes to /run/user/0/gnupg/d.<hash of the home>,
#   which no systemd unit listens on, or into the home itself when /run/user/0
#   does not exist. Measured on the host: `gpgconf --homedir <tmp> --list-dirs
#   socketdir` gives the d.<hash> directory, while the default home's sockets
#   are owned by gpg-agent.socket. --no-keyring keeps the fresh home from
#   announcing a new keybox in every log; symmetric encryption needs no keys.
# * /run/user/0 is still removed under a running agent when the last root
#   session ends. So a call that failed for want of an agent is asked again,
#   after its delays, with that home's agent killed first. Nothing else is
#   retried: a wrong passphrase, a missing file or a full disk fails the first
#   time, as it always has. Every call here reads and writes named files, so
#   asking twice writes nothing twice.
#
# Running without an agent is not an option. Measured on the host's gpg 2.4.4:
# --symmetric with --no-autostart writes the file and still exits 2.
#
# Sourced from the repository root, like deploy/pg_offsite_lib.sh. It defines
# and does not act, and it sets no shell options: errexit and the ERR trap
# belong to the caller, and a failed call returns gpg's own status for the
# caller's trap to name.

# Seconds to wait before each second ask. Tests set it to "0 0".
GPG_SAFE_DELAYS="${GPG_SAFE_DELAYS:-3 10}"

# Matched against what gpg said, never against the exit code: gpg exits 2 for
# a wrong passphrase too.
_GPG_SAFE_NO_AGENT='gpg-agent|No agent running|problem with the agent'

gpg_safe() {
    local home err rc delay attempt=1
    # /tmp and not $TMPDIR: the socket can land in the home, and a Unix socket
    # path is limited to about a hundred bytes.
    home="$(mktemp -d /tmp/ks-gnupg.XXXXXX)" || return 1
    err="$home/stderr"
    # shellcheck disable=SC2086  # the delays are a list of words on purpose
    for delay in $GPG_SAFE_DELAYS ""; do
        rc=0
        GNUPGHOME="$home" gpg --no-keyring "$@" 2>"$err" || rc=$?
        cat "$err" >&2
        if [ "$rc" -eq 0 ] || [ -z "$delay" ] \
                || ! grep -qE "$_GPG_SAFE_NO_AGENT" "$err"; then
            break
        fi
        echo "gpg: no agent (attempt $attempt), asking again in ${delay}s" >&2
        gpgconf --homedir "$home" --kill gpg-agent >/dev/null 2>&1 || true
        sleep "$delay"
        attempt=$((attempt + 1))
    done
    gpgconf --homedir "$home" --kill all >/dev/null 2>&1 || true
    rm -rf "$home"
    return "$rc"
}
