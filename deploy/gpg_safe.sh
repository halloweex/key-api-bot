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
# --symmetric with --no-autostart writes the file and still exits 2. Only
# encryption needs it: a decrypt with no agent at all returned 0 with the right
# plaintext (review of #290), so the second ask on the read-backs and the
# drill's decrypts is a guard that has never yet had to fire.
#
# Asked again only when that reads the same bytes: the call's last argument is
# a named regular file. gpg reads a pipe or stdin to the end before it finds the
# agent missing, so a second ask would encrypt an empty stream and return 0 —
# a 70-byte ciphertext that every `[ -s file ]` check accepts (reproduced in
# the review). A stream gets one attempt, as gpg alone always did.
#
# Bounds. Waiting: 3 s and 10 s, plus up to ~7 s per ask while gpg gives up on
# a socket — about half a minute at worst. Litter: the home and its agent's
# socket directory are removed with the call; a call killed by a signal leaves
# its home in /tmp, because a sourced function must not take the caller's
# traps — its agent exits when the socket directory goes.
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
    local home err rc delay attempt=1 retry=0
    # Only a named input file can be read twice (see the header).
    if [ "$#" -gt 0 ] && [ "${!#}" != "-" ] && [ -f "${!#}" ]; then
        retry=1
    fi
    # /tmp and not $TMPDIR: the socket can land in the home, and a Unix socket
    # path is limited to about a hundred bytes.
    home="$(mktemp -d /tmp/ks-gnupg.XXXXXX)" || return 1
    err="$home/stderr"
    # The default again, not only at the top: a caller that unsets it after
    # sourcing would otherwise die on `set -u` with no ERR trap and no alert.
    # shellcheck disable=SC2086  # the delays are a list of words on purpose
    for delay in ${GPG_SAFE_DELAYS-3 10} ""; do
        rc=0
        GNUPGHOME="$home" gpg --no-keyring "$@" 2>"$err" || rc=$?
        # `|| true` on everything that is not gpg: the caller's errexit and ERR
        # trap reach in here, and a failed cat, sleep or rm must not stand in
        # for gpg's own status, or name a step for a gpg call that succeeded.
        cat "$err" >&2 || true
        if [ "$rc" -eq 0 ] || [ -z "$delay" ] || [ "$retry" -eq 0 ] \
                || ! grep -qE "$_GPG_SAFE_NO_AGENT" "$err"; then
            break
        fi
        echo "gpg: no agent (attempt $attempt), asking again in ${delay}s" >&2
        gpgconf --homedir "$home" --kill gpg-agent >/dev/null 2>&1 || true
        sleep "$delay" || true          # a malformed delay costs only the wait
        attempt=$((attempt + 1))
    done
    gpgconf --homedir "$home" --kill all >/dev/null 2>&1 || true
    # The d.<hash> directory under /run/user/0/gnupg outlives the agent
    # otherwise: one per call for as long as a root session is open.
    gpgconf --homedir "$home" --remove-socketdir >/dev/null 2>&1 || true
    rm -rf "$home" || true
    return "$rc"
}
