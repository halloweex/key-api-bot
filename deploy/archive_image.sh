# shellcheck shell=bash
# The image the DuckDB archive tools run in (step 14, S14-02).
#
# Sourced by deploy/restore-test.sh, deploy/daily_offsite.sh and
# deploy/ark_ship.sh — the three host scripts that start a container to read a
# DuckDB archive with code mounted from this checkout. It defines and does not
# act, and it sets no shell options: errexit is the caller's. Bash 3.2 and up,
# because a laptop runs the tests.
#
# WHY A PINNED IMAGE, AND NOT :latest
# Step 14 takes DuckDB out of the application: its code leaves main PR by PR,
# and the `duckdb` package leaves the images at S14-19. The archives do not
# leave — the off-site Parquet archive, the nightly snapshots, two Arks — and
# what reads them is DuckDB 1.5.5, the version that wrote every one of them,
# with the restore code they were proven against (scripts/compact_duckdb.py's
# phases 2 and 3, from the image's /app/scripts). Both live in every image
# built before step 14, and the host tags the last of them — the image
# production ran when step 14's S14-00 deployed — `rollback-step14`, pinned at
# that deploy (the plan's H1/H2): a tag `docker image prune -f` never removes,
# on no registry, so nothing ever pulls it.
#
# `:latest` moves with every deploy, and from S14-19 on it carries no DuckDB
# at all. So each tool takes its image from a variable that must be set (in
# deploy/backup.env, see deploy/backup.env.example) and asks the image before
# using it:
#   - the variable is set, to a tag that does not move: not `:latest`, not
#     untagged (which is `:latest`); a digest pins by construction;
#   - the image is on this host: nothing here pulls;
#   - `import duckdb` inside it says 1.5.5.
# Any of them false is a refusal, exit 2 in every caller, before anything is
# downloaded, linked, frozen or shipped, with a line naming the tag to use.
#
# The host files mounted into that image (restore_from_export.py,
# snapshot_export.py, core/snapshot_validation.py, ark_freeze.py) import only
# what the image has; tests/unit/test_archive_image.py holds them to it.

ARCHIVE_DUCKDB_VERSION="1.5.5"
ARCHIVE_PINNED_IMAGE="halloweex/keycrm-web:rollback-step14"

_archive_image_hint() {
    printf 'run it on the image tagged rollback-step14 (the last image before step 14, pinned at S14-00'"'"'s deploy; DuckDB %s): %s=%s in deploy/backup.env' \
        "$ARCHIVE_DUCKDB_VERSION" "$1" "$ARCHIVE_PINNED_IMAGE"
}

# archive_image_require VAR VALUE
# Whether VALUE, the value of the variable named VAR, names a pinned image.
# Asks nothing of docker. Silent and 0 when it does; otherwise prints why on
# stdout, for the caller to word, and returns 2.
archive_image_require() {
    local var="$1" image="${2:-}" name
    if [ -z "$image" ]; then
        printf '%s is not set, and the archive tools no longer default to :latest; %s\n' \
            "$var" "$(_archive_image_hint "$var")"
        return 2
    fi
    case "$image" in
        *@sha256:*) return 0 ;;
    esac
    # The tag is after the last ':' of the last path component: a registry's
    # host:port sits before a '/'.
    name="${image##*/}"
    case "$name" in
        *:latest)
            printf '%s=%s moves with every deploy, and from step 14'"'"'s S14-19 carries no DuckDB; %s\n' \
                "$var" "$image" "$(_archive_image_hint "$var")"
            return 2 ;;
        *:?*) return 0 ;;
        *)
            printf '%s=%s names no tag, which is :latest; %s\n' \
                "$var" "$image" "$(_archive_image_hint "$var")"
            return 2 ;;
    esac
}

# archive_image_probe VAR IMAGE
# Whether IMAGE is on this host and carries DuckDB ARCHIVE_DUCKDB_VERSION, asked
# of the image itself in a throwaway container: no network, never pulled.
# Silent and 0 when it does; otherwise prints why on stdout and returns 2.
archive_image_probe() {
    local var="$1" image="$2" out last
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        printf 'no image %s on this host, and this does not pull (rollback-step14 is a local tag, on no registry); %s\n' \
            "$image" "$(_archive_image_hint "$var")"
        return 2
    fi
    if ! out="$(docker run --rm --pull never --network none --entrypoint python3 "$image" \
            -c 'import duckdb; print(duckdb.__version__)' 2>&1)"; then
        last="$(printf '%s\n' "$out" | tail -n 1 | tr -cd '[:print:]' | cut -c1-200)"
        printf '%s cannot import duckdb (%s); %s\n' "$image" "$last" "$(_archive_image_hint "$var")"
        return 2
    fi
    last="$(printf '%s\n' "$out" | tail -n 1 | tr -cd '[:print:]' | cut -c1-40)"
    if [ "$last" != "$ARCHIVE_DUCKDB_VERSION" ]; then
        printf '%s carries DuckDB %s, not %s, the version that wrote every archive; %s\n' \
            "$image" "${last:-?}" "$ARCHIVE_DUCKDB_VERSION" "$(_archive_image_hint "$var")"
        return 2
    fi
    return 0
}

# archive_image_ready VAR VALUE — require, then probe.
archive_image_ready() {
    archive_image_require "$1" "${2:-}" || return 2
    archive_image_probe "$1" "$2" || return 2
}
