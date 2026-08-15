#!/usr/bin/env bash
set -euo pipefail

# bump_version.sh — Called by CI after deploy to increment patch version,
# generate changelog entry, commit, tag, and push.
#
# Order is the whole story here. The sync with origin/main must happen while
# the working tree is still clean, because `git rebase` refuses outright to
# run with unstaged changes — even when there is nothing to rebase. Writing
# VERSION and CHANGELOG.md first and syncing afterwards therefore failed on
# every single run, and `|| exit 0` reported that failure as success: from
# 2026-04-10 (the commit that added the rebase) to 2026-08-15, twelve deploys
# shipped with no bump, no tag and no changelog entry, every one of them on a
# green check. VERSION sat at 3.0.76 while both Docker images kept being
# pushed under that same tag, so no version identified a build any more.
#
# Syncing first also does the job the rebase was added for, and does it
# better: a concurrent run's bump is now visible *before* this run picks the
# next number, rather than after it has already committed to one.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION_FILE="$REPO_ROOT/VERSION"
CHANGELOG_FILE="$REPO_ROOT/CHANGELOG.md"

# 1. Sync to the tip of main, tree still clean. Tags come too — the changelog
#    range below is measured from the newest one.
git fetch origin main --tags
git checkout -B main origin/main

# 2. Read current version, after the sync so a concurrent bump counts
CURRENT_VERSION=$(tr -d '[:space:]' < "$VERSION_FILE")
echo "Current version: $CURRENT_VERSION"

# 3. Increment patch
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"
NEW_PATCH=$((PATCH + 1))
NEW_VERSION="$MAJOR.$MINOR.$NEW_PATCH"
echo "New version: $NEW_VERSION"

# 4. Nothing to do if another run already claimed this number
if git ls-remote --tags origin "refs/tags/v$NEW_VERSION" | grep -q .; then
    echo "Tag v$NEW_VERSION already exists on remote — concurrent run handled it"
    exit 0
fi

# 5. Generate changelog entry from git log since last tag
LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [ -n "$LAST_TAG" ]; then
    COMMITS=$(git log "$LAST_TAG"..HEAD --oneline --no-merges 2>/dev/null || echo "No changes recorded")
else
    COMMITS=$(git log --oneline -20 --no-merges 2>/dev/null || echo "No changes recorded")
fi

# Build changelog entry
ENTRY="## $NEW_VERSION\n\n"
while IFS= read -r line; do
    if [ -n "$line" ]; then
        # Strip commit hash, keep message
        MSG=$(echo "$line" | sed 's/^[a-f0-9]* //')
        ENTRY+="- $MSG\n"
    fi
done <<< "$COMMITS"

# 6. Prepend entry to CHANGELOG.md (after the header)
HEADER=$(head -3 "$CHANGELOG_FILE")
BODY=$(tail -n +4 "$CHANGELOG_FILE")
{
    echo "$HEADER"
    echo ""
    echo -e "$ENTRY"
    echo "$BODY"
} > "$CHANGELOG_FILE"

# 7. Write new version
echo "$NEW_VERSION" > "$VERSION_FILE"

# 8. Commit with [skip ci]
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
git add "$VERSION_FILE" "$CHANGELOG_FILE"
git commit -m "chore: bump version to $NEW_VERSION [skip ci]"

# 9. Create git tag
git tag "v$NEW_VERSION"

# 10. Push commit, then tag.
#     A rejected push means main moved under us between the fetch above and
#     now. That is the one failure worth surviving quietly, and it is stated
#     as itself — not as a blanket "something went wrong, call it a success".
#     Everything else fails the step, loudly, which is what four silent
#     months cost.
if ! git push origin main; then
    echo "::warning::push rejected — main moved during this run; the next deploy's bump will catch up"
    exit 0
fi
git push origin "v$NEW_VERSION"

echo "Bumped to $NEW_VERSION and pushed tag v$NEW_VERSION"
