#!/usr/bin/env bash
set -euo pipefail

# bump_version.sh — Called by CI after deploy to increment patch version,
# generate changelog entry, commit, tag, and push.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION_FILE="$REPO_ROOT/VERSION"
CHANGELOG_FILE="$REPO_ROOT/CHANGELOG.md"

# 1. Read current version
CURRENT_VERSION=$(cat "$VERSION_FILE" | tr -d '[:space:]')
echo "Current version: $CURRENT_VERSION"

# 2. Increment patch
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"
NEW_PATCH=$((PATCH + 1))
NEW_VERSION="$MAJOR.$MINOR.$NEW_PATCH"
echo "New version: $NEW_VERSION"

# 3. Generate changelog entry from git log since last tag
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

# 4. Prepend entry to CHANGELOG.md (after the header)
HEADER=$(head -3 "$CHANGELOG_FILE")
BODY=$(tail -n +4 "$CHANGELOG_FILE")
{
    echo "$HEADER"
    echo ""
    echo -e "$ENTRY"
    echo "$BODY"
} > "$CHANGELOG_FILE"

# 5. Write new version
echo "$NEW_VERSION" > "$VERSION_FILE"

# 6. Commit with [skip ci]
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
git add "$VERSION_FILE" "$CHANGELOG_FILE"
git commit -m "chore: bump version to $NEW_VERSION [skip ci]"

# 7. Create git tag
git tag "v$NEW_VERSION"

# 8. Push commit + tag
git push origin main
git push origin "v$NEW_VERSION"

echo "Bumped to $NEW_VERSION and pushed tag v$NEW_VERSION"
