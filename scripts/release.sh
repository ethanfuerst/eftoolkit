#!/usr/bin/env bash
#
# Release script for eftoolkit
#
# Usage:
#   ./scripts/release.sh major    # 0.1.0 -> 1.0.0
#   ./scripts/release.sh minor    # 0.1.0 -> 0.2.0
#   ./scripts/release.sh patch    # 0.1.0 -> 0.1.1
#
# This script will:
#   1. Auto-bump the version based on major/minor/patch
#   2. Run pre-release checks (tests, linting, docs build)
#   3. Update the version in pyproject.toml
#   4. Commit the version bump
#   5. Create and push a git tag
#   6. Generate release notes and create a GitHub release

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check if bump type argument is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <bump_type>"
    echo ""
    echo "Bump types:"
    echo "  major    Bump major version (0.1.0 -> 1.0.0)"
    echo "  minor    Bump minor version (0.1.0 -> 0.2.0)"
    echo "  patch    Bump patch version (0.1.0 -> 0.1.1)"
    exit 1
fi

BUMP_TYPE="$1"

# Validate bump type
if [[ "$BUMP_TYPE" != "major" && "$BUMP_TYPE" != "minor" && "$BUMP_TYPE" != "patch" ]]; then
    log_error "Invalid bump type: $BUMP_TYPE"
    echo "Must be one of: major, minor, patch"
    exit 1
fi

# Get current version from pyproject.toml
CURRENT_VERSION=$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')

# Parse current version
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Calculate new version
case "$BUMP_TYPE" in
    major)
        NEW_MAJOR=$((MAJOR + 1))
        VERSION="${NEW_MAJOR}.0.0"
        ;;
    minor)
        NEW_MINOR=$((MINOR + 1))
        VERSION="${MAJOR}.${NEW_MINOR}.0"
        ;;
    patch)
        NEW_PATCH=$((PATCH + 1))
        VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"
        ;;
esac

TAG="v$VERSION"

# Check we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    log_error "Must be on 'main' branch to release (currently on '$CURRENT_BRANCH')"
    exit 1
fi

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    log_error "Working directory has uncommitted changes"
    echo "Please commit or stash your changes before releasing"
    exit 1
fi

# Check if tag already exists
if git rev-parse "$TAG" >/dev/null 2>&1; then
    log_error "Tag $TAG already exists"
    exit 1
fi

# Show what we're about to do
echo ""
echo -e "${CYAN}Release Summary${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "  Bump type:       ${YELLOW}$BUMP_TYPE${NC}"
echo -e "  Current version: ${YELLOW}$CURRENT_VERSION${NC}"
echo -e "  New version:     ${GREEN}$VERSION${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Confirm with user
read -p "Proceed with release? [y/N] " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_warn "Release cancelled"
    exit 0
fi

# Pull latest changes
log_info "Pulling latest changes from origin/main..."
git pull origin main

# Run pre-release checks
log_info "Running tests..."
uv run pytest

log_info "Running linting..."
uv run pre-commit run --all-files

log_info "Building documentation..."
uv run mkdocs build --strict

# Get the previous tag for release notes
PREVIOUS_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")

# Generate release notes
log_info "Generating release notes..."
RELEASE_NOTES=$(mktemp)

{
    echo "## What's Changed"
    echo ""

    # Get commits since last tag (or all commits if no previous tag)
    if [ -n "$PREVIOUS_TAG" ]; then
        # Group commits by type based on conventional commit prefixes
        FEATURES=$(git log "$PREVIOUS_TAG"..HEAD --pretty=format:"- %s" --grep="^feat" --regexp-ignore-case 2>/dev/null || true)
        FIXES=$(git log "$PREVIOUS_TAG"..HEAD --pretty=format:"- %s" --grep="^fix" --regexp-ignore-case 2>/dev/null || true)
        DOCS=$(git log "$PREVIOUS_TAG"..HEAD --pretty=format:"- %s" --grep="^docs" --regexp-ignore-case 2>/dev/null || true)

        # Get all other commits
        OTHER=$(git log "$PREVIOUS_TAG"..HEAD --pretty=format:"- %s" \
            --invert-grep --grep="^feat" --grep="^fix" --grep="^docs" \
            --regexp-ignore-case 2>/dev/null || true)

        if [ -n "$FEATURES" ]; then
            echo "### Features"
            echo "$FEATURES"
            echo ""
        fi

        if [ -n "$FIXES" ]; then
            echo "### Bug Fixes"
            echo "$FIXES"
            echo ""
        fi

        if [ -n "$DOCS" ]; then
            echo "### Documentation"
            echo "$DOCS"
            echo ""
        fi

        if [ -n "$OTHER" ]; then
            echo "### Other Changes"
            echo "$OTHER"
            echo ""
        fi

        echo "**Full Changelog**: https://github.com/ethanfuerst/eftoolkit/compare/${PREVIOUS_TAG}...${TAG}"
    else
        echo "Initial release of eftoolkit!"
        echo ""
        echo "### Features"
        echo "- DuckDB wrapper with S3 integration"
        echo "- S3FileSystem for parquet read/write operations"
        echo "- Google Sheets client with batching and local preview"
        echo "- Configuration utilities (JSON loading, logging setup)"
    fi
} > "$RELEASE_NOTES"

# Show release notes preview
echo ""
echo -e "${CYAN}Release Notes Preview${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cat "$RELEASE_NOTES"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Update version in pyproject.toml
log_info "Updating version in pyproject.toml..."
sed -i '' "s/^version = \".*\"/version = \"$VERSION\"/" pyproject.toml

# Commit the version bump
log_info "Committing version bump..."
git add pyproject.toml
git commit -m "Release v$VERSION"

# Create and push tag
log_info "Creating tag $TAG..."
git tag -a "$TAG" -m "Release $VERSION"

log_info "Pushing commits and tag to origin..."
git push origin main
git push origin "$TAG"

# Create GitHub release with generated notes
log_info "Creating GitHub release..."
gh release create "$TAG" \
    --title "$TAG" \
    --notes-file "$RELEASE_NOTES"

# Cleanup
rm -f "$RELEASE_NOTES"

log_info "Release $VERSION complete!"
echo ""
echo "Next steps:"
echo "  1. Monitor the publish workflow: https://github.com/ethanfuerst/eftoolkit/actions"
echo "  2. Verify on PyPI: https://pypi.org/project/eftoolkit/"
echo "  3. Test install: uv pip install eftoolkit==$VERSION"
