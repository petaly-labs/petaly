#!/bin/bash
# Version management helper script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get version from VERSION file or pyproject.toml
get_version() {
    if [ -f VERSION ]; then
        cat VERSION
    elif [ -f pyproject.toml ]; then
        grep '^version =' pyproject.toml | sed 's/version = "\(.*\)"/\1/'
    else
        echo "0.0.0"
    fi
}

# Get current branch
get_branch() {
    git branch --show-current 2>/dev/null || echo "unknown"
}

# Show version info
show_version() {
    VERSION=$(get_version)
    BRANCH=$(get_branch)
    
    echo -e "${GREEN}Current Version:${NC} $VERSION"
    echo -e "${GREEN}Current Branch:${NC} $BRANCH"
    
    # Check if branch matches version pattern (v1.0, v1.1, v2.0, etc.)
    if [[ $BRANCH =~ ^v([0-9]+\.[0-9]+) ]]; then
        BRANCH_VERSION="${BASH_REMATCH[1]}"
        echo -e "${GREEN}Branch Version:${NC} v$BRANCH_VERSION"
    fi
    
    # Compare with main if main exists
    if git show-ref --verify --quiet refs/heads/main || git show-ref --verify --quiet refs/heads/master; then
        MAIN_BRANCH=$(git show-ref --verify --quiet refs/heads/main && echo "main" || echo "master")
        AHEAD=$(git rev-list --count ${MAIN_BRANCH}..HEAD 2>/dev/null || echo "0")
        BEHIND=$(git rev-list --count HEAD..${MAIN_BRANCH} 2>/dev/null || echo "0")
        
        echo -e "${YELLOW}Commits ahead of ${MAIN_BRANCH}:${NC} $AHEAD"
        echo -e "${YELLOW}Commits behind ${MAIN_BRANCH}:${NC} $BEHIND"
    fi
}

# Update version
update_version() {
    NEW_VERSION=$1
    if [ -z "$NEW_VERSION" ]; then
        echo -e "${RED}Error: Version required${NC}"
        echo "Usage: $0 update <version>"
        exit 1
    fi
    
    # Update VERSION file
    if [ -f VERSION ]; then
        echo "$NEW_VERSION" > VERSION
        echo -e "${GREEN}Updated VERSION file:${NC} $NEW_VERSION"
    fi
    
    # Update pyproject.toml
    if [ -f pyproject.toml ]; then
        sed -i.bak "s/^version = \".*\"/version = \"$NEW_VERSION\"/" pyproject.toml
        rm -f pyproject.toml.bak
        echo -e "${GREEN}Updated pyproject.toml:${NC} $NEW_VERSION"
    fi
}

# Compare branches
compare_branches() {
    BRANCH1=${1:-main}
    BRANCH2=${2:-HEAD}
    
    echo -e "${GREEN}Comparing ${BRANCH1}..${BRANCH2}${NC}"
    echo ""
    echo -e "${YELLOW}Commits in ${BRANCH2} not in ${BRANCH1}:${NC}"
    git log ${BRANCH1}..${BRANCH2} --oneline | head -10
    echo ""
    echo -e "${YELLOW}File changes:${NC}"
    git diff ${BRANCH1}..${BRANCH2} --stat
}

# Main command handler
case "$1" in
    show|"")
        show_version
        ;;
    update)
        update_version "$2"
        ;;
    compare)
        compare_branches "$2" "$3"
        ;;
    *)
        echo "Usage: $0 {show|update <version>|compare [branch1] [branch2]}"
        exit 1
        ;;
esac

