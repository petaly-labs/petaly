#!/bin/bash
# Sync a bug fix across multiple version branches
# Usage: ./scripts/sync-fix.sh <commit-hash> [branch1] [branch2] ...

set -e

COMMIT_HASH=$1
shift
VERSIONS=("$@")

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

if [ -z "$COMMIT_HASH" ]; then
    echo -e "${RED}Error: Commit hash required${NC}"
    echo "Usage: $0 <commit-hash> [branch1] [branch2] ..."
    echo "Example: $0 abc1234 v1.0 v1.1 v2.0"
    exit 1
fi

# If no branches specified, use common version branches
if [ ${#VERSIONS[@]} -eq 0 ]; then
    VERSIONS=("v1.0" "v1.1" "v2.0")
    echo -e "${YELLOW}No branches specified, using default: ${VERSIONS[*]}${NC}"
fi

# Get current branch
CURRENT_BRANCH=$(git branch --show-current)
echo -e "${GREEN}Current branch: $CURRENT_BRANCH${NC}"
echo -e "${GREEN}Cherry-picking commit: $COMMIT_HASH${NC}"
echo ""

# Verify commit exists
if ! git cat-file -e "$COMMIT_HASH" 2>/dev/null; then
    echo -e "${RED}Error: Commit $COMMIT_HASH not found${NC}"
    exit 1
fi

# Show commit info
echo -e "${GREEN}Commit info:${NC}"
git log -1 --oneline "$COMMIT_HASH"
echo ""

# Ask for confirmation
read -p "Apply this fix to branches: ${VERSIONS[*]}? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

SUCCESSFUL=()
FAILED=()

# Cherry-pick to each branch
for version in "${VERSIONS[@]}"; do
    echo -e "${YELLOW}Processing $version...${NC}"
    
    # Check if branch exists
    if ! git show-ref --verify --quiet refs/heads/$version && ! git show-ref --verify --quiet refs/remotes/origin/$version; then
        echo -e "${RED}  ⚠️  Branch $version not found, skipping${NC}"
        FAILED+=("$version (not found)")
        continue
    fi
    
    # Checkout branch
    git checkout $version 2>/dev/null || git checkout -b $version origin/$version 2>/dev/null
    git pull origin $version 2>/dev/null || true
    
    # Check if commit already exists in this branch
    if git log --oneline | grep -q "$(git log -1 --format='%h' $COMMIT_HASH)"; then
        echo -e "${YELLOW}  ⚠️  Commit already in $version, skipping${NC}"
        continue
    fi
    
    # Attempt cherry-pick
    if git cherry-pick $COMMIT_HASH 2>/dev/null; then
        echo -e "${GREEN}  ✅ Successfully applied to $version${NC}"
        SUCCESSFUL+=("$version")
        
        # Ask if should push
        read -p "  Push $version to origin? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            git push origin $version
            echo -e "${GREEN}  ✅ Pushed $version${NC}"
        fi
    else
        echo -e "${RED}  ⚠️  Conflicts in $version - resolve manually${NC}"
        echo -e "${YELLOW}  To resolve:${NC}"
        echo -e "${YELLOW}    1. Fix conflicts${NC}"
        echo -e "${YELLOW}    2. git add <resolved-files>${NC}"
        echo -e "${YELLOW}    3. git cherry-pick --continue${NC}"
        echo -e "${YELLOW}  Or abort: git cherry-pick --abort${NC}"
        FAILED+=("$version (conflicts)")
        git cherry-pick --abort 2>/dev/null || true
    fi
    
    echo ""
done

# Return to original branch
git checkout $CURRENT_BRANCH 2>/dev/null || true

# Summary
echo -e "${GREEN}Summary:${NC}"
if [ ${#SUCCESSFUL[@]} -gt 0 ]; then
    echo -e "${GREEN}  ✅ Successfully applied to: ${SUCCESSFUL[*]}${NC}"
fi
if [ ${#FAILED[@]} -gt 0 ]; then
    echo -e "${RED}  ⚠️  Failed: ${FAILED[*]}${NC}"
fi

