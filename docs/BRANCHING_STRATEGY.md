# Branching Strategy for Open Source Projects

This document outlines the recommended branching strategy for Petaly, designed to handle breaking changes, long stabilization periods, and multiple concurrent versions.

## Branch Structure

### Core Branches

1. **`main`** (protected, stable)
   - Production-ready, stable code
   - Always deployable
   - Only receives merges from version branches or hotfixes
   - Tagged releases come from this branch

2. **`devel`** (protected, integration)
   - Integration branch for all development
   - Receives PRs from contributors
   - May contain breaking changes in progress
   - Merged to version branches when features are stable

### Version Branches

3. **`v1.0`**, **`v1.1`**, **`v2.0`**, etc. (protected, release branches)
   - Created from `main` when a new major.minor version is ready
   - Long-lived branches for a specific version line
   - Receives patches and bug fixes
   - Merged back to `main` when stable
   - Tags (e.g., `v1.0.1`, `v1.0.2`) are created from these branches

### Feature Branches

4. **`feature/*`** (temporary)
   - Short-lived branches for new features
   - PRs target `devel`
   - Deleted after merge

5. **`fix/*`** (temporary)
   - Bug fixes
   - PRs target version branches (e.g., `v1.0`) or `devel`
   - Deleted after merge

## Recommended Structure

```
main (stable, protected)
├── v1.0 (version branch, protected)
│   ├── v1.0.1 (tag)
│   ├── v1.0.2 (tag)
│   └── fix/* branches → PR to v1.0
├── v1.1 (version branch, protected)
│   ├── v1.1.0 (tag)
│   └── fix/* branches → PR to v1.1
├── v2.0 (version branch, protected, breaking changes)
│   ├── v2.0.0 (tag)
│   └── feature/* branches → PR to v2.0
└── devel (integration, protected)
    ├── feature/* branches → PR to devel
    └── Breaking changes develop here
```

## Understanding `devel` vs Version Branches

**Key Concept**: `devel` is the **integration point** where all development happens first. Version branches are created **from** `devel` when features are ready to be released.

### ⚠️ Important: Selective Merging Strategy

**Problem**: If everything merges into `devel`, how do you separate ready features from unstable ones?

**Solution**: Use **selective merging** - don't merge everything into `devel` blindly. Keep features separate until they're ready.

### The Flow:
```
feature/* branches → (selective merge) → devel → v1.1 (version branch) → main (stable)
```

**When to use `devel`:**
- ✅ **Stable, tested features** ready for integration
- ✅ Features that have passed review and testing
- ✅ Breaking changes that are **complete and tested**
- ✅ Integration testing of **ready features together**

**When to keep features separate:**
- ⚠️ **Unstable or incomplete features** → Keep in `feature/*` branches
- ⚠️ **Experimental features** → Keep separate, don't merge to `devel` yet
- ⚠️ **Features under heavy development** → Keep in their own branches

**When to use version branches:**
- ✅ Bug fixes for a specific version
- ✅ **Cherry-picked** ready features from `devel`
- ✅ Stable code that users depend on

## Workflow Scenarios

### Scenario 1: New Feature (Backward Compatible) - **Selective Approach**

**Key Strategy**: Only merge **ready** features to `devel`. Keep unstable features separate.

```bash
# 1. Create feature branch from devel
git checkout devel
git pull origin devel
git checkout -b feature/new-parquet-support

# 2. Develop and commit (may take weeks)
git add .
git commit -m "Add Parquet file connector support"
# ... more commits ...

# 3. Feature is complete and tested
#    - Code review passed
#    - Tests pass
#    - Documentation updated
#    → NOW create PR: feature/new-parquet-support → devel

# 4. After PR is merged to devel:
#    - Only THIS feature is in devel (not other unstable features)
#    - Test integration with other features in devel

# 5. When ready for release (e.g., version 1.1.0):
#    - devel contains only ready features
#    - Create version branch from devel
git checkout devel
git checkout -b v1.1
# OR cherry-pick specific commits if needed:
git checkout v1.1
git cherry-pick <commit-hash-from-devel>

# 6. Tag the release
git tag v1.1.0
git push origin v1.1.0

# 7. When stable, merge to main
git checkout main
git merge v1.1
git push origin main
```

**Why selective merging?**
- ✅ Only **ready** features go to `devel`
- ✅ Unstable features stay in their own branches
- ✅ Easy to create version branch with only stable features
- ✅ No need to "reverse engineer" or remove unstable code

### Scenario 2: Bug Fix (Current Version)

```bash
# 1. Create fix branch from version branch
git checkout v1.0
git pull origin v1.0
git checkout -b fix/csv-parsing-issue

# 2. Fix and commit
git add .
git commit -m "Fix CSV parsing with escaped commas"

# 3. Create PR: fix/csv-parsing-issue → v1.0
# 4. After merge:
#    - Tag: v1.0.2
#    - Merge v1.0 → main (if stable)
```

### Scenario 2b: Bug Fix (Multiple Versions) - **Keeping Branches in Sync**

**The Problem**: A bug exists in v1.0, v1.1, and v2.0. How to fix all without losing track?

**Solution**: Fix once, then cherry-pick to other branches.

```bash
# Step 1: Fix in the OLDEST affected version first
git checkout v1.0
git pull origin v1.0
git checkout -b fix/csv-parsing-issue

# Step 2: Make the fix
git add .
git commit -m "Fix CSV parsing with escaped commas"
# Note the commit hash: abc1234

# Step 3: Create PR and merge to v1.0
# PR: fix/csv-parsing-issue → v1.0 ✅
# After merge, tag: v1.0.2

# Step 4: Cherry-pick to v1.1 (if bug exists there)
git checkout v1.1
git pull origin v1.1
git cherry-pick abc1234  # Same commit from v1.0
# If conflicts occur, resolve them
git push origin v1.1
# Tag: v1.1.1

# Step 5: Cherry-pick to v2.0 (if bug exists there)
git checkout v2.0
git pull origin v2.0
git cherry-pick abc1234  # Same commit from v1.0
# If conflicts occur, resolve them
git push origin v2.0
# Tag: v2.0.1

# Step 6: Also apply to devel (so future versions have it)
git checkout devel
git pull origin devel
git cherry-pick abc1234
git push origin devel
```

**Alternative: Fix in devel first, then cherry-pick to version branches**

```bash
# Step 1: Fix in devel (ensures future versions have it)
git checkout devel
git pull origin devel
git checkout -b fix/csv-parsing-issue
git add .
git commit -m "Fix CSV parsing with escaped commas"
# Commit hash: xyz5678

# Step 2: PR and merge to devel
# PR: fix/csv-parsing-issue → devel ✅

# Step 3: Cherry-pick to all affected version branches
git checkout v1.0
git cherry-pick xyz5678
git push origin v1.0
git tag v1.0.2

git checkout v1.1
git cherry-pick xyz5678
git push origin v1.1
git tag v1.1.1

git checkout v2.0
git cherry-pick xyz5678
git push origin v2.0
git tag v2.0.1
```

**Best Practice**: Fix in the oldest affected version OR in `devel`, then cherry-pick to others.

### Scenario 3: Breaking Changes (Long Stabilization) - **Selective Merging**

**The Problem**: Breaking changes need weeks/months to stabilize, but you still need to release bug fixes for v1.0.

**The Solution**: Keep breaking changes in **separate feature branches** until they're ready. Only merge **complete, tested** features to `devel`.

```bash
# Month 1-3: Breaking changes develop in SEPARATE branches
# 1. Create feature branches (NOT merging to devel yet)
git checkout devel
git checkout -b feature/major-api-refactor
# ... develop for weeks ...

git checkout devel
git checkout -b feature/new-database-support
# ... develop for weeks ...

git checkout devel
git checkout -b feature/refactor-cli
# ... develop for weeks ...

# 2. Each feature is developed independently
#    - feature/major-api-refactor (complete, tested) ✅
#    - feature/new-database-support (complete, tested) ✅
#    - feature/refactor-cli (still unstable, not ready) ⚠️

# 3. Only merge READY features to devel:
#    - PR: feature/major-api-refactor → devel ✅ (merged)
#    - PR: feature/new-database-support → devel ✅ (merged)
#    - feature/refactor-cli → NOT merged yet (stays separate)

# 4. devel now contains only ready features
#    - Test integration of ready features
#    - Unstable features stay in their branches

# 5. Meanwhile, v1.0 continues independently:
#    - Bug fixes PR directly to v1.0
#    - v1.0.1, v1.0.2, v1.0.3 tags created
#    - Users stay on stable v1.0.x

# 6. When ready for v2.0:
#    - devel contains only stable breaking changes
#    - Create v2.0 from devel (clean, no unstable code)
git checkout devel
git checkout -b v2.0
git push -u origin v2.0

# 7. Tag initial release
git tag v2.0.0-beta.1
git push origin v2.0.0-beta.1

# 8. Later, when feature/refactor-cli is ready:
#    - PR: feature/refactor-cli → devel ✅
#    - Then cherry-pick or merge to v2.0 if needed
#    - Tag: v2.0.0-beta.2

# 9. Final release
git tag v2.0.0
git push origin v2.0.0

# 10. Merge v2.0 → main when production-ready
git checkout main
git merge v2.0
git push origin main
```

**Key Points**:
- ✅ **Don't merge unstable features** to `devel`
- ✅ Keep features in separate branches until ready
- ✅ `devel` contains only **tested, ready** features
- ✅ Easy to create version branch - no cleanup needed
- ✅ Unstable features can be merged later when ready

### Scenario 4: Hotfix (Critical Bug in Production)

```bash
# 1. Create hotfix from main
git checkout main
git pull origin main
git checkout -b hotfix/critical-security-fix

# 2. Fix and commit
git add .
git commit -m "Fix critical security vulnerability"

# 3. Create PR: hotfix/critical-security-fix → main
# 4. After merge to main:
#    - Tag: v1.0.3 (or appropriate version)
#    - Cherry-pick to v1.0 branch
git checkout v1.0
git cherry-pick <commit-hash>
git push origin v1.0
```

## PR Strategy

### Where to Create PRs

| Change Type | Source Branch | Target Branch | Why |
|------------|---------------|---------------|-----|
| New feature (compatible) | `feature/*` | `devel` | Integrate first, then create version branch when ready |
| Bug fix (current version) | `fix/*` | `v1.0` (or current version) | Direct fix to stable version, tag immediately |
| Bug fix (all versions) | `fix/*` | `v1.0` (then cherry-pick) | Fix once, cherry-pick to other versions |
| Breaking change | `feature/*` | `devel` | Stabilizes in devel for months, then create v2.0 |
| Hotfix | `hotfix/*` | `main` | Critical fix, then cherry-pick to version branches |

### PR Workflow - Detailed Flow with Selective Merging

**For New Features / Breaking Changes:**

```
1. Contributor: feature/new-thing → PR to devel
2. Maintainer: Review & test
   - ✅ If ready: Merge to devel
   - ⚠️ If not ready: Request changes, keep in feature branch
3. devel now contains: Only ready features
4. Maintainer: Test integration in devel
5. When ready for release (e.g., v1.1.0):
   - devel contains only stable features
   - Create v1.1 branch from devel (clean!)
   - OR cherry-pick specific commits if needed
6. Tag: v1.1.0
7. Merge v1.1 → main (when stable)
```

**Key Rule**: Only merge **complete, tested, reviewed** features to `devel`. Unstable features stay in their branches.

**For Bug Fixes:**

```
1. Contributor: fix/bug → PR directly to v1.0
2. Maintainer: Review & merge to v1.0
3. Tag: v1.0.2
4. (Optional) Cherry-pick to other version branches if needed
```

**Key Difference:**
- **Features** → `devel` first (integration point)
- **Bug fixes** → Version branch directly (quick fix to stable version)

## Benefits of This Strategy

### ✅ For Breaking Changes

- **Long stabilization period**: Breaking changes develop in `devel` without affecting stable versions
- **Parallel development**: `v1.0` stays stable while `v2.0` develops
- **Clear separation**: Users know `v1.0` is stable, `v2.0` is experimental

### ✅ For Open Source

- **Clear contribution path**: Contributors PR to `devel`
- **Version stability**: Version branches remain stable
- **Multiple versions**: Can maintain `v1.0`, `v1.1`, `v2.0` simultaneously
- **Rollback capability**: Can revert version branch without affecting others

### ✅ For Users

- **Predictable versions**: `v1.0.x` = stable, `v2.0.x` = may have breaking changes
- **Easy updates**: Can stay on `v1.0` while `v2.0` stabilizes
- **Clear migration path**: Breaking changes documented per version

## Version Branch Lifecycle

### Creating a Version Branch

```bash
# When ready to release version 1.1.0
git checkout devel
git pull origin devel
git checkout -b v1.1
git push -u origin v1.1

# Tag initial release
git tag v1.1.0
git push origin v1.1.0
```

### Maintaining a Version Branch

```bash
# Apply patches to version branch
git checkout v1.1
git pull origin v1.1
git checkout -b fix/some-bug
# ... fix ...
git checkout v1.1
git merge fix/some-bug
git tag v1.1.1
git push origin v1.1.1
git push origin v1.1
```

### Merging Version Branch to Main

```bash
# When version branch is stable and production-ready
git checkout main
git pull origin main
git merge v1.1
git push origin main
```

## Keeping Multiple Branches in Sync

### Strategy for Bug Fixes Across Versions

When a bug affects multiple versions, use **cherry-picking** to keep branches in sync:

**Method 1: Fix in oldest version, cherry-pick forward**
```bash
# Fix in v1.0 (oldest)
git checkout v1.0
git checkout -b fix/bug-123
# ... fix ...
git commit -m "Fix bug 123"
# Merge to v1.0, tag v1.0.2

# Cherry-pick to newer versions
git checkout v1.1
git cherry-pick <commit-hash>
git tag v1.1.1

git checkout v2.0
git cherry-pick <commit-hash>
git tag v2.0.1
```

**Method 2: Fix in devel, cherry-pick to versions**
```bash
# Fix in devel (ensures future versions have it)
git checkout devel
git checkout -b fix/bug-123
# ... fix ...
git commit -m "Fix bug 123"
# Merge to devel

# Cherry-pick to all affected versions
git checkout v1.0 && git cherry-pick <hash> && git tag v1.0.2
git checkout v1.1 && git cherry-pick <hash> && git tag v1.1.1
git checkout v2.0 && git cherry-pick <hash> && git tag v2.0.1
```

### Handling Cherry-pick Conflicts

```bash
# When cherry-picking causes conflicts
git cherry-pick <commit-hash>
# Git shows conflicts

# Resolve conflicts
git add <resolved-files>
git cherry-pick --continue

# Or abort if needed
git cherry-pick --abort
```

### Tracking Which Fixes Are Applied Where

**Option 1: Use commit messages**
```bash
git commit -m "Fix bug 123 [applies to: v1.0, v1.1, v2.0]"
```

**Option 2: Use tags with notes**
```bash
git tag -a v1.0.2 -m "Fix bug 123 (cherry-picked to v1.1, v2.0)"
```

**Option 3: Document in CHANGELOG**
```markdown
## [v1.0.2] - 2026-01-15
### Fixed
- Fix bug 123 (also fixed in v1.1.1, v2.0.1)
```

### Automated Sync Script

Create `scripts/sync-fix.sh`:

```bash
#!/bin/bash
# Sync a fix across multiple version branches

COMMIT_HASH=$1
VERSIONS=("v1.0" "v1.1" "v2.0")

for version in "${VERSIONS[@]}"; do
    echo "Cherry-picking to $version..."
    git checkout $version
    git pull origin $version
    if git cherry-pick $COMMIT_HASH; then
        echo "✅ Successfully applied to $version"
        git push origin $version
    else
        echo "⚠️  Conflicts in $version - resolve manually"
        git cherry-pick --abort
    fi
done

git checkout devel
```

Usage:
```bash
./scripts/sync-fix.sh abc1234
```

## Best Practices

1. **Keep `main` stable**: Only merge stable version branches
2. **Use `devel` for integration**: Only **ready** features merge here
3. **Selective merging**: Don't merge unstable/incomplete features to `devel`
4. **Keep features separate**: Unstable features stay in their branches until ready
5. **Version branches are long-lived**: Don't delete them after merge to main
6. **Tag from version branches**: Tags represent releases from version branches
7. **Document breaking changes**: Clearly mark which version has breaking changes
8. **Maintain multiple versions**: Keep `v1.0` alive while `v2.0` develops
9. **Cherry-pick fixes**: Apply critical fixes to multiple version branches
10. **Test before merging**: Only merge features that pass tests and review
11. **Fix once, cherry-pick many**: Fix in one branch, then cherry-pick to others
12. **Track fixes**: Document which fixes are applied to which versions

## Handling Unstable Features

### Problem: Feature merged to `devel` but not ready for release

**Solution 1: Revert in `devel` (if not yet in version branch)**
```bash
# Feature was merged to devel but found to be unstable
git checkout devel
git revert <commit-hash>
git push origin devel

# Feature stays in its branch, can be fixed and re-merged later
```

**Solution 2: Don't include in version branch**
```bash
# If devel has unstable feature, create version branch selectively
git checkout devel
git checkout -b v1.1

# Remove unstable commits
git revert <unstable-commit-hash>
# OR
git rebase -i HEAD~10  # Interactive rebase to remove commits

# Now v1.1 is clean, devel still has unstable feature
```

**Solution 3: Keep features separate (preventive)**
```bash
# Best practice: Don't merge to devel until ready
# Keep feature in its branch until:
# - Code complete
# - Tests pass
# - Documentation updated
# - Reviewed and approved
# → THEN merge to devel
```

## Feature Flag Strategy (Alternative)

For very large features, consider feature flags:

```python
# config.py
FEATURE_NEW_PARQUET = os.getenv('ENABLE_PARQUET', 'false') == 'true'

# code.py
if FEATURE_NEW_PARQUET:
    # New feature code
else:
    # Old code
```

This allows merging incomplete features to `devel` while keeping them disabled.

## Example Timeline - Visual Flow with Selective Merging

```
Timeline showing how devel and version branches work together:

Month 1-3: Breaking changes develop in SEPARATE branches
            ├── feature/api-refactor (complete ✅) → PR to devel ✅
            ├── feature/new-db (complete ✅) → PR to devel ✅
            ├── feature/cli-redesign (unstable ⚠️) → Stays in branch
            └── feature/experimental (unstable ⚠️) → Stays in branch
            
            devel contains: Only ready features (api-refactor, new-db)
            
            Meanwhile, v1.0 stays stable:
            ├── PR: fix/bug-123 → v1.0 ✅ → Tag v1.0.1
            ├── PR: fix/bug-456 → v1.0 ✅ → Tag v1.0.2
            └── Users continue using v1.0.x (stable)

Month 4:    devel has only stable breaking changes
            ├── Test integration in devel (clean, no unstable code)
            ├── Fix integration issues
            └── When ready: Create v2.0 from devel
                └── Tag v2.0.0-beta.1
                └── Announce: "v2.0 beta available, breaking changes"
                └── v2.0 is clean (no unstable features)

Month 5-6:  Stabilize v2.0 branch
            ├── More fixes in v2.0
            ├── Tag v2.0.0-beta.2
            ├── Tag v2.0.0-rc.1
            ├── feature/cli-redesign now ready → PR to devel ✅
            │   └── Cherry-pick to v2.0 if needed
            └── v1.0 still gets bug fixes → v1.0.3, v1.0.4

Month 7:    v2.0 is stable
            ├── Tag v2.0.0 (final)
            ├── Merge v2.0 → main
            └── Announce migration guide
            └── v1.0 still maintained for users who haven't migrated
            └── feature/experimental still in its branch (not merged)
```

**Key Difference**: Only **ready** features merge to `devel`, making version branches clean and easy to create.

**Key Insight**: `devel` allows you to:
- ✅ Develop breaking changes without affecting stable versions
- ✅ Integrate multiple features together before release
- ✅ Test everything in one place (`devel`) before creating version branch
- ✅ Keep `v1.0` stable while `v2.0` develops

## Migration from Current Structure

If you currently have only `main` and `devel`:

1. **Create first version branch**:
   ```bash
   git checkout main
   git checkout -b v1.0
   git push -u origin v1.0
   git tag v1.0.0
   git push origin v1.0.0
   ```

2. **Update PR targets**: 
   - New features → `devel`
   - Bug fixes → `v1.0`

3. **Gradually adopt**: Start using version branches for new releases

This strategy gives you flexibility to handle breaking changes while maintaining stability for users.

