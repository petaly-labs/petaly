# Versioning Strategy Guide

This document outlines the versioning strategy for Petaly, including branch management, version tracking, and release workflow.

## Branch Structure

### Main Branches

1. **`main`** (or `master`)
   - Production-ready code
   - Always stable and deployable
   - Protected branch (requires PR + reviews)
   - Latest stable release

2. **`develop`** (optional)
   - Integration branch for features
   - Pre-release testing
   - Can be merged to `main` for releases

### Version Branches

For each major.minor version, create a version branch:

- `v1.0` - Version 1.0 release branch (for patches: v1.0.1, v1.0.2, v1.0.3)
- `v1.1` - Version 1.1 release branch (for patches: v1.1.0, v1.1.1, v1.1.2)
- `v2.0` - Version 2.0 release branch (for patches: v2.0.0, v2.0.1, v2.0.2)

**Naming Convention:** `v{major}.{minor}` (e.g., `v1.0`, `v1.1`, `v2.0`)

## Version Numbering

Follow [Semantic Versioning](https://semver.org/): `MAJOR.MINOR.PATCH`

- **MAJOR** (1.x.x): Breaking changes or major releases
- **MINOR** (x.0.x): New features, backward compatible
- **PATCH** (x.x.1): Bug fixes, backward compatible

**Examples:**
- `v1.0.1` - Version 1.0, Patch 1
- `v1.0.2` - Version 1.0, Patch 2
- `v1.1.0` - Version 1.1, Minor release
- `v2.0.0` - Version 2.0, Major release

**Current Version:** v1.0.1

### Versioning Rules

- **MAJOR version** increments when you make incompatible API changes
- **MINOR version** increments when you add functionality in a backward compatible manner
- **PATCH version** increments when you make backward compatible bug fixes

This follows PEP 440 and is fully compatible with Python package management (PyPI, pip, etc.).

## Version Tracking

### Option 1: Version File (Recommended)

Create a `VERSION` file in the root:

```bash
# Create VERSION file
echo "1.0.1" > VERSION
```

Update it in your code:
```python
# src/petaly/__init__.py
with open(os.path.join(os.path.dirname(__file__), '..', '..', 'VERSION')) as f:
    __version__ = f.read().strip()
```

### Option 2: pyproject.toml (Current)

Already using this:
```toml
[project]
version = "1.0.1"
```

### Option 3: Version Branch Comparison

Track version by comparing branches:
```bash
# See commits in v1.0 that are not in main
git log main..v1.0

# See commits in main that are not in v1.0
git log v1.0..main

# Compare versions
git diff main v1.0
```

## Branch Protection Setup

### GitHub Branch Protection Rules

1. **For `main` branch:**
   - ✅ Require pull request reviews before merging
   - ✅ Require status checks to pass
   - ✅ Require branches to be up to date before merging
   - ✅ Require conversation resolution before merging
   - ✅ Do not allow force pushes
   - ✅ Do not allow deletions

2. **For version branches (`v1.0`, `v1.1`, `v2.0`, etc.):**
   - ✅ Require pull request reviews before merging
   - ✅ Require status checks to pass
   - ✅ Do not allow force pushes
   - ✅ Do not allow deletions
   - ⚠️ Allow maintainers to bypass (for hotfixes)

## Workflow Examples

### Creating a New Version Branch

```bash
# 1. Ensure main is up to date
git checkout main
git pull origin main

# 2. Create version branch from main
git checkout -b v1.0
git push -u origin v1.0

# 3. Set up branch protection in GitHub UI
# Settings → Branches → Add rule → Branch name pattern: v1.0
```

### Working on Version 1.0.1 (Patch Release)

```bash
# 1. Create feature branch from v1.0
git checkout v1.0
git checkout -b fix/csv-parsing-issue

# 2. Make changes and commit
git add .
git commit -m "Fix CSV parsing issue"

# 3. Push and create PR: fix/csv-parsing-issue → v1.0
# After merge, update version and tag the release
git checkout v1.0
git pull origin v1.0
echo "1.0.2" > VERSION  # Update patch number
git add VERSION
git commit -m "Bump version to 1.0.2"
git tag v1.0.2
git push origin v1.0.2
git push origin v1.0
```

### Working on Version 1.1.0 (Minor Release)

```bash
# 1. Create v1.1 branch from main (or v1.0)
git checkout main
git checkout -b v1.1

# 2. Update version in pyproject.toml and VERSION to 1.1.0
echo "1.1.0" > VERSION
# Update pyproject.toml: version = "1.1.0"

# 3. Create feature branches from v1.1
git checkout -b feature/new-parquet-support

# 4. After merging features, tag release
git tag v1.1.0
git push origin v1.1.0
```

### Working on Version 2.0.0 (Major Release)

```bash
# 1. Create v2.0 branch from main
git checkout main
git checkout -b v2.0

# 2. Update version in pyproject.toml and VERSION to 2.0.0
echo "2.0.0" > VERSION
# Update pyproject.toml: version = "2.0.0"

# 3. Create feature branches from v2.0
git checkout -b feature/major-refactor

# 4. After merging features, tag release
git tag v2.0.0
git push origin v2.0.0
```

### Comparing Versions

```bash
# See what's in v1.0 that's not in main
git log main..v1.0 --oneline

# See what's in main that's not in v1.0
git log v1.0..main --oneline

# See file differences
git diff main v1.0 --stat

# See specific file changes
git diff main v1.0 -- path/to/file.py

# Compare two patches within same version
git log v1.0.1..v1.0.2 --oneline
git diff v1.0.1 v1.0.2 --stat

# Compare minor versions
git log v1.0.2..v1.1.0 --oneline
git diff v1.0.2 v1.1.0 --stat
```

## Version Management Script

The `scripts/version.sh` script provides helper commands:

```bash
# Show current version info
./scripts/version.sh show

# Update version
./scripts/version.sh update 1.0.2

# Compare branches
./scripts/version.sh compare main v1.0
```

## Release Checklist

When releasing a new version:

1. ✅ Update `VERSION` file or `pyproject.toml` (e.g., `1.0.2`)
2. ✅ Update `CHANGELOG.md` with release date and version (e.g., `[v1.0.2]`)
3. ✅ Create version branch if new major/minor version (e.g., `v1.1` or `v2.0`)
4. ✅ Merge all features to version branch
5. ✅ Run tests: `pytest tests/unittest/ -v`
6. ✅ Update documentation if needed
7. ✅ Commit version update: `git add VERSION CHANGELOG.md && git commit -m "Bump version to 1.0.2"`
8. ✅ Create release tag: `git tag v1.0.2`
9. ✅ Push tag: `git push origin v1.0.2`
10. ✅ Push version branch: `git push origin v1.0`
11. ✅ Merge version branch to `main` (if stable)
12. ✅ Create GitHub Release with changelog

## Recommended Structure

```
main (stable, protected)
├── v1.0 (version branch, protected)
│   ├── v1.0.1 (tag)
│   ├── v1.0.2 (tag)
│   ├── v1.0.3 (tag)
│   └── fix/* branches
├── v1.1 (version branch, protected)
│   ├── v1.1.0 (tag)
│   ├── v1.1.1 (tag)
│   └── feature/* branches
├── v2.0 (version branch, protected)
│   ├── v2.0.0 (tag)
│   └── feature/* branches
└── develop (optional, integration branch)
    └── feature/* branches
```

## Quick Reference

```bash
# Create version branch
git checkout main && git pull
git checkout -b v1.0
git push -u origin v1.0

# Update version and tag a release
echo "1.0.2" > VERSION
git add VERSION
git commit -m "Bump version to 1.0.2"
git tag v1.0.2
git push origin v1.0.2
git push origin v1.0

# Compare versions
git log main..v1.0
git diff main v1.0

# Compare patches within same version
git log v1.0.1..v1.0.2
git diff v1.0.1 v1.0.2

# Compare minor versions
git log v1.0.2..v1.1.0
git diff v1.0.2 v1.1.0

# Update version in code
echo "1.0.2" > VERSION
# or update pyproject.toml: version = "1.0.2"
```

## Why Semantic Versioning?

**Semantic Versioning (SemVer)** is the industry standard and provides clear benefits:

### ✅ Advantages:
- **Industry standard**: Widely recognized and understood by developers
- **Clear signaling**: Version number communicates compatibility and change type
- **Dependency management**: Enables precise version ranges (`>=1.0.0,<2.0.0`)
- **PEP 440 compliant**: Fully compatible with Python package management
- **Tool support**: Works seamlessly with pip, conda, poetry, and other package managers
- **Professional**: Shows maturity and adherence to best practices

### 📋 Version Number Meaning:
- **MAJOR** (1.x.x → 2.x.x): Breaking changes, incompatible API changes
- **MINOR** (1.0.x → 1.1.x): New features, backward compatible
- **PATCH** (1.0.1 → 1.0.2): Bug fixes, backward compatible

### 📋 Best Practices:
- Start with `1.0.0` or `1.0.1` for your first stable release
- Increment PATCH for bug fixes
- Increment MINOR for new features
- Increment MAJOR for breaking changes
- Use version branches (`v1.0`, `v1.1`) for each major.minor release

## GitHub Actions Integration (Optional)

Create `.github/workflows/version-check.yml`:

```yaml
name: Version Check
on:
  pull_request:
    branches: [main, v*]

jobs:
  check-version:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Check version consistency
        run: |
          VERSION_FILE=$(cat VERSION 2>/dev/null || grep 'version =' pyproject.toml | cut -d'"' -f2)
          BRANCH=$(git branch --show-current)
          echo "Version: $VERSION_FILE"
          echo "Branch: $BRANCH"
          # Validate version format (MAJOR.MINOR.PATCH, e.g., 1.0.1)
          if [[ ! $VERSION_FILE =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "Error: Version must be in format MAJOR.MINOR.PATCH (e.g., 1.0.1)"
            exit 1
          fi
```
