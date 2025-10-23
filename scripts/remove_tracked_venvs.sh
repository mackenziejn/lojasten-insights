#!/usr/bin/env bash
set -euo pipefail

# Safely remove tracked venv/.venv folders from git index (does NOT delete local files)
# Usage: scripts/remove_tracked_venvs.sh [--yes] [--dry-run]

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$HERE"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "This directory is not a git repository. Run this script from the repo root."
    exit 2
fi

DRY_RUN=0
CONFIRM_FLAG=0
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        --yes) CONFIRM_FLAG=1 ;;
        *) ;;
    esac
done

echo "Scanning git index for tracked venv/ or .venv/ files..."
FOUND=$(git ls-files | grep -E '^(venv/|\.venv/)' || true)
if [ -z "$FOUND" ]; then
    echo "No tracked 'venv/' or '.venv/' files found in the repository index. Nothing to do."
    exit 0
fi

echo "Tracked files (first 25):"
count=0
echo "$FOUND" | while IFS= read -r line; do
    echo "$line"
    count=$((count+1))
    if [ $count -ge 25 ]; then
        break
    fi
done

if [ $DRY_RUN -eq 1 ]; then
    echo "\nDRY RUN: No changes made. To actually untrack, run: scripts/remove_tracked_venvs.sh --yes" 
    exit 0
fi

if [ $CONFIRM_FLAG -eq 0 ]; then
    read -p "Proceed to remove these from git (they will remain on disk)? [y/N]: " ans
    case "$ans" in
        [Yy]*) CONFIRM_FLAG=1 ;;
        *) echo "Aborted by user."; exit 1 ;;
    esac
fi

echo "Removing tracked venv files from git index..."
git rm -r --cached venv .venv || true
git commit -m "chore: stop tracking local virtualenv folders (venv, .venv)" || true

echo "Done. The files remain on disk but are no longer tracked by git."
echo "Ensure .gitignore contains entries for venv/ and .venv/."
#!/usr/bin/env bash
set -euo pipefail

# Safely remove tracked venv/.venv folders from git index (does NOT delete local files)
# Usage: scripts/remove_tracked_venvs.sh [--yes]

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$HERE"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "This directory is not a git repository. Run this script from the repo root."
    exit 2
fi

echo "Scanning git index for tracked venv/ or .venv/ files..."
FOUND=$(git ls-files | grep -E '^(venv/|\.venv/)' || true)
if [ -z "$FOUND" ]; then
    echo "No tracked 'venv/' or '.venv/' files found in the repository index. Nothing to do."
    exit 0
fi

echo "Tracked files (first 25):"
count=0
echo "$FOUND" | while IFS= read -r line; do
    echo "$line"
    count=$((count+1))
    if [ $count -ge 25 ]; then
        break
    fi
done

CONFIRM=0
if [ "${1:-}" = "--yes" ]; then
    CONFIRM=1
fi

if [ $CONFIRM -eq 0 ]; then
    read -p "Proceed to remove these from git (they will remain on disk)? [y/N]: " ans
    case "$ans" in
        [Yy]*) CONFIRM=1 ;;
        *) echo "Aborted by user."; exit 1 ;;
    esac
fi

echo "Removing tracked venv files from git index..."
git rm -r --cached venv .venv || true
git commit -m "chore: stop tracking local virtualenv folders (venv, .venv)" || true

echo "Done. The files remain on disk but are no longer tracked by git."
echo "Ensure .gitignore contains entries for venv/ and .venv/."
