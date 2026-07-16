#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  scripts/update_and_restart.sh <systemd-service-name>

Environment variables:
  MA_SERVICE_NAME   Service name fallback when the first argument is omitted.
  MA_VENV_DIR       Virtualenv directory. Default: <repo>/.venv
  TMPDIR            Temporary directory for pip. Default: $HOME/pip-tmp

Behavior:
  1. Refuse to pull when the worktree is dirty.
  2. Run `git pull --ff-only`.
  3. Reinstall Python dependencies only when dependency-related files changed.
  4. Restart the given systemd service.
EOF
}

SERVICE_NAME="${1:-${MA_SERVICE_NAME:-}}"
if [[ -z "$SERVICE_NAME" ]]; then
    usage
    exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(git -C "$SCRIPT_DIR/.." rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$REPO_DIR" ]]; then
    echo "ERROR: cannot find git repository root from $SCRIPT_DIR" >&2
    exit 1
fi

cd "$REPO_DIR"

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "ERROR: worktree has local changes; commit/stash them before pulling." >&2
    exit 2
fi

OLD_HEAD="$(git rev-parse HEAD)"
echo "Repo: $REPO_DIR"
echo "Old HEAD: $OLD_HEAD"

git pull --ff-only

NEW_HEAD="$(git rev-parse HEAD)"
echo "New HEAD: $NEW_HEAD"

CHANGED_FILES="$(git diff --name-only "$OLD_HEAD" "$NEW_HEAD" || true)"
if [[ -n "$CHANGED_FILES" ]]; then
    echo "Changed files:"
    printf '  %s\n' $CHANGED_FILES
else
    echo "No code changes pulled."
fi

NEEDS_INSTALL=0
if [[ -n "$CHANGED_FILES" ]] && grep -Eq \
    '(^pyproject\.toml$|^requirements_all\.txt$|^music_assistant/providers/[^/]+/manifest\.json$)' \
    <<<"$CHANGED_FILES"; then
    NEEDS_INSTALL=1
fi

VENV_DIR="${MA_VENV_DIR:-$REPO_DIR/.venv}"
PYTHON_BIN="$VENV_DIR/bin/python"

if [[ "$NEEDS_INSTALL" -eq 1 ]]; then
    if [[ ! -x "$PYTHON_BIN" ]]; then
        echo "ERROR: virtualenv python not found at $PYTHON_BIN" >&2
        exit 3
    fi
    export TMPDIR="${TMPDIR:-$HOME/pip-tmp}"
    mkdir -p "$TMPDIR"
    echo "Dependency-related files changed; reinstalling Python packages..."
    "$PYTHON_BIN" -m pip install --no-cache-dir --no-build-isolation -U . -r requirements_all.txt
else
    echo "No dependency changes detected; skipping pip install."
fi

if ! command -v systemctl >/dev/null 2>&1; then
    echo "ERROR: systemctl not found on this machine." >&2
    exit 4
fi

SYSTEMCTL=(systemctl)
if [[ "$(id -u)" -ne 0 ]]; then
    SYSTEMCTL=(sudo systemctl)
fi

echo "Restarting service: $SERVICE_NAME"
"${SYSTEMCTL[@]}" restart "$SERVICE_NAME"
"${SYSTEMCTL[@]}" --no-pager --lines=20 status "$SERVICE_NAME"
