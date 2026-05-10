#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRY_RUN=0
KEEP_SERVICES=0
KEEP_TEMPORAL=0
REMOVE_DEPS=0

usage() {
  cat <<'USAGE'
Usage: scripts/reset_demo.sh [options]

Reset the local Trusted Friends demo to a clean state.

By default this script:
  - stops local demo services (frontend, API, worker, Temporal dev server)
  - checks default demo listener ports 5173, 8000, 7233, and 8233
  - removes generated build/test/browser artifacts
  - removes Python bytecode and local packaging metadata
  - preserves dependencies (.venv and frontend/node_modules)

Options:
  --dry-run         Print what would be removed/stopped without changing anything
  --keep-services   Do not stop local demo processes
  --keep-temporal   Stop app processes but leave `temporal server start-dev` running
  --deps            Also remove .venv and frontend/node_modules
  -h, --help        Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      ;;
    --keep-services)
      KEEP_SERVICES=1
      ;;
    --keep-temporal)
      KEEP_TEMPORAL=1
      ;;
    --deps)
      REMOVE_DEPS=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

say() {
  printf '%s\n' "$*"
}

run() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf '[dry-run] %q' "$1"
    shift
    for arg in "$@"; do
      printf ' %q' "$arg"
    done
    printf '\n'
    return 0
  fi
  "$@"
}

stop_processes() {
  if [[ "$KEEP_SERVICES" -eq 1 ]]; then
    say "Skipping service stop (--keep-services)."
    return 0
  fi

  say "Stopping local demo services..."
  local patterns=(
    "uvicorn trusted_friends.api:app --reload"
    "trusted_friends.api:app --reload"
    "python -m trusted_friends.worker"
    "${ROOT_DIR}/frontend/node_modules/.bin/vite --host 127.0.0.1"
    "vite --host 127.0.0.1"
  )

  if [[ "$KEEP_TEMPORAL" -eq 0 ]]; then
    patterns+=("temporal server start-dev")
  fi

  local pattern
  for pattern in "${patterns[@]}"; do
    if pgrep -f "$pattern" >/dev/null 2>&1; then
      say "  stopping: $pattern"
      run pkill -f "$pattern" || true
    else
      say "  not running: $pattern"
    fi
  done

  stop_listeners_on_default_ports
}

stop_listeners_on_default_ports() {
  if ! command -v lsof >/dev/null 2>&1; then
    say "  lsof not available; skipped default port checks."
    return 0
  fi

  local ports=(5173 8000)
  if [[ "$KEEP_TEMPORAL" -eq 0 ]]; then
    ports+=(7233 8233)
  fi

  local port
  for port in "${ports[@]}"; do
    local pids=()
    local pid
    while IFS= read -r pid; do
      [[ -n "$pid" ]] && pids+=("$pid")
    done < <(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)

    if [[ "${#pids[@]}" -gt 0 ]]; then
      say "  stopping listener on port $port: ${pids[*]}"
      run kill "${pids[@]}" || true
    else
      say "  no listener on port $port"
    fi
  done
}

remove_paths() {
  say "Removing generated artifacts..."
  local paths=(
    "$ROOT_DIR/.coverage"
    "$ROOT_DIR/.mypy_cache"
    "$ROOT_DIR/.pytest_cache"
    "$ROOT_DIR/.ruff_cache"
    "$ROOT_DIR/.playwright-cli"
    "$ROOT_DIR/build"
    "$ROOT_DIR/coverage"
    "$ROOT_DIR/dist"
    "$ROOT_DIR/htmlcov"
    "$ROOT_DIR/output"
    "$ROOT_DIR/trusted_friends_demo.egg-info"
    "$ROOT_DIR/frontend/.vite"
    "$ROOT_DIR/frontend/.vite-temp"
    "$ROOT_DIR/frontend/coverage"
    "$ROOT_DIR/frontend/dist"
    "$ROOT_DIR/frontend/tsconfig.tsbuildinfo"
    "$ROOT_DIR/frontend/tsconfig.node.tsbuildinfo"
    "$ROOT_DIR/frontend/node_modules/.vite"
    "$ROOT_DIR/frontend/node_modules/.vite-temp"
  )

  if [[ "$REMOVE_DEPS" -eq 1 ]]; then
    paths+=(
      "$ROOT_DIR/.venv"
      "$ROOT_DIR/frontend/node_modules"
    )
  fi

  local path
  for path in "${paths[@]}"; do
    if [[ -e "$path" ]]; then
      say "  removing: ${path#$ROOT_DIR/}"
      run rm -rf "$path"
    fi
  done
}

remove_bytecode() {
  say "Removing Python bytecode and .DS_Store files..."
  if [[ "$DRY_RUN" -eq 1 ]]; then
    find "$ROOT_DIR" \
      \( -path "$ROOT_DIR/.venv" -o -path "$ROOT_DIR/frontend/node_modules" \) -prune \
      -o \( -name '__pycache__' -o -name '*.pyc' -o -name '*.pyo' -o -name '.DS_Store' \) \
      -print
    return 0
  fi

  find "$ROOT_DIR" \
    \( -path "$ROOT_DIR/.venv" -o -path "$ROOT_DIR/frontend/node_modules" \) -prune \
    -o \( -name '__pycache__' -o -name '*.pyc' -o -name '*.pyo' -o -name '.DS_Store' \) \
    -exec rm -rf {} +
}

main() {
  say "Resetting Trusted Friends demo at $ROOT_DIR"
  stop_processes
  remove_paths
  remove_bytecode
  say "Reset complete."
  if [[ "$KEEP_TEMPORAL" -eq 1 || "$KEEP_SERVICES" -eq 1 ]]; then
    say "Note: Temporal workflow state is only fully cleared when the local Temporal dev server is stopped."
  fi
  say "Dependencies were preserved. Re-run with --deps to remove .venv and frontend/node_modules."
  if [[ "$KEEP_SERVICES" -eq 0 ]]; then
    if [[ "$KEEP_TEMPORAL" -eq 1 ]]; then
      say "Restart the worker, API, and frontend before using the demo again."
    else
      say "Restart Temporal, the worker, API, and frontend before using the demo again."
    fi
  fi
}

main "$@"
