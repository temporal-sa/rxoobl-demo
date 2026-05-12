#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f "${ROOT_DIR}/.env" ]]; then
  while IFS= read -r raw_line || [[ -n "${raw_line}" ]]; do
    line="${raw_line#"${raw_line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    [[ "${line}" == export\ * ]] && line="${line#export }"

    key="${line%%=*}"
    value="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    [[ "${line}" != *=* || ! "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] && continue
    [[ -n "${!key+x}" ]] && continue

    if [[ ${#value} -ge 2 && "${value:0:1}" == "${value: -1}" ]]; then
      case "${value:0:1}" in
        "'" | '"') value="${value:1:${#value}-2}" ;;
      esac
    fi
    export "${key}=${value}"
  done < "${ROOT_DIR}/.env"
fi

# The API is a Temporal client, not a worker. It starts workflows, sends
# signals, and serves queries to the browser, so it only needs Cloud client
# credentials and the task queue name that the worker is polling.
: "${TEMPORAL_API_KEY:?Set TEMPORAL_API_KEY to a Temporal Cloud API key for the tf-demo.zsvab namespace.}"

# Keep the demo runnable with no flags while still allowing every Cloud setting
# to be overridden by the shell, Docker environment, or an orchestrator.
export TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-tf-demo.zsvab}"
export TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-${TEMPORAL_NAMESPACE}.tmprl.cloud:7233}"
export TEMPORAL_TLS="${TEMPORAL_TLS:-true}"
export TASK_QUEUE="${TASK_QUEUE:-trusted-friends-demo}"
export TEMPORAL_WORKER_VERSIONING="${TEMPORAL_WORKER_VERSIONING:-true}"

# exec replaces the shell with uvicorn so signals from Docker/Kubernetes reach
# the server process directly during shutdown.
exec uvicorn trusted_friends.api:app --host "${HOST:-0.0.0.0}" --port "${PORT:-8000}"
