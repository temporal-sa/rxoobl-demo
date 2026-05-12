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

# The worker needs the same Cloud connection settings as the API, plus a
# deployment name/build id so Temporal Cloud can route workflows to a compatible
# version when Worker Versioning is enabled.
: "${TEMPORAL_API_KEY:?Set TEMPORAL_API_KEY to a Temporal Cloud API key for the tf-demo.zsvab namespace.}"

export TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-tf-demo.zsvab}"
export TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-${TEMPORAL_NAMESPACE}.tmprl.cloud:7233}"
export TEMPORAL_TLS="${TEMPORAL_TLS:-true}"
export TASK_QUEUE="${TASK_QUEUE:-trusted-friends-demo}"
export TEMPORAL_WORKER_DEPLOYMENT_NAME="${TEMPORAL_WORKER_DEPLOYMENT_NAME:-trusted-friends-demo}"
# Use the installed wheel version by default. Container builds can override this
# with a git SHA or release tag when they want stricter provenance.
export TEMPORAL_WORKER_BUILD_ID="${TEMPORAL_WORKER_BUILD_ID:-$(python -c 'import importlib.metadata; print(importlib.metadata.version("trusted-friends-demo"))')}"
export TEMPORAL_WORKER_VERSIONING="${TEMPORAL_WORKER_VERSIONING:-true}"

# exec keeps the worker as PID 1 in the container, which makes termination and
# graceful Temporal worker shutdown behave predictably.
exec python -m trusted_friends.worker
