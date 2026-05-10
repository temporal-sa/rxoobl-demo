#!/usr/bin/env bash
set -euo pipefail

: "${TEMPORAL_API_KEY:?Set TEMPORAL_API_KEY to a Temporal Cloud API key for the tf-demo.zsvab namespace.}"

export TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-tf-demo.zsvab}"
export TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-${TEMPORAL_NAMESPACE}.tmprl.cloud:7233}"
export TEMPORAL_TLS="${TEMPORAL_TLS:-true}"
export TASK_QUEUE="${TASK_QUEUE:-trusted-friends-demo}"
export TEMPORAL_WORKER_VERSIONING="${TEMPORAL_WORKER_VERSIONING:-true}"

exec uvicorn trusted_friends.api:app --host "${HOST:-0.0.0.0}" --port "${PORT:-8000}"
