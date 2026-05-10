#!/usr/bin/env bash
set -euo pipefail

: "${TEMPORAL_API_KEY:?Set TEMPORAL_API_KEY to a Temporal Cloud API key for the tf-demo.zsvab namespace.}"

export TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-tf-demo.zsvab}"
export TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-${TEMPORAL_NAMESPACE}.tmprl.cloud:7233}"
export TEMPORAL_TLS="${TEMPORAL_TLS:-true}"
export TASK_QUEUE="${TASK_QUEUE:-trusted-friends-demo}"
export TEMPORAL_WORKER_DEPLOYMENT_NAME="${TEMPORAL_WORKER_DEPLOYMENT_NAME:-trusted-friends-demo}"
export TEMPORAL_WORKER_BUILD_ID="${TEMPORAL_WORKER_BUILD_ID:-$(python -c 'import importlib.metadata; print(importlib.metadata.version("trusted-friends-demo"))')}"
export TEMPORAL_WORKER_VERSIONING="${TEMPORAL_WORKER_VERSIONING:-true}"

exec python -m trusted_friends.worker
