#!/usr/bin/env bash
set -euo pipefail

# Worker Versioning has two steps: workers advertise a deployment/build id, and
# Cloud marks one build as the current version for new workflow tasks. This
# script performs the second step for the demo namespace.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Load .env when present so the command can be run from a fresh terminal without
# re-exporting the Temporal API key and namespace every time.
set -a
if [[ -f "${ROOT_DIR}/.env" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
fi
set +a

: "${TEMPORAL_API_KEY:?Set TEMPORAL_API_KEY in .env or the environment.}"

# Defaults match the known working demo deployment, while still allowing callers
# to promote a different build id by exporting TEMPORAL_WORKER_BUILD_ID.
TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-tf-demo.zsvab}"
TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-${TEMPORAL_NAMESPACE}.tmprl.cloud:7233}"
TEMPORAL_WORKER_DEPLOYMENT_NAME="${TEMPORAL_WORKER_DEPLOYMENT_NAME:-trusted-friends-demo}"
TEMPORAL_WORKER_BUILD_ID="${TEMPORAL_WORKER_BUILD_ID:-latest-8b4f}"

# Promote the build id for new tasks. Existing workflow executions may still
# follow their pinned versioning behavior depending on how they were started.
temporal worker deployment set-current-version \
  --address "${TEMPORAL_ADDRESS}" \
  --namespace "${TEMPORAL_NAMESPACE}" \
  --api-key "${TEMPORAL_API_KEY}" \
  --tls \
  --deployment-name "${TEMPORAL_WORKER_DEPLOYMENT_NAME}" \
  --build-id "${TEMPORAL_WORKER_BUILD_ID}" \
  --yes

# Print the server-side deployment state immediately so an operator can confirm
# Cloud accepted the promotion before starting traffic tests.
temporal worker deployment describe \
  --address "${TEMPORAL_ADDRESS}" \
  --namespace "${TEMPORAL_NAMESPACE}" \
  --api-key "${TEMPORAL_API_KEY}" \
  --tls \
  --name "${TEMPORAL_WORKER_DEPLOYMENT_NAME}"
