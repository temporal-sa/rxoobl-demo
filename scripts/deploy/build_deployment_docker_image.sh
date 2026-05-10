#!/usr/bin/env bash
set -euo pipefail

# This script turns the portable deployment package into a local Docker image.
# It is useful for testing the packaged worker/API exactly as they will run in
# Cloud infrastructure before pushing to a registry.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PACKAGE_ARCHIVE="${ROOT_DIR}/output/temporal-cloud-deployment/trusted-friends-temporal-cloud.tar.gz"
DOCKER_CONTEXT="${ROOT_DIR}/output/temporal-cloud-deployment/docker-context"
IMAGE_NAME="${IMAGE_NAME:-trusted-friends-temporal-cloud}"
IMAGE_TAG="${IMAGE_TAG:-0.1.0}"

usage() {
  cat <<'USAGE'
Usage: scripts/deploy/build_deployment_docker_image.sh [--rebuild-package] [--image NAME] [--tag TAG]

Builds a Docker image from the Temporal Cloud deployment package.

Options:
  --rebuild-package  Rebuild output/temporal-cloud-deployment/trusted-friends-temporal-cloud.tar.gz first.
  --image NAME       Docker image name. Defaults to trusted-friends-temporal-cloud.
  --tag TAG          Docker image tag. Defaults to 0.1.0.
  -h, --help         Show this help text.
USAGE
}

REBUILD_PACKAGE=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --rebuild-package)
      REBUILD_PACKAGE=true
      shift
      ;;
    --image)
      IMAGE_NAME="${2:?Missing value for --image}"
      shift 2
      ;;
    --tag)
      IMAGE_TAG="${2:?Missing value for --tag}"
      shift 2
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
done

# Rebuild on request or when no package exists. Keeping this optional makes
# rebuild iterations faster when only the Docker layer behavior is being tested.
if [[ "${REBUILD_PACKAGE}" == true || ! -f "${PACKAGE_ARCHIVE}" ]]; then
  "${ROOT_DIR}/scripts/deploy/build_deployment_package.sh"
fi

# Docker receives a clean context derived from the archive, not the full repo.
# That mirrors what gets deployed and prevents local files from affecting image
# contents accidentally.
rm -rf "${DOCKER_CONTEXT}"
mkdir -p "${DOCKER_CONTEXT}"
tar -xzf "${PACKAGE_ARCHIVE}" -C "${DOCKER_CONTEXT}"

# The image defaults to worker mode. The same image can run the API by changing
# the container command to ./scripts/run_temporal_cloud_api.sh.
cat > "${DOCKER_CONTEXT}/Dockerfile" <<'DOCKERFILE'
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TEMPORAL_NAMESPACE=tf-demo.zsvab \
    TEMPORAL_ADDRESS=tf-demo.zsvab.tmprl.cloud:7233 \
    TEMPORAL_TLS=true \
    TASK_QUEUE=trusted-friends-demo \
    HOST=0.0.0.0 \
    PORT=8000

WORKDIR /app

COPY package/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -f /tmp/*.whl

COPY package/scripts/ ./scripts/
RUN chmod +x ./scripts/run_temporal_cloud_worker.sh ./scripts/run_temporal_cloud_api.sh

CMD ["./scripts/run_temporal_cloud_worker.sh"]
DOCKERFILE

# Keep the Docker build explicit about its generated context and tag so the
# caller can tag again for ECR, GHCR, or another registry as needed.
docker build \
  --file "${DOCKER_CONTEXT}/Dockerfile" \
  --tag "${IMAGE_NAME}:${IMAGE_TAG}" \
  "${DOCKER_CONTEXT}"

echo "Built Docker image ${IMAGE_NAME}:${IMAGE_TAG}"
