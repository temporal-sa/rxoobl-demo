#!/usr/bin/env bash
set -euo pipefail

# End-to-end deployment helper for AWS ECR. It rebuilds the Python package,
# logs Docker into ECR, builds the image for linux/amd64, and pushes the result.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PACKAGE_ARCHIVE="${ROOT_DIR}/output/temporal-cloud-deployment/trusted-friends-temporal-cloud.tar.gz"
DOCKER_CONTEXT="${ROOT_DIR}/output/temporal-cloud-deployment/docker-context-ecr"

# These defaults match the demo account and region, but every value can be
# overridden by exporting the variable before invoking the script.
AWS_PROFILE="${AWS_PROFILE:-SolutionsArchitecture/AWSAdministratorAccess}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-429214323166}"
ECR_REPO="${ECR_REPO:-trusted-friends-demo}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
PLATFORM="${PLATFORM:-linux/amd64}"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com"
ECR_IMAGE="${ECR_REGISTRY}/${ECR_REPO}:${IMAGE_TAG}"

# Prefer the named AWS profile over raw static/session credentials in the shell.
# This avoids accidentally pushing with stale or higher-privilege credentials.
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
export AWS_PROFILE AWS_DEFAULT_REGION

"${ROOT_DIR}/scripts/deploy/build_deployment_package.sh"

# Create the ECR repository if this is the first push from a fresh AWS account.
aws ecr describe-repositories \
  --repository-names "${ECR_REPO}" \
  --region "${AWS_DEFAULT_REGION}" \
  >/dev/null 2>&1 || \
aws ecr create-repository \
  --repository-name "${ECR_REPO}" \
  --region "${AWS_DEFAULT_REGION}" \
  >/dev/null

aws ecr get-login-password \
  --region "${AWS_DEFAULT_REGION}" \
| docker login \
  --username AWS \
  --password-stdin "${ECR_REGISTRY}"

# Build from the packaged artifact rather than the repo root so the pushed image
# is exactly the same shape as the local deployment package.
rm -rf "${DOCKER_CONTEXT}"
mkdir -p "${DOCKER_CONTEXT}"
tar -xzf "${PACKAGE_ARCHIVE}" -C "${DOCKER_CONTEXT}"

# Default command is the worker; set the runtime command to
# ./scripts/run_temporal_cloud_api.sh when deploying the API container.
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

# buildx makes the architecture explicit. That matters when building from Apple
# Silicon for linux/amd64 infrastructure.
docker buildx build \
  --platform "${PLATFORM}" \
  --file "${DOCKER_CONTEXT}/Dockerfile" \
  --tag "${ECR_IMAGE}" \
  --push \
  "${DOCKER_CONTEXT}"

echo "Pushed ${ECR_IMAGE} for ${PLATFORM}"
