#!/usr/bin/env bash
set -euo pipefail

# The deployment package is the smallest portable artifact for Temporal Cloud:
# a built wheel plus the entrypoint scripts used to run the API or worker.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${ROOT_DIR}/output/temporal-cloud-deployment"
PACKAGE_DIR="${OUTPUT_DIR}/package"
ARCHIVE_PATH="${OUTPUT_DIR}/trusted-friends-temporal-cloud.tar.gz"

# Start from an empty output directory so stale wheels or scripts cannot sneak
# into the archive and produce a deployment that differs from the source tree.
rm -rf "${OUTPUT_DIR}"
mkdir -p "${PACKAGE_DIR}/dist" "${PACKAGE_DIR}/scripts"

# Building a wheel exercises the package metadata and creates the exact install
# artifact that the Dockerfile later installs into the runtime image.
uv build --out-dir "${OUTPUT_DIR}/dist"

# Copy only runtime-relevant files. Source code is already inside the wheel, so
# the package remains compact and avoids shipping local caches or credentials.
cp "${OUTPUT_DIR}"/dist/*.whl "${PACKAGE_DIR}/dist/"
cp "${ROOT_DIR}/pyproject.toml" "${PACKAGE_DIR}/pyproject.toml"
cp "${ROOT_DIR}/uv.lock" "${PACKAGE_DIR}/uv.lock"
cp "${ROOT_DIR}/docs/temporal-cloud.md" "${PACKAGE_DIR}/README.md"
cp "${ROOT_DIR}/scripts/run_temporal_cloud_worker.sh" "${PACKAGE_DIR}/scripts/"
cp "${ROOT_DIR}/scripts/run_temporal_cloud_api.sh" "${PACKAGE_DIR}/scripts/"

# Archive the package directory itself so Docker extraction always has a stable
# package/ prefix regardless of where the archive is unpacked.
tar -czf "${ARCHIVE_PATH}" -C "${OUTPUT_DIR}" package

echo "Built ${ARCHIVE_PATH}"
