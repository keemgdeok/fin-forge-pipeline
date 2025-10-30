#!/usr/bin/env bash
set -euo pipefail

# Build the PySpark integration-test container image.
# Usage:
#   SPARK_IMAGE_TAG=my-registry/finforge-spark-tests:latest ./scripts/spark/build.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

IMAGE_TAG="${SPARK_IMAGE_TAG:-finforge-spark-tests:latest}"

BUILD_ARGS=()
if [[ -n "${PIP_INDEX_URL:-}" ]]; then
  BUILD_ARGS+=("--build-arg" "PIP_INDEX_URL=${PIP_INDEX_URL}")
fi

docker build "${BUILD_ARGS[@]}" \
  -t "${IMAGE_TAG}" \
  -f "${SCRIPT_DIR}/Dockerfile" \
  "${REPO_ROOT}"

echo "✅ PySpark test image built: ${IMAGE_TAG}"
