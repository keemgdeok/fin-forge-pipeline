#!/usr/bin/env bash
set -euo pipefail

# Run transform Spark tests inside the PySpark container.
# Usage:
#   ./scripts/spark/run_tests.sh                       # default test target
#   ./scripts/spark/run_tests.sh pytest tests/integration/transform -m integration

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

IMAGE_TAG="${SPARK_IMAGE_TAG:-finforge-spark-tests:latest}"

if ! docker image inspect "${IMAGE_TAG}" >/dev/null 2>&1; then
  echo "❌ Docker image ${IMAGE_TAG} not found. Build it first with scripts/spark/build.sh" >&2
  exit 1
fi

if [[ $# -gt 0 ]]; then
  CMD=("$@")
else
  CMD=("pytest" "tests/integration/transform/test_etl_data_quality.py")
fi

docker run --rm -it \
  -v "${REPO_ROOT}:/workspace" \
  -w /workspace \
  -e PYTHONPATH=/workspace/src \
  -e PYTEST_ADDOPTS="${PYTEST_ADDOPTS:-}" \
  -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
  "${IMAGE_TAG}" \
  "${CMD[@]}"
