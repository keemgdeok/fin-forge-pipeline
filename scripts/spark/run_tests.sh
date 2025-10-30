#!/usr/bin/env bash
set -euo pipefail

# Run transform Spark tests inside the PySpark container.
# Usage:
#   ./scripts/spark/run_tests.sh                       # default Spark tests (runs with --runslow)
#   ./scripts/spark/run_tests.sh pytest tests/integration/transform -m integration

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

IMAGE_TAG="${SPARK_IMAGE_TAG:-finforge-spark-tests:latest}"

if ! docker image inspect "${IMAGE_TAG}" >/dev/null 2>&1; then
  echo "ℹ️ Docker image ${IMAGE_TAG} not found. Building it via scripts/spark/build.sh..."
  "${SCRIPT_DIR}/build.sh"
fi

# Ensure Spark-specific defaults propagate into the container.
export RUN_SPARK_TESTS="${RUN_SPARK_TESTS:-1}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

if [[ $# -gt 0 ]]; then
  CMD=("$@")
else
  CMD=(
    "pytest"
    "--runslow"
    "tests/integration/transform/test_etl_data_quality.py"
    "tests/integration/transform/test_indicators_etl_local_spark.py"
  )
fi

DOCKER_FLAGS=(--rm -i)
if [[ -t 1 && -t 0 ]]; then
  DOCKER_FLAGS+=(-t)
fi

docker run "${DOCKER_FLAGS[@]}" \
  -v "${REPO_ROOT}:/workspace" \
  -w /workspace \
  -e PYTHONPATH=/workspace/src \
  -e PYTEST_ADDOPTS="${PYTEST_ADDOPTS:-}" \
  -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
  -e RUN_SPARK_TESTS="${RUN_SPARK_TESTS:-1}" \
  "${IMAGE_TAG}" \
  "${CMD[@]}"
