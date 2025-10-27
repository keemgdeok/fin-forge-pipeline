#!/usr/bin/env bash
set -euo pipefail

# Remove the PySpark test image when it is no longer needed.
# Usage:
#   ./scripts/spark/clean.sh                     # remove default image tag
#   SPARK_IMAGE_TAG=my-registry/finforge-spark-tests:latest ./scripts/spark/clean.sh

IMAGE_TAG="${SPARK_IMAGE_TAG:-finforge-spark-tests:latest}"

if docker image inspect "${IMAGE_TAG}" >/dev/null 2>&1; then
  docker rmi "${IMAGE_TAG}"
  echo "🧹 Removed Docker image ${IMAGE_TAG}"
else
  echo "ℹ️ Docker image ${IMAGE_TAG} not found. Nothing to clean."
fi
