#!/bin/bash
set -euo pipefail

: "${LOCALSTACK_NAME:=localstack-sfn}"

if ! docker ps -a --format '{{.Names}}' | grep -q "^${LOCALSTACK_NAME}$"; then
  echo "LocalStack container '${LOCALSTACK_NAME}' is not running." >&2
  exit 0
fi

docker stop "${LOCALSTACK_NAME}"
