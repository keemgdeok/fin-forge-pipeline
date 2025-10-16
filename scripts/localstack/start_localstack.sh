#!/bin/bash
set -euo pipefail

: "${LOCALSTACK_IMAGE:=localstack/localstack:latest}"
: "${LOCALSTACK_NAME:=localstack-sfn}" 
: "${LOCALSTACK_SERVICES:=stepfunctions,lambda}" 

if docker ps -a --format '{{.Names}}' | grep -q "^${LOCALSTACK_NAME}$"; then
  echo "LocalStack container '${LOCALSTACK_NAME}' already exists. Stop and remove it before restarting." >&2
  exit 1
fi

exec docker run --rm -d \
  --name "${LOCALSTACK_NAME}" \
  -p 4566:4566 \
  -p 4510-4559:4510-4559 \
  -e "SERVICES=${LOCALSTACK_SERVICES}" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  "${LOCALSTACK_IMAGE}"
