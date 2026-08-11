#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBLING_IMAGE="${KUBLING_IMAGE:-kubling/kubling:latest}"
KUBLING_TEST_CONTAINER_NAME="${KUBLING_TEST_CONTAINER_NAME:-kubling-provider-test}"
KUBLING_TEST_GRPC_PORT="${KUBLING_TEST_GRPC_PORT:-50061}"

"${SCRIPT_DIR}/generate-bundle.sh"

docker_args=(
    run
    --rm
    --name "${KUBLING_TEST_CONTAINER_NAME}"
    --add-host "host.docker.internal:host-gateway"
    --publish "${KUBLING_TEST_GRPC_PORT}:50051"
    --env DESCRIPTOR_BUNDLE=/kubling-test/provider-test-descriptor-bundle.zip
    --env APP_CONFIG=/kubling-test/app-config.yaml
    --env GRPC_TRANSPORT_PORT=50051
    --env "KUBLING_GRPC_PROVIDER_HOST=${KUBLING_GRPC_PROVIDER_HOST:-host.docker.internal}"
    --env "KUBLING_GRPC_PROVIDER_PORT=${KUBLING_GRPC_PROVIDER_PORT:-50051}"
    --env "KUBLING_GRPC_PROVIDER_TIMEOUT_MILLIS=${KUBLING_GRPC_PROVIDER_TIMEOUT_MILLIS:-30000}"
    --volume "${SCRIPT_DIR}:/kubling-test:ro"
)

if [[ -n "${KUBLING_DOCKER_NETWORK:-}" ]]; then
    docker_args+=(--network "${KUBLING_DOCKER_NETWORK}")
fi

exec docker "${docker_args[@]}" "${KUBLING_IMAGE}"
