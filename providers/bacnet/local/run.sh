#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
CONFIG_FILE="${SCRIPT_DIR}/provider.yaml"
COMMAND="${1:-run}"

compose() {
    docker compose --file "${COMPOSE_FILE}" "$@"
}

start_device() {
    compose up --detach --wait device
}

run_provider() {
    local go_bin="${GO_BIN:-}"
    if [ -z "${go_bin}" ]; then
        if [ -x /usr/local/go/bin/go ]; then
            go_bin=/usr/local/go/bin/go
        else
            go_bin="$(command -v go)"
        fi
    fi

    cd "${MODULE_DIR}"
    exec env \
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
        GOFLAGS="${GOFLAGS:--buildvcs=false}" \
        "${go_bin}" run ./cmd/kubling-provider-bacnet \
        -config "${CONFIG_FILE}" \
        -listen "${KUBLING_BACNET_PROVIDER_LISTEN:-:50051}" \
        "$@"
}

case "${COMMAND}" in
    run)
        start_device
        run_provider
        ;;
    device)
        start_device
        ;;
    provider)
        run_provider
        ;;
    check)
        run_provider -check
        ;;
    metadata)
        run_provider -print-metadata
        ;;
    logs)
        compose logs --follow device
        ;;
    status)
        compose ps
        ;;
    down)
        compose down
        ;;
    reset)
        compose down
        exec "${BASH_SOURCE[0]}" run
        ;;
    *)
        echo "usage: $0 [run|device|provider|check|metadata|logs|status|down|reset]" >&2
        exit 2
        ;;
esac
