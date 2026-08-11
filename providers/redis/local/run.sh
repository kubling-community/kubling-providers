#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
COMMAND="${1:-run}"

compose() {
    docker compose --file "${COMPOSE_FILE}" "$@"
}

start_redis() {
    compose up --detach --wait redis
    compose run --rm seed
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
        "${go_bin}" run ./cmd/redis \
        -config "${SCRIPT_DIR}/provider.yaml" \
        -listen "${KUBLING_REDIS_PROVIDER_LISTEN:-:50053}"
}

case "${COMMAND}" in
    run)
        start_redis
        run_provider
        ;;
    redis)
        start_redis
        ;;
    provider)
        run_provider
        ;;
    status)
        compose ps
        ;;
    down)
        compose down
        ;;
    reset)
        compose down --volumes
        exec "${BASH_SOURCE[0]}" run
        ;;
    *)
        echo "usage: $0 [run|redis|provider|status|down|reset]" >&2
        exit 2
        ;;
esac
