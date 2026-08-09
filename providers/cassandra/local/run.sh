#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
COMMAND="${1:-run}"

compose() {
    docker compose --file "${COMPOSE_FILE}" "$@"
}

start_cassandra() {
    compose up --detach --wait cassandra
    compose run --rm schema-init
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
        "${go_bin}" run ./cmd/cassandra \
        -config "${SCRIPT_DIR}/provider.yaml" \
        -listen "${KUBLING_CASSANDRA_PROVIDER_LISTEN:-:50052}"
}

case "${COMMAND}" in
    run)
        start_cassandra
        run_provider
        ;;
    cassandra)
        start_cassandra
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
        echo "usage: $0 [run|cassandra|provider|status|down|reset]" >&2
        exit 2
        ;;
esac
