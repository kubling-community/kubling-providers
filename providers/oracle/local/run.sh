#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
ENV_FILE="${KUBLING_ORACLE_LOCAL_ENV_FILE:-${SCRIPT_DIR}/.env}"
COMMAND="${1:-run}"

compose() {
    local arguments=(--file "${COMPOSE_FILE}")
    if [[ -f "${ENV_FILE}" ]]; then
        arguments+=(--env-file "${ENV_FILE}")
    fi
    docker compose "${arguments[@]}" "$@"
}

go_bin() {
    if [[ -n "${GO_BIN:-}" ]]; then
        printf '%s\n' "${GO_BIN}"
    elif [[ -x /usr/local/go/bin/go ]]; then
        printf '%s\n' /usr/local/go/bin/go
    else
        command -v go
    fi
}

client() {
    cd "${SCRIPT_DIR}/client"
    env \
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
        GOFLAGS="${GOFLAGS:--buildvcs=false}" \
        "$(go_bin)" run . "$@"
}

start_fixture() {
    compose up --detach --build --wait
}

run_test() {
    compose down --remove-orphans
    start_fixture

    client -mode basic
    client -mode prepare

    compose kill --signal SIGKILL provider
    compose up --detach --no-build provider

    client -mode recover
    printf '%s\n' 'Oracle provider integration test passed'
}

case "${COMMAND}" in
    run)
        start_fixture
        compose logs --follow provider
        ;;

    test)
        run_test
        ;;

    status)
        compose ps --all
        ;;

    logs)
        compose logs --follow
        ;;

    down)
        compose down --remove-orphans
        ;;

    reset)
        compose down --remove-orphans
        start_fixture
        ;;

    *)
        echo "usage: $0 [run|test|status|logs|down|reset]" >&2
        exit 2
        ;;
esac
