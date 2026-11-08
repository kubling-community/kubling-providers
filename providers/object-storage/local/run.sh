#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
COMMAND="${1:-run}"

compose() {
    docker compose --file "${COMPOSE_FILE}" "$@"
}

resolve_go() {
    local go_bin="${GO_BIN:-}"
    if [ -z "${go_bin}" ]; then
        if [ -x /usr/local/go/bin/go ]; then
            go_bin=/usr/local/go/bin/go
        else
            go_bin="$(command -v go)"
        fi
    fi
    printf '%s\n' "${go_bin}"
}

go_command() {
    local go_bin
    go_bin="$(resolve_go)"
    env \
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
        GOFLAGS="${GOFLAGS:--buildvcs=false}" \
        "${go_bin}" "$@"
}

load_azure_fixture() {
    set -a
    . "${SCRIPT_DIR}/azure-fixture.env"
    set +a
}

start_store() {
    compose up --detach --wait minio
    cd "${MODULE_DIR}"
    go_command run ./local/seed
}

start_azure_store() {
    local go_bin
    go_bin="$(resolve_go)"
    load_azure_fixture
    compose up --detach --wait azurite
    cd "${MODULE_DIR}"
    env \
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
        GOFLAGS="${GOFLAGS:--buildvcs=false}" \
        "${go_bin}" run ./local/seed \
        -backend azure-blob \
        -endpoint http://127.0.0.1:10000/kublinglocal \
        -account-name kublinglocal
}

start_gcs_store() {
    local go_bin
    go_bin="$(resolve_go)"
    compose up --detach --wait fake-gcs
    cd "${MODULE_DIR}"
    env \
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
        GOFLAGS="${GOFLAGS:--buildvcs=false}" \
        "${go_bin}" run ./local/seed \
        -backend gcs \
        -endpoint http://127.0.0.1:4443 \
        -project-id kubling-local
}

run_provider() {
    local go_bin
    go_bin="$(resolve_go)"
    cd "${MODULE_DIR}"
    exec env \
        AWS_ACCESS_KEY_ID=kubling-local \
        AWS_SECRET_ACCESS_KEY=kubling-local-secret \
        AWS_REGION=us-east-1 \
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
        GOFLAGS="${GOFLAGS:--buildvcs=false}" \
        "${go_bin}" run ./cmd/kubling-provider-object-storage \
        -config "${SCRIPT_DIR}/provider.yaml" \
        -listen "${KUBLING_OBJECT_STORAGE_PROVIDER_LISTEN:-:50053}"
}

run_filesystem_provider() {
    local go_bin
    go_bin="$(resolve_go)"
    cd "${MODULE_DIR}"
    exec env \
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
        GOFLAGS="${GOFLAGS:--buildvcs=false}" \
        "${go_bin}" run ./cmd/kubling-provider-object-storage \
        -config "${SCRIPT_DIR}/filesystem.yaml" \
        -listen "${KUBLING_OBJECT_STORAGE_PROVIDER_LISTEN:-:50053}"
}

run_azure_provider() {
    local go_bin
    go_bin="$(resolve_go)"
    load_azure_fixture
    cd "${MODULE_DIR}"
    exec env \
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
        GOFLAGS="${GOFLAGS:--buildvcs=false}" \
        "${go_bin}" run ./cmd/kubling-provider-object-storage \
        -config "${SCRIPT_DIR}/azure.yaml" \
        -listen "${KUBLING_OBJECT_STORAGE_PROVIDER_LISTEN:-:50053}"
}

run_gcs_provider() {
    local go_bin
    go_bin="$(resolve_go)"
    cd "${MODULE_DIR}"
    exec env \
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
        GOFLAGS="${GOFLAGS:--buildvcs=false}" \
        "${go_bin}" run ./cmd/kubling-provider-object-storage \
        -config "${SCRIPT_DIR}/gcs.yaml" \
        -listen "${KUBLING_OBJECT_STORAGE_PROVIDER_LISTEN:-:50053}"
}

case "${COMMAND}" in
    run)
        start_store
        run_provider
        ;;
    store)
        start_store
        ;;
    provider)
        run_provider
        ;;
    filesystem)
        run_filesystem_provider
        ;;
    azure)
        start_azure_store
        run_azure_provider
        ;;
    azure-store)
        start_azure_store
        ;;
    azure-provider)
        run_azure_provider
        ;;
    gcs)
        start_gcs_store
        run_gcs_provider
        ;;
    gcs-store)
        start_gcs_store
        ;;
    gcs-provider)
        run_gcs_provider
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
    azure-reset)
        compose down --volumes
        exec "${BASH_SOURCE[0]}" azure
        ;;
    gcs-reset)
        compose down --volumes
        exec "${BASH_SOURCE[0]}" gcs
        ;;
    *)
        echo "usage: $0 [run|store|provider|filesystem|azure|azure-store|azure-provider|gcs|gcs-store|gcs-provider|status|down|reset|azure-reset|gcs-reset]" >&2
        exit 2
        ;;
esac
