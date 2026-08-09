#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
KUBECONFIG_PATH="${SCRIPT_DIR}/kubeconfig.local.yaml"
COMMAND="${1:-run}"

if [ "$#" -gt 0 ]; then
    shift
fi

compose() {
    docker compose --file "${COMPOSE_FILE}" "$@"
}

write_kubeconfig() {
    local api_port="${K3S_API_PORT:-6443}"
    local raw_config
    local rewritten_config="${KUBECONFIG_PATH}.tmp"

    raw_config="$(mktemp)"
    trap 'rm -f "${raw_config}" "${rewritten_config}"' RETURN

    compose cp \
        k3s:/etc/rancher/k3s/k3s.yaml \
        "${raw_config}"

    sed -E \
        "s#server: https://[^:]+:[0-9]+#server: https://127.0.0.1:${api_port}#" \
        "${raw_config}" > "${rewritten_config}"

    chmod 600 "${rewritten_config}"
    mv "${rewritten_config}" "${KUBECONFIG_PATH}"

    rm -f "${raw_config}"
    trap - RETURN
}

start_k3s() {
    compose up --detach --wait k3s

    compose exec --no-TTY \
        k3s \
        kubectl apply -f /fixtures/fixture.yaml

    write_kubeconfig
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

    if [ ! -f "${KUBECONFIG_PATH}" ]; then
        write_kubeconfig
    fi

    cd "${MODULE_DIR}"

    exec env \
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
        GOFLAGS="${GOFLAGS:--buildvcs=false}" \
        "${go_bin}" run ./cmd/kubernetes \
        -config "${SCRIPT_DIR}/provider.yaml" \
        -listen "${KUBLING_KUBERNETES_PROVIDER_LISTEN:-:50054}"
}

case "${COMMAND}" in
    run)
        start_k3s
        run_provider
        ;;

    k3s)
        start_k3s
        ;;

    provider)
        run_provider
        ;;

    kubeconfig)
        write_kubeconfig
        printf '%s\n' "${KUBECONFIG_PATH}"
        ;;

    kubectl)
        compose exec --no-TTY \
            k3s \
            kubectl "$@"
        ;;

    logs)
        compose logs --follow k3s
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
        echo \
            "usage: $0 [run|k3s|provider|kubeconfig|kubectl|logs|status|down|reset]" \
            >&2
        exit 2
        ;;
esac