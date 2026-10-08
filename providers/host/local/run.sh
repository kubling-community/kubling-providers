#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
RUNTIME_DIR="${KUBLING_HOST_LOCAL_STATE:-${TMPDIR:-/tmp}/kubling-host-local-${UID}}"
PROVIDER_LISTEN="${KUBLING_HOST_LOCAL_PROVIDER_LISTEN:-0.0.0.0:50051}"
AGENT_LISTEN="${KUBLING_HOST_LOCAL_AGENT_LISTEN:-127.0.0.1:50052}"
AGENT_TARGET="${KUBLING_HOST_LOCAL_AGENT_TARGET:-127.0.0.1:50052}"
NAMESPACE="${KUBLING_HOST_LOCAL_NAMESPACE:-local}"

umask 077
mkdir -p \
    "${RUNTIME_DIR}/bin" \
    "${RUNTIME_DIR}/provider" \
    "${RUNTIME_DIR}/agent" \
    "${RUNTIME_DIR}/tls"

if [[ -n "${GO_BIN:-}" ]]; then
    go_bin="${GO_BIN}"
elif [[ -x /usr/local/go/bin/go ]]; then
    go_bin=/usr/local/go/bin/go
else
    go_bin="$(command -v go)"
fi

create_credentials() {
    if [[ -s "${RUNTIME_DIR}/tls/ca.pem" && \
          -s "${RUNTIME_DIR}/tls/server.pem" && \
          -s "${RUNTIME_DIR}/tls/server-key.pem" && \
          -s "${RUNTIME_DIR}/agent.token" ]] && \
       openssl x509 -checkend 0 -noout -in "${RUNTIME_DIR}/tls/ca.pem" && \
       openssl x509 -checkend 0 -noout -in "${RUNTIME_DIR}/tls/server.pem"; then
        return
    fi

    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "${RUNTIME_DIR}/tls/ca-key.pem" \
        -out "${RUNTIME_DIR}/tls/ca.pem" \
        -days 30 \
        -subj '/CN=Kubling Host Local CA' >/dev/null 2>&1
    openssl req -newkey rsa:2048 -nodes \
        -keyout "${RUNTIME_DIR}/tls/server-key.pem" \
        -out "${RUNTIME_DIR}/tls/server.csr" \
        -subj '/CN=localhost' >/dev/null 2>&1
    openssl x509 -req \
        -in "${RUNTIME_DIR}/tls/server.csr" \
        -CA "${RUNTIME_DIR}/tls/ca.pem" \
        -CAkey "${RUNTIME_DIR}/tls/ca-key.pem" \
        -CAcreateserial \
        -out "${RUNTIME_DIR}/tls/server.pem" \
        -days 30 \
        -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n') \
        >/dev/null 2>&1
    openssl rand -hex 32 >"${RUNTIME_DIR}/agent.token"
}

build_binaries() {
    (
        cd "${MODULE_DIR}"
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
            GOFLAGS="${GOFLAGS:--buildvcs=false}" \
            "${go_bin}" build -o "${RUNTIME_DIR}/bin/kubling-host-provider" \
            ./cmd/kubling-host-provider
        GOCACHE="${GOCACHE:-/tmp/kubling-providers-go-build}" \
            GOFLAGS="${GOFLAGS:--buildvcs=false}" \
            "${go_bin}" build -o "${RUNTIME_DIR}/bin/kubling-host-agent" \
            ./cmd/kubling-host-agent
    )
}

create_credentials
build_binaries

pids=()
stop_processes() {
    if [[ ${#pids[@]} -eq 0 ]]; then
        return
    fi
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
}
trap stop_processes EXIT INT TERM

"${RUNTIME_DIR}/bin/kubling-host-provider" \
    -provider-listen "${PROVIDER_LISTEN}" \
    -agent-listen "${AGENT_LISTEN}" \
    -state-directory "${RUNTIME_DIR}/provider" \
    -agent-tls-certificate "${RUNTIME_DIR}/tls/server.pem" \
    -agent-tls-private-key "${RUNTIME_DIR}/tls/server-key.pem" \
    -namespace-token-file "${NAMESPACE}=${RUNTIME_DIR}/agent.token" \
    -allow-partial-results &
pids+=("$!")

"${RUNTIME_DIR}/bin/kubling-host-agent" \
    -gateway "${AGENT_TARGET}" \
    -namespace "${NAMESPACE}" \
    -state-directory "${RUNTIME_DIR}/agent" \
    -credential-file "${RUNTIME_DIR}/agent.token" \
    -tls-ca "${RUNTIME_DIR}/tls/ca.pem" \
    -tls-server-name localhost &
pids+=("$!")

printf 'Starting Host Provider for Kubling at %s (namespace %s)\n' \
    "${PROVIDER_LISTEN}" "${NAMESPACE}"
printf 'Runtime state: %s\n' "${RUNTIME_DIR}"

set +e
wait -n "${pids[@]}"
status=$?
set -e
exit "${status}"
