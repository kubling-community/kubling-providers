#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_DIR="$(cd -- "${SCRIPT_DIR}/../../.." && pwd)"

if [[ -n "${GO_BIN:-}" ]]; then
    go_bin="${GO_BIN}"
elif [[ -x /usr/local/go/bin/go ]]; then
    go_bin=/usr/local/go/bin/go
else
    go_bin="$(command -v go)"
fi

cd "${REPOSITORY_DIR}/testing"
"${go_bin}" run ./cmd/provider-test smoke
"${go_bin}" run ./cmd/provider-test query -sql \
    "SELECT namespace, host_id, hostname, state FROM provider.HOST WHERE namespace = 'local'"
"${go_bin}" run ./cmd/provider-test query -sql \
    "SELECT namespace, host_id, total_bytes, available_bytes FROM provider.MEMORY WHERE namespace = 'local'"
