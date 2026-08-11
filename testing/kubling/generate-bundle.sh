#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBLING_CLI_IMAGE="${KUBLING_CLI_IMAGE:-kubling/kubling-cli:latest}"

docker run --rm \
    --volume "${SCRIPT_DIR}:/workspace" \
    "${KUBLING_CLI_IMAGE}" \
    bundle genmod /workspace/descriptor \
    -o /workspace/provider-test-descriptor-bundle.zip \
    --parse
