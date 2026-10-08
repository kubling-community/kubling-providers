#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_DIR="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
BUF="${REPOSITORY_DIR}/tools/bin/buf"

if [[ ! -x "${BUF}" ]]; then
  "${REPOSITORY_DIR}/tools/bootstrap.sh"
fi

cd "${SCRIPT_DIR}"
"${BUF}" format --diff --exit-code
"${BUF}" lint
"${BUF}" generate --template buf.gen.yaml
