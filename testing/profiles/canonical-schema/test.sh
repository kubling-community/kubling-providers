#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GO_BIN="${GO_BIN:-go}"

cd "${SCRIPT_DIR}/../.."
exec "${GO_BIN}" run ./cmd/provider-test canonical
