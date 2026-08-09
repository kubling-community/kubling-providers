#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(dirname "$0")"

"${SCRIPT_DIR}/tools/bootstrap.sh"

"${SCRIPT_DIR}/tools/bin/buf" generate