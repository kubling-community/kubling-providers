#!/usr/bin/env bash

set -e

BUF_VERSION="1.59.0"

TOOLS_DIR="$(dirname "$0")"
BIN_DIR="${TOOLS_DIR}/bin"

mkdir -p "${BIN_DIR}"

if [ ! -x "${BIN_DIR}/buf" ]; then
    echo "Downloading buf ${BUF_VERSION}..."

    curl -sSL \
      "https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/buf-Linux-x86_64" \
      -o "${BIN_DIR}/buf"

    chmod +x "${BIN_DIR}/buf"
fi

echo "buf installed"