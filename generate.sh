#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
LANGUAGE="${1:-all}"

cd "${SCRIPT_DIR}"

case "$LANGUAGE" in
  all|go|java|python) ;;
  *) echo "Usage: $0 [all|go|java|python]" >&2; exit 2 ;;
esac

"${SCRIPT_DIR}/tools/bootstrap.sh"

if [[ "$LANGUAGE" == all || "$LANGUAGE" == go ]]; then
  "${SCRIPT_DIR}/tools/bin/buf" generate --template buf.gen.yaml
fi

for sdk in java python; do
  if [[ "$LANGUAGE" == all || "$LANGUAGE" == "$sdk" ]]; then
    "${SCRIPT_DIR}/tools/bin/buf" generate --template "sdk-${sdk}/buf.gen.yaml"
  fi
done
