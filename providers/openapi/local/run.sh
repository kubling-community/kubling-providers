#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
ENV_FILE="${OPENAI_LOCAL_ENV_FILE:-${SCRIPT_DIR}/.env}"
COMMAND="${1:-run}"

compose() {
    local arguments=(--file "${COMPOSE_FILE}")
    if [ -f "${ENV_FILE}" ]; then
        arguments+=(--env-file "${ENV_FILE}")
    fi
    docker compose "${arguments[@]}" "$@"
}

require_configuration() {
    local rendered
    rendered="$(compose config)"

    if printf '%s\n' "${rendered}" | grep -Eq 'OPENAI_ADMIN_KEY: (""|null)$'; then
        echo "OPENAI_ADMIN_KEY is required; copy local/.env.example to local/.env and edit it" >&2
        exit 2
    fi
    if printf '%s\n' "${rendered}" | grep -q 'OPENAI_ADMIN_KEY: sk-admin-replace-me'; then
        echo "replace OPENAI_ADMIN_KEY in local/.env with an organization Admin API key" >&2
        exit 2
    fi
    if printf '%s\n' "${rendered}" | grep -Eq 'OPENAI_USAGE_START_TIME: (""|null)$'; then
        echo "OPENAI_USAGE_START_TIME is required; use a Unix timestamp such as \$(date -d '30 days ago' +%s)" >&2
        exit 2
    fi
    if ! printf '%s\n' "${rendered}" | grep -Eq 'OPENAI_USAGE_START_TIME: "?[0-9]+"?$'; then
        echo "OPENAI_USAGE_START_TIME must be a Unix timestamp" >&2
        exit 2
    fi
}

provider_port() {
    compose config | awk '
        $1 == "published:" {
            gsub(/"/, "", $2)
            print $2
            exit
        }
    '
}

ensure_provider_port_available() {
    local port
    port="$(provider_port)"
    if [ -z "${port}" ]; then
        return
    fi
    if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
        echo "local port ${port} is already in use" >&2
        echo "choose another port, for example:" >&2
        echo "  KUBLING_OPENAPI_PROVIDER_PORT=50056 $0 ${COMMAND}" >&2
        echo "or set KUBLING_OPENAPI_PROVIDER_PORT in local/.env" >&2
        exit 2
    fi
}

start_provider() {
    require_configuration
    ensure_provider_port_available
    compose up --detach --build provider
}

case "${COMMAND}" in
    run)
        start_provider
        compose logs --follow provider
        ;;

    provider)
        start_provider
        ;;

    check)
        require_configuration
        compose run --rm provider -check
        ;;

    metadata)
        require_configuration
        compose run --rm provider -print-metadata
        ;;

    template)
        require_configuration
        compose run --rm provider -generate-config-template
        ;;

    build)
        compose build provider
        ;;

    logs)
        compose logs --follow provider
        ;;

    status)
        compose ps
        ;;

    down)
        compose down
        ;;

    reset)
        compose down --volumes --remove-orphans
        start_provider
        compose logs --follow provider
        ;;

    *)
        echo "usage: $0 [run|provider|check|metadata|template|build|logs|status|down|reset]" >&2
        exit 2
        ;;
esac
