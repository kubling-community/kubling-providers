#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <version> [amd64|arm64] [output-directory]" >&2
}

if [[ $# -lt 1 || $# -gt 3 ]]; then
  usage
  exit 2
fi

version="${1#v}"
architecture="${2:-amd64}"
output_directory="${3:-dist}"

if [[ ! "${version}" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid semantic version: ${version}" >&2
  exit 2
fi
if [[ "${architecture}" != "amd64" && "${architecture}" != "arm64" ]]; then
  echo "unsupported architecture: ${architecture}" >&2
  exit 2
fi

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
provider_directory="$(cd "${script_directory}/.." && pwd)"
repository_directory="$(cd "${provider_directory}/../.." && pwd)"
go_binary="${GO_BINARY:-go}"
nfpm_binary="${NFPM_BINARY:-nfpm}"

if ! command -v "${go_binary}" >/dev/null 2>&1; then
  echo "Go executable not found: ${go_binary}" >&2
  exit 1
fi
if ! command -v "${nfpm_binary}" >/dev/null 2>&1; then
  echo "nFPM executable not found: ${nfpm_binary}" >&2
  exit 1
fi

if [[ "${output_directory}" != /* ]]; then
  output_directory="${provider_directory}/${output_directory}"
fi
mkdir -p "${output_directory}"

package_binary="${provider_directory}/build/kubling-provider-bacnet"
mkdir -p "$(dirname "${package_binary}")"
trap 'rm -f -- "${package_binary}"' EXIT

if [[ -z "${SOURCE_DATE_EPOCH:-}" ]]; then
  SOURCE_DATE_EPOCH="$(git -C "${repository_directory}" show -s --format=%ct HEAD)"
  export SOURCE_DATE_EPOCH
fi

(
  cd "${provider_directory}"
  CGO_ENABLED=0 GOOS=linux GOARCH="${architecture}" "${go_binary}" build \
    -trimpath \
    -ldflags="-s -w" \
    -o "${package_binary}" \
    ./cmd/kubling-provider-bacnet

  PACKAGE_ARCH="${architecture}" \
  PACKAGE_VERSION="${version}" \
    "${nfpm_binary}" package \
      --config packaging/nfpm.yaml \
      --packager deb \
      --target "${output_directory}/kubling-bacnet-provider_${version}_${architecture}.deb"
)
