#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <package.deb>" >&2
  exit 2
fi

package="$1"
if [[ ! -f "${package}" ]]; then
  echo "package not found: ${package}" >&2
  exit 1
fi

package_name="$(dpkg-deb --field "${package}" Package)"
package_version="$(dpkg-deb --field "${package}" Version)"
package_architecture="$(dpkg-deb --field "${package}" Architecture)"

if [[ -z "${package_version}" ]]; then
  echo "package version is empty" >&2
  exit 1
fi
if [[ "${package_architecture}" != "amd64" && "${package_architecture}" != "arm64" ]]; then
  echo "unexpected package architecture: ${package_architecture}" >&2
  exit 1
fi

extraction_directory="$(mktemp -d)"
trap 'rm -rf -- "${extraction_directory}"' EXIT
dpkg-deb --extract "${package}" "${extraction_directory}"

case "${package_name}" in
  kubling-host-agent)
    test -x "${extraction_directory}/usr/bin/kubling-host-agent"
    test -f "${extraction_directory}/usr/lib/systemd/system/kubling-host-agent.service"
    test -f "${extraction_directory}/etc/kubling/host-agent.yaml.example"
    test -f "${extraction_directory}/usr/share/doc/kubling-host-agent/README.md"
    test -f "${extraction_directory}/usr/share/doc/kubling-host-agent/docs/deployment.md"
    test -f "${extraction_directory}/usr/share/doc/kubling-host-agent/copyright"
    grep -Fq \
      'ExecStart=/usr/bin/kubling-host-agent --config /etc/kubling/host-agent.yaml' \
      "${extraction_directory}/usr/lib/systemd/system/kubling-host-agent.service"
    ;;
  kubling-host-provider)
    test -x "${extraction_directory}/usr/bin/kubling-host-provider"
    test -f "${extraction_directory}/usr/share/doc/kubling-host-provider/README.md"
    test -f "${extraction_directory}/usr/share/doc/kubling-host-provider/docs/deployment.md"
    test -f "${extraction_directory}/usr/share/doc/kubling-host-provider/copyright"
    ;;
  *)
    echo "unexpected package name: ${package_name}" >&2
    exit 1
    ;;
esac

printf '%s %s %s\n' "${package_name}" "${package_version}" "${package_architecture}"
