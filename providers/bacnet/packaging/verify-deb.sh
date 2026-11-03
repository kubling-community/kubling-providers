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

if [[ "${package_name}" != "kubling-bacnet-provider" ]]; then
  echo "unexpected package name: ${package_name}" >&2
  exit 1
fi
if [[ -z "${package_version}" ]]; then
  echo "package version is empty" >&2
  exit 1
fi
if [[ "${package_architecture}" != "amd64" && "${package_architecture}" != "arm64" ]]; then
  echo "unexpected package architecture: ${package_architecture}" >&2
  exit 1
fi

inspection_directory="$(mktemp -d)"
trap 'rm -rf -- "${inspection_directory}"' EXIT
payload_directory="${inspection_directory}/payload"
control_directory="${inspection_directory}/control"
dpkg-deb --extract "${package}" "${payload_directory}"
dpkg-deb --control "${package}" "${control_directory}"

binary="${payload_directory}/usr/bin/kubling-provider-bacnet"
service="${payload_directory}/usr/lib/systemd/system/kubling-bacnet-provider.service"
test -x "${binary}"
test -f "${service}"
test -f "${payload_directory}/etc/kubling/bacnet-provider.yaml.example"
test -f "${payload_directory}/usr/share/doc/kubling-bacnet-provider/README.md"
test -f "${payload_directory}/usr/share/doc/kubling-bacnet-provider/profiles/README.md"
test -f "${payload_directory}/usr/share/doc/kubling-bacnet-provider/copyright"
test -x "${control_directory}/postinst"
test -x "${control_directory}/prerm"
test -x "${control_directory}/postrm"

grep -Fq \
  'ExecStart=/usr/bin/kubling-provider-bacnet -config /etc/kubling/bacnet-provider.yaml' \
  "${service}"
grep -Fq 'DynamicUser=yes' "${service}"

machine="$(readelf -h "${binary}" | awk -F: '/Machine:/ {sub(/^[[:space:]]+/, "", $2); print $2}')"
case "${package_architecture}" in
  amd64)
    [[ "${machine}" == "Advanced Micro Devices X86-64" ]]
    ;;
  arm64)
    [[ "${machine}" == "AArch64" ]]
    ;;
esac

printf '%s %s %s\n' "${package_name}" "${package_version}" "${package_architecture}"
