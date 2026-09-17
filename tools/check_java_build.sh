#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

JAVA_COMMAND="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
MAVEN_COMMAND="${MAVEN_COMMAND:-mvn}"
JAVA_PROPERTIES="$("${JAVA_COMMAND}" -XshowSettings:properties -version 2>&1)"
JAVA_VENDOR_VERSION="$(sed -n 's/^[[:space:]]*java.vendor.version = //p' <<< "${JAVA_PROPERTIES}")"
JAVA_SPECIFICATION="$(sed -n 's/^[[:space:]]*java.specification.version = //p' <<< "${JAVA_PROPERTIES}")"

if [[ "${JAVA_VENDOR_VERSION}" != "Oracle GraalVM 25."* || "${JAVA_SPECIFICATION}" != 25 ]]; then
  echo "The Java build requires Oracle GraalVM 25, as in Kubling Core." >&2
  exit 1
fi

TARGET_RELEASE="$("${MAVEN_COMMAND}" --quiet --no-transfer-progress -f sdk-java/pom.xml \
  help:evaluate -Dexpression=maven.compiler.release -DforceStdout | tail -n 1)"
if [[ "${TARGET_RELEASE}" != 21 ]]; then
  echo "The Java artifact must target release 21, as in Kubling Core." >&2
  exit 1
fi

echo "Verified GraalVM 25 build JDK and Java 21 artifact target"
