#!/bin/sh
set -eu

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload >/dev/null 2>&1 || true
  if systemctl is-active --quiet kubling-host-agent.service; then
    systemctl try-restart kubling-host-agent.service >/dev/null 2>&1 || true
  fi
fi

exit 0
