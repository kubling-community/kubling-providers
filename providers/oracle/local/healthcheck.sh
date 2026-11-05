#!/usr/bin/env bash

set -euo pipefail

/opt/oracle/checkDBStatus.sh >/dev/null

printf '%s\n' \
    'WHENEVER SQLERROR EXIT FAILURE' \
    'ALTER SESSION SET CONTAINER = FREEPDB1;' \
    'SELECT COUNT(*) FROM kubling.KUBLING_PROVIDER_TX_BRANCHES;' \
    'EXIT' \
    | sqlplus -L -s / as sysdba >/dev/null
