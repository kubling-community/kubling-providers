#!/bin/sh

set -eu

redis_cli="redis-cli -h redis --raw"

$redis_cli FLUSHDB

$redis_cli HSET PROJECT:project-1 \
  name "Provider SDK" status A active true budget 125000.50 \
  started_on 2026-07-01 metadata '{"owner":"platform","region":"eu"}'
$redis_cli HSET PROJECT:project-2 \
  name "Engine Integration" status P active true budget 87500.00 \
  started_on 2026-08-01 metadata '{"owner":"federation","region":"global"}'

$redis_cli HSET TASK:task-1 \
  project_id project-1 title "Review provider SDK" \
  description "Validate the public provider lifecycle through the SDK adapter." \
  completed true priority 1 estimate_hours 2.5 due_at '2026-08-04 17:00:00.000'
$redis_cli HSET TASK:task-2 \
  project_id project-1 title "Build in-memory provider" \
  completed false priority 2 estimate_hours 4.0 due_at '2026-08-07 12:30:00.000'
$redis_cli HSET TASK:task-3 \
  project_id project-2 title "Connect Kubling" completed false priority 3

$redis_cli HSET AUDIT_EVENT:1001 \
  entity_type TASK entity_id task-1 action U sequence 1 risk_score 0.15 \
  occurred_at '2026-08-02 09:15:30.125' source_address 127.0.0.1 \
  payload '{"completed":true}' raw_payload 'completed=true'
$redis_cli HSET AUDIT_EVENT:1002 \
  entity_type PROJECT entity_id project-2 action C sequence 2 risk_score 0.05 \
  occurred_at '2026-08-03 10:00:00.000' source_address 10.0.0.42 \
  payload '{"status":"P"}' raw_payload 'status=P'

$redis_cli HSET TYPE_SAMPLE:canonical \
  string_value Kubling varbinary_value KUB char_value K boolean_value true \
  byte_value 127 short_value 32767 integer_value 2147483647 \
  long_value 9223372036854775000 \
  biginteger_value 123456789012345678901234567890 \
  float_value 3.25 double_value 3.141592653589793 \
  bigdecimal_value 1234567890.12345678901234567890 \
  date_value 2026-08-03 time_value 14:30:15.125000000 \
  timestamp_value '2026-08-03 14:30:15.125' \
  blob_value 'binary large object' clob_value 'character large object' \
  geometry_value 'POINT(1 2)' geography_value 'POINT(1 2)' \
  json_value '{"engine":"kubling","sample":true}' \
  xml_value '<sample engine="kubling"/>'
