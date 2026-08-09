-- This native Cassandra fixture and ../schema.example.sql expose equivalent logical Kubling tables.
-- Cassandra reads this CQL; the SQL file is the equivalent Kubling DDL example.

CREATE KEYSPACE IF NOT EXISTS kubling_sample
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS kubling_sample."PROJECT"
(
    id text PRIMARY KEY,
    name text,
    status ascii,
    active boolean,
    budget decimal,
    started_on date,
    metadata map<text, text>
);

CREATE TABLE IF NOT EXISTS kubling_sample."TASK"
(
    id text PRIMARY KEY,
    project_id text,
    title text,
    description text,
    completed boolean,
    priority int,
    estimate_hours float,
    due_at timestamp
);

CREATE INDEX IF NOT EXISTS task_project_id_idx
ON kubling_sample."TASK" (project_id);

CREATE TABLE IF NOT EXISTS kubling_sample."AUDIT_EVENT"
(
    id bigint PRIMARY KEY,
    entity_type text,
    entity_id text,
    action ascii,
    sequence smallint,
    risk_score double,
    occurred_at timestamp,
    source_address blob,
    payload map<text, text>,
    raw_payload blob
);

CREATE TABLE IF NOT EXISTS kubling_sample."TYPE_SAMPLE"
(
    sample_id text PRIMARY KEY,
    string_value text,
    varbinary_value blob,
    char_value ascii,
    boolean_value boolean,
    byte_value tinyint,
    short_value smallint,
    integer_value int,
    long_value bigint,
    biginteger_value varint,
    float_value float,
    double_value double,
    bigdecimal_value decimal,
    date_value date,
    time_value time,
    timestamp_value timestamp,
    blob_value blob,
    clob_value text,
    geometry_value blob,
    geography_value blob,
    json_value map<text, text>,
    xml_value text
);

INSERT INTO kubling_sample."PROJECT"
    (id, name, status, active, budget, started_on, metadata)
VALUES
    ('project-1', 'Provider SDK', 'A', true, 125000.50, '2026-07-01', {'owner': 'platform', 'region': 'eu'});

INSERT INTO kubling_sample."PROJECT"
    (id, name, status, active, budget, started_on, metadata)
VALUES
    ('project-2', 'Engine Integration', 'P', true, 87500.00, '2026-08-01', {'owner': 'federation', 'region': 'global'});

INSERT INTO kubling_sample."TASK"
    (id, project_id, title, description, completed, priority, estimate_hours, due_at)
VALUES
    ('task-1', 'project-1', 'Review provider SDK', 'Validate the public provider lifecycle through the SDK adapter.', true, 1, 2.5, '2026-08-04T17:00:00.000Z');

INSERT INTO kubling_sample."TASK"
    (id, project_id, title, completed, priority, estimate_hours, due_at)
VALUES
    ('task-2', 'project-1', 'Build in-memory provider', false, 2, 4.0, '2026-08-07T12:30:00.000Z');

INSERT INTO kubling_sample."TASK"
    (id, project_id, title, completed, priority)
VALUES
    ('task-3', 'project-2', 'Connect Kubling', false, 3);

INSERT INTO kubling_sample."AUDIT_EVENT"
    (id, entity_type, entity_id, action, sequence, risk_score, occurred_at, source_address, payload, raw_payload)
VALUES
    (1001, 'TASK', 'task-1', 'U', 1, 0.15, '2026-08-02T09:15:30.125Z', 0x7f000001, {'completed': 'true'}, 0x636f6d706c657465643d74727565);

INSERT INTO kubling_sample."AUDIT_EVENT"
    (id, entity_type, entity_id, action, sequence, risk_score, occurred_at, source_address, payload, raw_payload)
VALUES
    (1002, 'PROJECT', 'project-2', 'C', 2, 0.05, '2026-08-03T10:00:00.000Z', 0x0a00002a, {'status': 'P'}, 0x7374617475733d50);

INSERT INTO kubling_sample."TYPE_SAMPLE"
    (sample_id, string_value, varbinary_value, char_value, boolean_value,
     byte_value, short_value, integer_value, long_value, biginteger_value,
     float_value, double_value, bigdecimal_value, date_value, time_value,
     timestamp_value, blob_value, clob_value, geometry_value, geography_value,
     json_value, xml_value)
VALUES
    ('canonical', 'Kubling', 0x4b5542, 'K', true,
     127, 32767, 2147483647, 9223372036854775000, 123456789012345678901234567890,
     3.25, 3.141592653589793, 1234567890.12345678901234567890,
     '2026-08-03', '14:30:15.125000000', '2026-08-03T14:30:15.125Z',
     0x62696e617279206c61726765206f626a656374, 'character large object',
     0x0101000000000000000000f03f0000000000000040,
     0x0101000000000000000000f03f0000000000000040,
     {'engine': 'kubling', 'sample': 'true'}, '<sample engine="kubling"/>');
