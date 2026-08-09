-- This Kubling DDL and schema.example.yaml define equivalent logical tables.
-- The Redis provider reads the YAML schema; this SQL file is an equivalent example.
-- "kbl.namespace" preserves the provider namespace that structured metadata carries separately.
-- The value "sample" matches config.example.yaml and must change if that namespace key changes.

CREATE FOREIGN TABLE PROJECT
(
    id string NOT NULL OPTIONS(ANNOTATION 'Stable identifier of the project.'),
    name string NOT NULL OPTIONS(ANNOTATION 'Human-readable project name.'),
    status char NOT NULL OPTIONS(ANNOTATION 'Single-character project lifecycle code.'),
    active boolean NOT NULL OPTIONS(ANNOTATION 'Whether work on the project is active.'),
    budget bigdecimal OPTIONS(ANNOTATION 'Planned project budget.'),
    started_on date OPTIONS(ANNOTATION 'Calendar date on which the project started.'),
    metadata json OPTIONS(ANNOTATION 'Extensible project metadata.'),
    PRIMARY KEY(id)
)
OPTIONS(
    updatable false,
    "kbl.namespace" 'sample',
    ANNOTATION 'Projects used to group tasks in the Redis sample domain.'
);

CREATE FOREIGN TABLE TASK
(
    id string NOT NULL OPTIONS(ANNOTATION 'Stable identifier of the task.'),
    project_id string NOT NULL OPTIONS(ANNOTATION 'Identifier of the owning project.'),
    title string NOT NULL OPTIONS(ANNOTATION 'Short description of the work.'),
    description clob OPTIONS(ANNOTATION 'Potentially large task description.'),
    completed boolean NOT NULL OPTIONS(ANNOTATION 'Whether the task is complete.'),
    priority integer NOT NULL OPTIONS(ANNOTATION 'Relative scheduling priority.'),
    estimate_hours float OPTIONS(ANNOTATION 'Estimated effort in hours.'),
    due_at timestamp OPTIONS(ANNOTATION 'Local completion deadline.'),
    PRIMARY KEY(id)
)
OPTIONS(
    updatable true,
    "kbl.namespace" 'sample',
    ANNOTATION 'Mutable work items stored as Redis hashes.'
);

CREATE FOREIGN TABLE AUDIT_EVENT
(
    id long NOT NULL OPTIONS(ANNOTATION 'Monotonically increasing event identifier.'),
    entity_type string NOT NULL OPTIONS(ANNOTATION 'Logical object type that produced the event.'),
    entity_id string NOT NULL OPTIONS(ANNOTATION 'Identifier of the affected object.'),
    action char NOT NULL OPTIONS(ANNOTATION 'Single-character action code.'),
    sequence short NOT NULL OPTIONS(ANNOTATION 'Source-local sequence number.'),
    risk_score double OPTIONS(ANNOTATION 'Computed event risk score.'),
    occurred_at timestamp NOT NULL OPTIONS(ANNOTATION 'Timestamp at which the event occurred.'),
    source_address varbinary OPTIONS(ANNOTATION 'Binary source network address.'),
    payload json OPTIONS(ANNOTATION 'Structured event payload.'),
    raw_payload blob OPTIONS(ANNOTATION 'Original binary event payload.'),
    PRIMARY KEY(id)
)
OPTIONS(
    updatable false,
    "kbl.namespace" 'sample',
    ANNOTATION 'Read-only audit events in the Redis sample domain.'
);

CREATE FOREIGN TABLE TYPE_SAMPLE
(
    sample_id string NOT NULL OPTIONS(ANNOTATION 'Identifier of the canonical sample row.'),
    string_value string NOT NULL OPTIONS(ANNOTATION 'Variable-length character string.'),
    varbinary_value varbinary NOT NULL OPTIONS(ANNOTATION 'Variable-length binary value.'),
    char_value char NOT NULL OPTIONS(ANNOTATION 'Single character.'),
    boolean_value boolean NOT NULL OPTIONS(ANNOTATION 'Boolean value.'),
    byte_value byte NOT NULL OPTIONS(ANNOTATION 'Signed 8-bit integer.'),
    short_value short NOT NULL OPTIONS(ANNOTATION 'Signed 16-bit integer.'),
    integer_value integer NOT NULL OPTIONS(ANNOTATION 'Signed 32-bit integer.'),
    long_value long NOT NULL OPTIONS(ANNOTATION 'Signed 64-bit integer.'),
    biginteger_value biginteger NOT NULL OPTIONS(ANNOTATION 'Arbitrary precision integer.'),
    float_value float NOT NULL OPTIONS(ANNOTATION '32-bit floating-point value.'),
    double_value double NOT NULL OPTIONS(ANNOTATION '64-bit floating-point value.'),
    bigdecimal_value bigdecimal NOT NULL OPTIONS(ANNOTATION 'Arbitrary precision decimal.'),
    date_value date NOT NULL OPTIONS(ANNOTATION 'Calendar date.'),
    time_value time NOT NULL OPTIONS(ANNOTATION 'Local time.'),
    timestamp_value timestamp NOT NULL OPTIONS(ANNOTATION 'Local timestamp.'),
    blob_value blob NOT NULL OPTIONS(ANNOTATION 'Binary large object.'),
    clob_value clob NOT NULL OPTIONS(ANNOTATION 'Character large object.'),
    geometry_value geometry NOT NULL OPTIONS(ANNOTATION 'Geometry bytes.'),
    geography_value geography NOT NULL OPTIONS(ANNOTATION 'Geography bytes.'),
    json_value json NOT NULL OPTIONS(ANNOTATION 'JSON document.'),
    xml_value xml NOT NULL OPTIONS(ANNOTATION 'XML compatibility value.'),
    PRIMARY KEY(sample_id)
)
OPTIONS(
    updatable false,
    "kbl.namespace" 'sample',
    ANNOTATION 'Canonical row covering every concrete Kubling value type.'
);
