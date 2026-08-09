CREATE FOREIGN TABLE PROJECT
(
    id string NOT NULL OPTIONS(ANNOTATION 'Stable identifier of the project'),
    name string NOT NULL OPTIONS(ANNOTATION 'Human-readable project name'),
    status char NOT NULL OPTIONS(ANNOTATION 'Single-character project lifecycle code'),
    active boolean NOT NULL OPTIONS(ANNOTATION 'Whether work on the project is currently active'),
    budget bigdecimal OPTIONS(ANNOTATION 'Planned project budget with arbitrary decimal precision'),
    started_on date OPTIONS(ANNOTATION 'Calendar date on which the project started'),
    metadata json OPTIONS(ANNOTATION 'Extensible project metadata encoded as JSON'),
    PRIMARY KEY(id)
)
OPTIONS(
    updatable false,
    tags 'sample;project-management',
    relationships 'TASK,AUDIT_EVENT',
    ANNOTATION 'Projects used to group tasks in the in-memory reference provider'
);

CREATE FOREIGN TABLE TASK
(
    id string NOT NULL OPTIONS(ANNOTATION 'Stable identifier of the task'),
    project_id string NOT NULL DEFAULT 'project-1' OPTIONS(ANNOTATION 'Identifier of the project that owns the task'),
    title string NOT NULL OPTIONS(ANNOTATION 'Short description of the work to perform'),
    description clob OPTIONS(ANNOTATION 'Potentially large detailed task description'),
    completed boolean NOT NULL DEFAULT false OPTIONS(ANNOTATION 'Whether the task has been completed'),
    priority integer NOT NULL DEFAULT 0 OPTIONS(ANNOTATION 'Relative scheduling priority where larger values are more important'),
    estimate_hours float OPTIONS(ANNOTATION 'Estimated effort in hours'),
    due_at timestamp OPTIONS(ANNOTATION 'Local timestamp by which the task should be completed'),
    PRIMARY KEY(id)
)
OPTIONS(
    updatable true,
    tags 'sample;project-management',
    relationships 'PROJECT,AUDIT_EVENT',
    ANNOTATION 'Mutable work items stored by the in-memory reference provider'
);

CREATE FOREIGN TABLE AUDIT_EVENT
(
    id long NOT NULL OPTIONS(ANNOTATION 'Monotonically increasing audit event identifier'),
    entity_type string NOT NULL OPTIONS(ANNOTATION 'Logical type of the object that produced the event'),
    entity_id string NOT NULL OPTIONS(ANNOTATION 'Identifier of the object that produced the event'),
    action char NOT NULL OPTIONS(ANNOTATION 'Single-character action code such as C, U, or D'),
    sequence short NOT NULL OPTIONS(ANNOTATION 'Source-local sequence number of the event'),
    risk_score double OPTIONS(ANNOTATION 'Computed risk score associated with the event'),
    occurred_at timestamp NOT NULL OPTIONS(ANNOTATION 'Local timestamp at which the event occurred'),
    source_address varbinary OPTIONS(ANNOTATION 'Binary representation of the source network address'),
    payload json OPTIONS(ANNOTATION 'Structured event payload encoded as JSON'),
    raw_payload blob OPTIONS(ANNOTATION 'Original binary event payload'),
    PRIMARY KEY(id)
)
OPTIONS(
    updatable false,
    tags 'sample;audit',
    relationship_affected_by 'PROJECT,TASK',
    ANNOTATION 'Read-only audit trail for the sample project domain'
);

CREATE FOREIGN TABLE TYPE_SAMPLE
(
    sample_id string NOT NULL OPTIONS(ANNOTATION 'Identifier of the canonical type sample row'),
    string_value string NOT NULL OPTIONS(ANNOTATION 'Example variable-length character string'),
    varbinary_value varbinary NOT NULL OPTIONS(ANNOTATION 'Example variable-length binary value'),
    char_value char NOT NULL OPTIONS(ANNOTATION 'Example single UTF-16 character'),
    boolean_value boolean NOT NULL OPTIONS(ANNOTATION 'Example boolean value'),
    byte_value byte NOT NULL OPTIONS(ANNOTATION 'Example signed 8-bit integer'),
    short_value short NOT NULL OPTIONS(ANNOTATION 'Example signed 16-bit integer'),
    integer_value integer NOT NULL OPTIONS(ANNOTATION 'Example signed 32-bit integer'),
    long_value long NOT NULL OPTIONS(ANNOTATION 'Example signed 64-bit integer'),
    biginteger_value biginteger NOT NULL OPTIONS(ANNOTATION 'Example arbitrary precision integer'),
    float_value float NOT NULL OPTIONS(ANNOTATION 'Example 32-bit floating-point number'),
    double_value double NOT NULL OPTIONS(ANNOTATION 'Example 64-bit floating-point number'),
    bigdecimal_value bigdecimal NOT NULL OPTIONS(ANNOTATION 'Example arbitrary precision decimal'),
    date_value date NOT NULL OPTIONS(ANNOTATION 'Example calendar date'),
    time_value time NOT NULL OPTIONS(ANNOTATION 'Example local time'),
    timestamp_value timestamp NOT NULL OPTIONS(ANNOTATION 'Example local timestamp with fractional seconds'),
    blob_value blob NOT NULL OPTIONS(ANNOTATION 'Example binary large object'),
    clob_value clob NOT NULL OPTIONS(ANNOTATION 'Example character large object'),
    geometry_value geometry NOT NULL OPTIONS(ANNOTATION 'Example geometry encoded as WKB'),
    geography_value geography NOT NULL OPTIONS(ANNOTATION 'Example geography encoded as WKB'),
    json_value json NOT NULL OPTIONS(ANNOTATION 'Example JSON document'),
    xml_value xml NOT NULL OPTIONS(ANNOTATION 'Deprecated XML type retained for protocol compatibility'),
    PRIMARY KEY(sample_id)
)
OPTIONS(
    updatable false,
    tags 'sample;reference;types',
    ANNOTATION 'Canonical values covering every logical type supported by the provider protocol'
);
