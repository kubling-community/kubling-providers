-- This Kubling DDL and local/schema.cql describe equivalent logical tables.
-- Cassandra uses the native CQL schema and the provider discovers it as structured metadata.
-- This SQL file illustrates that discovered model in Kubling notation.
-- "kbl.namespace" preserves the provider namespace that structured metadata carries separately.
-- The value "sample" matches local/provider.yaml and must change if that namespace key changes.

CREATE FOREIGN TABLE PROJECT
(
    id string NOT NULL,
    name string,
    status string,
    active boolean,
    budget bigdecimal,
    started_on date,
    metadata json,
    PRIMARY KEY(id)
)
OPTIONS(
    updatable true,
    "kbl.namespace" 'sample'
);

CREATE FOREIGN TABLE TASK
(
    id string NOT NULL,
    project_id string,
    title string,
    description string,
    completed boolean,
    priority integer,
    estimate_hours float,
    due_at timestamp,
    PRIMARY KEY(id)
)
OPTIONS(
    updatable true,
    "kbl.namespace" 'sample'
);

CREATE FOREIGN TABLE AUDIT_EVENT
(
    id long NOT NULL,
    entity_type string,
    entity_id string,
    action string,
    sequence short,
    risk_score double,
    occurred_at timestamp,
    source_address blob,
    payload json,
    raw_payload blob,
    PRIMARY KEY(id)
)
OPTIONS(
    updatable true,
    "kbl.namespace" 'sample'
);

CREATE FOREIGN TABLE TYPE_SAMPLE
(
    sample_id string NOT NULL,
    string_value string,
    varbinary_value blob,
    char_value string,
    boolean_value boolean,
    byte_value byte,
    short_value short,
    integer_value integer,
    long_value long,
    biginteger_value biginteger,
    float_value float,
    double_value double,
    bigdecimal_value bigdecimal,
    date_value date,
    time_value time,
    timestamp_value timestamp,
    blob_value blob,
    clob_value string,
    geometry_value blob,
    geography_value blob,
    json_value json,
    xml_value string,
    PRIMARY KEY(sample_id)
)
OPTIONS(
    updatable true,
    "kbl.namespace" 'sample'
);
