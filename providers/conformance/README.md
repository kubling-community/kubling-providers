# Provider conformance fixture

Reference providers should expose the same small project-management domain
whenever their native data model can represent it:

- `PROJECT`: two projects;
- `TASK`: three tasks, and the mutable entity used by CRUD tests;
- `AUDIT_EVENT`: two read-only audit records;
- `TYPE_SAMPLE`: one row covering the provider's supported native types.

The in-memory provider defines this model through Kubling DDL. The Cassandra
local environment creates equivalent native tables and lets the provider
discover them as structured metadata. Source fixtures do not need to use the
same schema-definition mechanism.

When an equivalent Kubling DDL is provided for documentation or manual schema
configuration, every table belonging to a non-empty provider namespace must
include `"kbl.namespace" '<namespace>'` in its table `OPTIONS`. Structured
metadata transports this value through `TableMetadata.namespace`; the DDL
property preserves the same routing information when no metadata processor is
involved.

A shared Kubling conformance suite should be capability-driven:

1. Always verify health, capabilities, schema discovery and connection
   lifecycle.
2. Run query, expression, pagination and ordering cases only when advertised.
3. Run each mutation case only when its capability is advertised.
4. Assert common logical entities and values separately from native type and
   source-specific metadata assertions.
5. Verify that unsupported operations return the expected gRPC status.

This keeps one behavioral test suite reusable without requiring every data
source to emulate features or types it does not natively support.
