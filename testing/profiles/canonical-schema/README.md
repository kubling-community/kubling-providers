# Canonical-schema test profile

This optional profile defines a small project-management model that can be
provisioned in different source technologies. It exists to reuse SQL and CRUD
tests across providers without assuming anything about their native storage
model.

The fixture contains:

- `PROJECT`: two project records;
- `TASK`: three task records and the entity used by mutation tests;
- `AUDIT_EVENT`: two audit records;
- `TYPE_SAMPLE`: one canonical row covering the types represented by the
  provider fixture.

The expected identifiers are:

- `project-1` and `project-2`;
- `task-1`, `task-2` and `task-3`;
- audit events `1001` and `1002`;
- type sample `canonical`.

The source fixture owns its schema and seed mechanism. It may use native DDL, a
provider-owned schema file or static Kubling DDL. What matters is that Kubling
imports equivalent logical table and column names.

When equivalent Kubling DDL is provided for documentation or manual schema
configuration, every table belonging to a non-empty provider namespace must
include `"kbl.namespace" '<namespace>'` in its table `OPTIONS`. Structured
metadata transports the same value through `TableMetadata.namespace`.

## Shared checks

Start the provider fixture and the [Kubling compatibility
template](../../kubling/README.md), then run:

```sh
./testing/profiles/canonical-schema/test.sh
```

The Go test client verifies the canonical records through Kubling's gRPC
transport and exercises an insert, update and delete against `TASK`. Set
`KUBLING_TEST_MUTATIONS=false` when the provider does not advertise all three
mutation operations.

Native sources do not need to represent every Kubling type identically. Tests
should separate common logical behavior from source-specific type mappings and
only assert lossless mappings advertised by the provider.
