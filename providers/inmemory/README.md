# In-memory provider

This provider is a runnable reference implementation of the Kubling Go
provider SDK. It exposes one in-memory project data universe and demonstrates
the complete query, streaming, health, and mutation lifecycle.

The executable wraps the implementation with the SDK's bounded in-process
query cache. Successful mutations invalidate `TASK` automatically; external
source changes would use the invalidation controller returned by `cache.Wrap`.

Every logical connection sees the same provider-owned catalog. The store starts
with the canonical project, task, audit and type-coverage records.

## Run

From this directory:

```sh
go run ./cmd/inmemory --listen :50051
```

The server enables gRPC reflection for local inspection with tools such as
`grpcurl`.

## Schema

The DDL returned by `GetSchema` is maintained in [`schema.sql`](schema.sql).
It defines four annotated tables:

- `PROJECT` groups work into realistic business objects.
- `TASK` is mutable and exercises the complete mutation lifecycle; the other
  tables are read-only.
- `AUDIT_EVENT` provides a related, read-only event stream.
- `TYPE_SAMPLE` returns canonical values for every logical protocol type.

This static DDL path intentionally remains supported alongside the structured
metadata path used by providers that discover schemas dynamically.

`TYPE_SAMPLE` covers every concrete `kubling.v1.ValueType`, including the
deprecated XML representation. `VALUE_TYPE_UNKNOWN` is metadata for an
unspecified type rather than a column type.

## Capabilities

- Queries with projections, filters, ordering, limit, offset, and streamed
  batches.
- `INSERT`, `UPDATE`, and `DELETE`, including generated insert values.
- Connection-agnostic provider health checks.
- No native local transactions.

Native transactions are deliberately reported as unsupported. Kubling can
therefore include this provider in distributed soft transactions without
mistaking the in-memory store for a native transactional participant.

## Purpose

The implementation favors clarity over storage sophistication. It is intended
to:

- Show the minimum lifecycle of an external Go provider.
- Exercise the public protobuf and SDK contracts end to end.
- Serve as a starting point for real providers.
- Reveal SDK ergonomics that can be improved without coupling provider logic to
  gRPC transport details.
