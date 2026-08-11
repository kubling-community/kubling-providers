# Provider testing

Provider tests have two independent concerns: whether the gRPC implementation
obeys the provider contract, and whether Kubling can import and execute against
that provider as a data source. Both should be covered, but not every provider
should be forced into the same data model.

Test profiles are additive labels, not a closed list of provider types. A new
profile should describe reusable behavior or fixture shape rather than name the
providers that currently implement it.

## Baseline profile

The baseline applies to every provider:

1. Health and capability discovery report the provider's real behavior.
2. Metadata discovery returns a usable catalog without opening a logical
   connection.
3. Logical connections can be opened, acquired, released and closed safely.
4. Advertised query and mutation operations work through the gRPC contract.
5. Unsupported operations return the expected gRPC status.
6. Cancellation and shutdown release source resources.

Provider module tests should verify these behaviors directly. The
[Kubling compatibility template](kubling/README.md) adds an engine-level smoke
test by importing the same metadata through a VDB with no DDL.

## Canonical-schema profile

The [canonical-schema profile](profiles/canonical-schema/README.md) applies
when a source can be prepared with a stable tabular model, keys and seed rows
without distorting how the provider normally represents that source. The
native storage model may be relational, key-value, document, wide-column or
something else.

This profile enables shared SQL assertions and CRUD checks. It is optional and
must never be treated as a requirement for provider compatibility.

## Source-shaped profile

A source-shaped provider preserves a catalog dictated by the source itself.
Its value would be lost if it were forced to expose the canonical schema.
These providers still use the baseline and Kubling metadata smoke tests, then
add fixtures and assertions that match their natural resource model.

Profiles do not need to be mutually exclusive. A provider may support a
canonical fixture for shared testing while also having source-shaped tests for
native metadata, types or operations.
