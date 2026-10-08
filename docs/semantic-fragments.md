# Semantic fragments

A provider may expose a source-local semantic fragment alongside its relational
metadata. The fragment gives names and meaning to the tables and columns owned
by that provider without defining cross-source composition or changing query
execution.

Semantic metadata is optional. A provider remains fully usable when it does not
offer a fragment.

## Contract behavior

`ProviderService.GetSemanticFragment` returns one exact, versioned
`kubling-semantic` document. The request is connection-agnostic: retrieving a
fragment does not open a logical connection and the artifact describes the
provider server rather than one query session.

An unset response fragment means that the provider does not offer semantic
metadata. Clients should also accept `UNIMPLEMENTED` from older or custom
providers.

The artifact contains:

- UTF-8 YAML or JSON document bytes;
- the corresponding canonical media type;
- a provider-defined document version;
- a SHA-256 digest of the exact bytes.

The Go SDK calculates the digest and rejects empty, invalid or oversized
artifacts. Documents are limited to 1 MiB. It deliberately treats the document
as opaque; semantic-schema and binding validation belong to the semantic
consumer and provider-specific tests.

## Choose the appropriate model

### Bundled fragment

Bundle a fragment when the provider owns a stable schema and can guarantee that
every declared relation and field exists. The artifact should be deterministic,
shipped with the provider binary and tested against its metadata implementation.

This is the preferred model for fixed schemas such as the Host Provider. A
small, guaranteed fragment may also be appropriate for a dynamic provider when
it covers only a stable core.

### Configured fragment

Use a configured fragment when table meaning depends on a particular API,
schema, keyspace or deployment. The provider should return the configured
document unchanged and require an explicit version and media type.

Configuration can establish bindings, but it must not cause the provider to
guess business relationships, identities or cardinalities from structural
metadata alone.

### Reference model

A broad generated model may be distributed as an opt-in reference without
being returned automatically by `GetSemanticFragment`. A reference model must
include:

- the provider configuration needed to produce its relations and fields;
- a pinned and reproducible source fixture;
- generator source and regeneration instructions;
- provenance describing the observed source version and generator revision;
- documented omissions and portability limits.

Do not describe a generated snapshot as universally complete. Optional APIs,
permissions, extensions and source versions can all change the visible schema.

## Binding rules

Provider fragments are source-local. Relation and field bindings use the exact
names returned by provider metadata:

```yaml
binding:
  relation: PROCESS
```

They must not embed a deployment-specific schema name such as
`production.PROCESS`. Kubling qualifies local bindings when the data source is
activated.

A bundled fragment may bind only guaranteed columns. Fields produced by an
optional expansion mode belong in a matching configured fragment or reference
profile. Deployment-specific aliases and fleet-wide normalization also belong
outside a provider-owned default.

Declare an executable relationship only when its join is exact. Relationships
derived from arrays, selectors, text normalization, incomplete identifiers or
other non-equivalent values should remain semantic-only until the execution
model can represent them correctly.

Fragments must not contain credentials, physical endpoint addresses or other
deployment secrets.

## Provider guidance

| Provider | Current behavior | Recommended model |
| --- | --- | --- |
| Kubernetes | Bundled workload-core fragment | Keep the guaranteed core bundled and publish broader generated cluster profiles as opt-in references. |
| Host | Bundled host-observability fragment | Regenerate it from the fixed provider metadata and keep only exact source-local joins executable. |
| OpenAPI | Optional configured fragment | Associate an explicit fragment with one API; generated output may provide bindings but must not invent domain relationships. |
| Redis | Optional configured fragment | Associate an explicit fragment with the configured Redis schema. |
| Cassandra | Optional configured fragment | Associate an explicit fragment with the configured data model or keyspace. |
| In-memory | Optional fixture fragment | Use fixture-specific fragments in examples and tests rather than a universal default. |

## Validation checklist

Before distributing a fragment:

1. Verify that every relation and field binding exists in provider metadata.
2. Verify that all entity identities use stable, source-local properties.
3. Verify relationship endpoints, cardinalities and executable joins.
4. Retrieve the artifact repeatedly and confirm deterministic bytes and digest.
5. Retrieve it without opening a logical connection or contacting a source
   unnecessarily.
6. Exercise the gRPC method and the no-fragment behavior.
7. Regenerate reference artifacts and fail validation when the committed output
   is stale.

Change the artifact version whenever its published document changes. Generated
artifacts should be edited through their generator rather than by hand.
