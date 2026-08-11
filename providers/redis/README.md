# Redis provider

External Kubling provider for Redis. It exposes configured Redis databases as
one logical data universe while keeping endpoints, credentials, key conventions
and native commands inside the provider process.

## Current implementation

The first complete vertical maps logical tables to Redis Hashes. Each table has
an external YAML definition with an opaque namespace, key prefix, key column,
typed hash fields, annotations and update policy. `GetSchema` returns structured
provider metadata; the provider does not generate Kubling DDL.

Exact key filters use direct lookup. Key patterns, non-key filters and
unfiltered table reads use bounded `SCAN` restricted to the table's key prefix;
the provider then evaluates the complete filter exactly before projection,
ordering and pagination. This avoids the blocking `KEYS` command, isolates
tables that share one Redis database and caps work through `maxScannedKeys`.
Non-hash keys matching the same prefix are ignored.

Insert, update and delete are supported for tables marked updatable. Inserts use
a Lua check-and-create operation so an existing key is never overwritten.
Updates and deletes require a filter, but that filter may target key or hash
fields. Non-key filters use the same prefix-restricted bounded scan. Redis has
no transaction surface in this provider; Kubling may apply soft transaction
behavior above it.

The SDK query cache is intentionally not enabled. Mutations made outside the
provider cannot be observed reliably, so correct invalidation cannot be
guaranteed.

## Run

```sh
go run ./cmd/redis -config ./config.example.yaml -listen :50051
```

The configuration path may also be supplied through `KUBLING_REDIS_CONFIG`.

## Schema examples

[`schema.example.yaml`](schema.example.yaml) demonstrates four annotated
tables and every concrete Kubling value type. This is the schema consumed by
the Redis provider.

[`schema.example.sql`](schema.example.sql) expresses the same logical model as
Kubling DDL. It is an illustrative equivalent and is not loaded by the Redis
provider. Every table includes `"kbl.namespace" 'sample'` because a static DDL
must preserve the opaque namespace that `TableMetadata.namespace` carries in
the structured metadata path. Kubling uses that property when sending the
entity reference back to the provider. The example value `sample` matches the
namespace key in `config.example.yaml`.

## Local environment

The local fixture starts Redis, seeds the same project-management domain used
by the other sample providers, and runs the gRPC provider on `:50053`:

```sh
./local/run.sh
```

Lifecycle commands are `redis`, `provider`, `status`, `down`, and `reset`.
Set `KUBLING_REDIS_PROVIDER_LISTEN` or `REDIS_IMAGE` to override their defaults.

With only Redis and its seed running, execute the real integration test with:

```sh
./local/run.sh redis
KUBLING_REDIS_INTEGRATION=1 go test -run TestRedisIntegrationLifecycleAndOperations -v .
```

The integration test and local provider use `local/provider.yaml`, which points
to the shared `schema.example.yaml` fixture.
