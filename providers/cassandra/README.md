# Cassandra provider

External Kubling provider for Apache Cassandra. The provider aggregates every
configured keyspace into one data universe. Endpoints, credentials and routing
remain inside the provider process and are never sent through the gRPC
protocol.

## Current implementation

The provider currently includes:

- strict external YAML configuration;
- multiple logical namespaces backed by Cassandra keyspaces;
- authentication, TLS, consistency and timeout settings;
- shared Cassandra driver sessions with coordinated creation and safe release;
- dynamic provider-neutral metadata discovery across every configured
  namespace;
- parameterized query translation with field projections, filters, ordering,
  limits and incremental result batches;
- parameterized insert, update and delete operations;
- Cassandra-to-Kubling value conversion for native scalar and structured
  types;
- an executable gRPC server;
- explicit rejection of native transaction operations.

The provider advertises the exact subset it can push down. Comparisons support
`=`, `<`, `<=`, `>` and `>=`; logical composition supports `AND`; limits and
ordering are supported, while offsets and explicit null ordering are not.
Cassandra may still reject a query when its table-specific partition and
clustering-key rules do not permit the requested filter or ordering. Updates
and deletes require a filter, primary-key columns cannot be updated, and
generated or returning insert values are not supported. Cassandra does not
report affected row counts for update or delete operations.

Schema discovery returns structured metadata rather than generated DDL. It
includes deterministic table and column ordering, logical and native types,
primary keys, partition and clustering key information, index searchability
and Cassandra-specific extension properties. Kubling requests the complete
catalog and may filter imported metadata locally; it never selects a Cassandra
cluster or keyspace through the provider protocol.

[`schema.example.sql`](schema.example.sql) shows the logical Kubling DDL
equivalent of the native [`local/schema.cql`](local/schema.cql) fixture. It is
documentation only; Cassandra remains the schema source of truth. Every table
includes `"kbl.namespace" 'sample'` because a static DDL must preserve the
opaque namespace that `TableMetadata.namespace` carries in the structured
metadata path. Kubling uses that property when routing operations back to the
provider. The example value `sample` matches the namespace key in
`local/provider.yaml`.

Native Cassandra transactions are not advertised. Kubling may provide its soft
transaction behavior above this provider, but Cassandra never contains a real
pending transaction managed by this provider.

The SDK query cache is deliberately disabled. Writes performed by clients
outside this provider cannot be observed reliably, so automatic invalidation
would not be safe.

## Run

Start from this module using an external configuration file:

```sh
go run ./cmd/cassandra -config ./config.example.yaml -listen :50051
```

The configuration path may also be provided through
`KUBLING_CASSANDRA_CONFIG`.

## Container image

Official releases are published to Docker Hub as
`docker.io/kubling/cassandra-provider`. The image runs as a non-root user,
listens on port `50051` and reads `/etc/kubling/provider.yaml` by default:

```sh
docker run --rm \
  --publish 50051:50051 \
  --volume "$PWD/config.example.yaml:/etc/kubling/provider.yaml:ro" \
  docker.io/kubling/cassandra-provider:0.1.0
```

The mounted configuration must use Cassandra hostnames reachable from inside
the container. Override the configuration path with
`KUBLING_CASSANDRA_CONFIG` or append `-config` and `-listen` arguments after
the image name.

Release tags use `providers/cassandra/vMAJOR.MINOR.PATCH`. Stable releases
publish the exact version, the moving `MAJOR.MINOR` tag and `latest` for
`linux/amd64` and `linux/arm64`. Prereleases publish only their exact version;
every release also publishes an immutable `sha-*` tag.

## Local environment

The local fixture starts a single Cassandra node, creates and seeds the same
project-management domain used by the in-memory provider, and runs this
provider on `:50052`:

```sh
./local/run.sh
```

`GetSchema` reads Cassandra's native schema and returns structured metadata for
the logical namespace `sample`; the provider does not define or return Kubling
DDL. The name comes from the provider-owned local configuration and is not a
connection parameter sent by Kubling.

The script keeps Cassandra running when the provider stops, which makes local
iteration faster. Available lifecycle commands are:

```sh
./local/run.sh status
./local/run.sh down
./local/run.sh reset
```

`reset` removes the Cassandra volume, recreates the fixture and starts the
provider. Set `KUBLING_CASSANDRA_PROVIDER_LISTEN` to change the gRPC address or
`CASSANDRA_IMAGE` to test another Cassandra image.

The fixture follows the shared guidance in `../conformance/README.md`. Native
Cassandra types cannot represent every Kubling logical type exactly, so common
tests should validate advertised capabilities and logical behavior separately
from source-specific type mappings.

With the local Cassandra fixture running, execute the real gRPC integration
test with:

```sh
KUBLING_CASSANDRA_INTEGRATION=1 go test -run TestCassandraIntegrationLifecycleAndOperations -v .
```
