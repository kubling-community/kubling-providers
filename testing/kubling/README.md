# Testing a provider with Kubling

This template starts a regular Kubling container and points one physical data
source at a provider already running over gRPC. The VDB contains no DDL:
Kubling imports the provider's structured metadata exactly as it would in a
real deployment.

The standard names are `ProviderTestVDB` for the VDB and `provider` for the
data source and imported schema. Keeping them stable allows test scripts to be
shared without knowing which provider is running.

## Run the template

Start the provider and its native fixture first. It should listen on port
`50051`, or expose its actual port through `KUBLING_GRPC_PROVIDER_PORT`.

In one terminal, generate the descriptor bundle and start Kubling:

```sh
cd testing/kubling
./run-kubling.sh
```

`KUBLING_IMAGE` must reference a Kubling build that includes the
`PROVIDER_GRPC` data source type. This can be a published release or a local
development image.

In another terminal, verify that Kubling started and imported at least one
provider table:

```sh
./smoke.sh
```

The smoke test uses the official Kubling Go SDK. This runtime enables and
exposes only Kubling's gRPC transport. The test checks metadata import only; it
deliberately avoids a generic `SELECT *` because some providers require
criteria or expose expensive catalogs.

Run an arbitrary query or mutation through the same Go client with:

```sh
cd ..
go run ./cmd/provider-test query -sql "SELECT * FROM provider.PROJECT"
go run ./cmd/provider-test exec -sql "DELETE FROM provider.TASK WHERE id = 'example'"
```

## Configuration

The scripts define their defaults directly and allow these overrides:

- `KUBLING_IMAGE`: Kubling runtime image;
- `KUBLING_CLI_IMAGE`: image used to generate the descriptor bundle;
- `KUBLING_GRPC_PROVIDER_HOST` and `KUBLING_GRPC_PROVIDER_PORT`: provider
  endpoint as seen by the Kubling container;
- `KUBLING_GRPC_PROVIDER_TIMEOUT_MILLIS`: provider RPC timeout;
- `KUBLING_TEST_GRPC_PORT`: host port exposing Kubling's gRPC transport;
- `KUBLING_TEST_CONTAINER_NAME`: local Kubling container name;
- `KUBLING_DOCKER_NETWORK`: optional existing Docker network for the Kubling
  container;
- `KUBLING_TEST_GRPC_ADDRESS`, `KUBLING_TEST_VDB`, `KUBLING_TEST_USER` and
  `KUBLING_TEST_PASSWORD`: Go client connection settings;
- `GO_BIN`: Go executable used by the convenience scripts.

By default, Kubling reaches a provider running on the host through
`host.docker.internal`. When the provider runs in another container, attach
both containers to the same network and set `KUBLING_GRPC_PROVIDER_HOST` to the
provider container name.

The defaults keep the two gRPC endpoints separate: the provider listens on
`50051`, while Kubling is exposed to the Go test client on `50061`.

The generated `provider-test-descriptor-bundle.zip` is a local artifact and is
not committed.

## Test profiles

Every provider should pass the metadata smoke test. Additional assertions
depend on its [testing profile](../README.md). A provider compatible with the
[canonical-schema profile](../profiles/canonical-schema/README.md) can run the
shared query and mutation checks after the smoke test.
