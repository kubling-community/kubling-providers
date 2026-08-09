# Kubling Providers

Protocol, Go SDK and official external providers for
[Kubling](https://docs.kubling.com/).

Kubling is a data federation engine and distributed planner. Providers keep
source-specific connectivity, metadata discovery and execution outside the
engine, exposing them through a language-neutral gRPC contract. The Go SDK
hides transport and connection lifecycle details so provider authors can work
with regular Go interfaces.

## Repository layout

- `proto/` — provider gRPC contract, published through Buf.
- `sdk-go/` — generated Go contract and provider server SDK.
- `providers/` — independent provider implementations, examples and shared
  conformance guidance. Each provider owns its configuration, documentation,
  source-specific behavior and release lifecycle.

Each Go SDK or provider directory is an independent Go module. Run validation
from the module being changed:

```sh
cd sdk-go
go test ./...
```

Regenerate the Go contract after changing protobuf files:

```sh
./generate.sh
cd sdk-go && go mod tidy
```

Generated Go protobuf sources are committed so tagged SDK versions can be
consumed directly by Go tooling.

## Releases

- Protocol definitions are published to `buf.build/kubling/kubling-providers`.
- Go SDK releases use tags such as `sdk-go/v0.1.0`.
- Official provider releases use tags such as
  `providers/cassandra/v0.1.0`.

Provider-specific configuration and local environments are documented in each
provider directory.

## License

Apache License 2.0.
