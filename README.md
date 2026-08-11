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
- `providers/` — independent provider implementations and examples. Each
  provider owns its configuration, documentation, source-specific behavior and
  release lifecycle.
- `testing/` — shared testing profiles and a dynamic Kubling compatibility
  template.

The Go SDK and each provider implementation are independent Go modules. Run
validation from the module being changed:

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

## Contributing

Contributions written by hand or with the help of a coding agent are welcome.
See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the development workflow, review
expectations and a practical guide to starting a new provider.

Provider authors can use the [testing guide](testing/README.md) to validate the
gRPC implementation directly and through a real Kubling VDB without writing
DDL.

## Releases

- Protocol definitions are published to `buf.build/kubling/kubling-providers`.
- Go SDK releases use tags such as `sdk-go/v0.1.0`.
- Official provider releases use tags such as `providers/kubernetes/v0.1.0`.

A provider tag validates its Go module, builds `linux/amd64` and `linux/arm64`
images, pushes `docker.io/kubling/<provider>-provider`, and creates a GitHub
Release. Stable versions publish `MAJOR.MINOR.PATCH`, `MAJOR.MINOR` and
`latest`; prereleases publish only their exact version. Configure the
repository secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` before creating
the first provider tag.

Provider-specific configuration and local environments are documented in each
provider directory.

## License

Apache License 2.0.
