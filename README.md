# Kubling Providers

The gRPC contract, generated bindings and external data-source providers for
[Kubling](https://docs.kubling.com/).

Providers keep source-specific connectivity and execution outside the engine.
The Go SDK supplies the server runtime used by the official providers.

## Repository layout

- `proto/` — provider gRPC contract, published through Buf.
- `sdk-go/` — generated Go contract and provider server SDK.
- `sdk-java/` — generated Java messages and gRPC stubs.
- `sdk-python/` — generated Python messages and gRPC stubs.
- `providers/` — provider implementations and examples.
- `testing/` — shared compatibility tooling.

## Development

```sh
./generate.sh
cd sdk-go && go mod tidy
go test ./...
```

Use `./generate.sh go`, `java` or `python` to generate only one language.
Go sources are versioned; Java and Python sources are generated while building
their packages.

## Releases

The protocol and generated bindings are released together. Provider runtimes
keep independent lifecycles. See [`docs/releases.md`](docs/releases.md).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md), [`testing/README.md`](testing/README.md)
and [`SECURITY.md`](SECURITY.md).

## License

Apache License 2.0.
