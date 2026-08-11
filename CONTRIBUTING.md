# Contributing to Kubling Providers

Thank you for helping expand the set of data sources that Kubling can reach.
Contributions may be written by hand, with a coding agent, or with a mixture of
both. The tool does not change the acceptance bar: the person submitting the
change is responsible for understanding it, reviewing it and validating its
behavior.

## Before you start

Providers keep source-specific connectivity, metadata discovery and execution
outside Kubling Engine. A provider should expose what its source can actually
do and advertise only the capabilities it implements. Do not emulate a feature
inside the provider merely to make its capability list look more complete.

Start by reading:

- the contract under [`proto/`](proto/);
- the Go server SDK under [`sdk-go/provider/`](sdk-go/provider/);
- the [provider testing profiles](testing/README.md);
- the existing provider whose data model is closest to your source.

Keep endpoints, credentials and source-specific routing inside the provider.
Never commit real credentials, generated kubeconfigs, private certificates or
customer schemas.

## Working with a coding agent

Tools such as OpenAI Codex, Claude Code and other coding agents can remove much
of the repetitive work involved in scaffolding a provider. They are most useful
when they receive the real contract and nearby implementations as context.
They should not be asked to invent protocol fields or source behavior from
memory.

A useful starting prompt is:

```text
Help me implement a Kubling provider for <data source>.

Before writing code, inspect the repository, Git status, proto contract,
sdk-go/provider, testing profiles, generated Go types and the closest
existing provider. Treat the current code and protocol as the source of truth.

First describe the source capabilities we can support honestly. Then work in
small reviewable blocks, adding tests as each block is completed. Preserve
existing lifecycle, cancellation, metadata and null-handling conventions.
```

Give the agent authoritative documentation for the source and driver whenever
possible. Review every generated dependency and every assumption about native
types, consistency, pagination, transactions and mutation semantics.

## Recommended implementation order

1. Define the Go module and strict external configuration.
2. Implement capabilities, health and metadata discovery.
3. Implement connection ownership and safe resource cleanup.
4. Add query translation and incremental result streaming.
5. Add mutations supported natively by the source.
6. Advertise transactions only when the source can provide them; otherwise
   implement the expected unsupported behavior.
7. Add focused tests, a local fixture and the applicable testing profiles.
8. Add the container image, release workflow and provider documentation.

Work in small blocks. A metadata-only provider that accurately describes its
source is a better foundation than a broad implementation with optimistic
capability declarations.

## Review checklist

Before opening a pull request, verify that:

- capability declarations match the operations actually implemented;
- metadata correctly marks namespaces, keys, nullability, defaults and
  mutability;
- query and mutation inputs are parameterized rather than concatenated;
- cancellation and connection closure release all source resources;
- source nulls remain null instead of becoming zero values or the string
  `"null"`;
- native and Kubling types round-trip without silent precision loss;
- external writes are considered before enabling the SDK query cache;
- unsupported operations return an appropriate gRPC status;
- configuration examples contain no real secrets or environment-specific
  paths.

Format and test every changed Go module from its own directory:

```sh
gofmt -w path/to/changed.go
go test ./...
go test -race ./...
```

When protobuf files change, regenerate the committed Go sources from the
repository root:

```sh
./generate.sh
cd sdk-go && go mod tidy
```

Before opening the pull request, use the [Kubling compatibility
template](testing/kubling/README.md) to verify that Kubling can import the
provider metadata without static DDL. Run any additional profile checks that
match the provider's data model.

Use focused conventional commits and explain source-specific limitations in
the provider README. A coding agent can accelerate the work, but the pull
request should be reviewable without knowing which tool produced it.
