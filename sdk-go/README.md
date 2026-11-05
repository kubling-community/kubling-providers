# Kubling provider Go SDK

Generated Go messages and a server-side runtime for implementing Kubling
providers. The SDK owns transport lifecycle, logical connections, capability
dispatch and optional query caching so provider implementations can focus on
source metadata and operations.

Import the public runtime from:

```go
import providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
```

Generated protocol types are under `kubling/provider/v1`. Provider modules
should depend on a released SDK version and must not copy generated sources.

## Semantic fragments

Implement `providersdk.SemanticFragmentProvider` when a provider owns or
accepts a source-local semantic document. Returning `nil`, or not implementing
the interface, reports that no fragment is available. The SDK validates the
artifact envelope and calculates its digest without interpreting the document.

```go
func (*Provider) SemanticFragment(context.Context) (*providersdk.SemanticFragment, error) {
    return &providersdk.SemanticFragment{
        Document:  semanticDocument,
        MediaType: providersdk.SemanticFragmentMediaTypeYAML,
        Version:   semanticDocumentVersion,
    }, nil
}
```

Configuration-driven providers can use `LoadSemanticFragmentFile`. It resolves
a local fragment path relative to the provider configuration, enforces the SDK
size limit and preserves the document bytes exactly. Remote fragment URLs are
not loaded.

See the [semantic fragment guide](../docs/semantic-fragments.md) before choosing
between a bundled, configured or reference model.

## Durable transaction recovery

`TransactionCapabilities.supported` describes a native transaction while its
logical connection remains alive. A provider should advertise `recovery` only
when prepared branches survive connection loss and provider restart. Such a
provider implements `TransactionBranchConnection` and
`TransactionRecoveryProvider`, using a stable resource-manager identifier that
does not contain credentials or an endpoint.

Provider integration tests can call
`providertest.RunDurableTransactionRecovery`. The fixture starts three provider
instances against the same backend and verifies retry-safe prepare, recovery,
idempotent resolution, retained terminal status and explicit cleanup.

Run the SDK tests from this directory:

```sh
go test ./...
go test -race ./...
```
