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

Run the SDK tests from this directory:

```sh
go test ./...
go test -race ./...
```
