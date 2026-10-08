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

Run the SDK tests from this directory:

```sh
go test ./...
go test -race ./...
```
