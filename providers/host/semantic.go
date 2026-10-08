package host

import (
	"bytes"
	"context"
	_ "embed"

	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/status"
)

const hostSemanticFragmentVersion = "1.0.0"

//go:generate go run ./internal/cmd/semanticgen -output semantic/host-v1.yaml

//go:embed semantic/host-v1.yaml
var hostSemanticFragmentDocument []byte

// SemanticFragment returns the stable host-observability model without
// accessing the registry, opening an agent session or starting a fleet scan.
func (*Provider) SemanticFragment(ctx context.Context) (*providersdk.SemanticFragment, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &providersdk.SemanticFragment{
		Document:  bytes.Clone(hostSemanticFragmentDocument),
		MediaType: providersdk.SemanticFragmentMediaTypeYAML,
		Version:   hostSemanticFragmentVersion,
	}, nil
}

var _ providersdk.SemanticFragmentProvider = (*Provider)(nil)
