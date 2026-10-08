package openapi

import (
	"context"

	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/status"
)

// SemanticFragment returns the explicitly configured API model without
// contacting the API or opening a logical connection.
func (p *Provider) SemanticFragment(
	ctx context.Context,
) (*providersdk.SemanticFragment, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return providersdk.CloneSemanticFragment(p.config.SemanticFragment), nil
}

var _ providersdk.SemanticFragmentProvider = (*Provider)(nil)
