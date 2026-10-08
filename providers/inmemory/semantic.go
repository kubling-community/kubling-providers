package inmemory

import (
	"context"

	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/status"
)

// SemanticFragment returns the model associated with this in-memory fixture.
func (p *Provider) SemanticFragment(
	ctx context.Context,
) (*providersdk.SemanticFragment, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return providersdk.CloneSemanticFragment(p.semanticFragment), nil
}
