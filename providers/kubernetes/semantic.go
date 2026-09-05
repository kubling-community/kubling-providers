package kubernetes

import (
	"bytes"
	"context"
	_ "embed"

	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/status"
)

const kubernetesSemanticFragmentVersion = "1.0.0"

//go:embed semantic/kubernetes-v1.yaml
var kubernetesSemanticFragmentDocument []byte

// SemanticFragment returns the provider-owned Kubernetes workload semantics
// without acquiring a cluster client or opening a logical connection.
func (*Provider) SemanticFragment(ctx context.Context) (*providersdk.SemanticFragment, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &providersdk.SemanticFragment{
		Document:  bytes.Clone(kubernetesSemanticFragmentDocument),
		MediaType: providersdk.SemanticFragmentMediaTypeYAML,
		Version:   kubernetesSemanticFragmentVersion,
	}, nil
}

var _ providersdk.SemanticFragmentProvider = (*Provider)(nil)
