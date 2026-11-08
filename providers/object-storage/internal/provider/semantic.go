package provider

import (
	"bytes"
	"context"
	_ "embed"

	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
)

const semanticFragmentVersion = "1.0.0"

//go:embed semantic/object-storage-v1.yaml
var semanticFragmentDocument []byte

// SemanticFragment returns the stable provider-neutral object model.
func (*Provider) SemanticFragment(ctx context.Context) (*providersdk.SemanticFragment, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	return &providersdk.SemanticFragment{
		Document:  bytes.Clone(semanticFragmentDocument),
		MediaType: providersdk.SemanticFragmentMediaTypeYAML,
		Version:   semanticFragmentVersion,
	}, nil
}

var _ providersdk.SemanticFragmentProvider = (*Provider)(nil)
