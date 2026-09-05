package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"unicode/utf8"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// SemanticFragmentMediaTypeYAML is the canonical media type for YAML
	// kubling-semantic documents.
	SemanticFragmentMediaTypeYAML = "application/yaml"

	// SemanticFragmentMediaTypeJSON is the canonical media type for JSON
	// kubling-semantic documents.
	SemanticFragmentMediaTypeJSON = "application/json"

	// MaxSemanticFragmentDocumentSize is the largest semantic document accepted
	// from a provider implementation.
	MaxSemanticFragmentDocumentSize = 1 << 20
)

// SemanticFragment is an exact, versioned kubling-semantic document supplied
// by a provider implementation. The SDK validates and envelopes the document.
type SemanticFragment struct {
	Document  []byte
	MediaType string
	Version   string
}

// SemanticFragmentProvider may be implemented by providers that distribute a
// source-local semantic fragment with their implementation.
//
// Returning nil means that the provider does not currently offer a fragment.
type SemanticFragmentProvider interface {
	SemanticFragment(context.Context) (*SemanticFragment, error)
}

// GetSemanticFragment returns the optional source-local semantic artifact
// exposed by the provider without opening a logical connection.
func (s *Server) GetSemanticFragment(
	ctx context.Context,
	_ *providerv1.GetSemanticFragmentRequest,
) (*providerv1.GetSemanticFragmentResponse, error) {
	semanticProvider, ok := s.implementation.(SemanticFragmentProvider)
	if !ok {
		return &providerv1.GetSemanticFragmentResponse{}, nil
	}

	fragment, err := semanticProvider.SemanticFragment(ctx)
	if err != nil {
		return nil, err
	}
	if fragment == nil {
		return &providerv1.GetSemanticFragmentResponse{}, nil
	}

	if err := validateSemanticFragment(fragment); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	document := bytes.Clone(fragment.Document)
	digest := sha256.Sum256(document)
	return &providerv1.GetSemanticFragmentResponse{
		Fragment: &providerv1.SemanticFragmentArtifact{
			Document:  document,
			MediaType: fragment.MediaType,
			Version:   fragment.Version,
			Digest:    fmt.Sprintf("sha256:%x", digest),
		},
	}, nil
}

func validateSemanticFragment(fragment *SemanticFragment) error {
	if len(fragment.Document) == 0 {
		return fmt.Errorf("provider returned an empty semantic fragment document")
	}
	if len(fragment.Document) > MaxSemanticFragmentDocumentSize {
		return fmt.Errorf(
			"provider returned a semantic fragment document larger than %d bytes",
			MaxSemanticFragmentDocumentSize,
		)
	}
	if !utf8.Valid(fragment.Document) {
		return fmt.Errorf("provider returned a semantic fragment document that is not valid UTF-8")
	}
	if strings.TrimSpace(fragment.Version) == "" {
		return fmt.Errorf("provider returned an empty semantic fragment version")
	}
	switch fragment.MediaType {
	case SemanticFragmentMediaTypeYAML, SemanticFragmentMediaTypeJSON:
		return nil
	default:
		return fmt.Errorf(
			"provider returned unsupported semantic fragment media type %q",
			fragment.MediaType,
		)
	}
}
