package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// SemanticFragmentFileConfig identifies one local semantic document. Providers
// may embed this type in their own configuration formats.
type SemanticFragmentFileConfig struct {
	FragmentFile string `json:"fragmentFile" yaml:"fragmentFile"`
	MediaType    string `json:"mediaType" yaml:"mediaType"`
	Version      string `json:"version" yaml:"version"`
}

// LoadSemanticFragmentFile loads and validates one local semantic fragment.
// Relative fragment paths are resolved from the provider configuration file.
// The document is kept opaque and its bytes are not normalized.
func LoadSemanticFragmentFile(
	providerConfigPath string,
	config SemanticFragmentFileConfig,
) (*SemanticFragment, error) {
	fragmentFile := strings.TrimSpace(config.FragmentFile)
	if fragmentFile == "" {
		return nil, fmt.Errorf("semantic fragment file is required")
	}
	if strings.Contains(fragmentFile, "://") {
		return nil, fmt.Errorf("semantic fragment URLs are not supported")
	}

	resolvedPath := fragmentFile
	if !filepath.IsAbs(resolvedPath) {
		resolvedPath = filepath.Join(filepath.Dir(providerConfigPath), resolvedPath)
	}
	file, err := os.Open(resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("open semantic fragment file %q: %w", fragmentFile, err)
	}
	defer file.Close()

	document, err := io.ReadAll(io.LimitReader(file, MaxSemanticFragmentDocumentSize+1))
	if err != nil {
		return nil, fmt.Errorf("read semantic fragment file %q: %w", fragmentFile, err)
	}
	fragment := &SemanticFragment{
		Document:  document,
		MediaType: config.MediaType,
		Version:   config.Version,
	}
	if err := ValidateSemanticFragment(fragment); err != nil {
		return nil, fmt.Errorf("validate semantic fragment file %q: %w", fragmentFile, err)
	}

	return fragment, nil
}

// CloneSemanticFragment returns an independent copy of fragment.
func CloneSemanticFragment(fragment *SemanticFragment) *SemanticFragment {
	if fragment == nil {
		return nil
	}
	return &SemanticFragment{
		Document:  bytes.Clone(fragment.Document),
		MediaType: fragment.MediaType,
		Version:   fragment.Version,
	}
}

// ValidateSemanticFragment validates the transport envelope without parsing
// the opaque YAML or JSON document.
func ValidateSemanticFragment(fragment *SemanticFragment) error {
	if fragment == nil {
		return fmt.Errorf("semantic fragment is required")
	}
	if len(fragment.Document) == 0 {
		return fmt.Errorf("empty semantic fragment document")
	}
	if len(fragment.Document) > MaxSemanticFragmentDocumentSize {
		return fmt.Errorf(
			"semantic fragment document is larger than %d bytes",
			MaxSemanticFragmentDocumentSize,
		)
	}
	if !utf8.Valid(fragment.Document) {
		return fmt.Errorf("semantic fragment document is not valid UTF-8")
	}
	if strings.TrimSpace(fragment.Version) == "" {
		return fmt.Errorf("empty semantic fragment version")
	}
	switch fragment.MediaType {
	case SemanticFragmentMediaTypeYAML, SemanticFragmentMediaTypeJSON:
		return nil
	default:
		return fmt.Errorf(
			"unsupported semantic fragment media type %q",
			fragment.MediaType,
		)
	}
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

	if err := ValidateSemanticFragment(fragment); err != nil {
		return nil, status.Errorf(codes.Internal, "provider returned invalid semantic fragment: %v", err)
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
