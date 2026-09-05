package provider

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serverTestSemanticFragmentProvider struct {
	*serverTestProvider

	semanticFragmentFunc func(context.Context) (*SemanticFragment, error)
}

func (p *serverTestSemanticFragmentProvider) SemanticFragment(
	ctx context.Context,
) (*SemanticFragment, error) {
	if p.semanticFragmentFunc == nil {
		panic("unexpected SemanticFragment call")
	}

	return p.semanticFragmentFunc(ctx)
}

func TestServerGetSemanticFragmentReturnsAbsentWhenUnsupported(t *testing.T) {
	openCalls := 0
	server := NewServer(&serverTestProvider{
		openFunc: func(context.Context) (Connection, error) {
			openCalls++
			return nil, nil
		},
	})

	response, err := server.GetSemanticFragment(
		context.Background(),
		&providerv1.GetSemanticFragmentRequest{},
	)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	if response == nil || response.GetFragment() != nil {
		t.Fatalf("GetSemanticFragment() response = %v, want absent fragment", response)
	}
	if openCalls != 0 {
		t.Fatalf("Open() calls = %d, want 0", openCalls)
	}
}

func TestServerGetSemanticFragmentReturnsAbsentForNilFragment(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var receivedContext context.Context
	server := NewServer(&serverTestSemanticFragmentProvider{
		serverTestProvider: &serverTestProvider{},
		semanticFragmentFunc: func(ctx context.Context) (*SemanticFragment, error) {
			receivedContext = ctx
			return nil, nil
		},
	})

	response, err := server.GetSemanticFragment(
		ctx,
		&providerv1.GetSemanticFragmentRequest{},
	)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	if response == nil || response.GetFragment() != nil {
		t.Fatalf("GetSemanticFragment() response = %v, want absent fragment", response)
	}
	if receivedContext != ctx {
		t.Fatal("SemanticFragment() did not receive the request context")
	}
}

func TestServerGetSemanticFragmentPreservesValidArtifacts(t *testing.T) {
	tests := []struct {
		name           string
		document       []byte
		mediaType      string
		version        string
		expectedDigest string
	}{
		{
			name:           "yaml",
			document:       []byte("schemaVersion: 1\nnamespace: k8s\n"),
			mediaType:      SemanticFragmentMediaTypeYAML,
			version:        "1.4.0",
			expectedDigest: "sha256:ff206a0f645aa169ef12f94a52a113558e19d25637cd9b34a566c50892dd3812",
		},
		{
			name:           "json",
			document:       []byte("{\"schemaVersion\":1,\"namespace\":\"api\"}\n"),
			mediaType:      SemanticFragmentMediaTypeJSON,
			version:        "2026.09.04",
			expectedDigest: "sha256:998b76b897ddd8eab4a6ffeb150e3e23e64366aa94e2347e332f91b65013866b",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := bytes.Clone(test.document)
			server := NewServer(&serverTestSemanticFragmentProvider{
				serverTestProvider: &serverTestProvider{},
				semanticFragmentFunc: func(context.Context) (*SemanticFragment, error) {
					return &SemanticFragment{
						Document:  document,
						MediaType: test.mediaType,
						Version:   test.version,
					}, nil
				},
			})

			response, err := server.GetSemanticFragment(
				context.Background(),
				&providerv1.GetSemanticFragmentRequest{},
			)
			if err != nil {
				t.Fatalf("GetSemanticFragment() error = %v", err)
			}
			artifact := response.GetFragment()
			if artifact == nil {
				t.Fatal("GetSemanticFragment() fragment = nil")
			}
			if !bytes.Equal(artifact.GetDocument(), test.document) {
				t.Fatalf("document = %q, want %q", artifact.GetDocument(), test.document)
			}
			if artifact.GetMediaType() != test.mediaType {
				t.Fatalf("media type = %q, want %q", artifact.GetMediaType(), test.mediaType)
			}
			if artifact.GetVersion() != test.version {
				t.Fatalf("version = %q, want %q", artifact.GetVersion(), test.version)
			}
			if artifact.GetDigest() != test.expectedDigest {
				t.Fatalf("digest = %q, want %q", artifact.GetDigest(), test.expectedDigest)
			}

			document[0] ^= 0xff
			if !bytes.Equal(artifact.GetDocument(), test.document) {
				t.Fatal("response document aliases provider-owned bytes")
			}
		})
	}
}

func TestServerGetSemanticFragmentPropagatesProviderError(t *testing.T) {
	expectedErr := errors.New("semantic fragment failed")
	server := NewServer(&serverTestSemanticFragmentProvider{
		serverTestProvider: &serverTestProvider{},
		semanticFragmentFunc: func(context.Context) (*SemanticFragment, error) {
			return nil, expectedErr
		},
	})

	response, err := server.GetSemanticFragment(
		context.Background(),
		&providerv1.GetSemanticFragmentRequest{},
	)
	if response != nil {
		t.Fatalf("GetSemanticFragment() response = %v, want nil", response)
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("GetSemanticFragment() error = %v, want %v", err, expectedErr)
	}
}

func TestServerGetSemanticFragmentAcceptsMaximumDocumentSize(t *testing.T) {
	document := bytes.Repeat([]byte("a"), MaxSemanticFragmentDocumentSize)
	server := NewServer(&serverTestSemanticFragmentProvider{
		serverTestProvider: &serverTestProvider{},
		semanticFragmentFunc: func(context.Context) (*SemanticFragment, error) {
			return &SemanticFragment{
				Document:  document,
				MediaType: SemanticFragmentMediaTypeYAML,
				Version:   "1.0.0",
			}, nil
		},
	})

	response, err := server.GetSemanticFragment(
		context.Background(),
		&providerv1.GetSemanticFragmentRequest{},
	)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	if got := len(response.GetFragment().GetDocument()); got != MaxSemanticFragmentDocumentSize {
		t.Fatalf("document size = %d, want %d", got, MaxSemanticFragmentDocumentSize)
	}
}

func TestServerGetSemanticFragmentDoesNotParseDocument(t *testing.T) {
	server := NewServer(&serverTestSemanticFragmentProvider{
		serverTestProvider: &serverTestProvider{},
		semanticFragmentFunc: func(context.Context) (*SemanticFragment, error) {
			return &SemanticFragment{
				Document:  []byte("{not-json"),
				MediaType: SemanticFragmentMediaTypeJSON,
				Version:   "1.0.0",
			}, nil
		},
	})

	response, err := server.GetSemanticFragment(
		context.Background(),
		&providerv1.GetSemanticFragmentRequest{},
	)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	if got := string(response.GetFragment().GetDocument()); got != "{not-json" {
		t.Fatalf("document = %q, want exact unparsed bytes", got)
	}
}

func TestServerGetSemanticFragmentRejectsInvalidArtifacts(t *testing.T) {
	tests := []struct {
		name        string
		fragment    *SemanticFragment
		wantMessage string
	}{
		{
			name: "empty document",
			fragment: &SemanticFragment{
				MediaType: SemanticFragmentMediaTypeYAML,
				Version:   "1.0.0",
			},
			wantMessage: "empty semantic fragment document",
		},
		{
			name: "oversized document",
			fragment: &SemanticFragment{
				Document:  bytes.Repeat([]byte("a"), MaxSemanticFragmentDocumentSize+1),
				MediaType: SemanticFragmentMediaTypeYAML,
				Version:   "1.0.0",
			},
			wantMessage: "larger than 1048576 bytes",
		},
		{
			name: "invalid UTF-8 document",
			fragment: &SemanticFragment{
				Document:  []byte{0xff},
				MediaType: SemanticFragmentMediaTypeYAML,
				Version:   "1.0.0",
			},
			wantMessage: "not valid UTF-8",
		},
		{
			name: "empty version",
			fragment: &SemanticFragment{
				Document:  []byte("schemaVersion: 1\n"),
				MediaType: SemanticFragmentMediaTypeYAML,
				Version:   " \t",
			},
			wantMessage: "empty semantic fragment version",
		},
		{
			name: "unsupported media type",
			fragment: &SemanticFragment{
				Document:  []byte("schemaVersion: 1\n"),
				MediaType: "text/yaml",
				Version:   "1.0.0",
			},
			wantMessage: `unsupported semantic fragment media type "text/yaml"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer(&serverTestSemanticFragmentProvider{
				serverTestProvider: &serverTestProvider{},
				semanticFragmentFunc: func(context.Context) (*SemanticFragment, error) {
					return test.fragment, nil
				},
			})

			response, err := server.GetSemanticFragment(
				context.Background(),
				&providerv1.GetSemanticFragmentRequest{},
			)
			if response != nil {
				t.Fatalf("GetSemanticFragment() response = %v, want nil", response)
			}
			if got := status.Code(err); got != codes.Internal {
				t.Fatalf("GetSemanticFragment() status = %s, want %s", got, codes.Internal)
			}
			if !strings.Contains(status.Convert(err).Message(), test.wantMessage) {
				t.Fatalf("GetSemanticFragment() error = %q, want %q", err, test.wantMessage)
			}
		})
	}
}
