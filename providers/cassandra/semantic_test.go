package cassandra

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSemanticFragmentIsOptionalAndCopied(t *testing.T) {
	provider := &Provider{}
	fragment, err := provider.SemanticFragment(context.Background())
	if err != nil {
		t.Fatalf("SemanticFragment() error = %v", err)
	}
	if fragment != nil {
		t.Fatalf("SemanticFragment() = %#v, want nil", fragment)
	}

	document := []byte("{}\n")
	provider.config.SemanticFragment = &providersdk.SemanticFragment{
		Document:  document,
		MediaType: providersdk.SemanticFragmentMediaTypeJSON,
		Version:   "inventory-model",
	}
	first, err := provider.SemanticFragment(context.Background())
	if err != nil {
		t.Fatalf("SemanticFragment() configured error = %v", err)
	}
	first.Document[0] = '['
	second, err := provider.SemanticFragment(context.Background())
	if err != nil {
		t.Fatalf("SemanticFragment() second error = %v", err)
	}
	if !bytes.Equal(second.Document, document) {
		t.Fatalf("SemanticFragment() retained caller mutation: %q", second.Document)
	}
}

func TestSemanticFragmentPreservesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := (&Provider{}).SemanticFragment(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("SemanticFragment() code = %v, want Canceled", status.Code(err))
	}
}

func TestSemanticFragmentServerEnvelope(t *testing.T) {
	document := []byte("format: kubling-semantic\nkind: fragment\n")
	provider := &Provider{config: Config{SemanticFragment: &providersdk.SemanticFragment{
		Document:  document,
		MediaType: providersdk.SemanticFragmentMediaTypeYAML,
		Version:   "inventory-model",
	}}}

	response, err := providersdk.NewServer(provider).GetSemanticFragment(
		context.Background(),
		&providerv1.GetSemanticFragmentRequest{},
	)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	fragment := response.GetFragment()
	if fragment == nil {
		t.Fatal("GetSemanticFragment() fragment = nil")
	}
	if !bytes.Equal(fragment.GetDocument(), document) ||
		fragment.GetMediaType() != providersdk.SemanticFragmentMediaTypeYAML ||
		fragment.GetVersion() != "inventory-model" {
		t.Fatalf("GetSemanticFragment() fragment = %#v", fragment)
	}
	wantDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(document))
	if fragment.GetDigest() != wantDigest {
		t.Fatalf("GetSemanticFragment() digest = %q, want %q", fragment.GetDigest(), wantDigest)
	}
}
