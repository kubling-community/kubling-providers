package inmemory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const fixtureSemanticVersion = "1.0.0"

type fixtureSemanticDocument struct {
	Metadata struct {
		Version string `json:"version"`
	} `json:"metadata"`
	Spec struct {
		Entities      []fixtureSemanticEntity       `json:"entities"`
		Relationships []fixtureSemanticRelationship `json:"relationships"`
	} `json:"spec"`
}

type fixtureSemanticEntity struct {
	ID      string `json:"id"`
	Binding struct {
		Relation string `json:"relation"`
	} `json:"binding"`
	Properties []struct {
		ID      string `json:"id"`
		Binding struct {
			Field string `json:"field"`
		} `json:"binding"`
	} `json:"properties"`
	Identities []struct {
		Properties []string `json:"properties"`
	} `json:"identities"`
}

type fixtureSemanticRelationship struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Joins []struct {
		Left  string `json:"left"`
		Right string `json:"right"`
	} `json:"joins"`
}

func TestProjectManagementSemanticFixtureMatchesProviderModel(t *testing.T) {
	document := readFixtureSemanticDocument(t)
	var fragment fixtureSemanticDocument
	if err := json.Unmarshal(document, &fragment); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if fragment.Metadata.Version != fixtureSemanticVersion {
		t.Fatalf("metadata version = %q, want %q", fragment.Metadata.Version, fixtureSemanticVersion)
	}

	definitions := make(map[string]map[string]struct{}, len(entityDefinitions))
	for _, definition := range entityDefinitions {
		fields := make(map[string]struct{}, len(definition.fields))
		for _, field := range definition.fields {
			fields[field.name] = struct{}{}
		}
		definitions[definition.name] = fields
	}
	properties := make(map[string]map[string]string, len(fragment.Spec.Entities))
	for _, entity := range fragment.Spec.Entities {
		fields, exists := definitions[entity.Binding.Relation]
		if !exists {
			t.Errorf("entity %q binds unknown relation %q", entity.ID, entity.Binding.Relation)
			continue
		}
		properties[entity.ID] = make(map[string]string, len(entity.Properties))
		for _, property := range entity.Properties {
			if _, exists := fields[property.Binding.Field]; !exists {
				t.Errorf("entity %q property %q binds unknown field %q", entity.ID, property.ID, property.Binding.Field)
			}
			properties[entity.ID][property.ID] = property.Binding.Field
		}
		for _, identity := range entity.Identities {
			for _, property := range identity.Properties {
				if _, exists := properties[entity.ID][property]; !exists {
					t.Errorf("entity %q identity uses unknown property %q", entity.ID, property)
				}
			}
		}
	}
	for _, relationship := range fragment.Spec.Relationships {
		if properties[relationship.From] == nil || properties[relationship.To] == nil {
			t.Errorf("relationship has unknown endpoints %q -> %q", relationship.From, relationship.To)
		}
		for _, join := range relationship.Joins {
			assertFixtureSemanticReference(t, properties, join.Left)
			assertFixtureSemanticReference(t, properties, join.Right)
		}
	}
}

func TestSemanticFragmentIsOptionalAndCopied(t *testing.T) {
	provider := New()
	fragment, err := provider.SemanticFragment(context.Background())
	if err != nil {
		t.Fatalf("SemanticFragment() error = %v", err)
	}
	if fragment != nil {
		t.Fatalf("SemanticFragment() = %#v, want nil", fragment)
	}

	document := readFixtureSemanticDocument(t)
	configured := &providersdk.SemanticFragment{
		Document:  document,
		MediaType: providersdk.SemanticFragmentMediaTypeJSON,
		Version:   fixtureSemanticVersion,
	}
	provider = New(WithSemanticFragment(configured))
	document[0] = '['
	first, err := provider.SemanticFragment(context.Background())
	if err != nil {
		t.Fatalf("SemanticFragment() configured error = %v", err)
	}
	first.Document[0] = '['
	second, err := provider.SemanticFragment(context.Background())
	if err != nil {
		t.Fatalf("SemanticFragment() second error = %v", err)
	}
	if second.Document[0] != '{' {
		t.Fatalf("SemanticFragment() retained caller mutation: %q", second.Document[:1])
	}
}

func TestSemanticFragmentThroughCachedGRPCProvider(t *testing.T) {
	document := readFixtureSemanticDocument(t)
	client := newTestClient(t, WithSemanticFragment(&providersdk.SemanticFragment{
		Document:  document,
		MediaType: providersdk.SemanticFragmentMediaTypeJSON,
		Version:   fixtureSemanticVersion,
	}))

	response, err := client.GetSemanticFragment(
		context.Background(),
		&providerv1.GetSemanticFragmentRequest{},
	)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	fragment := response.GetFragment()
	digest := sha256.Sum256(document)
	if fragment == nil || !bytes.Equal(fragment.GetDocument(), document) ||
		fragment.GetMediaType() != providersdk.SemanticFragmentMediaTypeJSON ||
		fragment.GetVersion() != fixtureSemanticVersion ||
		fragment.GetDigest() != fmt.Sprintf("sha256:%x", digest) {
		t.Fatalf("GetSemanticFragment() fragment = %v", fragment)
	}
}

func TestSemanticFragmentPreservesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := New().SemanticFragment(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("SemanticFragment() code = %v, want Canceled", status.Code(err))
	}
}

func readFixtureSemanticDocument(t *testing.T) []byte {
	t.Helper()
	document, err := os.ReadFile("examples/semantic/project-management.json")
	if err != nil {
		t.Fatalf("os.ReadFile(fixture) error = %v", err)
	}
	return document
}

func assertFixtureSemanticReference(
	t *testing.T,
	properties map[string]map[string]string,
	reference string,
) {
	t.Helper()
	entity, property, exists := strings.Cut(reference, ".")
	if !exists || properties[entity] == nil {
		t.Errorf("semantic reference %q has an unknown entity", reference)
		return
	}
	if _, exists := properties[entity][property]; !exists {
		t.Errorf("semantic reference %q has an unknown property", reference)
	}
}
