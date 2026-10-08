package kubernetes

import (
	"context"
	"os"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const k3sReferenceFragmentPath = "examples/semantic/k3s-reference/fragment.yaml"

func TestKubernetesIntegrationK3sReferenceFragmentMatchesMetadata(t *testing.T) {
	if os.Getenv("KUBLING_KUBERNETES_INTEGRATION") == "" {
		t.Skip("set KUBLING_KUBERNETES_INTEGRATION=1 with the pinned local k3s fixture running")
	}

	config, err := LoadConfig("examples/semantic/k3s-reference/provider.yaml")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	implementation, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	metadata, err := implementation.Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}

	documentBytes, err := os.ReadFile(k3sReferenceFragmentPath)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v", k3sReferenceFragmentPath, err)
	}
	var document semanticTestDocument
	if err := yaml.Unmarshal(documentBytes, &document); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}

	for _, entity := range document.Spec.Entities {
		table := integrationMetadataTable(metadata, entity.Binding.Relation, config.Namespace)
		if table == nil {
			t.Errorf(
				"entity %s relation %q was not discovered in namespace %q",
				entity.ID,
				entity.Binding.Relation,
				config.Namespace,
			)
			continue
		}
		for _, property := range entity.Properties {
			if !metadataColumnExists(table, property.Binding.Field) {
				t.Errorf(
					"entity %s property %s binding %q is absent from relation %q",
					entity.ID,
					property.ID,
					property.Binding.Field,
					entity.Binding.Relation,
				)
			}
		}
	}
}
