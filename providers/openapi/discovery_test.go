package openapi

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestProviderDiscoversUnambiguousGETEntity(t *testing.T) {
	provider := newDiscoveryTestProvider(t, testOpenAPISpec, DiscoveryConfig{Enabled: true}, nil)
	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if len(metadata.Tables) != 1 {
		t.Fatalf("discovered tables = %d, want 1", len(metadata.Tables))
	}
	table := metadata.Tables[0]
	if table.Name != "INVOICE" || table.SourceName != "listInvoices" || table.Properties["openapi.response_path"] != "/data/items" {
		t.Fatalf("discovered table = %#v", table)
	}
}

func TestExplicitEntityOverridesDiscoveryForOperation(t *testing.T) {
	provider := newDiscoveryTestProvider(t, testOpenAPISpec, DiscoveryConfig{Enabled: true}, []EntityConfig{{
		Name:          "RECEIVABLE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
	}})
	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if len(metadata.Tables) != 1 || metadata.Tables[0].Name != "RECEIVABLE" {
		t.Fatalf("override tables = %#v", metadata.Tables)
	}
}

func TestDiscoveryIncludeOperationReportsRequiredParameters(t *testing.T) {
	specification := strings.Replace(
		testOpenAPISpec,
		"        - name: status\n          in: query",
		"        - name: status\n          in: query\n          required: true",
		1,
	)

	_, err := newDiscoveryTestProviderError(t, specification, DiscoveryConfig{
		Enabled:           true,
		IncludeOperations: []string{"listInvoices"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "required parameters need an explicit entity mapping") {
		t.Fatalf("New() error = %v, want explicit mapping error", err)
	}
}

func TestDiscoveryIncludeOperationReportsAmbiguousResponse(t *testing.T) {
	_, err := newDiscoveryTestProviderError(t, ambiguousDiscoverySpec, DiscoveryConfig{
		Enabled:           true,
		IncludeOperations: []string{"listThings"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "found 2 candidate response arrays") {
		t.Fatalf("New() error = %v, want ambiguous response error", err)
	}
}

func TestDiscoverySelectorsFilterOperations(t *testing.T) {
	specification := strings.Replace(
		testOpenAPISpec,
		"      summary: Lists invoices.",
		"      summary: Lists invoices.\n      tags:\n        - Billing",
		1,
	)
	provider := newDiscoveryTestProvider(t, specification, DiscoveryConfig{
		Enabled:     true,
		IncludeTags: []string{"billing"},
	}, nil)
	metadata, err := provider.Metadata(context.Background())
	if err != nil || len(metadata.Tables) != 1 {
		t.Fatalf("Metadata() = %#v, %v", metadata, err)
	}
}

func newDiscoveryTestProvider(t *testing.T, specification string, discovery DiscoveryConfig, entities []EntityConfig) *Provider {
	t.Helper()
	provider, err := newDiscoveryTestProviderError(t, specification, discovery, entities)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return provider
}

func newDiscoveryTestProviderError(t *testing.T, specification string, discovery DiscoveryConfig, entities []EntityConfig) (*Provider, error) {
	t.Helper()
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, specPath, specification)
	return New(Config{
		SpecFile:  specPath,
		BaseURL:   "https://api.example.test",
		Discovery: &discovery,
		Entities:  entities,
	})
}

const ambiguousDiscoverySpec = `
openapi: 3.1.0
info:
  title: Ambiguous API
  version: 1.0.0
paths:
  /things:
    get:
      operationId: listThings
      responses:
        "200":
          description: Multiple collections.
          content:
            application/json:
              schema:
                type: object
                properties:
                  active:
                    type: array
                    items:
                      type: object
                      properties:
                        id:
                          type: string
                  archived:
                    type: array
                    items:
                      type: object
                      properties:
                        id:
                          type: string
`
