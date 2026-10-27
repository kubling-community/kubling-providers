package openapi

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
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

func TestProviderDiscoversParameterizedGETEntity(t *testing.T) {
	specification := strings.Replace(
		testParameterizedOpenAPISpec,
		"        - name: channel\n          in: query\n          required: true",
		"        - name: channel\n          in: query",
		1,
	)
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		if request.URL.EscapedPath() != "/owners/acme/repositories/widget%20api/releases" {
			t.Errorf("request path = %q", request.URL.EscapedPath())
		}
		return jsonResponse(http.StatusOK, `[{"id":7,"name":"v1"}]`), nil
	})
	provider, err := newDiscoveryTestProviderErrorWithClient(
		t,
		specification,
		DiscoveryConfig{Enabled: true},
		nil,
		client,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if len(metadata.Tables) != 1 {
		t.Fatalf("discovered tables = %d, want 1", len(metadata.Tables))
	}
	table := metadata.Tables[0]
	if table.Name != "LIST_RELEASES" || table.SourceName != "listReleases" ||
		table.Properties["openapi.path"] != "/owners/{owner}/repositories/{repository}/releases" {
		t.Fatalf("discovered table = %#v", table)
	}
	columns := make(map[string]bool, len(table.Columns))
	for _, column := range table.Columns {
		columns[column.Name] = true
	}
	for _, name := range []string{"owner", "repository"} {
		if !columns[name] {
			t.Fatalf("discovered path column %q is missing from %#v", name, table.Columns)
		}
	}
	patterns := table.GetAccessPatterns()
	if len(patterns) != 1 || len(patterns[0].GetColumns()) != 2 ||
		patterns[0].GetColumns()[0] != "owner" || patterns[0].GetColumns()[1] != "repository" {
		t.Fatalf("discovered access patterns = %#v", patterns)
	}

	opened, err := provider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	connection := opened.(*Connection)
	defer func() {
		_ = connection.Close(context.Background())
	}()
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "LIST_RELEASES"},
		Filter: logicalAND(
			equality(field("owner"), literalString("acme")),
			equality(field("repository"), literalString("widget api")),
		),
		Projections: []*providerv1.Projection{
			{Expression: field("owner")},
			{Expression: field("repository")},
			{Expression: field("id")},
		},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer stream.Close()
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if len(batch.GetTuples()) != 1 {
		t.Fatalf("discovered tuples = %#v", batch.GetTuples())
	}
	values := batch.GetTuples()[0].GetValues()
	if values[0].GetStringValue() != "acme" || values[1].GetStringValue() != "widget api" || values[2].GetLongValue() != 7 {
		t.Fatalf("discovered parameterized tuple = %#v", values)
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
	if err == nil || !strings.Contains(err.Error(), "required non-path parameters need an explicit entity mapping") {
		t.Fatalf("New() error = %v, want explicit mapping error", err)
	}
}

func TestDiscoveryRequiresExplicitMappingForPathFieldCollision(t *testing.T) {
	specification := strings.Replace(
		testParameterizedOpenAPISpec,
		"        - name: channel\n          in: query\n          required: true",
		"        - name: channel\n          in: query",
		1,
	)
	specification = strings.Replace(
		specification,
		"                  properties:\n                    id:",
		"                  properties:\n                    owner:\n                      type: string\n                    id:",
		1,
	)

	_, err := newDiscoveryTestProviderError(t, specification, DiscoveryConfig{
		Enabled:           true,
		IncludeOperations: []string{"listReleases"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), `path parameter "owner" would share field "owner"`) ||
		!strings.Contains(err.Error(), "use an explicit entity mapping") {
		t.Fatalf("New() error = %v, want path field collision", err)
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
	return newDiscoveryTestProviderErrorWithClient(t, specification, discovery, entities, nil)
}

func newDiscoveryTestProviderErrorWithClient(
	t *testing.T,
	specification string,
	discovery DiscoveryConfig,
	entities []EntityConfig,
	client *http.Client,
) (*Provider, error) {
	t.Helper()
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, specPath, specification)
	return New(Config{
		SpecFile:   specPath,
		BaseURL:    "https://api.example.test",
		HTTPClient: client,
		Discovery:  &discovery,
		Entities:   entities,
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
