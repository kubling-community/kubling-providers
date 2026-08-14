package openapi

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestProviderBuildsReadOnlyMetadataFromOpenAPI(t *testing.T) {
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, specPath, testOpenAPISpec)

	provider, err := New(Config{
		SpecFile:  specPath,
		BaseURL:   "https://billing.example.test/api",
		Namespace: "billing",
		Entities: []EntityConfig{{
			Name:            "INVOICE",
			ListOperation:   "listInvoices",
			ResponsePath:    "/data/items",
			PrimaryKey:      []string{"id"},
			EqualityFilters: []EqualityFilterConfig{{Field: "active", Parameter: "active"}},
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if metadata.Properties["openapi.version"] != "3.1.0" || metadata.Properties["openapi.title"] != "Billing API" {
		t.Fatalf("Metadata() properties = %v", metadata.Properties)
	}
	if len(metadata.Namespaces) != 1 || metadata.Namespaces[0].Name != "billing" || metadata.Namespaces[0].Annotation != "Billing API" {
		t.Fatalf("Metadata() namespaces = %#v", metadata.Namespaces)
	}
	if len(metadata.Tables) != 1 {
		t.Fatalf("Metadata() tables = %d, want 1", len(metadata.Tables))
	}

	table := metadata.Tables[0]
	if table.Name != "INVOICE" || table.SourceName != "listInvoices" || table.Namespace != "billing" {
		t.Fatalf("Metadata() table identity = %#v", table)
	}
	if table.Updatable == nil || table.GetUpdatable() {
		t.Fatalf("Metadata() table updatable = %v", table.Updatable)
	}
	if table.Annotation != "Lists invoices." {
		t.Fatalf("Metadata() table annotation = %q", table.Annotation)
	}
	if table.Properties["openapi.method"] != "GET" || table.Properties["openapi.path"] != "/invoices" || table.Properties["openapi.response_path"] != "/data/items" {
		t.Fatalf("Metadata() table properties = %v", table.Properties)
	}
	if len(table.Keys) != 1 || table.Keys[0].Kind != providerv1.KeyKind_KEY_KIND_PRIMARY || !slices.Equal(table.Keys[0].Columns, []string{"id"}) {
		t.Fatalf("Metadata() keys = %#v", table.Keys)
	}

	wantNames := []string{"active", "amount", "createdAt", "id", "labels", "note", "priority"}
	gotNames := make([]string, len(table.Columns))
	columns := make(map[string]*providerv1.ColumnMetadata, len(table.Columns))
	for index, column := range table.Columns {
		gotNames[index] = column.Name
		columns[column.Name] = column
		if column.Updatable == nil || column.GetUpdatable() {
			t.Fatalf("column %q updatable = %v", column.Name, column.Updatable)
		}
		wantSearchability := providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE
		if column.Name == "active" {
			wantSearchability = providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY
		}
		if column.Searchability != wantSearchability {
			t.Fatalf("column %q searchability = %v", column.Name, column.Searchability)
		}
	}
	if !slices.Equal(gotNames, wantNames) {
		t.Fatalf("Metadata() column names = %v, want %v", gotNames, wantNames)
	}

	assertColumn(t, columns["active"], kublingv1.ValueType_VALUE_TYPE_BOOLEAN, "boolean", false)
	assertColumn(t, columns["amount"], kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL, "number", true)
	assertColumn(t, columns["createdAt"], kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, "string(date-time)", false)
	assertColumn(t, columns["id"], kublingv1.ValueType_VALUE_TYPE_STRING, "string", false)
	assertColumn(t, columns["labels"], kublingv1.ValueType_VALUE_TYPE_JSON, "array", true)
	assertColumn(t, columns["note"], kublingv1.ValueType_VALUE_TYPE_STRING, "string", true)
	assertColumn(t, columns["priority"], kublingv1.ValueType_VALUE_TYPE_INTEGER, "integer(int32)", true)
	if columns["note"].Length == nil || columns["note"].GetLength() != 120 {
		t.Fatalf("note length = %v", columns["note"].Length)
	}
}

func TestProviderMetadataReturnsIndependentClone(t *testing.T) {
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, specPath, testOpenAPISpec)
	provider, err := New(Config{
		SpecFile: specPath,
		BaseURL:  "https://billing.example.test/api",
		Entities: []EntityConfig{{
			Name:          "INVOICE",
			ListOperation: "listInvoices",
			ResponsePath:  "/data/items",
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("first Metadata() error = %v", err)
	}
	first.Tables[0].Name = "CHANGED"
	first.Tables[0].Columns[0].Name = "changed"
	first.Properties["openapi.title"] = "Changed"

	second, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("second Metadata() error = %v", err)
	}
	if second.Tables[0].Name != "INVOICE" || second.Tables[0].Columns[0].Name != "active" || second.Properties["openapi.title"] != "Billing API" {
		t.Fatalf("Metadata() returned shared state: %#v", second)
	}
}

func TestProviderAdvertisesImplementedQueryCapabilities(t *testing.T) {
	provider := newMetadataTestProvider(t)
	capabilities, err := provider.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	query := capabilities.Query
	if query == nil || query.Pagination == nil || !query.Pagination.Limit || !query.Pagination.Offset {
		t.Fatalf("query pagination capabilities = %#v", query)
	}
	if !slices.Contains(query.Expressions.GetComparisonOperators(), providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL) {
		t.Fatalf("comparison capabilities = %v", query.Expressions.GetComparisonOperators())
	}
	if !slices.Contains(query.Expressions.GetLogicalOperators(), providerv1.LogicalOperator_LOGICAL_OPERATOR_AND) {
		t.Fatalf("logical capabilities = %v", query.Expressions.GetLogicalOperators())
	}
}

func newMetadataTestProvider(t *testing.T) *Provider {
	t.Helper()
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, specPath, testOpenAPISpec)
	provider, err := New(Config{
		SpecFile: specPath,
		BaseURL:  "https://billing.example.test/api",
		Entities: []EntityConfig{{
			Name:          "INVOICE",
			ListOperation: "listInvoices",
			ResponsePath:  "/data/items",
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return provider
}

func TestNewRejectsInvalidEntityDiscovery(t *testing.T) {
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, specPath, testOpenAPISpec)
	tests := []struct {
		name   string
		entity EntityConfig
		want   string
	}{
		{
			name:   "operation not found",
			entity: EntityConfig{Name: "INVOICE", ListOperation: "missingOperation", ResponsePath: "/data/items"},
			want:   `GET operationId "missingOperation" was not found`,
		},
		{
			name:   "response property not found",
			entity: EntityConfig{Name: "INVOICE", ListOperation: "listInvoices", ResponsePath: "/data/missing"},
			want:   `responsePath property "missing" was not found`,
		},
		{
			name:   "response is not array",
			entity: EntityConfig{Name: "INVOICE", ListOperation: "listInvoices", ResponsePath: "/data"},
			want:   "schema must be an array with an item schema",
		},
		{
			name:   "primary key absent",
			entity: EntityConfig{Name: "INVOICE", ListOperation: "listInvoices", ResponsePath: "/data/items", PrimaryKey: []string{"missing"}},
			want:   `primaryKey field "missing" is not present`,
		},
		{
			name: "query parameter absent",
			entity: EntityConfig{
				Name:            "INVOICE",
				ListOperation:   "listInvoices",
				ResponsePath:    "/data/items",
				QueryParameters: []QueryParameterConfig{{Name: "missing", Value: "value"}},
			},
			want: `query parameter "missing" is not defined`,
		},
		{
			name: "equality field absent",
			entity: EntityConfig{
				Name:            "INVOICE",
				ListOperation:   "listInvoices",
				ResponsePath:    "/data/items",
				EqualityFilters: []EqualityFilterConfig{{Field: "missing", Parameter: "status"}},
			},
			want: `equality filter field "missing" is not present`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(Config{SpecFile: specPath, BaseURL: "https://billing.example.test/api", Entities: []EntityConfig{test.entity}})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("New() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestNewRejectsUnboundRequiredQueryParameter(t *testing.T) {
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	spec := strings.Replace(
		testOpenAPISpec,
		"        - name: status\n          in: query",
		"        - name: status\n          in: query\n          required: true",
		1,
	)
	writeTestFile(t, specPath, spec)

	_, err := New(Config{
		SpecFile: specPath,
		BaseURL:  "https://billing.example.test/api",
		Entities: []EntityConfig{{
			Name:          "INVOICE",
			ListOperation: "listInvoices",
			ResponsePath:  "/data/items",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), `required query parameter "status" has no static, filter or pagination binding`) {
		t.Fatalf("New() error = %v, want required parameter binding error", err)
	}
}

func assertColumn(t *testing.T, column *providerv1.ColumnMetadata, valueType kublingv1.ValueType, nativeType string, nullable bool) {
	t.Helper()
	if column == nil {
		t.Fatal("column is nil")
	}
	if column.Type != valueType || column.NativeType != nativeType || column.Nullable == nil || column.GetNullable() != nullable {
		t.Fatalf("column %q = type %v, native %q, nullable %v", column.Name, column.Type, column.NativeType, column.Nullable)
	}
}

const testOpenAPISpec = `
openapi: 3.1.0
info:
  title: Billing API
  version: 1.2.0
paths:
  /invoices:
    get:
      operationId: listInvoices
      summary: Lists invoices.
      parameters:
        - name: limit
          in: query
          schema:
            type: integer
        - name: offset
          in: query
          schema:
            type: integer
        - name: per_page
          in: query
          schema:
            type: integer
        - name: page
          in: query
          schema:
            type: integer
        - name: after
          in: query
          schema:
            type: string
        - name: status
          in: query
          schema:
            type: string
        - name: active
          in: query
          schema:
            type: boolean
        - name: jql
          in: query
          schema:
            type: string
        - name: fields
          in: query
          schema:
            type: string
      responses:
        "200":
          description: Invoice page.
          content:
            application/json:
              schema:
                type: object
                properties:
                  data:
                    type: object
                    properties:
                      items:
                        type: array
                        items:
                          $ref: "#/components/schemas/Invoice"
components:
  securitySchemes:
    BearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT
    BasicAuth:
      type: http
      scheme: basic
    HeaderKey:
      type: apiKey
      in: header
      name: X-API-Key
    QueryKey:
      type: apiKey
      in: query
      name: api_key
    CookieKey:
      type: apiKey
      in: cookie
      name: session_key
    OAuth:
      type: oauth2
      flows: {}
  schemas:
    Entity:
      type: object
      required:
        - id
        - active
      properties:
        id:
          type: string
          description: Stable invoice identifier.
        active:
          type: boolean
    Invoice:
      allOf:
        - $ref: "#/components/schemas/Entity"
        - type: object
          required:
            - createdAt
          properties:
            createdAt:
              type: string
              format: date-time
            priority:
              type: integer
              format: int32
            amount:
              type: number
            labels:
              type: array
              items:
                type: string
            note:
              type:
                - string
                - "null"
              maxLength: 120
`
