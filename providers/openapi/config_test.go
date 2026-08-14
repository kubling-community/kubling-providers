package openapi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigReadsStrictEntityMappings(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "provider.yaml")
	writeTestFile(t, configPath, `
specFile: api.yaml
baseUrl: https://billing.example.test/api
namespace: billing
requestTimeout: 5s
maxResponseBytes: 1048576
headers:
  X-Tenant: tenant-a
authentication:
  securityScheme: BearerAuth
  credential: test-token
entities:
  - name: INVOICE
    listOperation: listInvoices
    responsePath: /data/items
    primaryKey:
      - id
    queryParameters:
      - name: jql
        value: project = KBL
    equalityFilters:
      - field: active
        parameter: active
    pagination:
      mode: CURSOR
      pageSize: 50
      pageSizeParameter: limit
      cursorParameter: cursor
      nextCursorPath: /data/nextCursor
      hasMorePath: /data/hasMore
      maxPages: 200
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.SpecFile != filepath.Join(directory, "api.yaml") {
		t.Fatalf("LoadConfig() specFile = %q", config.SpecFile)
	}
	if config.Namespace != "billing" {
		t.Fatalf("LoadConfig() namespace = %q", config.Namespace)
	}
	if config.BaseURL != "https://billing.example.test/api" || config.RequestTimeout.String() != "5s" {
		t.Fatalf("LoadConfig() HTTP config = (%q, %v)", config.BaseURL, config.RequestTimeout)
	}
	if config.MaxResponseBytes != 1048576 {
		t.Fatalf("LoadConfig() maxResponseBytes = %d", config.MaxResponseBytes)
	}
	if config.Headers["X-Tenant"] != "tenant-a" {
		t.Fatalf("LoadConfig() headers = %v", config.Headers)
	}
	if config.Authentication == nil || config.Authentication.SecurityScheme != "BearerAuth" || config.Authentication.Credential != "test-token" {
		t.Fatalf("LoadConfig() authentication = %#v", config.Authentication)
	}
	if len(config.Entities) != 1 {
		t.Fatalf("LoadConfig() entities = %d, want 1", len(config.Entities))
	}
	entity := config.Entities[0]
	if entity.Name != "INVOICE" || entity.ListOperation != "listInvoices" || entity.ResponsePath != "/data/items" {
		t.Fatalf("LoadConfig() entity = %#v", entity)
	}
	if len(entity.PrimaryKey) != 1 || entity.PrimaryKey[0] != "id" {
		t.Fatalf("LoadConfig() primaryKey = %v", entity.PrimaryKey)
	}
	if len(entity.QueryParameters) != 1 || entity.QueryParameters[0].Name != "jql" || entity.QueryParameters[0].Value != "project = KBL" {
		t.Fatalf("LoadConfig() queryParameters = %#v", entity.QueryParameters)
	}
	if len(entity.EqualityFilters) != 1 || entity.EqualityFilters[0].Field != "active" || entity.EqualityFilters[0].Parameter != "active" {
		t.Fatalf("LoadConfig() equalityFilters = %#v", entity.EqualityFilters)
	}
	if entity.Pagination == nil || entity.Pagination.Mode != PaginationModeCursor || entity.Pagination.PageSize != 50 || entity.Pagination.HasMorePath != "/data/hasMore" || entity.Pagination.MaxPages != 200 {
		t.Fatalf("LoadConfig() pagination = %#v", entity.Pagination)
	}
}

func TestLoadConfigReadsStableKey(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	writeTestFile(t, configPath, `
specFile: api.yaml
baseUrl: https://billing.example.test/api
entities:
  - name: INVOICE
    listOperation: listInvoices
    stableKey:
      columns:
        - tenant_id
        - id
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	stableKey := config.Entities[0].StableKey
	if stableKey == nil || stableKey.Name != "identifier" {
		t.Fatalf("LoadConfig() stableKey = %#v", stableKey)
	}
	if len(stableKey.Columns) != 2 || stableKey.Columns[0] != "tenant_id" || stableKey.Columns[1] != "id" {
		t.Fatalf("LoadConfig() stableKey columns = %v", stableKey.Columns)
	}
}

func TestNormalizeConfigDefaultsAndValidatesMaxResponseBytes(t *testing.T) {
	config := Config{
		SpecFile:  "api.yaml",
		BaseURL:   "https://api.example.test",
		Discovery: &DiscoveryConfig{Enabled: true},
	}
	normalized, err := normalizeConfig(config)
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	if normalized.MaxResponseBytes != defaultMaxResponseBytes {
		t.Fatalf("normalizeConfig() maxResponseBytes = %d, want %d", normalized.MaxResponseBytes, defaultMaxResponseBytes)
	}

	config.MaxResponseBytes = -1
	if _, err := normalizeConfig(config); err == nil || !strings.Contains(err.Error(), "maxResponseBytes must not be negative") {
		t.Fatalf("normalizeConfig() error = %v, want maxResponseBytes validation", err)
	}
	config.MaxResponseBytes = maxResponseBytesLimit + 1
	if _, err := normalizeConfig(config); err == nil || !strings.Contains(err.Error(), "maxResponseBytes must not exceed") {
		t.Fatalf("normalizeConfig() error = %v, want maxResponseBytes upper bound", err)
	}
}

func TestNormalizeConfigRejectsBaseURLUserInformation(t *testing.T) {
	_, err := normalizeConfig(Config{
		SpecFile:  "api.yaml",
		BaseURL:   "https://user:secret@api.example.test",
		Discovery: &DiscoveryConfig{Enabled: true},
	})
	if err == nil || !strings.Contains(err.Error(), "baseUrl must not contain user information") {
		t.Fatalf("normalizeConfig() error = %v, want user information rejection", err)
	}
}

func TestLoadConfigPreservesRemoteSpecificationURL(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	writeTestFile(t, configPath, `
specFile: https://spec.example.test/openapi.yaml?revision=main
baseUrl: https://billing.example.test
entities:
  - name: INVOICE
    listOperation: listInvoices
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.SpecFile != "https://spec.example.test/openapi.yaml?revision=main" {
		t.Fatalf("LoadConfig() specFile = %q", config.SpecFile)
	}
}

func TestLoadConfigReadsDiscoverySelectors(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	writeTestFile(t, configPath, `
specFile: api.yaml
baseUrl: https://api.example.test
discovery:
  enabled: true
  includeTags:
    - Public
  includeOperations:
    - listProjects
  excludeOperations:
    - listInternalProjects
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.Discovery == nil || !config.Discovery.Enabled || len(config.Discovery.IncludeTags) != 1 || len(config.Discovery.IncludeOperations) != 1 || len(config.Discovery.ExcludeOperations) != 1 {
		t.Fatalf("LoadConfig() discovery = %#v", config.Discovery)
	}
}

func TestLoadConfigReadsMutationMappings(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	writeTestFile(t, configPath, `
specFile: api.yaml
baseUrl: https://billing.example.test
entities:
  - name: INVOICE
    listOperation: listInvoices
    mutations:
      insert:
        operation: createInvoice
        queryParameters:
          - name: tenant
            value: tenant-a
        bodyPath: /payload
      update:
        operation: updateInvoice
        pathParameters:
          - parameter: invoiceId
            field: id
        bodyPath: /payload
      delete:
        operation: deleteInvoice
        pathParameters:
          - parameter: invoiceId
            field: id
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	mutations := config.Entities[0].Mutations
	if mutations == nil || mutations.Insert == nil || mutations.Update == nil || mutations.Delete == nil {
		t.Fatalf("LoadConfig() mutations = %#v", mutations)
	}
	if mutations.Insert.Operation != "createInvoice" || mutations.Insert.BodyPath != "/payload" || mutations.Insert.QueryParameters[0].Value != "tenant-a" {
		t.Fatalf("LoadConfig() insert = %#v", mutations.Insert)
	}
	if mutations.Update.PathParameters[0].Parameter != "invoiceId" || mutations.Update.PathParameters[0].Field != "id" {
		t.Fatalf("LoadConfig() update = %#v", mutations.Update)
	}
}

func TestLoadConfigExpandsEnvironmentVariables(t *testing.T) {
	t.Setenv("OPENAPI_SPEC_URL", "https://spec.example.test/openapi.yaml")
	t.Setenv("OPENAPI_TOKEN", "secret-token")
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	writeTestFile(t, configPath, `
specFile: ${OPENAPI_SPEC_URL}
baseUrl: ${OPENAPI_BASE_URL:-https://api.example.test}
authentication:
  securityScheme: BearerAuth
  credential: ${OPENAPI_TOKEN}
discovery:
  enabled: true
headers:
  X-Literal: $${NOT_EXPANDED}
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.SpecFile != "https://spec.example.test/openapi.yaml" || config.BaseURL != "https://api.example.test" {
		t.Fatalf("LoadConfig() locations = (%q, %q)", config.SpecFile, config.BaseURL)
	}
	if config.Authentication == nil || config.Authentication.Credential != "secret-token" {
		t.Fatalf("LoadConfig() authentication = %#v", config.Authentication)
	}
	if config.Headers["X-Literal"] != "${NOT_EXPANDED}" {
		t.Fatalf("LoadConfig() escaped placeholder = %q", config.Headers["X-Literal"])
	}
}

func TestLoadConfigRejectsMissingEnvironmentVariable(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	writeTestFile(t, configPath, `
specFile: ${DEFINITELY_MISSING_OPENAPI_SPEC}
baseUrl: https://api.example.test
discovery:
  enabled: true
`)

	_, err := LoadConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "environment variable DEFINITELY_MISSING_OPENAPI_SPEC is not set") {
		t.Fatalf("LoadConfig() error = %v, want missing environment error", err)
	}
}

func TestLoadConfigRejectsUnknownField(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	writeTestFile(t, configPath, `
specFile: api.yaml
unexpected: true
entities:
  - name: INVOICE
    listOperation: listInvoices
`)

	_, err := LoadConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("LoadConfig() error = %v, want strict YAML error", err)
	}
}

func TestNormalizeConfigRejectsInvalidEntityMappings(t *testing.T) {
	tests := []struct {
		name     string
		entities []EntityConfig
		want     string
	}{
		{
			name: "duplicate entity",
			entities: []EntityConfig{
				{Name: "INVOICE", ListOperation: "listInvoices"},
				{Name: "INVOICE", ListOperation: "listArchivedInvoices"},
			},
			want: `duplicate entity name "INVOICE"`,
		},
		{
			name:     "invalid pointer",
			entities: []EntityConfig{{Name: "INVOICE", ListOperation: "listInvoices", ResponsePath: "/data/~2items"}},
			want:     "invalid escape",
		},
		{
			name:     "duplicate key",
			entities: []EntityConfig{{Name: "INVOICE", ListOperation: "listInvoices", PrimaryKey: []string{"id", "id"}}},
			want:     `duplicate primaryKey field "id"`,
		},
		{
			name: "primary and stable key",
			entities: []EntityConfig{{
				Name:          "INVOICE",
				ListOperation: "listInvoices",
				PrimaryKey:    []string{"id"},
				StableKey:     &StableKeyConfig{Columns: []string{"tenant_id", "id"}},
			}},
			want: "primaryKey and stableKey cannot both be configured",
		},
		{
			name: "empty stable key",
			entities: []EntityConfig{{
				Name:          "INVOICE",
				ListOperation: "listInvoices",
				StableKey:     &StableKeyConfig{},
			}},
			want: "stableKey: at least one component column is required",
		},
		{
			name: "invalid pagination",
			entities: []EntityConfig{{
				Name:          "INVOICE",
				ListOperation: "listInvoices",
				Pagination:    &PaginationConfig{Mode: PaginationModeCursor, PageSize: 10},
			}},
			want: "cursorParameter is required",
		},
		{
			name: "static and filter conflict",
			entities: []EntityConfig{{
				Name:            "INVOICE",
				ListOperation:   "listInvoices",
				QueryParameters: []QueryParameterConfig{{Name: "status", Value: "open"}},
				EqualityFilters: []EqualityFilterConfig{{Field: "active", Parameter: "status"}},
			}},
			want: `query parameter "status" cannot be both static and filter-bound`,
		},
		{
			name: "pagination conflict",
			entities: []EntityConfig{{
				Name:            "INVOICE",
				ListOperation:   "listInvoices",
				QueryParameters: []QueryParameterConfig{{Name: "offset", Value: "10"}},
				Pagination: &PaginationConfig{
					Mode:            PaginationModeOffset,
					PageSize:        25,
					OffsetParameter: "offset",
				},
			}},
			want: `query parameter "offset" conflicts with pagination`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizeConfig(Config{SpecFile: "api.yaml", BaseURL: "https://example.test", Entities: test.entities})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("normalizeConfig() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.TrimSpace(content)+"\n"), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", path, err)
	}
}
