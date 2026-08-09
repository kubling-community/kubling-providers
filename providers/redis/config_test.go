package redis

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
)

func TestLoadConfigReadsStrictExternalSchema(t *testing.T) {
	directory := t.TempDir()
	schemaPath := filepath.Join(directory, "schema.yaml")
	configPath := filepath.Join(directory, "provider.yaml")
	writeTestFile(t, schemaPath, `
tables:
  - name: TASK
    structure: hash
    keyPrefix: "TASK:"
    annotation: Work items.
    updatable: true
    key:
      name: id
      type: STRING
    fields:
      - name: title
        type: STRING
        nullable: false
        updatable: true
        annotation: Human-readable title.
      - name: priority
        type: INT
`)
	writeTestFile(t, configPath, `
namespaces:
  some/path/to/redis:
    address: redis.internal:6380
    database: 2
    dialTimeout: 3s
    schemaFile: schema.yaml
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	namespace := config.Namespaces["some/path/to/redis"]
	if namespace.Address != "redis.internal:6380" || namespace.Database != 2 {
		t.Fatalf("namespace connection config = %#v", namespace)
	}
	if namespace.DialTimeout != 3*time.Second || namespace.ReadTimeout != defaultReadTimeout {
		t.Fatalf("namespace timeouts = (%v, %v)", namespace.DialTimeout, namespace.ReadTimeout)
	}
	if len(namespace.Tables) != 1 || namespace.Tables[0].Key.Type != kublingv1.ValueType_VALUE_TYPE_STRING {
		t.Fatalf("namespace tables = %#v", namespace.Tables)
	}
	if namespace.Tables[0].Fields[1].Type != kublingv1.ValueType_VALUE_TYPE_INTEGER {
		t.Fatalf("priority type = %v", namespace.Tables[0].Fields[1].Type)
	}
	if namespace.tablesByName["TASK"] == nil {
		t.Fatal("normalized table index is missing")
	}
}

func TestLoadConfigRejectsUnknownSchemaField(t *testing.T) {
	directory := t.TempDir()
	writeTestFile(t, filepath.Join(directory, "schema.yaml"), `
tables:
  - name: TASK
    structure: hash
    unexpected: true
    key:
      name: id
      type: STRING
    fields:
      - name: title
        type: STRING
`)
	configPath := filepath.Join(directory, "provider.yaml")
	writeTestFile(t, configPath, `
namespaces:
  sample:
    schemaFile: schema.yaml
`)

	_, err := LoadConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("LoadConfig() error = %v, want strict YAML error", err)
	}
}

func TestExampleConfigCoversSharedSampleSchema(t *testing.T) {
	config, err := LoadConfig("config.example.yaml")
	if err != nil {
		t.Fatalf("LoadConfig(config.example.yaml) error = %v", err)
	}
	namespace := config.Namespaces["sample"]
	if len(namespace.Tables) != 4 {
		t.Fatalf("example tables = %d, want 4", len(namespace.Tables))
	}
	if table := namespace.tablesByName["TYPE_SAMPLE"]; table == nil || len(table.Fields) != 21 {
		t.Fatalf("TYPE_SAMPLE = %#v", table)
	}
}

func TestExampleKublingDDLIncludesEquivalentTablesAndNamespace(t *testing.T) {
	ddl, err := os.ReadFile("schema.example.sql")
	if err != nil {
		t.Fatalf("os.ReadFile(schema.example.sql) error = %v", err)
	}
	text := string(ddl)
	for _, table := range []string{"PROJECT", "TASK", "AUDIT_EVENT", "TYPE_SAMPLE"} {
		if !strings.Contains(text, "CREATE FOREIGN TABLE "+table) {
			t.Fatalf("schema.example.sql is missing table %s", table)
		}
	}
	if got := strings.Count(text, `"kbl.namespace" 'sample'`); got != 4 {
		t.Fatalf("schema.example.sql namespace properties = %d, want 4", got)
	}
}

func TestNewCopiesProgrammaticConfig(t *testing.T) {
	fields := []ColumnConfig{{Name: "title", Type: kublingv1.ValueType_VALUE_TYPE_STRING}}
	tables := []TableConfig{{
		Name:      "TASK",
		Key:       ColumnConfig{Name: "id", Type: kublingv1.ValueType_VALUE_TYPE_STRING},
		Fields:    fields,
		Updatable: true,
	}}
	tlsConfig := &TLSConfig{ServerName: "redis.internal"}
	provider, err := New(Config{Namespaces: map[string]NamespaceConfig{
		"sample": {Tables: tables, TLS: tlsConfig},
	}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tables[0].Name = "CHANGED"
	fields[0].Name = "changed"
	tlsConfig.ServerName = "changed"
	stored := provider.config.Namespaces["sample"]
	if stored.Tables[0].Name != "TASK" || stored.Tables[0].Fields[0].Name != "title" {
		t.Fatalf("New() retained caller-owned slices: %#v", stored.Tables)
	}
	if stored.TLS.ServerName != "redis.internal" {
		t.Fatalf("New() retained caller-owned TLS = %#v", stored.TLS)
	}
}

func writeTestFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.TrimSpace(contents)+"\n"), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", path, err)
	}
}
