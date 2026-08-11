package kubernetes

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSchemaIncludeObjectDefaultsToTrue(t *testing.T) {
	config, err := normalizeConfig(Config{})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	if !config.Schema.includeObject() {
		t.Fatal("includeObject default = false, want true")
	}
}

func TestLoadConfigReadsIncludeObjectFalse(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "provider.yaml")
	writeIncludeObjectConfigFile(t, configPath, `
schema:
  fieldExpansionDepth: 4
  includeObject: false
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.Schema.includeObject() {
		t.Fatal("includeObject = true, want false")
	}
}

func TestNormalizeConfigCopiesIncludeObjectPointer(t *testing.T) {
	includeObject := false
	config, err := normalizeConfig(Config{Schema: SchemaConfig{IncludeObject: &includeObject}})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}

	includeObject = true
	if config.Schema.includeObject() {
		t.Fatal("normalized IncludeObject retained caller-owned pointer")
	}
}

func writeIncludeObjectConfigFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.TrimSpace(contents)+"\n"), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", path, err)
	}
}
