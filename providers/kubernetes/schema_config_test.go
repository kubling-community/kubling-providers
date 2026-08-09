package kubernetes

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigReadsSchemaConfiguration(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "provider.yaml")

	writeKubernetesConfigFile(t, configPath, `
requestTimeout: 30s
blankNamespaceStrategy: ALL

schema:
  fieldExpansionDepth: 3

  include:
    - "v1/*"
    - "apps/v1/*"
    - "batch/v1/*"

  exclude:
    - "v1/events"

  resources:
    "apps/v1/deployments":
      fieldExpansionDepth: 5

    "v1/pods":
      fieldExpansionDepth: 0
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if config.Schema.FieldExpansionDepth != 3 {
		t.Fatalf(
			"schema fieldExpansionDepth = %d, want 3",
			config.Schema.FieldExpansionDepth,
		)
	}

	if got := config.Schema.expansionDepth(
		"apps/v1",
		"deployments",
	); got != 5 {
		t.Fatalf(
			"deployment expansion depth = %d, want 5",
			got,
		)
	}

	if got := config.Schema.expansionDepth(
		"v1",
		"pods",
	); got != 0 {
		t.Fatalf(
			"pod expansion depth = %d, want 0",
			got,
		)
	}

	if got := config.Schema.expansionDepth(
		"batch/v1",
		"jobs",
	); got != 3 {
		t.Fatalf(
			"job expansion depth = %d, want 3",
			got,
		)
	}

	if !config.Schema.includesResource(
		"apps/v1",
		"deployments",
	) {
		t.Fatal(
			"apps/v1/deployments should be included",
		)
	}

	if config.Schema.includesResource(
		"v1",
		"events",
	) {
		t.Fatal(
			"v1/events should be excluded",
		)
	}

	if config.Schema.includesResource(
		"networking.k8s.io/v1",
		"ingresses",
	) {
		t.Fatal(
			"networking.k8s.io/v1/ingresses should not match include filters",
		)
	}
}

func TestNormalizeConfigDefaultsToCompactSchema(t *testing.T) {
	config, err := normalizeConfig(Config{})
	if err != nil {
		t.Fatalf(
			"normalizeConfig() error = %v",
			err,
		)
	}

	if config.Schema.FieldExpansionDepth != 0 {
		t.Fatalf(
			"schema fieldExpansionDepth = %d, want 0",
			config.Schema.FieldExpansionDepth,
		)
	}

	if !config.Schema.includesResource(
		"apps/v1",
		"deployments",
	) {
		t.Fatal(
			"empty include list should expose discoverable resources",
		)
	}
}

func TestLoadConfigRejectsNegativeFieldExpansionDepth(
	t *testing.T,
) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "provider.yaml")

	writeKubernetesConfigFile(t, configPath, `
schema:
  fieldExpansionDepth: -1
`)

	_, err := LoadConfig(configPath)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"fieldExpansionDepth must not be negative",
		) {
		t.Fatalf(
			"LoadConfig() error = %v, want negative depth validation",
			err,
		)
	}
}

func TestLoadConfigRejectsNegativeResourceFieldExpansionDepth(
	t *testing.T,
) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "provider.yaml")

	writeKubernetesConfigFile(t, configPath, `
schema:
  resources:
    "apps/v1/deployments":
      fieldExpansionDepth: -1
`)

	_, err := LoadConfig(configPath)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			`resource "apps/v1/deployments" fieldExpansionDepth must not be negative`,
		) {
		t.Fatalf(
			"LoadConfig() error = %v, want resource negative depth validation",
			err,
		)
	}
}

func TestLoadConfigRejectsUnknownNestedSchemaField(
	t *testing.T,
) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "provider.yaml")

	writeKubernetesConfigFile(t, configPath, `
schema:
  fieldExpansionDepth: 3
  unexpected: true
`)

	_, err := LoadConfig(configPath)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"field unexpected not found",
		) {
		t.Fatalf(
			"LoadConfig() error = %v, want strict YAML error",
			err,
		)
	}
}

func TestNormalizeConfigCopiesSchemaCollections(
	t *testing.T,
) {
	deploymentDepth := 5

	include := []string{
		"apps/v1/*",
	}

	exclude := []string{
		"apps/v1/controllerrevisions",
	}

	resources := map[string]ResourceSchemaConfig{
		"apps/v1/deployments": {
			FieldExpansionDepth: &deploymentDepth,
		},
	}

	config, err := normalizeConfig(Config{
		Schema: SchemaConfig{
			FieldExpansionDepth: 3,
			Include:             include,
			Exclude:             exclude,
			Resources:           resources,
		},
	})
	if err != nil {
		t.Fatalf(
			"normalizeConfig() error = %v",
			err,
		)
	}

	include[0] = "v1/*"
	exclude[0] = "v1/events"
	deploymentDepth = 9

	resources["apps/v1/deployments"] = ResourceSchemaConfig{}

	if config.Schema.Include[0] != "apps/v1/*" {
		t.Fatalf(
			"normalized include = %q, want copied value",
			config.Schema.Include[0],
		)
	}

	if config.Schema.Exclude[0] !=
		"apps/v1/controllerrevisions" {
		t.Fatalf(
			"normalized exclude = %q, want copied value",
			config.Schema.Exclude[0],
		)
	}

	if got := config.Schema.expansionDepth(
		"apps/v1",
		"deployments",
	); got != 5 {
		t.Fatalf(
			"normalized deployment depth = %d, want copied value 5",
			got,
		)
	}
}

func TestLoadConfigRejectsInvalidResourcePattern(
	t *testing.T,
) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "provider.yaml")

	writeKubernetesConfigFile(t, configPath, `
schema:
  include:
    - "apps/v1/["
`)

	_, err := LoadConfig(configPath)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"invalid include pattern",
		) {
		t.Fatalf(
			"LoadConfig() error = %v, want invalid pattern error",
			err,
		)
	}
}

func writeKubernetesConfigFile(
	t *testing.T,
	path string,
	contents string,
) {
	t.Helper()

	if err := os.WriteFile(
		path,
		[]byte(strings.TrimSpace(contents)+"\n"),
		0o600,
	); err != nil {
		t.Fatalf(
			"os.WriteFile(%q) error = %v",
			path,
			err,
		)
	}
}
