package kubernetes

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/openapi"
)

func TestResourceFieldExpansionDepthOverrideFromYAML(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "kubernetes.yaml")
	contents := `
schema:
  fieldExpansionDepth: 4
  includeObject: false
  resources:
    "batch/v1/cronjobs":
      fieldExpansionDepth: 6
`
	if err := os.WriteFile(
		configPath,
		[]byte(strings.TrimSpace(contents)+"\n"),
		0o600,
	); err != nil {
		t.Fatalf("write config: %v", err)
	}

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if got := config.Schema.FieldExpansionDepth; got != 4 {
		t.Fatalf("global fieldExpansionDepth = %d, want 4", got)
	}
	if got := config.Schema.expansionDepth("apps/v1", "deployments"); got != 4 {
		t.Fatalf("deployment expansionDepth = %d, want global 4", got)
	}
	if got := config.Schema.expansionDepth("batch/v1", "cronjobs"); got != 6 {
		t.Fatalf("cronjob expansionDepth = %d, want override 6; resources = %#v", got, config.Schema.Resources)
	}
}

func TestProviderMetadataUsesResourceFieldExpansionDepthOverride(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "kubernetes.yaml")
	contents := `
schema:
  fieldExpansionDepth: 4
  includeObject: false
  resources:
    "batch/v1/cronjobs":
      fieldExpansionDepth: 6
`
	if err := os.WriteFile(
		configPath,
		[]byte(strings.TrimSpace(contents)+"\n"),
		0o600,
	); err != nil {
		t.Fatalf("write config: %v", err)
	}

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	groupVersion := &staticOpenAPIGroupVersion{
		contents:          []byte(testCronJobOpenAPIV3),
		serverRelativeURL: "/openapi/v3/apis/batch/v1?hash=cronjob-override",
	}
	discoveryClient := &openAPIDiscovery{
		staticDiscovery: &staticDiscovery{resourceLists: []*metav1.APIResourceList{{
			GroupVersion: "batch/v1",
			APIResources: []metav1.APIResource{{
				Name:       "cronjobs",
				Kind:       "CronJob",
				Namespaced: true,
				Verbs:      metav1.Verbs{"get", "list", "create", "patch", "update", "delete"},
			}},
		}}},
		client: &staticOpenAPIClient{paths: map[string]openapi.GroupVersion{
			"apis/batch/v1": groupVersion,
		}},
	}
	client := &fakeKubernetesClient{discoveryClient: discoveryClient}

	provider := newProvider(config, func(context.Context, Config) (kubernetesClient, error) {
		return client, nil
	})

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}

	cronJob := metadataTable(t, metadata, "CRON_JOB")

	if got := cronJob.GetProperties()["kubernetes.field_expansion_depth"]; got != "6" {
		t.Fatalf(
			"CRON_JOB kubernetes.field_expansion_depth = %q, want 6; resources = %#v",
			got,
			config.Schema.Resources,
		)
	}

	for _, column := range []string{
		"spec__jobTemplate__spec__template__spec__containers",
		"spec__jobTemplate__spec__template__spec__restartPolicy",
	} {
		if !metadataColumnExists(cronJob, column) {
			t.Fatalf("CRON_JOB missing depth-6 column %q", column)
		}
	}

	if metadataColumnExists(cronJob, "spec__jobTemplate__spec__template") {
		t.Fatal("CRON_JOB stopped at depth 4 despite depth-6 resource override")
	}
}
