package kubernetes

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigNormalizesValues(t *testing.T) {
	path := writeTestFile(t, "config.yaml", `
context: development
requestTimeout: 12s
qps: 25
burst: 40
blankNamespaceStrategy: all
`)

	config, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.Context != "development" {
		t.Fatalf("Context = %q, want development", config.Context)
	}
	if config.RequestTimeout != 12*time.Second {
		t.Fatalf("RequestTimeout = %v, want 12s", config.RequestTimeout)
	}
	if config.QPS != 25 || config.Burst != 40 {
		t.Fatalf("QPS/Burst = %v/%d, want 25/40", config.QPS, config.Burst)
	}
	if config.BlankNamespaceStrategy != BlankNamespaceAll {
		t.Fatalf("BlankNamespaceStrategy = %q, want %q", config.BlankNamespaceStrategy, BlankNamespaceAll)
	}
}

func TestNormalizeConfigDefaults(t *testing.T) {
	config, err := normalizeConfig(Config{})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	if config.RequestTimeout != defaultRequestTimeout {
		t.Fatalf("RequestTimeout = %v, want %v", config.RequestTimeout, defaultRequestTimeout)
	}
	if config.BlankNamespaceStrategy != BlankNamespaceDefault {
		t.Fatalf("BlankNamespaceStrategy = %q, want %q", config.BlankNamespaceStrategy, BlankNamespaceDefault)
	}
}

func TestLoadConfigRejectsInvalidFiles(t *testing.T) {
	tests := map[string]string{
		"unknown field":       "unknown: true\n",
		"multiple documents":  "context: first\n---\ncontext: second\n",
		"invalid duration":    "requestTimeout: eventually\n",
		"in cluster conflict": "inCluster: true\ncontext: local\n",
		"negative qps":        "qps: -1\n",
		"negative burst":      "burst: -1\n",
		"invalid strategy":    "blankNamespaceStrategy: SOMETIMES\n",
	}

	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			path := writeTestFile(t, "config.yaml", contents)
			if _, err := LoadConfig(path); err == nil {
				t.Fatal("LoadConfig() error = nil, want error")
			}
		})
	}
}

func TestBuildRESTConfigUsesOneSelectedContext(t *testing.T) {
	kubeconfig := writeTestFile(t, "kubeconfig.yaml", `
apiVersion: v1
kind: Config
clusters:
  - name: first-cluster
    cluster:
      server: https://first.example.invalid
  - name: selected-cluster
    cluster:
      server: https://selected.example.invalid
contexts:
  - name: first
    context:
      cluster: first-cluster
      namespace: first
  - name: selected
    context:
      cluster: selected-cluster
      namespace: selected
current-context: first
`)
	config, err := normalizeConfig(Config{
		Kubeconfig:     kubeconfig,
		Context:        "selected",
		RequestTimeout: 17 * time.Second,
		QPS:            31,
		Burst:          47,
	})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}

	restConfig, defaultNamespace, err := loadClientConfiguration(config)
	if err != nil {
		t.Fatalf("buildRESTConfig() error = %v", err)
	}
	if restConfig.Host != "https://selected.example.invalid" {
		t.Fatalf("Host = %q, want selected cluster", restConfig.Host)
	}
	if restConfig.Timeout != 17*time.Second {
		t.Fatalf("Timeout = %v, want 17s", restConfig.Timeout)
	}
	if restConfig.QPS != 31 || restConfig.Burst != 47 {
		t.Fatalf("QPS/Burst = %v/%d, want 31/47", restConfig.QPS, restConfig.Burst)
	}
	if defaultNamespace != "selected" {
		t.Fatalf("default namespace = %q, want selected", defaultNamespace)
	}
}

func TestNormalizeConfigRejectsNegativeTimeout(t *testing.T) {
	_, err := normalizeConfig(Config{RequestTimeout: -time.Second})
	if err == nil || !strings.Contains(err.Error(), "request timeout") {
		t.Fatalf("normalizeConfig() error = %v, want request timeout error", err)
	}
}

func writeTestFile(t *testing.T, name string, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}
