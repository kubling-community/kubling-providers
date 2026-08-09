package kubernetes

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClusterClientHealthUsesReadyz(t *testing.T) {
	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestedPath = request.URL.Path
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	kubeconfig := writeTestFile(t, "kubeconfig.yaml", fmt.Sprintf(`
apiVersion: v1
kind: Config
clusters:
  - name: test
    cluster:
      server: %s
contexts:
  - name: test
    context:
      cluster: test
current-context: test
`, server.URL))
	config, err := normalizeConfig(Config{Kubeconfig: kubeconfig})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	client, err := newClusterClient(context.Background(), config)
	if err != nil {
		t.Fatalf("newClusterClient() error = %v", err)
	}
	defer client.Close()

	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if requestedPath != "/readyz" {
		t.Fatalf("requested path = %q, want /readyz", requestedPath)
	}
}

func TestNewClusterClientHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := newClusterClient(ctx, Config{})
	if err != context.Canceled {
		t.Fatalf("newClusterClient() error = %v, want context.Canceled", err)
	}
}
