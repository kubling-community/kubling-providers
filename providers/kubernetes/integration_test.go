package kubernetes

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestKubernetesIntegrationMetadataAndQuery(t *testing.T) {
	if os.Getenv("KUBLING_KUBERNETES_INTEGRATION") == "" {
		t.Skip("set KUBLING_KUBERNETES_INTEGRATION=1 with local k3s running")
	}

	config, err := LoadConfig("local/provider.yaml")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	implementation, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	health, err := implementation.Health(ctx)
	if err != nil || !health.GetHealthy() {
		t.Fatalf("Health() = (%v, %v)", health, err)
	}
	metadata, err := implementation.Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if integrationMetadataTable(metadata, "CONFIG_MAP", config.Namespace) == nil {
		t.Fatalf("CONFIG_MAP table was not discovered in %s", config.Namespace)
	}

	opened, err := implementation.Open(ctx)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer opened.Close(context.Background())
	stream, err := opened.Query(ctx, &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "CONFIG_MAP", Namespace: config.Namespace},
		Projections: []*providerv1.Projection{
			fieldProjection("metadata__name", ""),
			fieldProjection("metadata__namespace", ""),
			fieldProjection("object", ""),
		},
		Filter: andExpression(
			equalExpression("metadata__namespace", "kubling-sample"),
			equalExpression("metadata__name", "provider-sample"),
		),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer stream.Close()
	batch, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if len(batch.GetTuples()) != 1 ||
		batch.GetTuples()[0].GetValues()[0].GetStringValue() != "provider-sample" ||
		batch.GetTuples()[0].GetValues()[1].GetStringValue() != "kubling-sample" {
		t.Fatalf("Query() batch = %v", batch)
	}
	if _, err := stream.Next(ctx); err != io.EOF {
		t.Fatalf("second Next() error = %v, want EOF", err)
	}
}

func integrationMetadataTable(
	metadata *providerv1.SchemaMetadata,
	name string,
	namespace string,
) *providerv1.TableMetadata {
	for _, table := range metadata.GetTables() {
		if table.GetName() == name && table.GetNamespace() == namespace {
			return table
		}
	}
	return nil
}
