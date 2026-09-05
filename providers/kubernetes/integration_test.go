package kubernetes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	deployment := integrationMetadataTable(metadata, "DEPLOYMENT", config.Namespace)
	replicaSet := integrationMetadataTable(metadata, "REPLICA_SET", config.Namespace)
	pod := integrationMetadataTable(metadata, "POD", config.Namespace)
	if deployment == nil || replicaSet == nil || pod == nil {
		t.Fatalf(
			"workload tables = DEPLOYMENT:%v REPLICA_SET:%v POD:%v",
			deployment != nil,
			replicaSet != nil,
			pod != nil,
		)
	}
	assertOwnerReferenceColumn(t, replicaSet, "deployment__uid", "Deployment", "uid")
	assertOwnerReferenceColumn(t, pod, "replica_set__uid", "ReplicaSet", "uid")
	assertForeignKey(t, replicaSet, "deployment__uid", deployment.GetName())
	assertForeignKey(t, pod, "replica_set__uid", replicaSet.GetName())

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

func TestKubernetesIntegrationSemanticFragmentServer(t *testing.T) {
	if os.Getenv("KUBLING_KUBERNETES_SERVER_INTEGRATION") == "" {
		t.Skip("set KUBLING_KUBERNETES_SERVER_INTEGRATION=1 with the local provider server running")
	}

	address := os.Getenv("KUBLING_KUBERNETES_PROVIDER_ADDRESS")
	if address == "" {
		address = "127.0.0.1:50054"
	}
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	defer connection.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := providerv1.NewProviderServiceClient(connection).GetSemanticFragment(
		ctx,
		&providerv1.GetSemanticFragmentRequest{},
	)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	fragment := response.GetFragment()
	if fragment == nil {
		t.Fatal("GetSemanticFragment() fragment = nil")
	}
	if !bytes.Equal(fragment.GetDocument(), kubernetesSemanticFragmentDocument) {
		t.Fatal("server returned unexpected Kubernetes semantic document bytes")
	}
	if fragment.GetMediaType() != providersdk.SemanticFragmentMediaTypeYAML ||
		fragment.GetVersion() != kubernetesSemanticFragmentVersion {
		t.Fatalf("semantic envelope = %v", fragment)
	}
	wantDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(fragment.GetDocument()))
	if fragment.GetDigest() != wantDigest {
		t.Fatalf("digest = %q, want %q", fragment.GetDigest(), wantDigest)
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
