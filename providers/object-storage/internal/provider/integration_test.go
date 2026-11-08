package provider

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend/azureblobstore"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend/fsstore"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend/gcsstore"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend/s3store"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestS3IntegrationLifecycleAndLobStreaming(t *testing.T) {
	if os.Getenv("KUBLING_OBJECT_STORAGE_INTEGRATION") != "1" {
		t.Skip("set KUBLING_OBJECT_STORAGE_INTEGRATION=1 to run the S3 integration test")
	}
	configuration, err := config.Load("../../local/provider.yaml")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	compiled, err := catalog.Compile(configuration)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	store, err := s3store.New(context.Background(), configuration.Backends["local"])
	if err != nil {
		t.Fatalf("s3store.New() error = %v", err)
	}
	implementation, err := New(
		configuration,
		compiled,
		map[string]backend.Store{"local": store},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	exerciseIntegrationLifecycle(
		t,
		compiled,
		store,
		implementation,
		fmt.Sprintf("integration/%d.bin", time.Now().UnixNano()),
	)
}

func TestFilesystemIntegrationLifecycleAndLobStreaming(t *testing.T) {
	configuration := config.Config{
		Lobs: config.Lobs{
			MaxBytes:      1024 * 1024,
			MaxChunkBytes: 64 * 1024,
			Retention:     time.Minute,
		},
		Backends: map[string]config.Backend{
			"local": {
				Type:              "filesystem",
				Root:              filepath.Join(t.TempDir(), "objects"),
				CreateDirectories: true,
			},
		},
		Buckets: map[string]config.Bucket{
			"media": {
				Backend: "local",
				Name:    "media",
				Prefix:  "objects/",
			},
		},
	}
	compiled, err := catalog.Compile(configuration)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	store, err := fsstore.New(configuration.Backends["local"])
	if err != nil {
		t.Fatalf("fsstore.New() error = %v", err)
	}
	implementation, err := New(
		configuration,
		compiled,
		map[string]backend.Store{"local": store},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	exerciseIntegrationLifecycle(t, compiled, store, implementation, "integration/local.bin")
}

func TestAzureBlobIntegrationLifecycleAndLobStreaming(t *testing.T) {
	if os.Getenv("KUBLING_OBJECT_STORAGE_AZURE_INTEGRATION") != "1" {
		t.Skip("set KUBLING_OBJECT_STORAGE_AZURE_INTEGRATION=1 to run the Azure Blob integration test")
	}
	configuration, err := config.Load("../../local/azure.yaml")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	compiled, err := catalog.Compile(configuration)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	store, err := azureblobstore.New(configuration.Backends["local"])
	if err != nil {
		t.Fatalf("azureblobstore.New() error = %v", err)
	}
	implementation, err := New(
		configuration,
		compiled,
		map[string]backend.Store{"local": store},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	exerciseIntegrationLifecycle(
		t,
		compiled,
		store,
		implementation,
		fmt.Sprintf("integration/azure-%d.bin", time.Now().UnixNano()),
	)
}

func TestGCSIntegrationLifecycleAndLobStreaming(t *testing.T) {
	if os.Getenv("KUBLING_OBJECT_STORAGE_GCS_INTEGRATION") != "1" {
		t.Skip("set KUBLING_OBJECT_STORAGE_GCS_INTEGRATION=1 to run the GCS integration test")
	}
	configuration, err := config.Load("../../local/gcs.yaml")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	compiled, err := catalog.Compile(configuration)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	store, err := gcsstore.New(context.Background(), configuration.Backends["local"])
	if err != nil {
		t.Fatalf("gcsstore.New() error = %v", err)
	}
	implementation, err := New(
		configuration,
		compiled,
		map[string]backend.Store{"local": store},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	exerciseIntegrationLifecycle(
		t,
		compiled,
		store,
		implementation,
		fmt.Sprintf("integration/gcs-%d.bin", time.Now().UnixNano()),
	)
}

func exerciseIntegrationLifecycle(
	t *testing.T,
	compiled *catalog.Catalog,
	store backend.Store,
	implementation *Provider,
	key string,
) {
	t.Helper()
	client := integrationClient(t, implementation)

	health, err := client.Health(context.Background(), &providerv1.HealthRequest{})
	if err != nil || !health.GetHealthy() {
		t.Fatalf("Health() = %+v, %v", health, err)
	}
	opened, err := client.OpenConnection(context.Background(), &providerv1.OpenConnectionRequest{})
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	connectionID := opened.GetConnectionId()
	content := []byte("streamed through the provider LOB contract")
	inserted, err := client.Insert(context.Background(), &providerv1.InsertRequest{
		ConnectionId: connectionID,
		Entity:       entity(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{
				{Name: catalog.KeyColumn},
				{Name: catalog.ContentColumn},
				{Name: catalog.ContentTypeColumn},
			},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue(key),
				blobValue(content),
				stringValue("application/octet-stream"),
			}}},
		},
	})
	if err != nil || inserted.GetAffectedRows() != 1 {
		t.Fatalf("Insert() = %+v, %v", inserted, err)
	}
	_, err = client.Insert(context.Background(), &providerv1.InsertRequest{
		ConnectionId: connectionID,
		Entity:       entity(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{{Name: catalog.KeyColumn}, {Name: catalog.ContentColumn}},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue(key), blobValue([]byte("duplicate")),
			}}},
		},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate Insert() error = %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.Delete(context.Background(), &providerv1.DeleteRequest{
			ConnectionId: connectionID,
			Entity:       entity(),
			Filter:       keyFilter(key),
		})
	})

	query, err := client.Query(context.Background(), &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       entity(),
		Projections:  []*providerv1.Projection{fieldProjection(catalog.ContentColumn)},
		Filter:       keyFilter(key),
		AcceptedFeatures: []string{
			grpcfeatures.LobReadV1,
		},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	response, err := query.Recv()
	if err != nil {
		t.Fatalf("Query().Recv() error = %v", err)
	}
	reference := response.GetBatch().GetTuples()[0].GetValues()[0].GetLobReference()
	if reference == nil || reference.GetSessionId() != connectionID {
		t.Fatalf("LOB reference = %+v", reference)
	}
	lob, err := client.ReadLob(context.Background(), &providerv1.ReadLobRequest{
		ConnectionId:  connectionID,
		LobId:         reference.GetLobId(),
		MaxChunkBytes: 7,
	})
	if err != nil {
		t.Fatalf("ReadLob() error = %v", err)
	}
	var received bytes.Buffer
	ended := false
	for {
		chunk, err := lob.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadLob().Recv() error = %v", err)
		}
		received.Write(chunk.GetData())
		ended = ended || chunk.GetEndOfRead()
	}
	if !ended || !bytes.Equal(received.Bytes(), content) {
		t.Fatalf("LOB read = %q, ended=%t", received.Bytes(), ended)
	}
	if _, err := client.ReleaseLob(context.Background(), &providerv1.ReleaseLobRequest{
		ConnectionId: connectionID,
		LobId:        reference.GetLobId(),
	}); err != nil {
		t.Fatalf("ReleaseLob() error = %v", err)
	}
	updatedContent := []byte("replacement content")
	updated, err := client.Update(context.Background(), &providerv1.UpdateRequest{
		ConnectionId: connectionID,
		Entity:       entity(),
		Assignments: []*providerv1.Assignment{{
			Field: catalog.ContentColumn,
			Value: literal(blobValue(updatedContent)),
		}},
		Filter: keyFilter(key),
	})
	if err != nil || updated.GetAffectedRows() != 1 {
		t.Fatalf("Update() = %+v, %v", updated, err)
	}
	route, found := compiled.Resolve("media", catalog.ObjectsTable)
	if !found {
		t.Fatal("compiled media route is missing")
	}
	reader, err := store.Open(context.Background(), route.Target, backend.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Open() updated object error = %v", err)
	}
	updatedBytes, readErr := io.ReadAll(reader.Body)
	closeErr := reader.Body.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(updatedBytes, updatedContent) ||
		reader.Object.ContentType != "application/octet-stream" {
		t.Fatalf(
			"updated object = %q type=%q, read=%v close=%v",
			updatedBytes,
			reader.Object.ContentType,
			readErr,
			closeErr,
		)
	}
	attributesUpdated, err := client.Update(context.Background(), &providerv1.UpdateRequest{
		ConnectionId: connectionID,
		Entity:       entity(),
		Assignments: []*providerv1.Assignment{{
			Field: catalog.MetadataColumn,
			Value: literal(&kublingv1.Value{Kind: &kublingv1.Value_JsonValue{
				JsonValue: `{"lifecycle":"verified"}`,
			}}),
		}},
		Filter: keyFilter(key),
	})
	if err != nil || attributesUpdated.GetAffectedRows() != 1 {
		t.Fatalf("metadata Update() = %+v, %v", attributesUpdated, err)
	}
	attributesUpdated, err = client.Update(context.Background(), &providerv1.UpdateRequest{
		ConnectionId: connectionID,
		Entity:       entity(),
		Assignments: []*providerv1.Assignment{{
			Field: catalog.CacheControlColumn,
			Value: literal(stringValue("public, max-age=60")),
		}},
		Filter: keyFilter(key),
	})
	if err != nil || attributesUpdated.GetAffectedRows() != 1 {
		t.Fatalf("property Update() = %+v, %v", attributesUpdated, err)
	}
	reader, err = store.Open(context.Background(), route.Target, backend.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Open() attribute-updated object error = %v", err)
	}
	attributeUpdatedBytes, readErr := io.ReadAll(reader.Body)
	closeErr = reader.Body.Close()
	attributes, statErr := store.Stat(context.Background(), route.Target, key)
	if readErr != nil || closeErr != nil || statErr != nil ||
		!bytes.Equal(attributeUpdatedBytes, updatedContent) ||
		attributes.CacheControl != "public, max-age=60" ||
		attributes.Metadata["lifecycle"] != "verified" {
		t.Fatalf(
			"attribute-updated object = %q cache=%q metadata=%v, read=%v close=%v stat=%v",
			attributeUpdatedBytes,
			attributes.CacheControl,
			attributes.Metadata,
			readErr,
			closeErr,
			statErr,
		)
	}
	deleted, err := client.Delete(context.Background(), &providerv1.DeleteRequest{
		ConnectionId: connectionID,
		Entity:       entity(),
		Filter:       keyFilter(key),
	})
	if err != nil || deleted.GetAffectedRows() != 1 {
		t.Fatalf("Delete() = %+v, %v", deleted, err)
	}
}

func integrationClient(t *testing.T, implementation *Provider) providerv1.ProviderServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	service := providersdk.NewServer(implementation)
	server := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(server, service)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	connection, err := grpc.NewClient(
		"passthrough:///object-storage-integration",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		_ = service.Close(context.Background())
		server.Stop()
		_ = listener.Close()
		<-serveDone
		_ = implementation.Close()
	})
	return providerv1.NewProviderServiceClient(connection)
}
