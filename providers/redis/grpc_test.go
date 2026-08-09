package redis

import (
	"context"
	"io"
	"net"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCProviderLifecycleAndQueryEnvelope(t *testing.T) {
	provider := testProvider(t, map[string]*fakeRedisClient{testNamespace: seededFakeRedisClient()})
	client := newGRPCTestClient(t, provider)

	health, err := client.Health(context.Background(), &providerv1.HealthRequest{})
	if err != nil || !health.GetHealthy() {
		t.Fatalf("Health() = (%v, %v)", health, err)
	}
	schema, err := client.GetSchema(context.Background(), &providerv1.GetSchemaRequest{})
	if err != nil {
		t.Fatalf("GetSchema() error = %v", err)
	}
	if schema.GetSchemaDdl() != "" || len(schema.GetMetadata().GetTables()) != 1 {
		t.Fatalf("GetSchema() = %v", schema)
	}
	opened, err := client.OpenConnection(context.Background(), &providerv1.OpenConnectionRequest{})
	if err != nil || opened.GetConnectionId() == "" {
		t.Fatalf("OpenConnection() = (%v, %v)", opened, err)
	}

	stream, err := client.Query(context.Background(), &providerv1.QueryRequest{
		ConnectionId: opened.GetConnectionId(),
		Entity:       taskReference(),
		Filter:       equalExpression("id", stringValue("task-1")),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Query().Recv() error = %v", err)
	}
	if response.GetBatch() == nil || len(response.GetBatch().GetTuples()) != 1 {
		t.Fatalf("Query() response = %v", response)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("terminal Query().Recv() error = %v, want EOF", err)
	}
	if _, err := client.CloseConnection(context.Background(), &providerv1.CloseConnectionRequest{
		ConnectionId: opened.GetConnectionId(),
	}); err != nil {
		t.Fatalf("CloseConnection() error = %v", err)
	}
}

func newGRPCTestClient(t *testing.T, provider *Provider) providerv1.ProviderServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	service := providersdk.NewServer(provider)
	providerv1.RegisterProviderServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()

	connection, err := grpc.NewClient(
		"passthrough:///redis-provider-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		_ = service.Close(context.Background())
		grpcServer.Stop()
		_ = listener.Close()
	})
	return providerv1.NewProviderServiceClient(connection)
}
