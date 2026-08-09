package cassandra

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestCassandraIntegrationLifecycleAndOperations(t *testing.T) {
	if os.Getenv("KUBLING_CASSANDRA_INTEGRATION") == "" {
		t.Skip("set KUBLING_CASSANDRA_INTEGRATION=1 with local Cassandra running")
	}

	client := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capabilities, err := client.GetCapabilities(ctx, &providerv1.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetCapabilities() error = %v", err)
	}
	if !capabilities.GetMutations().GetInsert() ||
		!capabilities.GetMutations().GetUpdate() ||
		!capabilities.GetMutations().GetDelete() {
		t.Fatalf("GetCapabilities() mutations = %v", capabilities.GetMutations())
	}

	schema, err := client.GetSchema(ctx, &providerv1.GetSchemaRequest{})
	if err != nil {
		t.Fatalf("GetSchema() error = %v", err)
	}
	if schema.GetSchemaDdl() != "" || len(schema.GetMetadata().GetTables()) != 4 {
		t.Fatalf("GetSchema() = %v", schema)
	}

	opened, err := client.OpenConnection(ctx, &providerv1.OpenConnectionRequest{})
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	connectionID := opened.GetConnectionId()
	if connectionID == "" {
		t.Fatal("OpenConnection() connection id is empty")
	}

	typeRows := integrationQuery(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity: &providerv1.EntityReference{
			Name:      "TYPE_SAMPLE",
			Namespace: "sample",
		},
	})
	if len(typeRows) != 1 || len(typeRows[0].GetValues()) != 22 {
		t.Fatalf("Query(TYPE_SAMPLE) rows = %d, values = %d", len(typeRows), len(typeRows[0].GetValues()))
	}

	id := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	entity := &providerv1.EntityReference{Name: "TASK", Namespace: "sample"}
	t.Cleanup(func() {
		_, _ = client.Delete(context.Background(), &providerv1.DeleteRequest{
			ConnectionId: connectionID,
			Entity:       entity,
			Filter:       equalExpression("id", stringValue(id)),
		})
	})

	inserted, err := client.Insert(ctx, &providerv1.InsertRequest{
		ConnectionId: connectionID,
		Entity:       entity,
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{{Name: "id"}, {Name: "title"}, {Name: "priority"}},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue(id),
				stringValue("Cassandra integration"),
				integerProviderValue(9),
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if inserted.GetAffectedRows() != 1 {
		t.Fatalf("Insert() affected rows = %d, want 1", inserted.GetAffectedRows())
	}

	_, err = client.Update(ctx, &providerv1.UpdateRequest{
		ConnectionId: connectionID,
		Entity:       entity,
		Assignments: []*providerv1.Assignment{
			{Field: "title", Value: literalExpression(stringValue("Cassandra verified"))},
			{Field: "completed", Value: literalExpression(booleanValue(true))},
		},
		Filter: equalExpression("id", stringValue(id)),
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	rows := integrationQuery(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       entity,
		Projections: []*providerv1.Projection{
			{Expression: fieldExpression("id")},
			{Expression: fieldExpression("title")},
			{Expression: fieldExpression("completed")},
		},
		Filter: equalExpression("id", stringValue(id)),
	})
	if len(rows) != 1 || rows[0].GetValues()[1].GetStringValue() != "Cassandra verified" ||
		!rows[0].GetValues()[2].GetBooleanValue() {
		t.Fatalf("Query() updated rows = %v", rows)
	}

	if _, err := client.Delete(ctx, &providerv1.DeleteRequest{
		ConnectionId: connectionID,
		Entity:       entity,
		Filter:       equalExpression("id", stringValue(id)),
	}); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if rows := integrationQuery(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       entity,
		Filter:       equalExpression("id", stringValue(id)),
	}); len(rows) != 0 {
		t.Fatalf("Query() rows after delete = %d, want 0", len(rows))
	}

	if _, err := client.CloseConnection(ctx, &providerv1.CloseConnectionRequest{
		ConnectionId: connectionID,
	}); err != nil {
		t.Fatalf("CloseConnection() error = %v", err)
	}
}

func newIntegrationClient(t *testing.T) providerv1.ProviderServiceClient {
	t.Helper()

	config, err := LoadConfig("local/provider.yaml")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	provider, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	service := providersdk.NewServer(provider)
	providerv1.RegisterProviderServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()

	connection, err := grpc.NewClient(
		"passthrough:///cassandra-integration",
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

func integrationQuery(
	t *testing.T,
	client providerv1.ProviderServiceClient,
	request *providerv1.QueryRequest,
) []*providerv1.Tuple {
	t.Helper()

	stream, err := client.Query(context.Background(), request)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	var rows []*providerv1.Tuple
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			return rows
		}
		if err != nil {
			t.Fatalf("Query().Recv() error = %v", err)
		}
		rows = append(rows, response.GetBatch().GetTuples()...)
	}
}
