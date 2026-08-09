package redis

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestRedisIntegrationLifecycleAndOperations(t *testing.T) {
	if os.Getenv("KUBLING_REDIS_INTEGRATION") == "" {
		t.Skip("set KUBLING_REDIS_INTEGRATION=1 with local Redis running")
	}
	config, err := LoadConfig("local/provider.integration.yaml")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	provider, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	client := newGRPCTestClient(t, provider)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	health, err := client.Health(ctx, &providerv1.HealthRequest{})
	if err != nil || !health.GetHealthy() {
		t.Fatalf("Health() = (%v, %v)", health, err)
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
		t.Fatal("OpenConnection() connection ID is empty")
	}

	typeRows := integrationRows(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       &providerv1.EntityReference{Name: "TYPE_SAMPLE", Namespace: "sample"},
	})
	if len(typeRows) != 1 || len(typeRows[0].GetValues()) != 22 {
		t.Fatalf("TYPE_SAMPLE rows = %d, values = %d", len(typeRows), len(typeRows[0].GetValues()))
	}

	id := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	insertTitle := "Redis integration " + id
	updatedTitle := "Redis verified " + id
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
			Fields: []*providerv1.Field{{Name: "id"}, {Name: "project_id"}, {Name: "title"}, {Name: "completed"}, {Name: "priority"}},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue(id), stringValue("project-1"), stringValue(insertTitle), booleanValue(false), integerValue(9),
			}}},
		},
	})
	if err != nil || inserted.GetAffectedRows() != 1 {
		t.Fatalf("Insert() = (%v, %v)", inserted, err)
	}
	updated, err := client.Update(ctx, &providerv1.UpdateRequest{
		ConnectionId: connectionID,
		Entity:       entity,
		Assignments: []*providerv1.Assignment{
			{Field: "title", Value: literalExpression(stringValue(updatedTitle))},
			{Field: "completed", Value: literalExpression(booleanValue(true))},
		},
		Filter: equalExpression("title", stringValue(insertTitle)),
	})
	if err != nil || updated.GetAffectedRows() != 1 {
		t.Fatalf("Update() = (%v, %v)", updated, err)
	}
	rows := integrationRows(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       entity,
		Projections: []*providerv1.Projection{
			{Expression: fieldExpression("id")},
			{Expression: fieldExpression("title")},
			{Expression: fieldExpression("completed")},
		},
		Filter: equalExpression("id", stringValue(id)),
	})
	if len(rows) != 1 || rows[0].GetValues()[1].GetStringValue() != updatedTitle ||
		!rows[0].GetValues()[2].GetBooleanValue() {
		t.Fatalf("Query() updated rows = %v", rows)
	}
	deleted, err := client.Delete(ctx, &providerv1.DeleteRequest{
		ConnectionId: connectionID,
		Entity:       entity,
		Filter:       equalExpression("title", stringValue(updatedTitle)),
	})
	if err != nil || deleted.GetAffectedRows() != 1 {
		t.Fatalf("Delete() = (%v, %v)", deleted, err)
	}
	if rows := integrationRows(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       entity,
		Filter:       equalExpression("id", stringValue(id)),
	}); len(rows) != 0 {
		t.Fatalf("Query() after delete rows = %v", rows)
	}
	if _, err := client.CloseConnection(ctx, &providerv1.CloseConnectionRequest{ConnectionId: connectionID}); err != nil {
		t.Fatalf("CloseConnection() error = %v", err)
	}
}

func integrationRows(
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
