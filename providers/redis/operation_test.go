package redis

import (
	"context"
	"errors"
	"io"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQuerySelectsByKeyPatternFiltersOrdersAndBatches(t *testing.T) {
	client := seededFakeRedisClient()
	connection := testConnection(t, client)
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	batchSize := uint32(1)
	limit := uint64(2)

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: taskReference(),
		Projections: []*providerv1.Projection{
			{Expression: fieldExpression("id")},
			{Expression: fieldExpression("title")},
			{Expression: fieldExpression("priority")},
		},
		Filter: andExpression(
			likeExpression("id", "task-%"),
			comparisonExpression(
				providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL,
				"priority",
				integerValue(2),
			),
		),
		OrderBy: []*providerv1.OrderBy{{
			Expression: fieldExpression("priority"),
			Direction:  providerv1.SortDirection_SORT_DIRECTION_DESCENDING,
		}},
		Limit:     &limit,
		BatchSize: &batchSize,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	first, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	second, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	if got := first.GetTuples()[0].GetValues()[0].GetStringValue(); got != "task-3" {
		t.Fatalf("first id = %q, want task-3", got)
	}
	if got := second.GetTuples()[0].GetValues()[0].GetStringValue(); got != "task-2" {
		t.Fatalf("second id = %q, want task-2", got)
	}
	if batch, err := stream.Next(context.Background()); batch != nil || !errors.Is(err, io.EOF) {
		t.Fatalf("terminal Next() = (%v, %v), want (nil, EOF)", batch, err)
	}
	if client.scanCalls.Load() != 1 {
		t.Fatalf("Scan() calls = %d, want 1", client.scanCalls.Load())
	}
}

func TestQueryKeepsExplicitNullOrderingWhenDescending(t *testing.T) {
	connection := testConnection(t, seededFakeRedisClient())
	t.Cleanup(func() { _ = connection.Close(context.Background()) })

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity:      taskReference(),
		Projections: []*providerv1.Projection{{Expression: fieldExpression("id")}},
		Filter:      likeExpression("id", "task-%"),
		OrderBy: []*providerv1.OrderBy{{
			Expression:   fieldExpression("note"),
			Direction:    providerv1.SortDirection_SORT_DIRECTION_DESCENDING,
			NullOrdering: providerv1.NullOrdering_NULL_ORDERING_FIRST,
		}},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	got := []string{
		batch.GetTuples()[0].GetValues()[0].GetStringValue(),
		batch.GetTuples()[1].GetValues()[0].GetStringValue(),
		batch.GetTuples()[2].GetValues()[0].GetStringValue(),
	}
	if got[0] != "task-2" || got[1] != "task-3" || got[2] != "task-1" {
		t.Fatalf("ordered ids = %v, want [task-2 task-3 task-1]", got)
	}
}

func TestQueryUsesPrefixBoundedScanWithoutAKeyConstraint(t *testing.T) {
	client := seededFakeRedisClient()
	connection := testConnection(t, client)
	t.Cleanup(func() { _ = connection.Close(context.Background()) })

	rows := collectRows(t, connection, &providerv1.QueryRequest{Entity: taskReference()})
	if got := rowIDs(rows); !equalStrings(got, []string{"task-1", "task-2", "task-3"}) {
		t.Fatalf("unfiltered Query() ids = %v", got)
	}
	rows = collectRows(t, connection, &providerv1.QueryRequest{
		Entity: taskReference(),
		Filter: &providerv1.Expression{Kind: &providerv1.Expression_Logical{
			Logical: &providerv1.LogicalExpression{
				Operator: providerv1.LogicalOperator_LOGICAL_OPERATOR_OR,
				Operands: []*providerv1.Expression{
					equalExpression("id", stringValue("task-1")),
					equalExpression("priority", integerValue(2)),
				},
			},
		}},
	})
	if got := rowIDs(rows); !equalStrings(got, []string{"task-1", "task-2"}) {
		t.Fatalf("mixed OR Query() ids = %v", got)
	}
	rows = collectRows(t, connection, &providerv1.QueryRequest{
		Entity: taskReference(),
		Filter: comparisonExpression(
			providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL,
			"priority",
			integerValue(2),
		),
	})
	if got := rowIDs(rows); !equalStrings(got, []string{"task-2", "task-3"}) {
		t.Fatalf("non-key Query() ids = %v", got)
	}
	if client.scanCalls.Load() != 3 {
		t.Fatalf("Scan() calls = %d, want 3", client.scanCalls.Load())
	}
}

func TestQueryEnforcesMaxScannedKeys(t *testing.T) {
	connection := testConnection(t, seededFakeRedisClient())
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	namespace := connection.provider.config.Namespaces[testNamespace]
	namespace.MaxScannedKeys = 2
	connection.provider.config.Namespaces[testNamespace] = namespace

	_, err := connection.Query(context.Background(), &providerv1.QueryRequest{Entity: taskReference()})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Query() code = %v, want %v", status.Code(err), codes.ResourceExhausted)
	}
}

func TestNonKeyLikeFallsBackToTableScanWithNumericKey(t *testing.T) {
	table := &TableConfig{
		Key: ColumnConfig{Name: "id", Type: kublingv1.ValueType_VALUE_TYPE_LONG},
	}
	selectors, covered, err := keySelectors(table, likeExpression("status", "PENDING%"))
	if err != nil {
		t.Fatalf("keySelectors() error = %v", err)
	}
	if covered || len(selectors) != 0 {
		t.Fatalf("keySelectors() = (%v, %v), want table-scan fallback", selectors, covered)
	}
}

func TestHashMutationsRoundTrip(t *testing.T) {
	client := seededFakeRedisClient()
	connection := testConnection(t, client)
	t.Cleanup(func() { _ = connection.Close(context.Background()) })

	inserted, err := connection.Insert(context.Background(), &providerv1.InsertRequest{
		Entity: taskReference(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{{Name: "id"}, {Name: "title"}, {Name: "completed"}, {Name: "priority"}},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue("task-4"),
				stringValue("Ship Redis provider"),
				booleanValue(false),
				integerValue(4),
			}}},
		},
	})
	if err != nil || inserted.GetAffectedRows() != 1 {
		t.Fatalf("Insert() = (%v, %v), want affected 1", inserted, err)
	}
	_, err = connection.Insert(context.Background(), &providerv1.InsertRequest{
		Entity: taskReference(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{{Name: "id"}, {Name: "title"}},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue("task-4"), stringValue("Duplicate"),
			}}},
		},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate Insert() code = %v, want %v", status.Code(err), codes.AlreadyExists)
	}

	updated, err := connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity: taskReference(),
		Assignments: []*providerv1.Assignment{
			{Field: "title", Value: literalExpression(stringValue("Redis verified"))},
			{Field: "completed", Value: literalExpression(booleanValue(true))},
			{Field: "note", Value: literalExpression(nullValue())},
		},
		Filter: equalExpression("id", stringValue("task-4")),
	})
	if err != nil || updated.GetAffectedRows() != 1 {
		t.Fatalf("Update() = (%v, %v), want affected 1", updated, err)
	}
	rows := collectRows(t, connection, queryByID("task-4"))
	if len(rows) != 1 || rows[0].GetValues()[1].GetStringValue() != "Redis verified" ||
		!rows[0].GetValues()[2].GetBooleanValue() || rows[0].GetValues()[4].GetNullValue() == nil {
		t.Fatalf("Query() updated row = %v", rows)
	}

	deleted, err := connection.Delete(context.Background(), &providerv1.DeleteRequest{
		Entity: taskReference(),
		Filter: equalExpression("id", stringValue("task-4")),
	})
	if err != nil || deleted.GetAffectedRows() != 1 {
		t.Fatalf("Delete() = (%v, %v), want affected 1", deleted, err)
	}
	if rows := collectRows(t, connection, queryByID("task-4")); len(rows) != 0 {
		t.Fatalf("Query() after delete rows = %v", rows)
	}
}

func TestMutationsAllowNonKeyFiltersThroughBoundedScan(t *testing.T) {
	client := seededFakeRedisClient()
	connection := testConnection(t, client)
	t.Cleanup(func() { _ = connection.Close(context.Background()) })

	updated, err := connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity: taskReference(),
		Assignments: []*providerv1.Assignment{{
			Field: "note",
			Value: literalExpression(stringValue("selected")),
		}},
		Filter: comparisonExpression(
			providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL,
			"priority",
			integerValue(2),
		),
	})
	if err != nil || updated.GetAffectedRows() != 2 {
		t.Fatalf("Update() = (%v, %v), want affected 2", updated, err)
	}
	if client.hashes["TASK:task-1"]["note"] != "alpha" ||
		client.hashes["TASK:task-2"]["note"] != "selected" ||
		client.hashes["TASK:task-3"]["note"] != "selected" {
		t.Fatalf("updated hashes = %v", client.hashes)
	}

	deleted, err := connection.Delete(context.Background(), &providerv1.DeleteRequest{
		Entity: taskReference(),
		Filter: equalExpression("priority", integerValue(1)),
	})
	if err != nil || deleted.GetAffectedRows() != 1 {
		t.Fatalf("Delete() = (%v, %v), want affected 1", deleted, err)
	}
	if _, exists := client.hashes["TASK:task-1"]; exists {
		t.Fatal("Delete() retained TASK:task-1")
	}
	if client.scanCalls.Load() != 2 {
		t.Fatalf("Scan() calls = %d, want 2", client.scanCalls.Load())
	}
}

func TestMutationsRejectUnsafeRequests(t *testing.T) {
	connection := testConnection(t, seededFakeRedisClient())
	t.Cleanup(func() { _ = connection.Close(context.Background()) })

	_, err := connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity:      taskReference(),
		Assignments: []*providerv1.Assignment{{Field: "title", Value: literalExpression(stringValue("unsafe"))}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Update() without filter code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	_, err = connection.Delete(context.Background(), &providerv1.DeleteRequest{Entity: taskReference()})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Delete() without filter code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	_, err = connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity:      taskReference(),
		Assignments: []*providerv1.Assignment{{Field: "id", Value: literalExpression(stringValue("changed"))}},
		Filter:      equalExpression("id", stringValue("task-1")),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Update() key assignment code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	_, err = connection.Insert(context.Background(), &providerv1.InsertRequest{
		Entity: taskReference(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{{Name: "id"}, {Name: "completed"}},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue("incomplete"), booleanValue(false),
			}}},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("incomplete Insert() code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func seededFakeRedisClient() *fakeRedisClient {
	client := newFakeRedisClient()
	client.hashes["TASK:task-1"] = map[string]string{
		"title": "Review SDK", "completed": "false", "priority": "1", "note": "alpha",
	}
	client.hashes["TASK:task-2"] = map[string]string{
		"title": "Connect engine", "completed": "true", "priority": "2",
	}
	client.hashes["TASK:task-3"] = map[string]string{
		"title": "Validate Redis", "completed": "false", "priority": "3",
	}
	client.otherTypes["TASK:not-a-hash"] = "string"
	return client
}

func queryByID(id string) *providerv1.QueryRequest {
	return &providerv1.QueryRequest{Entity: taskReference(), Filter: equalExpression("id", stringValue(id))}
}

func rowIDs(rows []*providerv1.Tuple) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.GetValues()[0].GetStringValue())
	}
	return ids
}

func equalStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func collectRows(t *testing.T, connection *Connection, request *providerv1.QueryRequest) []*providerv1.Tuple {
	t.Helper()
	stream, err := connection.Query(context.Background(), request)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer stream.Close()
	var rows []*providerv1.Tuple
	for {
		batch, err := stream.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return rows
		}
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		rows = append(rows, batch.GetTuples()...)
	}
}
