package cassandra

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/cassandra-gocql-driver/v2"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const operationTestNamespace = "some/path/to/resource"

type recordedQuery struct {
	statement string
	values    []any
	pageSize  int
}

type recordedExec struct {
	statement string
	values    []any
}

type recordingSession struct {
	mu         sync.Mutex
	metadata   *gocql.KeyspaceMetadata
	queries    []recordedQuery
	execs      []recordedExec
	iterators  []driverIterator
	execErrors []error
	closeCount atomic.Int32
}

func (s *recordingSession) Close() {
	s.closeCount.Add(1)
}

func (s *recordingSession) KeyspaceMetadata(
	string,
) (*gocql.KeyspaceMetadata, error) {
	return s.metadata, nil
}

func (s *recordingSession) Query(
	_ context.Context,
	statement string,
	values []any,
	pageSize int,
) driverIterator {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.queries = append(s.queries, recordedQuery{
		statement: statement,
		values:    append([]any(nil), values...),
		pageSize:  pageSize,
	})
	if len(s.iterators) == 0 {
		return &recordingIterator{}
	}
	iterator := s.iterators[0]
	s.iterators = s.iterators[1:]
	return iterator
}

func (s *recordingSession) Exec(
	_ context.Context,
	statement string,
	values []any,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.execs = append(s.execs, recordedExec{
		statement: statement,
		values:    append([]any(nil), values...),
	})
	if len(s.execErrors) == 0 {
		return nil
	}
	err := s.execErrors[0]
	s.execErrors = s.execErrors[1:]
	return err
}

type recordingIterator struct {
	rows       []map[string]any
	index      int
	closeErr   error
	closeCount atomic.Int32
}

type pointerScanningIterator struct {
	t          *testing.T
	types      map[string]gocql.TypeInfo
	rows       []map[string]any
	index      int
	closeCount atomic.Int32
}

func (*recordingIterator) Columns() []gocql.ColumnInfo { return nil }

func (i *recordingIterator) MapScan(destination map[string]any) bool {
	if i.index >= len(i.rows) {
		return false
	}
	for name, value := range i.rows[i.index] {
		destination[name] = value
	}
	i.index++
	return true
}

func (i *recordingIterator) Close() error {
	i.closeCount.Add(1)
	return i.closeErr
}

func (*pointerScanningIterator) Columns() []gocql.ColumnInfo { return nil }

func (i *pointerScanningIterator) MapScan(destination map[string]any) bool {
	if i.index >= len(i.rows) {
		return false
	}
	for name, typeInfo := range i.types {
		target, exists := destination[name]
		if !exists {
			i.t.Fatalf("scan destination %q is missing", name)
		}
		var encoded []byte
		value := i.rows[i.index][name]
		if value != nil {
			var err error
			encoded, err = gocql.Marshal(typeInfo, value)
			if err != nil {
				i.t.Fatalf("marshal %q: %v", name, err)
			}
		}
		if err := gocql.Unmarshal(typeInfo, encoded, target); err != nil {
			i.t.Fatalf("unmarshal %q: %v", name, err)
		}
		destination[name] = reflect.Indirect(reflect.ValueOf(target)).Interface()
	}
	i.index++
	return true
}

func (i *pointerScanningIterator) Close() error {
	i.closeCount.Add(1)
	return nil
}

func TestQueryTranslatesAndStreamsRows(t *testing.T) {
	iterator := &recordingIterator{rows: []map[string]any{
		{"id": "task-1", "title": "Review SDK"},
		{"id": "task-2", "title": "Connect engine"},
	}}
	session, connection := operationConnection(t, iterator)
	batchSize := uint32(1)
	limit := uint64(2)

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: taskReference(),
		Projections: []*providerv1.Projection{
			{Expression: fieldExpression("id"), OutputName: "task_id"},
			{Expression: fieldExpression("title")},
		},
		Filter:    equalExpression("id", stringValue("task-1")),
		Limit:     &limit,
		BatchSize: &batchSize,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(session.queries) != 1 {
		t.Fatalf("Query() driver calls = %d, want 1", len(session.queries))
	}
	wantStatement := `SELECT "id", "title" FROM "kubling_sample"."TASK" WHERE "id" = ? LIMIT 2`
	if got := session.queries[0].statement; got != wantStatement {
		t.Fatalf("Query() statement = %q, want %q", got, wantStatement)
	}
	if !reflect.DeepEqual(session.queries[0].values, []any{"task-1"}) {
		t.Fatalf("Query() values = %#v, want [task-1]", session.queries[0].values)
	}
	if session.queries[0].pageSize != 1 {
		t.Fatalf("Query() page size = %d, want 1", session.queries[0].pageSize)
	}

	first, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	if len(first.GetFields()) != 2 || first.GetFields()[0].GetName() != "task_id" {
		t.Fatalf("first Next() fields = %v", first.GetFields())
	}
	if got := first.GetTuples()[0].GetValues()[0].GetStringValue(); got != "task-1" {
		t.Fatalf("first Next() id = %q, want task-1", got)
	}

	second, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	if got := second.GetTuples()[0].GetValues()[1].GetStringValue(); got != "Connect engine" {
		t.Fatalf("second Next() title = %q, want Connect engine", got)
	}
	if batch, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) || batch != nil {
		t.Fatalf("terminal Next() = (%v, %v), want (nil, EOF)", batch, err)
	}
	if iterator.closeCount.Load() != 1 {
		t.Fatalf("iterator Close() calls = %d, want 1", iterator.closeCount.Load())
	}
	if session.closeCount.Load() != 0 {
		t.Fatalf("session Close() before connection close = %d, want 0", session.closeCount.Load())
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("stream Close() error = %v", err)
	}
	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("connection Close() error = %v", err)
	}
	if session.closeCount.Load() != 1 {
		t.Fatalf("session Close() calls = %d, want 1", session.closeCount.Load())
	}
}

func TestResultStreamPreservesCassandraNullsAndZeroValues(t *testing.T) {
	collectionType := gocql.NewNativeType(4, gocql.TypeCustom, "map<text,text>")
	types := map[string]gocql.TypeInfo{
		"title":     gocql.NewNativeType(4, gocql.TypeText, ""),
		"completed": gocql.NewNativeType(4, gocql.TypeBoolean, ""),
		"priority":  gocql.NewNativeType(4, gocql.TypeInt, ""),
		"labels":    collectionType,
	}
	projections := []projectionPlan{
		{column: &gocql.ColumnMetadata{Name: "title", Type: types["title"]}, outputName: "title"},
		{column: &gocql.ColumnMetadata{Name: "completed", Type: types["completed"]}, outputName: "completed"},
		{column: &gocql.ColumnMetadata{Name: "priority", Type: types["priority"]}, outputName: "priority"},
		{column: &gocql.ColumnMetadata{Name: "labels", Type: collectionType}, outputName: "labels"},
	}
	iterator := &pointerScanningIterator{t: t, types: types, rows: []map[string]any{
		{"title": nil, "completed": nil, "priority": nil, "labels": nil},
		{"title": "", "completed": false, "priority": 0, "labels": map[string]string{}},
	}}
	stream := newCassandraResultStream(
		&resolvedEntity{},
		iterator,
		projections,
		2,
	)

	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if len(batch.GetTuples()) != 2 {
		t.Fatalf("Next() tuples = %d, want 2", len(batch.GetTuples()))
	}
	for index, value := range batch.GetTuples()[0].GetValues() {
		if value.GetNullValue() == nil {
			t.Fatalf("null tuple value %d = %T, want null", index, value.GetKind())
		}
	}

	zeroValues := batch.GetTuples()[1].GetValues()
	if _, ok := zeroValues[0].GetKind().(*kublingv1.Value_StringValue); !ok {
		t.Fatalf("empty string kind = %T, want string", zeroValues[0].GetKind())
	}
	if _, ok := zeroValues[1].GetKind().(*kublingv1.Value_BooleanValue); !ok {
		t.Fatalf("false kind = %T, want boolean", zeroValues[1].GetKind())
	}
	if _, ok := zeroValues[2].GetKind().(*kublingv1.Value_IntegerValue); !ok {
		t.Fatalf("zero integer kind = %T, want integer", zeroValues[2].GetKind())
	}
	if got := zeroValues[3].GetJsonValue(); got != "{}" {
		t.Fatalf("empty map JSON = %q, want {}", got)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if iterator.closeCount.Load() != 1 {
		t.Fatalf("iterator Close() calls = %d, want 1", iterator.closeCount.Load())
	}
}

func TestQueryRejectsUnsupportedShape(t *testing.T) {
	_, connection := operationConnection(t)
	offset := uint64(1)

	_, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: taskReference(),
		Offset: &offset,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Query() offset code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}

	_, err = connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: taskReference(),
		Filter: &providerv1.Expression{Kind: &providerv1.Expression_Logical{
			Logical: &providerv1.LogicalExpression{
				Operator: providerv1.LogicalOperator_LOGICAL_OPERATOR_OR,
				Operands: []*providerv1.Expression{
					equalExpression("id", stringValue("task-1")),
					equalExpression("id", stringValue("task-2")),
				},
			},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Query() OR code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func TestResultStreamMapsIteratorCloseFailure(t *testing.T) {
	closeFailure := errors.New("read timeout")
	iterator := &recordingIterator{closeErr: closeFailure}
	_, connection := operationConnection(t, iterator)

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: taskReference(),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if code := status.Code(stream.Close()); code != codes.Unavailable {
		t.Fatalf("stream Close() code = %v, want %v", code, codes.Unavailable)
	}
}

func TestMutationsTranslateToParameterizedCQL(t *testing.T) {
	session, connection := operationConnection(t)

	insert, err := connection.Insert(context.Background(), &providerv1.InsertRequest{
		Entity: taskReference(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{{Name: "id"}, {Name: "title"}, {Name: "priority"}},
			Tuples: []*providerv1.Tuple{
				{Values: []*kublingv1.Value{stringValue("task-4"), stringValue("Ship"), integerProviderValue(4)}},
				{Values: []*kublingv1.Value{stringValue("task-5"), stringValue("Verify"), integerProviderValue(5)}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if insert.GetAffectedRows() != 2 {
		t.Fatalf("Insert() affected rows = %d, want 2", insert.GetAffectedRows())
	}

	_, err = connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity: taskReference(),
		Assignments: []*providerv1.Assignment{
			{Field: "title", Value: literalExpression(stringValue("Shipped"))},
			{Field: "completed", Value: literalExpression(booleanValue(true))},
		},
		Filter: equalExpression("id", stringValue("task-4")),
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	_, err = connection.Delete(context.Background(), &providerv1.DeleteRequest{
		Entity: taskReference(),
		Filter: equalExpression("id", stringValue("task-5")),
	})
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	want := []recordedExec{
		{
			statement: `INSERT INTO "kubling_sample"."TASK" ("id", "title", "priority") VALUES (?, ?, ?)`,
			values:    []any{"task-4", "Ship", int32(4)},
		},
		{
			statement: `INSERT INTO "kubling_sample"."TASK" ("id", "title", "priority") VALUES (?, ?, ?)`,
			values:    []any{"task-5", "Verify", int32(5)},
		},
		{
			statement: `UPDATE "kubling_sample"."TASK" SET "title" = ?, "completed" = ? WHERE "id" = ?`,
			values:    []any{"Shipped", true, "task-4"},
		},
		{
			statement: `DELETE FROM "kubling_sample"."TASK" WHERE "id" = ?`,
			values:    []any{"task-5"},
		},
	}
	if !reflect.DeepEqual(session.execs, want) {
		t.Fatalf("driver executions = %#v, want %#v", session.execs, want)
	}
}

func TestMutationsRejectUnsafeOrUnsupportedRequests(t *testing.T) {
	_, connection := operationConnection(t)

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

	_, err = connection.Insert(context.Background(), &providerv1.InsertRequest{
		Entity:          taskReference(),
		ReturningFields: []string{"id"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Insert() returning code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}

	_, err = connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity:      taskReference(),
		Assignments: []*providerv1.Assignment{{Field: "id", Value: literalExpression(stringValue("new-id"))}},
		Filter:      equalExpression("id", stringValue("task-1")),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Update() primary key code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func operationConnection(
	t *testing.T,
	iterators ...driverIterator,
) (*recordingSession, *Connection) {
	t.Helper()

	session := &recordingSession{
		metadata: &gocql.KeyspaceMetadata{
			Name: "kubling_sample",
			Tables: map[string]*gocql.TableMetadata{
				"TASK": taskTableMetadata(),
			},
		},
		iterators: iterators,
	}
	config, err := normalizeConfig(Config{DataSources: map[string]DataSourceConfig{
		operationTestNamespace: {
			Hosts:    []string{"127.0.0.1"},
			Keyspace: "kubling_sample",
		},
	}})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	provider := newProvider(config, func(context.Context, DataSourceConfig) (driverSession, error) {
		return session, nil
	})

	return session, openTestConnection(t, provider)
}

func taskTableMetadata() *gocql.TableMetadata {
	id := testColumn("id", gocql.TypeText, gocql.ColumnPartitionKey)
	return &gocql.TableMetadata{
		Name:           "TASK",
		PartitionKey:   []*gocql.ColumnMetadata{id},
		OrderedColumns: []string{"id", "project_id", "title", "completed", "priority"},
		Columns: map[string]*gocql.ColumnMetadata{
			"id":         id,
			"project_id": testColumn("project_id", gocql.TypeText, gocql.ColumnRegular),
			"title":      testColumn("title", gocql.TypeText, gocql.ColumnRegular),
			"completed":  testColumn("completed", gocql.TypeBoolean, gocql.ColumnRegular),
			"priority":   testColumn("priority", gocql.TypeInt, gocql.ColumnRegular),
		},
	}
}

func taskReference() *providerv1.EntityReference {
	return &providerv1.EntityReference{Name: "TASK", Namespace: operationTestNamespace}
}

func fieldExpression(name string) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Field{
		Field: &providerv1.FieldReference{Name: name},
	}}
}

func literalExpression(value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Literal{
		Literal: &providerv1.Literal{Value: value},
	}}
}

func equalExpression(name string, value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Comparison{
		Comparison: &providerv1.ComparisonExpression{
			Operator: providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			Left:     fieldExpression(name),
			Right:    literalExpression(value),
		},
	}}
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func integerProviderValue(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: value}}
}

func booleanValue(value bool) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{BooleanValue: value}}
}
