package inmemory

import (
	"context"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	providercache "github.com/kubling-community/kubling-providers/sdk-go/provider/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestProviderServiceLifecycleAndOperations(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	capabilities, err := client.GetCapabilities(
		ctx,
		&providerv1.GetCapabilitiesRequest{},
	)
	if err != nil {
		t.Fatalf("GetCapabilities() error = %v", err)
	}
	if capabilities.GetTransactions().GetSupported() {
		t.Fatal("GetCapabilities() transactions supported = true, want false")
	}
	if !capabilities.GetQuery().GetOrdering().GetSupported() {
		t.Fatal("GetCapabilities() ordering supported = false, want true")
	}
	if !capabilities.GetQuery().GetPagination().GetLimit() ||
		!capabilities.GetQuery().GetPagination().GetOffset() {
		t.Fatal("GetCapabilities() pagination is not fully supported")
	}
	if got := len(capabilities.GetQuery().GetExpressions().GetComparisonOperators()); got != 6 {
		t.Fatalf("GetCapabilities() comparison operators = %d, want 6", got)
	}
	if got := len(capabilities.GetQuery().GetExpressions().GetLogicalOperators()); got != 3 {
		t.Fatalf("GetCapabilities() logical operators = %d, want 3", got)
	}
	if got := len(capabilities.GetQuery().GetExpressions().GetNullPredicateOperators()); got != 2 {
		t.Fatalf("GetCapabilities() null predicate operators = %d, want 2", got)
	}
	aggregates := capabilities.GetQuery().GetAggregates()
	if len(aggregates.GetFunctions()) != 7 ||
		!aggregates.GetDistinct() ||
		!aggregates.GetGroupBy() ||
		!aggregates.GetHaving() {
		t.Fatalf("GetCapabilities() aggregates = %v", aggregates)
	}
	if !capabilities.GetMutations().GetInsert() ||
		!capabilities.GetMutations().GetUpdate() ||
		!capabilities.GetMutations().GetDelete() ||
		!capabilities.GetMutations().GetGeneratedValues() {
		t.Fatal("GetCapabilities() mutations are not fully supported")
	}
	if got := len(capabilities.GetValues().GetSupportedTypes()); got != int(kublingv1.ValueType_VALUE_TYPE_ARRAY) {
		t.Fatalf("GetCapabilities() supported value types = %d, want %d", got, kublingv1.ValueType_VALUE_TYPE_ARRAY)
	}
	if !containsString(capabilities.GetValues().GetFeatures(), grpcfeatures.ArrayValuesV1) ||
		!containsString(capabilities.GetValues().GetFeatures(), grpcfeatures.SpatialValuesV1) ||
		!containsString(capabilities.GetValues().GetFeatures(), grpcfeatures.LobReadV1) {
		t.Fatalf("GetCapabilities() value features = %v", capabilities.GetValues().GetFeatures())
	}
	if got := capabilities.GetValues().GetMaxArrayDimensions(); got != 1 {
		t.Fatalf("GetCapabilities() max array dimensions = %d, want 1", got)
	}
	if got := capabilities.GetValues().GetMaxLobChunkBytes(); got != inMemoryMaxLobChunkBytes {
		t.Fatalf("GetCapabilities() max LOB chunk bytes = %d, want %d", got, inMemoryMaxLobChunkBytes)
	}
	if got := capabilities.GetValues().GetMaxLobBytes(); got != inMemoryMaxLobBytes {
		t.Fatalf("GetCapabilities() max LOB bytes = %d, want %d", got, inMemoryMaxLobBytes)
	}
	if got := capabilities.GetValues().GetLobReferenceRetentionSeconds(); got != uint64(inMemoryLobRetention/time.Second) {
		t.Fatalf("GetCapabilities() LOB retention = %d", got)
	}

	schema, err := client.GetSchema(
		ctx,
		&providerv1.GetSchemaRequest{},
	)
	if err != nil {
		t.Fatalf("GetSchema() error = %v", err)
	}
	expectedSchema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("os.ReadFile(schema.sql) error = %v", err)
	}
	if schema.GetSchemaDdl() != string(expectedSchema) {
		t.Fatalf(
			"GetSchema() DDL = %q, want schema.sql content %q",
			schema.GetSchemaDdl(),
			expectedSchema,
		)
	}

	connection, err := client.OpenConnection(
		ctx,
		&providerv1.OpenConnectionRequest{},
	)
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	connectionID := connection.GetConnectionId()
	if connectionID == "" {
		t.Fatal("OpenConnection() connection_id is empty")
	}

	health, err := client.Health(ctx, &providerv1.HealthRequest{})
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if !health.GetHealthy() {
		t.Fatalf("Health() healthy = false, message = %q", health.GetMessage())
	}

	batchSize := uint32(2)
	initialBatches := queryBatches(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		OrderBy: []*providerv1.OrderBy{
			{
				Expression:   fieldExpression("priority"),
				Direction:    providerv1.SortDirection_SORT_DIRECTION_DESCENDING,
				NullOrdering: providerv1.NullOrdering_NULL_ORDERING_LAST,
			},
		},
		BatchSize: &batchSize,
	})
	if len(initialBatches) != 2 {
		t.Fatalf("Query() batches = %d, want 2", len(initialBatches))
	}
	initialRows := flattenTuples(initialBatches)
	if len(initialRows) != 3 {
		t.Fatalf("Query() rows = %d, want 3", len(initialRows))
	}
	if got := initialRows[0].GetValues()[0].GetStringValue(); got != "task-3" {
		t.Fatalf("Query() first id = %q, want task-3", got)
	}

	inserted, err := client.Insert(ctx, &providerv1.InsertRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{
				{Name: "title"},
				{Name: "priority"},
			},
			Tuples: []*providerv1.Tuple{
				{
					Values: []*kublingv1.Value{
						stringValue("Exercise the reference provider"),
						integerValue(10),
					},
				},
			},
		},
		ReturningFields: []string{"id"},
	})
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if inserted.GetAffectedRows() != 1 {
		t.Fatalf("Insert() affected_rows = %d, want 1", inserted.GetAffectedRows())
	}
	generatedValues := inserted.GetGeneratedValues()
	if len(generatedValues.GetTuples()) != 1 {
		t.Fatalf(
			"Insert() generated tuples = %d, want 1",
			len(generatedValues.GetTuples()),
		)
	}
	generatedID := generatedValues.GetTuples()[0].
		GetValues()[0].
		GetStringValue()
	if generatedID == "" {
		t.Fatal("Insert() generated id is empty")
	}

	updated, err := client.Update(ctx, &providerv1.UpdateRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		Assignments: []*providerv1.Assignment{
			{
				Field: "completed",
				Value: literalExpression(booleanValue(true)),
			},
		},
		Filter: equalExpression(
			fieldExpression("id"),
			literalExpression(stringValue(generatedID)),
		),
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.GetAffectedRows() != 1 {
		t.Fatalf("Update() affected_rows = %d, want 1", updated.GetAffectedRows())
	}

	matchingBatches := queryBatches(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		Projections: []*providerv1.Projection{
			{Expression: fieldExpression("id")},
			{Expression: fieldExpression("title")},
			{Expression: fieldExpression("completed")},
		},
		Filter: equalExpression(
			fieldExpression("id"),
			literalExpression(stringValue(generatedID)),
		),
	})
	matchingRows := flattenTuples(matchingBatches)
	if len(matchingRows) != 1 {
		t.Fatalf("Query() matching rows = %d, want 1", len(matchingRows))
	}
	values := matchingRows[0].GetValues()
	if values[0].GetStringValue() != generatedID {
		t.Fatalf("Query() id = %q, want %q", values[0].GetStringValue(), generatedID)
	}
	if values[1].GetStringValue() != "Exercise the reference provider" {
		t.Fatalf("Query() title = %q", values[1].GetStringValue())
	}
	if !values[2].GetBooleanValue() {
		t.Fatal("Query() completed = false, want true")
	}

	deleted, err := client.Delete(ctx, &providerv1.DeleteRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		Filter: equalExpression(
			fieldExpression("id"),
			literalExpression(stringValue(generatedID)),
		),
	})
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if deleted.GetAffectedRows() != 1 {
		t.Fatalf("Delete() affected_rows = %d, want 1", deleted.GetAffectedRows())
	}

	remaining := flattenTuples(queryBatches(
		t,
		client,
		&providerv1.QueryRequest{
			ConnectionId: connectionID,
			Entity:       taskEntity(),
			Filter: equalExpression(
				fieldExpression("id"),
				literalExpression(stringValue(generatedID)),
			),
		},
	))
	if len(remaining) != 0 {
		t.Fatalf("Query() rows after delete = %d, want 0", len(remaining))
	}

	_, err = client.BeginTransaction(
		ctx,
		&providerv1.BeginTransactionRequest{
			ConnectionId: connectionID,
		},
	)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf(
			"BeginTransaction() code = %v, want %v",
			status.Code(err),
			codes.Unimplemented,
		)
	}

	_, err = client.CloseConnection(
		ctx,
		&providerv1.CloseConnectionRequest{
			ConnectionId: connectionID,
		},
	)
	if err != nil {
		t.Fatalf("CloseConnection() error = %v", err)
	}

	health, err = client.Health(ctx, &providerv1.HealthRequest{})
	if err != nil {
		t.Fatalf("Health() after connection close error = %v", err)
	}
	if !health.GetHealthy() {
		t.Fatalf(
			"Health() after connection close healthy = false, message = %q",
			health.GetMessage(),
		)
	}
}

func TestProviderQueriesEverySchemaEntityAndValueType(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	schema, err := client.GetSchema(
		ctx,
		&providerv1.GetSchemaRequest{},
	)
	if err != nil {
		t.Fatalf("GetSchema() error = %v", err)
	}
	if got := strings.Count(
		schema.GetSchemaDdl(),
		"CREATE FOREIGN TABLE",
	); got != 4 {
		t.Fatalf("GetSchema() table count = %d, want 4", got)
	}
	if got := strings.Count(
		schema.GetSchemaDdl(),
		"OPTIONS(ANNOTATION",
	); got != 48 {
		t.Fatalf("GetSchema() annotated field count = %d, want 48", got)
	}

	connection, err := client.OpenConnection(
		ctx,
		&providerv1.OpenConnectionRequest{},
	)
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	connectionID := connection.GetConnectionId()

	entityRows := map[string]int{
		projectEntityName:    2,
		taskEntityName:       3,
		auditEventEntityName: 2,
		typeSampleEntityName: 1,
	}
	for entityName, expectedRows := range entityRows {
		batches := queryBatches(t, client, &providerv1.QueryRequest{
			ConnectionId:     connectionID,
			Entity:           entity(entityName),
			AcceptedFeatures: extendedValueFeatures(),
		})
		if got := len(flattenTuples(batches)); got != expectedRows {
			t.Fatalf(
				"Query(%s) rows = %d, want %d",
				entityName,
				got,
				expectedRows,
			)
		}
	}

	typeBatches := queryBatches(t, client, &providerv1.QueryRequest{
		ConnectionId:     connectionID,
		Entity:           entity(typeSampleEntityName),
		AcceptedFeatures: extendedValueFeatures(),
	})
	if len(typeBatches) != 1 {
		t.Fatalf("Query(TYPE_SAMPLE) batches = %d, want 1", len(typeBatches))
	}
	typeBatch := typeBatches[0]
	if len(typeBatch.GetTuples()) != 1 {
		t.Fatalf(
			"Query(TYPE_SAMPLE) tuples = %d, want 1",
			len(typeBatch.GetTuples()),
		)
	}

	tupleValues := typeBatch.GetTuples()[0].GetValues()
	if len(tupleValues) != len(typeBatch.GetFields()) {
		t.Fatalf(
			"Query(TYPE_SAMPLE) values = %d, fields = %d",
			len(tupleValues),
			len(typeBatch.GetFields()),
		)
	}

	coveredTypes := make(map[kublingv1.ValueType]bool)
	for index, field := range typeBatch.GetFields() {
		actualType, err := valueType(tupleValues[index])
		if err != nil {
			t.Fatalf(
				"valueType(TYPE_SAMPLE.%s) error = %v",
				field.GetName(),
				err,
			)
		}
		if actualType != field.GetType() {
			t.Fatalf(
				"TYPE_SAMPLE.%s value type = %v, field type = %v",
				field.GetName(),
				actualType,
				field.GetType(),
			)
		}
		if field.GetType() == kublingv1.ValueType_VALUE_TYPE_ARRAY {
			descriptor := field.GetTypeDescriptor()
			if descriptor.GetType() != kublingv1.ValueType_VALUE_TYPE_ARRAY ||
				descriptor.GetElementType().GetType() != kublingv1.ValueType_VALUE_TYPE_INTEGER {
				t.Fatalf("TYPE_SAMPLE.%s descriptor = %v", field.GetName(), descriptor)
			}
		}

		coveredTypes[field.GetType()] = true
	}

	for typeNumber := int32(kublingv1.ValueType_VALUE_TYPE_STRING); typeNumber <= int32(kublingv1.ValueType_VALUE_TYPE_ARRAY); typeNumber++ {
		valueType := kublingv1.ValueType(typeNumber)
		if !coveredTypes[valueType] {
			t.Fatalf("Query(TYPE_SAMPLE) does not cover %v", valueType)
		}
	}

	_, err = client.Insert(ctx, &providerv1.InsertRequest{
		ConnectionId: connectionID,
		Entity:       entity(projectEntityName),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf(
			"Insert(PROJECT) code = %v, want %v",
			status.Code(err),
			codes.FailedPrecondition,
		)
	}
}

func TestProviderNegotiatesExtendedValueRepresentations(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	connection, err := client.OpenConnection(
		ctx,
		&providerv1.OpenConnectionRequest{},
	)
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	connectionID := connection.GetConnectionId()

	spatialProjections := []*providerv1.Projection{
		{Expression: fieldExpression("geometry_value")},
		{Expression: fieldExpression("geography_value")},
	}
	legacyBatches := queryBatches(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       entity(typeSampleEntityName),
		Projections:  spatialProjections,
	})
	legacyValues := legacyBatches[0].GetTuples()[0].GetValues()
	if legacyValues[0].GetGeometryValue() == nil ||
		legacyValues[1].GetGeographyValue() == nil {
		t.Fatalf("legacy spatial values = %v", legacyValues)
	}

	spatialBatches := queryBatches(t, client, &providerv1.QueryRequest{
		ConnectionId:     connectionID,
		Entity:           entity(typeSampleEntityName),
		Projections:      spatialProjections,
		AcceptedFeatures: []string{grpcfeatures.SpatialValuesV1},
	})
	spatialValues := spatialBatches[0].GetTuples()[0].GetValues()
	if got := spatialValues[0].GetGeometryWithCrs().GetSrid(); got != 4326 {
		t.Fatalf("geometry SRID = %d, want 4326", got)
	}
	if got := spatialValues[1].GetGeographyWithCrs().GetSrid(); got != 4326 {
		t.Fatalf("geography SRID = %d, want 4326", got)
	}

	arrayProjection := []*providerv1.Projection{{
		Expression: fieldExpression("integer_array_value"),
	}}
	stream, err := client.Query(ctx, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       entity(typeSampleEntityName),
		Projections:  arrayProjection,
	})
	if err != nil {
		t.Fatalf("Query(array) setup error = %v", err)
	}
	_, err = stream.Recv()
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("Query(array) status = %s, want %s", got, codes.FailedPrecondition)
	}

	arrayBatches := queryBatches(t, client, &providerv1.QueryRequest{
		ConnectionId:     connectionID,
		Entity:           entity(typeSampleEntityName),
		Projections:      arrayProjection,
		AcceptedFeatures: []string{grpcfeatures.ArrayValuesV1},
	})
	arrayField := arrayBatches[0].GetFields()[0]
	array := arrayBatches[0].GetTuples()[0].GetValues()[0].GetArrayValue()
	if arrayField.GetTypeDescriptor().GetElementType().GetType() !=
		kublingv1.ValueType_VALUE_TYPE_INTEGER {
		t.Fatalf("array field descriptor = %v", arrayField.GetTypeDescriptor())
	}
	if len(array.GetElements()) != 3 || !isNullValue(array.GetElements()[1]) {
		t.Fatalf("array value = %v", array)
	}
}

func TestProviderLobLifecycle(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	connection, err := client.OpenConnection(
		ctx,
		&providerv1.OpenConnectionRequest{},
	)
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	connectionID := connection.GetConnectionId()
	batches := queryBatches(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       entity(typeSampleEntityName),
		Projections: []*providerv1.Projection{
			{Expression: fieldExpression("blob_value")},
			{Expression: fieldExpression("clob_value")},
		},
		AcceptedFeatures: []string{grpcfeatures.LobReadV1},
	})
	values := batches[0].GetTuples()[0].GetValues()
	expected := [][]byte{
		[]byte("binary large object"),
		[]byte("character large object"),
	}

	for index, value := range values {
		reference := value.GetLobReference()
		if reference.GetSessionId() != connectionID {
			t.Fatalf("LOB %d session_id = %q, want %q", index, reference.GetSessionId(), connectionID)
		}
		if reference.ExpiresAtUnixMs == nil || reference.GetSizeBytes() != uint64(len(expected[index])) {
			t.Fatalf("LOB %d reference = %v", index, reference)
		}

		stream, err := client.ReadLob(ctx, &providerv1.ReadLobRequest{
			ConnectionId:  reference.GetSessionId(),
			LobId:         reference.GetLobId(),
			MaxChunkBytes: 4,
		})
		if err != nil {
			t.Fatalf("ReadLob(%d) setup error = %v", index, err)
		}
		var data []byte
		var expectedOffset uint64
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ReadLob(%d).Recv() error = %v", index, err)
			}
			if response.GetOffset() != expectedOffset || len(response.GetData()) > 4 {
				t.Fatalf("ReadLob(%d) response = %v", index, response)
			}
			data = append(data, response.GetData()...)
			expectedOffset += uint64(len(response.GetData()))
		}
		if string(data) != string(expected[index]) {
			t.Fatalf("ReadLob(%d) data = %q, want %q", index, data, expected[index])
		}

		rangeLength := uint64(5)
		rangeStream, err := client.ReadLob(ctx, &providerv1.ReadLobRequest{
			ConnectionId:  reference.GetSessionId(),
			LobId:         reference.GetLobId(),
			Offset:        2,
			Length:        &rangeLength,
			MaxChunkBytes: 3,
		})
		if err != nil {
			t.Fatalf("ReadLob(range %d) setup error = %v", index, err)
		}
		var rangeData []byte
		for {
			response, err := rangeStream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ReadLob(range %d).Recv() error = %v", index, err)
			}
			rangeData = append(rangeData, response.GetData()...)
		}
		if string(rangeData) != string(expected[index][2:7]) {
			t.Fatalf(
				"ReadLob(range %d) data = %q, want %q",
				index,
				rangeData,
				expected[index][2:7],
			)
		}

		zeroLength := uint64(0)
		zeroStream, err := client.ReadLob(ctx, &providerv1.ReadLobRequest{
			ConnectionId: reference.GetSessionId(),
			LobId:        reference.GetLobId(),
			Offset:       uint64(len(expected[index])),
			Length:       &zeroLength,
		})
		if err != nil {
			t.Fatalf("ReadLob(zero %d) setup error = %v", index, err)
		}
		zeroResponse, err := zeroStream.Recv()
		if err != nil {
			t.Fatalf("ReadLob(zero %d).Recv() error = %v", index, err)
		}
		if zeroResponse.GetOffset() != uint64(len(expected[index])) ||
			len(zeroResponse.GetData()) != 0 ||
			!zeroResponse.GetEndOfRead() {
			t.Fatalf("ReadLob(zero %d) response = %v", index, zeroResponse)
		}
		if _, err := zeroStream.Recv(); err != io.EOF {
			t.Fatalf("ReadLob(zero %d) terminal error = %v, want EOF", index, err)
		}

		for releaseCall := 0; releaseCall < 2; releaseCall++ {
			if _, err := client.ReleaseLob(ctx, &providerv1.ReleaseLobRequest{
				ConnectionId: reference.GetSessionId(),
				LobId:        reference.GetLobId(),
			}); err != nil {
				t.Fatalf("ReleaseLob(%d, call %d) error = %v", index, releaseCall, err)
			}
		}

		releasedStream, err := client.ReadLob(ctx, &providerv1.ReadLobRequest{
			ConnectionId: reference.GetSessionId(),
			LobId:        reference.GetLobId(),
		})
		if err != nil {
			t.Fatalf("ReadLob(released %d) setup error = %v", index, err)
		}
		_, err = releasedStream.Recv()
		if got := status.Code(err); got != codes.NotFound {
			t.Fatalf("ReadLob(released %d) status = %s, want %s", index, got, codes.NotFound)
		}
	}
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}

	return false
}

func extendedValueFeatures() []string {
	return []string{
		grpcfeatures.ArrayValuesV1,
		grpcfeatures.SpatialValuesV1,
	}
}

func TestProviderCacheInvalidatesSuccessfulMutations(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	connection, err := client.OpenConnection(
		ctx,
		&providerv1.OpenConnectionRequest{},
	)
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	connectionID := connection.GetConnectionId()
	request := &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		Projections: []*providerv1.Projection{
			{Expression: fieldExpression("completed")},
		},
		Filter: equalExpression(
			fieldExpression("id"),
			literalExpression(stringValue("task-2")),
		),
	}

	initialRows := flattenTuples(queryBatches(t, client, request))
	if len(initialRows) != 1 {
		t.Fatalf("initial Query() rows = %d, want 1", len(initialRows))
	}
	if initialRows[0].GetValues()[0].GetBooleanValue() {
		t.Fatal("initial Query() completed = true, want false")
	}
	flattenTuples(queryBatches(t, client, request))

	if _, err := client.Update(ctx, &providerv1.UpdateRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		Assignments: []*providerv1.Assignment{
			{
				Field: "completed",
				Value: literalExpression(booleanValue(true)),
			},
		},
		Filter: equalExpression(
			fieldExpression("id"),
			literalExpression(stringValue("task-2")),
		),
	}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	updatedRows := flattenTuples(queryBatches(t, client, request))
	if len(updatedRows) != 1 {
		t.Fatalf("Query() rows after Update = %d, want 1", len(updatedRows))
	}
	if !updatedRows[0].GetValues()[0].GetBooleanValue() {
		t.Fatal("Query() completed after Update = false, want true")
	}

	if _, err := client.Delete(ctx, &providerv1.DeleteRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		Filter: equalExpression(
			fieldExpression("id"),
			literalExpression(stringValue("task-2")),
		),
	}); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	deletedRows := flattenTuples(queryBatches(t, client, request))
	if len(deletedRows) != 0 {
		t.Fatalf("Query() rows after Delete = %d, want 0", len(deletedRows))
	}
}

func newTestClient(t *testing.T) providerv1.ProviderServiceClient {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	cachedProvider, _ := providercache.Wrap(New(), providercache.Config{})
	service := providersdk.NewServer(cachedProvider)
	providerv1.RegisterProviderServiceServer(grpcServer, service)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	clientConnection, err := grpc.NewClient(
		"passthrough:///inmemory",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(
			func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			},
		),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}

	t.Cleanup(func() {
		_ = clientConnection.Close()
		_ = service.Close(context.Background())
		grpcServer.Stop()
		_ = listener.Close()
	})

	return providerv1.NewProviderServiceClient(clientConnection)
}

func queryBatches(
	t *testing.T,
	client providerv1.ProviderServiceClient,
	request *providerv1.QueryRequest,
) []*providerv1.TupleBatch {
	t.Helper()

	stream, err := client.Query(context.Background(), request)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	var batches []*providerv1.TupleBatch
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			return batches
		}
		if err != nil {
			t.Fatalf("Query().Recv() error = %v", err)
		}

		batches = append(batches, response.GetBatch())
	}
}

func flattenTuples(
	batches []*providerv1.TupleBatch,
) []*providerv1.Tuple {
	var tuples []*providerv1.Tuple
	for _, batch := range batches {
		tuples = append(tuples, batch.GetTuples()...)
	}

	return tuples
}

func taskEntity() *providerv1.EntityReference {
	return entity(taskEntityName)
}

func entity(name string) *providerv1.EntityReference {
	return &providerv1.EntityReference{Name: name}
}

func fieldExpression(name string) *providerv1.Expression {
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Field{
			Field: &providerv1.FieldReference{Name: name},
		},
	}
}

func literalExpression(value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Literal{
			Literal: &providerv1.Literal{Value: value},
		},
	}
}

func equalExpression(
	left *providerv1.Expression,
	right *providerv1.Expression,
) *providerv1.Expression {
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Comparison{
			Comparison: &providerv1.ComparisonExpression{
				Operator: providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
				Left:     left,
				Right:    right,
			},
		},
	}
}
