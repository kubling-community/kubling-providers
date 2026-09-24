package inmemory

import (
	"context"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAggregateQueryGroupsFiltersAndOrders(t *testing.T) {
	client, connectionID := aggregateTestClient(t)
	group := fieldExpression("project_id")
	count := aggregateExpression(
		providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR,
		nil,
		false,
		kublingv1.ValueType_VALUE_TYPE_LONG,
	)
	average := aggregateExpression(
		providerv1.AggregateFunction_AGGREGATE_FUNCTION_AVG,
		fieldExpression("priority"),
		false,
		kublingv1.ValueType_VALUE_TYPE_DOUBLE,
	)

	tuples := flattenTuples(queryBatches(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		Projections: []*providerv1.Projection{
			{Expression: group, OutputName: "project_id"},
			{Expression: count, OutputName: "task_count"},
			{Expression: average, OutputName: "average_priority"},
		},
		GroupBy: []*providerv1.Expression{group},
		Having: comparisonExpression(
			providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN,
			count,
			literalExpression(longValue(1)),
		),
		OrderBy: []*providerv1.OrderBy{{
			Expression: count,
			Direction:  providerv1.SortDirection_SORT_DIRECTION_DESCENDING,
		}},
	}))

	if len(tuples) != 1 {
		t.Fatalf("aggregate rows = %d, want 1", len(tuples))
	}
	values := tuples[0].GetValues()
	if values[0].GetStringValue() != "project-1" ||
		values[1].GetLongValue() != 2 ||
		values[2].GetDoubleValue() != 1.5 {
		t.Fatalf("aggregate values = %v", values)
	}
}

func TestAggregateQueryFunctionsDistinctAndNulls(t *testing.T) {
	client, connectionID := aggregateTestClient(t)
	projections := []*providerv1.Projection{
		aggregateProjection("count_star", providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR, nil, false, kublingv1.ValueType_VALUE_TYPE_LONG),
		aggregateProjection("count", providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT, fieldExpression("estimate_hours"), false, kublingv1.ValueType_VALUE_TYPE_LONG),
		aggregateProjection("count_big", providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_BIG, fieldExpression("title"), false, kublingv1.ValueType_VALUE_TYPE_LONG),
		aggregateProjection("distinct_completed", providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT, fieldExpression("completed"), true, kublingv1.ValueType_VALUE_TYPE_LONG),
		aggregateProjection("minimum", providerv1.AggregateFunction_AGGREGATE_FUNCTION_MIN, fieldExpression("priority"), false, kublingv1.ValueType_VALUE_TYPE_INTEGER),
		aggregateProjection("maximum", providerv1.AggregateFunction_AGGREGATE_FUNCTION_MAX, fieldExpression("priority"), false, kublingv1.ValueType_VALUE_TYPE_INTEGER),
		aggregateProjection("sum", providerv1.AggregateFunction_AGGREGATE_FUNCTION_SUM, fieldExpression("priority"), false, kublingv1.ValueType_VALUE_TYPE_LONG),
		aggregateProjection("average", providerv1.AggregateFunction_AGGREGATE_FUNCTION_AVG, fieldExpression("estimate_hours"), false, kublingv1.ValueType_VALUE_TYPE_DOUBLE),
	}

	tuples := flattenTuples(queryBatches(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		Projections:  projections,
	}))
	if len(tuples) != 1 {
		t.Fatalf("aggregate rows = %d, want 1", len(tuples))
	}
	values := tuples[0].GetValues()
	if values[0].GetLongValue() != 3 ||
		values[1].GetLongValue() != 2 ||
		values[2].GetLongValue() != 3 ||
		values[3].GetLongValue() != 2 ||
		values[4].GetIntegerValue() != 1 ||
		values[5].GetIntegerValue() != 3 ||
		values[6].GetLongValue() != 6 ||
		values[7].GetDoubleValue() != 3.25 {
		t.Fatalf("aggregate values = %v", values)
	}
}

func TestAggregateQueryOverEmptyInputReturnsSQLIdentities(t *testing.T) {
	client, connectionID := aggregateTestClient(t)
	tuples := flattenTuples(queryBatches(t, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       taskEntity(),
		Projections: []*providerv1.Projection{
			aggregateProjection("count_star", providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR, nil, false, kublingv1.ValueType_VALUE_TYPE_LONG),
			aggregateProjection("sum", providerv1.AggregateFunction_AGGREGATE_FUNCTION_SUM, fieldExpression("priority"), false, kublingv1.ValueType_VALUE_TYPE_LONG),
		},
		Filter: equalExpression(
			fieldExpression("id"),
			literalExpression(stringValue("missing")),
		),
	}))
	if len(tuples) != 1 {
		t.Fatalf("aggregate rows = %d, want 1", len(tuples))
	}
	values := tuples[0].GetValues()
	if values[0].GetLongValue() != 0 || !isNullValue(values[1]) {
		t.Fatalf("aggregate values = %v", values)
	}
}

func TestAggregateMinMaxPreserveExactNumericOrdering(t *testing.T) {
	rows := []entityRow{
		{values: map[string]*kublingv1.Value{
			"integer_value": bigintegerValue("9007199254740993"),
			"decimal_value": bigdecimalValue("1.0000000000000000"),
		}},
		{values: map[string]*kublingv1.Value{
			"integer_value": bigintegerValue("9007199254740992"),
			"decimal_value": bigdecimalValue("1.0000000000000001"),
		}},
	}

	minimum, err := evaluateAggregate(rows, &providerv1.AggregateCall{
		Function: providerv1.AggregateFunction_AGGREGATE_FUNCTION_MIN,
		Arguments: []*providerv1.Expression{
			fieldExpression("integer_value"),
		},
		ResultType: &kublingv1.TypeDescriptor{
			Type: kublingv1.ValueType_VALUE_TYPE_BIGINTEGER,
		},
	})
	if err != nil {
		t.Fatalf("evaluateAggregate(MIN) error = %v", err)
	}
	if got := minimum.GetBigintegerValue(); got != "9007199254740992" {
		t.Fatalf("evaluateAggregate(MIN) = %q", got)
	}

	maximum, err := evaluateAggregate(rows, &providerv1.AggregateCall{
		Function: providerv1.AggregateFunction_AGGREGATE_FUNCTION_MAX,
		Arguments: []*providerv1.Expression{
			fieldExpression("decimal_value"),
		},
		ResultType: &kublingv1.TypeDescriptor{
			Type: kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL,
		},
	})
	if err != nil {
		t.Fatalf("evaluateAggregate(MAX) error = %v", err)
	}
	if got := maximum.GetBigdecimalValue(); got != "1.0000000000000001" {
		t.Fatalf("evaluateAggregate(MAX) = %q", got)
	}
}

func TestAggregateQueryRejectsUnsafeShapes(t *testing.T) {
	client, connectionID := aggregateTestClient(t)
	count := aggregateExpression(
		providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR,
		nil,
		false,
		kublingv1.ValueType_VALUE_TYPE_LONG,
	)
	tests := map[string]*providerv1.QueryRequest{
		"aggregate in filter": {
			ConnectionId: connectionID,
			Entity:       taskEntity(),
			Projections: []*providerv1.Projection{{
				Expression: count,
				OutputName: "task_count",
			}},
			Filter: comparisonExpression(
				providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN,
				count,
				literalExpression(longValue(0)),
			),
		},
		"ungrouped projection": {
			ConnectionId: connectionID,
			Entity:       taskEntity(),
			Projections: []*providerv1.Projection{
				{Expression: fieldExpression("project_id")},
				{Expression: fieldExpression("title")},
			},
			GroupBy: []*providerv1.Expression{fieldExpression("project_id")},
		},
		"nested aggregate": {
			ConnectionId: connectionID,
			Entity:       taskEntity(),
			Projections: []*providerv1.Projection{{
				Expression: aggregateExpression(
					providerv1.AggregateFunction_AGGREGATE_FUNCTION_SUM,
					count,
					false,
					kublingv1.ValueType_VALUE_TYPE_LONG,
				),
				OutputName: "invalid",
			}},
		},
	}

	for name, request := range tests {
		t.Run(name, func(t *testing.T) {
			stream, err := client.Query(context.Background(), request)
			if err == nil {
				_, err = stream.Recv()
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Query() code = %v, want %v", status.Code(err), codes.InvalidArgument)
			}
		})
	}
}

func aggregateTestClient(
	t *testing.T,
) (providerv1.ProviderServiceClient, string) {
	t.Helper()
	client := newTestClient(t)
	opened, err := client.OpenConnection(
		context.Background(),
		&providerv1.OpenConnectionRequest{},
	)
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	return client, opened.GetConnectionId()
}

func aggregateProjection(
	outputName string,
	function providerv1.AggregateFunction,
	argument *providerv1.Expression,
	distinct bool,
	resultType kublingv1.ValueType,
) *providerv1.Projection {
	return &providerv1.Projection{
		Expression: aggregateExpression(function, argument, distinct, resultType),
		OutputName: outputName,
	}
}

func aggregateExpression(
	function providerv1.AggregateFunction,
	argument *providerv1.Expression,
	distinct bool,
	resultType kublingv1.ValueType,
) *providerv1.Expression {
	aggregate := &providerv1.AggregateCall{
		Function:   function,
		Distinct:   distinct,
		ResultType: &kublingv1.TypeDescriptor{Type: resultType},
	}
	if argument != nil {
		aggregate.Arguments = []*providerv1.Expression{argument}
	}
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Aggregate{Aggregate: aggregate},
	}
}

func comparisonExpression(
	operator providerv1.ComparisonOperator,
	left *providerv1.Expression,
	right *providerv1.Expression,
) *providerv1.Expression {
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Comparison{
			Comparison: &providerv1.ComparisonExpression{
				Operator: operator,
				Left:     left,
				Right:    right,
			},
		},
	}
}
