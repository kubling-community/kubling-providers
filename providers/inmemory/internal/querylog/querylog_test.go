package querylog

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestSummarizeAggregateQuery(t *testing.T) {
	count := aggregateExpression(
		providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR,
	)
	average := aggregateExpression(
		providerv1.AggregateFunction_AGGREGATE_FUNCTION_AVG,
	)
	request := &providerv1.QueryRequest{
		ConnectionId: "not-observable",
		Entity:       &providerv1.EntityReference{Name: "TASK", Namespace: "private"},
		Projections: []*providerv1.Projection{
			{Expression: count},
			{Expression: average},
		},
		GroupBy: []*providerv1.Expression{{
			Kind: &providerv1.Expression_Field{
				Field: &providerv1.FieldReference{Name: "project_id"},
			},
		}},
		Having: &providerv1.Expression{
			Kind: &providerv1.Expression_NullPredicate{
				NullPredicate: &providerv1.NullPredicate{Expression: count},
			},
		},
	}

	got := Summarize(request)
	if got.Entity != "TASK" || got.Mode != "aggregate" ||
		got.ProjectionCount != 2 || got.GroupByCount != 1 || !got.Having {
		t.Fatalf("Summarize() = %#v", got)
	}
	wantFunctions := []string{"COUNT_STAR", "AVG"}
	if !reflect.DeepEqual(got.AggregateFunctions, wantFunctions) {
		t.Fatalf(
			"Summarize() aggregate functions = %v, want %v",
			got.AggregateFunctions,
			wantFunctions,
		)
	}
}

func TestReceivedDoesNotLogValuesOrConnectionIdentity(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	request := &providerv1.QueryRequest{
		ConnectionId: "private-connection",
		Entity:       &providerv1.EntityReference{Name: "TASK", Namespace: "private-namespace"},
		Projections: []*providerv1.Projection{{
			Expression: aggregateExpression(
				providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR,
			),
		}},
		Filter: &providerv1.Expression{
			Kind: &providerv1.Expression_Literal{
				Literal: &providerv1.Literal{Value: &kublingv1.Value{
					Kind: &kublingv1.Value_StringValue{StringValue: "private-literal"},
				}},
			},
		},
	}

	Received(context.Background(), logger, request)
	logged := output.String()
	for _, forbidden := range []string{
		"private-connection",
		"private-namespace",
		"private-literal",
	} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("Received() log contains %q: %s", forbidden, logged)
		}
	}
	for _, expected := range []string{"TASK", "aggregate", "COUNT_STAR"} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("Received() log does not contain %q: %s", expected, logged)
		}
	}
}

func aggregateExpression(
	function providerv1.AggregateFunction,
) *providerv1.Expression {
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Aggregate{
			Aggregate: &providerv1.AggregateCall{Function: function},
		},
	}
}
