package query

import (
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestBuildPlanExtractsAccessRouting(t *testing.T) {
	plan, err := buildPlan(&providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "memory"},
		Filter: logicalAnd(
			filterEquality("namespace", "fleet-a"),
			filterEquality("hostname", "node-a"),
		),
	})
	if err != nil {
		t.Fatalf("buildPlan() error = %v", err)
	}
	if plan.table != "MEMORY" || plan.route.namespace == nil ||
		*plan.route.namespace != "fleet-a" || plan.route.hostname == nil ||
		*plan.route.hostname != "node-a" || plan.route.impossible {
		t.Fatalf("plan route = %#v", plan.route)
	}
	if len(plan.projections) != 15 {
		t.Fatalf("default MEMORY projections = %d, want 15", len(plan.projections))
	}
}

func TestBuildPlanRecognizesContradictoryAccessRouting(t *testing.T) {
	plan, err := buildPlan(&providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "MEMORY"},
		Filter: logicalAnd(
			filterEquality("host_id", "host-a"),
			filterEquality("host_id", "host-b"),
		),
	})
	if err != nil {
		t.Fatalf("buildPlan() error = %v", err)
	}
	if !plan.route.impossible {
		t.Fatalf("plan route = %#v, want impossible", plan.route)
	}
}

func TestBuildPlanRejectsResidualFilterAndGeneratedProjection(t *testing.T) {
	tests := map[string]*providerv1.QueryRequest{
		"non-access filter": {
			Entity: &providerv1.EntityReference{Name: "MEMORY"},
			Filter: filterEquality("total_bytes", "1"),
		},
		"generated identifier projection": {
			Entity: &providerv1.EntityReference{Name: "MEMORY"},
			Projections: []*providerv1.Projection{{
				Expression: &providerv1.Expression{Kind: &providerv1.Expression_Field{
					Field: &providerv1.FieldReference{Name: "identifier"},
				}},
			}},
		},
	}
	for name, request := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := buildPlan(request); err == nil {
				t.Fatal("buildPlan() error = nil")
			}
		})
	}
}

func filterEquality(field string, value string) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Comparison{
		Comparison: &providerv1.ComparisonExpression{
			Operator: providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			Left: &providerv1.Expression{Kind: &providerv1.Expression_Field{
				Field: &providerv1.FieldReference{Name: field},
			}},
			Right: &providerv1.Expression{Kind: &providerv1.Expression_Literal{
				Literal: &providerv1.Literal{Value: &kublingv1.Value{
					Kind: &kublingv1.Value_StringValue{StringValue: value},
				}},
			}},
		},
	}}
}

func logicalAnd(operands ...*providerv1.Expression) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Logical{
		Logical: &providerv1.LogicalExpression{
			Operator: providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
			Operands: operands,
		},
	}}
}
