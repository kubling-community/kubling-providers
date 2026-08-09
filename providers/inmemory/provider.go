package inmemory

import (
	"context"
	_ "embed"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
)

//go:embed schema.sql
var schemaDDL string

// Provider exposes one in-memory sample data universe.
type Provider struct {
	store *store
}

// New creates an in-memory provider with the canonical sample data.
func New() *Provider {
	return &Provider{store: newStore()}
}

// Capabilities describes the operations implemented by this provider.
func (p *Provider) Capabilities(
	context.Context,
) (*providersdk.Capabilities, error) {
	return &providersdk.Capabilities{
		Transactions: &providerv1.TransactionCapabilities{
			Supported: false,
		},
		Query: &providerv1.QueryCapabilities{
			RequiresCriteria: false,
			Ordering: &providerv1.OrderingCapabilities{
				Supported:            true,
				ExplicitNullOrdering: true,
				DefaultNullOrder:     providerv1.DefaultNullOrder_DEFAULT_NULL_ORDER_LAST,
			},
			Pagination: &providerv1.PaginationCapabilities{
				Limit:  true,
				Offset: true,
			},
			Expressions: &providerv1.ExpressionCapabilities{
				ComparisonOperators: []providerv1.ComparisonOperator{
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_NOT_EQUAL,
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN,
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL,
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN,
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL,
				},
				LogicalOperators: []providerv1.LogicalOperator{
					providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
					providerv1.LogicalOperator_LOGICAL_OPERATOR_OR,
					providerv1.LogicalOperator_LOGICAL_OPERATOR_NOT,
				},
				NullPredicateOperators: []providerv1.NullPredicateOperator{
					providerv1.NullPredicateOperator_NULL_PREDICATE_OPERATOR_IS_NULL,
					providerv1.NullPredicateOperator_NULL_PREDICATE_OPERATOR_IS_NOT_NULL,
				},
			},
		},
		Mutations: &providerv1.MutationCapabilities{
			Insert:          true,
			Update:          true,
			Delete:          true,
			GeneratedValues: true,
		},
	}, nil
}

// Health reports whether the in-memory provider is ready to serve requests.
func (p *Provider) Health(
	context.Context,
) (*providerv1.HealthResponse, error) {
	return &providerv1.HealthResponse{
		Healthy: true,
		Message: "in-memory provider is ready",
	}, nil
}

// Schema returns the Kubling DDL exposed by the provider.
func (p *Provider) Schema(context.Context) (string, error) {
	return schemaDDL, nil
}

// Open creates a logical connection to the in-memory data universe.
func (p *Provider) Open(
	_ context.Context,
) (providersdk.Connection, error) {
	return &Connection{
		store: p.store,
	}, nil
}

var (
	_ providersdk.Provider       = (*Provider)(nil)
	_ providersdk.SchemaProvider = (*Provider)(nil)
)
