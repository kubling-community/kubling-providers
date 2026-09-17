package inmemory

import (
	"context"
	_ "embed"
	"time"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
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
	maxArrayDimensions := uint32(1)
	maxLobChunkBytes := uint32(inMemoryMaxLobChunkBytes)
	maxLobBytes := uint64(inMemoryMaxLobBytes)
	lobRetentionSeconds := uint64(inMemoryLobRetention / time.Second)
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
		Values: &providerv1.ValueCapabilities{
			SupportedTypes: supportedValueTypes(),
			Features: []string{
				grpcfeatures.ArrayValuesV1,
				grpcfeatures.SpatialValuesV1,
				grpcfeatures.LobReadV1,
			},
			MaxArrayDimensions:           &maxArrayDimensions,
			MaxLobChunkBytes:             &maxLobChunkBytes,
			MaxLobBytes:                  &maxLobBytes,
			LobReferenceRetentionSeconds: &lobRetentionSeconds,
		},
	}, nil
}

func supportedValueTypes() []*providerv1.SupportedValueType {
	types := make(
		[]*providerv1.SupportedValueType,
		0,
		int(kublingv1.ValueType_VALUE_TYPE_ARRAY),
	)
	for valueType := kublingv1.ValueType_VALUE_TYPE_STRING; valueType <= kublingv1.ValueType_VALUE_TYPE_ARRAY; valueType++ {
		types = append(types, &providerv1.SupportedValueType{
			Type:   valueType,
			Input:  true,
			Output: true,
		})
	}

	return types
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
		lobs:  make(map[string]inMemoryLob),
	}, nil
}

var (
	_ providersdk.Provider       = (*Provider)(nil)
	_ providersdk.SchemaProvider = (*Provider)(nil)
)
