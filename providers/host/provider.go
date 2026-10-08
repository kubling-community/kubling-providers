package host

import (
	"context"
	"errors"
	"fmt"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/status"
)

// Provider exposes one dynamic host fleet through the Kubling provider API.
type Provider struct {
	readiness     Readiness
	queryExecutor QueryExecutor
	config        Config
}

// Config controls behavior advertised through the northbound provider API.
type Config struct {
	PartialResults bool
}

// QueryExecutor supplies the Host Provider's registry and distributed-scan
// query paths without exposing Agent Gateway internals through the public API.
type QueryExecutor interface {
	Query(context.Context, *providerv1.QueryRequest) (providersdk.ResultStream, error)
}

// New creates a host provider backed by an Agent Gateway query coordinator.
func New(
	readiness Readiness,
	queryExecutor QueryExecutor,
	config Config,
) (*Provider, error) {
	if readiness == nil {
		return nil, errors.New("host provider readiness source is required")
	}
	if queryExecutor == nil {
		return nil, errors.New("host provider query executor is required")
	}
	return &Provider{
		readiness:     readiness,
		queryExecutor: queryExecutor,
		config:        config,
	}, nil
}

// Capabilities reports only behavior implemented by the current host provider.
func (p *Provider) Capabilities(context.Context) (*providersdk.Capabilities, error) {
	maxArrayDimensions := uint32(1)
	return &providersdk.Capabilities{
		Transactions: &providerv1.TransactionCapabilities{Supported: false},
		Query: &providerv1.QueryCapabilities{
			PartialResults: p.config.PartialResults,
			Expressions: &providerv1.ExpressionCapabilities{
				ComparisonOperators: []providerv1.ComparisonOperator{
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
				},
				LogicalOperators: []providerv1.LogicalOperator{
					providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
				},
			},
		},
		Mutations: &providerv1.MutationCapabilities{},
		Values: &providerv1.ValueCapabilities{
			SupportedTypes: []*providerv1.SupportedValueType{
				{Type: kublingv1.ValueType_VALUE_TYPE_STRING, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_INTEGER, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_LONG, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_BIGINTEGER, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_DOUBLE, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_ARRAY, Output: true},
			},
			Features:           []string{grpcfeatures.ArrayValuesV1},
			MaxArrayDimensions: &maxArrayDimensions,
		},
	}, nil
}

// Health reports whether the provider is ready to serve host data.
func (p *Provider) Health(ctx context.Context) (*providerv1.HealthResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := p.readiness.Ready(ctx); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, status.FromContextError(contextErr).Err()
		}
		return &providerv1.HealthResponse{
			Healthy: false,
			Message: fmt.Sprintf("host provider is not ready: %v", err),
		}, nil
	}
	return &providerv1.HealthResponse{
		Healthy: true,
		Message: "host provider is ready",
	}, nil
}

// Metadata returns the stable root model available before agent discovery.
func (*Provider) Metadata(ctx context.Context) (*providersdk.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return baseMetadata(), nil
}

// Open creates a logical connection to the Host Provider.
func (p *Provider) Open(ctx context.Context) (providersdk.Connection, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &Connection{queryExecutor: p.queryExecutor}, nil
}

var (
	_ providersdk.Provider         = (*Provider)(nil)
	_ providersdk.MetadataProvider = (*Provider)(nil)
)
