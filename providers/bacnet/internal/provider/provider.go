// Package provider implements the Kubling BACnet provider lifecycle.
package provider

import (
	"context"
	"fmt"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/mutation"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/query"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/status"
)

// Provider is the process-level BACnet provider implementation.
type Provider struct {
	catalog *catalog.Catalog
	devices DeviceRegistry
}

// DeviceRegistry supplies stable snapshots and bounded physical I/O.
type DeviceRegistry interface {
	query.DeviceSource
	mutation.DeviceSource
}

// New creates the provider around its compiled catalog and device registry.
func New(compiled *catalog.Catalog, devices DeviceRegistry) *Provider {
	return &Provider{catalog: compiled, devices: devices}
}

func (p *Provider) String() string {
	if p == nil {
		return "BACnet provider (nil)"
	}
	return "BACnet provider"
}

func (p *Provider) GoString() string {
	return p.String()
}

// Capabilities advertises bounded single-device queries and updates.
func (p *Provider) Capabilities(ctx context.Context) (*providersdk.Capabilities, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	maxArrayDimensions := uint32(1)
	return &providersdk.Capabilities{
		Transactions: &providerv1.TransactionCapabilities{},
		Query: &providerv1.QueryCapabilities{
			Ordering:   &providerv1.OrderingCapabilities{},
			Pagination: &providerv1.PaginationCapabilities{},
			Expressions: &providerv1.ExpressionCapabilities{
				ComparisonOperators: []providerv1.ComparisonOperator{
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
				},
				LogicalOperators: []providerv1.LogicalOperator{
					providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
				},
			},
			Aggregates: &providerv1.AggregateCapabilities{},
		},
		Mutations: &providerv1.MutationCapabilities{Update: true},
		Values: &providerv1.ValueCapabilities{
			SupportedTypes: []*providerv1.SupportedValueType{
				{Type: kublingv1.ValueType_VALUE_TYPE_STRING, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_VARBINARY, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_BOOLEAN, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_INTEGER, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_LONG, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_BIGINTEGER, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_FLOAT, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_DOUBLE, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_DATE, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_TIME, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_ARRAY, Output: true},
			},
			Features:           []string{grpcfeatures.ArrayValuesV1},
			MaxArrayDimensions: &maxArrayDimensions,
		},
	}, nil
}

// Health reports process health independently from future BACnet attachments.
func (p *Provider) Health(ctx context.Context) (*providerv1.HealthResponse, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	return &providerv1.HealthResponse{
		Healthy: true,
		Message: "BACnet provider process is ready",
	}, nil
}

// Metadata returns the fixed union of the profiles compiled at startup.
func (p *Provider) Metadata(ctx context.Context) (*providersdk.Metadata, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	return p.catalog.Metadata(), nil
}

// SemanticFragment returns the semantic model derived from the same fixed
// catalog as Metadata.
func (p *Provider) SemanticFragment(
	ctx context.Context,
) (*providersdk.SemanticFragment, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	return p.catalog.SemanticFragment(), nil
}

// Open creates a protocol-neutral logical connection.
func (p *Provider) Open(ctx context.Context) (providersdk.Connection, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	return &Connection{catalog: p.catalog, devices: p.devices}, nil
}

func contextStatus(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

var (
	_ providersdk.Provider                 = (*Provider)(nil)
	_ providersdk.MetadataProvider         = (*Provider)(nil)
	_ providersdk.SemanticFragmentProvider = (*Provider)(nil)
	_ fmt.Stringer                         = (*Provider)(nil)
)
