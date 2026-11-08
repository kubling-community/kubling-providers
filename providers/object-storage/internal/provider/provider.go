// Package provider implements the Kubling object-storage provider lifecycle.
package provider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider is the process-level object-storage provider.
type Provider struct {
	configuration config.Config
	catalog       *catalog.Catalog
	stores        map[string]backend.Store
}

// New creates a provider from a compiled catalog and initialized backends.
func New(
	configuration config.Config,
	compiled *catalog.Catalog,
	stores map[string]backend.Store,
) (*Provider, error) {
	if compiled == nil {
		return nil, errors.New("object-storage catalog is required")
	}
	if configuration.Lobs.MaxBytes == 0 || configuration.Lobs.MaxChunkBytes == 0 ||
		configuration.Lobs.Retention < time.Second {
		return nil, errors.New("valid object-storage LOB limits are required")
	}
	cloned := make(map[string]backend.Store, len(stores))
	for name, store := range stores {
		if store == nil {
			return nil, fmt.Errorf("backend %q is nil", name)
		}
		cloned[name] = store
	}
	for _, route := range compiled.Buckets() {
		store := stores[route.BackendName]
		if store == nil {
			return nil, fmt.Errorf("backend %q is not initialized", route.BackendName)
		}
	}
	return &Provider{
		configuration: configuration,
		catalog:       compiled,
		stores:        cloned,
	}, nil
}

// Capabilities advertises key-bound queries, streamed BLOB reads and object mutations.
func (p *Provider) Capabilities(ctx context.Context) (*providersdk.Capabilities, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	maxChunkBytes := p.configuration.Lobs.MaxChunkBytes
	maxBytes := p.configuration.Lobs.MaxBytes
	retentionSeconds := uint64(p.configuration.Lobs.Retention.Seconds())
	return &providersdk.Capabilities{
		Transactions: &providerv1.TransactionCapabilities{Supported: false},
		Query: &providerv1.QueryCapabilities{
			Aggregates: &providerv1.AggregateCapabilities{},
			Ordering:   &providerv1.OrderingCapabilities{},
			Pagination: &providerv1.PaginationCapabilities{Limit: true},
			Expressions: &providerv1.ExpressionCapabilities{
				ComparisonOperators: []providerv1.ComparisonOperator{
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
				},
				LogicalOperators: []providerv1.LogicalOperator{
					providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
				},
			},
		},
		Mutations: &providerv1.MutationCapabilities{
			Insert: true,
			Update: true,
			Delete: true,
		},
		Values: &providerv1.ValueCapabilities{
			SupportedTypes: []*providerv1.SupportedValueType{
				{Type: kublingv1.ValueType_VALUE_TYPE_STRING, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_LONG, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_BLOB, Input: true, Output: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_JSON, Input: true, Output: true},
			},
			Features:                     []string{grpcfeatures.LobReadV1},
			MaxLobChunkBytes:             &maxChunkBytes,
			MaxLobBytes:                  &maxBytes,
			LobReferenceRetentionSeconds: &retentionSeconds,
		},
	}, nil
}

// Health verifies that every advertised logical bucket is reachable.
func (p *Provider) Health(ctx context.Context) (*providerv1.HealthResponse, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	for _, route := range p.catalog.Buckets() {
		if err := p.stores[route.BackendName].Probe(ctx, route.Target); err != nil {
			return &providerv1.HealthResponse{
				Healthy: false,
				Message: fmt.Sprintf("logical bucket %q is unavailable", route.Namespace),
			}, nil
		}
	}
	return &providerv1.HealthResponse{
		Healthy: true,
		Message: "object-storage provider is ready",
	}, nil
}

// Metadata returns the fixed logical bucket catalog compiled at startup.
func (p *Provider) Metadata(ctx context.Context) (*providersdk.Metadata, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	return p.catalog.Metadata(), nil
}

// Open creates a logical connection sharing the process-level backend clients.
func (p *Provider) Open(ctx context.Context) (providersdk.Connection, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	return &Connection{
		catalog:   p.catalog,
		stores:    p.stores,
		lobPolicy: p.configuration.Lobs,
		lobs:      make(map[string]lobReference),
	}, nil
}

// Close releases backend resources after the gRPC server has stopped.
func (p *Provider) Close() error {
	var failures []error
	for name, store := range p.stores {
		if err := store.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close backend %q: %w", name, err))
		}
	}
	return errors.Join(failures...)
}

func contextStatus(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

func backendStatus(operation string, err error) error {
	if err == nil || errors.Is(err, backend.ErrNotFound) {
		return err
	}
	if errors.Is(err, backend.ErrConflict) {
		return status.Errorf(codes.Aborted, "%s conflicted with a concurrent object mutation", operation)
	}
	var keyError *backend.KeyError
	if errors.As(err, &keyError) {
		return status.Errorf(codes.InvalidArgument, "%s: %v", operation, keyError)
	}
	slog.Error("object-storage backend operation failed", "operation", operation, "error", err)
	return status.Errorf(codes.Unavailable, "%s failed", operation)
}

func normalizeName(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

var (
	_ providersdk.Provider         = (*Provider)(nil)
	_ providersdk.MetadataProvider = (*Provider)(nil)
)
