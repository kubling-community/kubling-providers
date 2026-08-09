package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/discovery"
)

// Provider exposes one Kubernetes cluster as one Kubling data source.
type Provider struct {
	mu           sync.Mutex
	config       Config
	client       *clientEntry
	newClient    clientFactory
	openAPICache openAPISchemaCache
}

type clientEntry struct {
	ready      chan struct{}
	client     kubernetesClient
	err        error
	references int
}

// New creates a provider for exactly one Kubernetes cluster.
func New(config Config) (*Provider, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	return newProvider(normalized, newClusterClient), nil
}

func newProvider(config Config, factory clientFactory) *Provider {
	return &Provider{config: config, newClient: factory}
}

// Capabilities reports only the operations currently implemented.
func (*Provider) Capabilities(context.Context) (*providersdk.Capabilities, error) {
	return &providersdk.Capabilities{
		Transactions: &providerv1.TransactionCapabilities{Supported: false},
		Query: &providerv1.QueryCapabilities{
			Ordering:   &providerv1.OrderingCapabilities{Supported: false},
			Pagination: &providerv1.PaginationCapabilities{Limit: true, Offset: false},
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
	}, nil
}

// Health checks the readiness endpoint of the configured cluster.
func (p *Provider) Health(ctx context.Context) (*providerv1.HealthResponse, error) {
	entry, err := p.acquireClient(ctx)
	if err != nil {
		return nil, err
	}
	healthErr := entry.client.Health(ctx)
	closeErr := p.releaseClient(entry)
	if healthErr != nil {
		if errors.Is(healthErr, context.Canceled) || errors.Is(healthErr, context.DeadlineExceeded) {
			return nil, status.FromContextError(healthErr).Err()
		}
		return &providerv1.HealthResponse{
			Healthy: false,
			Message: fmt.Sprintf("Kubernetes cluster is unavailable: %v", healthErr),
		}, nil
	}
	if closeErr != nil {
		return nil, operationError("close Kubernetes health client", closeErr)
	}
	return &providerv1.HealthResponse{
		Healthy: true,
		Message: "Kubernetes cluster is ready",
	}, nil
}

// Metadata discovers every listable resource in the cluster's preferred API
// versions without opening a logical provider connection.
func (p *Provider) Metadata(ctx context.Context) (*providersdk.Metadata, error) {
	entry, err := p.acquireClient(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = p.releaseClient(entry)
		return nil, status.FromContextError(err).Err()
	}

	discoveryClient := entry.client.Discovery()
	if discoveryClient == nil {
		_ = p.releaseClient(entry)
		return nil, status.Error(codes.Internal, "Kubernetes client returned nil discovery interface")
	}

	resourceLists, discoveryErr := discoveryClient.ServerPreferredResources()
	defaultNamespace := entry.client.DefaultNamespace()
	if err := ctx.Err(); err != nil {
		_ = p.releaseClient(entry)
		return nil, status.FromContextError(err).Err()
	}

	failedGroupVersions := failedDiscoveryGroups(discoveryErr)
	if discoveryErr != nil && len(failedGroupVersions) == 0 {
		_ = p.releaseClient(entry)
		return nil, operationError("discover Kubernetes resources", discoveryErr)
	}

	var resolver *openAPISchemaResolver
	if schemaExpansionConfigured(p.config.Schema) {
		resolver = newOpenAPISchemaResolver(ctx, discoveryClient, &p.openAPICache)
	}

	metadata := buildMetadataWithSchema(
		resourceLists,
		failedGroupVersions,
		p.config.Schema,
		resolver,
	)
	applyNamespaceInsertDefaults(metadata, p.config.BlankNamespaceStrategy, defaultNamespace)

	if err := ctx.Err(); err != nil {
		_ = p.releaseClient(entry)
		return nil, status.FromContextError(err).Err()
	}
	if len(metadata.GetTables()) == 0 {
		_ = p.releaseClient(entry)
		if discoveryErr != nil {
			return nil, operationError("discover Kubernetes resources", discoveryErr)
		}
		return nil, status.Error(codes.FailedPrecondition, "Kubernetes discovery returned no configured listable resources")
	}

	if closeErr := p.releaseClient(entry); closeErr != nil {
		return nil, operationError("close Kubernetes metadata client", closeErr)
	}
	return metadata, nil
}

func failedDiscoveryGroups(err error) []string {
	groups, partial := discovery.GroupDiscoveryFailedErrorGroups(err)
	if !partial {
		return nil
	}
	failed := make([]string, 0, len(groups))
	for groupVersion := range groups {
		failed = append(failed, groupVersion.String())
	}
	sort.Strings(failed)
	return failed
}

// Open creates a logical connection to the configured cluster.
func (p *Provider) Open(context.Context) (providersdk.Connection, error) {
	return &Connection{provider: p}, nil
}

func (p *Provider) acquireClient(ctx context.Context) (*clientEntry, error) {
	for {
		p.mu.Lock()
		entry := p.client
		if entry != nil && entry.ready == nil {
			entry.references++
			p.mu.Unlock()
			return entry, nil
		}
		if entry != nil {
			ready := entry.ready
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			case <-ready:
			}

			p.mu.Lock()
			if entry.err != nil {
				err := entry.err
				p.mu.Unlock()
				return nil, operationError("open Kubernetes cluster", err)
			}
			if p.client != entry {
				p.mu.Unlock()
				continue
			}
			entry.references++
			p.mu.Unlock()
			return entry, nil
		}

		entry = &clientEntry{ready: make(chan struct{})}
		p.client = entry
		p.mu.Unlock()

		client, err := p.newClient(ctx, p.config)
		if err == nil && client == nil {
			err = status.Error(codes.Internal, "Kubernetes client factory returned nil")
		}

		p.mu.Lock()
		entry.err = err
		if err != nil {
			p.client = nil
			close(entry.ready)
			p.mu.Unlock()
			return nil, operationError("open Kubernetes cluster", err)
		}
		entry.client = client
		entry.references = 1
		close(entry.ready)
		entry.ready = nil
		p.mu.Unlock()
		return entry, nil
	}
}

func (p *Provider) releaseClient(entry *clientEntry) error {
	p.mu.Lock()
	if p.client != entry || entry.references == 0 {
		p.mu.Unlock()
		return nil
	}
	entry.references--
	if entry.references > 0 {
		p.mu.Unlock()
		return nil
	}
	p.client = nil
	client := entry.client
	p.mu.Unlock()
	return client.Close()
}

func operationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	return status.Errorf(codes.Unavailable, "%s: %v", operation, err)
}

var (
	_ providersdk.Provider         = (*Provider)(nil)
	_ providersdk.MetadataProvider = (*Provider)(nil)
)
