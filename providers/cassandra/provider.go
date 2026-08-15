package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider aggregates configured Cassandra namespaces into one data universe.
type Provider struct {
	mu         sync.Mutex
	config     Config
	sessions   map[string]*sessionEntry
	newSession sessionFactory
}

type sessionEntry struct {
	ready      chan struct{}
	session    driverSession
	err        error
	references int
}

// New creates a Cassandra provider from validated local configuration.
func New(config Config) (*Provider, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}

	return newProvider(normalized, createSession), nil
}

func newProvider(config Config, factory sessionFactory) *Provider {
	return &Provider{
		config:     config,
		sessions:   make(map[string]*sessionEntry),
		newSession: factory,
	}
}

// Capabilities reports the exact Cassandra subset implemented by this provider.
func (p *Provider) Capabilities(
	context.Context,
) (*providersdk.Capabilities, error) {
	return &providersdk.Capabilities{
		Transactions: &providerv1.TransactionCapabilities{Supported: false},
		Query: &providerv1.QueryCapabilities{
			Ordering: &providerv1.OrderingCapabilities{
				Supported:            true,
				ExplicitNullOrdering: false,
			},
			Pagination: &providerv1.PaginationCapabilities{
				Limit:  true,
				Offset: false,
			},
			Expressions: &providerv1.ExpressionCapabilities{
				ComparisonOperators: []providerv1.ComparisonOperator{
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN,
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL,
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN,
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL,
				},
				LogicalOperators: []providerv1.LogicalOperator{
					providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
				},
			},
		},
		Mutations: &providerv1.MutationCapabilities{
			Insert:          true,
			Update:          true,
			Delete:          true,
			GeneratedValues: false,
		},
	}, nil
}

// Health reports whether the provider configuration is ready to accept opens.
func (p *Provider) Health(
	context.Context,
) (*providerv1.HealthResponse, error) {
	return &providerv1.HealthResponse{
		Healthy: true,
		Message: fmt.Sprintf(
			"Cassandra provider configured with %d namespace(s)",
			len(p.config.DataSources),
		),
	}, nil
}

// Metadata discovers and aggregates every configured Cassandra namespace.
func (p *Provider) Metadata(
	ctx context.Context,
) (*providersdk.Metadata, error) {
	namespaces := make([]string, 0, len(p.config.DataSources))
	for namespace := range p.config.DataSources {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)

	metadata := &providersdk.Metadata{
		Properties: map[string]string{
			"cassandra.namespace_count": strconv.Itoa(len(namespaces)),
		},
	}
	for _, namespace := range namespaces {
		dataSource := p.config.DataSources[namespace]
		session, err := p.acquireSession(ctx, namespace, dataSource)
		if err != nil {
			return nil, err
		}

		keyspace, metadataErr := session.KeyspaceMetadata(dataSource.Keyspace)
		p.releaseSession(namespace, session)
		if metadataErr != nil {
			return nil, newStatusError(
				codes.Unavailable,
				fmt.Sprintf(
					"discover metadata for Cassandra namespace %q: %v",
					namespace,
					metadataErr,
				),
				metadataErr,
			)
		}
		if keyspace == nil {
			return nil, status.Errorf(
				codes.Internal,
				"Cassandra driver returned nil metadata for namespace %q",
				namespace,
			)
		}

		discovered := schemaMetadata(namespace, keyspace)
		metadata.Namespaces = append(metadata.Namespaces, discovered.Namespaces...)
		metadata.Tables = append(metadata.Tables, discovered.Tables...)
	}
	if p.config.NamespaceColumn.Enabled {
		if err := providersdk.AddNamespaceColumns(metadata, providersdk.NamespaceColumnOptions{
			ColumnName: p.config.NamespaceColumn.Name,
		}); err != nil {
			return nil, fmt.Errorf("add Cassandra namespace columns: %w", err)
		}
	}

	return metadata, nil
}

// Open creates a logical connection to the complete Cassandra data universe.
func (p *Provider) Open(
	context.Context,
) (providersdk.Connection, error) {
	return &Connection{
		provider: p,
		sessions: make(map[string]driverSession),
	}, nil
}

func (p *Provider) acquireSession(
	ctx context.Context,
	dataSourceRef string,
	config DataSourceConfig,
) (driverSession, error) {
	for {
		p.mu.Lock()
		entry, exists := p.sessions[dataSourceRef]
		if exists && entry.ready == nil {
			entry.references++
			session := entry.session
			p.mu.Unlock()
			return session, nil
		}
		if exists {
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
				return nil, openSessionError(dataSourceRef, err)
			}
			if p.sessions[dataSourceRef] != entry {
				p.mu.Unlock()
				continue
			}
			entry.references++
			session := entry.session
			p.mu.Unlock()
			return session, nil
		}

		entry = &sessionEntry{ready: make(chan struct{})}
		p.sessions[dataSourceRef] = entry
		p.mu.Unlock()

		session, err := p.newSession(ctx, config)
		if err == nil && session == nil {
			err = status.Error(codes.Internal, "Cassandra session factory returned nil")
		}

		p.mu.Lock()
		entry.err = err
		if err != nil {
			delete(p.sessions, dataSourceRef)
			close(entry.ready)
			p.mu.Unlock()
			return nil, openSessionError(dataSourceRef, err)
		}

		entry.session = session
		entry.references = 1
		close(entry.ready)
		entry.ready = nil
		p.mu.Unlock()

		return session, nil
	}
}

func (p *Provider) releaseSession(
	dataSourceRef string,
	session driverSession,
) {
	p.mu.Lock()
	entry := p.sessions[dataSourceRef]
	if entry == nil || entry.session != session || entry.references == 0 {
		p.mu.Unlock()
		return
	}

	entry.references--
	if entry.references > 0 {
		p.mu.Unlock()
		return
	}

	delete(p.sessions, dataSourceRef)
	p.mu.Unlock()

	session.Close()
}

func openSessionError(dataSourceRef string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if status.Code(err) != codes.Unknown {
		return err
	}

	return newStatusError(
		codes.Unavailable,
		fmt.Sprintf("open Cassandra data source %q: %v", dataSourceRef, err),
		err,
	)
}

func newStatusError(code codes.Code, message string, cause error) error {
	return &statusError{
		status: status.New(code, message),
		cause:  cause,
	}
}

type statusError struct {
	status *status.Status
	cause  error
}

func (e *statusError) Error() string {
	return e.status.Err().Error()
}

func (e *statusError) Unwrap() error {
	return e.cause
}

func (e *statusError) GRPCStatus() *status.Status {
	return e.status
}

var (
	_ providersdk.Provider         = (*Provider)(nil)
	_ providersdk.MetadataProvider = (*Provider)(nil)
)
