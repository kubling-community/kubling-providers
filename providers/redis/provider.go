package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider exposes configured Redis databases as one logical data universe.
type Provider struct {
	mu        sync.Mutex
	config    Config
	clients   map[string]*clientEntry
	newClient clientFactory
}

type clientEntry struct {
	client     redisClient
	references int
}

// New creates a Redis provider from validated local configuration.
func New(config Config) (*Provider, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	return newProvider(normalized, newRedisClient), nil
}

func newProvider(config Config, factory clientFactory) *Provider {
	return &Provider{
		config:    config,
		clients:   make(map[string]*clientEntry),
		newClient: factory,
	}
}

// Capabilities reports the exact first-version Redis Hash surface.
func (*Provider) Capabilities(context.Context) (*providersdk.Capabilities, error) {
	return &providersdk.Capabilities{
		Transactions: &providerv1.TransactionCapabilities{Supported: false},
		Query: &providerv1.QueryCapabilities{
			RequiresCriteria: false,
			Ordering: &providerv1.OrderingCapabilities{
				Supported:            true,
				ExplicitNullOrdering: true,
				DefaultNullOrder:     providerv1.DefaultNullOrder_DEFAULT_NULL_ORDER_LAST,
			},
			Pagination: &providerv1.PaginationCapabilities{Limit: true, Offset: true},
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
				},
				NullPredicateOperators: []providerv1.NullPredicateOperator{
					providerv1.NullPredicateOperator_NULL_PREDICATE_OPERATOR_IS_NULL,
					providerv1.NullPredicateOperator_NULL_PREDICATE_OPERATOR_IS_NOT_NULL,
				},
				PatternOperators: []providerv1.PatternOperator{
					providerv1.PatternOperator_PATTERN_OPERATOR_LIKE,
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

// Metadata returns the provider-owned relational model without contacting Redis.
func (p *Provider) Metadata(context.Context) (*providersdk.Metadata, error) {
	return buildMetadata(p.config), nil
}

// Health pings every configured Redis namespace.
func (p *Provider) Health(ctx context.Context) (*providerv1.HealthResponse, error) {
	for _, namespace := range sortedNamespaces(p.config) {
		client := p.acquireClient(namespace)
		err := client.Ping(ctx)
		closeErr := p.releaseClient(namespace, client)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, status.FromContextError(err).Err()
			}
			return &providerv1.HealthResponse{
				Healthy: false,
				Message: fmt.Sprintf("Redis namespace %q is unavailable: %v", namespace, err),
			}, nil
		}
		if closeErr != nil {
			return nil, operationError("close Redis health client", closeErr)
		}
	}
	return &providerv1.HealthResponse{
		Healthy: true,
		Message: fmt.Sprintf("Redis provider reached %d namespace(s)", len(p.config.Namespaces)),
	}, nil
}

// Open creates a logical connection to the complete Redis data universe.
func (p *Provider) Open(context.Context) (providersdk.Connection, error) {
	return &Connection{
		provider: p,
		clients:  make(map[string]redisClient),
	}, nil
}

func (p *Provider) acquireClient(namespace string) redisClient {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry := p.clients[namespace]; entry != nil {
		entry.references++
		return entry.client
	}
	client := p.newClient(p.config.Namespaces[namespace])
	p.clients[namespace] = &clientEntry{client: client, references: 1}
	return client
}

func (p *Provider) releaseClient(namespace string, client redisClient) error {
	p.mu.Lock()
	entry := p.clients[namespace]
	if entry == nil || entry.client != client || entry.references == 0 {
		p.mu.Unlock()
		return nil
	}
	entry.references--
	if entry.references > 0 {
		p.mu.Unlock()
		return nil
	}
	delete(p.clients, namespace)
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
	return newStatusError(codes.Unavailable, fmt.Sprintf("%s: %v", operation, err), err)
}

func newStatusError(code codes.Code, message string, cause error) error {
	return &statusError{status: status.New(code, message), cause: cause}
}

type statusError struct {
	status *status.Status
	cause  error
}

func (e *statusError) Error() string              { return e.status.Err().Error() }
func (e *statusError) Unwrap() error              { return e.cause }
func (e *statusError) GRPCStatus() *status.Status { return e.status }

var (
	_ providersdk.Provider         = (*Provider)(nil)
	_ providersdk.MetadataProvider = (*Provider)(nil)
)
