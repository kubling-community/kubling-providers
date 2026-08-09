package redis

import (
	"context"
	"errors"
	"strings"
	"sync"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Connection is a logical connection to the provider's Redis data universe.
type Connection struct {
	mu       sync.RWMutex
	provider *Provider
	clients  map[string]redisClient
	closed   bool
}

type resolvedTable struct {
	namespace string
	config    NamespaceConfig
	table     *TableConfig
	client    redisClient
}

// Close releases every Redis client held by this logical connection.
func (c *Connection) Close(context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	clients := c.clients
	c.clients = nil
	c.mu.Unlock()

	var closeErr error
	for namespace, client := range clients {
		closeErr = errors.Join(closeErr, c.provider.releaseClient(namespace, client))
	}
	return closeErr
}

func (c *Connection) Begin(context.Context) error    { return transactionsUnsupportedError() }
func (c *Connection) Commit(context.Context) error   { return transactionsUnsupportedError() }
func (c *Connection) Rollback(context.Context) error { return transactionsUnsupportedError() }

func (c *Connection) InTransaction(context.Context) (bool, error) {
	return false, transactionsUnsupportedError()
}

func (c *Connection) resolveTable(
	ctx context.Context,
	reference *providerv1.EntityReference,
) (*resolvedTable, error) {
	if reference == nil || strings.TrimSpace(reference.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "entity is required")
	}
	namespace := reference.GetNamespace()
	if namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "Redis entities require a namespace")
	}
	config, exists := c.provider.config.Namespaces[namespace]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "Redis namespace %q was not found", namespace)
	}
	table := config.tablesByName[strings.ToUpper(reference.GetName())]
	if table == nil {
		return nil, status.Errorf(codes.NotFound, "entity %q was not found in namespace %q", reference.GetName(), namespace)
	}
	client, err := c.client(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return &resolvedTable{namespace: namespace, config: config, table: table, client: client}, nil
}

func (c *Connection) client(ctx context.Context, namespace string) (redisClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, "connection is closed")
	}
	if client := c.clients[namespace]; client != nil {
		c.mu.RUnlock()
		return client, nil
	}
	c.mu.RUnlock()

	client := c.provider.acquireClient(namespace)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = c.provider.releaseClient(namespace, client)
		return nil, status.Error(codes.FailedPrecondition, "connection is closed")
	}
	if existing := c.clients[namespace]; existing != nil {
		c.mu.Unlock()
		_ = c.provider.releaseClient(namespace, client)
		return existing, nil
	}
	c.clients[namespace] = client
	c.mu.Unlock()
	return client, nil
}

func transactionsUnsupportedError() error {
	return status.Error(codes.Unimplemented, "native transactions are not supported; use Kubling soft transactions")
}

var _ providersdk.Connection = (*Connection)(nil)
