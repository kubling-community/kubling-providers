package cassandra

import (
	"context"
	"sync"

	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Connection is a logical connection to the provider's Cassandra data universe.
type Connection struct {
	mu       sync.RWMutex
	provider *Provider
	sessions map[string]driverSession
	closed   bool
}

// Close marks this logical connection as closed.
func (c *Connection) Close(context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	sessions := c.sessions
	c.sessions = nil
	c.mu.Unlock()

	for namespace, session := range sessions {
		c.provider.releaseSession(namespace, session)
	}

	return nil
}

// Begin reports that Cassandra has no provider-level native transaction.
func (c *Connection) Begin(context.Context) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	return transactionsUnsupportedError()
}

// Commit reports that Cassandra has no provider-level native transaction.
func (c *Connection) Commit(context.Context) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	return transactionsUnsupportedError()
}

// Rollback reports that Cassandra has no provider-level native transaction.
func (c *Connection) Rollback(context.Context) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	return transactionsUnsupportedError()
}

// InTransaction reports that no native Cassandra transaction can be active.
func (c *Connection) InTransaction(context.Context) (bool, error) {
	if err := c.ensureOpen(); err != nil {
		return false, err
	}
	return false, transactionsUnsupportedError()
}

func (c *Connection) ensureOpen() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return status.Error(codes.FailedPrecondition, "connection is closed")
	}
	return nil
}

func (c *Connection) session(
	ctx context.Context,
	namespace string,
	config DataSourceConfig,
) (driverSession, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, "connection is closed")
	}
	if session := c.sessions[namespace]; session != nil {
		c.mu.RUnlock()
		return session, nil
	}
	c.mu.RUnlock()

	session, err := c.provider.acquireSession(ctx, namespace, config)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.provider.releaseSession(namespace, session)
		return nil, status.Error(codes.FailedPrecondition, "connection is closed")
	}
	if existing := c.sessions[namespace]; existing != nil {
		c.mu.Unlock()
		c.provider.releaseSession(namespace, session)
		return existing, nil
	}
	c.sessions[namespace] = session
	c.mu.Unlock()

	return session, nil
}

func transactionsUnsupportedError() error {
	return status.Error(
		codes.Unimplemented,
		"native transactions are not supported; use Kubling soft transactions",
	)
}

var _ providersdk.Connection = (*Connection)(nil)
