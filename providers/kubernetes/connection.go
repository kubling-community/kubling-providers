package kubernetes

import (
	"context"
	"sync"

	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Connection is a logical connection to the provider's Kubernetes cluster.
type Connection struct {
	mu       sync.RWMutex
	provider *Provider
	client   *clientEntry
	closed   bool
}

// Close releases this connection's reference to the shared cluster client.
func (c *Connection) Close(context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	entry := c.client
	c.client = nil
	c.mu.Unlock()
	if entry == nil {
		return nil
	}
	return c.provider.releaseClient(entry)
}

func (c *Connection) clientFor(ctx context.Context) (kubernetesClient, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, connectionClosedError()
	}
	if c.client != nil {
		client := c.client.client
		c.mu.RUnlock()
		return client, nil
	}
	c.mu.RUnlock()

	entry, err := c.provider.acquireClient(ctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = c.provider.releaseClient(entry)
		return nil, connectionClosedError()
	}
	if c.client != nil {
		client := c.client.client
		c.mu.Unlock()
		_ = c.provider.releaseClient(entry)
		return client, nil
	}
	c.client = entry
	c.mu.Unlock()
	return entry.client, nil
}

// Begin reports that Kubernetes has no native transaction support.
func (c *Connection) Begin(context.Context) error {
	return c.unsupported("native transactions are not supported; use Kubling soft transactions")
}

// Commit reports that Kubernetes has no native transaction support.
func (c *Connection) Commit(context.Context) error {
	return c.unsupported("native transactions are not supported; use Kubling soft transactions")
}

// Rollback reports that Kubernetes has no native transaction support.
func (c *Connection) Rollback(context.Context) error {
	return c.unsupported("native transactions are not supported; use Kubling soft transactions")
}

// InTransaction reports that no native Kubernetes transaction can be active.
func (c *Connection) InTransaction(context.Context) (bool, error) {
	if err := c.ensureOpen(); err != nil {
		return false, err
	}
	return false, status.Error(codes.Unimplemented, "native transactions are not supported; use Kubling soft transactions")
}

func (c *Connection) unsupported(message string) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	return status.Error(codes.Unimplemented, message)
}

func (c *Connection) ensureOpen() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return connectionClosedError()
	}
	return nil
}

func connectionClosedError() error {
	return status.Error(codes.FailedPrecondition, "connection is closed")
}

var _ providersdk.Connection = (*Connection)(nil)
