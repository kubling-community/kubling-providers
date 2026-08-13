package openapi

import (
	"context"
	"sync"

	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Connection struct {
	provider *Provider
	mu       sync.RWMutex
	closed   bool
}

func (c *Connection) Close(context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *Connection) ensureOpen() error {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return status.Error(codes.FailedPrecondition, "OpenAPI connection is closed")
	}
	return nil
}

func (*Connection) Begin(context.Context) error {
	return status.Error(codes.Unimplemented, "OpenAPI native transactions are not supported")
}

func (*Connection) Commit(context.Context) error {
	return status.Error(codes.Unimplemented, "OpenAPI native transactions are not supported")
}

func (*Connection) Rollback(context.Context) error {
	return status.Error(codes.Unimplemented, "OpenAPI native transactions are not supported")
}

func (*Connection) InTransaction(context.Context) (bool, error) {
	return false, status.Error(codes.Unimplemented, "OpenAPI native transactions are not supported")
}

var _ providersdk.Connection = (*Connection)(nil)
