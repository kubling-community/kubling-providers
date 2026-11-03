package provider

import (
	"context"
	"sync"

	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/mutation"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/query"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Connection is a logical connection to the BACnet provider.
type Connection struct {
	mu      sync.RWMutex
	closed  bool
	catalog *catalog.Catalog
	devices DeviceRegistry
}

// Close releases this logical connection. It is safe to call more than once.
func (c *Connection) Close(context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *Connection) Begin(ctx context.Context) error {
	if err := c.checkOpen(ctx); err != nil {
		return err
	}
	return transactionsUnsupportedError()
}

func (c *Connection) Commit(ctx context.Context) error {
	if err := c.checkOpen(ctx); err != nil {
		return err
	}
	return transactionsUnsupportedError()
}

func (c *Connection) Rollback(ctx context.Context) error {
	if err := c.checkOpen(ctx); err != nil {
		return err
	}
	return transactionsUnsupportedError()
}

func (c *Connection) InTransaction(ctx context.Context) (bool, error) {
	if err := c.checkOpen(ctx); err != nil {
		return false, err
	}
	return false, transactionsUnsupportedError()
}

func (c *Connection) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	if err := c.checkOpen(ctx); err != nil {
		return nil, err
	}
	return query.Execute(ctx, c.catalog, c.devices, request)
}

func (c *Connection) Insert(
	ctx context.Context,
	_ *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	if err := c.checkOpen(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "BACnet INSERT is not supported")
}

func (c *Connection) Update(
	ctx context.Context,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	if err := c.checkOpen(ctx); err != nil {
		return nil, err
	}
	return mutation.Execute(ctx, c.catalog, c.devices, request)
}

func (c *Connection) Delete(
	ctx context.Context,
	_ *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	if err := c.checkOpen(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "BACnet DELETE is not supported")
}

func (c *Connection) checkOpen(ctx context.Context) error {
	if err := contextStatus(ctx); err != nil {
		return err
	}
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return status.Error(codes.FailedPrecondition, "connection is closed")
	}
	return nil
}

func transactionsUnsupportedError() error {
	return status.Error(
		codes.Unimplemented,
		"native transactions are not supported; use Kubling soft transactions",
	)
}

var _ providersdk.Connection = (*Connection)(nil)
