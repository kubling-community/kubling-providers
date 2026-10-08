package host

import (
	"context"
	"sync"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Connection is a logical connection to the Host Provider.
type Connection struct {
	mu            sync.RWMutex
	closed        bool
	queryExecutor QueryExecutor
}

// Close releases this logical connection.
func (c *Connection) Close(context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

// Begin reports that the host provider has no native local transactions.
func (c *Connection) Begin(ctx context.Context) error {
	if err := c.checkOpen(ctx); err != nil {
		return err
	}
	return transactionsUnsupportedError()
}

// Commit reports that the host provider has no native local transactions.
func (c *Connection) Commit(ctx context.Context) error {
	if err := c.checkOpen(ctx); err != nil {
		return err
	}
	return transactionsUnsupportedError()
}

// Rollback reports that the host provider has no native local transactions.
func (c *Connection) Rollback(ctx context.Context) error {
	if err := c.checkOpen(ctx); err != nil {
		return err
	}
	return transactionsUnsupportedError()
}

// InTransaction reports that no native local transaction can be active.
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
	return c.queryExecutor.Query(ctx, request)
}

// Insert reports that host inventory is read-only.
func (c *Connection) Insert(
	ctx context.Context,
	_ *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	if err := c.checkOpen(ctx); err != nil {
		return nil, err
	}
	return nil, mutationsUnsupportedError()
}

// Update reports that host inventory is read-only.
func (c *Connection) Update(
	ctx context.Context,
	_ *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	if err := c.checkOpen(ctx); err != nil {
		return nil, err
	}
	return nil, mutationsUnsupportedError()
}

// Delete reports that host inventory is read-only.
func (c *Connection) Delete(
	ctx context.Context,
	_ *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	if err := c.checkOpen(ctx); err != nil {
		return nil, err
	}
	return nil, mutationsUnsupportedError()
}

func (c *Connection) checkOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
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

func mutationsUnsupportedError() error {
	return status.Error(codes.Unimplemented, "host inventory is read-only")
}

var _ providersdk.Connection = (*Connection)(nil)
