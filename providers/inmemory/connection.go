package inmemory

import (
	"context"
	"sync"

	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Connection is a logical connection to one in-memory task store.
type Connection struct {
	mu     sync.RWMutex
	store  *store
	closed bool
}

// Close releases the logical connection.
func (c *Connection) Close(context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	return nil
}

// Begin reports that this provider relies on Kubling soft transactions.
func (c *Connection) Begin(context.Context) error {
	return transactionsUnsupportedError()
}

// Commit reports that this provider relies on Kubling soft transactions.
func (c *Connection) Commit(context.Context) error {
	return transactionsUnsupportedError()
}

// Rollback reports that this provider relies on Kubling soft transactions.
func (c *Connection) Rollback(context.Context) error {
	return transactionsUnsupportedError()
}

// InTransaction reports that no native local transaction can be active.
func (c *Connection) InTransaction(context.Context) (bool, error) {
	return false, transactionsUnsupportedError()
}

func (c *Connection) lockOpen() error {
	c.mu.RLock()

	if c.closed {
		c.mu.RUnlock()
		return status.Error(
			codes.FailedPrecondition,
			"connection is closed",
		)
	}

	return nil
}

func (c *Connection) unlockOpen() {
	c.mu.RUnlock()
}

func transactionsUnsupportedError() error {
	return status.Error(
		codes.Unimplemented,
		"native transactions are not supported; use Kubling soft transactions",
	)
}

var _ providersdk.Connection = (*Connection)(nil)
