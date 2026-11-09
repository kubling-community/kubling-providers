package provider

import (
	"context"
	"sync"

	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Connection is one logical Kubling connection to the configured bucket catalog.
type Connection struct {
	mu      sync.RWMutex
	closed  bool
	catalog *catalog.Catalog
	stores  map[string]backend.Store

	lobMu     sync.Mutex
	lobPolicy config.Lobs
	lobs      map[string]lobReference
}

// Close releases connection-scoped LOB references. Backend clients are process-scoped.
func (c *Connection) Close(context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.lobMu.Lock()
	clear(c.lobs)
	c.lobMu.Unlock()
	c.mu.Unlock()
	return nil
}

func (c *Connection) Begin(ctx context.Context) error {
	if err := c.lockOpen(ctx); err != nil {
		return err
	}
	defer c.unlockOpen()
	return transactionsUnsupportedError()
}

func (c *Connection) Commit(ctx context.Context) error {
	if err := c.lockOpen(ctx); err != nil {
		return err
	}
	defer c.unlockOpen()
	return transactionsUnsupportedError()
}

func (c *Connection) Rollback(ctx context.Context) error {
	if err := c.lockOpen(ctx); err != nil {
		return err
	}
	defer c.unlockOpen()
	return transactionsUnsupportedError()
}

func (c *Connection) InTransaction(ctx context.Context) (bool, error) {
	if err := c.lockOpen(ctx); err != nil {
		return false, err
	}
	defer c.unlockOpen()
	return false, transactionsUnsupportedError()
}

func (c *Connection) lockOpen(ctx context.Context) error {
	if err := contextStatus(ctx); err != nil {
		return err
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return status.Error(codes.FailedPrecondition, "connection is closed")
	}
	return nil
}

func (c *Connection) unlockOpen() {
	c.mu.RUnlock()
}

func (c *Connection) route(entity *providerv1.EntityReference) (catalog.Bucket, backend.Store, error) {
	if entity == nil {
		return catalog.Bucket{}, nil, status.Error(codes.InvalidArgument, "entity is required")
	}
	route, found := c.catalog.Resolve(entity.GetNamespace(), entity.GetName())
	if !found {
		return catalog.Bucket{}, nil, status.Errorf(
			codes.NotFound,
			"entity %q in namespace %q is not configured",
			entity.GetName(),
			entity.GetNamespace(),
		)
	}
	store := c.stores[route.BackendName]
	if store == nil {
		return catalog.Bucket{}, nil, status.Error(codes.Internal, "configured backend is unavailable")
	}
	return route, store, nil
}

func transactionsUnsupportedError() error {
	return status.Error(
		codes.Unimplemented,
		"native transactions are not supported; use Kubling soft transactions",
	)
}

var (
	_ providersdk.Connection    = (*Connection)(nil)
	_ providersdk.LobConnection = (*Connection)(nil)
)
