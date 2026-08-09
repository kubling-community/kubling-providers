package provider

import (
	"context"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

// Connection represents an open logical connection to a data source.
//
// The SDK associates each Connection with its public connection identifier.
type Connection interface {
	// Close releases all resources associated with the connection.
	Close(context.Context) error

	// Begin starts a native local transaction.
	Begin(context.Context) error

	// Commit commits the active native transaction.
	Commit(context.Context) error

	// Rollback rolls back the active native transaction.
	Rollback(context.Context) error

	// InTransaction reports whether a native transaction is active.
	InTransaction(context.Context) (bool, error)

	// Query executes a query and returns its result as a sequence of batches.
	Query(context.Context, *providerv1.QueryRequest) (ResultStream, error)

	// Insert inserts one or more tuples.
	Insert(
		context.Context,
		*providerv1.InsertRequest,
	) (*providerv1.InsertResponse, error)

	// Update updates the tuples selected by the request.
	Update(
		context.Context,
		*providerv1.UpdateRequest,
	) (*providerv1.UpdateResponse, error)

	// Delete deletes the tuples selected by the request.
	Delete(
		context.Context,
		*providerv1.DeleteRequest,
	) (*providerv1.DeleteResponse, error)
}

// ResultStream produces query result batches.
//
// Next returns io.EOF when no more batches are available. Close must release
// any resources held by the underlying data source, even after Next returns
// io.EOF.
type ResultStream interface {
	Next(context.Context) (*providerv1.TupleBatch, error)
	Close() error
}
