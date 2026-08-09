package provider

import (
	"context"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

// BeginTransaction starts a native transaction on the logical connection.
func (s *Server) BeginTransaction(
	ctx context.Context,
	request *providerv1.BeginTransactionRequest,
) (*providerv1.BeginTransactionResponse, error) {
	connection, release, err :=
		s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer release()

	if err := connection.Begin(ctx); err != nil {
		return nil, err
	}

	return &providerv1.BeginTransactionResponse{}, nil
}

// CommitTransaction commits the active native transaction.
func (s *Server) CommitTransaction(
	ctx context.Context,
	request *providerv1.CommitTransactionRequest,
) (*providerv1.CommitTransactionResponse, error) {
	connection, release, err :=
		s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer release()

	if err := connection.Commit(ctx); err != nil {
		return nil, err
	}

	return &providerv1.CommitTransactionResponse{}, nil
}

// RollbackTransaction rolls back the active native transaction.
func (s *Server) RollbackTransaction(
	ctx context.Context,
	request *providerv1.RollbackTransactionRequest,
) (*providerv1.RollbackTransactionResponse, error) {
	connection, release, err :=
		s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer release()

	if err := connection.Rollback(ctx); err != nil {
		return nil, err
	}

	return &providerv1.RollbackTransactionResponse{}, nil
}

// IsInTransaction reports whether a native transaction is active.
func (s *Server) IsInTransaction(
	ctx context.Context,
	request *providerv1.IsInTransactionRequest,
) (*providerv1.IsInTransactionResponse, error) {
	connection, release, err :=
		s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer release()

	active, err := connection.InTransaction(ctx)
	if err != nil {
		return nil, err
	}

	return &providerv1.IsInTransactionResponse{
		Active: active,
	}, nil
}
