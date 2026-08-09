package provider

import (
	"context"
	"errors"
	"testing"
	"time"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type transactionTestConnection struct {
	Connection

	beginFunc         func(context.Context) error
	commitFunc        func(context.Context) error
	rollbackFunc      func(context.Context) error
	inTransactionFunc func(context.Context) (bool, error)
}

func (c *transactionTestConnection) Close(context.Context) error {
	return nil
}

func (c *transactionTestConnection) Begin(ctx context.Context) error {
	if c.beginFunc == nil {
		panic("unexpected Begin call")
	}

	return c.beginFunc(ctx)
}

func (c *transactionTestConnection) Commit(ctx context.Context) error {
	if c.commitFunc == nil {
		panic("unexpected Commit call")
	}

	return c.commitFunc(ctx)
}

func (c *transactionTestConnection) Rollback(ctx context.Context) error {
	if c.rollbackFunc == nil {
		panic("unexpected Rollback call")
	}

	return c.rollbackFunc(ctx)
}

func (c *transactionTestConnection) InTransaction(
	ctx context.Context,
) (bool, error) {
	if c.inTransactionFunc == nil {
		panic("unexpected InTransaction call")
	}

	return c.inTransactionFunc(ctx)
}

type transactionRPC func(
	context.Context,
	*Server,
	string,
) (bool, error)

type transactionOperation struct {
	name      string
	setMethod func(
		*transactionTestConnection,
		func(context.Context) error,
	)
	invoke transactionRPC
}

func TestServerTransactionLifecycle(t *testing.T) {
	operations := []transactionOperation{
		{
			name: "begin",
			setMethod: func(
				connection *transactionTestConnection,
				method func(context.Context) error,
			) {
				connection.beginFunc = method
			},
			invoke: func(
				ctx context.Context,
				server *Server,
				connectionID string,
			) (bool, error) {
				response, err := server.BeginTransaction(
					ctx,
					&providerv1.BeginTransactionRequest{
						ConnectionId: connectionID,
					},
				)

				return response != nil, err
			},
		},
		{
			name: "commit",
			setMethod: func(
				connection *transactionTestConnection,
				method func(context.Context) error,
			) {
				connection.commitFunc = method
			},
			invoke: func(
				ctx context.Context,
				server *Server,
				connectionID string,
			) (bool, error) {
				response, err := server.CommitTransaction(
					ctx,
					&providerv1.CommitTransactionRequest{
						ConnectionId: connectionID,
					},
				)

				return response != nil, err
			},
		},
		{
			name: "rollback",
			setMethod: func(
				connection *transactionTestConnection,
				method func(context.Context) error,
			) {
				connection.rollbackFunc = method
			},
			invoke: func(
				ctx context.Context,
				server *Server,
				connectionID string,
			) (bool, error) {
				response, err := server.RollbackTransaction(
					ctx,
					&providerv1.RollbackTransactionRequest{
						ConnectionId: connectionID,
					},
				)

				return response != nil, err
			},
		},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			t.Run("delegates to connection", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				var receivedContext context.Context
				callCount := 0
				connection := &transactionTestConnection{}
				operation.setMethod(
					connection,
					func(ctx context.Context) error {
						callCount++
						receivedContext = ctx

						return nil
					},
				)
				server := NewServer(&serverTestProvider{})
				connectionID := addServerTestConnection(
					t,
					server,
					connection,
				)

				hasResponse, err :=
					operation.invoke(ctx, server, connectionID)
				if err != nil {
					t.Fatalf("%s transaction: %v", operation.name, err)
				}
				if !hasResponse {
					t.Fatalf("%s transaction returned nil", operation.name)
				}
				if callCount != 1 {
					t.Fatalf(
						"%s call count = %d, want 1",
						operation.name,
						callCount,
					)
				}
				if receivedContext != ctx {
					t.Fatalf(
						"%s did not receive the request context",
						operation.name,
					)
				}

				assertServerConnectionReleased(
					t,
					server,
					connectionID,
				)
			})

			t.Run("propagates connection error", func(t *testing.T) {
				operationErr := errors.New(operation.name + " failed")
				connection := &transactionTestConnection{}
				operation.setMethod(
					connection,
					func(context.Context) error {
						return operationErr
					},
				)
				server := NewServer(&serverTestProvider{})
				connectionID := addServerTestConnection(
					t,
					server,
					connection,
				)

				hasResponse, err := operation.invoke(
					context.Background(),
					server,
					connectionID,
				)
				if hasResponse {
					t.Fatalf(
						"%s transaction returned a response with an error",
						operation.name,
					)
				}
				if !errors.Is(err, operationErr) {
					t.Fatalf(
						"%s transaction error = %v, want %v",
						operation.name,
						err,
						operationErr,
					)
				}

				assertServerConnectionReleased(
					t,
					server,
					connectionID,
				)
			})

			testTransactionInvalidConnectionIDs(
				t,
				operation.invoke,
			)
		})
	}
}

func TestServerIsInTransaction(t *testing.T) {
	t.Run("returns connection state", func(t *testing.T) {
		tests := []struct {
			name   string
			active bool
		}{
			{
				name:   "active",
				active: true,
			},
			{
				name: "inactive",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				var receivedContext context.Context
				callCount := 0
				connection := &transactionTestConnection{
					inTransactionFunc: func(
						ctx context.Context,
					) (bool, error) {
						callCount++
						receivedContext = ctx

						return test.active, nil
					},
				}
				server := NewServer(&serverTestProvider{})
				connectionID := addServerTestConnection(
					t,
					server,
					connection,
				)

				response, err := server.IsInTransaction(
					ctx,
					&providerv1.IsInTransactionRequest{
						ConnectionId: connectionID,
					},
				)
				if err != nil {
					t.Fatalf("IsInTransaction: %v", err)
				}
				if response.GetActive() != test.active {
					t.Fatalf(
						"IsInTransaction active = %t, want %t",
						response.GetActive(),
						test.active,
					)
				}
				if callCount != 1 {
					t.Fatalf(
						"InTransaction call count = %d, want 1",
						callCount,
					)
				}
				if receivedContext != ctx {
					t.Fatal(
						"InTransaction did not receive the request context",
					)
				}

				assertServerConnectionReleased(
					t,
					server,
					connectionID,
				)
			})
		}
	})

	t.Run("propagates connection error", func(t *testing.T) {
		transactionErr := errors.New("transaction state failed")
		connection := &transactionTestConnection{
			inTransactionFunc: func(
				context.Context,
			) (bool, error) {
				return false, transactionErr
			},
		}
		server := NewServer(&serverTestProvider{})
		connectionID := addServerTestConnection(
			t,
			server,
			connection,
		)

		response, err := server.IsInTransaction(
			context.Background(),
			&providerv1.IsInTransactionRequest{
				ConnectionId: connectionID,
			},
		)
		if response != nil {
			t.Fatal("IsInTransaction returned a response with an error")
		}
		if !errors.Is(err, transactionErr) {
			t.Fatalf(
				"IsInTransaction error = %v, want %v",
				err,
				transactionErr,
			)
		}

		assertServerConnectionReleased(t, server, connectionID)
	})

	testTransactionInvalidConnectionIDs(
		t,
		func(
			ctx context.Context,
			server *Server,
			connectionID string,
		) (bool, error) {
			response, err := server.IsInTransaction(
				ctx,
				&providerv1.IsInTransactionRequest{
					ConnectionId: connectionID,
				},
			)

			return response != nil, err
		},
	)
}

func testTransactionInvalidConnectionIDs(
	t *testing.T,
	invoke transactionRPC,
) {
	t.Helper()

	tests := []struct {
		name         string
		connectionID string
		wantCode     codes.Code
	}{
		{
			name:     "empty connection ID",
			wantCode: codes.InvalidArgument,
		},
		{
			name:         "unknown connection ID",
			connectionID: "unknown",
			wantCode:     codes.NotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer(&serverTestProvider{})

			hasResponse, err := invoke(
				context.Background(),
				server,
				test.connectionID,
			)
			if hasResponse {
				t.Fatal("transaction RPC returned a response for an invalid ID")
			}
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf(
					"transaction RPC status = %s, want %s",
					got,
					test.wantCode,
				)
			}
		})
	}
}

func assertServerConnectionReleased(
	t *testing.T,
	server *Server,
	connectionID string,
) {
	t.Helper()

	type closeResult struct {
		found bool
		err   error
	}

	result := make(chan closeResult, 1)
	go func() {
		found, err := server.connections.close(
			context.Background(),
			connectionID,
		)
		result <- closeResult{
			found: found,
			err:   err,
		}
	}()

	select {
	case result := <-result:
		if result.err != nil {
			t.Fatalf("close released connection: %v", result.err)
		}
		if !result.found {
			t.Fatal("released connection was not registered")
		}
	case <-time.After(time.Second):
		t.Fatal("connection remained acquired after transaction RPC")
	}
}
