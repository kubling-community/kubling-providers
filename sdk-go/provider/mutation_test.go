package provider

import (
	"context"
	"errors"
	"testing"
	"time"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type mutationTestConnection struct {
	Connection

	insertFunc func(
		context.Context,
		*providerv1.InsertRequest,
	) (*providerv1.InsertResponse, error)
	updateFunc func(
		context.Context,
		*providerv1.UpdateRequest,
	) (*providerv1.UpdateResponse, error)
	deleteFunc func(
		context.Context,
		*providerv1.DeleteRequest,
	) (*providerv1.DeleteResponse, error)
}

func (c *mutationTestConnection) Close(context.Context) error {
	return nil
}

func (c *mutationTestConnection) Insert(
	ctx context.Context,
	request *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	if c.insertFunc == nil {
		panic("unexpected Insert call")
	}

	return c.insertFunc(ctx, request)
}

func (c *mutationTestConnection) Update(
	ctx context.Context,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	if c.updateFunc == nil {
		panic("unexpected Update call")
	}

	return c.updateFunc(ctx, request)
}

func (c *mutationTestConnection) Delete(
	ctx context.Context,
	request *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	if c.deleteFunc == nil {
		panic("unexpected Delete call")
	}

	return c.deleteFunc(ctx, request)
}

type mutationHandler func(
	context.Context,
	proto.Message,
) (proto.Message, error)

type mutationOperation struct {
	name                string
	newRequest          func(string) proto.Message
	clearConnectionID   func(proto.Message)
	newResponse         func() proto.Message
	newEmptyResponse    func() proto.Message
	setConnectionMethod func(*mutationTestConnection, mutationHandler)
	invoke              func(
		context.Context,
		*Server,
		proto.Message,
	) (proto.Message, error)
}

func TestServerMutations(t *testing.T) {
	for _, operation := range mutationOperations() {
		t.Run(operation.name, func(t *testing.T) {
			t.Run("delegates cloned request", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				expectedResponse := operation.newResponse()
				var receivedContext context.Context
				var receivedRequest proto.Message
				callCount := 0
				connection := &mutationTestConnection{}
				operation.setConnectionMethod(
					connection,
					func(
						ctx context.Context,
						request proto.Message,
					) (proto.Message, error) {
						callCount++
						receivedContext = ctx
						receivedRequest = request

						return expectedResponse, nil
					},
				)
				server := NewServer(&serverTestProvider{})
				connectionID := addServerTestConnection(
					t,
					server,
					connection,
				)
				request := operation.newRequest(connectionID)
				originalRequest := proto.Clone(request)

				response, err :=
					operation.invoke(ctx, server, request)
				if err != nil {
					t.Fatalf("%s: %v", operation.name, err)
				}
				if !proto.Equal(response, expectedResponse) {
					t.Fatalf(
						"%s response = %v, want %v",
						operation.name,
						response,
						expectedResponse,
					)
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

				assertMutationRequestCloned(
					t,
					operation,
					request,
					originalRequest,
					receivedRequest,
				)
				assertMutationConnectionReleased(
					t,
					server,
					connectionID,
				)
			})

			t.Run("propagates connection error", func(t *testing.T) {
				mutationErr := errors.New(operation.name + " failed")
				connection := &mutationTestConnection{}
				operation.setConnectionMethod(
					connection,
					func(
						context.Context,
						proto.Message,
					) (proto.Message, error) {
						return nil, mutationErr
					},
				)
				server := NewServer(&serverTestProvider{})
				connectionID := addServerTestConnection(
					t,
					server,
					connection,
				)

				response, err := operation.invoke(
					context.Background(),
					server,
					operation.newRequest(connectionID),
				)
				if response != nil {
					t.Fatalf(
						"%s returned a response with an error",
						operation.name,
					)
				}
				if !errors.Is(err, mutationErr) {
					t.Fatalf(
						"%s error = %v, want %v",
						operation.name,
						err,
						mutationErr,
					)
				}

				assertMutationConnectionReleased(
					t,
					server,
					connectionID,
				)
			})

			t.Run("normalizes nil response", func(t *testing.T) {
				connection := &mutationTestConnection{}
				operation.setConnectionMethod(
					connection,
					func(
						context.Context,
						proto.Message,
					) (proto.Message, error) {
						return nil, nil
					},
				)
				server := NewServer(&serverTestProvider{})
				connectionID := addServerTestConnection(
					t,
					server,
					connection,
				)

				response, err := operation.invoke(
					context.Background(),
					server,
					operation.newRequest(connectionID),
				)
				if err != nil {
					t.Fatalf("%s nil response: %v", operation.name, err)
				}
				if response == nil {
					t.Fatalf("%s did not normalize nil response", operation.name)
				}
				if !proto.Equal(response, operation.newEmptyResponse()) {
					t.Fatalf(
						"%s normalized response = %v, want empty",
						operation.name,
						response,
					)
				}

				assertMutationConnectionReleased(
					t,
					server,
					connectionID,
				)
			})

			testMutationInvalidConnectionIDs(t, operation)
		})
	}
}

func mutationOperations() []mutationOperation {
	return []mutationOperation{
		{
			name: "insert",
			newRequest: func(connectionID string) proto.Message {
				return &providerv1.InsertRequest{
					ConnectionId: connectionID,
					Entity: &providerv1.EntityReference{
						Name: "TASK",
					},
					Rows: &providerv1.TupleBatch{
						Fields: []*providerv1.Field{
							{
								Name: "title",
							},
						},
						Tuples: []*providerv1.Tuple{
							{},
						},
					},
					ReturningFields: []string{"id"},
				}
			},
			clearConnectionID: func(message proto.Message) {
				message.(*providerv1.InsertRequest).ConnectionId = ""
			},
			newResponse: func() proto.Message {
				return &providerv1.InsertResponse{
					AffectedRows: mutationRowCount(1),
					GeneratedValues: &providerv1.TupleBatch{
						Fields: []*providerv1.Field{
							{
								Name: "id",
							},
						},
					},
				}
			},
			newEmptyResponse: func() proto.Message {
				return &providerv1.InsertResponse{}
			},
			setConnectionMethod: func(
				connection *mutationTestConnection,
				handler mutationHandler,
			) {
				connection.insertFunc = func(
					ctx context.Context,
					request *providerv1.InsertRequest,
				) (*providerv1.InsertResponse, error) {
					response, err := handler(ctx, request)
					if response == nil {
						return nil, err
					}

					return response.(*providerv1.InsertResponse), err
				}
			},
			invoke: func(
				ctx context.Context,
				server *Server,
				message proto.Message,
			) (proto.Message, error) {
				response, err := server.Insert(
					ctx,
					message.(*providerv1.InsertRequest),
				)
				if response == nil {
					return nil, err
				}

				return response, err
			},
		},
		{
			name: "update",
			newRequest: func(connectionID string) proto.Message {
				return &providerv1.UpdateRequest{
					ConnectionId: connectionID,
					Entity: &providerv1.EntityReference{
						Name: "TASK",
					},
					Assignments: []*providerv1.Assignment{
						{
							Field: "completed",
							Value: &providerv1.Expression{},
						},
					},
					Filter: &providerv1.Expression{},
				}
			},
			clearConnectionID: func(message proto.Message) {
				message.(*providerv1.UpdateRequest).ConnectionId = ""
			},
			newResponse: func() proto.Message {
				return &providerv1.UpdateResponse{
					AffectedRows: mutationRowCount(2),
				}
			},
			newEmptyResponse: func() proto.Message {
				return &providerv1.UpdateResponse{}
			},
			setConnectionMethod: func(
				connection *mutationTestConnection,
				handler mutationHandler,
			) {
				connection.updateFunc = func(
					ctx context.Context,
					request *providerv1.UpdateRequest,
				) (*providerv1.UpdateResponse, error) {
					response, err := handler(ctx, request)
					if response == nil {
						return nil, err
					}

					return response.(*providerv1.UpdateResponse), err
				}
			},
			invoke: func(
				ctx context.Context,
				server *Server,
				message proto.Message,
			) (proto.Message, error) {
				response, err := server.Update(
					ctx,
					message.(*providerv1.UpdateRequest),
				)
				if response == nil {
					return nil, err
				}

				return response, err
			},
		},
		{
			name: "delete",
			newRequest: func(connectionID string) proto.Message {
				return &providerv1.DeleteRequest{
					ConnectionId: connectionID,
					Entity: &providerv1.EntityReference{
						Name: "TASK",
					},
					Filter: &providerv1.Expression{},
				}
			},
			clearConnectionID: func(message proto.Message) {
				message.(*providerv1.DeleteRequest).ConnectionId = ""
			},
			newResponse: func() proto.Message {
				return &providerv1.DeleteResponse{
					AffectedRows: mutationRowCount(3),
				}
			},
			newEmptyResponse: func() proto.Message {
				return &providerv1.DeleteResponse{}
			},
			setConnectionMethod: func(
				connection *mutationTestConnection,
				handler mutationHandler,
			) {
				connection.deleteFunc = func(
					ctx context.Context,
					request *providerv1.DeleteRequest,
				) (*providerv1.DeleteResponse, error) {
					response, err := handler(ctx, request)
					if response == nil {
						return nil, err
					}

					return response.(*providerv1.DeleteResponse), err
				}
			},
			invoke: func(
				ctx context.Context,
				server *Server,
				message proto.Message,
			) (proto.Message, error) {
				response, err := server.Delete(
					ctx,
					message.(*providerv1.DeleteRequest),
				)
				if response == nil {
					return nil, err
				}

				return response, err
			},
		},
	}
}

func assertMutationRequestCloned(
	t *testing.T,
	operation mutationOperation,
	request proto.Message,
	originalRequest proto.Message,
	receivedRequest proto.Message,
) {
	t.Helper()

	if receivedRequest == nil {
		t.Fatalf("%s did not receive a request", operation.name)
	}
	if receivedRequest == request {
		t.Fatalf("%s received the original request pointer", operation.name)
	}
	if !proto.Equal(request, originalRequest) {
		t.Fatalf(
			"%s modified original request: got %v, want %v",
			operation.name,
			request,
			originalRequest,
		)
	}

	expectedRequest := proto.Clone(originalRequest)
	operation.clearConnectionID(expectedRequest)

	if !proto.Equal(receivedRequest, expectedRequest) {
		t.Fatalf(
			"%s provider request = %v, want %v",
			operation.name,
			receivedRequest,
			expectedRequest,
		)
	}
}

func testMutationInvalidConnectionIDs(
	t *testing.T,
	operation mutationOperation,
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

			response, err := operation.invoke(
				context.Background(),
				server,
				operation.newRequest(test.connectionID),
			)
			if response != nil {
				t.Fatalf(
					"%s returned a response for an invalid ID",
					operation.name,
				)
			}
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf(
					"%s status = %s, want %s",
					operation.name,
					got,
					test.wantCode,
				)
			}
		})
	}
}

func assertMutationConnectionReleased(
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
		t.Fatal("connection remained acquired after mutation RPC")
	}
}

func mutationRowCount(value uint64) *uint64 {
	return &value
}
