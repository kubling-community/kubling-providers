package provider

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type queryTestConnection struct {
	Connection

	queryFunc func(
		context.Context,
		*providerv1.QueryRequest,
	) (ResultStream, error)
	closeFunc func(context.Context) error
}

func (c *queryTestConnection) Close(ctx context.Context) error {
	if c.closeFunc == nil {
		return nil
	}

	return c.closeFunc(ctx)
}

func (c *queryTestConnection) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (ResultStream, error) {
	if c.queryFunc == nil {
		panic("unexpected Query call")
	}

	return c.queryFunc(ctx, request)
}

type queryTestServerStream struct {
	grpc.ServerStream

	ctx       context.Context
	responses []*providerv1.QueryResponse
	sendFunc  func(*providerv1.QueryResponse) error
}

func (s *queryTestServerStream) Context() context.Context {
	return s.ctx
}

func (s *queryTestServerStream) Send(
	response *providerv1.QueryResponse,
) error {
	s.responses = append(s.responses, response)

	if s.sendFunc == nil {
		return nil
	}

	return s.sendFunc(response)
}

type queryNextResult struct {
	batch *providerv1.TupleBatch
	err   error
}

type queryTestResultStream struct {
	nextResults  []queryNextResult
	nextIndex    int
	nextContexts []context.Context
	nextFunc     func(context.Context) (*providerv1.TupleBatch, error)
	closeCount   int
	closeErr     error
	closeFunc    func()
}

func (s *queryTestResultStream) Next(
	ctx context.Context,
) (*providerv1.TupleBatch, error) {
	s.nextContexts = append(s.nextContexts, ctx)

	if s.nextFunc != nil {
		return s.nextFunc(ctx)
	}

	if s.nextIndex >= len(s.nextResults) {
		return nil, io.EOF
	}

	result := s.nextResults[s.nextIndex]
	s.nextIndex++

	return result.batch, result.err
}

func (s *queryTestResultStream) Close() error {
	s.closeCount++

	if s.closeFunc != nil {
		s.closeFunc()
	}

	return s.closeErr
}

func TestServerQueryStreamsBatches(t *testing.T) {
	streamContext, cancel := context.WithCancel(context.Background())
	defer cancel()

	batches := []*providerv1.TupleBatch{
		{
			Fields: []*providerv1.Field{
				{
					Name: "id",
				},
			},
			Tuples: []*providerv1.Tuple{
				{},
			},
		},
		{
			Fields: []*providerv1.Field{
				{
					Name: "title",
				},
			},
			Tuples: []*providerv1.Tuple{
				{},
				{},
			},
		},
	}
	resultStream := &queryTestResultStream{
		nextResults: []queryNextResult{
			{
				batch: batches[0],
			},
			{
				batch: batches[1],
			},
		},
	}
	var receivedContext context.Context
	var receivedRequest *providerv1.QueryRequest
	queryCallCount := 0
	connection := &queryTestConnection{
		queryFunc: func(
			ctx context.Context,
			request *providerv1.QueryRequest,
		) (ResultStream, error) {
			queryCallCount++
			receivedContext = ctx
			receivedRequest = request

			return resultStream, nil
		},
	}
	server := NewServer(&serverTestProvider{})
	connectionID := addServerTestConnection(t, server, connection)
	request := newQueryTestRequest(connectionID)
	originalRequest := proto.Clone(request)
	serverStream := &queryTestServerStream{
		ctx: streamContext,
	}

	err := server.Query(request, serverStream)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if queryCallCount != 1 {
		t.Fatalf("connection Query call count = %d, want 1", queryCallCount)
	}
	if receivedContext != streamContext {
		t.Fatal("connection Query did not receive the stream context")
	}

	assertQueryRequestCloned(
		t,
		request,
		originalRequest,
		receivedRequest,
	)

	if len(serverStream.responses) != len(batches) {
		t.Fatalf(
			"sent response count = %d, want %d",
			len(serverStream.responses),
			len(batches),
		)
	}
	for index, response := range serverStream.responses {
		if response == nil {
			t.Fatalf("sent response %d is nil", index)
		}
		if !proto.Equal(response.GetBatch(), batches[index]) {
			t.Fatalf(
				"sent batch %d = %v, want %v",
				index,
				response.GetBatch(),
				batches[index],
			)
		}
	}

	if len(resultStream.nextContexts) != len(batches)+1 {
		t.Fatalf(
			"Next call count = %d, want %d",
			len(resultStream.nextContexts),
			len(batches)+1,
		)
	}
	for index, ctx := range resultStream.nextContexts {
		if ctx != streamContext {
			t.Fatalf("Next call %d did not receive stream context", index)
		}
	}
	if resultStream.closeCount != 1 {
		t.Fatalf(
			"result stream close count = %d, want 1",
			resultStream.closeCount,
		)
	}

	assertQueryConnectionReleased(t, server, connectionID)
}

func TestServerQueryCreationFailures(t *testing.T) {
	t.Run("propagates connection error", func(t *testing.T) {
		queryErr := errors.New("query failed")
		connection := &queryTestConnection{
			queryFunc: func(
				context.Context,
				*providerv1.QueryRequest,
			) (ResultStream, error) {
				return nil, queryErr
			},
		}
		server := NewServer(&serverTestProvider{})
		connectionID := addServerTestConnection(t, server, connection)

		err := server.Query(
			newQueryTestRequest(connectionID),
			&queryTestServerStream{
				ctx: context.Background(),
			},
		)
		if !errors.Is(err, queryErr) {
			t.Fatalf("Query error = %v, want %v", err, queryErr)
		}

		assertQueryConnectionReleased(t, server, connectionID)
	})

	t.Run("rejects nil result stream", func(t *testing.T) {
		connection := &queryTestConnection{
			queryFunc: func(
				context.Context,
				*providerv1.QueryRequest,
			) (ResultStream, error) {
				return nil, nil
			},
		}
		server := NewServer(&serverTestProvider{})
		connectionID := addServerTestConnection(t, server, connection)

		err := server.Query(
			newQueryTestRequest(connectionID),
			&queryTestServerStream{
				ctx: context.Background(),
			},
		)
		if got := status.Code(err); got != codes.Internal {
			t.Fatalf("Query status = %s, want %s", got, codes.Internal)
		}

		assertQueryConnectionReleased(t, server, connectionID)
	})
}

func TestServerQueryErrorPrecedence(t *testing.T) {
	nextErr := errors.New("next failed")
	sendErr := errors.New("send failed")
	closeAfterNextErr := errors.New("close after next failed")
	closeAfterSendErr := errors.New("close after send failed")
	closeAfterNilBatchErr := errors.New("close after nil batch failed")
	closeOnlyErr := errors.New("close failed")
	batch := &providerv1.TupleBatch{
		Fields: []*providerv1.Field{
			{
				Name: "id",
			},
		},
	}

	tests := []struct {
		name              string
		nextResults       []queryNextResult
		sendErr           error
		closeErr          error
		wantErr           error
		wantCode          codes.Code
		wantResponseCount int
	}{
		{
			name: "next error wins over close error",
			nextResults: []queryNextResult{
				{
					err: nextErr,
				},
			},
			closeErr: closeAfterNextErr,
			wantErr:  nextErr,
		},
		{
			name: "send error wins over close error",
			nextResults: []queryNextResult{
				{
					batch: batch,
				},
			},
			sendErr:           sendErr,
			closeErr:          closeAfterSendErr,
			wantErr:           sendErr,
			wantResponseCount: 1,
		},
		{
			name: "nil batch error wins over close error",
			nextResults: []queryNextResult{
				{},
			},
			closeErr: closeAfterNilBatchErr,
			wantCode: codes.Internal,
		},
		{
			name:     "close error follows successful EOF",
			closeErr: closeOnlyErr,
			wantErr:  closeOnlyErr,
		},
		{
			name: "cancellation still closes result stream",
			nextResults: []queryNextResult{
				{
					err: context.Canceled,
				},
			},
			wantErr: context.Canceled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resultStream := &queryTestResultStream{
				nextResults: test.nextResults,
				closeErr:    test.closeErr,
			}
			connection := &queryTestConnection{
				queryFunc: func(
					context.Context,
					*providerv1.QueryRequest,
				) (ResultStream, error) {
					return resultStream, nil
				},
			}
			server := NewServer(&serverTestProvider{})
			connectionID := addServerTestConnection(
				t,
				server,
				connection,
			)
			serverStream := &queryTestServerStream{
				ctx: context.Background(),
				sendFunc: func(
					*providerv1.QueryResponse,
				) error {
					return test.sendErr
				},
			}

			err := server.Query(
				newQueryTestRequest(connectionID),
				serverStream,
			)

			switch {
			case test.wantErr != nil:
				if !errors.Is(err, test.wantErr) {
					t.Fatalf(
						"Query error = %v, want %v",
						err,
						test.wantErr,
					)
				}
			case test.wantCode != codes.OK:
				if got := status.Code(err); got != test.wantCode {
					t.Fatalf(
						"Query status = %s, want %s",
						got,
						test.wantCode,
					)
				}
			default:
				if err != nil {
					t.Fatalf("Query: %v", err)
				}
			}

			if test.closeErr != nil &&
				test.wantErr != test.closeErr &&
				errors.Is(err, test.closeErr) {
				t.Fatalf(
					"Query error %v includes secondary close error %v",
					err,
					test.closeErr,
				)
			}
			if len(serverStream.responses) != test.wantResponseCount {
				t.Fatalf(
					"sent response count = %d, want %d",
					len(serverStream.responses),
					test.wantResponseCount,
				)
			}
			if resultStream.closeCount != 1 {
				t.Fatalf(
					"result stream close count = %d, want 1",
					resultStream.closeCount,
				)
			}

			assertQueryConnectionReleased(
				t,
				server,
				connectionID,
			)
		})
	}
}

func TestServerQueryValidatesConnectionID(t *testing.T) {
	tests := []struct {
		name         string
		connectionID string
		wantCode     codes.Code
	}{
		{
			name:     "empty",
			wantCode: codes.InvalidArgument,
		},
		{
			name:         "unknown",
			connectionID: "unknown",
			wantCode:     codes.NotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer(&serverTestProvider{})

			err := server.Query(
				newQueryTestRequest(test.connectionID),
				&queryTestServerStream{
					ctx: context.Background(),
				},
			)
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf(
					"Query status = %s, want %s",
					got,
					test.wantCode,
				)
			}
		})
	}
}

func TestServerQueryClosesResultBeforeRelease(t *testing.T) {
	nextStarted := make(chan struct{})
	finishNext := make(chan struct{})
	resultClosed := make(chan struct{})
	resultStream := &queryTestResultStream{
		nextFunc: func(
			context.Context,
		) (*providerv1.TupleBatch, error) {
			close(nextStarted)
			<-finishNext

			return nil, io.EOF
		},
		closeFunc: func() {
			close(resultClosed)
		},
	}
	connection := &queryTestConnection{
		queryFunc: func(
			context.Context,
			*providerv1.QueryRequest,
		) (ResultStream, error) {
			return resultStream, nil
		},
	}
	type connectionCloseResult struct {
		found                   bool
		err                     error
		resultClosedBeforeClose bool
	}
	closeResult := make(chan connectionCloseResult, 1)
	connection.closeFunc = func(context.Context) error {
		resultClosedBeforeClose := false
		select {
		case <-resultClosed:
			resultClosedBeforeClose = true
		default:
		}

		closeResult <- connectionCloseResult{
			found:                   true,
			resultClosedBeforeClose: resultClosedBeforeClose,
		}

		return nil
	}

	server := NewServer(&serverTestProvider{})
	connectionID := addServerTestConnection(t, server, connection)
	queryResult := make(chan error, 1)
	go func() {
		queryResult <- server.Query(
			newQueryTestRequest(connectionID),
			&queryTestServerStream{
				ctx: context.Background(),
			},
		)
	}()

	select {
	case <-nextStarted:
	case <-time.After(time.Second):
		t.Fatal("Query did not call Next")
	}

	registryCloseResult := make(chan struct {
		found bool
		err   error
	}, 1)
	go func() {
		found, err := server.connections.close(
			context.Background(),
			connectionID,
		)
		registryCloseResult <- struct {
			found bool
			err   error
		}{
			found: found,
			err:   err,
		}
	}()

	waitForQueryConnectionRemoval(t, server, connectionID)
	close(finishNext)

	select {
	case err := <-queryResult:
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Query did not complete")
	}

	select {
	case result := <-registryCloseResult:
		if result.err != nil {
			t.Fatalf("close connection: %v", result.err)
		}
		if !result.found {
			t.Fatal("close connection: not found")
		}
	case <-time.After(time.Second):
		t.Fatal("connection close did not complete")
	}

	select {
	case result := <-closeResult:
		if !result.found {
			t.Fatal("connection close was not observed")
		}
		if !result.resultClosedBeforeClose {
			t.Fatal(
				"connection closed before the result stream was closed",
			)
		}
	case <-time.After(time.Second):
		t.Fatal("connection Close was not called")
	}

	if resultStream.closeCount != 1 {
		t.Fatalf(
			"result stream close count = %d, want 1",
			resultStream.closeCount,
		)
	}
}

func newQueryTestRequest(
	connectionID string,
) *providerv1.QueryRequest {
	limit := uint64(10)
	offset := uint64(2)
	batchSize := uint32(3)

	return &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity: &providerv1.EntityReference{
			Name:      "TASK",
			Namespace: "some/path/to/resource",
		},
		Projections: []*providerv1.Projection{
			{
				Expression: &providerv1.Expression{},
				OutputName: "id",
			},
		},
		Filter: &providerv1.Expression{},
		OrderBy: []*providerv1.OrderBy{
			{
				Expression:   &providerv1.Expression{},
				Direction:    providerv1.SortDirection_SORT_DIRECTION_ASCENDING,
				NullOrdering: providerv1.NullOrdering_NULL_ORDERING_LAST,
			},
		},
		Limit:     &limit,
		Offset:    &offset,
		BatchSize: &batchSize,
	}
}

func assertQueryRequestCloned(
	t *testing.T,
	request *providerv1.QueryRequest,
	originalRequest proto.Message,
	receivedRequest *providerv1.QueryRequest,
) {
	t.Helper()

	if receivedRequest == nil {
		t.Fatal("connection Query did not receive a request")
	}
	if receivedRequest == request {
		t.Fatal("connection Query received the original request pointer")
	}
	if !proto.Equal(request, originalRequest) {
		t.Fatalf(
			"Query modified original request: got %v, want %v",
			request,
			originalRequest,
		)
	}

	expectedRequest :=
		proto.Clone(originalRequest).(*providerv1.QueryRequest)
	expectedRequest.ConnectionId = ""

	if !proto.Equal(receivedRequest, expectedRequest) {
		t.Fatalf(
			"provider Query request = %v, want %v",
			receivedRequest,
			expectedRequest,
		)
	}
}

func assertQueryConnectionReleased(
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
		t.Fatal("connection remained acquired after Query")
	}
}

func waitForQueryConnectionRemoval(
	t *testing.T,
	server *Server,
	connectionID string,
) {
	t.Helper()

	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	for {
		_, release, found :=
			server.connections.acquire(connectionID)
		if !found {
			return
		}

		release()

		select {
		case <-timeout.C:
			t.Fatal("connection was not removed while close waited")
		default:
			runtime.Gosched()
		}
	}
}
