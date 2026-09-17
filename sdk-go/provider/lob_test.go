package provider

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type lobTestConnection struct {
	Connection

	queryFunc func(
		context.Context,
		*providerv1.QueryRequest,
	) (ResultStream, error)
	readFunc func(
		context.Context,
		*providerv1.ReadLobRequest,
	) (LobStream, error)
	releaseFunc func(context.Context, *providerv1.ReleaseLobRequest) error
}

func (c *lobTestConnection) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (ResultStream, error) {
	return c.queryFunc(ctx, request)
}

func (c *lobTestConnection) ReadLob(
	ctx context.Context,
	request *providerv1.ReadLobRequest,
) (LobStream, error) {
	return c.readFunc(ctx, request)
}

func (c *lobTestConnection) ReleaseLob(
	ctx context.Context,
	request *providerv1.ReleaseLobRequest,
) error {
	return c.releaseFunc(ctx, request)
}

type lobTestStream struct {
	responses []*providerv1.ReadLobResponse
	next      int
	nextErr   error
	closed    bool
	closeErr  error
}

func (s *lobTestStream) Next(
	context.Context,
) (*providerv1.ReadLobResponse, error) {
	if s.next < len(s.responses) {
		response := s.responses[s.next]
		s.next++
		return response, nil
	}
	if s.nextErr != nil {
		return nil, s.nextErr
	}
	return nil, io.EOF
}

func (s *lobTestStream) Close() error {
	s.closed = true
	return s.closeErr
}

type lobTestServerStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*providerv1.ReadLobResponse
}

func (s *lobTestServerStream) Context() context.Context {
	return s.ctx
}

func (s *lobTestServerStream) Send(response *providerv1.ReadLobResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func TestServerReadLobStreamsValidatedChunks(t *testing.T) {
	streamContext := context.Background()
	providerStream := &lobTestStream{responses: []*providerv1.ReadLobResponse{
		{Offset: 5, Data: []byte("ab")},
		{Offset: 7, Data: []byte("c"), EndOfRead: true},
	}}
	var receivedContext context.Context
	var receivedRequest *providerv1.ReadLobRequest
	connection := &lobTestConnection{readFunc: func(
		ctx context.Context,
		request *providerv1.ReadLobRequest,
	) (LobStream, error) {
		receivedContext = ctx
		receivedRequest = request
		return providerStream, nil
	}}
	server := NewServer(&serverTestProvider{})
	connectionID := addServerTestConnection(t, server, connection)
	length := uint64(4)
	request := &providerv1.ReadLobRequest{
		ConnectionId:  connectionID,
		LobId:         "lob-1",
		Offset:        5,
		Length:        &length,
		MaxChunkBytes: 2,
	}
	originalRequest := proto.Clone(request)
	serverStream := &lobTestServerStream{ctx: streamContext}

	if err := server.ReadLob(request, serverStream); err != nil {
		t.Fatalf("ReadLob() error = %v", err)
	}
	if receivedContext != streamContext {
		t.Fatal("ReadLob() did not propagate stream context")
	}
	if receivedRequest.GetConnectionId() != "" ||
		receivedRequest.GetLobId() != "lob-1" {
		t.Fatalf("provider ReadLob request = %v", receivedRequest)
	}
	if !proto.Equal(request, originalRequest) {
		t.Fatalf("ReadLob mutated request: got %v, want %v", request, originalRequest)
	}
	if len(serverStream.responses) != 2 ||
		!serverStream.responses[1].GetEndOfRead() {
		t.Fatalf("ReadLob responses = %v", serverStream.responses)
	}
	if !providerStream.closed {
		t.Fatal("ReadLob did not close provider stream")
	}
}

func TestServerReadLobRejectsInvalidProviderStreams(t *testing.T) {
	lengthOne := uint64(1)
	tests := []struct {
		name      string
		responses []*providerv1.ReadLobResponse
		length    *uint64
		maxChunk  uint32
		wantText  string
	}{
		{
			name:     "premature EOF",
			wantText: "before end_of_read",
		},
		{
			name: "non-contiguous offset",
			responses: []*providerv1.ReadLobResponse{{
				Offset: 1, Data: []byte("a"), EndOfRead: true,
			}},
			wantText: "does not match expected offset",
		},
		{
			name: "empty non-terminal chunk",
			responses: []*providerv1.ReadLobResponse{{
				Offset: 0,
			}},
			wantText: "empty non-terminal",
		},
		{
			name: "chunk exceeds requested maximum",
			responses: []*providerv1.ReadLobResponse{{
				Offset: 0, Data: []byte("ab"), EndOfRead: true,
			}},
			maxChunk: 1,
			wantText: "exceeding requested maximum",
		},
		{
			name: "stream exceeds requested length",
			responses: []*providerv1.ReadLobResponse{{
				Offset: 0, Data: []byte("ab"), EndOfRead: true,
			}},
			length:   &lengthOne,
			maxChunk: 2,
			wantText: "exceeded the requested length",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			providerStream := &lobTestStream{responses: test.responses}
			connection := &lobTestConnection{readFunc: func(
				context.Context,
				*providerv1.ReadLobRequest,
			) (LobStream, error) {
				return providerStream, nil
			}}
			server := NewServer(&serverTestProvider{})
			connectionID := addServerTestConnection(t, server, connection)
			err := server.ReadLob(
				&providerv1.ReadLobRequest{
					ConnectionId:  connectionID,
					LobId:         "lob-1",
					Length:        test.length,
					MaxChunkBytes: test.maxChunk,
				},
				&lobTestServerStream{ctx: context.Background()},
			)
			if status.Code(err) != codes.Internal ||
				!strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("ReadLob() error = %v, want Internal containing %q", err, test.wantText)
			}
			if !providerStream.closed {
				t.Fatal("ReadLob did not close invalid provider stream")
			}
		})
	}
}

func TestServerReadLobAcceptsZeroLengthTerminalChunk(t *testing.T) {
	providerStream := &lobTestStream{responses: []*providerv1.ReadLobResponse{{
		Offset:    7,
		EndOfRead: true,
	}}}
	connection := &lobTestConnection{readFunc: func(
		context.Context,
		*providerv1.ReadLobRequest,
	) (LobStream, error) {
		return providerStream, nil
	}}
	server := NewServer(&serverTestProvider{})
	connectionID := addServerTestConnection(t, server, connection)
	length := uint64(0)
	serverStream := &lobTestServerStream{ctx: context.Background()}

	err := server.ReadLob(
		&providerv1.ReadLobRequest{
			ConnectionId: connectionID,
			LobId:        "lob-1",
			Offset:       7,
			Length:       &length,
		},
		serverStream,
	)
	if err != nil {
		t.Fatalf("ReadLob() error = %v", err)
	}
	if len(serverStream.responses) != 1 ||
		!serverStream.responses[0].GetEndOfRead() ||
		len(serverStream.responses[0].GetData()) != 0 {
		t.Fatalf("ReadLob() responses = %v", serverStream.responses)
	}
	if !providerStream.closed {
		t.Fatal("ReadLob did not close zero-length provider stream")
	}
}

func TestServerReleaseLobDelegatesClonedRequest(t *testing.T) {
	var receivedRequest *providerv1.ReleaseLobRequest
	connection := &lobTestConnection{releaseFunc: func(
		_ context.Context,
		request *providerv1.ReleaseLobRequest,
	) error {
		receivedRequest = request
		return nil
	}}
	server := NewServer(&serverTestProvider{})
	connectionID := addServerTestConnection(t, server, connection)
	request := &providerv1.ReleaseLobRequest{
		ConnectionId: connectionID,
		LobId:        "lob-1",
	}
	originalRequest := proto.Clone(request)

	response, err := server.ReleaseLob(context.Background(), request)
	if err != nil {
		t.Fatalf("ReleaseLob() error = %v", err)
	}
	if response == nil {
		t.Fatal("ReleaseLob() response is nil")
	}
	if receivedRequest.GetConnectionId() != "" ||
		receivedRequest.GetLobId() != "lob-1" {
		t.Fatalf("provider ReleaseLob request = %v", receivedRequest)
	}
	if !proto.Equal(request, originalRequest) {
		t.Fatalf("ReleaseLob mutated request: got %v, want %v", request, originalRequest)
	}
}

func TestServerQueryScopesAcceptedProviderLobReference(t *testing.T) {
	batch := lobFeatureBatch()
	connection := &lobTestConnection{
		queryFunc: func(
			context.Context,
			*providerv1.QueryRequest,
		) (ResultStream, error) {
			return &queryTestResultStream{nextResults: []queryNextResult{{
				batch: batch,
			}}}, nil
		},
		readFunc: func(
			context.Context,
			*providerv1.ReadLobRequest,
		) (LobStream, error) {
			return nil, errors.New("unexpected ReadLob call")
		},
		releaseFunc: func(
			context.Context,
			*providerv1.ReleaseLobRequest,
		) error {
			return errors.New("unexpected ReleaseLob call")
		},
	}
	server := NewServer(&serverTestProvider{})
	connectionID := addServerTestConnection(t, server, connection)
	request := newQueryTestRequest(connectionID)
	request.AcceptedFeatures = []string{grpcfeatures.LobReadV1}
	stream := &queryTestServerStream{ctx: context.Background()}

	if err := server.Query(request, stream); err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	reference := stream.responses[0].GetBatch().GetTuples()[0].GetValues()[0].GetLobReference()
	if got := reference.GetSessionId(); got != connectionID {
		t.Fatalf("Query() LOB session_id = %q, want %q", got, connectionID)
	}
	if got := batch.GetTuples()[0].GetValues()[0].GetLobReference().GetSessionId(); got != "" {
		t.Fatalf("Query() mutated provider LOB session_id = %q", got)
	}
}
