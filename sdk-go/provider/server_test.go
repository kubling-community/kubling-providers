package provider

import (
	"context"
	"errors"
	"sync"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type serverTestProvider struct {
	capabilitiesFunc func(context.Context) (*Capabilities, error)
	healthFunc       func(context.Context) (*providerv1.HealthResponse, error)
	openFunc         func(context.Context) (Connection, error)
}

func (p *serverTestProvider) Capabilities(
	ctx context.Context,
) (*Capabilities, error) {
	if p.capabilitiesFunc == nil {
		panic("unexpected Capabilities call")
	}

	return p.capabilitiesFunc(ctx)
}

func (p *serverTestProvider) Health(
	ctx context.Context,
) (*providerv1.HealthResponse, error) {
	if p.healthFunc == nil {
		panic("unexpected Health call")
	}

	return p.healthFunc(ctx)
}

func (p *serverTestProvider) Open(
	ctx context.Context,
) (Connection, error) {
	if p.openFunc == nil {
		panic("unexpected Open call")
	}

	return p.openFunc(ctx)
}

type serverTestSchemaProvider struct {
	*serverTestProvider

	schemaFunc func(context.Context) (string, error)
}

type serverTestMetadataProvider struct {
	*serverTestProvider

	metadataFunc func(context.Context) (*Metadata, error)
	schemaFunc   func(context.Context) (string, error)
}

func (p *serverTestMetadataProvider) Metadata(
	ctx context.Context,
) (*Metadata, error) {
	if p.metadataFunc == nil {
		panic("unexpected Metadata call")
	}

	return p.metadataFunc(ctx)
}

func (p *serverTestMetadataProvider) Schema(
	ctx context.Context,
) (string, error) {
	if p.schemaFunc == nil {
		panic("unexpected Schema call")
	}

	return p.schemaFunc(ctx)
}

func (p *serverTestSchemaProvider) Schema(
	ctx context.Context,
) (string, error) {
	if p.schemaFunc == nil {
		panic("unexpected Schema call")
	}

	return p.schemaFunc(ctx)
}

type serverTestConnection struct {
	Connection

	mu            sync.Mutex
	closeCount    int
	closeContexts []context.Context
	closeErr      error
}

func (c *serverTestConnection) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closeCount++
	c.closeContexts = append(c.closeContexts, ctx)

	return c.closeErr
}

func (c *serverTestConnection) closeSnapshot() (
	int,
	[]context.Context,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	contexts := append([]context.Context(nil), c.closeContexts...)

	return c.closeCount, contexts
}

func TestNewServer(t *testing.T) {
	implementation := &serverTestProvider{}

	server := NewServer(implementation)

	if server == nil {
		t.Fatal("NewServer returned nil")
	}
	if server.implementation != implementation {
		t.Fatal("NewServer did not preserve the provider implementation")
	}
	if server.connections == nil {
		t.Fatal("NewServer did not initialize the connection registry")
	}
}

func TestServerGetCapabilities(t *testing.T) {
	t.Run("returns provider capabilities", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expected := &Capabilities{
			Transactions: &providerv1.TransactionCapabilities{},
		}
		var receivedContext context.Context
		callCount := 0
		server := NewServer(&serverTestProvider{
			capabilitiesFunc: func(
				ctx context.Context,
			) (*Capabilities, error) {
				callCount++
				receivedContext = ctx

				return expected, nil
			},
		})

		response, err := server.GetCapabilities(
			ctx,
			&providerv1.GetCapabilitiesRequest{},
		)
		if err != nil {
			t.Fatalf("GetCapabilities: %v", err)
		}
		if !proto.Equal(response, expected) {
			t.Fatalf(
				"GetCapabilities response = %v, want %v",
				response,
				expected,
			)
		}
		if callCount != 1 {
			t.Fatalf("Capabilities call count = %d, want 1", callCount)
		}
		if receivedContext != ctx {
			t.Fatal("Capabilities did not receive the request context")
		}
	})

	t.Run("propagates provider error", func(t *testing.T) {
		capabilitiesErr := errors.New("capabilities failed")
		server := NewServer(&serverTestProvider{
			capabilitiesFunc: func(
				context.Context,
			) (*Capabilities, error) {
				return nil, capabilitiesErr
			},
		})

		response, err := server.GetCapabilities(
			context.Background(),
			&providerv1.GetCapabilitiesRequest{},
		)
		if response != nil {
			t.Fatal("GetCapabilities returned a response with an error")
		}
		if !errors.Is(err, capabilitiesErr) {
			t.Fatalf(
				"GetCapabilities error = %v, want %v",
				err,
				capabilitiesErr,
			)
		}
	})

	t.Run("rejects nil capabilities", func(t *testing.T) {
		server := NewServer(&serverTestProvider{
			capabilitiesFunc: func(
				context.Context,
			) (*Capabilities, error) {
				return nil, nil
			},
		})

		response, err := server.GetCapabilities(
			context.Background(),
			&providerv1.GetCapabilitiesRequest{},
		)
		if response != nil {
			t.Fatal("GetCapabilities returned a response for nil capabilities")
		}
		if got := status.Code(err); got != codes.Internal {
			t.Fatalf(
				"GetCapabilities status = %s, want %s",
				got,
				codes.Internal,
			)
		}
	})
}

func TestServerGetSchema(t *testing.T) {
	t.Run("returns empty schema when unsupported", func(t *testing.T) {
		server := NewServer(&serverTestProvider{})

		response, err := server.GetSchema(
			context.Background(),
			&providerv1.GetSchemaRequest{},
		)
		if err != nil {
			t.Fatalf("GetSchema: %v", err)
		}
		if response == nil {
			t.Fatal("GetSchema returned nil")
		}
		if response.GetSchemaDdl() != "" {
			t.Fatalf(
				"GetSchema schema = %q, want empty",
				response.GetSchemaDdl(),
			)
		}
	})

	t.Run("returns metadata before DDL", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expected := &Metadata{
			Tables: []*providerv1.TableMetadata{
				{Name: "events", Namespace: "operations"},
			},
		}
		var receivedContext context.Context
		server := NewServer(&serverTestMetadataProvider{
			serverTestProvider: &serverTestProvider{},
			metadataFunc: func(ctx context.Context) (*Metadata, error) {
				receivedContext = ctx
				return expected, nil
			},
			schemaFunc: func(context.Context) (string, error) {
				panic("unexpected Schema call")
			},
		})

		response, err := server.GetSchema(
			ctx,
			&providerv1.GetSchemaRequest{},
		)
		if err != nil {
			t.Fatalf("GetSchema: %v", err)
		}
		if !proto.Equal(response.GetMetadata(), expected) {
			t.Fatalf("GetSchema metadata = %v, want %v", response.GetMetadata(), expected)
		}
		if response.GetSchemaDdl() != "" {
			t.Fatalf("GetSchema DDL = %q, want empty", response.GetSchemaDdl())
		}
		if receivedContext != ctx {
			t.Fatal("Metadata did not receive the request context")
		}
	})

	t.Run("returns global metadata", func(t *testing.T) {
		expected := &Metadata{
			Tables: []*providerv1.TableMetadata{{Name: "projects"}},
		}
		server := NewServer(&serverTestMetadataProvider{
			serverTestProvider: &serverTestProvider{},
			metadataFunc: func(context.Context) (*Metadata, error) {
				return expected, nil
			},
		})

		response, err := server.GetSchema(
			context.Background(),
			&providerv1.GetSchemaRequest{},
		)
		if err != nil {
			t.Fatalf("GetSchema: %v", err)
		}
		if !proto.Equal(response.GetMetadata(), expected) {
			t.Fatalf("GetSchema metadata = %v, want %v", response.GetMetadata(), expected)
		}
	})

	t.Run("falls back to DDL after nil metadata", func(t *testing.T) {
		const expectedSchema = "CREATE FOREIGN TABLE fallback"
		server := NewServer(&serverTestMetadataProvider{
			serverTestProvider: &serverTestProvider{},
			metadataFunc: func(context.Context) (*Metadata, error) {
				return nil, nil
			},
			schemaFunc: func(context.Context) (string, error) {
				return expectedSchema, nil
			},
		})

		response, err := server.GetSchema(
			context.Background(),
			&providerv1.GetSchemaRequest{},
		)
		if err != nil {
			t.Fatalf("GetSchema: %v", err)
		}
		if response.GetMetadata() != nil {
			t.Fatalf("GetSchema metadata = %v, want nil", response.GetMetadata())
		}
		if response.GetSchemaDdl() != expectedSchema {
			t.Fatalf("GetSchema DDL = %q, want %q", response.GetSchemaDdl(), expectedSchema)
		}
	})

	t.Run("propagates metadata error", func(t *testing.T) {
		metadataErr := errors.New("metadata failed")
		server := NewServer(&serverTestMetadataProvider{
			serverTestProvider: &serverTestProvider{},
			metadataFunc: func(context.Context) (*Metadata, error) {
				return nil, metadataErr
			},
		})

		response, err := server.GetSchema(
			context.Background(),
			&providerv1.GetSchemaRequest{},
		)
		if response != nil {
			t.Fatal("GetSchema returned a response with a metadata error")
		}
		if !errors.Is(err, metadataErr) {
			t.Fatalf("GetSchema error = %v, want %v", err, metadataErr)
		}
	})

	t.Run("returns provider schema", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const expectedSchema = "CREATE FOREIGN TABLE example"
		var receivedContext context.Context
		callCount := 0
		server := NewServer(&serverTestSchemaProvider{
			serverTestProvider: &serverTestProvider{},
			schemaFunc: func(ctx context.Context) (string, error) {
				callCount++
				receivedContext = ctx

				return expectedSchema, nil
			},
		})

		response, err := server.GetSchema(
			ctx,
			&providerv1.GetSchemaRequest{},
		)
		if err != nil {
			t.Fatalf("GetSchema: %v", err)
		}
		if got := response.GetSchemaDdl(); got != expectedSchema {
			t.Fatalf("GetSchema schema = %q, want %q", got, expectedSchema)
		}
		if callCount != 1 {
			t.Fatalf("Schema call count = %d, want 1", callCount)
		}
		if receivedContext != ctx {
			t.Fatal("Schema did not receive the request context")
		}
	})

	t.Run("propagates provider error", func(t *testing.T) {
		schemaErr := errors.New("schema failed")
		server := NewServer(&serverTestSchemaProvider{
			serverTestProvider: &serverTestProvider{},
			schemaFunc: func(context.Context) (string, error) {
				return "", schemaErr
			},
		})

		response, err := server.GetSchema(
			context.Background(),
			&providerv1.GetSchemaRequest{},
		)
		if response != nil {
			t.Fatal("GetSchema returned a response with an error")
		}
		if !errors.Is(err, schemaErr) {
			t.Fatalf("GetSchema error = %v, want %v", err, schemaErr)
		}
	})
}

func TestServerOpenConnection(t *testing.T) {
	t.Run("opens and registers connection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		request := &providerv1.OpenConnectionRequest{}
		connection := &serverTestConnection{}
		var receivedContext context.Context
		callCount := 0
		server := NewServer(&serverTestProvider{
			openFunc: func(ctx context.Context) (Connection, error) {
				callCount++
				receivedContext = ctx

				return connection, nil
			},
		})

		response, err := server.OpenConnection(ctx, request)
		if err != nil {
			t.Fatalf("OpenConnection: %v", err)
		}
		if response.GetConnectionId() == "" {
			t.Fatal("OpenConnection returned an empty connection ID")
		}
		if callCount != 1 {
			t.Fatalf("Open call count = %d, want 1", callCount)
		}
		if receivedContext != ctx {
			t.Fatal("Open did not receive the request context")
		}
		acquired, release, found :=
			server.connections.acquire(response.GetConnectionId())
		if !found {
			t.Fatal("opened connection was not registered")
		}
		if acquired != connection {
			t.Fatal("registered connection differs from provider connection")
		}

		release()
	})

	t.Run("propagates provider error", func(t *testing.T) {
		openErr := errors.New("open failed")
		server := NewServer(&serverTestProvider{
			openFunc: func(context.Context) (Connection, error) {
				return nil, openErr
			},
		})

		response, err := server.OpenConnection(
			context.Background(),
			&providerv1.OpenConnectionRequest{},
		)
		if response != nil {
			t.Fatal("OpenConnection returned a response with an error")
		}
		if !errors.Is(err, openErr) {
			t.Fatalf("OpenConnection error = %v, want %v", err, openErr)
		}
	})

	t.Run("rejects nil connection", func(t *testing.T) {
		server := NewServer(&serverTestProvider{
			openFunc: func(context.Context) (Connection, error) {
				return nil, nil
			},
		})

		response, err := server.OpenConnection(
			context.Background(),
			&providerv1.OpenConnectionRequest{},
		)
		if response != nil {
			t.Fatal("OpenConnection returned a response for nil connection")
		}
		if got := status.Code(err); got != codes.Internal {
			t.Fatalf(
				"OpenConnection status = %s, want %s",
				got,
				codes.Internal,
			)
		}
	})
}

func TestServerAcquireConnection(t *testing.T) {
	t.Run("rejects empty ID", func(t *testing.T) {
		server := NewServer(&serverTestProvider{})

		connection, release, err := server.acquireConnection("")

		if connection != nil || release != nil {
			t.Fatal("acquireConnection returned state for an empty ID")
		}
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Fatalf(
				"acquireConnection status = %s, want %s",
				got,
				codes.InvalidArgument,
			)
		}
	})

	t.Run("reports unknown ID", func(t *testing.T) {
		server := NewServer(&serverTestProvider{})

		connection, release, err :=
			server.acquireConnection("unknown")

		if connection != nil || release != nil {
			t.Fatal("acquireConnection returned state for an unknown ID")
		}
		if got := status.Code(err); got != codes.NotFound {
			t.Fatalf(
				"acquireConnection status = %s, want %s",
				got,
				codes.NotFound,
			)
		}
	})

	t.Run("returns registered connection", func(t *testing.T) {
		server := NewServer(&serverTestProvider{})
		expected := &serverTestConnection{}
		connectionID := addServerTestConnection(t, server, expected)

		connection, release, err :=
			server.acquireConnection(connectionID)
		if err != nil {
			t.Fatalf("acquireConnection: %v", err)
		}
		if connection != expected {
			t.Fatal("acquireConnection returned a different connection")
		}
		if release == nil {
			t.Fatal("acquireConnection returned nil release")
		}

		release()
	})
}

func TestServerCloseConnection(t *testing.T) {
	t.Run("rejects empty ID", func(t *testing.T) {
		server := NewServer(&serverTestProvider{})

		response, err := server.CloseConnection(
			context.Background(),
			&providerv1.CloseConnectionRequest{},
		)

		if response != nil {
			t.Fatal("CloseConnection returned a response for an empty ID")
		}
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Fatalf(
				"CloseConnection status = %s, want %s",
				got,
				codes.InvalidArgument,
			)
		}
	})

	t.Run("reports unknown ID", func(t *testing.T) {
		server := NewServer(&serverTestProvider{})

		response, err := server.CloseConnection(
			context.Background(),
			&providerv1.CloseConnectionRequest{
				ConnectionId: "unknown",
			},
		)

		if response != nil {
			t.Fatal("CloseConnection returned a response for an unknown ID")
		}
		if got := status.Code(err); got != codes.NotFound {
			t.Fatalf(
				"CloseConnection status = %s, want %s",
				got,
				codes.NotFound,
			)
		}
	})

	t.Run("closes registered connection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		server := NewServer(&serverTestProvider{})
		connection := &serverTestConnection{}
		connectionID := addServerTestConnection(t, server, connection)

		response, err := server.CloseConnection(
			ctx,
			&providerv1.CloseConnectionRequest{
				ConnectionId: connectionID,
			},
		)
		if err != nil {
			t.Fatalf("CloseConnection: %v", err)
		}
		if response == nil {
			t.Fatal("CloseConnection returned nil")
		}

		closeCount, closeContexts := connection.closeSnapshot()
		if closeCount != 1 {
			t.Fatalf("connection close count = %d, want 1", closeCount)
		}
		if len(closeContexts) != 1 || closeContexts[0] != ctx {
			t.Fatal("connection did not receive the close context")
		}

		_, _, found := server.connections.acquire(connectionID)
		if found {
			t.Fatal("closed connection remained registered")
		}
	})

	t.Run("propagates close error and unregisters", func(t *testing.T) {
		closeErr := errors.New("close failed")
		server := NewServer(&serverTestProvider{})
		connection := &serverTestConnection{
			closeErr: closeErr,
		}
		connectionID := addServerTestConnection(t, server, connection)

		response, err := server.CloseConnection(
			context.Background(),
			&providerv1.CloseConnectionRequest{
				ConnectionId: connectionID,
			},
		)
		if response != nil {
			t.Fatal("CloseConnection returned a response with an error")
		}
		if !errors.Is(err, closeErr) {
			t.Fatalf("CloseConnection error = %v, want %v", err, closeErr)
		}

		_, _, found := server.connections.acquire(connectionID)
		if found {
			t.Fatal("connection remained registered after close error")
		}
	})
}

func TestServerClose(t *testing.T) {
	firstCloseErr := errors.New("first close failed")
	secondCloseErr := errors.New("second close failed")
	server := NewServer(&serverTestProvider{})
	connections := []*serverTestConnection{
		{closeErr: firstCloseErr},
		{},
		{closeErr: secondCloseErr},
	}
	connectionIDs := make([]string, 0, len(connections))

	for _, connection := range connections {
		connectionIDs = append(
			connectionIDs,
			addServerTestConnection(t, server, connection),
		)
	}

	err := server.Close(context.Background())
	if !errors.Is(err, firstCloseErr) {
		t.Errorf("Close error = %v, missing %v", err, firstCloseErr)
	}
	if !errors.Is(err, secondCloseErr) {
		t.Errorf("Close error = %v, missing %v", err, secondCloseErr)
	}

	for index, connection := range connections {
		closeCount, _ := connection.closeSnapshot()
		if closeCount != 1 {
			t.Errorf(
				"connection %d close count = %d, want 1",
				index,
				closeCount,
			)
		}

		_, _, found := server.connections.acquire(connectionIDs[index])
		if found {
			t.Errorf("connection %d remained registered", index)
		}
	}
}

func addServerTestConnection(
	t *testing.T,
	server *Server,
	connection Connection,
) string {
	t.Helper()

	connectionID, err := server.connections.add(connection)
	if err != nil {
		t.Fatalf("add connection: %v", err)
	}

	return connectionID
}
