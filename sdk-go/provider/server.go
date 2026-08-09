package provider

import (
	"context"
	"errors"
	"fmt"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server adapts a Provider implementation to the generated gRPC service.
//
// The SDK owns the transport-facing connection identifiers and maps them to the
// logical Connection instances returned by the provider.
type Server struct {
	providerv1.UnimplementedProviderServiceServer

	implementation Provider
	connections    *connectionRegistry
}

// NewServer creates a provider gRPC service adapter.
func NewServer(implementation Provider) *Server {
	return &Server{
		implementation: implementation,
		connections:    newConnectionRegistry(),
	}
}

func (s *Server) acquireConnection(
	connectionID string,
) (Connection, func(), error) {
	if connectionID == "" {
		return nil, nil, status.Error(
			codes.InvalidArgument,
			"connection_id is required",
		)
	}

	connection, release, found :=
		s.connections.acquire(connectionID)

	if !found {
		return nil, nil, status.Errorf(
			codes.NotFound,
			"connection %q was not found",
			connectionID,
		)
	}

	return connection, release, nil
}

// GetCapabilities returns the capabilities exposed by the provider.
func (s *Server) GetCapabilities(
	ctx context.Context,
	_ *providerv1.GetCapabilitiesRequest,
) (*providerv1.GetCapabilitiesResponse, error) {
	response, err := s.implementation.Capabilities(ctx)
	if err != nil {
		return nil, err
	}

	if response == nil {
		return nil, status.Error(
			codes.Internal,
			"provider returned nil capabilities",
		)
	}

	return response, nil
}

// GetSchema returns the optional Kubling schema exposed by the provider.
func (s *Server) GetSchema(
	ctx context.Context,
	_ *providerv1.GetSchemaRequest,
) (*providerv1.GetSchemaResponse, error) {
	if metadataProvider, ok := s.implementation.(MetadataProvider); ok {
		metadata, err := metadataProvider.Metadata(ctx)
		if err != nil {
			return nil, err
		}
		if metadata != nil {
			return &providerv1.GetSchemaResponse{Metadata: metadata}, nil
		}
	}

	schemaProvider, ok := s.implementation.(SchemaProvider)
	if !ok {
		return &providerv1.GetSchemaResponse{}, nil
	}

	schemaDDL, err := schemaProvider.Schema(ctx)
	if err != nil {
		return nil, err
	}

	return &providerv1.GetSchemaResponse{
		SchemaDdl: schemaDDL,
	}, nil
}

// OpenConnection opens and registers a logical data source connection.
func (s *Server) OpenConnection(
	ctx context.Context,
	_ *providerv1.OpenConnectionRequest,
) (*providerv1.OpenConnectionResponse, error) {
	connection, err := s.implementation.Open(ctx)
	if err != nil {
		return nil, err
	}

	if connection == nil {
		return nil, status.Error(
			codes.Internal,
			"provider returned a nil connection",
		)
	}

	connectionID, err := s.connections.add(connection)
	if err != nil {
		closeErr := connection.Close(ctx)

		return nil, errors.Join(
			fmt.Errorf("register connection: %w", err),
			closeErr,
		)
	}

	return &providerv1.OpenConnectionResponse{
		ConnectionId: connectionID,
	}, nil
}

// CloseConnection closes and unregisters a logical data source connection.
func (s *Server) CloseConnection(
	ctx context.Context,
	request *providerv1.CloseConnectionRequest,
) (*providerv1.CloseConnectionResponse, error) {
	connectionID := request.GetConnectionId()
	if connectionID == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"connection_id is required",
		)
	}

	found, err := s.connections.close(ctx, connectionID)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, status.Errorf(
			codes.NotFound,
			"connection %q was not found",
			connectionID,
		)
	}

	return &providerv1.CloseConnectionResponse{}, nil
}

// Close closes all connections currently managed by the server.
func (s *Server) Close(ctx context.Context) error {
	return s.connections.closeAll(ctx)
}

// Verify that Server implements the generated gRPC service.
var _ providerv1.ProviderServiceServer = (*Server)(nil)
