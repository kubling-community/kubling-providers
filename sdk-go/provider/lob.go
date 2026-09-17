package provider

import (
	"context"
	"errors"
	"io"
	"math"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ReadLob streams a provider-owned LOB through its logical connection.
func (s *Server) ReadLob(
	request *providerv1.ReadLobRequest,
	serverStream providerv1.ProviderService_ReadLobServer,
) (returnErr error) {
	connection, release, err := s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return err
	}
	defer release()

	lobConnection, ok := connection.(LobConnection)
	if !ok {
		return status.Error(
			codes.Unimplemented,
			"connection does not support provider LOB reads",
		)
	}
	if request.GetLobId() == "" {
		return status.Error(codes.InvalidArgument, "lob_id is required")
	}

	providerRequest := proto.Clone(request).(*providerv1.ReadLobRequest)
	providerRequest.ConnectionId = ""
	stream, err := lobConnection.ReadLob(serverStream.Context(), providerRequest)
	if err != nil {
		return err
	}
	if stream == nil {
		return status.Error(codes.Internal, "provider returned a nil LOB stream")
	}

	defer func() {
		closeErr := stream.Close()
		if returnErr == nil {
			returnErr = closeErr
		}
	}()

	expectedOffset := request.GetOffset()
	var bytesSent uint64
	for {
		response, err := stream.Next(serverStream.Context())
		if errors.Is(err, io.EOF) {
			return status.Error(
				codes.Internal,
				"provider LOB stream ended before end_of_read",
			)
		}
		if err != nil {
			return err
		}
		if response == nil {
			return status.Error(
				codes.Internal,
				"provider returned a nil LOB response",
			)
		}

		chunkSize := uint64(len(response.GetData()))
		if response.GetOffset() != expectedOffset {
			return status.Errorf(
				codes.Internal,
				"provider LOB chunk offset %d does not match expected offset %d",
				response.GetOffset(),
				expectedOffset,
			)
		}
		if !response.GetEndOfRead() && chunkSize == 0 {
			return status.Error(
				codes.Internal,
				"provider returned an empty non-terminal LOB chunk",
			)
		}
		if maxChunkBytes := request.GetMaxChunkBytes(); maxChunkBytes > 0 && chunkSize > uint64(maxChunkBytes) {
			return status.Errorf(
				codes.Internal,
				"provider LOB chunk has %d bytes, exceeding requested maximum %d",
				chunkSize,
				maxChunkBytes,
			)
		}
		if request.Length != nil &&
			(chunkSize > request.GetLength() ||
				bytesSent > request.GetLength()-chunkSize) {
			return status.Error(
				codes.Internal,
				"provider LOB stream exceeded the requested length",
			)
		}
		if chunkSize > math.MaxUint64-expectedOffset {
			return status.Error(
				codes.Internal,
				"provider LOB chunk offset overflow",
			)
		}

		if err := serverStream.Send(response); err != nil {
			return err
		}

		expectedOffset += chunkSize
		bytesSent += chunkSize
		if response.GetEndOfRead() {
			return nil
		}
	}
}

// ReleaseLob releases a provider-owned LOB reference.
func (s *Server) ReleaseLob(
	ctx context.Context,
	request *providerv1.ReleaseLobRequest,
) (*providerv1.ReleaseLobResponse, error) {
	connection, release, err := s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer release()

	lobConnection, ok := connection.(LobConnection)
	if !ok {
		return nil, status.Error(
			codes.Unimplemented,
			"connection does not support provider LOB release",
		)
	}
	if request.GetLobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "lob_id is required")
	}

	providerRequest := proto.Clone(request).(*providerv1.ReleaseLobRequest)
	providerRequest.ConnectionId = ""
	if err := lobConnection.ReleaseLob(ctx, providerRequest); err != nil {
		return nil, err
	}

	return &providerv1.ReleaseLobResponse{}, nil
}
