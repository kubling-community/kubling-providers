package provider

import (
	"errors"
	"io"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Query executes a query and streams the resulting batches.
func (s *Server) Query(
	request *providerv1.QueryRequest,
	serverStream providerv1.ProviderService_QueryServer,
) (returnErr error) {
	connection, release, err :=
		s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return err
	}
	defer release()

	providerRequest :=
		proto.Clone(request).(*providerv1.QueryRequest)
	providerRequest.ConnectionId = ""

	resultStream, err :=
		connection.Query(serverStream.Context(), providerRequest)
	if err != nil {
		return err
	}

	if resultStream == nil {
		return status.Error(
			codes.Internal,
			"provider returned a nil result stream",
		)
	}

	defer func() {
		closeErr := resultStream.Close()

		// Preserve the primary query or transport error. A close error is returned
		// only when the stream otherwise completed successfully.
		if returnErr == nil {
			returnErr = closeErr
		}
	}()

	for {
		batch, err := resultStream.Next(serverStream.Context())

		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		if batch == nil {
			return status.Error(
				codes.Internal,
				"provider returned a nil query batch",
			)
		}

		if err := serverStream.Send(
			&providerv1.QueryResponse{
				Batch: batch,
			},
		); err != nil {
			return err
		}

	}
}
