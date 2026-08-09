package provider

import (
	"context"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/protobuf/proto"
)

// Insert inserts one or more tuples through the logical connection.
func (s *Server) Insert(
	ctx context.Context,
	request *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	connection, release, err :=
		s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer release()

	providerRequest :=
		proto.Clone(request).(*providerv1.InsertRequest)
	providerRequest.ConnectionId = ""

	response, err := connection.Insert(ctx, providerRequest)
	if err != nil {
		return nil, err
	}

	if response == nil {
		return &providerv1.InsertResponse{}, nil
	}

	return response, nil
}

// Update updates the tuples selected by the request.
func (s *Server) Update(
	ctx context.Context,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	connection, release, err :=
		s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer release()

	providerRequest :=
		proto.Clone(request).(*providerv1.UpdateRequest)
	providerRequest.ConnectionId = ""

	response, err := connection.Update(ctx, providerRequest)
	if err != nil {
		return nil, err
	}

	if response == nil {
		return &providerv1.UpdateResponse{}, nil
	}

	return response, nil
}

// Delete deletes the tuples selected by the request.
func (s *Server) Delete(
	ctx context.Context,
	request *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	connection, release, err :=
		s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer release()

	providerRequest :=
		proto.Clone(request).(*providerv1.DeleteRequest)
	providerRequest.ConnectionId = ""

	response, err := connection.Delete(ctx, providerRequest)
	if err != nil {
		return nil, err
	}

	if response == nil {
		return &providerv1.DeleteResponse{}, nil
	}

	return response, nil
}
