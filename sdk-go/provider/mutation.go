package provider

import (
	"context"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	if err := validateInputBatchStructure(request.GetRows()); err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"insert contains an invalid value: %v",
			err,
		)
	}

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
	_, lobTransportAvailable := connection.(LobConnection)
	if err := validateOutputBatchFeatures(
		response.GetGeneratedValues(),
		request.GetAcceptedFeatures(),
		lobTransportAvailable,
	); err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"provider returned an unsupported generated value: %v",
			err,
		)
	}
	preparedValues := prepareOutputBatch(
		response.GetGeneratedValues(),
		request.GetConnectionId(),
	)
	if preparedValues != response.GetGeneratedValues() {
		response = proto.Clone(response).(*providerv1.InsertResponse)
		response.GeneratedValues = preparedValues
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

	for assignmentIndex, assignment := range request.GetAssignments() {
		if assignment == nil {
			continue
		}
		if err := validateExpressionValues(assignment.GetValue()); err != nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"update assignment %d contains an invalid value: %v",
				assignmentIndex,
				err,
			)
		}
	}
	if err := validateExpressionValues(request.GetFilter()); err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"update filter contains an invalid value: %v",
			err,
		)
	}

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

	if err := validateExpressionValues(request.GetFilter()); err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"delete filter contains an invalid value: %v",
			err,
		)
	}

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
