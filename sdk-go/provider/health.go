package provider

import (
	"context"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Health returns the connection-agnostic health state of the provider.
func (s *Server) Health(
	ctx context.Context,
	_ *providerv1.HealthRequest,
) (*providerv1.HealthResponse, error) {
	response, err := s.implementation.Health(ctx)
	if err != nil {
		return nil, err
	}

	if response == nil {
		return nil, status.Error(
			codes.Internal,
			"provider returned a nil health response",
		)
	}

	return response, nil
}
