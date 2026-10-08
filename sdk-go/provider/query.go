package provider

import (
	"errors"
	"fmt"
	"io"
	"strings"

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
	if request.GetAllowPartialResults() && !request.GetAcceptOutcome() {
		return status.Error(
			codes.InvalidArgument,
			"allow_partial_results requires accept_outcome",
		)
	}
	if err := validateQueryRequestValues(request); err != nil {
		return status.Errorf(
			codes.InvalidArgument,
			"query is invalid: %v",
			err,
		)
	}

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
	_, lobTransportAvailable := connection.(LobConnection)

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
			outcome, outcomeErr := prepareQueryOutcome(resultStream, request)
			if outcomeErr != nil {
				return status.Errorf(
					codes.Internal,
					"provider returned an invalid query outcome: %v",
					outcomeErr,
				)
			}
			if outcome == nil {
				return nil
			}
			if err := serverStream.Send(
				&providerv1.QueryResponse{
					Outcome: outcome,
				},
			); err != nil {
				return err
			}
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

		if err := validateOutputBatchFeatures(
			batch,
			request.GetAcceptedFeatures(),
			lobTransportAvailable,
		); err != nil {
			return status.Errorf(
				codes.Internal,
				"provider returned an unsupported query value: %v",
				err,
			)
		}
		batch = prepareOutputBatch(batch, request.GetConnectionId())

		if err := serverStream.Send(
			&providerv1.QueryResponse{
				Batch: batch,
			},
		); err != nil {
			return err
		}

	}
}

func prepareQueryOutcome(
	resultStream ResultStream,
	request *providerv1.QueryRequest,
) (*providerv1.QueryOutcome, error) {
	outcome := &providerv1.QueryOutcome{
		Completion: providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE,
	}
	if outcomeStream, ok := resultStream.(QueryOutcomeStream); ok {
		providerOutcome := outcomeStream.Outcome()
		if providerOutcome == nil {
			return nil, errors.New("outcome stream returned nil")
		}
		outcome = proto.Clone(providerOutcome).(*providerv1.QueryOutcome)
	}

	if err := validateQueryOutcome(outcome); err != nil {
		return nil, err
	}
	if outcome.GetCompletion() ==
		providerv1.QueryCompletion_QUERY_COMPLETION_PARTIAL &&
		!request.GetAllowPartialResults() {
		return nil, errors.New(
			"partial results were produced for a strict query",
		)
	}
	if !request.GetAcceptOutcome() {
		if len(outcome.GetWarnings()) > 0 {
			return nil, errors.New(
				"query warnings were produced without outcome acceptance",
			)
		}
		return nil, nil
	}

	return outcome, nil
}

func validateQueryOutcome(outcome *providerv1.QueryOutcome) error {
	switch outcome.GetCompletion() {
	case providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE:
	case providerv1.QueryCompletion_QUERY_COMPLETION_PARTIAL:
	case providerv1.QueryCompletion_QUERY_COMPLETION_UNSPECIFIED:
		return errors.New("completion is QUERY_COMPLETION_UNSPECIFIED")
	default:
		return fmt.Errorf(
			"completion %d is unknown",
			outcome.GetCompletion(),
		)
	}

	partialResultCauses := 0
	for index, warning := range outcome.GetWarnings() {
		if warning == nil {
			return fmt.Errorf("warnings[%d] is nil", index)
		}
		if strings.TrimSpace(warning.GetMessage()) == "" {
			return fmt.Errorf("warnings[%d].message is required", index)
		}
		if warning.GetCode() != "" &&
			strings.TrimSpace(warning.GetCode()) == "" {
			return fmt.Errorf(
				"warnings[%d].code must not be whitespace",
				index,
			)
		}
		if warning.GetTarget() != "" &&
			strings.TrimSpace(warning.GetTarget()) == "" {
			return fmt.Errorf(
				"warnings[%d].target must not be whitespace",
				index,
			)
		}
		switch warning.GetRole() {
		case providerv1.QueryDiagnosticRole_QUERY_DIAGNOSTIC_ROLE_WARNING:
		case providerv1.QueryDiagnosticRole_QUERY_DIAGNOSTIC_ROLE_PARTIAL_RESULT_CAUSE:
			partialResultCauses++
		case providerv1.QueryDiagnosticRole_QUERY_DIAGNOSTIC_ROLE_UNSPECIFIED:
			return fmt.Errorf(
				"warnings[%d].role is QUERY_DIAGNOSTIC_ROLE_UNSPECIFIED",
				index,
			)
		default:
			return fmt.Errorf(
				"warnings[%d].role %d is unknown",
				index,
				warning.GetRole(),
			)
		}
	}

	switch outcome.GetCompletion() {
	case providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE:
		if partialResultCauses > 0 {
			return errors.New(
				"complete outcome must not contain partial-result causes",
			)
		}
	case providerv1.QueryCompletion_QUERY_COMPLETION_PARTIAL:
		if partialResultCauses == 0 {
			return errors.New(
				"partial outcome requires at least one partial-result cause",
			)
		}
	}

	return nil
}
