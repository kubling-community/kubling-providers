package provider

import (
	"context"
	"fmt"
	"io"
	"strings"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const maxTransactionBranchIDBytes = 256

// TransactionBranchConnection may be implemented by a logical connection that
// can host a durable transaction branch.
//
// BeginBranch associates the branch with this connection. Repeated BeginBranch
// and PrepareBranch calls for the same branch and connection must return their
// original results. PrepareBranch must make a PREPARED result recoverable
// independently from the connection before returning it. CommitBranch and
// RollbackBranch resolve an active branch while the connection is still
// available and must retain an idempotent terminal result.
type TransactionBranchConnection interface {
	BeginBranch(context.Context, *providerv1.TransactionBranchId) error
	PrepareBranch(
		context.Context,
		*providerv1.TransactionBranchId,
	) (providerv1.TransactionPrepareOutcome, error)
	CommitBranch(
		context.Context,
		*providerv1.TransactionBranchId,
	) (providerv1.TransactionBranchState, error)
	RollbackBranch(
		context.Context,
		*providerv1.TransactionBranchId,
	) (providerv1.TransactionBranchState, error)
}

// TransactionRecoveryProvider may be implemented by a provider that exposes a
// durable transaction recovery manager.
//
// TransactionRecovery returns nil when recovery is not available for the
// current provider configuration.
type TransactionRecoveryProvider interface {
	TransactionRecovery() TransactionRecoveryManager
}

// TransactionRecoveryManager resolves prepared branches without depending on
// their original logical connections. Resolution and cleanup operations must
// be idempotent.
type TransactionRecoveryManager interface {
	// ResourceManagerID returns the stable identity advertised in capabilities.
	ResourceManagerID() string

	CommitBranch(
		context.Context,
		*providerv1.TransactionBranchId,
	) (providerv1.TransactionBranchState, error)
	RollbackBranch(
		context.Context,
		*providerv1.TransactionBranchId,
	) (providerv1.TransactionBranchState, error)
	RecoverBranches(context.Context) (TransactionRecoveryStream, error)
	BranchStatus(
		context.Context,
		*providerv1.TransactionBranchId,
	) (providerv1.TransactionBranchState, error)
	ForgetBranch(context.Context, *providerv1.TransactionBranchId) error
}

// TransactionRecoveryStream produces stable batches from one recovery scan.
//
// Next returns io.EOF after the snapshot has been exhausted. Close must release
// any resources held by the scan.
type TransactionRecoveryStream interface {
	Next(context.Context) ([]*providerv1.TransactionBranch, error)
	Close() error
}

// BeginTransactionBranch starts a durable branch on a logical connection.
func (s *Server) BeginTransactionBranch(
	ctx context.Context,
	request *providerv1.BeginTransactionBranchRequest,
) (*providerv1.BeginTransactionBranchResponse, error) {
	_, branch, err := s.prepareTransactionBranchRequest(request.GetBranch())
	if err != nil {
		return nil, err
	}

	connection, release, err := s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer release()

	branchConnection, ok := connection.(TransactionBranchConnection)
	if !ok {
		return nil, status.Error(
			codes.Unimplemented,
			"connection does not support durable transaction branches",
		)
	}
	if err := branchConnection.BeginBranch(ctx, branch); err != nil {
		return nil, err
	}

	return &providerv1.BeginTransactionBranchResponse{
		State: providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ACTIVE,
	}, nil
}

// PrepareTransactionBranch durably prepares a branch on its logical
// connection.
func (s *Server) PrepareTransactionBranch(
	ctx context.Context,
	request *providerv1.PrepareTransactionBranchRequest,
) (*providerv1.PrepareTransactionBranchResponse, error) {
	_, branch, err := s.prepareTransactionBranchRequest(request.GetBranch())
	if err != nil {
		return nil, err
	}

	connection, release, err := s.acquireConnection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer release()

	branchConnection, ok := connection.(TransactionBranchConnection)
	if !ok {
		return nil, status.Error(
			codes.Unimplemented,
			"connection does not support durable transaction branches",
		)
	}
	outcome, err := branchConnection.PrepareBranch(ctx, branch)
	if err != nil {
		return nil, err
	}
	if !validPrepareOutcome(outcome) {
		return nil, status.Errorf(
			codes.Internal,
			"provider returned invalid transaction prepare outcome %d",
			outcome,
		)
	}

	return &providerv1.PrepareTransactionBranchResponse{Outcome: outcome}, nil
}

// CommitTransactionBranch commits an active one-phase or prepared branch.
func (s *Server) CommitTransactionBranch(
	ctx context.Context,
	request *providerv1.CommitTransactionBranchRequest,
) (*providerv1.CommitTransactionBranchResponse, error) {
	manager, branch, err := s.prepareTransactionBranchRequest(request.GetBranch())
	if err != nil {
		return nil, err
	}

	var state providerv1.TransactionBranchState
	if request.GetOnePhase() {
		if request.ConnectionId == nil || request.GetConnectionId() == "" {
			return nil, status.Error(
				codes.InvalidArgument,
				"connection_id is required for a one-phase commit",
			)
		}

		connection, release, acquireErr :=
			s.acquireConnection(request.GetConnectionId())
		if acquireErr != nil {
			return nil, acquireErr
		}
		defer release()

		branchConnection, ok := connection.(TransactionBranchConnection)
		if !ok {
			return nil, status.Error(
				codes.Unimplemented,
				"connection does not support durable transaction branches",
			)
		}
		state, err = branchConnection.CommitBranch(ctx, branch)
	} else {
		if request.ConnectionId != nil {
			return nil, status.Error(
				codes.InvalidArgument,
				"connection_id must be absent when committing a prepared branch",
			)
		}
		state, err = manager.CommitBranch(ctx, branch)
	}
	if err != nil {
		return nil, err
	}
	if !validResolutionState(state) {
		return nil, status.Errorf(
			codes.Internal,
			"provider returned invalid transaction commit state %d",
			state,
		)
	}

	return &providerv1.CommitTransactionBranchResponse{State: state}, nil
}

// RollbackTransactionBranch rolls back an active or prepared branch.
func (s *Server) RollbackTransactionBranch(
	ctx context.Context,
	request *providerv1.RollbackTransactionBranchRequest,
) (*providerv1.RollbackTransactionBranchResponse, error) {
	manager, branch, err := s.prepareTransactionBranchRequest(request.GetBranch())
	if err != nil {
		return nil, err
	}

	var state providerv1.TransactionBranchState
	if request.ConnectionId != nil {
		if request.GetConnectionId() == "" {
			return nil, status.Error(
				codes.InvalidArgument,
				"connection_id must not be empty when rolling back an active branch",
			)
		}

		connection, release, acquireErr :=
			s.acquireConnection(request.GetConnectionId())
		if acquireErr != nil {
			return nil, acquireErr
		}
		defer release()

		branchConnection, ok := connection.(TransactionBranchConnection)
		if !ok {
			return nil, status.Error(
				codes.Unimplemented,
				"connection does not support durable transaction branches",
			)
		}
		state, err = branchConnection.RollbackBranch(ctx, branch)
	} else {
		state, err = manager.RollbackBranch(ctx, branch)
	}
	if err != nil {
		return nil, err
	}
	if !validResolutionState(state) {
		return nil, status.Errorf(
			codes.Internal,
			"provider returned invalid transaction rollback state %d",
			state,
		)
	}

	return &providerv1.RollbackTransactionBranchResponse{State: state}, nil
}

// RecoverTransactionBranches streams a stable snapshot of in-doubt branches.
func (s *Server) RecoverTransactionBranches(
	request *providerv1.RecoverTransactionBranchesRequest,
	serverStream providerv1.ProviderService_RecoverTransactionBranchesServer,
) (returnErr error) {
	manager, err := s.transactionRecoveryManager()
	if err != nil {
		return err
	}
	if err := validateRequestedResourceManager(
		request.GetResourceManagerId(),
		manager.ResourceManagerID(),
	); err != nil {
		return err
	}

	stream, err := manager.RecoverBranches(serverStream.Context())
	if err != nil {
		return err
	}
	if stream == nil {
		return status.Error(
			codes.Internal,
			"provider returned a nil transaction recovery stream",
		)
	}
	defer func() {
		if closeErr := stream.Close(); returnErr == nil {
			returnErr = closeErr
		}
	}()

	seen := make(map[string]struct{})
	for {
		branches, nextErr := stream.Next(serverStream.Context())
		if nextErr == io.EOF {
			return nil
		}
		if nextErr != nil {
			return nextErr
		}
		if len(branches) == 0 {
			return status.Error(
				codes.Internal,
				"provider returned an empty transaction recovery batch",
			)
		}

		response := &providerv1.RecoverTransactionBranchesResponse{
			Branches: make([]*providerv1.TransactionBranch, 0, len(branches)),
		}
		for index, recovered := range branches {
			if recovered == nil {
				return status.Errorf(
					codes.Internal,
					"provider returned nil transaction branch at batch index %d",
					index,
				)
			}
			if !validRecoveryState(recovered.GetState()) {
				return status.Errorf(
					codes.Internal,
					"provider returned invalid recovery state %d",
					recovered.GetState(),
				)
			}
			branch, validationErr := validateTransactionBranchID(
				recovered.GetBranch(),
				manager.ResourceManagerID(),
			)
			if validationErr != nil {
				return status.Errorf(
					codes.Internal,
					"provider returned invalid recovered branch: %v",
					validationErr,
				)
			}
			key := transactionBranchKey(branch)
			if _, duplicate := seen[key]; duplicate {
				return status.Error(
					codes.Internal,
					"provider returned a duplicate recovered branch",
				)
			}
			seen[key] = struct{}{}
			response.Branches = append(response.Branches, &providerv1.TransactionBranch{
				Branch: branch,
				State:  recovered.GetState(),
			})
		}

		if err := serverStream.Send(response); err != nil {
			return err
		}
	}
}

// GetTransactionBranchStatus returns a durable branch state.
func (s *Server) GetTransactionBranchStatus(
	ctx context.Context,
	request *providerv1.GetTransactionBranchStatusRequest,
) (*providerv1.GetTransactionBranchStatusResponse, error) {
	manager, branch, err := s.prepareTransactionBranchRequest(request.GetBranch())
	if err != nil {
		return nil, err
	}
	state, err := manager.BranchStatus(ctx, branch)
	if err != nil {
		return nil, err
	}
	if !validReportedState(state) {
		return nil, status.Errorf(
			codes.Internal,
			"provider returned invalid transaction branch state %d",
			state,
		)
	}

	return &providerv1.GetTransactionBranchStatusResponse{State: state}, nil
}

// ForgetTransactionBranch releases a retained terminal outcome.
func (s *Server) ForgetTransactionBranch(
	ctx context.Context,
	request *providerv1.ForgetTransactionBranchRequest,
) (*providerv1.ForgetTransactionBranchResponse, error) {
	manager, branch, err := s.prepareTransactionBranchRequest(request.GetBranch())
	if err != nil {
		return nil, err
	}
	if err := manager.ForgetBranch(ctx, branch); err != nil {
		return nil, err
	}

	return &providerv1.ForgetTransactionBranchResponse{}, nil
}

func (s *Server) prepareTransactionBranchRequest(
	requested *providerv1.TransactionBranchId,
) (TransactionRecoveryManager, *providerv1.TransactionBranchId, error) {
	manager, err := s.transactionRecoveryManager()
	if err != nil {
		return nil, nil, err
	}
	branch, err := validateTransactionBranchID(
		requested,
		manager.ResourceManagerID(),
	)
	if err != nil {
		return nil, nil, status.Errorf(
			codes.InvalidArgument,
			"transaction branch is invalid: %v",
			err,
		)
	}

	return manager, branch, nil
}

func (s *Server) transactionRecoveryManager() (
	TransactionRecoveryManager,
	error,
) {
	recoveryProvider, ok := s.implementation.(TransactionRecoveryProvider)
	if !ok {
		return nil, status.Error(
			codes.Unimplemented,
			"provider does not support durable transaction recovery",
		)
	}
	manager := recoveryProvider.TransactionRecovery()
	if manager == nil {
		return nil, status.Error(
			codes.Unimplemented,
			"provider does not support durable transaction recovery",
		)
	}
	if err := validateResourceManagerID(manager.ResourceManagerID()); err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"provider returned invalid transaction recovery manager: %v",
			err,
		)
	}

	return manager, nil
}

func validateTransactionRecoveryImplementation(
	capabilities *Capabilities,
	implementation Provider,
) error {
	recovery := capabilities.GetTransactions().GetRecovery()
	recoveryProvider, implemented := implementation.(TransactionRecoveryProvider)
	var manager TransactionRecoveryManager
	if implemented {
		manager = recoveryProvider.TransactionRecovery()
	}

	if recovery == nil || !recovery.GetSupported() {
		if manager != nil {
			return fmt.Errorf(
				"provider exposes a transaction recovery manager without advertising it",
			)
		}

		return nil
	}
	if manager == nil {
		return fmt.Errorf(
			"transactions.recovery is supported but the provider exposes no recovery manager",
		)
	}
	if err := validateResourceManagerID(manager.ResourceManagerID()); err != nil {
		return fmt.Errorf("transaction recovery manager: %w", err)
	}
	if manager.ResourceManagerID() != recovery.GetResourceManagerId() {
		return fmt.Errorf(
			"transaction recovery manager ID %q does not match advertised ID %q",
			manager.ResourceManagerID(),
			recovery.GetResourceManagerId(),
		)
	}

	return nil
}

func validateTransactionBranchID(
	branch *providerv1.TransactionBranchId,
	expectedResourceManagerID string,
) (*providerv1.TransactionBranchId, error) {
	if branch == nil {
		return nil, fmt.Errorf("branch is required")
	}
	if err := validateResourceManagerID(branch.GetResourceManagerId()); err != nil {
		return nil, err
	}
	if branch.GetResourceManagerId() != expectedResourceManagerID {
		return nil, fmt.Errorf(
			"resource manager ID %q does not match %q",
			branch.GetResourceManagerId(),
			expectedResourceManagerID,
		)
	}
	if len(branch.GetBranchId()) == 0 {
		return nil, fmt.Errorf("branch_id is required")
	}
	if len(branch.GetBranchId()) > maxTransactionBranchIDBytes {
		return nil, fmt.Errorf(
			"branch_id exceeds %d bytes",
			maxTransactionBranchIDBytes,
		)
	}

	return proto.Clone(branch).(*providerv1.TransactionBranchId), nil
}

func validateRequestedResourceManager(requested, expected string) error {
	if err := validateResourceManagerID(requested); err != nil {
		return status.Errorf(
			codes.InvalidArgument,
			"resource_manager_id is invalid: %v",
			err,
		)
	}
	if requested != expected {
		return status.Errorf(
			codes.FailedPrecondition,
			"resource manager ID %q does not match provider resource manager %q",
			requested,
			expected,
		)
	}

	return nil
}

func validateResourceManagerID(value string) error {
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("resource_manager_id must be a non-empty canonical identifier")
	}
	if len(value) > maxResourceManagerIDBytes {
		return fmt.Errorf(
			"resource_manager_id exceeds %d bytes",
			maxResourceManagerIDBytes,
		)
	}

	return nil
}

func transactionBranchKey(branch *providerv1.TransactionBranchId) string {
	return branch.GetResourceManagerId() + "\x00" + string(branch.GetBranchId())
}

func validPrepareOutcome(outcome providerv1.TransactionPrepareOutcome) bool {
	return outcome == providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED ||
		outcome == providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_READ_ONLY
}

func validReportedState(state providerv1.TransactionBranchState) bool {
	_, known := providerv1.TransactionBranchState_name[int32(state)]
	return known && state != providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED
}

func validResolutionState(state providerv1.TransactionBranchState) bool {
	switch state {
	case providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_READ_ONLY,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_COMMIT,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_ROLLBACK,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_MIXED,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_HAZARD:
		return true
	default:
		return false
	}
}

func validRecoveryState(state providerv1.TransactionBranchState) bool {
	switch state {
	case providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_COMMIT,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_ROLLBACK,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_MIXED,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_HAZARD:
		return true
	default:
		return false
	}
}
