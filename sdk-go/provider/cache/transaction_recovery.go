package cache

import (
	"context"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cachedRecoveryManager struct {
	providersdk.TransactionRecoveryManager
	state *cacheState
}

func (m *cachedRecoveryManager) CommitBranch(
	ctx context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	state, err := m.TransactionRecoveryManager.CommitBranch(ctx, branch)
	if err != nil {
		m.state.resolveBranchInvalidation(branch, true)
		return state, err
	}
	m.state.resolveBranchInvalidation(branch, branchStateMayHaveCommitted(state))

	return state, nil
}

func (m *cachedRecoveryManager) RollbackBranch(
	ctx context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	state, err := m.TransactionRecoveryManager.RollbackBranch(ctx, branch)
	if err != nil {
		m.state.resolveBranchInvalidation(branch, true)
		return state, err
	}
	m.state.resolveBranchInvalidation(branch, branchStateMayHaveCommitted(state))

	return state, nil
}

func (m *cachedRecoveryManager) ForgetBranch(
	ctx context.Context,
	branch *providerv1.TransactionBranchId,
) error {
	if err := m.TransactionRecoveryManager.ForgetBranch(ctx, branch); err != nil {
		return err
	}
	m.state.discardBranchInvalidation(branch)

	return nil
}

func (c *cachedConnection) BeginBranch(
	ctx context.Context,
	branch *providerv1.TransactionBranchId,
) error {
	branchConnection, err := c.transactionBranchConnection()
	if err != nil {
		return err
	}

	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	wasActive := c.transactionActive
	if err := branchConnection.BeginBranch(ctx, branch); err != nil {
		return err
	}
	c.transactionActive = true
	if !wasActive {
		c.clearPendingLocked()
	}

	return nil
}

func (c *cachedConnection) PrepareBranch(
	ctx context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionPrepareOutcome, error) {
	branchConnection, err := c.transactionBranchConnection()
	if err != nil {
		return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_UNSPECIFIED, err
	}

	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	outcome, err := branchConnection.PrepareBranch(ctx, branch)
	if err != nil {
		c.invalidatePendingLocked()
		return outcome, err
	}
	switch outcome {
	case providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED:
		c.state.retainBranchInvalidation(
			branch,
			c.pendingEntities,
			c.pendingAll,
		)
		c.transactionActive = false
		c.clearPendingLocked()
	case providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_READ_ONLY:
		c.transactionActive = false
		c.clearPendingLocked()
	default:
		c.invalidatePendingLocked()
	}

	return outcome, nil
}

func (c *cachedConnection) CommitBranch(
	ctx context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	branchConnection, err := c.transactionBranchConnection()
	if err != nil {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED, err
	}

	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	state, err := branchConnection.CommitBranch(ctx, branch)
	if err != nil {
		c.invalidatePendingLocked()
		return state, err
	}
	if branchStateMayHaveCommitted(state) {
		c.invalidatePendingLocked()
	}
	c.transactionActive = false
	c.clearPendingLocked()

	return state, nil
}

func (c *cachedConnection) RollbackBranch(
	ctx context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	branchConnection, err := c.transactionBranchConnection()
	if err != nil {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED, err
	}

	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	state, err := branchConnection.RollbackBranch(ctx, branch)
	if err != nil {
		c.invalidatePendingLocked()
		return state, err
	}
	if branchStateMayHaveCommitted(state) {
		c.invalidatePendingLocked()
	}
	c.transactionActive = false
	c.clearPendingLocked()

	return state, nil
}

func (c *cachedConnection) transactionBranchConnection() (
	providersdk.TransactionBranchConnection,
	error,
) {
	branchConnection, ok := c.Connection.(providersdk.TransactionBranchConnection)
	if !ok {
		return nil, status.Error(
			codes.Unimplemented,
			"connection does not support durable transaction branches",
		)
	}

	return branchConnection, nil
}

func branchStateMayHaveCommitted(state providerv1.TransactionBranchState) bool {
	switch state {
	case providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_COMMIT,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_MIXED,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_HEURISTIC_HAZARD,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND:
		return true
	default:
		return false
	}
}

var _ providersdk.TransactionRecoveryProvider = (*Provider)(nil)
var _ providersdk.TransactionRecoveryManager = (*cachedRecoveryManager)(nil)
var _ providersdk.TransactionBranchConnection = (*cachedConnection)(nil)
