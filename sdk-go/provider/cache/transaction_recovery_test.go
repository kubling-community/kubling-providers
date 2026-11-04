package cache

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
)

type recoveryCacheTestProvider struct {
	*testProvider
	manager providersdk.TransactionRecoveryManager
}

func (p *recoveryCacheTestProvider) TransactionRecovery() providersdk.TransactionRecoveryManager {
	return p.manager
}

type recoveryCacheTestConnection struct {
	*testConnection
}

func (*recoveryCacheTestConnection) BeginBranch(
	context.Context,
	*providerv1.TransactionBranchId,
) error {
	return nil
}

func (*recoveryCacheTestConnection) PrepareBranch(
	context.Context,
	*providerv1.TransactionBranchId,
) (providerv1.TransactionPrepareOutcome, error) {
	return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED, nil
}

func (*recoveryCacheTestConnection) CommitBranch(
	context.Context,
	*providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED, nil
}

func (*recoveryCacheTestConnection) RollbackBranch(
	context.Context,
	*providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK, nil
}

type recoveryCacheTestManager struct {
	commitState   providerv1.TransactionBranchState
	rollbackState providerv1.TransactionBranchState
}

func (*recoveryCacheTestManager) ResourceManagerID() string {
	return "warehouse-primary"
}

func (m *recoveryCacheTestManager) CommitBranch(
	context.Context,
	*providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	return m.commitState, nil
}

func (m *recoveryCacheTestManager) RollbackBranch(
	context.Context,
	*providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	return m.rollbackState, nil
}

func (*recoveryCacheTestManager) RecoverBranches(
	context.Context,
) (providersdk.TransactionRecoveryStream, error) {
	return &emptyRecoveryCacheTestStream{}, nil
}

func (m *recoveryCacheTestManager) BranchStatus(
	context.Context,
	*providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	return m.commitState, nil
}

func (*recoveryCacheTestManager) ForgetBranch(
	context.Context,
	*providerv1.TransactionBranchId,
) error {
	return nil
}

type emptyRecoveryCacheTestStream struct{}

func (*emptyRecoveryCacheTestStream) Next(
	context.Context,
) ([]*providerv1.TransactionBranch, error) {
	return nil, io.EOF
}

func (*emptyRecoveryCacheTestStream) Close() error {
	return nil
}

func TestPreparedBranchInvalidatesCacheOnlyWhenCommitted(t *testing.T) {
	branch := &providerv1.TransactionBranchId{
		ResourceManagerId: "warehouse-primary",
		BranchId:          []byte("branch-1"),
	}
	tests := []struct {
		name          string
		resolve       func(providersdk.TransactionRecoveryManager) error
		wantQueryCall int32
	}{
		{
			name: "commit",
			resolve: func(manager providersdk.TransactionRecoveryManager) error {
				_, err := manager.CommitBranch(context.Background(), branch)
				return err
			},
			wantQueryCall: 2,
		},
		{
			name: "rollback",
			resolve: func(manager providersdk.TransactionRecoveryManager) error {
				_, err := manager.RollbackBranch(context.Background(), branch)
				return err
			},
			wantQueryCall: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var queryCalls atomic.Int32
			base := queryTestConnection(&queryCalls)
			base.insertFunc = func(
				context.Context,
				*providerv1.InsertRequest,
			) (*providerv1.InsertResponse, error) {
				return &providerv1.InsertResponse{}, nil
			}
			connection := &recoveryCacheTestConnection{testConnection: base}
			manager := &recoveryCacheTestManager{
				commitState:   providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED,
				rollbackState: providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK,
			}
			cachedProvider, _ := Wrap(
				&recoveryCacheTestProvider{
					testProvider: &testProvider{connection: connection},
					manager:      manager,
				},
				Config{},
			)
			wrapped := openTestConnection(t, cachedProvider, "source")
			branchConnection, ok := wrapped.(providersdk.TransactionBranchConnection)
			if !ok {
				t.Fatalf("wrapped connection type %T has no branch support", wrapped)
			}
			request := testQuery("", "TASK")
			queryAndClose(t, wrapped, request)
			queryAndClose(t, wrapped, request)
			if queryCalls.Load() != 1 {
				t.Fatalf("initial Query calls = %d, want 1", queryCalls.Load())
			}

			if err := branchConnection.BeginBranch(context.Background(), branch); err != nil {
				t.Fatalf("BeginBranch() error = %v", err)
			}
			if _, err := wrapped.Insert(
				context.Background(),
				&providerv1.InsertRequest{Entity: testEntity("TASK")},
			); err != nil {
				t.Fatalf("Insert() error = %v", err)
			}
			outcome, err := branchConnection.PrepareBranch(context.Background(), branch)
			if err != nil {
				t.Fatalf("PrepareBranch() error = %v", err)
			}
			if outcome != providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED {
				t.Fatalf("PrepareBranch() outcome = %s", outcome)
			}
			outcome, err = branchConnection.PrepareBranch(context.Background(), branch)
			if err != nil {
				t.Fatalf("repeated PrepareBranch() error = %v", err)
			}
			if outcome != providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED {
				t.Fatalf("repeated PrepareBranch() outcome = %s", outcome)
			}

			queryAndClose(t, wrapped, request)
			if queryCalls.Load() != 1 {
				t.Fatalf("prepare invalidated cache: Query calls = %d", queryCalls.Load())
			}
			if err := test.resolve(cachedProvider.TransactionRecovery()); err != nil {
				t.Fatalf("resolve branch: %v", err)
			}
			queryAndClose(t, wrapped, request)
			if queryCalls.Load() != test.wantQueryCall {
				t.Fatalf(
					"Query calls after %s = %d, want %d",
					test.name,
					queryCalls.Load(),
					test.wantQueryCall,
				)
			}
		})
	}
}

func TestRecoveredCommitWithoutPendingMetadataInvalidatesAll(t *testing.T) {
	var queryCalls atomic.Int32
	base := queryTestConnection(&queryCalls)
	manager := &recoveryCacheTestManager{
		commitState: providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED,
	}
	cachedProvider, _ := Wrap(
		&recoveryCacheTestProvider{
			testProvider: &testProvider{connection: base},
			manager:      manager,
		},
		Config{},
	)
	wrapped := openTestConnection(t, cachedProvider, "source")
	request := testQuery("", "TASK")
	queryAndClose(t, wrapped, request)
	queryAndClose(t, wrapped, request)

	branch := &providerv1.TransactionBranchId{
		ResourceManagerId: "warehouse-primary",
		BranchId:          []byte("recovered-branch"),
	}
	if _, err := cachedProvider.TransactionRecovery().CommitBranch(
		context.Background(),
		branch,
	); err != nil {
		t.Fatalf("CommitBranch() error = %v", err)
	}
	queryAndClose(t, wrapped, request)
	if queryCalls.Load() != 2 {
		t.Fatalf("Query calls after recovered commit = %d, want 2", queryCalls.Load())
	}
}
