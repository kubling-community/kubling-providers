package providertest

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/protobuf/proto"
)

const testResourceManagerID = "conformance-test-resource"

type testStore struct {
	mu      sync.Mutex
	records map[string]testRecord
}

type testRecord struct {
	branch *providerv1.TransactionBranchId
	state  providerv1.TransactionBranchState
}

type testProvider struct {
	store *testStore
}

func (*testProvider) Capabilities(context.Context) (*providersdk.Capabilities, error) {
	return &providersdk.Capabilities{
		Transactions: &providerv1.TransactionCapabilities{
			Supported: true,
			Recovery: &providerv1.TransactionRecoveryCapabilities{
				Supported:         true,
				ResourceManagerId: testResourceManagerID,
			},
		},
	}, nil
}

func (*testProvider) Health(context.Context) (*providerv1.HealthResponse, error) {
	return &providerv1.HealthResponse{Healthy: true}, nil
}

func (p *testProvider) Open(context.Context) (providersdk.Connection, error) {
	return &testConnection{store: p.store}, nil
}

func (p *testProvider) TransactionRecovery() providersdk.TransactionRecoveryManager {
	return &testRecoveryManager{store: p.store}
}

type testConnection struct {
	store  *testStore
	active *providerv1.TransactionBranchId
}

func (c *testConnection) Close(context.Context) error {
	c.active = nil
	return nil
}

func (*testConnection) Begin(context.Context) error    { return nil }
func (*testConnection) Commit(context.Context) error   { return nil }
func (*testConnection) Rollback(context.Context) error { return nil }

func (*testConnection) InTransaction(context.Context) (bool, error) {
	return false, nil
}

func (*testConnection) Query(
	context.Context,
	*providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	return nil, fmt.Errorf("query is not used by this fixture")
}

func (*testConnection) Insert(
	context.Context,
	*providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	return nil, fmt.Errorf("insert is not used by this fixture")
}

func (c *testConnection) Update(
	context.Context,
	*providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	if c.active == nil {
		return nil, fmt.Errorf("update requires an active branch")
	}

	return &providerv1.UpdateResponse{}, nil
}

func (*testConnection) Delete(
	context.Context,
	*providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	return nil, fmt.Errorf("delete is not used by this fixture")
}

func (c *testConnection) BeginBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) error {
	if c.active != nil && !proto.Equal(c.active, branch) {
		return fmt.Errorf("another branch is active")
	}
	c.active = proto.Clone(branch).(*providerv1.TransactionBranchId)

	return nil
}

func (c *testConnection) PrepareBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionPrepareOutcome, error) {
	if c.active == nil {
		state, found := c.store.get(branch)
		if found && state == providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED {
			return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED, nil
		}
		return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_UNSPECIFIED,
			fmt.Errorf("branch is not active")
	}
	if !proto.Equal(c.active, branch) {
		return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_UNSPECIFIED,
			fmt.Errorf("branch is not active")
	}
	c.store.set(branch, providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED)
	c.active = nil

	return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED, nil
}

func (c *testConnection) CommitBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	if c.active == nil || !proto.Equal(c.active, branch) {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED,
			fmt.Errorf("branch is not active")
	}
	c.store.set(branch, providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED)
	c.active = nil

	return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED, nil
}

func (c *testConnection) RollbackBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	if c.active == nil || !proto.Equal(c.active, branch) {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED,
			fmt.Errorf("branch is not active")
	}
	c.store.set(branch, providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK)
	c.active = nil

	return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK, nil
}

type testRecoveryManager struct {
	store *testStore
}

func (*testRecoveryManager) ResourceManagerID() string {
	return testResourceManagerID
}

func (m *testRecoveryManager) CommitBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	state, found := m.store.get(branch)
	if !found {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND, nil
	}
	if state == providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED ||
		state == providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED {
		m.store.set(branch, providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED)
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED, nil
	}

	return state, nil
}

func (m *testRecoveryManager) RollbackBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	state, found := m.store.get(branch)
	if !found {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND, nil
	}
	if state == providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED ||
		state == providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK {
		m.store.set(branch, providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK)
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK, nil
	}

	return state, nil
}

func (m *testRecoveryManager) RecoverBranches(
	context.Context,
) (providersdk.TransactionRecoveryStream, error) {
	return &testRecoveryStream{branches: m.store.prepared()}, nil
}

func (m *testRecoveryManager) BranchStatus(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	state, found := m.store.get(branch)
	if !found {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND, nil
	}

	return state, nil
}

func (m *testRecoveryManager) ForgetBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) error {
	state, found := m.store.get(branch)
	if !found {
		return nil
	}
	if state == providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED {
		return fmt.Errorf("prepared branch cannot be forgotten")
	}
	m.store.remove(branch)

	return nil
}

type testRecoveryStream struct {
	branches  []*providerv1.TransactionBranch
	delivered bool
}

func (s *testRecoveryStream) Next(
	context.Context,
) ([]*providerv1.TransactionBranch, error) {
	if s.delivered || len(s.branches) == 0 {
		return nil, io.EOF
	}
	s.delivered = true

	return s.branches, nil
}

func (*testRecoveryStream) Close() error { return nil }

func (s *testStore) set(
	branch *providerv1.TransactionBranchId,
	state providerv1.TransactionBranchState,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[testBranchKey(branch)] = testRecord{
		branch: proto.Clone(branch).(*providerv1.TransactionBranchId),
		state:  state,
	}
}

func (s *testStore) get(
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, found := s.records[testBranchKey(branch)]

	return record.state, found
}

func (s *testStore) remove(branch *providerv1.TransactionBranchId) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, testBranchKey(branch))
}

func (s *testStore) prepared() []*providerv1.TransactionBranch {
	s.mu.Lock()
	defer s.mu.Unlock()
	branches := make([]*providerv1.TransactionBranch, 0, len(s.records))
	for _, record := range s.records {
		if record.state != providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED {
			continue
		}
		branches = append(branches, &providerv1.TransactionBranch{
			Branch: proto.Clone(record.branch).(*providerv1.TransactionBranchId),
			State:  record.state,
		})
	}

	return branches
}

func testBranchKey(branch *providerv1.TransactionBranchId) string {
	return branch.GetResourceManagerId() + "\x00" + string(branch.GetBranchId())
}

func TestRunDurableTransactionRecovery(t *testing.T) {
	store := &testStore{
		records: make(map[string]testRecord),
	}
	var starts atomic.Int32
	var stops atomic.Int32

	RunDurableTransactionRecovery(t, DurableTransactionFixture{
		NewProvider: func(
			context.Context,
		) (providersdk.Provider, func(context.Context) error, error) {
			starts.Add(1)
			return &testProvider{store: store}, func(context.Context) error {
				stops.Add(1)
				return nil
			}, nil
		},
		Mutate: func(
			ctx context.Context,
			server *providersdk.Server,
			connectionID string,
			_ *providerv1.TransactionBranchId,
		) error {
			_, err := server.Update(ctx, &providerv1.UpdateRequest{
				ConnectionId: connectionID,
			})
			return err
		},
	})

	if starts.Load() != 3 || stops.Load() != 3 {
		t.Fatalf(
			"provider lifecycle starts/stops = %d/%d, want 3/3",
			starts.Load(),
			stops.Load(),
		)
	}
}

var _ providersdk.TransactionRecoveryProvider = (*testProvider)(nil)
var _ providersdk.TransactionBranchConnection = (*testConnection)(nil)
var _ providersdk.TransactionRecoveryManager = (*testRecoveryManager)(nil)
