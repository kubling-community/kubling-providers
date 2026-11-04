package provider

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type durableTestRecord struct {
	ResourceManagerID string                            `json:"resource_manager_id"`
	BranchID          []byte                            `json:"branch_id"`
	State             providerv1.TransactionBranchState `json:"state"`
}

type durableTestManager struct {
	resourceManagerID string
	path              string
	mu                sync.Mutex
}

func newDurableTestManager(
	path string,
	resourceManagerID string,
) *durableTestManager {
	return &durableTestManager{
		resourceManagerID: resourceManagerID,
		path:              path,
	}
}

func (m *durableTestManager) ResourceManagerID() string {
	return m.resourceManagerID
}

func (m *durableTestManager) CommitBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	state, found, err := m.readState(branch)
	if err != nil {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED, err
	}
	if !found {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND, nil
	}
	switch state {
	case providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED:
		if err := m.writeState(
			branch,
			providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED,
		); err != nil {
			return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED, err
		}
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED, nil
	default:
		return state, nil
	}
}

func (m *durableTestManager) RollbackBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	state, found, err := m.readState(branch)
	if err != nil {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED, err
	}
	if !found {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND, nil
	}
	switch state {
	case providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK:
		if err := m.writeState(
			branch,
			providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK,
		); err != nil {
			return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED, err
		}
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK, nil
	default:
		return state, nil
	}
}

func (m *durableTestManager) RecoverBranches(
	context.Context,
) (TransactionRecoveryStream, error) {
	records, err := m.readRecords()
	if err != nil {
		return nil, err
	}
	branches := make([]*providerv1.TransactionBranch, 0, len(records))
	for _, record := range records {
		if !validRecoveryState(record.State) {
			continue
		}
		branches = append(branches, &providerv1.TransactionBranch{
			Branch: &providerv1.TransactionBranchId{
				ResourceManagerId: record.ResourceManagerID,
				BranchId:          append([]byte(nil), record.BranchID...),
			},
			State: record.State,
		})
	}
	sort.Slice(branches, func(left, right int) bool {
		return transactionBranchKey(branches[left].GetBranch()) <
			transactionBranchKey(branches[right].GetBranch())
	})

	return &durableTestRecoveryStream{branches: branches}, nil
}

func (m *durableTestManager) BranchStatus(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	state, found, err := m.readState(branch)
	if err != nil {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED, err
	}
	if !found {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND, nil
	}

	return state, nil
}

func (m *durableTestManager) ForgetBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	records, err := m.readRecordsLocked()
	if err != nil {
		return err
	}
	key := durableTestBranchKey(branch)
	record, found := records[key]
	if !found {
		return nil
	}
	if record.State == providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED {
		return status.Error(codes.FailedPrecondition, "prepared branch cannot be forgotten")
	}
	delete(records, key)

	return m.writeRecordsLocked(records)
}

func (m *durableTestManager) readState(
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, bool, error) {
	records, err := m.readRecords()
	if err != nil {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED, false, err
	}
	record, found := records[durableTestBranchKey(branch)]

	return record.State, found, nil
}

func (m *durableTestManager) writeState(
	branch *providerv1.TransactionBranchId,
	state providerv1.TransactionBranchState,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	records, err := m.readRecordsLocked()
	if err != nil {
		return err
	}
	records[durableTestBranchKey(branch)] = durableTestRecord{
		ResourceManagerID: branch.GetResourceManagerId(),
		BranchID:          append([]byte(nil), branch.GetBranchId()...),
		State:             state,
	}

	return m.writeRecordsLocked(records)
}

func (m *durableTestManager) readRecords() (
	map[string]durableTestRecord,
	error,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.readRecordsLocked()
}

func (m *durableTestManager) readRecordsLocked() (
	map[string]durableTestRecord,
	error,
) {
	payload, err := os.ReadFile(m.path)
	if errors.Is(err, os.ErrNotExist) {
		return make(map[string]durableTestRecord), nil
	}
	if err != nil {
		return nil, err
	}

	records := make(map[string]durableTestRecord)
	if err := json.Unmarshal(payload, &records); err != nil {
		return nil, err
	}

	return records, nil
}

func (m *durableTestManager) writeRecordsLocked(
	records map[string]durableTestRecord,
) error {
	payload, err := json.Marshal(records)
	if err != nil {
		return err
	}
	temporary := m.path + ".tmp"
	if err := os.WriteFile(temporary, payload, 0o600); err != nil {
		return err
	}

	return os.Rename(temporary, m.path)
}

func durableTestBranchKey(branch *providerv1.TransactionBranchId) string {
	return branch.GetResourceManagerId() + ":" +
		hex.EncodeToString(branch.GetBranchId())
}

type durableTestRecoveryStream struct {
	branches  []*providerv1.TransactionBranch
	delivered bool
	closed    bool
}

func (s *durableTestRecoveryStream) Next(
	context.Context,
) ([]*providerv1.TransactionBranch, error) {
	if s.delivered || len(s.branches) == 0 {
		return nil, io.EOF
	}
	s.delivered = true

	return s.branches, nil
}

func (s *durableTestRecoveryStream) Close() error {
	s.closed = true
	return nil
}

type durableTestConnection struct {
	Connection

	manager *durableTestManager
	active  *providerv1.TransactionBranchId
}

func (c *durableTestConnection) Close(context.Context) error {
	c.active = nil
	return nil
}

func (c *durableTestConnection) BeginBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) error {
	if c.active != nil && !proto.Equal(c.active, branch) {
		return status.Error(codes.FailedPrecondition, "another branch is active")
	}
	c.active = proto.Clone(branch).(*providerv1.TransactionBranchId)

	return nil
}

func (c *durableTestConnection) PrepareBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionPrepareOutcome, error) {
	if c.active == nil {
		state, found, err := c.manager.readState(branch)
		if err != nil {
			return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_UNSPECIFIED, err
		}
		if found && state == providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED {
			return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED, nil
		}
		return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_UNSPECIFIED,
			status.Error(codes.FailedPrecondition, "branch is not active")
	}
	if !proto.Equal(c.active, branch) {
		return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_UNSPECIFIED,
			status.Error(codes.FailedPrecondition, "branch is not active")
	}
	if err := c.manager.writeState(
		branch,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED,
	); err != nil {
		return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_UNSPECIFIED, err
	}
	c.active = nil

	return providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED, nil
}

func (c *durableTestConnection) CommitBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	if c.active == nil || !proto.Equal(c.active, branch) {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED,
			status.Error(codes.FailedPrecondition, "branch is not active")
	}
	if err := c.manager.writeState(
		branch,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED,
	); err != nil {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED, err
	}
	c.active = nil

	return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED, nil
}

func (c *durableTestConnection) RollbackBranch(
	_ context.Context,
	branch *providerv1.TransactionBranchId,
) (providerv1.TransactionBranchState, error) {
	if c.active == nil || !proto.Equal(c.active, branch) {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED,
			status.Error(codes.FailedPrecondition, "branch is not active")
	}
	if err := c.manager.writeState(
		branch,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK,
	); err != nil {
		return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_UNSPECIFIED, err
	}
	c.active = nil

	return providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK, nil
}

type durableTestProvider struct {
	manager      *durableTestManager
	advertisedID string
}

func (p *durableTestProvider) Capabilities(
	context.Context,
) (*Capabilities, error) {
	return &Capabilities{
		Transactions: &providerv1.TransactionCapabilities{
			Supported: true,
			Recovery: &providerv1.TransactionRecoveryCapabilities{
				Supported:         true,
				ResourceManagerId: p.advertisedID,
			},
		},
	}, nil
}

func (*durableTestProvider) Health(
	context.Context,
) (*providerv1.HealthResponse, error) {
	return &providerv1.HealthResponse{Healthy: true}, nil
}

func (p *durableTestProvider) Open(context.Context) (Connection, error) {
	return &durableTestConnection{manager: p.manager}, nil
}

func (p *durableTestProvider) TransactionRecovery() TransactionRecoveryManager {
	if p.manager == nil {
		return nil
	}

	return p.manager
}

type recoveryTestServerStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*providerv1.RecoverTransactionBranchesResponse
}

func (s *recoveryTestServerStream) Context() context.Context {
	return s.ctx
}

func (s *recoveryTestServerStream) Send(
	response *providerv1.RecoverTransactionBranchesResponse,
) error {
	s.responses = append(s.responses, response)
	return nil
}

func TestDurableTransactionBranchSurvivesServerRestart(t *testing.T) {
	const resourceManagerID = "warehouse-primary"
	storePath := filepath.Join(t.TempDir(), "branches.json")
	branchID := make([]byte, maxTransactionBranchIDBytes)
	for index := range branchID {
		branchID[index] = byte(index)
	}
	branch := &providerv1.TransactionBranchId{
		ResourceManagerId: resourceManagerID,
		BranchId:          branchID,
	}

	first := NewServer(&durableTestProvider{
		manager:      newDurableTestManager(storePath, resourceManagerID),
		advertisedID: resourceManagerID,
	})
	if _, err := first.GetCapabilities(
		context.Background(),
		&providerv1.GetCapabilitiesRequest{},
	); err != nil {
		t.Fatalf("GetCapabilities() error = %v", err)
	}
	opened, err := first.OpenConnection(
		context.Background(),
		&providerv1.OpenConnectionRequest{},
	)
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	if response, err := first.BeginTransactionBranch(
		context.Background(),
		&providerv1.BeginTransactionBranchRequest{
			ConnectionId: opened.GetConnectionId(),
			Branch:       branch,
		},
	); err != nil {
		t.Fatalf("BeginTransactionBranch() error = %v", err)
	} else if response.GetState() !=
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ACTIVE {
		t.Fatalf("BeginTransactionBranch() state = %s", response.GetState())
	}
	if response, err := first.PrepareTransactionBranch(
		context.Background(),
		&providerv1.PrepareTransactionBranchRequest{
			ConnectionId: opened.GetConnectionId(),
			Branch:       branch,
		},
	); err != nil {
		t.Fatalf("PrepareTransactionBranch() error = %v", err)
	} else if response.GetOutcome() !=
		providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED {
		t.Fatalf("PrepareTransactionBranch() outcome = %s", response.GetOutcome())
	}
	if _, err := first.CloseConnection(
		context.Background(),
		&providerv1.CloseConnectionRequest{ConnectionId: opened.GetConnectionId()},
	); err != nil {
		t.Fatalf("CloseConnection() error = %v", err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatalf("first Server.Close() error = %v", err)
	}

	second := NewServer(&durableTestProvider{
		manager:      newDurableTestManager(storePath, resourceManagerID),
		advertisedID: resourceManagerID,
	})
	recoveryStream := &recoveryTestServerStream{ctx: context.Background()}
	if err := second.RecoverTransactionBranches(
		&providerv1.RecoverTransactionBranchesRequest{
			ResourceManagerId: resourceManagerID,
		},
		recoveryStream,
	); err != nil {
		t.Fatalf("RecoverTransactionBranches() error = %v", err)
	}
	if len(recoveryStream.responses) != 1 ||
		len(recoveryStream.responses[0].GetBranches()) != 1 {
		t.Fatalf("recovery responses = %v, want one branch", recoveryStream.responses)
	}
	recovered := recoveryStream.responses[0].GetBranches()[0]
	if !proto.Equal(recovered.GetBranch(), branch) || recovered.GetState() !=
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED {
		t.Fatalf("recovered branch = %v, want PREPARED %v", recovered, branch)
	}

	commitRequest := &providerv1.CommitTransactionBranchRequest{Branch: branch}
	for attempt := 0; attempt < 2; attempt++ {
		response, err := second.CommitTransactionBranch(
			context.Background(),
			commitRequest,
		)
		if err != nil {
			t.Fatalf("CommitTransactionBranch() attempt %d error = %v", attempt, err)
		}
		if response.GetState() !=
			providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED {
			t.Fatalf("CommitTransactionBranch() state = %s", response.GetState())
		}
	}

	third := NewServer(&durableTestProvider{
		manager:      newDurableTestManager(storePath, resourceManagerID),
		advertisedID: resourceManagerID,
	})
	statusResponse, err := third.GetTransactionBranchStatus(
		context.Background(),
		&providerv1.GetTransactionBranchStatusRequest{Branch: branch},
	)
	if err != nil {
		t.Fatalf("GetTransactionBranchStatus() error = %v", err)
	}
	if statusResponse.GetState() !=
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED {
		t.Fatalf("GetTransactionBranchStatus() state = %s", statusResponse.GetState())
	}

	afterCommit := &recoveryTestServerStream{ctx: context.Background()}
	if err := third.RecoverTransactionBranches(
		&providerv1.RecoverTransactionBranchesRequest{
			ResourceManagerId: resourceManagerID,
		},
		afterCommit,
	); err != nil {
		t.Fatalf("RecoverTransactionBranches() after commit error = %v", err)
	}
	if len(afterCommit.responses) != 0 {
		t.Fatalf("recovery returned committed branch: %v", afterCommit.responses)
	}

	if _, err := third.ForgetTransactionBranch(
		context.Background(),
		&providerv1.ForgetTransactionBranchRequest{Branch: branch},
	); err != nil {
		t.Fatalf("ForgetTransactionBranch() error = %v", err)
	}
	statusResponse, err = third.GetTransactionBranchStatus(
		context.Background(),
		&providerv1.GetTransactionBranchStatusRequest{Branch: branch},
	)
	if err != nil {
		t.Fatalf("GetTransactionBranchStatus() after forget error = %v", err)
	}
	if statusResponse.GetState() !=
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND {
		t.Fatalf("state after forget = %s, want NOT_FOUND", statusResponse.GetState())
	}
}

func TestDurableTransactionBranchesRemainIsolated(t *testing.T) {
	const resourceManagerID = "warehouse-primary"
	storePath := filepath.Join(t.TempDir(), "branches.json")
	server := NewServer(&durableTestProvider{
		manager:      newDurableTestManager(storePath, resourceManagerID),
		advertisedID: resourceManagerID,
	})
	first := &providerv1.TransactionBranchId{
		ResourceManagerId: resourceManagerID,
		BranchId:          []byte("branch-1"),
	}
	second := &providerv1.TransactionBranchId{
		ResourceManagerId: resourceManagerID,
		BranchId:          []byte("branch-2"),
	}
	prepareDurableTestBranch(t, server, first)
	prepareDurableTestBranch(t, server, second)

	restarted := NewServer(&durableTestProvider{
		manager:      newDurableTestManager(storePath, resourceManagerID),
		advertisedID: resourceManagerID,
	})
	commitResponse, err := restarted.CommitTransactionBranch(
		context.Background(),
		&providerv1.CommitTransactionBranchRequest{Branch: first},
	)
	if err != nil {
		t.Fatalf("CommitTransactionBranch(first) error = %v", err)
	}
	if commitResponse.GetState() !=
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED {
		t.Fatalf("first state = %s, want COMMITTED", commitResponse.GetState())
	}
	secondStatus, err := restarted.GetTransactionBranchStatus(
		context.Background(),
		&providerv1.GetTransactionBranchStatusRequest{Branch: second},
	)
	if err != nil {
		t.Fatalf("GetTransactionBranchStatus(second) error = %v", err)
	}
	if secondStatus.GetState() !=
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED {
		t.Fatalf("second state = %s, want PREPARED", secondStatus.GetState())
	}

	for attempt := 0; attempt < 2; attempt++ {
		rollbackResponse, err := restarted.RollbackTransactionBranch(
			context.Background(),
			&providerv1.RollbackTransactionBranchRequest{Branch: second},
		)
		if err != nil {
			t.Fatalf("RollbackTransactionBranch(second) attempt %d error = %v", attempt, err)
		}
		if rollbackResponse.GetState() !=
			providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK {
			t.Fatalf("second rollback state = %s", rollbackResponse.GetState())
		}
	}

	recoveryStream := &recoveryTestServerStream{ctx: context.Background()}
	if err := restarted.RecoverTransactionBranches(
		&providerv1.RecoverTransactionBranchesRequest{
			ResourceManagerId: resourceManagerID,
		},
		recoveryStream,
	); err != nil {
		t.Fatalf("RecoverTransactionBranches() error = %v", err)
	}
	if len(recoveryStream.responses) != 0 {
		t.Fatalf("recovery returned resolved branches: %v", recoveryStream.responses)
	}
}

func TestActiveTransactionBranchResolutionUsesConnection(t *testing.T) {
	const resourceManagerID = "warehouse-primary"
	server := NewServer(&durableTestProvider{
		manager: newDurableTestManager(
			filepath.Join(t.TempDir(), "branches.json"),
			resourceManagerID,
		),
		advertisedID: resourceManagerID,
	})

	tests := []struct {
		name      string
		branchID  string
		resolve   func(string, *providerv1.TransactionBranchId) (providerv1.TransactionBranchState, error)
		wantState providerv1.TransactionBranchState
	}{
		{
			name:     "one phase commit",
			branchID: "commit-branch",
			resolve: func(
				connectionID string,
				branch *providerv1.TransactionBranchId,
			) (providerv1.TransactionBranchState, error) {
				response, err := server.CommitTransactionBranch(
					context.Background(),
					&providerv1.CommitTransactionBranchRequest{
						Branch:       branch,
						ConnectionId: &connectionID,
						OnePhase:     true,
					},
				)
				return response.GetState(), err
			},
			wantState: providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED,
		},
		{
			name:     "active rollback",
			branchID: "rollback-branch",
			resolve: func(
				connectionID string,
				branch *providerv1.TransactionBranchId,
			) (providerv1.TransactionBranchState, error) {
				response, err := server.RollbackTransactionBranch(
					context.Background(),
					&providerv1.RollbackTransactionBranchRequest{
						Branch:       branch,
						ConnectionId: &connectionID,
					},
				)
				return response.GetState(), err
			},
			wantState: providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			branch := &providerv1.TransactionBranchId{
				ResourceManagerId: resourceManagerID,
				BranchId:          []byte(test.branchID),
			}
			opened, err := server.OpenConnection(
				context.Background(),
				&providerv1.OpenConnectionRequest{},
			)
			if err != nil {
				t.Fatalf("OpenConnection() error = %v", err)
			}
			if _, err := server.BeginTransactionBranch(
				context.Background(),
				&providerv1.BeginTransactionBranchRequest{
					ConnectionId: opened.GetConnectionId(),
					Branch:       branch,
				},
			); err != nil {
				t.Fatalf("BeginTransactionBranch() error = %v", err)
			}
			state, err := test.resolve(opened.GetConnectionId(), branch)
			if err != nil {
				t.Fatalf("resolve active branch: %v", err)
			}
			if state != test.wantState {
				t.Fatalf("resolved state = %s, want %s", state, test.wantState)
			}
		})
	}
}

func TestTransactionBranchRequestValidation(t *testing.T) {
	const resourceManagerID = "warehouse-primary"
	server := NewServer(&durableTestProvider{
		manager: newDurableTestManager(
			filepath.Join(t.TempDir(), "branches.json"),
			resourceManagerID,
		),
		advertisedID: resourceManagerID,
	})
	tests := []struct {
		name   string
		branch *providerv1.TransactionBranchId
	}{
		{name: "missing branch"},
		{
			name: "missing resource manager",
			branch: &providerv1.TransactionBranchId{
				BranchId: []byte("branch"),
			},
		},
		{
			name: "wrong resource manager",
			branch: &providerv1.TransactionBranchId{
				ResourceManagerId: "warehouse-secondary",
				BranchId:          []byte("branch"),
			},
		},
		{
			name: "missing branch ID",
			branch: &providerv1.TransactionBranchId{
				ResourceManagerId: resourceManagerID,
			},
		},
		{
			name: "oversized branch ID",
			branch: &providerv1.TransactionBranchId{
				ResourceManagerId: resourceManagerID,
				BranchId:          make([]byte, maxTransactionBranchIDBytes+1),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := server.GetTransactionBranchStatus(
				context.Background(),
				&providerv1.GetTransactionBranchStatusRequest{Branch: test.branch},
			)
			if response != nil {
				t.Fatalf("GetTransactionBranchStatus() response = %v, want nil", response)
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("GetTransactionBranchStatus() error = %v, want INVALID_ARGUMENT", err)
			}
		})
	}

	recoveryStream := &recoveryTestServerStream{ctx: context.Background()}
	err := server.RecoverTransactionBranches(
		&providerv1.RecoverTransactionBranchesRequest{
			ResourceManagerId: "warehouse-secondary",
		},
		recoveryStream,
	)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RecoverTransactionBranches() error = %v, want FAILED_PRECONDITION", err)
	}
}

func prepareDurableTestBranch(
	t *testing.T,
	server *Server,
	branch *providerv1.TransactionBranchId,
) {
	t.Helper()
	opened, err := server.OpenConnection(
		context.Background(),
		&providerv1.OpenConnectionRequest{},
	)
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	if _, err := server.BeginTransactionBranch(
		context.Background(),
		&providerv1.BeginTransactionBranchRequest{
			ConnectionId: opened.GetConnectionId(),
			Branch:       branch,
		},
	); err != nil {
		t.Fatalf("BeginTransactionBranch() error = %v", err)
	}
	if _, err := server.PrepareTransactionBranch(
		context.Background(),
		&providerv1.PrepareTransactionBranchRequest{
			ConnectionId: opened.GetConnectionId(),
			Branch:       branch,
		},
	); err != nil {
		t.Fatalf("PrepareTransactionBranch() error = %v", err)
	}
	if _, err := server.CloseConnection(
		context.Background(),
		&providerv1.CloseConnectionRequest{ConnectionId: opened.GetConnectionId()},
	); err != nil {
		t.Fatalf("CloseConnection() error = %v", err)
	}
}

func TestDurableTransactionCapabilitiesRequireMatchingManager(t *testing.T) {
	const resourceManagerID = "warehouse-primary"
	tests := []struct {
		name         string
		manager      *durableTestManager
		advertisedID string
		wantText     string
	}{
		{
			name:         "missing manager",
			advertisedID: resourceManagerID,
			wantText:     "exposes no recovery manager",
		},
		{
			name: "mismatched manager",
			manager: newDurableTestManager(
				filepath.Join(t.TempDir(), "branches.json"),
				"warehouse-secondary",
			),
			advertisedID: resourceManagerID,
			wantText:     "does not match advertised ID",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer(&durableTestProvider{
				manager:      test.manager,
				advertisedID: test.advertisedID,
			})
			response, err := server.GetCapabilities(
				context.Background(),
				&providerv1.GetCapabilitiesRequest{},
			)
			if response != nil {
				t.Fatalf("GetCapabilities() response = %v, want nil", response)
			}
			if status.Code(err) != codes.Internal ||
				!strings.Contains(err.Error(), test.wantText) {
				t.Fatalf(
					"GetCapabilities() error = %v, want INTERNAL containing %q",
					err,
					test.wantText,
				)
			}
		})
	}
}
