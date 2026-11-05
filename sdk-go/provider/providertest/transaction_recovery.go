// Package providertest contains reusable conformance checks for provider
// implementations.
package providertest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
)

// DurableTransactionFixture creates consecutive provider instances backed by
// the same durable resource manager.
//
// NewProvider must return a new provider instance on every call. The returned
// stop function releases provider-level resources after all logical
// connections have been closed. Mutate must perform a real write on the open
// logical connection so preparing the branch cannot resolve as read-only.
type DurableTransactionFixture struct {
	NewProvider func(
		context.Context,
	) (providersdk.Provider, func(context.Context) error, error)
	Mutate func(
		context.Context,
		*providersdk.Server,
		string,
		*providerv1.TransactionBranchId,
	) error
}

// RunDurableTransactionRecovery verifies the lifecycle required from a
// provider that advertises durable transaction recovery.
//
// The check prepares two branches, stops the original provider, discovers and
// resolves the branches from a new instance, restarts again, and verifies that
// terminal outcomes remain queryable until explicitly forgotten.
func RunDurableTransactionRecovery(
	t *testing.T,
	fixture DurableTransactionFixture,
) {
	t.Helper()
	if fixture.NewProvider == nil {
		t.Fatal("durable transaction fixture requires NewProvider")
	}
	if fixture.Mutate == nil {
		t.Fatal("durable transaction fixture requires Mutate")
	}

	ctx := context.Background()
	first := startProvider(t, ctx, fixture)
	defer first.cleanup(t, ctx)

	resourceManagerID := recoveryResourceManagerID(t, ctx, first.server)
	commitBranch := newBranchID(t, resourceManagerID, 0x01)
	rollbackBranch := newBranchID(t, resourceManagerID, 0x02)
	prepareBranch(t, ctx, first.server, fixture, commitBranch)
	prepareBranch(t, ctx, first.server, fixture, rollbackBranch)
	first.closeAndRequire(t, ctx)

	second := startProvider(t, ctx, fixture)
	defer second.cleanup(t, ctx)
	if got := recoveryResourceManagerID(t, ctx, second.server); got != resourceManagerID {
		t.Fatalf(
			"resource manager ID after restart = %q, want %q",
			got,
			resourceManagerID,
		)
	}
	assertRecoveredState(
		t,
		ctx,
		second.server,
		resourceManagerID,
		commitBranch,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED,
	)
	assertRecoveredState(
		t,
		ctx,
		second.server,
		resourceManagerID,
		rollbackBranch,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED,
	)
	if _, err := second.server.ForgetTransactionBranch(
		ctx,
		&providerv1.ForgetTransactionBranchRequest{Branch: commitBranch},
	); err == nil {
		t.Fatal("forget prepared branch succeeded")
	}
	assertBranchStatus(
		t,
		ctx,
		second.server,
		commitBranch,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED,
	)

	for attempt := 0; attempt < 2; attempt++ {
		response, err := second.server.CommitTransactionBranch(
			ctx,
			&providerv1.CommitTransactionBranchRequest{Branch: commitBranch},
		)
		if err != nil {
			t.Fatalf("commit prepared branch attempt %d: %v", attempt+1, err)
		}
		if response.GetState() !=
			providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED {
			t.Fatalf("commit state = %s, want COMMITTED", response.GetState())
		}

		rollbackResponse, err := second.server.RollbackTransactionBranch(
			ctx,
			&providerv1.RollbackTransactionBranchRequest{Branch: rollbackBranch},
		)
		if err != nil {
			t.Fatalf("rollback prepared branch attempt %d: %v", attempt+1, err)
		}
		if rollbackResponse.GetState() !=
			providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK {
			t.Fatalf(
				"rollback state = %s, want ROLLED_BACK",
				rollbackResponse.GetState(),
			)
		}
	}
	conflictingRollback, err := second.server.RollbackTransactionBranch(
		ctx,
		&providerv1.RollbackTransactionBranchRequest{Branch: commitBranch},
	)
	if err != nil {
		t.Fatalf("conflicting rollback after commit: %v", err)
	}
	if conflictingRollback.GetState() !=
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED {
		t.Fatalf(
			"conflicting rollback state = %s, want COMMITTED",
			conflictingRollback.GetState(),
		)
	}
	conflictingCommit, err := second.server.CommitTransactionBranch(
		ctx,
		&providerv1.CommitTransactionBranchRequest{Branch: rollbackBranch},
	)
	if err != nil {
		t.Fatalf("conflicting commit after rollback: %v", err)
	}
	if conflictingCommit.GetState() !=
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK {
		t.Fatalf(
			"conflicting commit state = %s, want ROLLED_BACK",
			conflictingCommit.GetState(),
		)
	}
	second.closeAndRequire(t, ctx)

	third := startProvider(t, ctx, fixture)
	defer third.cleanup(t, ctx)
	if got := recoveryResourceManagerID(t, ctx, third.server); got != resourceManagerID {
		t.Fatalf(
			"resource manager ID after second restart = %q, want %q",
			got,
			resourceManagerID,
		)
	}
	assertBranchStatus(
		t,
		ctx,
		third.server,
		commitBranch,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED,
	)
	assertBranchStatus(
		t,
		ctx,
		third.server,
		rollbackBranch,
		providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ROLLED_BACK,
	)
	assertNotRecovered(t, ctx, third.server, resourceManagerID, commitBranch)
	assertNotRecovered(t, ctx, third.server, resourceManagerID, rollbackBranch)

	for _, branch := range []*providerv1.TransactionBranchId{
		commitBranch,
		rollbackBranch,
	} {
		for attempt := 0; attempt < 2; attempt++ {
			if _, err := third.server.ForgetTransactionBranch(
				ctx,
				&providerv1.ForgetTransactionBranchRequest{Branch: branch},
			); err != nil {
				t.Fatalf(
					"forget branch %x attempt %d: %v",
					branch.GetBranchId(),
					attempt+1,
					err,
				)
			}
		}
		assertBranchStatus(
			t,
			ctx,
			third.server,
			branch,
			providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND,
		)
	}
}

type runningProvider struct {
	server *providersdk.Server
	stop   func(context.Context) error
	closed bool
}

func startProvider(
	t *testing.T,
	ctx context.Context,
	fixture DurableTransactionFixture,
) *runningProvider {
	t.Helper()
	implementation, stop, err := fixture.NewProvider(ctx)
	if err != nil {
		t.Fatalf("start provider: %v", err)
	}
	if implementation == nil {
		t.Fatal("NewProvider returned nil")
	}
	if stop == nil {
		stop = func(context.Context) error { return nil }
	}

	return &runningProvider{
		server: providersdk.NewServer(implementation),
		stop:   stop,
	}
}

func (p *runningProvider) close(ctx context.Context) error {
	if p.closed {
		return nil
	}
	p.closed = true

	return errors.Join(p.server.Close(ctx), p.stop(ctx))
}

func (p *runningProvider) closeAndRequire(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := p.close(ctx); err != nil {
		t.Fatalf("stop provider: %v", err)
	}
}

func (p *runningProvider) cleanup(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := p.close(ctx); err != nil {
		t.Errorf("clean up provider: %v", err)
	}
}

func recoveryResourceManagerID(
	t *testing.T,
	ctx context.Context,
	server *providersdk.Server,
) string {
	t.Helper()
	capabilities, err := server.GetCapabilities(
		ctx,
		&providerv1.GetCapabilitiesRequest{},
	)
	if err != nil {
		t.Fatalf("get provider capabilities: %v", err)
	}
	recovery := capabilities.GetTransactions().GetRecovery()
	if !capabilities.GetTransactions().GetSupported() || !recovery.GetSupported() {
		t.Fatal("provider does not advertise durable transaction recovery")
	}

	return recovery.GetResourceManagerId()
}

func newBranchID(
	t *testing.T,
	resourceManagerID string,
	marker byte,
) *providerv1.TransactionBranchId {
	t.Helper()
	branchID := make([]byte, 32)
	branchID[0] = 0x00
	branchID[1] = 0xff
	branchID[2] = 0x80
	branchID[3] = marker
	if _, err := rand.Read(branchID[4:]); err != nil {
		t.Fatalf("generate transaction branch ID: %v", err)
	}

	return &providerv1.TransactionBranchId{
		ResourceManagerId: resourceManagerID,
		BranchId:          branchID,
	}
}

func prepareBranch(
	t *testing.T,
	ctx context.Context,
	server *providersdk.Server,
	fixture DurableTransactionFixture,
	branch *providerv1.TransactionBranchId,
) {
	t.Helper()
	opened, err := server.OpenConnection(ctx, &providerv1.OpenConnectionRequest{})
	if err != nil {
		t.Fatalf("open branch connection: %v", err)
	}
	connectionID := opened.GetConnectionId()
	if _, err := server.BeginTransactionBranch(
		ctx,
		&providerv1.BeginTransactionBranchRequest{
			ConnectionId: connectionID,
			Branch:       branch,
		},
	); err != nil {
		t.Fatalf("begin branch: %v", err)
	}
	if _, err := server.BeginTransactionBranch(
		ctx,
		&providerv1.BeginTransactionBranchRequest{
			ConnectionId: connectionID,
			Branch:       branch,
		},
	); err != nil {
		t.Fatalf("repeat begin branch: %v", err)
	}
	if err := fixture.Mutate(ctx, server, connectionID, branch); err != nil {
		t.Fatalf("mutate branch: %v", err)
	}
	prepared, err := server.PrepareTransactionBranch(
		ctx,
		&providerv1.PrepareTransactionBranchRequest{
			ConnectionId: connectionID,
			Branch:       branch,
		},
	)
	if err != nil {
		t.Fatalf("prepare branch: %v", err)
	}
	if prepared.GetOutcome() !=
		providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED {
		t.Fatalf("prepare outcome = %s, want PREPARED", prepared.GetOutcome())
	}
	prepared, err = server.PrepareTransactionBranch(
		ctx,
		&providerv1.PrepareTransactionBranchRequest{
			ConnectionId: connectionID,
			Branch:       branch,
		},
	)
	if err != nil {
		t.Fatalf("repeat prepare branch: %v", err)
	}
	if prepared.GetOutcome() !=
		providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED {
		t.Fatalf(
			"repeated prepare outcome = %s, want PREPARED",
			prepared.GetOutcome(),
		)
	}
	if _, err := server.CloseConnection(
		ctx,
		&providerv1.CloseConnectionRequest{ConnectionId: connectionID},
	); err != nil {
		t.Fatalf("close prepared branch connection: %v", err)
	}
}

type recoveryCapture struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*providerv1.RecoverTransactionBranchesResponse
}

func (s *recoveryCapture) Context() context.Context {
	return s.ctx
}

func (s *recoveryCapture) Send(
	response *providerv1.RecoverTransactionBranchesResponse,
) error {
	s.responses = append(s.responses, response)
	return nil
}

func recoverBranches(
	t *testing.T,
	ctx context.Context,
	server *providersdk.Server,
	resourceManagerID string,
) []*providerv1.TransactionBranch {
	t.Helper()
	stream := &recoveryCapture{ctx: ctx}
	if err := server.RecoverTransactionBranches(
		&providerv1.RecoverTransactionBranchesRequest{
			ResourceManagerId: resourceManagerID,
		},
		stream,
	); err != nil {
		t.Fatalf("recover transaction branches: %v", err)
	}

	var branches []*providerv1.TransactionBranch
	for _, response := range stream.responses {
		branches = append(branches, response.GetBranches()...)
	}

	return branches
}

func assertRecoveredState(
	t *testing.T,
	ctx context.Context,
	server *providersdk.Server,
	resourceManagerID string,
	wantBranch *providerv1.TransactionBranchId,
	wantState providerv1.TransactionBranchState,
) {
	t.Helper()
	for _, branch := range recoverBranches(t, ctx, server, resourceManagerID) {
		if transactionBranchKey(branch.GetBranch()) == transactionBranchKey(wantBranch) {
			if branch.GetState() != wantState {
				t.Fatalf(
					"recovered branch state = %s, want %s",
					branch.GetState(),
					wantState,
				)
			}
			return
		}
	}
	t.Fatalf("prepared branch %x was not recovered", wantBranch.GetBranchId())
}

func assertNotRecovered(
	t *testing.T,
	ctx context.Context,
	server *providersdk.Server,
	resourceManagerID string,
	wantAbsent *providerv1.TransactionBranchId,
) {
	t.Helper()
	for _, branch := range recoverBranches(t, ctx, server, resourceManagerID) {
		if transactionBranchKey(branch.GetBranch()) == transactionBranchKey(wantAbsent) {
			t.Fatalf(
				"terminal branch %x was returned by recovery",
				wantAbsent.GetBranchId(),
			)
		}
	}
}

func assertBranchStatus(
	t *testing.T,
	ctx context.Context,
	server *providersdk.Server,
	branch *providerv1.TransactionBranchId,
	want providerv1.TransactionBranchState,
) {
	t.Helper()
	response, err := server.GetTransactionBranchStatus(
		ctx,
		&providerv1.GetTransactionBranchStatusRequest{Branch: branch},
	)
	if err != nil {
		t.Fatalf("get branch %x status: %v", branch.GetBranchId(), err)
	}
	if response.GetState() != want {
		t.Fatalf(
			"branch %x state = %s, want %s",
			branch.GetBranchId(),
			response.GetState(),
			want,
		)
	}
}

func transactionBranchKey(branch *providerv1.TransactionBranchId) string {
	if branch == nil {
		return ""
	}

	return fmt.Sprintf(
		"%s\x00%x",
		branch.GetResourceManagerId(),
		branch.GetBranchId(),
	)
}
