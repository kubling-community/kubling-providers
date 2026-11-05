package provider

import (
	"bytes"
	"context"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestTransactionBranchIDPreservesOpaqueBytes(t *testing.T) {
	branchID := make([]byte, maxTransactionBranchIDBytes)
	for index := range branchID {
		branchID[index] = byte(index)
	}
	want := &providerv1.TransactionBranchId{
		ResourceManagerId: "warehouse-primary",
		BranchId:          branchID,
	}

	payload, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("marshal branch ID: %v", err)
	}

	got := &providerv1.TransactionBranchId{}
	if err := proto.Unmarshal(payload, got); err != nil {
		t.Fatalf("unmarshal branch ID: %v", err)
	}

	if got.GetResourceManagerId() != want.GetResourceManagerId() {
		t.Fatalf(
			"resource manager ID = %q, want %q",
			got.GetResourceManagerId(),
			want.GetResourceManagerId(),
		)
	}
	if !bytes.Equal(got.GetBranchId(), want.GetBranchId()) {
		t.Fatalf("branch ID = %x, want %x", got.GetBranchId(), want.GetBranchId())
	}

	validated, err := validateTransactionBranchID(got, want.GetResourceManagerId())
	if err != nil {
		t.Fatalf("validate maximum-length branch ID: %v", err)
	}
	if !bytes.Equal(validated.GetBranchId(), want.GetBranchId()) {
		t.Fatalf("validated branch ID = %x, want %x", validated.GetBranchId(), want.GetBranchId())
	}
}

func TestProviderWithoutRecoveryKeepsNewRPCsUnimplemented(t *testing.T) {
	server := NewServer(&serverTestProvider{})
	ctx := context.Background()
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "BeginTransactionBranch",
			call: func() error {
				_, err := server.BeginTransactionBranch(
					ctx,
					&providerv1.BeginTransactionBranchRequest{},
				)
				return err
			},
		},
		{
			name: "PrepareTransactionBranch",
			call: func() error {
				_, err := server.PrepareTransactionBranch(
					ctx,
					&providerv1.PrepareTransactionBranchRequest{},
				)
				return err
			},
		},
		{
			name: "CommitTransactionBranch",
			call: func() error {
				_, err := server.CommitTransactionBranch(
					ctx,
					&providerv1.CommitTransactionBranchRequest{},
				)
				return err
			},
		},
		{
			name: "RollbackTransactionBranch",
			call: func() error {
				_, err := server.RollbackTransactionBranch(
					ctx,
					&providerv1.RollbackTransactionBranchRequest{},
				)
				return err
			},
		},
		{
			name: "RecoverTransactionBranches",
			call: func() error {
				return server.RecoverTransactionBranches(
					&providerv1.RecoverTransactionBranchesRequest{},
					&recoveryTestServerStream{ctx: ctx},
				)
			},
		},
		{
			name: "GetTransactionBranchStatus",
			call: func() error {
				_, err := server.GetTransactionBranchStatus(
					ctx,
					&providerv1.GetTransactionBranchStatusRequest{},
				)
				return err
			},
		},
		{
			name: "ForgetTransactionBranch",
			call: func() error {
				_, err := server.ForgetTransactionBranch(
					ctx,
					&providerv1.ForgetTransactionBranchRequest{},
				)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); status.Code(err) != codes.Unimplemented {
				t.Fatalf("error = %v, want UNIMPLEMENTED", err)
			}
		})
	}
}
