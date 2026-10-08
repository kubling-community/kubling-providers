package gateway

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
)

func TestDirectoryReplacementAndStaleUnbind(t *testing.T) {
	directory := NewDirectory()
	key := model.HostKey{Namespace: "fleet-a", ID: "agent-a"}
	first, err := directory.Bind(testSnapshot(key, "session-1", 1), &collectingSender{})
	if err != nil {
		t.Fatalf("Bind(first) error = %v", err)
	}
	secondSender := &collectingSender{}
	second, err := directory.Bind(testSnapshot(key, "session-2", 2), secondSender)
	if err != nil {
		t.Fatalf("Bind(second) error = %v", err)
	}

	select {
	case <-first.Done():
	default:
		t.Fatal("replaced session remains open")
	}
	if _, exists := directory.Resolve("session-1"); exists {
		t.Fatal("replaced session remains resolvable")
	}
	directory.Unbind(first)
	current, exists := directory.Current(key)
	if !exists || current != second {
		t.Fatalf("current session after stale unbind = %v, %t; want second", current, exists)
	}

	response := &gatewaypb.ConnectResponse{
		Payload: &gatewaypb.ConnectResponse_HeartbeatAcknowledged{
			HeartbeatAcknowledged: &gatewaypb.HeartbeatAcknowledged{Sequence: 1},
		},
	}
	if err := second.Send(response); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got := secondSender.count(); got != 1 {
		t.Fatalf("sent responses = %d, want 1", got)
	}

	directory.Unbind(second)
	if err := second.Send(response); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Send(closed) error = %v, want ErrSessionClosed", err)
	}
}

func TestSessionScanLifecycle(t *testing.T) {
	directory := NewDirectory()
	key := model.HostKey{Namespace: "fleet-a", ID: "agent-a"}
	sender := &collectingSender{}
	session, err := directory.Bind(testSnapshot(key, "session-1", 1), sender)
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	scan, err := session.StartScan(&gatewaypb.ScanRequest{
		ScanId: "scan-1",
		Table:  gatewaypb.HostTable_HOST_TABLE_MEMORY,
		Columns: []string{
			"total_bytes",
		},
	})
	if err != nil {
		t.Fatalf("StartScan() error = %v", err)
	}
	if got := sender.last().GetScanRequest().GetScanId(); got != "scan-1" {
		t.Fatalf("dispatched scan ID = %q, want scan-1", got)
	}
	if _, err := session.StartScan(&gatewaypb.ScanRequest{
		ScanId: "scan-2",
		Table:  gatewaypb.HostTable_HOST_TABLE_MEMORY,
	}); !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("StartScan(over limit) error = %v, want ErrSessionBusy", err)
	}

	batch := &gatewaypb.ScanBatch{
		ScanId:     "scan-1",
		BatchIndex: 0,
		Rows:       []*gatewaypb.ScanRow{{}},
	}
	if err := session.deliverBatch(context.Background(), batch); err != nil {
		t.Fatalf("deliverBatch() error = %v", err)
	}
	event, err := scan.Next(context.Background())
	if err != nil || event.Batch.GetBatchIndex() != 0 {
		t.Fatalf("Next(batch) = %v, %v", event, err)
	}
	if err := session.deliverBatch(context.Background(), batch); err == nil {
		t.Fatal("deliverBatch(duplicate) error = nil")
	}

	finished := &gatewaypb.ScanFinished{
		ScanId: "scan-1",
		Status: gatewaypb.ScanStatus_SCAN_STATUS_SUCCEEDED,
	}
	if err := session.finishScan(context.Background(), finished); err != nil {
		t.Fatalf("finishScan() error = %v", err)
	}
	event, err = scan.Next(context.Background())
	if err != nil || event.Finished.GetStatus() != gatewaypb.ScanStatus_SCAN_STATUS_SUCCEEDED {
		t.Fatalf("Next(finished) = %v, %v", event, err)
	}
	if err := session.finishScan(context.Background(), finished); !errors.Is(err, ErrScanRetired) {
		t.Fatalf("finishScan(duplicate) error = %v, want ErrScanRetired", err)
	}
}

func TestSessionCancellationRetiresScanAndDiscardsLateResults(t *testing.T) {
	directory := NewDirectory()
	key := model.HostKey{Namespace: "fleet-a", ID: "agent-a"}
	sender := &collectingSender{}
	session, err := directory.Bind(testSnapshot(key, "session-1", 1), sender)
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	scan, err := session.StartScan(&gatewaypb.ScanRequest{
		ScanId: "scan-1",
		Table:  gatewaypb.HostTable_HOST_TABLE_MEMORY,
	})
	if err != nil {
		t.Fatalf("StartScan() error = %v", err)
	}
	if err := scan.Cancel(gatewaypb.CancelReason_CANCEL_REASON_CLIENT_CANCELLED); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if _, err := scan.Next(context.Background()); !errors.Is(err, ErrScanRetired) {
		t.Fatalf("Next(cancelled) error = %v, want ErrScanRetired", err)
	}
	if _, err := session.StartScan(&gatewaypb.ScanRequest{
		ScanId: "scan-2",
		Table:  gatewaypb.HostTable_HOST_TABLE_MEMORY,
	}); err != nil {
		t.Fatalf("StartScan(after cancel) error = %v", err)
	}
	if err := session.deliverBatch(context.Background(), &gatewaypb.ScanBatch{
		ScanId: "scan-1", Rows: []*gatewaypb.ScanRow{{}},
	}); !errors.Is(err, ErrScanRetired) {
		t.Fatalf("deliverBatch(late) error = %v, want ErrScanRetired", err)
	}
	if err := session.finishScan(context.Background(), &gatewaypb.ScanFinished{
		ScanId: "scan-1", Status: gatewaypb.ScanStatus_SCAN_STATUS_CANCELLED,
	}); !errors.Is(err, ErrScanRetired) {
		t.Fatalf("finishScan(late) error = %v, want ErrScanRetired", err)
	}
}

func TestSessionRejectsOversizedBatch(t *testing.T) {
	directory := NewDirectory()
	key := model.HostKey{Namespace: "fleet-a", ID: "agent-a"}
	session, err := directory.Bind(testSnapshot(key, "session-1", 1), &collectingSender{})
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if _, err := session.StartScan(&gatewaypb.ScanRequest{
		ScanId: "scan-1",
		Table:  gatewaypb.HostTable_HOST_TABLE_MEMORY,
	}); err != nil {
		t.Fatalf("StartScan() error = %v", err)
	}
	rows := make([]*gatewaypb.ScanRow, maxScanBatchRows+1)
	for index := range rows {
		rows[index] = &gatewaypb.ScanRow{}
	}
	if err := session.deliverBatch(context.Background(), &gatewaypb.ScanBatch{
		ScanId: "scan-1",
		Rows:   rows,
	}); err == nil {
		t.Fatal("deliverBatch(too many rows) error = nil")
	}
	if err := session.deliverBatch(context.Background(), &gatewaypb.ScanBatch{
		ScanId: "scan-1",
		Rows: []*gatewaypb.ScanRow{{Values: []*kublingv1.Value{{
			Kind: &kublingv1.Value_StringValue{StringValue: strings.Repeat("x", maxScanBatchBytes)},
		}}}},
	}); err == nil {
		t.Fatal("deliverBatch(too many bytes) error = nil")
	}
}

type collectingSender struct {
	mu        sync.Mutex
	responses []*gatewaypb.ConnectResponse
}

func (s *collectingSender) Send(response *gatewaypb.ConnectResponse) error {
	s.mu.Lock()
	s.responses = append(s.responses, response)
	s.mu.Unlock()
	return nil
}

func (s *collectingSender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.responses)
}

func (s *collectingSender) last() *gatewaypb.ConnectResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.responses) == 0 {
		return nil
	}
	return s.responses[len(s.responses)-1]
}

func testSnapshot(
	key model.HostKey,
	sessionID string,
	generation uint64,
) model.HostSnapshot {
	now := time.Date(2026, time.October, 8, 8, 0, 0, 0, time.UTC)
	return model.HostSnapshot{
		Identity: model.HostIdentity{Key: key, EnrolledAt: now},
		Session: &model.AgentSession{
			ID:                 sessionID,
			Host:               key,
			Generation:         generation,
			MaxConcurrentScans: 1,
			ConnectedAt:        now,
			LastSeenAt:         now,
			ExpiresAt:          now.Add(time.Minute),
		},
	}
}
