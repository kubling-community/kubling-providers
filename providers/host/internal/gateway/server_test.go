package gateway

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
	"github.com/kubling-community/kubling-providers/providers/host/internal/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestServerRegistersRenewsAndClosesAgentSession(t *testing.T) {
	harness := newGatewayHarness(t)
	stream, err := harness.client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := stream.Send(helloRequest("fleet-a", "agent-a")); err != nil {
		t.Fatalf("Send(hello) error = %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(accepted) error = %v", err)
	}
	accepted := response.GetSessionAccepted()
	if accepted == nil || accepted.GetSessionId() == "" ||
		accepted.GetGeneration() != 1 || accepted.GetNamespace() != "fleet-a" ||
		accepted.GetHostId() != "agent-a" {
		t.Fatalf("SessionAccepted = %v", accepted)
	}

	harness.clock.advance(5 * time.Second)
	if err := stream.Send(&gatewaypb.ConnectRequest{
		Payload: &gatewaypb.ConnectRequest_Heartbeat{
			Heartbeat: &gatewaypb.Heartbeat{Sequence: 1},
		},
	}); err != nil {
		t.Fatalf("Send(heartbeat) error = %v", err)
	}
	response, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv(heartbeat ack) error = %v", err)
	}
	acknowledged := response.GetHeartbeatAcknowledged()
	if acknowledged == nil || acknowledged.GetSequence() != 1 {
		t.Fatalf("HeartbeatAcknowledged = %v", acknowledged)
	}
	wantExpiry := harness.clock.now().Add(harness.config.LeaseDuration)
	if got := acknowledged.GetLeaseExpiresAt().AsTime(); !got.Equal(wantExpiry) {
		t.Fatalf("lease expiry = %v, want %v", got, wantExpiry)
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("Recv(after close) error = %v, want EOF", err)
	}
	waitFor(t, func() bool {
		_, exists := harness.directory.Resolve(accepted.GetSessionId())
		return !exists
	})

	resolved, err := harness.registry.ResolveSession(
		context.Background(),
		accepted.GetSessionId(),
	)
	if err != nil {
		t.Fatalf("ResolveSession() error = %v", err)
	}
	if resolved.Session.DisconnectedAt.IsZero() {
		t.Fatal("closed stream did not mark session disconnected")
	}
}

func TestServerSupersedesPreviousStreamForSameHost(t *testing.T) {
	harness := newGatewayHarness(t)
	first := connectAgent(t, harness, "fleet-a", "agent-a")
	harness.clock.advance(time.Second)
	second := connectAgent(t, harness, "fleet-a", "agent-a")

	if second.accepted.GetGeneration() != 2 {
		t.Fatalf("second generation = %d, want 2", second.accepted.GetGeneration())
	}
	current, exists := harness.directory.Current(first.key())
	if !exists || current.Snapshot().Session.ID != second.accepted.GetSessionId() {
		t.Fatalf("current binding = %v, %t; want second", current, exists)
	}
	resolvedFirst, err := harness.registry.ResolveSession(
		context.Background(),
		first.accepted.GetSessionId(),
	)
	if err != nil {
		t.Fatalf("ResolveSession(first) error = %v", err)
	}
	if resolvedFirst.Session.DisconnectedAt.IsZero() {
		t.Fatal("superseded registry session remains connected")
	}

	if _, err := first.stream.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("Recv(first after replacement) code = %v, want Aborted", status.Code(err))
	}
	_ = second.stream.CloseSend()
	_, _ = second.stream.Recv()
}

func TestServerExpiresSilentAgentLease(t *testing.T) {
	config := DefaultConfig()
	config.HeartbeatInterval = 10 * time.Millisecond
	config.LeaseDuration = 40 * time.Millisecond
	harness := newGatewayHarnessWithConfig(t, config)
	agent := connectAgent(t, harness, "fleet-a", "agent-a")

	if _, err := agent.stream.Recv(); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Recv(after silent lease) code = %v, want DeadlineExceeded", status.Code(err))
	}
	waitFor(t, func() bool {
		_, exists := harness.directory.Resolve(agent.accepted.GetSessionId())
		return !exists
	})
}

func TestServerIgnoresLateResponsesForCancelledScan(t *testing.T) {
	harness := newGatewayHarness(t)
	agent := connectAgent(t, harness, "fleet-a", "agent-a")
	session, exists := harness.directory.Resolve(agent.accepted.GetSessionId())
	if !exists {
		t.Fatal("accepted session is not resolvable")
	}
	scan, err := session.StartScan(&gatewaypb.ScanRequest{
		ScanId: "scan-1",
		Table:  gatewaypb.HostTable_HOST_TABLE_MEMORY,
	})
	if err != nil {
		t.Fatalf("StartScan() error = %v", err)
	}
	if response, err := agent.stream.Recv(); err != nil || response.GetScanRequest().GetScanId() != "scan-1" {
		t.Fatalf("Recv(scan request) = %v, %v", response, err)
	}
	if err := scan.Cancel(gatewaypb.CancelReason_CANCEL_REASON_CLIENT_CANCELLED); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if response, err := agent.stream.Recv(); err != nil || response.GetCancelScan().GetScanId() != "scan-1" {
		t.Fatalf("Recv(cancel scan) = %v, %v", response, err)
	}
	if err := agent.stream.Send(&gatewaypb.ConnectRequest{
		Payload: &gatewaypb.ConnectRequest_ScanBatch{ScanBatch: &gatewaypb.ScanBatch{
			ScanId: "scan-1", Rows: []*gatewaypb.ScanRow{{}},
		}},
	}); err != nil {
		t.Fatalf("Send(late batch) error = %v", err)
	}
	if err := agent.stream.Send(&gatewaypb.ConnectRequest{
		Payload: &gatewaypb.ConnectRequest_ScanFinished{ScanFinished: &gatewaypb.ScanFinished{
			ScanId: "scan-1", Status: gatewaypb.ScanStatus_SCAN_STATUS_CANCELLED,
		}},
	}); err != nil {
		t.Fatalf("Send(late completion) error = %v", err)
	}
	harness.clock.advance(time.Second)
	if err := agent.stream.Send(&gatewaypb.ConnectRequest{
		Payload: &gatewaypb.ConnectRequest_Heartbeat{
			Heartbeat: &gatewaypb.Heartbeat{Sequence: 1},
		},
	}); err != nil {
		t.Fatalf("Send(heartbeat) error = %v", err)
	}
	response, err := agent.stream.Recv()
	if err != nil || response.GetHeartbeatAcknowledged().GetSequence() != 1 {
		t.Fatalf("Recv(heartbeat ack) = %v, %v", response, err)
	}
	_ = agent.stream.CloseSend()
	_, _ = agent.stream.Recv()
}

func TestServerRejectsInvalidHandshake(t *testing.T) {
	harness := newGatewayHarness(t)
	tests := []struct {
		name    string
		request *gatewaypb.ConnectRequest
		code    codes.Code
	}{
		{
			name: "first message is not hello",
			request: &gatewaypb.ConnectRequest{
				Payload: &gatewaypb.ConnectRequest_Heartbeat{
					Heartbeat: &gatewaypb.Heartbeat{},
				},
			},
			code: codes.InvalidArgument,
		},
		{
			name: "incompatible protocol",
			request: helloRequestWith(
				"fleet-a",
				"agent-a",
				&gatewaypb.ProtocolVersion{Major: 2},
				CurrentSchemaVersion,
				1,
			),
			code: codes.FailedPrecondition,
		},
		{
			name: "incompatible schema",
			request: helloRequestWith(
				"fleet-a",
				"agent-a",
				gatewayProtocolVersion(currentProtocolVersion),
				"other",
				1,
			),
			code: codes.FailedPrecondition,
		},
		{
			name: "zero concurrency",
			request: helloRequestWith(
				"fleet-a",
				"agent-a",
				gatewayProtocolVersion(currentProtocolVersion),
				CurrentSchemaVersion,
				0,
			),
			code: codes.InvalidArgument,
		},
		{
			name:    "unauthorized namespace",
			request: helloRequest("fleet-b", "agent-a"),
			code:    codes.PermissionDenied,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream, err := harness.client.Connect(context.Background())
			if err != nil {
				t.Fatalf("Connect() error = %v", err)
			}
			if err := stream.Send(test.request); err != nil {
				t.Fatalf("Send() error = %v", err)
			}
			if _, err := stream.Recv(); status.Code(err) != test.code {
				t.Fatalf("Recv() code = %v, want %v", status.Code(err), test.code)
			}
		})
	}
}

type gatewayHarness struct {
	client    gatewaypb.AgentGatewayServiceClient
	registry  *registry.Memory
	directory *Directory
	clock     *testClock
	config    Config
}

func newGatewayHarness(t *testing.T) *gatewayHarness {
	t.Helper()
	return newGatewayHarnessWithConfig(t, DefaultConfig())
}

func newGatewayHarnessWithConfig(t *testing.T, config Config) *gatewayHarness {
	t.Helper()
	clock := &testClock{
		value: time.Date(2026, time.October, 8, 8, 0, 0, 0, time.UTC),
	}
	registry := registry.NewMemory()
	directory := NewDirectory()
	server, err := newServer(
		config,
		registry,
		directory,
		AuthorizerFunc(func(_ context.Context, requested string) (string, error) {
			if requested != "fleet-a" {
				return "", status.Error(codes.PermissionDenied, "namespace is not authorized")
			}
			return requested, nil
		}),
		clock.now,
	)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	gatewaypb.RegisterAgentGatewayServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	connection, err := grpc.NewClient(
		"passthrough:///host-agent-gateway-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
	})
	return &gatewayHarness{
		client:    gatewaypb.NewAgentGatewayServiceClient(connection),
		registry:  registry,
		directory: directory,
		clock:     clock,
		config:    config,
	}
}

type connectedAgent struct {
	stream   gatewaypb.AgentGatewayService_ConnectClient
	accepted *gatewaypb.SessionAccepted
}

func connectAgent(
	t *testing.T,
	harness *gatewayHarness,
	namespace string,
	agentID string,
) connectedAgent {
	t.Helper()
	stream, err := harness.client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := stream.Send(helloRequest(namespace, agentID)); err != nil {
		t.Fatalf("Send(hello) error = %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(accepted) error = %v", err)
	}
	if response.GetSessionAccepted() == nil {
		t.Fatalf("response = %v, want SessionAccepted", response)
	}
	return connectedAgent{stream: stream, accepted: response.GetSessionAccepted()}
}

func (a connectedAgent) key() model.HostKey {
	return model.HostKey{
		Namespace: a.accepted.GetNamespace(),
		ID:        a.accepted.GetHostId(),
	}
}

func helloRequest(namespace string, agentID string) *gatewaypb.ConnectRequest {
	return helloRequestWith(
		namespace,
		agentID,
		gatewayProtocolVersion(currentProtocolVersion),
		CurrentSchemaVersion,
		1,
	)
}

func helloRequestWith(
	namespace string,
	agentID string,
	version *gatewaypb.ProtocolVersion,
	schemaVersion string,
	maxConcurrentScans uint32,
) *gatewaypb.ConnectRequest {
	return &gatewaypb.ConnectRequest{
		Payload: &gatewaypb.ConnectRequest_Hello{
			Hello: &gatewaypb.AgentHello{
				AgentId:            agentID,
				RequestedNamespace: namespace,
				Hostname:           agentID + ".example",
				AgentVersion:       "test",
				ProtocolVersion:    version,
				SchemaVersion:      schemaVersion,
				MaxConcurrentScans: maxConcurrentScans,
			},
		},
	}
}

type testClock struct {
	mu    sync.Mutex
	value time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

func (c *testClock) advance(duration time.Duration) {
	c.mu.Lock()
	c.value = c.value.Add(duration)
	c.mu.Unlock()
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
