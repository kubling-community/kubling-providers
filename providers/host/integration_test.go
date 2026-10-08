package host_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	host "github.com/kubling-community/kubling-providers/providers/host"
	"github.com/kubling-community/kubling-providers/providers/host/internal/agent"
	"github.com/kubling-community/kubling-providers/providers/host/internal/agent/collector"
	"github.com/kubling-community/kubling-providers/providers/host/internal/gateway"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/query"
	"github.com/kubling-community/kubling-providers/providers/host/internal/registry"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestHostProviderMemoryEndToEndAcrossAgents(t *testing.T) {
	harness := newEndToEndHarness(t)
	goodA := harness.startAgent(t, strings.Repeat("a", 32), "host-a", 100, nil)
	goodB := harness.startAgent(t, strings.Repeat("b", 32), "host-b", 200, nil)
	failed := harness.startAgent(t, strings.Repeat("c", 32), "host-c", 0, errors.New("sensor unavailable"))
	harness.waitForActiveHosts(t, 3)
	harness.waitForHeartbeats(t, 3)

	rows, outcome, err := harness.query(
		t,
		memoryRequest(true, nil),
	)
	if err != nil {
		t.Fatalf("partial MEMORY query error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("partial MEMORY rows = %v, want 2", rows)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i]["host_id"] < rows[j]["host_id"] })
	if rows[0]["host_id"] != strings.Repeat("a", 32) || rows[0]["hostname"] != "host-a" ||
		rows[0]["total_bytes"] != "100" || rows[1]["host_id"] != strings.Repeat("b", 32) ||
		rows[1]["hostname"] != "host-b" || rows[1]["total_bytes"] != "200" {
		t.Fatalf("partial MEMORY rows = %v", rows)
	}
	if outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_PARTIAL ||
		len(outcome.GetWarnings()) != 1 ||
		outcome.GetWarnings()[0].GetRole() != providerv1.QueryDiagnosticRole_QUERY_DIAGNOSTIC_ROLE_PARTIAL_RESULT_CAUSE ||
		outcome.GetWarnings()[0].GetTarget() != "fleet-a/"+strings.Repeat("c", 32) ||
		outcome.GetWarnings()[0].GetCode() != "MEMORY_COLLECTION_FAILED" ||
		!strings.Contains(outcome.GetWarnings()[0].GetMessage(), "sensor unavailable") ||
		!outcome.GetWarnings()[0].GetRetryable() {
		t.Fatalf("partial MEMORY outcome = %v", outcome)
	}

	beforeA := goodA.calls.Load()
	beforeB := goodB.calls.Load()
	beforeFailed := failed.calls.Load()
	rows, outcome, err = harness.query(
		t,
		memoryRequest(true, logicalAccessAnd(
			accessEquality("namespace", "fleet-a"),
			accessEquality("hostname", "host-b"),
		)),
	)
	if err != nil {
		t.Fatalf("routed MEMORY query error = %v", err)
	}
	if len(rows) != 1 || rows[0]["host_id"] != strings.Repeat("b", 32) ||
		outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE {
		t.Fatalf("routed MEMORY result = %v, %v", rows, outcome)
	}
	if goodA.calls.Load() != beforeA || goodB.calls.Load() != beforeB+1 ||
		failed.calls.Load() != beforeFailed {
		t.Fatalf(
			"routed collector calls = %d, %d, %d; before %d, %d, %d",
			goodA.calls.Load(), goodB.calls.Load(), failed.calls.Load(),
			beforeA, beforeB, beforeFailed,
		)
	}

	hostRows, hostOutcome, err := harness.query(t, hostRequest())
	if err != nil {
		t.Fatalf("HOST query error = %v", err)
	}
	if len(hostRows) != 3 || hostOutcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE {
		t.Fatalf("HOST result = %v, %v", hostRows, hostOutcome)
	}

	// Keep the strict failure last: it deliberately cancels still-running
	// targets as soon as one target fails, so their cooperative cancellation
	// may briefly consume the advertised per-agent scan slot.
	_, _, err = harness.query(t, memoryRequest(false, nil))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("strict MEMORY query code = %v, want Unavailable", status.Code(err))
	}
}

func TestHostProviderAllAgentTablesEndToEnd(t *testing.T) {
	harness := newEndToEndHarness(t)
	harness.startAgent(t, strings.Repeat("a", 32), "host-a", 100, nil)
	harness.waitForActiveHosts(t, 1)
	harness.waitForHeartbeats(t, 1)

	tests := []struct {
		table       string
		projections []string
		wantRows    int
		check       func(*testing.T, []map[string]string)
	}{
		{
			table: "HOST_FACTS", projections: []string{"host_id", "reported_hostname", "process_count"}, wantRows: 1,
			check: func(t *testing.T, rows []map[string]string) {
				if rows[0]["reported_hostname"] != "observed-host" || rows[0]["process_count"] != "2" {
					t.Fatalf("HOST_FACTS rows = %v", rows)
				}
			},
		},
		{
			table: "CPU_INFO", projections: []string{"host_id", "logical_id", "flags"}, wantRows: 2,
			check: func(t *testing.T, rows []map[string]string) {
				if rows[0]["flags"] != "sse,avx" || rows[1]["logical_id"] != "1" {
					t.Fatalf("CPU_INFO rows = %v", rows)
				}
			},
		},
		{table: "CPU_TIMES", projections: []string{"host_id", "cpu", "user_seconds"}, wantRows: 2},
		{table: "MEMORY", projections: []string{"host_id", "total_bytes"}, wantRows: 1},
		{table: "FILESYSTEMS", projections: []string{"host_id", "mountpoint", "options"}, wantRows: 1},
		{table: "NETWORK_INTERFACES", projections: []string{"host_id", "name", "addresses"}, wantRows: 1},
		{table: "NETWORK_IO", projections: []string{"host_id", "name", "bytes_received"}, wantRows: 1},
		{
			table: "PROCESSES", projections: []string{"host_id", "pid", "created_at", "statuses"}, wantRows: 2,
			check: func(t *testing.T, rows []map[string]string) {
				if rows[0]["pid"] != "10" || rows[1]["pid"] != "11" || rows[1]["statuses"] != "sleeping" {
					t.Fatalf("PROCESSES rows = %v", rows)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.table, func(t *testing.T) {
			rows, outcome, err := harness.query(t, agentTableRequest(test.table, test.projections...))
			if err != nil {
				t.Fatalf("query error = %v", err)
			}
			if len(rows) != test.wantRows || outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE {
				t.Fatalf("rows/outcome = %v, %v; want %d complete rows", rows, outcome, test.wantRows)
			}
			if test.check != nil {
				test.check(t, rows)
			}
		})
	}
}

func TestHostProviderBoundsConcurrentFanout(t *testing.T) {
	config := testQueryConfig()
	config.MaxConcurrentTargets = 2
	harness := newEndToEndHarnessWithQueryConfig(t, config)
	gate := make(chan struct{})
	tracker := &concurrencyTracker{}
	for index, id := range []string{"a", "b", "c", "d"} {
		memory := &fakeMemoryCollector{
			total:   uint64(index + 1),
			gate:    gate,
			tracker: tracker,
		}
		harness.startAgentWithCollector(
			t,
			strings.Repeat(id, 32),
			"host-"+id,
			memory,
		)
	}
	harness.waitForActiveHosts(t, 4)

	type queryResult struct {
		rows    []map[string]string
		outcome *providerv1.QueryOutcome
		err     error
	}
	result := make(chan queryResult, 1)
	go func() {
		rows, outcome, err := harness.query(t, memoryRequest(true, accessEquality("namespace", "fleet-a")))
		result <- queryResult{rows: rows, outcome: outcome, err: err}
	}()
	waitForCondition(t, func() bool { return tracker.active.Load() == 2 })
	if got := tracker.maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrent targets = %d, want 2", got)
	}
	time.Sleep(25 * time.Millisecond)
	if got := tracker.active.Load(); got != 2 {
		t.Fatalf("active targets while blocked = %d, want 2", got)
	}
	close(gate)
	query := <-result
	if query.err != nil || len(query.rows) != 4 ||
		query.outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE {
		t.Fatalf("bounded fanout result = %v, %v, %v", query.rows, query.outcome, query.err)
	}
	if got := tracker.maximum.Load(); got > 2 {
		t.Fatalf("maximum concurrent targets = %d, want at most 2", got)
	}
}

func TestHostProviderKeepsQueryTargetSnapshotFixed(t *testing.T) {
	config := testQueryConfig()
	config.MaxConcurrentTargets = 1
	harness := newEndToEndHarnessWithQueryConfig(t, config)
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	harness.startAgentWithCollector(t, strings.Repeat("a", 32), "host-a", &fakeMemoryCollector{
		total:   100,
		gate:    gate,
		started: started,
	})
	harness.waitForActiveHosts(t, 1)

	type queryResult struct {
		rows    []map[string]string
		outcome *providerv1.QueryOutcome
		err     error
	}
	result := make(chan queryResult, 1)
	go func() {
		rows, outcome, err := harness.query(t, memoryRequest(true, accessEquality("namespace", "fleet-a")))
		result <- queryResult{rows: rows, outcome: outcome, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initial target scan did not start")
	}
	harness.startAgent(t, strings.Repeat("b", 32), "host-b", 200, nil)
	harness.waitForActiveHosts(t, 2)
	close(gate)
	query := <-result
	if query.err != nil || len(query.rows) != 1 ||
		query.rows[0]["host_id"] != strings.Repeat("a", 32) ||
		query.outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE {
		t.Fatalf("snapshot query result = %v, %v, %v", query.rows, query.outcome, query.err)
	}
}

func TestHostProviderReportsTimeoutAndKeepsSessionUsable(t *testing.T) {
	config := testQueryConfig()
	config.ScanTimeout = 50 * time.Millisecond
	harness := newEndToEndHarnessWithQueryConfig(t, config)
	gate := make(chan struct{})
	harness.startAgentWithCollector(t, strings.Repeat("a", 32), "host-a", &fakeMemoryCollector{
		total: 100,
		gate:  gate,
	})
	harness.waitForActiveHosts(t, 1)

	rows, outcome, err := harness.query(t, memoryRequest(true, accessEquality("hostname", "host-a")))
	if err != nil || len(rows) != 0 ||
		outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_PARTIAL ||
		len(outcome.GetWarnings()) != 1 ||
		outcome.GetWarnings()[0].GetCode() != "HOST_SCAN_TIMEOUT" ||
		outcome.GetWarnings()[0].GetRole() != providerv1.QueryDiagnosticRole_QUERY_DIAGNOSTIC_ROLE_PARTIAL_RESULT_CAUSE {
		t.Fatalf("timeout result = %v, %v, %v", rows, outcome, err)
	}
	close(gate)
	rows, outcome, err = harness.query(t, memoryRequest(true, accessEquality("hostname", "host-a")))
	if err != nil || len(rows) != 1 ||
		outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE {
		t.Fatalf("post-timeout result = %v, %v, %v", rows, outcome, err)
	}
}

func TestHostProviderBoundsRowsAndKeepsSessionUsable(t *testing.T) {
	config := testQueryConfig()
	config.MaxRowsPerTarget = 1
	harness := newEndToEndHarnessWithQueryConfig(t, config)
	harness.startAgent(t, strings.Repeat("a", 32), "host-a", 100, nil)
	harness.waitForActiveHosts(t, 1)

	request := agentTableRequest("PROCESSES", "host_id", "pid")
	request.AllowPartialResults = true
	rows, outcome, err := harness.query(t, request)
	if err != nil || len(rows) != 0 ||
		outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_PARTIAL ||
		len(outcome.GetWarnings()) != 1 ||
		outcome.GetWarnings()[0].GetCode() != "HOST_RESULT_LIMIT_EXCEEDED" ||
		outcome.GetWarnings()[0].GetRetryable() {
		t.Fatalf("row-limited result = %v, %v, %v", rows, outcome, err)
	}
	rows, outcome, err = harness.query(t, memoryRequest(true, accessEquality("hostname", "host-a")))
	if err != nil || len(rows) != 1 ||
		outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE {
		t.Fatalf("post-limit result = %v, %v, %v", rows, outcome, err)
	}
}

func TestHostProviderBoundsEncodedBytesPerTarget(t *testing.T) {
	config := testQueryConfig()
	config.MaxBytesPerTarget = 1
	harness := newEndToEndHarnessWithQueryConfig(t, config)
	harness.startAgent(t, strings.Repeat("a", 32), "host-a", 100, nil)
	harness.waitForActiveHosts(t, 1)

	rows, outcome, err := harness.query(t, memoryRequest(true, accessEquality("hostname", "host-a")))
	if err != nil || len(rows) != 0 ||
		outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_PARTIAL ||
		len(outcome.GetWarnings()) != 1 ||
		outcome.GetWarnings()[0].GetCode() != "HOST_RESULT_LIMIT_EXCEEDED" {
		t.Fatalf("byte-limited result = %v, %v, %v", rows, outcome, err)
	}
}

func TestHostAgentRejectsOversizedRowWithoutBreakingSession(t *testing.T) {
	harness := newEndToEndHarness(t)
	memory := &fakeMemoryCollector{
		total:              100,
		processCommandLine: strings.Repeat("x", maximumTestRowBytes),
	}
	harness.startAgentWithCollector(t, strings.Repeat("a", 32), "host-a", memory)
	harness.waitForActiveHosts(t, 1)

	request := agentTableRequest("PROCESSES", "host_id", "pid")
	request.AllowPartialResults = true
	rows, outcome, err := harness.query(t, request)
	if err != nil || len(rows) != 0 ||
		outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_PARTIAL ||
		len(outcome.GetWarnings()) != 1 ||
		outcome.GetWarnings()[0].GetCode() != "HOST_ROW_TOO_LARGE" ||
		outcome.GetWarnings()[0].GetRetryable() {
		t.Fatalf("oversized-row result = %v, %v, %v", rows, outcome, err)
	}
	rows, outcome, err = harness.query(t, memoryRequest(true, accessEquality("hostname", "host-a")))
	if err != nil || len(rows) != 1 ||
		outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE {
		t.Fatalf("post-oversized-row result = %v, %v, %v", rows, outcome, err)
	}
}

func TestHostProviderReportsDisconnectedSnapshotTarget(t *testing.T) {
	harness := newEndToEndHarness(t)
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	connector := &closeTrackingConnector{
		delegate:  harness.connector,
		connected: make(chan io.Closer, 1),
	}
	cancelAgent := harness.startAgentWithConnector(t, strings.Repeat("a", 32), "host-a", &fakeMemoryCollector{
		total:   100,
		gate:    gate,
		started: started,
	}, connector)
	harness.waitForActiveHosts(t, 1)
	var agentConnection io.Closer
	select {
	case agentConnection = <-connector.connected:
	case <-time.After(time.Second):
		t.Fatal("agent transport was not established")
	}

	type queryResult struct {
		rows    []map[string]string
		outcome *providerv1.QueryOutcome
		err     error
	}
	result := make(chan queryResult, 1)
	go func() {
		rows, outcome, err := harness.query(t, memoryRequest(true, accessEquality("hostname", "host-a")))
		result <- queryResult{rows: rows, outcome: outcome, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("target scan did not start")
	}
	if err := agentConnection.Close(); err != nil {
		t.Fatalf("close agent transport: %v", err)
	}
	cancelAgent()
	query := <-result
	if query.err != nil || len(query.rows) != 0 ||
		query.outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_PARTIAL ||
		len(query.outcome.GetWarnings()) != 1 ||
		query.outcome.GetWarnings()[0].GetCode() != "HOST_TARGET_DISCONNECTED" {
		t.Fatalf("disconnected target result = %v, %v, %v", query.rows, query.outcome, query.err)
	}
}

type endToEndHarness struct {
	ctx         context.Context
	cancel      context.CancelFunc
	registry    *registry.Memory
	client      providerv1.ProviderServiceClient
	connection  string
	agentTarget string
	connector   agent.StreamConnector
	agentErrors chan error
	agentCount  int
}

func newEndToEndHarness(t *testing.T) *endToEndHarness {
	t.Helper()
	return newEndToEndHarnessWithQueryConfig(t, testQueryConfig())
}

func testQueryConfig() query.Config {
	return query.Config{
		AllowPartialResults:  true,
		AllowUnboundedFanout: true,
		ScanTimeout:          2 * time.Second,
		PreferredBatchRows:   1,
	}
}

func newEndToEndHarnessWithQueryConfig(
	t *testing.T,
	queryConfig query.Config,
) *endToEndHarness {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	fleet := registry.NewMemory()
	sessions := gateway.NewDirectory()
	coordinator, err := query.New(fleet, sessions, queryConfig)
	if err != nil {
		t.Fatalf("query.New() error = %v", err)
	}
	implementation, err := host.New(
		host.ReadinessFunc(func(ctx context.Context) error { return ctx.Err() }),
		coordinator,
		host.Config{PartialResults: true},
	)
	if err != nil {
		t.Fatalf("host.New() error = %v", err)
	}
	providerService := providersdk.NewServer(implementation)
	gatewayConfig := gateway.DefaultConfig()
	gatewayConfig.HeartbeatInterval = 10 * time.Millisecond
	gatewayConfig.LeaseDuration = time.Second
	gatewayService, err := gateway.NewServer(
		gatewayConfig,
		fleet,
		sessions,
		gateway.AuthorizerFunc(func(_ context.Context, requested string) (string, error) {
			if requested != "fleet-a" {
				return "", status.Error(codes.PermissionDenied, "namespace is not authorized")
			}
			return requested, nil
		}),
	)
	if err != nil {
		t.Fatalf("gateway.NewServer() error = %v", err)
	}
	server := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(server, providerService)
	gatewaypb.RegisterAgentGatewayServiceServer(server, gatewayService)
	go func() { _ = server.Serve(listener) }()

	connection, err := grpc.NewClient(
		"passthrough:///host-provider-e2e",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		server.Stop()
		_ = listener.Close()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	client := providerv1.NewProviderServiceClient(connection)
	opened, err := client.OpenConnection(context.Background(), &providerv1.OpenConnectionRequest{})
	if err != nil {
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
		t.Fatalf("OpenConnection() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	harness := &endToEndHarness{
		ctx:         ctx,
		cancel:      cancel,
		registry:    fleet,
		client:      client,
		connection:  opened.GetConnectionId(),
		agentTarget: "passthrough:///host-provider-e2e",
		connector:   bufConnector{listener: listener},
		agentErrors: make(chan error, 8),
	}
	t.Cleanup(func() {
		cancel()
		for range harness.agentCount {
			select {
			case agentErr := <-harness.agentErrors:
				if agentErr != nil {
					t.Errorf("agent runtime error = %v", agentErr)
				}
			case <-time.After(2 * time.Second):
				t.Error("agent runtime did not stop before timeout")
			}
		}
		_, _ = client.CloseConnection(context.Background(), &providerv1.CloseConnectionRequest{
			ConnectionId: harness.connection,
		})
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
	})
	return harness
}

func (h *endToEndHarness) startAgent(
	t *testing.T,
	agentID string,
	hostname string,
	total uint64,
	collectionError error,
) *fakeMemoryCollector {
	t.Helper()
	memory := &fakeMemoryCollector{total: total, err: collectionError}
	h.startAgentWithCollector(t, agentID, hostname, memory)
	return memory
}

func (h *endToEndHarness) startAgentWithCollector(
	t *testing.T,
	agentID string,
	hostname string,
	memory *fakeMemoryCollector,
) context.CancelFunc {
	return h.startAgentWithConnector(t, agentID, hostname, memory, h.connector)
}

func (h *endToEndHarness) startAgentWithConnector(
	t *testing.T,
	agentID string,
	hostname string,
	memory *fakeMemoryCollector,
	connector agent.StreamConnector,
) context.CancelFunc {
	t.Helper()
	agentContext, cancelAgent := context.WithCancel(h.ctx)
	runtime, err := agent.NewRuntime(
		agent.Config{
			GatewayAddress:     h.agentTarget,
			Namespace:          "fleet-a",
			StateDirectory:     t.TempDir(),
			Insecure:           true,
			Hostname:           hostname,
			MaxConcurrentScans: 1,
			ConnectTimeout:     time.Second,
			Backoff: agent.BackoffConfig{
				Initial: 10 * time.Millisecond,
				Maximum: 50 * time.Millisecond,
			},
		},
		agentID,
		"test",
		memory,
		agent.WithConnector(connector),
		agent.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("agent.NewRuntime() error = %v", err)
	}
	h.agentCount++
	go func() { h.agentErrors <- runtime.Run(agentContext) }()
	return cancelAgent
}

type bufConnector struct {
	listener *bufconn.Listener
}

type closeTrackingConnector struct {
	delegate  agent.StreamConnector
	connected chan io.Closer
}

func (c *closeTrackingConnector) Connect(
	ctx context.Context,
) (gatewaypb.AgentGatewayService_ConnectClient, io.Closer, error) {
	stream, closer, err := c.delegate.Connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	select {
	case c.connected <- closer:
	case <-ctx.Done():
		_ = closer.Close()
		return nil, nil, ctx.Err()
	}
	return stream, closer, nil
}

func (c bufConnector) Connect(
	ctx context.Context,
) (gatewaypb.AgentGatewayService_ConnectClient, io.Closer, error) {
	connection, err := grpc.NewClient(
		"passthrough:///host-agent-e2e",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return c.listener.Dial()
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	stream, err := gatewaypb.NewAgentGatewayServiceClient(connection).Connect(ctx)
	if err != nil {
		_ = connection.Close()
		return nil, nil, err
	}
	return stream, connection, nil
}

func (h *endToEndHarness) waitForActiveHosts(t *testing.T, count int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		active, err := h.registry.SnapshotActive(context.Background(), "fleet-a", time.Now())
		if err == nil && len(active) == count {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("active host count did not reach %d", count)
}

func (h *endToEndHarness) waitForHeartbeats(t *testing.T, count int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		active, err := h.registry.SnapshotActive(context.Background(), "fleet-a", time.Now())
		if err == nil && len(active) == count {
			allRenewed := true
			for _, snapshot := range active {
				if !snapshot.Session.LastSeenAt.After(snapshot.Session.ConnectedAt) {
					allRenewed = false
					break
				}
			}
			if allRenewed {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("agents did not renew their sessions before timeout")
}

func (h *endToEndHarness) query(
	t *testing.T,
	request *providerv1.QueryRequest,
) ([]map[string]string, *providerv1.QueryOutcome, error) {
	t.Helper()
	request.ConnectionId = h.connection
	stream, err := h.client.Query(context.Background(), request)
	if err != nil {
		return nil, nil, err
	}
	var rows []map[string]string
	var outcome *providerv1.QueryOutcome
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return rows, outcome, nil
		}
		if err != nil {
			return rows, outcome, err
		}
		if response.GetOutcome() != nil {
			outcome = response.GetOutcome()
			continue
		}
		batch := response.GetBatch()
		for _, tuple := range batch.GetTuples() {
			row := make(map[string]string, len(batch.GetFields()))
			for index, field := range batch.GetFields() {
				value := tuple.GetValues()[index]
				switch typed := value.GetKind().(type) {
				case *kublingv1.Value_StringValue:
					row[field.GetName()] = typed.StringValue
				case *kublingv1.Value_IntegerValue:
					row[field.GetName()] = fmt.Sprint(typed.IntegerValue)
				case *kublingv1.Value_LongValue:
					row[field.GetName()] = fmt.Sprint(typed.LongValue)
				case *kublingv1.Value_BigintegerValue:
					row[field.GetName()] = typed.BigintegerValue
				case *kublingv1.Value_DoubleValue:
					row[field.GetName()] = fmt.Sprint(typed.DoubleValue)
				case *kublingv1.Value_TimestampValue:
					row[field.GetName()] = typed.TimestampValue
				case *kublingv1.Value_ArrayValue:
					elements := make([]string, 0, len(typed.ArrayValue.GetElements()))
					for _, element := range typed.ArrayValue.GetElements() {
						elements = append(elements, element.GetStringValue())
					}
					row[field.GetName()] = strings.Join(elements, ",")
				case *kublingv1.Value_NullValue:
					row[field.GetName()] = "NULL"
				default:
					row[field.GetName()] = value.String()
				}
			}
			rows = append(rows, row)
		}
	}
}

type fakeMemoryCollector struct {
	calls              atomic.Int64
	total              uint64
	err                error
	gate               <-chan struct{}
	started            chan<- struct{}
	tracker            *concurrencyTracker
	processCommandLine string
}

const maximumTestRowBytes = (2 << 20) + 1024

type concurrencyTracker struct {
	active  atomic.Int64
	maximum atomic.Int64
}

func (t *concurrencyTracker) enter() func() {
	active := t.active.Add(1)
	for {
		maximum := t.maximum.Load()
		if active <= maximum || t.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	return func() { t.active.Add(-1) }
}

func (c *fakeMemoryCollector) Host(
	context.Context,
) (collector.Observation[collector.HostRow], error) {
	return collector.Observation[collector.HostRow]{
		ObservedAt: testObservedAt(),
		Rows: []collector.HostRow{{
			Hostname: "observed-host", OS: "linux", Platform: "debian",
			BootedAt: testObservedAt().Add(-time.Hour), UptimeSeconds: 3600,
			ProcessCount: 2,
		}},
	}, nil
}

func (c *fakeMemoryCollector) CPUInfo(
	context.Context,
) (collector.Observation[collector.CPUInfoRow], error) {
	return collector.Observation[collector.CPUInfoRow]{
		ObservedAt: testObservedAt(),
		Rows: []collector.CPUInfoRow{
			{LogicalID: 0, VendorID: "vendor", MHz: 3000, Flags: []string{"sse", "avx"}},
			{LogicalID: 1, VendorID: "vendor", MHz: 3000, Flags: []string{"sse", "avx"}},
		},
	}, nil
}

func (c *fakeMemoryCollector) CPUTimes(
	context.Context,
) (collector.Observation[collector.CPUTimesRow], error) {
	return collector.Observation[collector.CPUTimesRow]{
		ObservedAt: testObservedAt(),
		Rows: []collector.CPUTimesRow{
			{CPU: "cpu0", User: 1, System: 2, Idle: 3},
			{CPU: "cpu1", User: 4, System: 5, Idle: 6},
		},
	}, nil
}

func (c *fakeMemoryCollector) Memory(
	ctx context.Context,
) (collector.Observation[collector.MemoryRow], error) {
	c.calls.Add(1)
	if c.tracker != nil {
		leave := c.tracker.enter()
		defer leave()
	}
	if c.started != nil {
		select {
		case c.started <- struct{}{}:
		default:
		}
	}
	if c.gate != nil {
		select {
		case <-c.gate:
		case <-ctx.Done():
			return collector.Observation[collector.MemoryRow]{}, ctx.Err()
		}
	}
	if c.err != nil {
		return collector.Observation[collector.MemoryRow]{}, c.err
	}
	return collector.Observation[collector.MemoryRow]{
		ObservedAt: testObservedAt(),
		Rows: []collector.MemoryRow{{
			TotalBytes:     c.total,
			AvailableBytes: c.total / 2,
			UsedBytes:      c.total / 2,
			UsedPercent:    50,
			FreeBytes:      c.total / 4,
		}},
	}, nil
}

func (c *fakeMemoryCollector) Filesystems(
	context.Context,
) (collector.Observation[collector.FilesystemRow], error) {
	total := c.total
	return collector.Observation[collector.FilesystemRow]{
		ObservedAt: testObservedAt(),
		Rows: []collector.FilesystemRow{{
			Device: "/dev/sda1", Mountpoint: "/", FilesystemType: "ext4",
			Options: []string{"rw"}, TotalBytes: &total,
		}},
	}, nil
}

func (c *fakeMemoryCollector) NetworkInterfaces(
	context.Context,
) (collector.Observation[collector.NetworkInterfaceRow], error) {
	return collector.Observation[collector.NetworkInterfaceRow]{
		ObservedAt: testObservedAt(),
		Rows: []collector.NetworkInterfaceRow{{
			Index: 2, Name: "eth0", MTU: 1500, Flags: []string{"up"},
			Addresses: []string{"192.0.2.1/24"},
		}},
	}, nil
}

func (c *fakeMemoryCollector) NetworkIO(
	context.Context,
) (collector.Observation[collector.NetworkIORow], error) {
	return collector.Observation[collector.NetworkIORow]{
		ObservedAt: testObservedAt(),
		Rows:       []collector.NetworkIORow{{Name: "eth0", BytesReceived: c.total}},
	}, nil
}

func (c *fakeMemoryCollector) Processes(
	context.Context,
) (collector.Observation[collector.ProcessRow], error) {
	parent := int32(1)
	name := "process"
	commandLine := c.processCommandLine
	if commandLine == "" {
		commandLine = "process"
	}
	return collector.Observation[collector.ProcessRow]{
		ObservedAt: testObservedAt(),
		Rows: []collector.ProcessRow{
			{PID: 10, CreatedAt: testObservedAt().Add(-time.Minute), ParentPID: &parent, Name: &name, CommandLine: &commandLine, Statuses: []string{"running"}},
			{PID: 11, CreatedAt: testObservedAt(), ParentPID: &parent, Name: &name, Statuses: []string{"sleeping"}},
		},
	}, nil
}

func testObservedAt() time.Time {
	return time.Date(2026, time.October, 8, 10, 0, 0, 0, time.UTC)
}

func memoryRequest(
	allowPartial bool,
	filter *providerv1.Expression,
) *providerv1.QueryRequest {
	return &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "MEMORY"},
		Projections: []*providerv1.Projection{
			fieldProjection("namespace"),
			fieldProjection("host_id"),
			fieldProjection("hostname"),
			fieldProjection("total_bytes"),
		},
		Filter:              filter,
		AcceptOutcome:       true,
		AllowPartialResults: allowPartial,
	}
}

func agentTableRequest(table string, projections ...string) *providerv1.QueryRequest {
	request := &providerv1.QueryRequest{
		Entity:           &providerv1.EntityReference{Name: table},
		Filter:           accessEquality("hostname", "host-a"),
		AcceptOutcome:    true,
		AcceptedFeatures: []string{grpcfeatures.ArrayValuesV1},
	}
	for _, projection := range projections {
		request.Projections = append(request.Projections, fieldProjection(projection))
	}
	return request
}

func hostRequest() *providerv1.QueryRequest {
	return &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "HOST"},
		Projections: []*providerv1.Projection{
			fieldProjection("namespace"),
			fieldProjection("host_id"),
			fieldProjection("state"),
		},
		AcceptOutcome: true,
	}
}

func fieldProjection(name string) *providerv1.Projection {
	return &providerv1.Projection{
		Expression: &providerv1.Expression{Kind: &providerv1.Expression_Field{
			Field: &providerv1.FieldReference{Name: name},
		}},
	}
}

func accessEquality(field string, value string) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Comparison{
		Comparison: &providerv1.ComparisonExpression{
			Operator: providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			Left: &providerv1.Expression{Kind: &providerv1.Expression_Field{
				Field: &providerv1.FieldReference{Name: field},
			}},
			Right: &providerv1.Expression{Kind: &providerv1.Expression_Literal{
				Literal: &providerv1.Literal{Value: &kublingv1.Value{
					Kind: &kublingv1.Value_StringValue{StringValue: value},
				}},
			}},
		},
	}}
}

func logicalAccessAnd(operands ...*providerv1.Expression) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Logical{
		Logical: &providerv1.LogicalExpression{
			Operator: providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
			Operands: operands,
		},
	}}
}

func waitForCondition(t *testing.T, condition func() bool) {
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
