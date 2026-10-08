package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubling-community/kubling-providers/providers/host/internal/agent/collector"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLoadAndNormalizeConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte(`
gatewayAddress: provider.internal:50052
namespace: rack-a
stateDirectory: /var/lib/example
credentialFile: /run/secrets/host-agent.token
tls:
  caFile: /run/secrets/host-agent-ca.pem
  serverName: provider.internal
hostname: host-a
attributes:
  zone: west
maxConcurrentScans: 3
connectTimeout: 4s
backoff:
  initial: 250ms
  maximum: 5s
  jitter: 0
collector:
  includePseudoFilesystems: true
`), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	config, err := NormalizeConfig(loaded)
	if err != nil {
		t.Fatalf("NormalizeConfig() error = %v", err)
	}
	if config.GatewayAddress != "provider.internal:50052" ||
		config.Namespace != "rack-a" || config.MaxConcurrentScans != 3 ||
		config.CredentialFile != "/run/secrets/host-agent.token" ||
		config.TLS.CAFile != "/run/secrets/host-agent-ca.pem" ||
		config.TLS.ServerName != "provider.internal" ||
		config.ConnectTimeout != 4*time.Second ||
		config.Backoff.Initial != 250*time.Millisecond ||
		config.Backoff.Maximum != 5*time.Second || config.Backoff.Jitter != 0 ||
		!config.Collector.IncludePseudoFilesystems || config.Attributes["zone"] != "west" {
		t.Fatalf("normalized config = %#v", config)
	}
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte("unknown: true\n"), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("LoadConfig() error = nil, want unknown field error")
	}
}

func TestNormalizeConfigRequiresExplicitSecureOrInsecureTransport(t *testing.T) {
	base := DefaultConfig()
	base.GatewayAddress = "gateway"
	base.Namespace = "fleet-a"
	if _, err := NormalizeConfig(base); err == nil {
		t.Fatal("NormalizeConfig(without credential) error = nil")
	}
	base.Insecure = true
	base.CredentialFile = "/run/secrets/token"
	if _, err := NormalizeConfig(base); err == nil {
		t.Fatal("NormalizeConfig(insecure with credential) error = nil")
	}
}

func TestLoadOrCreateIdentityPersistsOwnerOnlyID(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	first, err := LoadOrCreateIdentity(directory)
	if err != nil {
		t.Fatalf("LoadOrCreateIdentity(first) error = %v", err)
	}
	second, err := LoadOrCreateIdentity(directory)
	if err != nil {
		t.Fatalf("LoadOrCreateIdentity(second) error = %v", err)
	}
	if first != second || len(first) != 32 {
		t.Fatalf("identities = %q, %q", first, second)
	}
	info, err := os.Stat(filepath.Join(directory, identityFilename))
	if err != nil {
		t.Fatalf("os.Stat(identity) error = %v", err)
	}
	if permissions := info.Mode().Perm(); permissions != 0o600 {
		t.Fatalf("identity permissions = %o, want 600", permissions)
	}
}

func TestRuntimeRetriesTransientConnectionFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	connector := &failingConnector{err: errors.New("gateway unavailable")}
	var waited time.Duration
	runtime := testRuntime(t, connector,
		withTiming(func() float64 { return 0.5 }, func(_ context.Context, delay time.Duration) error {
			waited = delay
			cancel()
			return context.Canceled
		}),
	)
	if err := runtime.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if connector.calls.Load() != 1 || waited != time.Second {
		t.Fatalf("reconnect attempts = %d, wait = %v", connector.calls.Load(), waited)
	}
}

func TestRuntimeStopsOnPermanentGatewayFailure(t *testing.T) {
	connector := &failingConnector{
		err: status.Error(codes.FailedPrecondition, "schema incompatible"),
	}
	runtime := testRuntime(t, connector)
	err := runtime.Run(context.Background())
	if !errors.Is(err, ErrIncompatibleGateway) {
		t.Fatalf("Run() error = %v, want ErrIncompatibleGateway", err)
	}
	if connector.calls.Load() != 1 {
		t.Fatalf("connect attempts = %d, want 1", connector.calls.Load())
	}
}

func testRuntime(
	t *testing.T,
	connector StreamConnector,
	options ...Option,
) *Runtime {
	t.Helper()
	options = append(options,
		WithConnector(connector),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	runtime, err := NewRuntime(
		Config{
			GatewayAddress:     "gateway",
			Namespace:          "fleet-a",
			StateDirectory:     t.TempDir(),
			Insecure:           true,
			Hostname:           "host-a",
			MaxConcurrentScans: 1,
			ConnectTimeout:     time.Second,
			Backoff: BackoffConfig{
				Initial: time.Second,
				Maximum: 2 * time.Second,
			},
		},
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"test",
		&staticMemoryCollector{},
		options...,
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	return runtime
}

type failingConnector struct {
	calls atomic.Int64
	err   error
}

func (c *failingConnector) Connect(
	context.Context,
) (gatewaypb.AgentGatewayService_ConnectClient, io.Closer, error) {
	c.calls.Add(1)
	return nil, nil, c.err
}

type staticMemoryCollector struct {
	*collector.Collector
}

func (*staticMemoryCollector) Memory(
	context.Context,
) (collector.Observation[collector.MemoryRow], error) {
	return collector.Observation[collector.MemoryRow]{}, nil
}
