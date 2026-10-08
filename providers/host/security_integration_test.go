package host_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	host "github.com/kubling-community/kubling-providers/providers/host"
	"github.com/kubling-community/kubling-providers/providers/host/internal/agent"
	"github.com/kubling-community/kubling-providers/providers/host/internal/gateway"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/query"
	"github.com/kubling-community/kubling-providers/providers/host/internal/registry"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func TestHostProviderSecureEndToEndWithDurableRegistry(t *testing.T) {
	certificateFile, privateKeyFile, caFile := writeGatewayTLSMaterial(t)
	credentialFile := filepath.Join(t.TempDir(), "fleet-a.token")
	const token = "0123456789abcdef0123456789abcdef"
	if err := os.WriteFile(credentialFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatalf("write credential: %v", err)
	}

	fleet, err := registry.OpenBolt(t.TempDir(), time.Now().UTC())
	if err != nil {
		t.Fatalf("registry.OpenBolt() error = %v", err)
	}
	t.Cleanup(func() { _ = fleet.Close() })
	directory := gateway.NewDirectory()
	coordinator, err := query.New(fleet, directory, query.Config{
		AllowPartialResults: true,
		ScanTimeout:         time.Second,
	})
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
	providerServer := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(providerServer, providerService)
	providerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("provider net.Listen() error = %v", err)
	}
	go func() { _ = providerServer.Serve(providerListener) }()
	t.Cleanup(func() {
		providerServer.Stop()
		_ = providerListener.Close()
		_ = providerService.Close(context.Background())
	})

	authorizer, err := gateway.NewTokenAuthorizer(map[string]string{"fleet-a": token})
	if err != nil {
		t.Fatalf("gateway.NewTokenAuthorizer() error = %v", err)
	}
	gatewayConfig := gateway.DefaultConfig()
	gatewayConfig.HeartbeatInterval = 10 * time.Millisecond
	gatewayConfig.LeaseDuration = time.Second
	gatewayService, err := gateway.NewServer(
		gatewayConfig,
		fleet,
		directory,
		authorizer,
	)
	if err != nil {
		t.Fatalf("gateway.NewServer() error = %v", err)
	}
	serverCertificate, err := tls.LoadX509KeyPair(certificateFile, privateKeyFile)
	if err != nil {
		t.Fatalf("tls.LoadX509KeyPair() error = %v", err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCertificate},
	})))
	gatewaypb.RegisterAgentGatewayServiceServer(server, gatewayService)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	runtime, err := agent.NewRuntime(
		agent.Config{
			GatewayAddress: listener.Addr().String(),
			Namespace:      "fleet-a",
			StateDirectory: t.TempDir(),
			CredentialFile: credentialFile,
			TLS: agent.TLSConfig{
				CAFile:     caFile,
				ServerName: "localhost",
			},
			Hostname:           "host-a",
			MaxConcurrentScans: 1,
			ConnectTimeout:     time.Second,
			Backoff: agent.BackoffConfig{
				Initial: 10 * time.Millisecond,
				Maximum: 50 * time.Millisecond,
			},
		},
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"test",
		&fakeMemoryCollector{total: 100},
		agent.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("agent.NewRuntime() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runtimeResult := make(chan error, 1)
	go func() { runtimeResult <- runtime.Run(ctx) }()
	waitForCondition(t, func() bool {
		active, snapshotErr := fleet.SnapshotActive(context.Background(), "fleet-a", time.Now().UTC())
		return snapshotErr == nil && len(active) == 1 &&
			active[0].Identity.Key.ID == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" &&
			active[0].Session.LastSeenAt.After(active[0].Session.ConnectedAt)
	})

	providerConnection, err := grpc.NewClient(
		providerListener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("provider grpc.NewClient() error = %v", err)
	}
	providerClient := providerv1.NewProviderServiceClient(providerConnection)
	opened, err := providerClient.OpenConnection(
		context.Background(),
		&providerv1.OpenConnectionRequest{},
	)
	if err != nil {
		_ = providerConnection.Close()
		t.Fatalf("ProviderService.OpenConnection() error = %v", err)
	}
	t.Cleanup(func() {
		_, _ = providerClient.CloseConnection(
			context.Background(),
			&providerv1.CloseConnectionRequest{ConnectionId: opened.GetConnectionId()},
		)
		_ = providerConnection.Close()
	})
	providerHarness := &endToEndHarness{
		client:     providerClient,
		connection: opened.GetConnectionId(),
	}
	rows, outcome, err := providerHarness.query(
		t,
		memoryRequest(false, accessEquality("namespace", "fleet-a")),
	)
	if err != nil || len(rows) != 1 || rows[0]["total_bytes"] != "100" ||
		outcome.GetCompletion() != providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE {
		t.Fatalf("secure ProviderService query = %v, %v, %v", rows, outcome, err)
	}

	cancel()
	select {
	case err := <-runtimeResult:
		if err != nil {
			t.Fatalf("agent Runtime.Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("secure agent runtime did not stop before timeout")
	}

	wrongCredential := filepath.Join(t.TempDir(), "wrong.token")
	if err := os.WriteFile(
		wrongCredential,
		[]byte("fedcba9876543210fedcba9876543210\n"),
		0o600,
	); err != nil {
		t.Fatalf("write wrong credential: %v", err)
	}
	wrongRuntime, err := agent.NewRuntime(
		agent.Config{
			GatewayAddress: listener.Addr().String(),
			Namespace:      "fleet-a",
			StateDirectory: t.TempDir(),
			CredentialFile: wrongCredential,
			TLS: agent.TLSConfig{
				CAFile:     caFile,
				ServerName: "localhost",
			},
			Hostname:           "host-b",
			MaxConcurrentScans: 1,
			ConnectTimeout:     time.Second,
			Backoff: agent.BackoffConfig{
				Initial: 10 * time.Millisecond,
				Maximum: 50 * time.Millisecond,
			},
		},
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"test",
		&fakeMemoryCollector{total: 200},
		agent.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("agent.NewRuntime(wrong credential) error = %v", err)
	}
	if err := wrongRuntime.Run(context.Background()); !errors.Is(err, agent.ErrIncompatibleGateway) {
		t.Fatalf("wrong credential error = %v, want ErrIncompatibleGateway", err)
	}
}

func writeGatewayTLSMaterial(t *testing.T) (string, string, string) {
	t.Helper()
	now := time.Now().UTC()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Kubling Host Test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(
		rand.Reader,
		serverTemplate,
		caTemplate,
		&serverKey.PublicKey,
		caKey,
	)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}
	serverKeyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}
	directory := t.TempDir()
	certificateFile := filepath.Join(directory, "server.pem")
	privateKeyFile := filepath.Join(directory, "server-key.pem")
	caFile := filepath.Join(directory, "ca.pem")
	writePEMFile(t, certificateFile, "CERTIFICATE", serverDER)
	writePEMFile(t, privateKeyFile, "PRIVATE KEY", serverKeyDER)
	writePEMFile(t, caFile, "CERTIFICATE", caDER)
	return certificateFile, privateKeyFile, caFile
}

func writePEMFile(t *testing.T, path string, blockType string, contents []byte) {
	t.Helper()
	encoded := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: contents})
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
