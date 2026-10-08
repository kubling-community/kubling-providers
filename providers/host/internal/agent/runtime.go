package agent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kubling-community/kubling-providers/providers/host/internal/agent/collector"
	"github.com/kubling-community/kubling-providers/providers/host/internal/gatewayauth"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var ErrIncompatibleGateway = errors.New("host agent is incompatible with the Agent Gateway")

type hostCollector interface {
	Host(context.Context) (collector.Observation[collector.HostRow], error)
	CPUInfo(context.Context) (collector.Observation[collector.CPUInfoRow], error)
	CPUTimes(context.Context) (collector.Observation[collector.CPUTimesRow], error)
	Memory(context.Context) (collector.Observation[collector.MemoryRow], error)
	Filesystems(context.Context) (collector.Observation[collector.FilesystemRow], error)
	NetworkInterfaces(context.Context) (collector.Observation[collector.NetworkInterfaceRow], error)
	NetworkIO(context.Context) (collector.Observation[collector.NetworkIORow], error)
	Processes(context.Context) (collector.Observation[collector.ProcessRow], error)
}

// StreamConnector opens one Agent Gateway stream for a runtime attempt.
type StreamConnector interface {
	Connect(context.Context) (
		gatewaypb.AgentGatewayService_ConnectClient,
		io.Closer,
		error,
	)
}

type grpcConnector struct {
	address              string
	transportCredentials credentials.TransportCredentials
	bearerToken          string
}

func (c grpcConnector) Connect(
	ctx context.Context,
) (gatewaypb.AgentGatewayService_ConnectClient, io.Closer, error) {
	connection, err := grpc.NewClient(
		c.address,
		grpc.WithTransportCredentials(c.transportCredentials),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create Agent Gateway client: %w", err)
	}
	streamContext := ctx
	if c.bearerToken != "" {
		streamContext = metadata.AppendToOutgoingContext(
			ctx,
			gatewayauth.MetadataKey,
			gatewayauth.BearerValue(c.bearerToken),
		)
	}
	stream, err := gatewaypb.NewAgentGatewayServiceClient(connection).Connect(streamContext)
	if err != nil {
		_ = connection.Close()
		return nil, nil, fmt.Errorf("open Agent Gateway stream: %w", err)
	}
	return stream, connection, nil
}

func newGRPCConnector(config Config) (grpcConnector, error) {
	connector := grpcConnector{address: config.GatewayAddress}
	if config.Insecure {
		connector.transportCredentials = insecure.NewCredentials()
		return connector, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: config.TLS.ServerName,
	}
	if config.TLS.CAFile != "" {
		contents, err := os.ReadFile(config.TLS.CAFile)
		if err != nil {
			return grpcConnector{}, fmt.Errorf("read Agent Gateway CA file: %w", err)
		}
		certificateAuthorities := x509.NewCertPool()
		if !certificateAuthorities.AppendCertsFromPEM(contents) {
			return grpcConnector{}, errors.New("Agent Gateway CA file contains no valid certificates")
		}
		tlsConfig.RootCAs = certificateAuthorities
	}
	contents, err := os.ReadFile(config.CredentialFile)
	if err != nil {
		return grpcConnector{}, fmt.Errorf("read Agent Gateway credential file: %w", err)
	}
	token := strings.TrimSpace(string(contents))
	if err := gatewayauth.ValidateToken(token); err != nil {
		return grpcConnector{}, fmt.Errorf("invalid Agent Gateway credential file: %w", err)
	}
	connector.transportCredentials = credentials.NewTLS(tlsConfig)
	connector.bearerToken = token
	return connector, nil
}

// Runtime owns the reconnecting lifecycle of one host agent.
type Runtime struct {
	config       Config
	agentID      string
	agentVersion string
	collector    hostCollector
	connector    StreamConnector
	logger       *slog.Logger
	random       func() float64
	wait         func(context.Context, time.Duration) error
}

// Option changes process integrations without changing agent semantics.
type Option func(*Runtime)

func WithLogger(logger *slog.Logger) Option {
	return func(runtime *Runtime) {
		if logger != nil {
			runtime.logger = logger
		}
	}
}

// WithConnector replaces the production gRPC connector, primarily for
// embedded deployments and transport-level tests.
func WithConnector(connector StreamConnector) Option {
	return func(runtime *Runtime) { runtime.connector = connector }
}

func withTiming(
	random func() float64,
	wait func(context.Context, time.Duration) error,
) Option {
	return func(runtime *Runtime) {
		runtime.random = random
		runtime.wait = wait
	}
}

// NewRuntime creates a validated reconnecting host-agent runtime.
func NewRuntime(
	config Config,
	agentID string,
	agentVersion string,
	hosts hostCollector,
	options ...Option,
) (*Runtime, error) {
	normalized, err := NormalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if err := validateIdentity(agentID); err != nil {
		return nil, fmt.Errorf("agent ID: %w", err)
	}
	agentVersion = strings.TrimSpace(agentVersion)
	if agentVersion == "" {
		return nil, errors.New("agent version is required")
	}
	if hosts == nil {
		return nil, errors.New("host collector is required")
	}
	connector, err := newGRPCConnector(normalized)
	if err != nil {
		return nil, err
	}
	runtime := &Runtime{
		config:       normalized,
		agentID:      agentID,
		agentVersion: agentVersion,
		collector:    hosts,
		connector:    connector,
		logger:       slog.Default(),
		random:       rand.Float64,
		wait:         waitForReconnect,
	}
	for _, option := range options {
		option(runtime)
	}
	if runtime.connector == nil || runtime.random == nil || runtime.wait == nil {
		return nil, errors.New("host-agent runtime integration is nil")
	}
	return runtime, nil
}

// Run maintains a gateway session until shutdown or a permanent compatibility
// or authorization failure occurs.
func (r *Runtime) Run(ctx context.Context) error {
	delay := r.config.Backoff.Initial
	for {
		accepted, err := r.runOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return nil
		}
		if isPermanentGatewayError(err) {
			return fmt.Errorf("%w: %v", ErrIncompatibleGateway, err)
		}
		if accepted {
			delay = r.config.Backoff.Initial
		}
		wait := jittered(delay, r.config.Backoff.Jitter, r.random())
		r.logger.Warn("host agent reconnecting", "delay", wait, "error", err)
		if err := r.wait(ctx, wait); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if delay < r.config.Backoff.Maximum {
			delay *= 2
			if delay > r.config.Backoff.Maximum {
				delay = r.config.Backoff.Maximum
			}
		}
	}
}

func (r *Runtime) runOnce(ctx context.Context) (bool, error) {
	sessionContext, cancelSession := context.WithCancel(ctx)
	defer cancelSession()
	stream, closer, err := r.connector.Connect(sessionContext)
	if err != nil {
		return false, err
	}
	defer closer.Close()

	hostname := r.config.Hostname
	if hostname == "" {
		hostname, err = os.Hostname()
		if err != nil {
			return false, fmt.Errorf("read hostname: %w", err)
		}
	}
	if err := stream.Send(&gatewaypb.ConnectRequest{
		Payload: &gatewaypb.ConnectRequest_Hello{Hello: &gatewaypb.AgentHello{
			AgentId:            r.agentID,
			RequestedNamespace: r.config.Namespace,
			Hostname:           hostname,
			AgentVersion:       r.agentVersion,
			ProtocolVersion:    &gatewaypb.ProtocolVersion{Major: 1},
			SchemaVersion:      hostschema.Version,
			MaxConcurrentScans: r.config.MaxConcurrentScans,
			Attributes:         cloneAttributes(r.config.Attributes),
		}},
	}); err != nil {
		return false, fmt.Errorf("send agent hello: %w", err)
	}

	first := make(chan receivedResponse, 1)
	go receiveOne(stream, first)
	timer := time.NewTimer(r.config.ConnectTimeout)
	defer timer.Stop()
	var response *gatewaypb.ConnectResponse
	select {
	case received := <-first:
		if received.err != nil {
			return false, fmt.Errorf("receive session acceptance: %w", received.err)
		}
		response = received.response
	case <-timer.C:
		return false, errors.New("Agent Gateway handshake timed out")
	case <-ctx.Done():
		return false, ctx.Err()
	}
	accepted := response.GetSessionAccepted()
	if err := validateAcceptedSession(accepted); err != nil {
		return false, err
	}
	r.logger.Info(
		"host agent connected",
		"namespace", accepted.GetNamespace(),
		"host_id", accepted.GetHostId(),
		"generation", accepted.GetGeneration(),
	)
	return true, r.runSession(sessionContext, stream, accepted)
}

type receivedResponse struct {
	response *gatewaypb.ConnectResponse
	err      error
}

func receiveOne(
	stream gatewaypb.AgentGatewayService_ConnectClient,
	result chan<- receivedResponse,
) {
	response, err := stream.Recv()
	result <- receivedResponse{response: response, err: err}
}

func validateAcceptedSession(accepted *gatewaypb.SessionAccepted) error {
	if accepted == nil {
		return status.Error(codes.FailedPrecondition, "Agent Gateway did not accept the session")
	}
	if strings.TrimSpace(accepted.GetSessionId()) == "" ||
		strings.TrimSpace(accepted.GetNamespace()) == "" ||
		strings.TrimSpace(accepted.GetHostId()) == "" ||
		accepted.GetGeneration() == 0 {
		return status.Error(codes.FailedPrecondition, "Agent Gateway returned an invalid session identity")
	}
	version := accepted.GetProtocolVersion()
	if version.GetMajor() != 1 || accepted.GetSchemaVersion() != hostschema.Version {
		return status.Error(codes.FailedPrecondition, "Agent Gateway returned incompatible protocol or schema")
	}
	if err := accepted.GetHeartbeatInterval().CheckValid(); err != nil ||
		accepted.GetHeartbeatInterval().AsDuration() <= 0 {
		return status.Error(codes.FailedPrecondition, "Agent Gateway returned an invalid heartbeat interval")
	}
	if err := accepted.GetLeaseDuration().CheckValid(); err != nil ||
		accepted.GetLeaseDuration().AsDuration() <= accepted.GetHeartbeatInterval().AsDuration() {
		return status.Error(codes.FailedPrecondition, "Agent Gateway returned an invalid lease duration")
	}
	return nil
}

func isPermanentGatewayError(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition,
		codes.PermissionDenied, codes.Unauthenticated:
		return true
	default:
		return false
	}
}

func jittered(base time.Duration, ratio float64, random float64) time.Duration {
	if ratio == 0 {
		return base
	}
	factor := 1 - ratio + 2*ratio*random
	return time.Duration(float64(base) * factor)
}

func waitForReconnect(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cloneAttributes(attributes map[string]string) map[string]string {
	cloned := make(map[string]string, len(attributes))
	for key, value := range attributes {
		cloned[key] = value
	}
	return cloned
}

type synchronizedSender struct {
	mu     sync.Mutex
	stream gatewaypb.AgentGatewayService_ConnectClient
}

func (s *synchronizedSender) Send(request *gatewaypb.ConnectRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(request)
}
