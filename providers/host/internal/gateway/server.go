package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
	"github.com/kubling-community/kubling-providers/providers/host/internal/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const CurrentSchemaVersion = hostschema.Version

var currentProtocolVersion = model.ProtocolVersion{Major: 1, Minor: 0}

// Config controls protocol compatibility and liveness intervals.
type Config struct {
	ProtocolVersion   model.ProtocolVersion
	SchemaVersion     string
	HeartbeatInterval time.Duration
	LeaseDuration     time.Duration
}

// DefaultConfig returns the initial Agent Gateway compatibility policy.
func DefaultConfig() Config {
	return Config{
		ProtocolVersion:   currentProtocolVersion,
		SchemaVersion:     CurrentSchemaVersion,
		HeartbeatInterval: 15 * time.Second,
		LeaseDuration:     45 * time.Second,
	}
}

// Authorizer resolves the namespace an agent may join. Production
// implementations authenticate the transport context and must not trust
// requestedNamespace by itself.
type Authorizer interface {
	Authorize(context.Context, string) (string, error)
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(context.Context, string) (string, error)

func (f AuthorizerFunc) Authorize(
	ctx context.Context,
	requestedNamespace string,
) (string, error) {
	return f(ctx, requestedNamespace)
}

// Server implements the host-specific Agent Gateway service.
type Server struct {
	gatewaypb.UnimplementedAgentGatewayServiceServer

	config     Config
	registry   registry.Registry
	directory  *Directory
	authorizer Authorizer
	now        func() time.Time
}

// NewServer creates an Agent Gateway server.
func NewServer(
	config Config,
	registry registry.Registry,
	directory *Directory,
	authorizer Authorizer,
) (*Server, error) {
	return newServer(config, registry, directory, authorizer, time.Now)
}

func newServer(
	config Config,
	registry registry.Registry,
	directory *Directory,
	authorizer Authorizer,
	now func() time.Time,
) (*Server, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, errors.New("agent registry is required")
	}
	if directory == nil {
		return nil, errors.New("agent session directory is required")
	}
	if authorizer == nil {
		return nil, errors.New("agent namespace authorizer is required")
	}
	if now == nil {
		return nil, errors.New("agent gateway clock is required")
	}
	return &Server{
		config:     config,
		registry:   registry,
		directory:  directory,
		authorizer: authorizer,
		now:        now,
	}, nil
}

// Connect establishes and maintains one authenticated agent session.
func (s *Server) Connect(
	stream gatewaypb.AgentGatewayService_ConnectServer,
) (returnErr error) {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		return receiveError(ctx, err, "receive agent hello")
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "first agent message must be hello")
	}
	if err := s.validateHello(hello); err != nil {
		return err
	}
	namespace, err := s.authorizer.Authorize(ctx, hello.GetRequestedNamespace())
	if err != nil {
		return authorizationError(ctx, err)
	}
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(namespace) != namespace {
		return status.Error(codes.Internal, "agent authorizer returned an invalid namespace")
	}

	observedAt := s.now().UTC()
	window := registry.SessionWindow{
		ObservedAt: observedAt,
		ExpiresAt:  observedAt.Add(s.config.LeaseDuration),
	}
	snapshot, err := s.registry.OpenSession(
		ctx,
		registry.Registration{
			Namespace:          namespace,
			AgentIdentifier:    hello.GetAgentId(),
			Hostname:           hello.GetHostname(),
			AgentVersion:       hello.GetAgentVersion(),
			ProtocolVersion:    protocolVersion(hello.GetProtocolVersion()),
			SchemaVersion:      hello.GetSchemaVersion(),
			MaxConcurrentScans: hello.GetMaxConcurrentScans(),
			Attributes:         hello.GetAttributes(),
		},
		window,
	)
	if err != nil {
		return status.Errorf(codes.Internal, "open agent session: %v", err)
	}
	binding, err := s.directory.Bind(snapshot, stream)
	if err != nil {
		_ = s.registry.CloseSession(context.Background(), snapshot.Session.ID, s.now().UTC())
		return status.Errorf(codes.Internal, "bind agent session: %v", err)
	}
	defer func() {
		s.directory.Unbind(binding)
		closeErr := s.registry.CloseSession(
			context.Background(),
			snapshot.Session.ID,
			s.now().UTC(),
		)
		if returnErr == nil && closeErr != nil &&
			!errors.Is(closeErr, registry.ErrSessionNotFound) {
			returnErr = status.Errorf(codes.Internal, "close agent session: %v", closeErr)
		}
	}()

	if err := binding.Send(&gatewaypb.ConnectResponse{
		Payload: &gatewaypb.ConnectResponse_SessionAccepted{
			SessionAccepted: &gatewaypb.SessionAccepted{
				SessionId:         snapshot.Session.ID,
				Generation:        snapshot.Session.Generation,
				Namespace:         snapshot.Identity.Key.Namespace,
				HostId:            snapshot.Identity.Key.ID,
				ProtocolVersion:   gatewayProtocolVersion(s.config.ProtocolVersion),
				SchemaVersion:     s.config.SchemaVersion,
				HeartbeatInterval: durationpb.New(s.config.HeartbeatInterval),
				LeaseDuration:     durationpb.New(s.config.LeaseDuration),
			},
		},
	}); err != nil {
		return err
	}

	received := make(chan receivedRequest, 1)
	go receiveRequests(ctx, stream, received)
	leaseTimer := time.NewTimer(s.config.LeaseDuration)
	defer leaseTimer.Stop()

	var lastHeartbeat uint64
	var receivedHeartbeat bool
	for {
		var request *gatewaypb.ConnectRequest
		select {
		case <-binding.Done():
			return status.Error(codes.Aborted, "agent session was superseded")
		case <-leaseTimer.C:
			return status.Error(codes.DeadlineExceeded, "agent session lease expired")
		case result := <-received:
			if result.err != nil {
				return receiveError(ctx, result.err, "receive agent message")
			}
			request = result.request
		}
		if !s.directory.IsCurrent(binding) {
			return status.Error(codes.Aborted, "agent session was superseded")
		}

		switch payload := request.GetPayload().(type) {
		case *gatewaypb.ConnectRequest_Heartbeat:
			sequence := payload.Heartbeat.GetSequence()
			if receivedHeartbeat && sequence <= lastHeartbeat {
				return status.Error(codes.InvalidArgument, "heartbeat sequence must increase")
			}
			receivedHeartbeat = true
			lastHeartbeat = sequence
			observedAt = s.now().UTC()
			window = registry.SessionWindow{
				ObservedAt: observedAt,
				ExpiresAt:  observedAt.Add(s.config.LeaseDuration),
			}
			renewed, renewErr := s.registry.RenewSession(
				ctx,
				snapshot.Session.ID,
				window,
			)
			if renewErr != nil {
				return sessionError(ctx, renewErr)
			}
			if err := binding.update(renewed); err != nil {
				return status.Errorf(codes.Internal, "update agent binding: %v", err)
			}
			resetLeaseTimer(leaseTimer, s.config.LeaseDuration)
			if err := binding.Send(&gatewaypb.ConnectResponse{
				Payload: &gatewaypb.ConnectResponse_HeartbeatAcknowledged{
					HeartbeatAcknowledged: &gatewaypb.HeartbeatAcknowledged{
						Sequence:       sequence,
						LeaseExpiresAt: timestamppb.New(window.ExpiresAt),
					},
				},
			}); err != nil {
				return err
			}
		case *gatewaypb.ConnectRequest_Hello:
			return status.Error(codes.InvalidArgument, "agent hello may only be sent once")
		case *gatewaypb.ConnectRequest_ScanBatch:
			if err := binding.deliverBatch(ctx, payload.ScanBatch); err != nil {
				if errors.Is(err, ErrScanRetired) {
					continue
				}
				return scanResponseError(ctx, err)
			}
		case *gatewaypb.ConnectRequest_ScanFinished:
			if err := binding.finishScan(ctx, payload.ScanFinished); err != nil {
				if errors.Is(err, ErrScanRetired) {
					continue
				}
				return scanResponseError(ctx, err)
			}
		case nil:
			return status.Error(codes.InvalidArgument, "agent message payload is required")
		default:
			return status.Error(codes.InvalidArgument, "agent message payload is unknown")
		}
	}
}

func resetLeaseTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

type receivedRequest struct {
	request *gatewaypb.ConnectRequest
	err     error
}

func receiveRequests(
	ctx context.Context,
	stream gatewaypb.AgentGatewayService_ConnectServer,
	received chan<- receivedRequest,
) {
	for {
		request, err := stream.Recv()
		select {
		case received <- receivedRequest{request: request, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func (s *Server) validateHello(hello *gatewaypb.AgentHello) error {
	if strings.TrimSpace(hello.GetAgentId()) == "" ||
		strings.TrimSpace(hello.GetAgentId()) != hello.GetAgentId() {
		return status.Error(codes.InvalidArgument, "agent ID is required without surrounding whitespace")
	}
	if strings.TrimSpace(hello.GetRequestedNamespace()) == "" ||
		strings.TrimSpace(hello.GetRequestedNamespace()) != hello.GetRequestedNamespace() {
		return status.Error(codes.InvalidArgument, "requested namespace is required without surrounding whitespace")
	}
	if strings.TrimSpace(hello.GetAgentVersion()) == "" {
		return status.Error(codes.InvalidArgument, "agent version is required")
	}
	version := protocolVersion(hello.GetProtocolVersion())
	if version.Major != s.config.ProtocolVersion.Major ||
		version.Minor < s.config.ProtocolVersion.Minor {
		return status.Errorf(
			codes.FailedPrecondition,
			"agent gateway protocol %d.%d is incompatible",
			version.Major,
			version.Minor,
		)
	}
	if hello.GetSchemaVersion() != s.config.SchemaVersion {
		return status.Errorf(
			codes.FailedPrecondition,
			"agent schema %q is incompatible",
			hello.GetSchemaVersion(),
		)
	}
	if hello.GetMaxConcurrentScans() == 0 {
		return status.Error(codes.InvalidArgument, "max concurrent scans must be positive")
	}
	return nil
}

func validateConfig(config Config) error {
	if config.ProtocolVersion.Major == 0 {
		return errors.New("agent gateway protocol major version is required")
	}
	if strings.TrimSpace(config.SchemaVersion) == "" {
		return errors.New("agent schema version is required")
	}
	if config.HeartbeatInterval <= 0 {
		return errors.New("agent heartbeat interval must be positive")
	}
	if config.LeaseDuration <= config.HeartbeatInterval {
		return errors.New("agent lease duration must exceed heartbeat interval")
	}
	return nil
}

func protocolVersion(version *gatewaypb.ProtocolVersion) model.ProtocolVersion {
	if version == nil {
		return model.ProtocolVersion{}
	}
	return model.ProtocolVersion{Major: version.GetMajor(), Minor: version.GetMinor()}
}

func gatewayProtocolVersion(version model.ProtocolVersion) *gatewaypb.ProtocolVersion {
	return &gatewaypb.ProtocolVersion{Major: version.Major, Minor: version.Minor}
}

func authorizationError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	if code := status.Code(err); code != codes.Unknown {
		return err
	}
	return status.Error(codes.PermissionDenied, "agent namespace authorization failed")
}

func sessionError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	switch {
	case errors.Is(err, registry.ErrSessionNotFound):
		return status.Error(codes.FailedPrecondition, "agent session does not exist")
	case errors.Is(err, registry.ErrSessionNotCurrent):
		return status.Error(codes.Aborted, "agent session was superseded")
	case errors.Is(err, registry.ErrSessionExpired):
		return status.Error(codes.DeadlineExceeded, "agent session lease expired")
	default:
		return status.Errorf(codes.Internal, "renew agent session: %v", err)
	}
}

func scanResponseError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	switch {
	case errors.Is(err, ErrScanNotActive):
		return status.Error(codes.FailedPrecondition, "agent scan is not active")
	case errors.Is(err, ErrSessionClosed):
		return status.Error(codes.Aborted, "agent session was superseded")
	default:
		return status.Errorf(codes.InvalidArgument, "invalid agent scan response: %v", err)
	}
}

func receiveError(ctx context.Context, err error, operation string) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	return fmt.Errorf("%s: %w", operation, err)
}

var _ gatewaypb.AgentGatewayServiceServer = (*Server)(nil)
