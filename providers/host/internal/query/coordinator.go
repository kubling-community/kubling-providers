package query

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	"github.com/kubling-community/kubling-providers/providers/host/internal/gateway"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
	"github.com/kubling-community/kubling-providers/providers/host/internal/registry"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultBatchSize          = 128
	defaultScanTimeout        = 10 * time.Second
	defaultConcurrentTargets  = 8
	defaultMaxRowsPerTarget   = 100000
	defaultMaxBytesPerTarget  = 32 << 20
	maximumPreferredBatchRows = 1024
)

// Config controls distributed scan behavior.
type Config struct {
	AllowPartialResults  bool
	AllowUnboundedFanout bool
	ScanTimeout          time.Duration
	PreferredBatchRows   uint32
	MaxConcurrentTargets int
	MaxRowsPerTarget     int
	MaxBytesPerTarget    int
}

// DefaultConfig returns bounded defaults for distributed host scans.
func DefaultConfig() Config {
	return Config{
		ScanTimeout:          defaultScanTimeout,
		PreferredBatchRows:   defaultBatchSize,
		MaxConcurrentTargets: defaultConcurrentTargets,
		MaxRowsPerTarget:     defaultMaxRowsPerTarget,
		MaxBytesPerTarget:    defaultMaxBytesPerTarget,
	}
}

// Coordinator owns routing and fan-out/fan-in without evaluating relational
// operations that belong to Kubling.
type Coordinator struct {
	registry  registry.Registry
	sessions  *gateway.Directory
	config    Config
	now       func() time.Time
	newScanID func() (string, error)
}

// New creates a query coordinator over one homogeneous host fleet.
func New(
	fleet registry.Registry,
	sessions *gateway.Directory,
	config Config,
) (*Coordinator, error) {
	return newCoordinator(fleet, sessions, config, time.Now, randomScanID)
}

func newCoordinator(
	fleet registry.Registry,
	sessions *gateway.Directory,
	config Config,
	now func() time.Time,
	newScanID func() (string, error),
) (*Coordinator, error) {
	if fleet == nil {
		return nil, errors.New("host query registry is required")
	}
	if sessions == nil {
		return nil, errors.New("host query session directory is required")
	}
	if config.ScanTimeout == 0 {
		config.ScanTimeout = defaultScanTimeout
	}
	if config.ScanTimeout < 0 {
		return nil, errors.New("host query scan timeout must be positive")
	}
	if config.PreferredBatchRows == 0 {
		config.PreferredBatchRows = defaultBatchSize
	}
	if config.PreferredBatchRows > maximumPreferredBatchRows {
		return nil, fmt.Errorf(
			"host query preferred batch rows must not exceed %d",
			maximumPreferredBatchRows,
		)
	}
	if config.MaxConcurrentTargets == 0 {
		config.MaxConcurrentTargets = defaultConcurrentTargets
	}
	if config.MaxConcurrentTargets < 0 {
		return nil, errors.New("host query concurrent target limit must be positive")
	}
	if config.MaxRowsPerTarget == 0 {
		config.MaxRowsPerTarget = defaultMaxRowsPerTarget
	}
	if config.MaxRowsPerTarget < 0 {
		return nil, errors.New("host query row limit per target must be positive")
	}
	if config.MaxBytesPerTarget == 0 {
		config.MaxBytesPerTarget = defaultMaxBytesPerTarget
	}
	if config.MaxBytesPerTarget < 0 {
		return nil, errors.New("host query byte limit per target must be positive")
	}
	if now == nil || newScanID == nil {
		return nil, errors.New("host query runtime integration is required")
	}
	return &Coordinator{
		registry:  fleet,
		sessions:  sessions,
		config:    config,
		now:       now,
		newScanID: newScanID,
	}, nil
}

// Query executes HOST locally or fans an agent-backed table scan out to the
// fixed active session snapshot selected at query start.
func (c *Coordinator) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	plan, err := buildPlan(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan host query: %v", err)
	}
	if request.GetAllowPartialResults() && !c.config.AllowPartialResults {
		return nil, status.Error(codes.FailedPrecondition, "partial host results are disabled")
	}
	if hostschema.RequiresArrayFeature(plan.table, plan.sourceColumns()) &&
		!slices.Contains(request.GetAcceptedFeatures(), grpcfeatures.ArrayValuesV1) {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"host query requires feature %q",
			grpcfeatures.ArrayValuesV1,
		)
	}
	switch plan.table {
	case hostschema.HostTable:
		return c.queryHosts(ctx, request, plan)
	default:
		if _, exists := hostschema.GatewayTable(plan.table); !exists {
			return nil, status.Errorf(codes.NotFound, "host table %q was not found", plan.table)
		}
		return c.queryAgents(ctx, request, plan)
	}
}

func (c *Coordinator) queryHosts(
	ctx context.Context,
	request *providerv1.QueryRequest,
	plan queryPlan,
) (providersdk.ResultStream, error) {
	if plan.route.impossible {
		return newStaticStream(plan.fields(), nil, queryBatchSize(request)), nil
	}
	namespace := ""
	if plan.route.namespace != nil {
		namespace = *plan.route.namespace
	}
	hosts, err := c.registry.List(ctx, namespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list registered hosts: %v", err)
	}
	tuples := make([]*providerv1.Tuple, 0, len(hosts))
	at := c.now().UTC()
	for _, host := range hosts {
		if plan.route.hostID != nil && host.Identity.Key.ID != *plan.route.hostID {
			continue
		}
		if plan.route.hostname != nil && host.Identity.Hostname != *plan.route.hostname {
			continue
		}
		values := hostValues(host, at)
		tuple, err := projectValues(plan, values)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "project HOST row: %v", err)
		}
		tuples = append(tuples, tuple)
	}
	return newStaticStream(plan.fields(), tuples, queryBatchSize(request)), nil
}

func (c *Coordinator) queryAgents(
	ctx context.Context,
	request *providerv1.QueryRequest,
	plan queryPlan,
) (providersdk.ResultStream, error) {
	if !plan.route.impossible && plan.route.unbounded() && !c.config.AllowUnboundedFanout {
		return nil, status.Error(
			codes.FailedPrecondition,
			"unbounded host fan-out is disabled; constrain namespace, host_id or hostname",
		)
	}
	var targets []model.HostSnapshot
	if !plan.route.impossible {
		namespace := ""
		if plan.route.namespace != nil {
			namespace = *plan.route.namespace
		}
		var err error
		targets, err = c.registry.SnapshotActive(ctx, namespace, c.now().UTC())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "snapshot active hosts: %v", err)
		}
		if plan.route.hostID != nil || plan.route.hostname != nil {
			filtered := targets[:0]
			for _, target := range targets {
				if plan.route.hostID != nil && target.Identity.Key.ID != *plan.route.hostID {
					continue
				}
				if plan.route.hostname != nil && target.Identity.Hostname != *plan.route.hostname {
					continue
				}
				filtered = append(filtered, target)
			}
			targets = filtered
		}
	}

	return newDistributedStream(
		ctx,
		request,
		plan,
		targets,
		c,
		c.newScanID,
	), nil
}

func (c *Coordinator) dispatchScan(
	ctx context.Context,
	table string,
	target model.HostSnapshot,
	newScanID func() (string, error),
) (*gateway.Scan, error) {
	if target.Session == nil {
		return nil, gateway.ErrSessionClosed
	}
	session, exists := c.sessions.Resolve(target.Session.ID)
	if !exists || session.Snapshot().Session.Generation != target.Session.Generation {
		return nil, gateway.ErrSessionClosed
	}
	scanID, err := newScanID()
	if err != nil {
		return nil, fmt.Errorf("generate host scan ID: %w", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("host scan deadline is required")
	}
	gatewayTable, exists := hostschema.GatewayTable(table)
	if !exists {
		return nil, fmt.Errorf("host table %q is not agent-backed", table)
	}
	columns, exists := hostschema.LocalColumns(table)
	if !exists {
		return nil, fmt.Errorf("host table %q has no local scan columns", table)
	}
	return session.StartScan(&gatewaypb.ScanRequest{
		ScanId:             scanID,
		Table:              gatewayTable,
		Columns:            columns,
		Deadline:           timestamppb.New(deadline),
		PreferredBatchRows: c.config.PreferredBatchRows,
	})
}

func randomScanID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func queryBatchSize(request *providerv1.QueryRequest) int {
	if request.GetBatchSize() == 0 {
		return defaultBatchSize
	}
	return int(request.GetBatchSize())
}

func targetName(key model.HostKey) string {
	return key.Namespace + "/" + key.ID
}

func diagnosticTarget(key model.HostKey, local string) string {
	target := targetName(key)
	if strings.TrimSpace(local) != "" {
		target += "/" + strings.TrimSpace(local)
	}
	return target
}
