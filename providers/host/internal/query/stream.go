package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/host/internal/gateway"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type gatewayRow struct {
	values []*kublingv1.Value
}

type targetEvent struct {
	tuples   []*providerv1.Tuple
	warnings []*providerv1.QueryWarning
	failure  *providerv1.QueryWarning
}

type distributedStream struct {
	mu sync.Mutex

	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	events        <-chan targetEvent
	fields        []*providerv1.Field
	batchSize     int
	remaining     int
	pending       []*providerv1.Tuple
	warnings      []*providerv1.QueryWarning
	partial       bool
	outcome       *providerv1.QueryOutcome
	terminalErr   error
	closed        bool
	acceptOutcome bool
	allowPartial  bool
}

func newDistributedStream(
	parent context.Context,
	request *providerv1.QueryRequest,
	plan queryPlan,
	targets []model.HostSnapshot,
	coordinator *Coordinator,
	newScanID func() (string, error),
) *distributedStream {
	ctx, cancel := context.WithCancel(parent)
	events := make(chan targetEvent)
	jobs := make(chan model.HostSnapshot)
	done := make(chan struct{})
	stream := &distributedStream{
		ctx:           ctx,
		cancel:        cancel,
		done:          done,
		events:        events,
		fields:        plan.fields(),
		batchSize:     queryBatchSize(request),
		remaining:     len(targets),
		acceptOutcome: request.GetAcceptOutcome(),
		allowPartial:  request.GetAllowPartialResults(),
	}
	workerCount := coordinator.config.MaxConcurrentTargets
	if workerCount > len(targets) {
		workerCount = len(targets)
	}
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for target := range jobs {
				event := coordinator.scanTarget(ctx, plan, target, newScanID)
				select {
				case events <- event:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, target := range targets {
			select {
			case jobs <- target.Clone():
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(events)
		close(done)
	}()
	return stream
}

func (c *Coordinator) scanTarget(
	parent context.Context,
	plan queryPlan,
	target model.HostSnapshot,
	newScanID func() (string, error),
) targetEvent {
	event := targetEvent{}
	ctx, cancel := context.WithTimeout(parent, c.config.ScanTimeout)
	defer cancel()
	scan, err := c.dispatchScan(ctx, plan.table, target, newScanID)
	if err != nil {
		event.failure = dispatchFailure(target.Identity.Key, err)
		return event
	}
	completed := false
	defer func() {
		if !completed {
			reason := gatewaypb.CancelReason_CANCEL_REASON_CLIENT_CANCELLED
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				reason = gatewaypb.CancelReason_CANCEL_REASON_DEADLINE_EXCEEDED
			}
			_ = scan.Cancel(reason)
		}
	}()

	localColumns, exists := hostschema.LocalColumns(plan.table)
	if !exists {
		event.failure = targetFailure(
			target.Identity.Key,
			"INVALID_HOST_RESULT",
			fmt.Sprintf("host table %q has no local scan columns", plan.table),
			false,
		)
		return event
	}
	var rows []*gatewayRow
	var bufferedBytes int
	for {
		scanEvent, err := scan.Next(ctx)
		if err != nil {
			event.failure = scanFailure(target.Identity.Key, err)
			return event
		}
		if scanEvent.Batch != nil {
			if len(rows)+len(scanEvent.Batch.GetRows()) > c.config.MaxRowsPerTarget {
				event.failure = targetFailure(
					target.Identity.Key,
					"HOST_RESULT_LIMIT_EXCEEDED",
					fmt.Sprintf("host scan exceeded the per-target row limit of %d", c.config.MaxRowsPerTarget),
					false,
				)
				return event
			}
			for _, row := range scanEvent.Batch.GetRows() {
				if row == nil {
					event.failure = targetFailure(target.Identity.Key, "INVALID_HOST_RESULT", "agent returned a nil host row", false)
					return event
				}
				bufferedBytes += proto.Size(row)
				if bufferedBytes > c.config.MaxBytesPerTarget {
					event.failure = targetFailure(
						target.Identity.Key,
						"HOST_RESULT_LIMIT_EXCEEDED",
						fmt.Sprintf("host scan exceeded the per-target byte limit of %d", c.config.MaxBytesPerTarget),
						false,
					)
					return event
				}
				rows = append(rows, &gatewayRow{values: row.GetValues()})
			}
			continue
		}
		finished := scanEvent.Finished
		if finished == nil {
			event.failure = targetFailure(target.Identity.Key, "INVALID_HOST_RESULT", "agent returned an empty scan event", false)
			return event
		}
		completed = true
		switch finished.GetStatus() {
		case gatewaypb.ScanStatus_SCAN_STATUS_SUCCEEDED:
			if hostschema.SingletonPerHost(plan.table) && len(rows) != 1 {
				event.failure = targetFailure(
					target.Identity.Key,
					"INVALID_HOST_RESULT",
					fmt.Sprintf("agent returned %d %s rows, want 1", len(rows), plan.table),
					false,
				)
				return event
			}
			event.tuples = make([]*providerv1.Tuple, 0, len(rows))
			for _, row := range rows {
				values, err := agentValues(plan.table, target.Identity, localColumns, row)
				if err != nil {
					event.failure = targetFailure(target.Identity.Key, "INVALID_HOST_RESULT", err.Error(), false)
					return event
				}
				tuple, err := projectValues(plan, values)
				if err != nil {
					event.failure = targetFailure(target.Identity.Key, "INVALID_HOST_RESULT", err.Error(), false)
					return event
				}
				event.tuples = append(event.tuples, tuple)
			}
			for _, diagnostic := range finished.GetDiagnostics() {
				event.warnings = append(event.warnings, queryWarning(
					target.Identity.Key,
					diagnostic,
					providerv1.QueryDiagnosticRole_QUERY_DIAGNOSTIC_ROLE_WARNING,
				))
			}
			return event
		case gatewaypb.ScanStatus_SCAN_STATUS_FAILED,
			gatewaypb.ScanStatus_SCAN_STATUS_CANCELLED:
			event.failure = failureFromCompletion(target.Identity.Key, finished)
			return event
		default:
			event.failure = targetFailure(target.Identity.Key, "INVALID_HOST_RESULT", "agent returned an unspecified scan status", false)
			return event
		}
	}
}

func dispatchFailure(key model.HostKey, err error) *providerv1.QueryWarning {
	switch {
	case errors.Is(err, gateway.ErrSessionBusy):
		return targetFailure(key, "HOST_TARGET_BUSY", err.Error(), true)
	case errors.Is(err, gateway.ErrSessionClosed):
		return targetFailure(key, "HOST_TARGET_DISCONNECTED", err.Error(), true)
	default:
		return targetFailure(key, "HOST_TARGET_UNAVAILABLE", err.Error(), true)
	}
}

func scanFailure(key model.HostKey, err error) *providerv1.QueryWarning {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return targetFailure(key, "HOST_SCAN_TIMEOUT", "host scan exceeded its deadline", true)
	case errors.Is(err, gateway.ErrSessionClosed):
		return targetFailure(key, "HOST_TARGET_DISCONNECTED", err.Error(), true)
	case errors.Is(err, gateway.ErrScanRetired):
		return targetFailure(key, "HOST_SCAN_CANCELLED", err.Error(), true)
	default:
		return targetFailure(key, "HOST_SCAN_INTERRUPTED", err.Error(), true)
	}
}

func (s *distributedStream) Next(ctx context.Context) (*providerv1.TupleBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, io.EOF
	}
	if s.terminalErr != nil {
		return nil, s.terminalErr
	}
	for {
		if len(s.pending) > 0 {
			count := s.batchSize
			if count > len(s.pending) {
				count = len(s.pending)
			}
			tuples := append([]*providerv1.Tuple(nil), s.pending[:count]...)
			s.pending = s.pending[count:]
			return &providerv1.TupleBatch{Fields: s.fields, Tuples: tuples}, nil
		}
		if s.remaining == 0 {
			s.finishOutcome()
			return nil, io.EOF
		}

		s.mu.Unlock()
		var event targetEvent
		var ok bool
		select {
		case event, ok = <-s.events:
		case <-ctx.Done():
			s.mu.Lock()
			return nil, ctx.Err()
		case <-s.ctx.Done():
			s.mu.Lock()
			return nil, s.ctx.Err()
		}
		s.mu.Lock()
		if !ok {
			s.remaining = 0
			continue
		}
		s.remaining--
		if event.failure != nil {
			if !s.allowPartial {
				s.terminalErr = status.Errorf(
					codes.Unavailable,
					"host target %q failed: %s",
					event.failure.GetTarget(),
					event.failure.GetMessage(),
				)
				s.cancel()
				return nil, s.terminalErr
			}
			s.partial = true
			s.warnings = append(s.warnings, event.failure)
		}
		if len(event.warnings) > 0 {
			if !s.acceptOutcome {
				s.terminalErr = status.Error(codes.FailedPrecondition, "host query produced warnings without outcome acceptance")
				s.cancel()
				return nil, s.terminalErr
			}
			s.warnings = append(s.warnings, event.warnings...)
		}
		s.pending = append(s.pending, event.tuples...)
	}
}

func (s *distributedStream) finishOutcome() {
	if s.outcome != nil {
		return
	}
	sort.Slice(s.warnings, func(i, j int) bool {
		left := s.warnings[i]
		right := s.warnings[j]
		if left.GetTarget() == right.GetTarget() {
			if left.GetCode() == right.GetCode() {
				return left.GetMessage() < right.GetMessage()
			}
			return left.GetCode() < right.GetCode()
		}
		return left.GetTarget() < right.GetTarget()
	})
	completion := providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE
	if s.partial {
		completion = providerv1.QueryCompletion_QUERY_COMPLETION_PARTIAL
	}
	s.outcome = &providerv1.QueryOutcome{
		Completion: completion,
		Warnings:   append([]*providerv1.QueryWarning(nil), s.warnings...),
	}
}

func (s *distributedStream) Outcome() *providerv1.QueryOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.outcome == nil {
		return nil
	}
	return proto.Clone(s.outcome).(*providerv1.QueryOutcome)
}

func (s *distributedStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	<-s.done
	return nil
}

type staticStream struct {
	mu        sync.Mutex
	fields    []*providerv1.Field
	tuples    []*providerv1.Tuple
	batchSize int
	index     int
	closed    bool
	outcome   *providerv1.QueryOutcome
}

func newStaticStream(
	fields []*providerv1.Field,
	tuples []*providerv1.Tuple,
	batchSize int,
) *staticStream {
	return &staticStream{
		fields:    fields,
		tuples:    tuples,
		batchSize: batchSize,
		outcome: &providerv1.QueryOutcome{
			Completion: providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE,
		},
	}
}

func (s *staticStream) Next(ctx context.Context) (*providerv1.TupleBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.index >= len(s.tuples) {
		return nil, io.EOF
	}
	end := s.index + s.batchSize
	if end > len(s.tuples) {
		end = len(s.tuples)
	}
	batch := &providerv1.TupleBatch{
		Fields: s.fields,
		Tuples: append([]*providerv1.Tuple(nil), s.tuples[s.index:end]...),
	}
	s.index = end
	return batch, nil
}

func (s *staticStream) Outcome() *providerv1.QueryOutcome {
	return proto.Clone(s.outcome).(*providerv1.QueryOutcome)
}

func (s *staticStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.tuples = nil
	s.mu.Unlock()
	return nil
}

func targetFailure(
	key model.HostKey,
	code string,
	message string,
	retryable bool,
) *providerv1.QueryWarning {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "host target did not complete the scan"
	}
	return &providerv1.QueryWarning{
		Code:      code,
		Message:   message,
		Target:    targetName(key),
		Retryable: retryable,
		Role:      providerv1.QueryDiagnosticRole_QUERY_DIAGNOSTIC_ROLE_PARTIAL_RESULT_CAUSE,
	}
}

func failureFromCompletion(
	key model.HostKey,
	finished *gatewaypb.ScanFinished,
) *providerv1.QueryWarning {
	if diagnostic := firstDiagnostic(finished.GetDiagnostics()); diagnostic != nil {
		return queryWarning(
			key,
			diagnostic,
			providerv1.QueryDiagnosticRole_QUERY_DIAGNOSTIC_ROLE_PARTIAL_RESULT_CAUSE,
		)
	}
	code := "HOST_SCAN_FAILED"
	message := "agent failed the host scan"
	retryable := true
	if finished.GetStatus() == gatewaypb.ScanStatus_SCAN_STATUS_CANCELLED {
		code = "HOST_SCAN_CANCELLED"
		message = "agent cancelled the host scan"
	}
	return targetFailure(key, code, message, retryable)
}

func firstDiagnostic(diagnostics []*gatewaypb.ScanDiagnostic) *gatewaypb.ScanDiagnostic {
	for _, diagnostic := range diagnostics {
		if diagnostic != nil && strings.TrimSpace(diagnostic.GetMessage()) != "" {
			return diagnostic
		}
	}
	return nil
}

func queryWarning(
	key model.HostKey,
	diagnostic *gatewaypb.ScanDiagnostic,
	role providerv1.QueryDiagnosticRole,
) *providerv1.QueryWarning {
	if diagnostic == nil {
		return &providerv1.QueryWarning{
			Message: "agent returned an empty diagnostic",
			Target:  targetName(key),
			Role:    role,
		}
	}
	message := strings.TrimSpace(diagnostic.GetMessage())
	if message == "" {
		message = "agent reported a host scan diagnostic"
	}
	return &providerv1.QueryWarning{
		Code:      strings.TrimSpace(diagnostic.GetCode()),
		Message:   message,
		Target:    diagnosticTarget(key, diagnostic.GetTarget()),
		Retryable: diagnostic.GetRetryable(),
		Role:      role,
	}
}

var (
	_ providersdk.ResultStream       = (*staticStream)(nil)
	_ providersdk.QueryOutcomeStream = (*staticStream)(nil)
	_ providersdk.ResultStream       = (*distributedStream)(nil)
	_ providersdk.QueryOutcomeStream = (*distributedStream)(nil)
)
