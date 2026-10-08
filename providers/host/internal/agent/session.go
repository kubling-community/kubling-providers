package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/kubling-community/kubling-providers/providers/host/internal/agent/collector"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var errScanRowTooLarge = errors.New("encoded host row exceeds the agent batch limit")

const (
	defaultPreferredBatchRows = 128
	maximumScanBatchRows      = 1024
	maximumScanBatchBytes     = 2 << 20
)

type activeScan struct {
	cancel context.CancelFunc
}

func (r *Runtime) runSession(
	ctx context.Context,
	stream gatewaypb.AgentGatewayService_ConnectClient,
	accepted *gatewaypb.SessionAccepted,
) error {
	sender := &synchronizedSender{stream: stream}
	responses := make(chan receivedResponse, 1)
	go receiveResponses(ctx, stream, responses)
	heartbeats := time.NewTicker(accepted.GetHeartbeatInterval().AsDuration())
	defer heartbeats.Stop()
	leaseWatchdog := time.NewTimer(accepted.GetLeaseDuration().AsDuration())
	defer leaseWatchdog.Stop()

	scanErrors := make(chan error, r.config.MaxConcurrentScans)
	semaphore := make(chan struct{}, r.config.MaxConcurrentScans)
	active := make(map[string]activeScan)
	var activeMu sync.Mutex
	var scans sync.WaitGroup
	cancelAll := func() {
		activeMu.Lock()
		for _, scan := range active {
			scan.cancel()
		}
		activeMu.Unlock()
	}
	defer func() {
		cancelAll()
		scans.Wait()
		_ = stream.CloseSend()
	}()

	var heartbeatSequence uint64
	var acknowledgedSequence uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeats.C:
			heartbeatSequence++
			if err := sender.Send(&gatewaypb.ConnectRequest{
				Payload: &gatewaypb.ConnectRequest_Heartbeat{
					Heartbeat: &gatewaypb.Heartbeat{Sequence: heartbeatSequence},
				},
			}); err != nil {
				return fmt.Errorf("send agent heartbeat: %w", err)
			}
		case <-leaseWatchdog.C:
			return status.Error(codes.Unavailable, "Agent Gateway heartbeat lease expired")
		case received := <-responses:
			if received.err != nil {
				if errors.Is(received.err, io.EOF) {
					return errors.New("Agent Gateway closed the session")
				}
				return fmt.Errorf("receive Agent Gateway message: %w", received.err)
			}
			switch payload := received.response.GetPayload().(type) {
			case *gatewaypb.ConnectResponse_HeartbeatAcknowledged:
				sequence := payload.HeartbeatAcknowledged.GetSequence()
				if sequence <= acknowledgedSequence || sequence > heartbeatSequence {
					return status.Error(codes.InvalidArgument, "Agent Gateway acknowledged an invalid heartbeat sequence")
				}
				acknowledgedSequence = sequence
				resetTimer(leaseWatchdog, accepted.GetLeaseDuration().AsDuration())
			case *gatewaypb.ConnectResponse_ScanRequest:
				request := payload.ScanRequest
				if request == nil || request.GetScanId() == "" {
					return status.Error(codes.InvalidArgument, "Agent Gateway sent an invalid scan request")
				}
				activeMu.Lock()
				_, duplicate := active[request.GetScanId()]
				activeMu.Unlock()
				if duplicate {
					return status.Error(codes.InvalidArgument, "Agent Gateway reused an active scan ID")
				}
				select {
				case semaphore <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				default:
					if err := sendFailedScan(
						sender,
						request.GetScanId(),
						"AGENT_SCAN_LIMIT",
						"agent scan concurrency limit reached",
						true,
					); err != nil {
						return err
					}
					continue
				}
				scanContext, cancel := scanContext(ctx, request)
				activeMu.Lock()
				active[request.GetScanId()] = activeScan{cancel: cancel}
				activeMu.Unlock()
				scans.Add(1)
				go func() {
					defer scans.Done()
					defer func() { <-semaphore }()
					defer cancel()
					err := r.executeScan(scanContext, sender, request)
					activeMu.Lock()
					delete(active, request.GetScanId())
					activeMu.Unlock()
					if err != nil {
						select {
						case scanErrors <- err:
						case <-ctx.Done():
						}
					}
				}()
			case *gatewaypb.ConnectResponse_CancelScan:
				activeMu.Lock()
				scan, exists := active[payload.CancelScan.GetScanId()]
				activeMu.Unlock()
				if exists {
					scan.cancel()
				}
			case *gatewaypb.ConnectResponse_SessionAccepted:
				return status.Error(codes.InvalidArgument, "Agent Gateway accepted the session more than once")
			case nil:
				return status.Error(codes.InvalidArgument, "Agent Gateway message payload is required")
			default:
				return status.Error(codes.InvalidArgument, "Agent Gateway message payload is unknown")
			}
		case err := <-scanErrors:
			return err
		}
	}
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

func receiveResponses(
	ctx context.Context,
	stream gatewaypb.AgentGatewayService_ConnectClient,
	responses chan<- receivedResponse,
) {
	for {
		response, err := stream.Recv()
		select {
		case responses <- receivedResponse{response: response, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func scanContext(
	parent context.Context,
	request *gatewaypb.ScanRequest,
) (context.Context, context.CancelFunc) {
	deadline := request.GetDeadline()
	if deadline == nil || deadline.CheckValid() != nil {
		return context.WithCancel(parent)
	}
	return context.WithDeadline(parent, deadline.AsTime())
}

func (r *Runtime) executeScan(
	ctx context.Context,
	sender *synchronizedSender,
	request *gatewaypb.ScanRequest,
) error {
	table, supported := hostschema.TableName(request.GetTable())
	if !supported {
		return sendFailedScan(
			sender,
			request.GetScanId(),
			"UNSUPPORTED_HOST_TABLE",
			"agent does not implement the requested host table",
			false,
		)
	}
	if request.GetDeadline() == nil || request.GetDeadline().CheckValid() != nil {
		return sendFailedScan(
			sender,
			request.GetScanId(),
			"INVALID_SCAN_DEADLINE",
			"scan deadline is required",
			false,
		)
	}
	result, collectionCode, err := r.collectScan(ctx, request.GetTable(), request.GetColumns())
	if err != nil {
		if ctx.Err() != nil {
			return sendCancelledScan(sender, request.GetScanId(), ctx.Err())
		}
		if collectionCode == "" {
			return sendFailedScan(
				sender,
				request.GetScanId(),
				"INVALID_HOST_SCAN",
				err.Error(),
				false,
			)
		}
		return sendFailedScan(
			sender,
			request.GetScanId(),
			collectionCode,
			err.Error(),
			true,
		)
	}
	if ctx.Err() != nil {
		return sendCancelledScan(sender, request.GetScanId(), ctx.Err())
	}
	if err := sendScanRows(ctx, sender, request, result.rows); err != nil {
		if ctx.Err() != nil {
			return sendCancelledScan(sender, request.GetScanId(), ctx.Err())
		}
		if errors.Is(err, errScanRowTooLarge) {
			return sendFailedScan(
				sender,
				request.GetScanId(),
				"HOST_ROW_TOO_LARGE",
				err.Error(),
				false,
			)
		}
		return fmt.Errorf("send %s scan rows: %w", table, err)
	}
	diagnostics := make([]*gatewaypb.ScanDiagnostic, 0, len(result.warnings))
	for _, warning := range result.warnings {
		diagnostics = append(diagnostics, &gatewaypb.ScanDiagnostic{
			Code:      warning.Code,
			Message:   warning.Message,
			Target:    warning.Target,
			Retryable: warning.Retryable,
		})
	}
	return sendScanFinished(
		sender,
		request.GetScanId(),
		gatewaypb.ScanStatus_SCAN_STATUS_SUCCEEDED,
		diagnostics,
	)
}

type collectedScan struct {
	rows     []*providerv1.Tuple
	warnings []collector.Warning
}

func (r *Runtime) collectScan(
	ctx context.Context,
	table gatewaypb.HostTable,
	columns []string,
) (collectedScan, string, error) {
	switch table {
	case gatewaypb.HostTable_HOST_TABLE_FACTS:
		observation, err := r.collector.Host(ctx)
		if err != nil {
			return collectedScan{}, "HOST_FACTS_COLLECTION_FAILED", fmt.Errorf("host facts collection failed: %w", err)
		}
		rows, err := hostschema.EncodeHostFactsRows(observation, columns)
		return collectedScan{rows: rows, warnings: observation.Warnings}, "", err
	case gatewaypb.HostTable_HOST_TABLE_CPU_INFO:
		observation, err := r.collector.CPUInfo(ctx)
		if err != nil {
			return collectedScan{}, "CPU_INFO_COLLECTION_FAILED", fmt.Errorf("CPU information collection failed: %w", err)
		}
		rows, err := hostschema.EncodeCPUInfoRows(observation, columns)
		return collectedScan{rows: rows, warnings: observation.Warnings}, "", err
	case gatewaypb.HostTable_HOST_TABLE_CPU_TIMES:
		observation, err := r.collector.CPUTimes(ctx)
		if err != nil {
			return collectedScan{}, "CPU_TIMES_COLLECTION_FAILED", fmt.Errorf("CPU times collection failed: %w", err)
		}
		rows, err := hostschema.EncodeCPUTimesRows(observation, columns)
		return collectedScan{rows: rows, warnings: observation.Warnings}, "", err
	case gatewaypb.HostTable_HOST_TABLE_MEMORY:
		observation, err := r.collector.Memory(ctx)
		if err != nil {
			return collectedScan{}, "MEMORY_COLLECTION_FAILED", fmt.Errorf("memory collection failed: %w", err)
		}
		rows, err := hostschema.EncodeMemoryRows(observation, columns)
		return collectedScan{rows: rows, warnings: observation.Warnings}, "", err
	case gatewaypb.HostTable_HOST_TABLE_FILESYSTEMS:
		observation, err := r.collector.Filesystems(ctx)
		if err != nil {
			return collectedScan{}, "FILESYSTEMS_COLLECTION_FAILED", fmt.Errorf("filesystems collection failed: %w", err)
		}
		rows, err := hostschema.EncodeFilesystemRows(observation, columns)
		return collectedScan{rows: rows, warnings: observation.Warnings}, "", err
	case gatewaypb.HostTable_HOST_TABLE_NETWORK_INTERFACES:
		observation, err := r.collector.NetworkInterfaces(ctx)
		if err != nil {
			return collectedScan{}, "NETWORK_INTERFACES_COLLECTION_FAILED", fmt.Errorf("network interfaces collection failed: %w", err)
		}
		rows, err := hostschema.EncodeNetworkInterfaceRows(observation, columns)
		return collectedScan{rows: rows, warnings: observation.Warnings}, "", err
	case gatewaypb.HostTable_HOST_TABLE_NETWORK_IO:
		observation, err := r.collector.NetworkIO(ctx)
		if err != nil {
			return collectedScan{}, "NETWORK_IO_COLLECTION_FAILED", fmt.Errorf("network I/O collection failed: %w", err)
		}
		rows, err := hostschema.EncodeNetworkIORows(observation, columns)
		return collectedScan{rows: rows, warnings: observation.Warnings}, "", err
	case gatewaypb.HostTable_HOST_TABLE_PROCESSES:
		observation, err := r.collector.Processes(ctx)
		if err != nil {
			return collectedScan{}, "PROCESSES_COLLECTION_FAILED", fmt.Errorf("process collection failed: %w", err)
		}
		rows, err := hostschema.EncodeProcessRows(observation, columns)
		return collectedScan{rows: rows, warnings: observation.Warnings}, "", err
	default:
		return collectedScan{}, "", errors.New("agent does not implement the requested host table")
	}
}

func sendScanRows(
	ctx context.Context,
	sender *synchronizedSender,
	request *gatewaypb.ScanRequest,
	tuples []*providerv1.Tuple,
) error {
	batchSize := int(request.GetPreferredBatchRows())
	if batchSize <= 0 {
		batchSize = defaultPreferredBatchRows
	}
	if batchSize > maximumScanBatchRows {
		batchSize = maximumScanBatchRows
	}
	var batchIndex uint64
	batch := &gatewaypb.ScanBatch{
		ScanId: request.GetScanId(),
		Rows:   make([]*gatewaypb.ScanRow, 0, batchSize),
	}
	flush := func() error {
		if len(batch.Rows) == 0 {
			return nil
		}
		batch.BatchIndex = batchIndex
		if err := sender.Send(&gatewaypb.ConnectRequest{
			Payload: &gatewaypb.ConnectRequest_ScanBatch{ScanBatch: batch},
		}); err != nil {
			return err
		}
		batchIndex++
		batch = &gatewaypb.ScanBatch{
			ScanId: request.GetScanId(),
			Rows:   make([]*gatewaypb.ScanRow, 0, batchSize),
		}
		return nil
	}
	for _, tuple := range tuples {
		if err := ctx.Err(); err != nil {
			return err
		}
		row := &gatewaypb.ScanRow{Values: tuple.GetValues()}
		batch.Rows = append(batch.Rows, row)
		if proto.Size(batch) > maximumScanBatchBytes {
			batch.Rows = batch.Rows[:len(batch.Rows)-1]
			if len(batch.Rows) == 0 {
				return fmt.Errorf("%w (%d bytes)", errScanRowTooLarge, proto.Size(row))
			}
			if err := flush(); err != nil {
				return err
			}
			batch.Rows = append(batch.Rows, row)
			if proto.Size(batch) > maximumScanBatchBytes {
				return fmt.Errorf("%w (%d bytes)", errScanRowTooLarge, proto.Size(row))
			}
		}
		if len(batch.Rows) == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func sendFailedScan(
	sender *synchronizedSender,
	scanID string,
	code string,
	message string,
	retryable bool,
) error {
	return sendScanFinished(
		sender,
		scanID,
		gatewaypb.ScanStatus_SCAN_STATUS_FAILED,
		[]*gatewaypb.ScanDiagnostic{{
			Code:      code,
			Message:   message,
			Retryable: retryable,
		}},
	)
}

func sendCancelledScan(
	sender *synchronizedSender,
	scanID string,
	cause error,
) error {
	code := "HOST_SCAN_CANCELLED"
	message := "agent cancelled the host scan"
	if errors.Is(cause, context.DeadlineExceeded) {
		code = "HOST_SCAN_TIMEOUT"
		message = "host scan exceeded its deadline"
	}
	return sendScanFinished(
		sender,
		scanID,
		gatewaypb.ScanStatus_SCAN_STATUS_CANCELLED,
		[]*gatewaypb.ScanDiagnostic{{
			Code:      code,
			Message:   message,
			Retryable: true,
		}},
	)
}

func sendScanFinished(
	sender *synchronizedSender,
	scanID string,
	statusValue gatewaypb.ScanStatus,
	diagnostics []*gatewaypb.ScanDiagnostic,
) error {
	if err := sender.Send(&gatewaypb.ConnectRequest{
		Payload: &gatewaypb.ConnectRequest_ScanFinished{ScanFinished: &gatewaypb.ScanFinished{
			ScanId:      scanID,
			Status:      statusValue,
			Diagnostics: diagnostics,
		}},
	}); err != nil {
		return fmt.Errorf("send scan completion: %w", err)
	}
	return nil
}
