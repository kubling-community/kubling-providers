package cache

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/protobuf/proto"
)

type cachedConnection struct {
	providersdk.Connection

	state *cacheState

	transactionMu     sync.Mutex
	transactionActive bool
	pendingEntities   map[string]struct{}
	pendingAll        bool
}

func (c *cachedConnection) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	if request == nil {
		return c.Connection.Query(ctx, request)
	}
	if c.inTransaction() {
		return c.Connection.Query(ctx, request)
	}

	digest, err := queryDigest(request)
	if err != nil {
		return nil, fmt.Errorf("build cache key: %w", err)
	}

	entity, err := normalizedEntityKey(request.GetEntity())
	if err != nil {
		return c.Connection.Query(ctx, request)
	}
	capture, result, found := c.state.lookup(
		entity,
		digest,
	)
	if found {
		return &replayStream{result: result}, nil
	}

	stream, err := c.Connection.Query(ctx, request)
	if err != nil || stream == nil {
		return stream, err
	}

	return &recordingStream{
		stream:    stream,
		state:     c.state,
		capture:   capture,
		cacheable: true,
	}, nil
}

func (c *cachedConnection) inTransaction() bool {
	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	return c.transactionActive
}

func queryDigest(request *providerv1.QueryRequest) ([sha256.Size]byte, error) {
	normalized := proto.Clone(request).(*providerv1.QueryRequest)
	normalized.ConnectionId = ""

	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
	if err != nil {
		return [sha256.Size]byte{}, err
	}

	return sha256.Sum256(encoded), nil
}

type recordingStream struct {
	stream  providersdk.ResultStream
	state   *cacheState
	capture queryCapture

	mu        sync.Mutex
	batches   []*providerv1.TupleBatch
	outcome   *providerv1.QueryOutcome
	size      int64
	cacheable bool
	complete  bool
	closed    bool
	closeErr  error
}

func (s *recordingStream) Next(
	ctx context.Context,
) (*providerv1.TupleBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, io.EOF
	}
	if s.complete {
		return nil, io.EOF
	}

	batch, err := s.stream.Next(ctx)

	if err != nil {
		if err == io.EOF {
			s.complete = true
			s.captureOutcome()
		} else {
			s.cacheable = false
		}

		return batch, err
	}
	if batch == nil {
		s.cacheable = false
		return nil, nil
	}

	if s.cacheable && batchContainsLobReference(batch) {
		s.cacheable = false
		s.batches = nil
		s.size = 0
	}
	if s.cacheable {
		cloned := proto.Clone(batch).(*providerv1.TupleBatch)
		size := int64(proto.Size(cloned))
		if s.size+size > s.state.maxEntryBytes {
			s.cacheable = false
			s.batches = nil
			s.size = 0
		} else {
			s.batches = append(s.batches, cloned)
			s.size += size
		}
	}

	return batch, nil
}

func batchContainsLobReference(batch *providerv1.TupleBatch) bool {
	for _, tuple := range batch.GetTuples() {
		for _, value := range tuple.GetValues() {
			if valueContainsLobReference(value) {
				return true
			}
		}
	}

	return false
}

func (s *recordingStream) captureOutcome() {
	outcome := &providerv1.QueryOutcome{
		Completion: providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE,
	}
	if outcomeStream, ok := s.stream.(providersdk.QueryOutcomeStream); ok {
		providerOutcome := outcomeStream.Outcome()
		if providerOutcome == nil {
			s.disableCaching()
			s.outcome = nil
			return
		}
		outcome = proto.Clone(providerOutcome).(*providerv1.QueryOutcome)
	}
	s.outcome = outcome

	// Partial, invalid and diagnostic-bearing results are deliberately not
	// cached. Re-execution preserves source visibility and ensures warnings are
	// not detached from the conditions that produced them.
	if outcome.GetCompletion() !=
		providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE ||
		len(outcome.GetWarnings()) > 0 {
		s.disableCaching()
		return
	}

	if !s.cacheable {
		return
	}
	outcomeSize := int64(proto.Size(outcome))
	if s.size+outcomeSize > s.state.maxEntryBytes {
		s.disableCaching()
		return
	}
	s.size += outcomeSize
}

func (s *recordingStream) disableCaching() {
	s.cacheable = false
	s.batches = nil
	s.size = 0
}

func valueContainsLobReference(value *kublingv1.Value) bool {
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_LobReference:
		return true
	case *kublingv1.Value_ArrayValue:
		for _, element := range typed.ArrayValue.GetElements() {
			if valueContainsLobReference(element) {
				return true
			}
		}
	}

	return false
}

func (s *recordingStream) Close() error {
	s.mu.Lock()
	if s.closed {
		closeErr := s.closeErr
		s.mu.Unlock()
		return closeErr
	}
	s.closed = true
	closeErr := s.stream.Close()
	s.closeErr = closeErr
	cacheable := closeErr == nil && s.complete && s.cacheable
	result := cachedResult{
		batches: append([]*providerv1.TupleBatch(nil), s.batches...),
		outcome: cloneQueryOutcome(s.outcome),
		size:    s.size,
	}
	s.mu.Unlock()

	if cacheable {
		s.state.storeIfCurrent(s.capture, result)
	}

	return closeErr
}

func (s *recordingStream) Outcome() *providerv1.QueryOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.complete {
		return nil
	}

	return cloneQueryOutcome(s.outcome)
}

type replayStream struct {
	mu     sync.Mutex
	result cachedResult
	next   int
	closed bool
}

func (s *replayStream) Next(
	ctx context.Context,
) (*providerv1.TupleBatch, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.next >= len(s.result.batches) {
		return nil, io.EOF
	}

	batch := proto.Clone(s.result.batches[s.next]).(*providerv1.TupleBatch)
	s.next++

	return batch, nil
}

func (s *replayStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	return nil
}

func (s *replayStream) Outcome() *providerv1.QueryOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()

	return cloneQueryOutcome(s.result.outcome)
}

func cloneQueryOutcome(outcome *providerv1.QueryOutcome) *providerv1.QueryOutcome {
	if outcome == nil {
		return nil
	}
	return proto.Clone(outcome).(*providerv1.QueryOutcome)
}

var (
	_ providersdk.Connection         = (*cachedConnection)(nil)
	_ providersdk.ResultStream       = (*recordingStream)(nil)
	_ providersdk.ResultStream       = (*replayStream)(nil)
	_ providersdk.QueryOutcomeStream = (*recordingStream)(nil)
	_ providersdk.QueryOutcomeStream = (*replayStream)(nil)
)
