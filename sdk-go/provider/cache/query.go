package cache

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"

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
		} else {
			s.cacheable = false
		}

		return batch, err
	}
	if batch == nil {
		s.cacheable = false
		return nil, nil
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
		size:    s.size,
	}
	s.mu.Unlock()

	if cacheable {
		s.state.storeIfCurrent(s.capture, result)
	}

	return closeErr
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

var (
	_ providersdk.Connection   = (*cachedConnection)(nil)
	_ providersdk.ResultStream = (*recordingStream)(nil)
	_ providersdk.ResultStream = (*replayStream)(nil)
)
