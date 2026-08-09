package redis

import (
	"context"
	"io"
	"sync"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
)

type resultStream struct {
	mu        sync.Mutex
	fields    []*providerv1.Field
	tuples    []*providerv1.Tuple
	batchSize int
	index     int
	closed    bool
}

func newResultStream(fields []*providerv1.Field, tuples []*providerv1.Tuple, batchSize int) providersdk.ResultStream {
	return &resultStream{fields: fields, tuples: tuples, batchSize: batchSize}
}

func (s *resultStream) Next(ctx context.Context) (*providerv1.TupleBatch, error) {
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

func (s *resultStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.fields = nil
	s.tuples = nil
	s.mu.Unlock()
	return nil
}

var _ providersdk.ResultStream = (*resultStream)(nil)
