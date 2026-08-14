package openapi

import (
	"context"
	"io"
	"net/url"
	"sync"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type resultStream struct {
	mu          sync.Mutex
	connection  *Connection
	descriptor  *entityDescriptor
	projections []queryProjection
	fields      []*providerv1.Field
	parameters  url.Values
	buffer      []*providerv1.Tuple
	batchSize   int
	pagination  paginationState
	skip        uint64
	remaining   uint64
	hasLimit    bool
	done        bool
	closed      bool
	terminalErr error
}

type paginationState struct {
	pages       uint32
	offset      uint64
	page        uint64
	cursor      string
	seenCursors map[string]struct{}
}

func newResultStream(
	connection *Connection,
	descriptor *entityDescriptor,
	projections []queryProjection,
	batchSize int,
	parameters url.Values,
	limit *uint64,
	offset *uint64,
) providersdk.ResultStream {
	fields := make([]*providerv1.Field, len(projections))
	for index, projection := range projections {
		fields[index] = projection.field
	}
	state := paginationState{seenCursors: make(map[string]struct{})}
	if pagination := descriptor.config.Pagination; pagination != nil && pagination.StartPage != nil {
		state.page = *pagination.StartPage
	}
	var skip uint64
	if offset != nil {
		if pagination := descriptor.config.Pagination; pagination != nil && pagination.Mode == PaginationModeOffset {
			state.offset = *offset
		} else {
			skip = *offset
		}
	}
	var remaining uint64
	hasLimit := limit != nil
	if hasLimit {
		remaining = *limit
	}
	return &resultStream{
		connection:  connection,
		descriptor:  descriptor,
		projections: projections,
		fields:      fields,
		parameters:  cloneURLValues(parameters),
		batchSize:   batchSize,
		pagination:  state,
		skip:        skip,
		remaining:   remaining,
		hasLimit:    hasLimit,
		done:        hasLimit && remaining == 0,
	}
}

func cloneURLValues(values url.Values) url.Values {
	cloned := make(url.Values, len(values))
	for name, candidates := range values {
		cloned[name] = append([]string(nil), candidates...)
	}
	return cloned
}

func (s *resultStream) Next(ctx context.Context) (*providerv1.TupleBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, io.EOF
	}

	for len(s.buffer) < s.batchSize && !s.done {
		rows, done, err := s.connection.fetchPage(ctx, s.descriptor, &s.pagination, s.parameters)
		if err != nil {
			s.done = true
			s.terminalErr = err
			if len(s.buffer) == 0 {
				return nil, err
			}
			break
		}
		for rowIndex, row := range rows {
			if s.skip > 0 {
				s.skip--
				continue
			}
			if s.hasLimit && s.remaining == 0 {
				s.done = true
				break
			}
			tuple, err := rowTuple(row, s.projections)
			if err != nil {
				s.done = true
				s.terminalErr = status.Errorf(codes.Internal, "decode OpenAPI row %d: %v", rowIndex, err)
				if len(s.buffer) == 0 {
					return nil, s.terminalErr
				}
				break
			}
			s.buffer = append(s.buffer, tuple)
			if s.hasLimit {
				s.remaining--
				if s.remaining == 0 {
					s.done = true
					break
				}
			}
		}
		if s.terminalErr != nil {
			break
		}
		s.done = s.done || done
	}
	if len(s.buffer) == 0 {
		if s.terminalErr != nil {
			return nil, s.terminalErr
		}
		return nil, io.EOF
	}

	end := s.batchSize
	if end > len(s.buffer) {
		end = len(s.buffer)
	}
	batch := &providerv1.TupleBatch{
		Fields: s.fields,
		Tuples: append([]*providerv1.Tuple(nil), s.buffer[:end]...),
	}
	s.buffer = append(s.buffer[:0], s.buffer[end:]...)
	return batch, nil
}

func (s *resultStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.connection = nil
	s.descriptor = nil
	s.projections = nil
	s.fields = nil
	s.parameters = nil
	s.buffer = nil
	err := s.terminalErr
	s.mu.Unlock()
	return err
}

var _ providersdk.ResultStream = (*resultStream)(nil)
